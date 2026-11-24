"""Upgrade check: a store written by an older Trailbeam (0.3.0 or 0.4.0) keeps working under this one.

    # SQLite, both servers from pip installs (the old one in its own venv):
    python smoke/upgrade_check.py --store sqlite --old-cmd /tmp/old/bin/trailbeam --new-cmd trailbeam
    # SQLite, the published image, then this checkout's image, on one Docker volume:
    python smoke/upgrade_check.py --store sqlite --old-image ghcr.io/nick-lennox/trailbeam:0.3.0 --new-image tb:ci
    # Postgres (the image has SQLite only, so pip installs):
    python smoke/upgrade_check.py --store postgres --pg-url postgresql://postgres:postgres@localhost:5432/postgres \\
        --old-cmd /tmp/old/bin/trailbeam --new-cmd trailbeam

What it does, with one access token throughout:

1. Makes a realistic set of spans (bench/loadgen.py: 1,200 messages over the last ~2.5 days, several queues
   and tenants, bodies on two queues, redeliveries, dead-letter moves, stuck messages, late-joining
   receives) and splits the export batches in two. Messages whose spans straddle the split are in flight
   across the upgrade.
2. The old server ingests the first part; everything it serves is recorded (every message's timeline, the
   lists, tenants, stats, a few bodies and web pages), and a session cookie from its login page is kept.
3. The new server starts on the same file or database. The schema must move forward by itself; every
   answer the old server gave must come back the same (ignoring fields the new one added, and the wording
   of a notice, whose ``code`` must stay), and must equal what a new server given the same spans from
   empty serves. The token and the old session cookie must still work. With SQLite and commands, the new
   server runs as ``trailbeam start`` on the data directory the old one used; the token and receipt-hash
   key files there must be kept as they were, and not printed: the banner shows a ``cat`` command for each
   and reads them with ``$(cat ...)`` in the environment lines.
4. The new server ingests the rest, then everything must equal a new server given all the spans from empty:
   so the in-flight messages joined across the upgrade.
5. The old server is started again on the upgraded store. Postgres: it must refuse, saying a newer Trailbeam
   wrote it, and leave the database as it is. SQLite: the same, when the new server moved the file's schema
   (0.4 adds schema 13, the probes of ``trailbeam doctor``; then 14, deduplicated FIFO sends, and 15, the tables of people and tokens); when it left the schema as it was, the old server
   must start and serve the same data. Then the new server must again serve exactly step 4.

Exit status 0 when every check passes; each failure is printed. Leaves nothing behind but ``--workdir``
(removed unless ``--keep``), and with images, removes the containers and volume it made.
"""
from __future__ import annotations

import argparse
import http.client
import json
import os
import random
import re
import secrets
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import traceback
import urllib.parse
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / "bench"))
import loadgen  # noqa: E402

NS = 1_000_000_000
H = 3600 * NS
N_MESSAGES = 1200
SPLIT = 0.7  # the share of export batches the old server ingests
PROFILE = loadgen.Profile(queues=6, tenants=8, services=4, body_size=300, body_queues=2, late_join=0.2,
                          late_by_batches=2, batch_spans=200, redelivery=0.08, dlq=0.03, stuck=0.03)
STUCK_AFTER_S = 600  # every message is older than this, so "stuck" doesn't change while the check runs

failures: list = []


def fail(what: str) -> None:
    failures.append(what)
    print(f"FAIL: {what}", flush=True)


def ok(what: str) -> None:
    print(f"ok: {what}", flush=True)


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


# ---- the spans ----------------------------------------------------------------------------


def make_batches(now_ns: int) -> list:
    start = now_ns - 60 * H
    step = (58 * H) // N_MESSAGES
    n = iter(range(10 ** 9))
    stream = loadgen.Stream(PROFILE, seed=170, clock=lambda: start + next(n) * step)
    return [loadgen.encode(b) for b in stream.batches_of_messages(N_MESSAGES)]


# ---- HTTP ---------------------------------------------------------------------------------


def request(port: int, method: str, path: str, *, token=None, cookie=None, body=None, ctype=None, timeout=60):
    c = http.client.HTTPConnection("127.0.0.1", port, timeout=timeout)
    headers = {}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    if cookie:
        headers["Cookie"] = cookie
    if ctype:
        headers["Content-Type"] = ctype
    try:
        c.request(method, path, body=body, headers=headers)
        r = c.getresponse()
        return r.status, dict(r.getheaders()), r.read()
    finally:
        c.close()


def api(port: int, path: str, token: str):
    status, _, data = request(port, "GET", path, token=token)
    if status != 200:
        raise AssertionError(f"GET {path}: {status} {data[:200]!r}")
    return json.loads(data)


def ingest(port: int, token: str, batches: list) -> None:
    for b in batches:
        for attempt in range(20):
            status, _, data = request(port, "POST", "/v1/traces", token=token, body=b,
                                      ctype="application/x-protobuf")
            if status == 200:
                break
            if status in (429, 503):
                time.sleep(0.5)
                continue
            raise AssertionError(f"POST /v1/traces: {status} {data[:200]!r}")
        else:
            raise AssertionError("POST /v1/traces: still refused after 20 tries")


def login_cookie(port: int, token: str) -> str:
    body = urllib.parse.urlencode({"token": token, "next": "/"}).encode()
    status, headers, _ = request(port, "POST", "/login", body=body, ctype="application/x-www-form-urlencoded")
    cookie = next((v for k, v in headers.items() if k.lower() == "set-cookie"), "")
    if status != 303 or not cookie:
        raise AssertionError(f"POST /login: {status}, no session cookie")
    return cookie.split(";", 1)[0]


def wait_ready(port: int, proc_alive, timeout_s: float = 180) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if not proc_alive():
            raise AssertionError("the server exited before it was ready")
        try:
            if request(port, "GET", "/api/ready", timeout=5)[0] == 200:
                return
        except OSError:
            pass
        time.sleep(0.3)
    raise AssertionError(f"not ready after {timeout_s:.0f} s")


# ---- what a server serves -------------------------------------------------------------------


def snapshot(port: int, token: str, window: tuple) -> dict:
    """Everything the API serves about the stored spans, and a few web pages (checked here, not compared)."""
    def pages(path: str, key: str) -> list:
        out, cursor = [], None
        while True:
            q = {"limit": 1000, **({"cursor": cursor} if cursor else {})}
            page = api(port, f"/api/{path}?{urllib.parse.urlencode(q)}", token)
            out += page[key]
            cursor = page.get("next_cursor")
            if not cursor:
                return out

    messages = pages("messages", "messages")
    ids = [m["message_id"] for m in messages]
    snap = {
        "messages": messages,
        "timelines": {mid: api(port, "/api/messages/" + urllib.parse.quote(mid, safe=""), token) for mid in ids},
        "duplicates": api(port, "/api/duplicates?limit=1000", token),
        "stuck": drop_age(api(port, "/api/stuck?limit=1000", token)),
        "tenants": api(port, "/api/tenants?" + urllib.parse.urlencode({"from_ms": window[0], "to_ms": window[1]}), token),
        "bodies": {},
    }
    for queue in loadgen.body_queue_names(PROFILE):
        for mid in [m["message_id"] for m in messages if m.get("source_queue") == queue][:5]:
            status, _, data = request(port, "GET", "/api/messages/" + urllib.parse.quote(mid, safe="") + "/body",
                                      token=token)
            snap["bodies"][mid] = [status, data.decode("utf-8", "replace")]
    stats = api(port, "/api/stats", token)
    snap["stats"] = {k: v for k, v in stats.items() if k in ("spans", "messages", "queues")}
    for mid in random.Random(1).sample(ids, min(10, len(ids))):
        status, _, html = request(port, "GET", "/message?id=" + urllib.parse.quote(mid), token=token)
        # the UI shell (the wheel and image have it) loads the data with the API; without it, the server-rendered page
        shell = b'id="root"' in html or b"<script" in html
        if status != 200 or not (shell or mid in html.decode("utf-8", "replace")):
            fail(f"web page of message {mid}: {status}")
    status, _, _ = request(port, "GET", "/", token=token)
    if status != 200:
        fail(f"web screen /: {status}")
    return snap


def drop_age(page: dict) -> dict:
    """``age_s`` counts from now: the rest of a stuck message is what must not change."""
    for m in page.get("messages", []):
        m.pop("age_s", None)
    return page


NOTICE = re.compile(r"\.notices\[\d+\]$")


def diffs(old, new, path="", limit=10, *, notice_words=True) -> list:
    """Where ``new`` differs from ``old``, ignoring dict keys ``old`` doesn't have (fields added since).

    ``notice_words=False`` skips the ``message`` of each notice: across versions a notice keeps its
    ``code`` (the contract), but its wording may be rewritten.
    """
    out: list = []

    def walk(a, b, p):
        if len(out) >= limit:
            return
        if isinstance(a, dict) and isinstance(b, dict):
            for k in a:
                if k == "message" and not notice_words and NOTICE.search(p):
                    continue
                if k not in b:
                    out.append(f"{p}.{k}: missing")
                else:
                    walk(a[k], b[k], f"{p}.{k}")
        elif isinstance(a, list) and isinstance(b, list):
            if len(a) != len(b):
                out.append(f"{p}: {len(a)} items, now {len(b)}")
            for i, (x, y) in enumerate(zip(a, b)):
                walk(x, y, f"{p}[{i}]")
        elif a != b:
            out.append(f"{p}: {str(a)[:120]!r} -> {str(b)[:120]!r}")

    walk(old, new, path)
    return out


def same(what: str, expected: dict, got: dict, *, exact: bool) -> None:
    """``exact``: both answers come from this version. Otherwise one is the old server's, and fields added
    since and the wording of notices are not compared."""
    d = diffs(expected, got, notice_words=exact) + (diffs(got, expected) if exact else [])
    if d:
        fail(f"{what}:\n    " + "\n    ".join(d))
    else:
        ok(f"{what} ({len(got['timelines'])} messages)")


# ---- servers ------------------------------------------------------------------------------


class Server:
    """One Trailbeam server process (a command) or container (an image), on a port of its own."""

    def __init__(self, label: str, *, cmd=None, image=None, store: dict, token_file: Path, extra=(),
                 start_dir: Path | None = None):
        self.label, self.cmd, self.image = label, cmd, image
        self.store, self.token_file, self.extra, self.start_dir = store, token_file, list(extra), start_dir
        self.port = free_port()
        self.proc = None
        self.container = None
        self.log = None

    def _args(self, inside_container: bool) -> list:
        a = ["--stuck-after", str(STUCK_AFTER_S), "--store-bodies", ",".join(loadgen.body_queue_names(PROFILE)),
             *self.extra]
        if "url" in self.store:
            a += ["--store-url", self.store["url"]]
        elif not inside_container and not self.start_dir:
            a += ["--db", str(self.store["file"])]
        if not inside_container and not self.start_dir:
            a += ["--token-file", str(self.token_file)]
        return a

    def run(self, workdir: Path, wait=True):
        self.log = workdir / f"{self.label}.log"
        if self.image:
            name = f"tb-upgrade-{self.label}-{secrets.token_hex(3)}"
            subprocess.run(["docker", "run", "-d", "--name", name, "-p", f"127.0.0.1:{self.port}:4318",
                            "-v", f"{self.store['volume']}:/data", "-e", f"TRAILBEAM_TOKEN={self.token_file.read_text().strip()}",
                            self.image, *self._args(True)], check=True, stdout=subprocess.DEVNULL)
            self.container = name
        else:
            argv = [*self.cmd]
            if self.start_dir:
                argv += ["start", "--data-dir", str(self.start_dir)]
            argv += ["--host", "127.0.0.1", "--port", str(self.port), *self._args(False)]
            self.out = workdir / f"{self.label}.stdout"
            env = {k: v for k, v in os.environ.items() if not k.startswith("TRAILBEAM_")}
            self.proc = subprocess.Popen(argv, stdout=open(self.out, "wb"), stderr=open(self.log, "wb"), env=env)
        if wait:
            wait_ready(self.port, self.alive)
        return self

    def alive(self) -> bool:
        if self.proc:
            return self.proc.poll() is None
        if not self.container:
            return False
        r = subprocess.run(["docker", "inspect", "-f", "{{.State.Running}}", self.container],
                           capture_output=True, text=True)
        return r.stdout.strip() == "true"

    def wait_exit(self, timeout_s: float) -> int | None:
        if self.proc:
            try:
                return self.proc.wait(timeout_s)
            except subprocess.TimeoutExpired:
                return None
        r = subprocess.run(["docker", "wait", self.container], capture_output=True, text=True, timeout=timeout_s)
        return int(r.stdout.strip())

    def logs(self) -> str:
        if self.container:  # still there; after stop() the log file has them
            r = subprocess.run(["docker", "logs", self.container], capture_output=True, text=True)
            return r.stdout + r.stderr
        return self.log.read_text(errors="replace") if self.log and self.log.exists() else ""

    def stop(self) -> None:
        if self.proc and self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(60)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                fail(f"{self.label}: did not stop within 60 s of SIGTERM")
        if self.container:
            subprocess.run(["docker", "stop", "-t", "60", self.container], capture_output=True)
            if self.log:
                self.log.write_text(self.logs())
            subprocess.run(["docker", "rm", "-f", self.container], capture_output=True)
            self.container = None


# ---- stores -------------------------------------------------------------------------------


class Stores:
    """Fresh stores: SQLite files (or Docker volumes), or Postgres databases made on ``--pg-url``."""

    def __init__(self, kind: str, workdir: Path, pg_url=None, docker=False):
        self.kind, self.workdir, self.pg_url, self.docker = kind, workdir, pg_url, docker
        self.dbs: list = []
        self.volumes: list = []

    def new(self, name: str, *, volume=False) -> dict:
        if self.kind == "sqlite":
            if volume:
                v = f"tb-upgrade-{name}-{secrets.token_hex(3)}"
                subprocess.run(["docker", "volume", "create", v], check=True, stdout=subprocess.DEVNULL)
                self.volumes.append(v)
                return {"volume": v}
            d = self.workdir / name
            d.mkdir(mode=0o700)
            return {"file": d / "trailbeam.sqlite", "dir": d}
        import psycopg
        db = f"tb_upgrade_{name}_{secrets.token_hex(3)}"
        with psycopg.connect(self.pg_url, autocommit=True) as c:
            c.execute(f'CREATE DATABASE "{db}"')
        self.dbs.append(db)
        u = urllib.parse.urlsplit(self.pg_url)
        return {"url": urllib.parse.urlunsplit(u._replace(path="/" + db)), "db": db}

    def pg_version(self, store: dict):
        import psycopg
        with psycopg.connect(store["url"]) as c:
            return c.execute("SELECT MAX(version) FROM schema_version").fetchone()[0]

    def cleanup(self) -> None:
        if self.dbs:
            import psycopg
            with psycopg.connect(self.pg_url, autocommit=True) as c:
                for db in self.dbs:
                    c.execute(f'DROP DATABASE IF EXISTS "{db}" WITH (FORCE)')
        for v in self.volumes:
            subprocess.run(["docker", "volume", "rm", "-f", v], capture_output=True)


def sqlite_version(path: Path) -> int:
    import sqlite3
    with sqlite3.connect(f"file:{path}?mode=ro", uri=True) as c:
        return c.execute("SELECT MAX(version) FROM schema_version").fetchone()[0]


# ---- the check ----------------------------------------------------------------------------


def run(a) -> int:
    workdir = Path(a.workdir or tempfile.mkdtemp(prefix="tb-upgrade-"))
    workdir.mkdir(parents=True, exist_ok=True)
    old_kw = {"image": a.old_image} if a.old_image else {"cmd": a.old_cmd.split()}
    new_kw = {"image": a.new_image} if a.new_image else {"cmd": a.new_cmd.split()}
    # the servers given the spans from empty: this checkout, as the interpreter running the check has it
    ref_kw = {"cmd": a.new_cmd.split() if a.new_cmd else [sys.executable, "-m", "trailbeam.server"]}
    stores = Stores(a.store, workdir, a.pg_url)
    running: list = []

    def server(label, kw, store, **extra):
        s = Server(label, store=store, token_file=token_file, **kw, **extra)
        running.append(s)
        return s

    now_ns = time.time_ns()
    window = ((now_ns - 72 * H) // 1_000_000, now_ns // 1_000_000)
    batches = make_batches(now_ns)
    cut = int(len(batches) * SPLIT)
    print(f"{len(batches)} export batches of up to {PROFILE.batch_spans} spans, {N_MESSAGES} messages; "
          f"the old server ingests {cut}", flush=True)

    token = secrets.token_hex(32)
    key = secrets.token_hex(32)
    token_file = workdir / "token"
    token_file.write_text(token + "\n")
    token_file.chmod(0o600)
    sqlite_old = sqlite_new = None  # the SQLite file's schema under the old and the new server, when it is at hand
    try:
        # What this version serves for the same spans, from empty: after the first part, and after all of them.
        refs = {}
        for name, part in (("ref_part", batches[:cut]), ("ref_all", batches)):
            s = server(name, ref_kw, stores.new(name)).run(workdir)
            ingest(s.port, token, part)
            refs[name] = snapshot(s.port, token, window)
            s.stop()

        # The store the upgrade happens on. SQLite with commands: laid out as trailbeam start keeps it.
        upgraded = stores.new("upgraded", volume=bool(a.old_image or a.new_image) and a.store == "sqlite")
        use_start = a.store == "sqlite" and not (a.old_image or a.new_image)
        if use_start:
            shutil.copy(token_file, upgraded["dir"] / "token")
            (upgraded["dir"] / "token").chmod(0o600)
            (upgraded["dir"] / "receipt-hash-key").write_text(key + "\n")
            (upgraded["dir"] / "receipt-hash-key").chmod(0o600)

        old = server("old", old_kw, upgraded).run(workdir)
        ingest(old.port, token, batches[:cut])
        before = snapshot(old.port, token, window)
        cookie = login_cookie(old.port, token)
        old_version = api(old.port, "/api/health", token).get("version")
        old.stop()
        if old.alive():
            fail("the old server is still running")
        ok(f"old server ({old_version}) ingested {len(before['timelines'])} messages")
        if a.store == "postgres":
            v = stores.pg_version(upgraded)
            print(f"   Postgres schema after the old server: {v}")
        elif "file" in upgraded:
            sqlite_old = sqlite_version(upgraded["file"])
            print(f"   SQLite schema after the old server: {sqlite_old}")

        new = server("new", new_kw, upgraded, **({"start_dir": upgraded["dir"]} if use_start else {})).run(workdir)
        new_version = api(new.port, "/api/health", token).get("version")
        ok(f"new server ({new_version}) started on the old store with the old token")
        if a.store == "postgres":
            v = stores.pg_version(upgraded)
            (ok if v == a.expect_pg_schema else fail)(f"Postgres schema moved to {v} (expected {a.expect_pg_schema})")
        elif "file" in upgraded:
            sqlite_new = sqlite_version(upgraded["file"])
            print(f"   SQLite schema under the new server: {sqlite_new}")
        after = snapshot(new.port, token, window)
        same("every answer of the old server is the same after the upgrade", before, after, exact=False)
        same("after the upgrade = this version from empty, same spans", refs["ref_part"], after, exact=True)
        status = request(new.port, "GET", "/api/stats", cookie=cookie)[0]
        (ok if status == 200 else fail)(f"the old server's session cookie still works ({status})")
        status = request(new.port, "GET", "/api/stats", token="wrong" + token[5:])[0]
        (ok if status == 401 else fail)(f"a wrong token is still refused ({status})")
        if use_start:
            banner = new.out.read_text(errors="replace")
            # Since QV-203 the token and key are printed in full only on the run that made them; a later run
            # prints a command that shows them and env lines that read the files with $(cat ...).
            (ok if token not in banner else fail)("trailbeam start did not print the existing token")
            (ok if key not in banner else fail)("trailbeam start did not print the existing receipt-hash key")
            for f in ("token", "receipt-hash-key"):
                (ok if any("show it with: cat " in line and f in line for line in banner.splitlines()) else fail)(
                    f"trailbeam start showed how to read the existing {f} file")
            (ok if "$(cat " in banner else fail)("trailbeam start's environment lines read the files with $(cat ...)")
            for f in ("token", "receipt-hash-key"):
                (ok if (upgraded["dir"] / f).read_text().strip() == (token if f == "token" else key) else fail)(
                    f"trailbeam start left the {f} file as it was")

        ingest(new.port, token, batches[cut:])
        final = snapshot(new.port, token, window)
        same("after the rest of the spans = this version from empty, all spans (in-flight messages joined)",
             refs["ref_all"], final, exact=True)
        new.stop()

        # Downgrade.
        down = server("downgrade", old_kw, upgraded).run(workdir, wait=False)
        # Without the file at hand (the image's volume) the schema can't be read: a refusal is then expected,
        # as this version's SQLite schema is newer than 0.3.0's.
        schema_moved = a.store == "postgres" or sqlite_new is None or sqlite_new != sqlite_old
        if schema_moved:
            code = down.wait_exit(90)
            logs = down.logs()
            if code is None:
                fail("downgrade: the old server is still running on the upgraded database")
            elif code == 0:
                fail("downgrade: the old server exited with status 0")
            elif "newer Trailbeam" not in logs:
                fail(f"downgrade: refused (status {code}), but not saying why:\n{logs[-1500:]}")
            else:
                line = next(l for l in logs.splitlines() if "newer Trailbeam" in l)
                ok(f"downgrade refused, status {code}: {line.strip()[-220:]}")
            if a.store == "postgres":
                v = stores.pg_version(upgraded)
                (ok if v == a.expect_pg_schema else fail)(f"the database is still at schema {v} after the refused downgrade")
            else:
                v = sqlite_version(upgraded["file"]) if "file" in upgraded else None
                (ok if v == sqlite_new else fail)(f"the file is still at schema {v} after the refused downgrade")
        else:
            wait_ready(down.port, down.alive)
            same("downgrade (same SQLite schema): the old server serves what it served before, and the new spans",
                 final, snapshot(down.port, token, window), exact=False)
        down.stop()

        again = server("new_again", new_kw, upgraded, **({"start_dir": upgraded["dir"]} if use_start else {})).run(workdir)
        same("the new server after the downgrade attempt: unchanged", refs["ref_all"],
             snapshot(again.port, token, window), exact=True)
        again.stop()
    except Exception as e:  # noqa: BLE001: report it as a failure, with the servers' last log lines
        fail(f"{type(e).__name__}: {e}")
        traceback.print_exc()
        for s in running:
            print(f"--- {s.label} log (last lines)\n{s.logs()[-2000:]}")
    finally:
        for s in running:
            s.stop()
        stores.cleanup()
        if not a.keep:
            shutil.rmtree(workdir, ignore_errors=True)
    if failures:
        print(f"\n{len(failures)} check(s) failed")
        return 1
    print("\nupgrade check passed")
    return 0


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--store", choices=("sqlite", "postgres"), required=True)
    p.add_argument("--pg-url", help="a Postgres the check may create and drop databases on (with --store postgres)")
    old = p.add_mutually_exclusive_group(required=True)
    old.add_argument("--old-cmd", help="the old version's trailbeam command, e.g. /tmp/old/bin/trailbeam")
    old.add_argument("--old-image", help="the old version's image, e.g. ghcr.io/nick-lennox/trailbeam:0.3.0")
    new = p.add_mutually_exclusive_group(required=True)
    new.add_argument("--new-cmd", help="this version's trailbeam command")
    new.add_argument("--new-image", help="this version's image")
    p.add_argument("--expect-pg-schema", type=int, default=None,
                   help="the Postgres schema version the new server must leave (default: its pg_schema.LATEST)")
    p.add_argument("--workdir")
    p.add_argument("--keep", action="store_true", help="keep --workdir (the logs and SQLite files)")
    a = p.parse_args(argv)
    if a.store == "postgres" and not a.pg_url:
        p.error("--store postgres needs --pg-url")
    if a.store == "postgres" and (a.old_image or a.new_image):
        p.error("the images have SQLite only: use --old-cmd and --new-cmd with --store postgres")
    if bool(a.old_image) != bool(a.new_image):
        p.error("give both --old-image and --new-image, or both commands")
    if a.expect_pg_schema is None:
        from trailbeam.server import pg_schema
        a.expect_pg_schema = pg_schema.LATEST
    return run(a)


if __name__ == "__main__":
    sys.exit(main())
