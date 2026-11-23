"""Trailbeam server fault tests (QV-238): what is stored and shown when things break under steady ingest.

    python bench/faults.py                       # every case, SQLite and Postgres (needs Docker for Postgres
                                                 # and for the disk-full cases)
    python bench/faults.py --case kill9 --case duplicate --backend sqlite
    python bench/faults.py --list

Each case starts a server (``python -m trailbeam.server``, or the image built from this checkout for the
SQLite disk-full case), runs steady ingest against it from ``--workers`` exporter threads, injects one fault,
lets it recover without any help, then stops the ingest and checks three things:

- **stored**: every span the server acknowledged (answered 200) is in the database exactly once
  (``lost`` and ``doubled`` must be 0). Spans the server never acknowledged may or may not be stored: the
  exporter sent them again, and the copy is skipped (``unacked stored`` counts those);
- **shown**: ``/api/stats`` and the timelines of up to ``--sample`` messages are exactly what a clean SQLite
  server shows for the same stored spans, ingested once, in order (the reference);
- **recovered**: the server answered ``/api/ready`` 200 and acknowledged new exports after the fault ended,
  with nothing done by hand (for SIGKILL the process is started again, as Docker's or systemd's restart
  policy does).

The exporter threads behave as an OTLP/HTTP exporter that retries (the Collector's ``retry_on_failure``):
a connection error or 429/502/503/504 is retried with backoff for up to ``--give-up-s``; any other status is
final, so the export is dropped (``dropped``; a server fault if it isn't 400). The case fails when an
acknowledged span is lost or doubled, the API disagrees with the reference, the server doesn't recover, or
an export is dropped.

Docker objects are named ``qv238-*`` and removed when the case ends (``--keep`` keeps them for a look).
Results: a JSON file and a Markdown table (``--out``), as in README "Fault tests". The quick cases also run in
CI as ``tests/server/test_faults.py``.
"""
from __future__ import annotations

import argparse
import concurrent.futures
import dataclasses
import http.client
import json
import os
import random
import shutil
import signal
import socket
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
from collections import Counter
from pathlib import Path
from typing import Callable, Optional

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(ROOT / "python" / "tests" / "server"))

import loadgen  # noqa: E402

RETRYABLE = {429, 502, 503, 504}
NS = 10 ** 9
PG_IMAGE = "postgres:16"
PG_PASSWORD = "qv238-faults"
PREFIX = "qv238"


def log(*a) -> None:
    print(time.strftime("%H:%M:%S"), *a, flush=True)


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def get(base: str, path: str, timeout: float = 30) -> tuple[Optional[int], bytes]:
    try:
        with urllib.request.urlopen(base + path, timeout=timeout) as r:
            return r.status, r.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read()
    except (OSError, http.client.HTTPException):
        return None, b""


def wait_until(cond: Callable[[], bool], timeout: float, step: float = 0.2) -> Optional[float]:
    """Seconds until ``cond()`` was true, None if it wasn't within ``timeout``."""
    t0 = time.monotonic()
    while time.monotonic() - t0 < timeout:
        try:
            if cond():
                return time.monotonic() - t0
        except Exception:  # noqa: BLE001
            pass
        time.sleep(step)
    return None


def ready(base: str) -> bool:
    return get(base, "/api/ready", timeout=5)[0] == 200


def docker(*args: str, check: bool = True, quiet: bool = True) -> str:
    r = subprocess.run(["docker", *args], capture_output=True, text=True)
    if check and r.returncode:
        raise RuntimeError(f"docker {' '.join(args)}: {r.stderr.strip()}")
    return r.stdout.strip()


# ---- what was sent ----------------------------------------------------------------------------------------

class Ledger:
    """Every span sent, in the order the exporters made them, and which were acknowledged."""

    def __init__(self):
        self.lock = threading.Lock()
        self.spans: dict = {}         # (trace_hex, span_hex) -> GenSpan, in creation order
        self.acked: set = set()
        self.expired: set = set()     # sent older than the retention on purpose (retention cases)
        self.dropped: Counter = Counter()  # final status (or "gave up") -> exports
        self.exports = Counter()      # "acked", "retried" (attempts that were retried), "resent"
        self.acked_at: list = []      # monotonic times of acknowledgements

    @staticmethod
    def key(s) -> tuple:
        return s.trace_id.hex(), s.span_id.hex()

    def made(self, batch, expired: bool = False) -> None:
        with self.lock:
            for s in batch:
                self.spans.setdefault(self.key(s), s)
                if expired:
                    self.expired.add(self.key(s))

    def ack(self, batch) -> None:
        with self.lock:
            self.acked.update(self.key(s) for s in batch)
            self.exports["acked"] += 1
            self.acked_at.append(time.monotonic())

    def acked_since(self, t: float) -> int:
        with self.lock:
            return sum(1 for a in self.acked_at if a >= t)


class Exporter:
    """One exporter thread's HTTP: POST, retried as an OTLP exporter with retry_on_failure does."""

    def __init__(self, url_fn: Callable[[], str], give_up_s: float, ledger: Ledger):
        self.url_fn = url_fn
        self.give_up_s = give_up_s
        self.ledger = ledger

    def send(self, body: bytes) -> object:
        """200, or the final status that dropped the export, or "gave up"."""
        end = time.monotonic() + self.give_up_s
        backoff = 0.05
        while True:
            status, retry_after = None, None
            try:
                req = urllib.request.Request(self.url_fn() + "/v1/traces", data=body,
                                             headers={"Content-Type": "application/x-protobuf"})
                with urllib.request.urlopen(req, timeout=15) as r:
                    r.read()
                    status = r.status
            except urllib.error.HTTPError as e:
                status, retry_after = e.code, e.headers.get("Retry-After")
                e.read()
            except (OSError, http.client.HTTPException):
                status = None
            if status == 200:
                return 200
            if status is not None and status not in RETRYABLE:
                return status
            with self.ledger.lock:
                self.ledger.exports["retried"] += 1
            if time.monotonic() > end:
                return "gave up"
            wait = backoff
            if retry_after and retry_after.isdigit():
                wait = min(float(retry_after), 5.0)  # a cap, so a case doesn't wait out a long Retry-After
            time.sleep(wait * random.uniform(0.8, 1.2))
            backoff = min(backoff * 2, 2.0)


@dataclasses.dataclass
class Traffic:
    """What the exporter threads send."""
    workers: int = 4
    batch_spans: int = 64
    interval_s: float = 0.05     # between one worker's exports, after its previous one finished
    twice: bool = False          # every acknowledged export is sent again (a client retry after a lost answer)
    disorder: int = 0            # >0: spans are shuffled over a window of this many batches before sending
    skew_ns: tuple = ()          # per service, added to its spans' times (a skewed clock); () = none
    clock_ns: Optional[Callable[[], int]] = None  # the messages' send time; default: a few seconds ago
    expired: bool = False        # spans are recorded as sent past the retention on purpose


class Ingest:
    """Steady ingest: ``traffic.workers`` threads, each with a loadgen.Stream of its own, until ``stop``."""

    def __init__(self, url_fn: Callable[[], str], ledger: Ledger, traffic: Traffic, *, give_up_s: float = 300,
                 seed: int = 0, profile: Optional[loadgen.Profile] = None):
        self.ledger = ledger
        self.t = traffic
        self.exporter = Exporter(url_fn, give_up_s, ledger)
        self.profile = dataclasses.replace(profile or loadgen.Profile(), batch_spans=traffic.batch_spans,
                                           late_join=0.0)
        self.seed = seed
        self._stop = threading.Event()
        self.threads = [threading.Thread(target=self._run, args=(i,), daemon=True) for i in range(traffic.workers)]

    def start(self) -> "Ingest":
        for t in self.threads:
            t.start()
        return self

    def stop(self) -> None:
        """Stop making exports; wait for the ones under way (and their retries) to finish."""
        self._stop.set()
        for t in self.threads:
            t.join()

    def _skewed(self, s):
        if not self.t.skew_ns:
            return s
        d = self.t.skew_ns[s.service % len(self.t.skew_ns)]
        return dataclasses.replace(s, start_ns=s.start_ns + d, end_ns=s.end_ns + d)

    def _post(self, batch, worker: int) -> None:
        body = loadgen.encode(batch, worker)
        got = self.exporter.send(body)
        if got != 200:
            with self.ledger.lock:
                self.ledger.dropped[str(got)] += 1
            return
        self.ledger.ack(batch)
        if self.t.twice:
            with self.ledger.lock:
                self.ledger.exports["resent"] += 1
            if self.exporter.send(body) != 200:  # the copy: answered 200 too, and stored once
                with self.ledger.lock:
                    self.ledger.dropped["resend not acknowledged"] += 1

    def _run(self, worker: int) -> None:
        rng = random.Random(self.seed * 1000 + worker)
        stream = loadgen.Stream(self.profile, seed=self.seed * 1000 + worker, worker=worker,
                                clock=self.t.clock_ns)
        pool: list = []
        while not self._stop.is_set():
            batch = [self._skewed(s) for s in stream.batch()]
            self.ledger.made(batch, expired=self.t.expired)
            if self.t.disorder:
                pool.extend(batch)
                if len(pool) < self.t.disorder * self.t.batch_spans:
                    continue
                rng.shuffle(pool)
                batch, pool = pool[:self.t.batch_spans], pool[self.t.batch_spans:]
            self._post(batch, worker)
            self._stop.wait(self.t.interval_s)
        while pool:  # what the shuffle still holds
            batch, pool = pool[:self.t.batch_spans], pool[self.t.batch_spans:]
            self._post(batch, worker)


# ---- the server under test ---------------------------------------------------------------------------------

class Server:
    """``python -m trailbeam.server`` as a subprocess on a fixed port, so a restart answers at the same URL."""

    def __init__(self, workdir: Path, store_args: list, *args: str, name: str = "server"):
        self.workdir = workdir
        self.store_args = list(store_args)
        self.args = list(args)
        self.port = free_port()
        self.name = name
        self.proc: Optional[subprocess.Popen] = None
        self.starts = 0
        self.log_path = workdir / f"{name}.log"

    @property
    def base(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    def start(self, wait: bool = True) -> "Server":
        env = {k: v for k, v in os.environ.items() if not k.startswith("TRAILBEAM_")}
        env["PYTHONUNBUFFERED"] = "1"
        with open(self.log_path, "ab") as f:
            f.write(f"\n---- start {self.starts + 1} ----\n".encode())
            self.proc = subprocess.Popen(
                [sys.executable, "-m", "trailbeam.server", *self.store_args, "--host", "127.0.0.1",
                 "--port", str(self.port), *self.args], env=env, stdout=f, stderr=f)
        self.starts += 1
        if wait and wait_until(lambda: ready(self.base), 120) is None:
            raise RuntimeError(f"{self.name} did not become ready; see {self.log_path}")
        return self

    def kill(self) -> None:
        self.proc.send_signal(signal.SIGKILL)
        self.proc.wait()

    def stop(self) -> None:
        if self.proc and self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(30)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait()

    def alive(self) -> bool:
        return self.proc is not None and self.proc.poll() is None

    def errors(self) -> list[str]:
        """Tracebacks and ERROR lines in its log."""
        lines = (self.log_path.read_text(errors="replace") if self.log_path.exists() else "").splitlines()
        out = []
        for i, line in enumerate(lines):
            if " ERROR " in line:
                out.append(line)
            elif line.startswith("Traceback"):  # its last line: the exception
                j = i + 1
                while j < len(lines) and (lines[j].startswith(" ") or not lines[j].strip()
                                          or lines[j].startswith("During handling") or lines[j].startswith("The above")
                                          or lines[j].startswith("Traceback")):
                    j += 1
                out.append("Traceback: " + (lines[j] if j < len(lines) else "?"))
        return out


class Postgres:
    """A Postgres container of its own, on a fixed host port (so a restart keeps the URL). ``disk``: its data
    directory on a file system of that many MB (small_disk), to fill it."""

    def __init__(self, workdir: Path, disk_mb: int = 0):
        self.name = f"{PREFIX}-pg-{os.getpid()}-{random.randrange(1 << 20):x}"
        self.port = free_port()
        mount = small_disk("/var/lib/postgresql/data", disk_mb) if disk_mb else []
        docker("run", "-d", "--name", self.name, "-p", f"127.0.0.1:{self.port}:5432", "-e",
               f"POSTGRES_PASSWORD={PG_PASSWORD}", "-e", "PGDATA=/var/lib/postgresql/data/pg", *mount, PG_IMAGE,
               "-c", "max_locks_per_transaction=256")
        if wait_until(self.up, 90) is None:
            raise RuntimeError("Postgres did not start: " + docker("logs", self.name, check=False))

    def url(self, db: str = "postgres") -> str:
        return f"postgresql://postgres:{PG_PASSWORD}@127.0.0.1:{self.port}/{db}"

    def up(self) -> bool:
        import psycopg
        try:
            with psycopg.connect(self.url(), connect_timeout=3) as c:
                c.execute("SELECT 1")
            return True
        except psycopg.Error:
            return False

    def create_db(self, name: str) -> str:
        import psycopg
        with psycopg.connect(self.url(), autocommit=True) as c:
            c.execute(f'CREATE DATABASE "{name}"')
        return self.url(name)

    def stop(self) -> None:
        docker("stop", "-t", "10", self.name)

    def start(self) -> None:
        docker("start", self.name)

    def restart(self, fast: bool = True) -> None:
        # -t 0: SIGKILL, so Postgres does crash recovery as after a power cut; else a clean fast shutdown
        docker("restart", "-t", "0" if fast else "10", self.name)

    def exec(self, *cmd: str, user: str = "root") -> str:
        return docker("exec", "-u", user, self.name, *cmd)

    def remove(self) -> None:
        docker("rm", "-f", "-v", self.name, check=False)


class ExternalPostgres:
    """A Postgres server that is already running (``--postgres-url``, e.g. CI's service): a database per case,
    dropped after it. It can't be stopped or filled, so the cases that do that need Docker (Postgres)."""

    def __init__(self, url: str):
        self.base_url = url
        self.made: list = []

    def url(self, db: str) -> str:
        from urllib.parse import urlsplit, urlunsplit
        return urlunsplit(urlsplit(self.base_url)._replace(path="/" + db))

    def create_db(self, name: str) -> str:
        import psycopg
        name = f"{name}_{os.getpid()}_{random.randrange(1 << 20):x}"
        with psycopg.connect(self.base_url, autocommit=True) as c:
            c.execute(f'CREATE DATABASE "{name}"')
        self.made.append(name)
        return self.url(name)

    def remove(self) -> None:
        import psycopg
        with psycopg.connect(self.base_url, autocommit=True) as c:
            for name in self.made:
                c.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')


NEEDS_OWN_POSTGRES = {"pg-down", "retention-restart", "disk-full"}


def small_disk(dst: str, mb: int) -> list:
    """``docker run`` options for a file system of ``mb`` MB at ``dst`` that fills up for real (ENOSPC): a
    tmpfs of that size, writable by any user. It lasts as long as the container, so the cases free it with
    ``docker exec`` while the container runs, and read the data out before removing it."""
    return ["--mount", f"type=tmpfs,dst={dst},tmpfs-size={mb * 1024 * 1024},tmpfs-mode=1777"]


# ---- what is stored, and what is shown ---------------------------------------------------------------------

def stored_sqlite(path: str) -> Counter:
    c = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        return Counter({(t, s): n for t, s, n in
                        c.execute("SELECT trace_id, span_id, count(*) FROM spans GROUP BY 1, 2")})
    finally:
        c.close()


def stored_postgres(url: str) -> Counter:
    import psycopg
    with psycopg.connect(url) as c:
        return Counter({(t, s): n for t, s, n in
                        c.execute("SELECT trace_id, span_id, count(*) FROM spans GROUP BY 1, 2")})


def message_id(s) -> Optional[str]:
    return s.attrs.get("messaging.message.id")


def reference(ledger: Ledger, keys: set):
    """A clean in-memory SQLite server holding ``keys``' spans, ingested once in the order they were made."""
    from trailbeam.server.db import SqliteStore
    from trailbeam.server.server import make_server

    db = SqliteStore(":memory:")
    spans = [s for k, s in ledger.spans.items() if k in keys]
    for i in range(0, len(spans), 512):
        db.insert(loadgen.records(spans[i:i + 512]))
    httpd = make_server(db, "127.0.0.1", 0, ui_dir=None)
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    return httpd, db, f"http://127.0.0.1:{httpd.server_address[1]}"


def compare_shown(base: str, ledger: Ledger, keys: set, sample: int, seed: int = 0) -> list[str]:
    """What the server shows, against the reference for the same spans: /api/stats and sampled timelines."""
    httpd, db, ref = reference(ledger, keys)
    problems = []
    try:
        for path in ["/api/stats"]:
            a, b = get(base, path, 300), get(ref, path, 300)
            if a[0] != 200 or json.loads(a[1]) != json.loads(b[1]):
                problems.append(f"{path}: {a[0]} {a[1][:300]!r} != reference {b[1][:300]!r}")
        mids = sorted({m for k in keys if (m := message_id(ledger.spans[k]))})
        for mid in random.Random(seed).sample(mids, min(sample, len(mids))):
            path = f"/api/messages/{urllib.request.quote(mid, safe='')}"
            a, b = get(base, path), get(ref, path)
            if a[0] != b[0] or (a[0] == 200 and json.loads(a[1]) != json.loads(b[1])):
                problems.append(f"{path}: differs from the reference ({a[0]} vs {b[0]})")
    finally:
        httpd.shutdown()
        httpd.server_close()
        db.close()
    return problems


def verdict(ledger: Ledger, stored: Counter, base: str, sample: int, *, recovered: dict,
            expect_expired_gone: bool = False) -> dict:
    sent = set(ledger.spans)
    keys = set(stored)
    live_acked = ledger.acked - ledger.expired
    lost = live_acked - keys
    doubled = {k for k, n in stored.items() if n > 1}
    unknown = keys - sent
    expired_left = keys & ledger.expired if expect_expired_gone else set()
    shown = compare_shown(base, ledger, keys, sample)
    dropped = {k: v for k, v in ledger.dropped.items()}
    ok = (not lost and not doubled and not unknown and not shown and not dropped and not expired_left
          and all(v is not None for v in recovered.values()))
    return {
        "ok": ok,
        "spans_sent": len(sent), "spans_acked": len(ledger.acked), "spans_stored": sum(stored.values()),
        "lost": len(lost), "doubled": len(doubled), "unknown": len(unknown),
        "unacked_stored": len(keys - ledger.acked), "expired_left": len(expired_left),
        "exports_acked": ledger.exports["acked"], "attempts_retried": ledger.exports["retried"],
        "exports_resent": ledger.exports["resent"], "dropped": dropped,
        "shown_problems": shown[:10], "recovered_s": recovered,
        "examples_lost": sorted(lost)[:3],
    }


# ---- the cases ---------------------------------------------------------------------------------------------

class Case:
    def __init__(self, a, workdir: Path):
        self.a = a
        self.workdir = workdir
        self.pg: Optional[Postgres] = None
        self.servers: list[Server] = []
        self.cleanup: list[Callable] = []

    def postgres(self, disk_mb: int = 0):
        if self.a.postgres_url and not disk_mb:
            self.pg = ExternalPostgres(self.a.postgres_url)
            self.cleanup.append(self.pg.remove)  # a database on someone else's server: always dropped
            return self.pg
        self.pg = Postgres(self.workdir, disk_mb)
        if not self.a.keep:
            self.cleanup.append(self.pg.remove)
        return self.pg

    def server(self, backend: str, *args: str, durability: str = "full", name: str = "server",
               store_url: Optional[str] = None) -> tuple[Server, Callable[[], Counter]]:
        """A server on a new store; returns it and a function reading what that store holds."""
        if backend == "sqlite":
            path = str(self.workdir / f"{name}.sqlite")
            store = ["--db", path] + (["--durability", durability] if durability != "full" else [])
            read = lambda: stored_sqlite(path)  # noqa: E731
        else:
            url = store_url or (self.pg or self.postgres()).create_db(f"faults_{name}".replace("-", "_"))
            store = ["--store-url", url]
            read = lambda: stored_postgres(url)  # noqa: E731
        s = Server(self.workdir, store, "--retention", "forever", "--log-level", "INFO", *args, name=name)
        self.servers.append(s)
        return s, read

    def close(self) -> None:
        for s in self.servers:
            s.stop()
        for f in reversed(self.cleanup):
            f()


def steady(a) -> Traffic:
    return Traffic(workers=a.workers, batch_spans=a.batch_spans, interval_s=a.interval_s)


def case_kill9(c: Case, backend: str, durability: str = "full") -> dict:
    """SIGKILL the server mid-write, three times, and start it again at once (a restart policy)."""
    srv, read = c.server(backend, durability=durability)
    srv.start()
    ledger = Ledger()
    ing = Ingest(lambda: srv.base, ledger, steady(c.a), give_up_s=c.a.give_up_s).start()
    recovered = {}
    for i in range(c.a.kills):
        time.sleep(c.a.phase_s * random.uniform(0.5, 1.5))
        srv.kill()
        log(f"  SIGKILL {i + 1}: {len(ledger.acked)} spans acknowledged so far")
        time.sleep(1)
        t0 = time.monotonic()
        srv.start(wait=False)
        recovered[f"kill {i + 1}"] = wait_until(lambda: ready(srv.base) and ledger.acked_since(t0) > 0, 120)
    time.sleep(c.a.phase_s)
    ing.stop()
    return verdict(ledger, read(), srv.base, c.a.sample, recovered=recovered)


def case_pg_down(c: Case, backend: str) -> dict:
    """Stop Postgres for ``--outage-s`` and start it again; the server is not touched."""
    srv, read = c.server(backend)
    srv.start()
    ledger = Ledger()
    ing = Ingest(lambda: srv.base, ledger, steady(c.a), give_up_s=c.a.give_up_s).start()
    time.sleep(c.a.phase_s)
    c.pg.stop()
    log(f"  Postgres stopped; {len(ledger.acked)} spans acknowledged so far")
    down = {"ready while down": None}
    if wait_until(lambda: not ready(srv.base), 30) is not None:
        down["ready while down"] = "503"
    time.sleep(c.a.outage_s)
    c.pg.start()
    t0 = time.monotonic()
    log("  Postgres started again")
    recovered = {"postgres back": wait_until(lambda: ready(srv.base) and ledger.acked_since(t0) > 0, 180)}
    time.sleep(c.a.phase_s)
    ing.stop()
    r = verdict(ledger, read(), srv.base, c.a.sample, recovered=recovered)
    r["server_alive"] = srv.alive() and srv.starts == 1
    r["ok"] = r["ok"] and r["server_alive"] and down["ready while down"] == "503"
    r["notes"] = [f"/api/ready during the outage: {down['ready while down'] or 'still 200'}"]
    return r


def case_retention_restart(c: Case, backend: str) -> dict:
    """Old spans (past the retention) are stored, then a pass deletes them under steady ingest, and the store
    goes away in the middle of it: Postgres restarted (killed), or for SQLite the server SIGKILLed."""
    srv, read = c.server(backend, "--purge-interval", "3600", name="retention")
    srv.args[srv.args.index("--retention") + 1] = "2d"
    srv.start()
    ledger = Ledger()
    old = dataclasses.replace(steady(c.a), clock_ns=lambda: time.time_ns() - 10 * 86400 * NS
                              - random.randrange(3 * 86400) * NS, expired=True, interval_s=0)
    seeding = Ingest(lambda: srv.base, ledger, old, give_up_s=c.a.give_up_s, seed=7).start()
    time.sleep(c.a.phase_s)
    seeding.stop()
    log(f"  {len(ledger.expired)} expired spans stored")
    ing = Ingest(lambda: srv.base, ledger, steady(c.a), give_up_s=c.a.give_up_s).start()
    # the pass: the first one ran at start, on an empty store; restart so the next runs now, on the old spans
    srv.stop()
    srv.args[srv.args.index("--purge-interval") + 1] = "30"
    srv.start()

    def running() -> bool:
        st, body = get(srv.base, "/api/health")
        return st == 200 and json.loads(body)["config"]["retention"]["running"] is not None
    during = wait_until(running, 60, step=0.05)
    if backend == "postgres":
        c.pg.restart(fast=True)
        log(f"  Postgres killed and restarted during the pass (found running after {during} s)")
    else:
        srv.kill()
        log(f"  server SIGKILLed during the pass (found running after {during} s)")
        srv.start(wait=False)
    t0 = time.monotonic()
    recovered = {"after the restart": wait_until(lambda: ready(srv.base) and ledger.acked_since(t0) > 0, 180)}

    def purged() -> bool:
        st, body = get(srv.base, "/api/health")
        last = json.loads(body)["config"]["retention"]["last_pass"]
        return bool(last) and not last.get("stopped") and not (set(read()) & ledger.expired)
    recovered["expired spans deleted"] = wait_until(purged, 240, step=2)
    time.sleep(c.a.phase_s)
    ing.stop()
    r = verdict(ledger, read(), srv.base, c.a.sample, recovered=recovered, expect_expired_gone=True)
    r["notes"] = [f"pass seen running {during} s after start" if during is not None else
                  "the pass was not seen running: the fault may have missed it"]
    r["ok"] = r["ok"] and during is not None
    return r


def case_replicas(c: Case, backend: str) -> dict:
    """Two replicas on one Postgres behind a round-robin balancer that skips a dead one; SIGKILL one."""
    from tcp_proxy import Proxy
    pg = c.pg or c.postgres()
    url = pg.create_db("faults_replicas")
    a, read = c.server("postgres", name="replica-a", store_url=url)
    b, _ = c.server("postgres", name="replica-b", store_url=url)
    a.start()
    b.start()
    lb = Proxy([("127.0.0.1", a.port), ("127.0.0.1", b.port)])
    c.cleanup.append(lb.cut)
    base = f"http://127.0.0.1:{lb.port}"
    ledger = Ledger()
    ing = Ingest(lambda: base, ledger, steady(c.a), give_up_s=c.a.give_up_s).start()
    time.sleep(c.a.phase_s)
    a.kill()
    log(f"  replica A SIGKILLed; {len(ledger.acked)} spans acknowledged so far")
    t0 = time.monotonic()
    recovered = {"via replica B": wait_until(lambda: ledger.acked_since(t0) > 0, 60)}
    time.sleep(c.a.phase_s)
    a.start(wait=False)
    recovered["replica A back"] = wait_until(lambda: ready(a.base), 120)
    time.sleep(c.a.phase_s)
    ing.stop()
    return verdict(ledger, read(), base, c.a.sample, recovered=recovered)


def case_duplicate(c: Case, backend: str) -> dict:
    """Every acknowledged export is sent again, as an exporter does when the 200 is lost on the way."""
    srv, read = c.server(backend)
    srv.start()
    ledger = Ledger()
    ing = Ingest(lambda: srv.base, ledger, dataclasses.replace(steady(c.a), twice=True),
                 give_up_s=c.a.give_up_s).start()
    time.sleep(c.a.phase_s * 2)
    ing.stop()
    r = verdict(ledger, read(), srv.base, c.a.sample, recovered={})
    st, body = get(srv.base, "/api/health")
    dup = json.loads(body)["ingest"]["duplicates"]
    r["notes"] = [f"/api/health counted {dup} duplicates for {len(ledger.acked)} spans sent twice"]
    r["ok"] = r["ok"] and dup == len(ledger.acked)
    return r


def case_disorder(c: Case, backend: str) -> dict:
    """Spans shuffled over 40 batches (settles before receives before sends, far apart) from services whose
    clocks are skewed by up to an hour either way (some spans from the future)."""
    srv, read = c.server(backend)
    srv.start()
    ledger = Ledger()
    t = dataclasses.replace(steady(c.a), disorder=40,
                            skew_ns=(0, 90 * NS, -300 * NS, 3600 * NS, -3600 * NS, 2 * NS, 0, -45 * NS))
    ing = Ingest(lambda: srv.base, ledger, t, give_up_s=c.a.give_up_s).start()
    time.sleep(c.a.phase_s * 2)
    ing.stop()
    return verdict(ledger, read(), srv.base, c.a.sample, recovered={})


def build_image() -> str:
    tag = f"{PREFIX}-trailbeam:faults"
    log(f"  building {tag} from this checkout")
    subprocess.run(["docker", "build", "-q", "-t", tag, str(ROOT)], check=True, capture_output=True)
    return tag


def case_disk_full(c: Case, backend: str) -> dict:
    """The store's disk fills up under steady ingest, stays full for ``--outage-s``, and is freed again. SQLite:
    the server runs in a container of this checkout's image, its /data on a small disk; Postgres: its data
    directory on one. A ballast file takes the free space and is deleted to free it."""
    ledger = Ledger()
    notes = []
    if backend == "sqlite":
        tag = build_image()
        if not c.a.keep:
            c.cleanup.append(lambda: docker("rmi", "-f", tag, check=False))
        name = f"{PREFIX}-tb-{os.getpid()}"
        port = free_port()
        if not c.a.keep:
            c.cleanup.append(lambda: docker("rm", "-f", "-v", name, check=False))
        docker("run", "-d", "--name", name, "-p", f"127.0.0.1:{port}:4318", *small_disk("/data", c.a.disk_mb),
               tag, "--retention", "forever", "--log-level", "INFO")
        base = f"http://127.0.0.1:{port}"
        if wait_until(lambda: ready(base), 60) is None:
            raise RuntimeError(docker("logs", name, check=False))

        def run(*cmd):
            return docker("exec", name, *cmd)
        ballast = "/data/ballast"

        def read() -> Counter:
            run("python", "-c", "import sqlite3; s = sqlite3.connect('/data/trailbeam.sqlite'); "
                "d = sqlite3.connect('/tmp/copy.sqlite'); s.backup(d); d.close()")
            out = str(c.workdir / "disk-full-copy.sqlite")
            docker("cp", f"{name}:/tmp/copy.sqlite", out)
            run("rm", "-f", "/tmp/copy.sqlite")
            return stored_sqlite(out)
        alive = lambda: docker("inspect", "-f", "{{.State.Running}} {{.RestartCount}}", name)  # noqa: E731
    else:
        pg = c.postgres(disk_mb=c.a.disk_mb)
        url = pg.create_db("faults_disk")
        srv, read = c.server("postgres", name="disk", store_url=url)
        srv.start()
        base = srv.base

        def run(*cmd):
            return pg.exec(*cmd)
        ballast = "/var/lib/postgresql/data/ballast"
        alive = lambda: f"server alive {srv.alive()}, starts {srv.starts}"  # noqa: E731

    def free_kb() -> int:
        return int(run("df", "-k", "--output=avail", ballast.rsplit("/", 1)[0]).split()[-1])

    ing = Ingest(lambda: base, ledger, steady(c.a), give_up_s=c.a.give_up_s).start()
    time.sleep(c.a.phase_s)
    # leave ~2 MB, so the next exports fill it
    for margin_kb in (2048, 8192, 32768):  # the store grows meanwhile: leave a little more if it beat us to it
        if free_kb() <= margin_kb:  # the store has (nearly) filled it by itself
            break
        try:
            run("fallocate", "-l", f"{free_kb() - margin_kb}k", ballast)
            break
        except RuntimeError:
            run("rm", "-f", ballast)
    else:
        raise RuntimeError("could not fill the disk")
    log(f"  disk filled (free {free_kb()} kB); {len(ledger.acked)} spans acknowledged so far")
    t_full = time.monotonic()
    stalled = wait_until(lambda: time.monotonic() - max(ledger.acked_at[-1:] or [0]) > 3, 60)
    notes.append(f"ingest stopped being acknowledged {stalled:.1f} s after filling" if stalled is not None
                 else "ingest was still acknowledged a minute after filling (the disk did not fill?)")
    time.sleep(c.a.outage_s)
    notes.append(f"while full: {alive()}; dropped so far {dict(ledger.dropped)}")
    run("rm", "-f", ballast)
    t0 = time.monotonic()
    log(f"  disk freed after {t0 - t_full:.0f} s")
    recovered = {"disk freed": wait_until(lambda: ready(base) and ledger.acked_since(t0) > 0, 180)}
    time.sleep(c.a.phase_s)
    ing.stop()
    r = verdict(ledger, read(), base, c.a.sample, recovered=recovered)
    notes.append(f"after: {alive()}")
    r["notes"] = notes
    r["ok"] = r["ok"] and stalled is not None
    return r


CASES = {
    "kill9": (("sqlite", "postgres"), case_kill9),
    "kill9-relaxed": (("sqlite",), lambda c, b: case_kill9(c, b, durability="relaxed")),
    "pg-down": (("postgres",), case_pg_down),
    "disk-full": (("sqlite", "postgres"), case_disk_full),
    "retention-restart": (("sqlite", "postgres"), case_retention_restart),
    "replicas": (("postgres",), case_replicas),
    "duplicate": (("sqlite", "postgres"), case_duplicate),
    "disorder": (("sqlite", "postgres"), case_disorder),
}


def table(results: list) -> str:
    rows = ["| Case | Store | Result | Sent | Acked | Lost | Doubled | Unacked, stored | Dropped | Shown = reference "
            "| Recovered (s) |", "|---|---|---|---:|---:|---:|---:|---:|---|---|---|"]
    for r in results:
        if "error" in r:
            rows.append(f"| {r['case']} | {r['backend']} | error: {r['error']} |" + " |" * 8)
            continue
        rec = ", ".join(f"{k}: {'never' if v is None else f'{v:.1f}'}" for k, v in r["recovered_s"].items()) or "n/a"
        rows.append(f"| {r['case']} | {r['backend']} | {'pass' if r['ok'] else 'FAIL'} | {r['spans_sent']} | "
                    f"{r['spans_acked']} | {r['lost']} | {r['doubled']} | {r['unacked_stored']} | "
                    f"{', '.join(f'{k}: {v}' for k, v in r['dropped'].items()) or '0'} | "
                    f"{'yes' if not r['shown_problems'] else 'no'} | {rec} |")
    return "\n".join(rows)


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0], formatter_class=argparse.RawTextHelpFormatter)
    p.add_argument("--case", action="append", choices=sorted(CASES), help="a case to run (repeatable); default all")
    p.add_argument("--backend", action="append", choices=["sqlite", "postgres"], help="default both")
    p.add_argument("--list", action="store_true")
    p.add_argument("--workers", type=int, default=4, help="exporter threads (default 4)")
    p.add_argument("--batch-spans", type=int, default=64)
    p.add_argument("--interval-s", type=float, default=0.05, help="each worker's pause between exports")
    p.add_argument("--phase-s", type=float, default=8.0, help="seconds of ingest before and after each fault")
    p.add_argument("--outage-s", type=float, default=60.0, help="how long Postgres stays down, or the disk full")
    p.add_argument("--kills", type=int, default=3)
    p.add_argument("--disk-mb", type=int, default=512, help="size of the disk that fills up")
    p.add_argument("--give-up-s", type=float, default=300, help="an exporter's retry budget (Collector: 300 s)")
    p.add_argument("--sample", type=int, default=200, help="timelines compared with the reference per case")
    p.add_argument("--out", default=None, help="directory for faults.json and faults.md")
    p.add_argument("--keep", action="store_true", help="keep containers, volumes and files for a look")
    p.add_argument("--postgres-url", default=os.environ.get("TRAILBEAM_TEST_POSTGRES_URL"),
                   help="use this Postgres server (a database per case) instead of a container; the cases that "
                        f"stop or fill Postgres ({', '.join(sorted(NEEDS_OWN_POSTGRES))}) still need Docker. "
                        "Default: $TRAILBEAM_TEST_POSTGRES_URL")
    a = p.parse_args(argv)
    if a.list:
        for name, (backends, f) in CASES.items():
            print(f"{name:18} {'/'.join(backends):16} {f.__doc__.split(chr(10))[0] if f.__doc__ else ''}")
        return 0
    todo = [(n, b) for n in (a.case or CASES) for b in CASES[n][0] if not a.backend or b in a.backend]
    results = []
    for name, backend in todo:
        workdir = Path(tempfile.mkdtemp(prefix=f"{PREFIX}-{name}-{backend}-"))
        log(f"{name} on {backend} (work dir {workdir})")
        c = Case(a, workdir)
        t0 = time.monotonic()
        try:
            if backend == "postgres" and name not in ("disk-full", "replicas"):
                if name in NEEDS_OWN_POSTGRES:
                    c.pg = Postgres(workdir)
                    if not a.keep:
                        c.cleanup.append(c.pg.remove)
                else:
                    c.postgres()
            r = CASES[name][1](c, backend)
        except Exception as e:  # noqa: BLE001
            import traceback
            traceback.print_exc()
            r = {"ok": False, "error": f"{type(e).__name__}: {e}"}
        finally:
            r_errors = [line for s in c.servers for line in s.errors()][:10]
            c.close()
            if not a.keep:
                shutil.rmtree(workdir, ignore_errors=True)
        r.update(case=name, backend=backend, seconds=round(time.monotonic() - t0, 1), server_errors=r_errors)
        log(f"  -> {'pass' if r.get('ok') else 'FAIL'}: " + json.dumps({k: v for k, v in r.items() if k not in
                                                                       ("case", "backend")}, default=str))
        results.append(r)
    md = table(results)
    print(md)
    if a.out:
        out = Path(a.out)
        out.mkdir(parents=True, exist_ok=True)
        (out / "faults.json").write_text(json.dumps(results, indent=2, default=str))
        (out / "faults.md").write_text(md + "\n")
    return 0 if all(r.get("ok") for r in results) else 1


if __name__ == "__main__":
    sys.exit(main())
