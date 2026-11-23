"""Backup and restore check: a backup taken while the server ingests restores onto a new machine (QV-239).

    # SQLite, trailbeam start's data directory (the token, the receipt-hash key and the file):
    python smoke/backup_restore_check.py --store sqlite
    # SQLite in a Docker volume, as npx trailbeam start keeps it, with the image's own tools:
    python smoke/backup_restore_check.py --store sqlite --image tb:ci
    # Postgres: pg_dump while ingesting, pg_restore into a new database owned by the same role:
    python smoke/backup_restore_check.py --store postgres --pg-url postgresql://postgres:postgres@localhost:5432/postgres

Each step runs the commands the README's "Backup and restore" sections give (and the docs site's "Back up and
restore" page), not a library call:

1. A server starts on an empty store with a new access token and receipt-hash key. SQLite: ``trailbeam start``
   makes both in its data directory. Postgres: the token is a file and the key is the operator's, kept outside
   the database, as with the team install's ``.env``.
2. One thread sends export batches back to back (1,000s of spans, every receipt handle hashed with the key, as
   the packages do) and another reads the API, while the backup is taken: SQLite's backup API (``python -c
   'import sqlite3 ... s.backup(d)'``), or ``pg_dump -Fc``. The token and key files are copied with it.
   Ingest goes on after the backup until the last batch, then the server stops.
3. "A new machine": a new data directory (SQLite), Docker volume (image), or database made with
   ``createdb -O <role>``, restored with ``pg_restore --no-owner --role`` and ``ANALYZE`` (Postgres). SQLite's
   backup must pass ``PRAGMA integrity_check``. The server starts there with the restored token (SQLite:
   ``trailbeam start`` must keep the restored token and key, not make new ones).
4. The backup is one consistent moment: it holds exactly the first k export batches, for a k between the
   batches answered when the backup started and those sent by the time it ended (each export is one
   transaction, and the thread sends one at a time). Everything the restored server serves must equal a server
   given those k batches from empty. The token, a browser session cookie from before the backup, and a wrong
   token's 401 are checked.
5. The services on the new machine take the key from the restored file and send the remaining batches.
   Deletes whose receive is in the backup must join it: everything must equal a server given every batch.
   A control server, given the remaining batches under a new key (what a lost key file leads to), must show
   ``key_mismatch`` notices, so the check would notice a key that wasn't restored.

Exit status 0 when every check passes; each failure is printed. Removes the processes, containers, volumes,
databases and role it made, and ``--workdir`` (unless ``--keep``).
"""
from __future__ import annotations

import argparse
import dataclasses
import os
import re
import secrets
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import traceback
import urllib.parse
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import upgrade_check as uc  # noqa: E402: the HTTP, snapshot and comparison helpers of the upgrade check
from upgrade_check import loadgen  # noqa: E402

from trailbeam._contract import ReceiptHasher  # noqa: E402

N_MESSAGES = 4000
BACKUP_AT = 0.3  # the share of batches answered before the backup starts
HMAC = "trailbeam.receipt_handle.hmac"
KEY_ID = "trailbeam.receipt_handle.key_id"
# The upgrade check's traffic (the snapshot reads its body queues), in smaller exports: more of them overlap the backup.
PROFILE = dataclasses.replace(uc.PROFILE, batch_spans=50)

# The README's commands, verbatim but for the paths.
SQLITE_BACKUP = ("import sqlite3, sys; s = sqlite3.connect(sys.argv[1]); d = sqlite3.connect(sys.argv[2]); "
                 "s.backup(d); d.close(); s.close()")
SQLITE_CHECK = 'import sqlite3, sys; print(sqlite3.connect(sys.argv[1]).execute("PRAGMA integrity_check").fetchone()[0])'

fail, ok = uc.fail, uc.ok


def sh(argv, **kw) -> subprocess.CompletedProcess:
    r = subprocess.run(argv, capture_output=True, text=True, **kw)
    if r.returncode != 0:
        raise AssertionError(f"{' '.join(map(str, argv))[:200]}: exit {r.returncode}: {(r.stderr or r.stdout)[-800:]}")
    return r


# ---- the spans: loadgen's, with each receipt handle hashed with a real key ------------------------------


def make_messages(now_ns: int) -> list:
    start = now_ns - 60 * uc.H
    step = (58 * uc.H) // N_MESSAGES
    n = iter(range(10 ** 9))
    stream = loadgen.Stream(PROFILE, seed=239, clock=lambda: start + next(n) * step)
    return list(stream.batches_of_messages(N_MESSAGES))


def encode(batch: list, key: str) -> bytes:
    """loadgen's random ``hmac`` is taken as the receipt handle SQS issued; it is hashed as the packages do."""
    h = ReceiptHasher(key)
    out = []
    for s in batch:
        if HMAC in s.attrs:
            s = dataclasses.replace(s, attrs={**s.attrs, HMAC: h(s.attrs[HMAC]), KEY_ID: h.key_id})
        out.append(s)
    return loadgen.encode(out)


def handles(batches: list) -> set:
    return {s.attrs[HMAC] for b in batches for s in b if HMAC in s.attrs}


def key_mismatches(snap: dict) -> int:
    return sum(any(n.get("code") == "key_mismatch" for n in t.get("notices", [])) for t in snap["timelines"].values())


# ---- servers --------------------------------------------------------------------------------------------


class Server:
    """``trailbeam`` (or ``trailbeam start``) as a process, or ``start`` in the image on a Docker volume."""

    def __init__(self, label: str, workdir: Path, *, args=(), cmd=None, image=None, volume=None):
        self.label, self.workdir, self.args = label, workdir, list(args)
        self.cmd, self.image, self.volume = cmd, image, volume
        self.port = uc.free_port()
        self.proc = self.container = None
        self.out = workdir / f"{label}.stdout"
        self.log = workdir / f"{label}.log"

    def run(self):
        extra = ["--stuck-after", str(uc.STUCK_AFTER_S), "--store-bodies", ",".join(loadgen.body_queue_names(uc.PROFILE))]
        if self.image:
            self.container = f"tb-backup-{self.label}-{secrets.token_hex(3)}"
            sh(["docker", "run", "-d", "--init", "--name", self.container, "-p", f"127.0.0.1:{self.port}:4318",
                "-v", f"{self.volume}:/data", self.image, "start", "--data-dir", "/data", *extra, *self.args])
        else:
            env = {k: v for k, v in os.environ.items() if not k.startswith("TRAILBEAM_")}
            self.proc = subprocess.Popen([*self.cmd, *self.args, "--host", "127.0.0.1", "--port", str(self.port), *extra],
                                         stdout=open(self.out, "wb"), stderr=open(self.log, "wb"), env=env)
        uc.wait_ready(self.port, self.alive)
        return self

    def alive(self) -> bool:
        if self.proc:
            return self.proc.poll() is None
        r = subprocess.run(["docker", "inspect", "-f", "{{.State.Running}}", self.container], capture_output=True, text=True)
        return r.stdout.strip() == "true"

    def banner(self) -> str:
        if self.container:
            return subprocess.run(["docker", "logs", self.container], capture_output=True, text=True).stdout
        return self.out.read_text(errors="replace")

    def logs(self) -> str:
        if self.container:
            r = subprocess.run(["docker", "logs", self.container], capture_output=True, text=True)
            return r.stdout + r.stderr
        return self.log.read_text(errors="replace") if self.log.exists() else ""

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
            self.log.write_text(self.logs())
            subprocess.run(["docker", "rm", "-f", self.container], capture_output=True)
            self.container = None


# ---- load while the backup runs -------------------------------------------------------------------------


class Load:
    """Sends ``bodies`` one export at a time, back to back, and reads the API meanwhile."""

    def __init__(self, port: int, token: str, bodies: list):
        self.port, self.token, self.bodies = port, token, bodies
        self.acked = 0    # batches answered 200: the first ``acked`` are stored
        self.started = 0  # batches whose POST has begun
        self.reads = 0
        self.error = None
        self.done = threading.Event()

    def _send(self):
        try:
            for i, body in enumerate(self.bodies):
                self.started = i + 1
                uc.ingest(self.port, self.token, [body])
                self.acked = i + 1
        except Exception as e:  # noqa: BLE001: reported by the check
            self.error = e
        finally:
            self.done.set()

    def _read(self):
        while not self.done.is_set():
            for path in ("/api/messages?limit=50", "/api/stats", "/api/stuck?limit=50"):
                try:
                    if uc.request(self.port, "GET", path, token=self.token, timeout=30)[0] == 200:
                        self.reads += 1
                except OSError:
                    pass

    def start(self):
        self.threads = [threading.Thread(target=self._send, daemon=True), threading.Thread(target=self._read, daemon=True)]
        for t in self.threads:
            t.start()
        return self

    def wait_acked(self, n: int, timeout_s=300):
        deadline = time.monotonic() + timeout_s
        while self.acked < n and not self.done.is_set():
            if time.monotonic() > deadline:
                raise AssertionError(f"only {self.acked} of {n} batches answered in {timeout_s} s")
            time.sleep(0.02)

    def join(self):
        for t in self.threads:
            t.join(600)
        if self.error:
            raise AssertionError(f"ingest under load: {self.error}")


# ---- the three ways ---------------------------------------------------------------------------------------


class SqliteCommands:
    """trailbeam start, data directory on this machine (pip install)."""
    name = "SQLite, trailbeam start's data directory"

    def __init__(self, a, workdir: Path):
        self.a, self.w = a, workdir
        self.cmd = a.cmd.split()

    def source(self) -> Server:
        self.src_dir = self.w / "old-machine" / ".trailbeam"
        self.src_dir.parent.mkdir(mode=0o700)
        return Server("source", self.w, cmd=self.cmd, args=["start", "--data-dir", str(self.src_dir)]).run()

    def secrets(self) -> tuple:
        return ((self.src_dir / "token").read_text().strip(), (self.src_dir / "receipt-hash-key").read_text().strip())

    def backup(self, server: Server) -> None:
        self.bk = self.w / "backups"
        self.bk.mkdir(mode=0o700)
        sh(["python3", "-c", SQLITE_BACKUP, str(self.src_dir / "trailbeam.sqlite"), str(self.bk / "trailbeam.sqlite")])
        sh(["cp", "-p", str(self.src_dir / "token"), str(self.src_dir / "receipt-hash-key"), str(self.bk)])

    def restore(self) -> Server:
        r = sh(["python3", "-c", SQLITE_CHECK, str(self.bk / "trailbeam.sqlite")])
        (ok if r.stdout.strip() == "ok" else fail)(f"PRAGMA integrity_check on the backup: {r.stdout.strip()}")
        self.new_dir = self.w / "new-machine" / ".trailbeam"
        self.new_dir.mkdir(parents=True, mode=0o700)
        sh(["install", "-m", "600", *(str(self.bk / f) for f in ("trailbeam.sqlite", "token", "receipt-hash-key")),
            str(self.new_dir)])
        return Server("restored", self.w, cmd=self.cmd, args=["start", "--data-dir", str(self.new_dir)]).run()

    def restored_key(self) -> str:
        return (self.new_dir / "receipt-hash-key").read_text().strip()

    def reference(self, label: str, token_file: Path) -> Server:
        d = self.w / label
        d.mkdir(mode=0o700)
        return Server(label, self.w, cmd=self.cmd, args=["--db", str(d / "trailbeam.sqlite"), "--token-file", str(token_file)]).run()

    def cleanup(self) -> None:
        pass


class SqliteImage(SqliteCommands):
    """start in the image, data in a Docker volume (npx trailbeam start), with docker exec / cp / run."""
    name = "SQLite in a Docker volume (the image, as npx trailbeam start runs it)"

    def __init__(self, a, workdir: Path):
        super().__init__(a, workdir)
        self.cmd = [sys.executable, "-m", "trailbeam.server"]  # the reference servers
        self.volumes: list = []

    def _volume(self, what: str) -> str:
        v = f"tb-backup-{what}-{secrets.token_hex(3)}"
        sh(["docker", "volume", "create", v])
        self.volumes.append(v)
        return v

    def source(self) -> Server:
        self.src_vol = self._volume("old")
        self.src = Server("source", self.w, image=self.a.image, volume=self.src_vol).run()
        return self.src

    def secrets(self) -> tuple:
        get = lambda f: sh(["docker", "exec", self.src.container, "cat", f"/data/{f}"]).stdout.strip()  # noqa: E731
        return get("token"), get("receipt-hash-key")

    def backup(self, server: Server) -> None:
        self.bk = self.w / "backups"
        self.bk.mkdir(mode=0o700)
        c = server.container
        sh(["docker", "exec", c, "python", "-c", SQLITE_BACKUP, "/data/trailbeam.sqlite", "/data/backup.sqlite"])
        for f, to in (("backup.sqlite", "trailbeam.sqlite"), ("token", "token"), ("receipt-hash-key", "receipt-hash-key")):
            sh(["docker", "cp", f"{c}:/data/{f}", str(self.bk / to)])
        sh(["docker", "exec", c, "rm", "/data/backup.sqlite"])

    def restore(self) -> Server:
        r = sh(["python3", "-c", SQLITE_CHECK, str(self.bk / "trailbeam.sqlite")])
        (ok if r.stdout.strip() == "ok" else fail)(f"PRAGMA integrity_check on the backup: {r.stdout.strip()}")
        self.new_vol = self._volume("new")
        sh(["docker", "run", "--rm", "--user", "root", "-v", f"{self.new_vol}:/data", "-v", f"{self.bk}:/restore:ro",
            "--entrypoint", "sh", self.a.image, "-c",
            "rm -f /data/trailbeam.sqlite-wal /data/trailbeam.sqlite-shm"
            " && install -m 600 -o trailbeam -g trailbeam /restore/trailbeam.sqlite /restore/token"
            " /restore/receipt-hash-key /data/"])
        self.new = Server("restored", self.w, image=self.a.image, volume=self.new_vol).run()
        return self.new

    def restored_key(self) -> str:
        return sh(["docker", "exec", self.new.container, "cat", "/data/receipt-hash-key"]).stdout.strip()

    def cleanup(self) -> None:
        for v in self.volumes:
            subprocess.run(["docker", "volume", "rm", "-f", v], capture_output=True)


class Postgres:
    """trailbeam --store-url, as one role that owns its database; pg_dump, createdb, pg_restore, ANALYZE."""
    name = "Postgres, pg_dump and pg_restore"

    def __init__(self, a, workdir: Path):
        self.a, self.w = a, workdir
        self.cmd = a.cmd.split()
        self.bin = (lambda t: str(Path(a.pg_bin) / t)) if a.pg_bin else (lambda t: t)
        self.role = f"tb_backup_{secrets.token_hex(3)}"
        self.password = secrets.token_hex(16)
        self.dbs: list = []
        self.made_role = False
        self.token_file = workdir / "secrets" / "token"
        self.key_file = workdir / "secrets" / "receipt-hash-key"

    def url(self, db: str, as_role=True) -> str:
        u = urllib.parse.urlsplit(self.a.pg_url)
        if as_role:
            netloc = f"{self.role}:{self.password}@{u.hostname}:{u.port or 5432}"
            u = u._replace(netloc=netloc)
        return urllib.parse.urlunsplit(u._replace(path="/" + db))

    def admin(self, sql: str) -> None:
        import psycopg
        with psycopg.connect(self.a.pg_url, autocommit=True) as c:
            c.execute(sql)

    def source(self) -> Server:
        import psycopg
        with psycopg.connect(self.a.pg_url) as c:
            server = c.info.server_version // 10000
        tools = sh([self.bin("pg_restore"), "--version"]).stdout.strip()
        print(f"   Postgres server {server}; {tools}", flush=True)
        client = int(re.search(r"(\d+)", tools.split(")", 1)[-1]).group(1))
        # A newer pg_restore sends settings an older server rejects (18 into 16: transaction_timeout); the README
        # says to use the server's major version, so a mismatch is the setup's fault, not the backup's.
        assert client <= server, (f"pg_restore {client} is newer than the Postgres {server} server; pass --pg-bin "
                                  f"with the {server} client tools (e.g. /usr/lib/postgresql/{server}/bin)")
        self.admin(f"CREATE ROLE {self.role} LOGIN PASSWORD '{self.password}'")
        self.made_role = True
        self.src_db = self._db("old", owner=self.role)
        self.token_file.parent.mkdir(mode=0o700)
        for f in (self.token_file, self.key_file):
            f.write_text(secrets.token_hex(32) + "\n")
            f.chmod(0o600)
        return Server("source", self.w, cmd=self.cmd,
                      args=["--store-url", self.url(self.src_db), "--token-file", str(self.token_file)]).run()

    def _db(self, what: str, owner=None) -> str:
        db = f"tb_backup_{what}_{secrets.token_hex(3)}"
        self.admin(f'CREATE DATABASE "{db}"' + (f" OWNER {owner}" if owner else ""))
        self.dbs.append(db)
        return db

    def secrets(self) -> tuple:
        return self.token_file.read_text().strip(), self.key_file.read_text().strip()

    def backup(self, server: Server) -> None:
        self.bk = self.w / "backups"
        self.bk.mkdir(mode=0o700)
        sh([self.bin("pg_dump"), "-Fc", "-f", str(self.bk / "trailbeam.dump"), self.url(self.src_db)])
        sh(["cp", "-p", str(self.token_file), str(self.key_file), str(self.bk)])

    def restore(self) -> Server:
        self.new_db = f"tb_backup_new_{secrets.token_hex(3)}"
        sh([self.bin("createdb"), f"--maintenance-db={self.a.pg_url}", "-O", self.role, self.new_db])
        self.dbs.append(self.new_db)
        sh([self.bin("pg_restore"), "--no-owner", f"--role={self.role}", "-d", self.url(self.new_db),
            str(self.bk / "trailbeam.dump")])
        sh([self.bin("psql"), self.url(self.new_db), "-c", "ANALYZE"])
        self.new_secrets = self.w / "new-machine"
        self.new_secrets.mkdir(mode=0o700)
        sh(["install", "-m", "600", str(self.bk / "token"), str(self.bk / "receipt-hash-key"), str(self.new_secrets)])
        return Server("restored", self.w, cmd=self.cmd,
                      args=["--store-url", self.url(self.new_db), "--token-file", str(self.new_secrets / "token")]).run()

    def restored_key(self) -> str:
        return (self.new_secrets / "receipt-hash-key").read_text().strip()

    def reference(self, label: str, token_file: Path) -> Server:
        return Server(label, self.w, cmd=self.cmd,
                      args=["--store-url", self.url(self._db(label), as_role=False), "--token-file", str(token_file)]).run()

    def cleanup(self) -> None:
        for db in self.dbs:
            subprocess.run([self.bin("dropdb"), f"--maintenance-db={self.a.pg_url}", "--if-exists", "--force", db],
                           capture_output=True)
        if self.made_role:
            try:
                self.admin(f"DROP ROLE IF EXISTS {self.role}")
            except Exception as e:  # noqa: BLE001
                print(f"could not drop role {self.role}: {e}")


# ---- the check ----------------------------------------------------------------------------------------------


def run(a) -> int:
    workdir = Path(a.workdir or tempfile.mkdtemp(prefix="tb-backup-"))
    workdir.mkdir(parents=True, exist_ok=True)
    way = Postgres(a, workdir) if a.store == "postgres" else SqliteImage(a, workdir) if a.image else SqliteCommands(a, workdir)
    print(way.name, flush=True)
    running: list = []

    def started(s: Server) -> Server:
        running.append(s)
        return s

    now_ns = time.time_ns()
    window = ((now_ns - 72 * uc.H) // 1_000_000, now_ns // 1_000_000)
    batches = make_messages(now_ns)
    cum = [0]
    for b in batches:
        cum.append(cum[-1] + len(b))
    print(f"{len(batches)} export batches, {cum[-1]} spans, {N_MESSAGES} messages", flush=True)
    try:
        # 1. The server, its token and key.
        src = way.source()
        running.append(src)
        token, key = way.secrets()
        token_file = workdir / "token-for-references"
        token_file.write_text(token + "\n")
        token_file.chmod(0o600)
        cookie = uc.login_cookie(src.port, token)

        # 2. The backup, under load.
        load = Load(src.port, token, [encode(b, key) for b in batches]).start()
        load.wait_acked(int(len(batches) * BACKUP_AT))
        lo, reads0, t0 = load.acked, load.reads, time.monotonic()
        way.backup(src)
        took, hi, during_reads = time.monotonic() - t0, load.started, load.reads - reads0
        load.join()
        print(f"   backup took {took:.2f} s, with exports {lo + 1} to {hi} sent meanwhile and {during_reads} API reads; "
              f"ingest went on to batch {load.acked}", flush=True)
        (ok if hi > lo and load.acked > hi else fail)(
            f"the backup ran while the server ingested (exports in flight during it: {hi - lo}, after it: {load.acked - hi})")
        src.stop()

        # 3. The restore, on a new machine.
        new = started(way.restore())
        (ok if uc.api(new.port, "/api/ready", token) is not None else fail)("the restored server is ready with the old token")
        status = uc.request(new.port, "GET", "/api/stats", cookie=cookie)[0]
        (ok if status == 200 else fail)(f"a browser session from before the backup still works ({status})")
        status = uc.request(new.port, "GET", "/api/stats", token="wrong" + token[5:])[0]
        (ok if status == 401 else fail)(f"a wrong token is refused ({status})")
        if a.store == "sqlite":
            banner = new.banner()
            (ok if token not in banner and key not in banner else fail)(
                "trailbeam start kept the restored token and key (it prints new ones only when it makes them)")
        (ok if way.restored_key() == key else fail)("the restored receipt-hash key is the one the services used")

        # 4. One consistent moment.
        spans = sum(uc.api(new.port, "/api/stats", token)["data"]["spans"].values())
        k = cum.index(spans) if spans in cum else None
        if k is None or not lo <= k <= hi:
            raise AssertionError(f"the restored store has {spans} spans: not the first k batches for any k in "
                                 f"[{lo}, {hi}] (batch boundaries there: {cum[lo:hi + 1]})")
        ok(f"the backup holds exactly the first {k} export batches ({spans} spans), "
           f"between the {lo} answered when it started and the {hi} sent by its end")
        ref = started(way.reference("ref", token_file))
        uc.ingest(ref.port, token, [encode(b, key) for b in batches[:k]])
        restored = uc.snapshot(new.port, token, window)
        uc.same("the restored server = a server given those batches from empty", uc.snapshot(ref.port, token, window),
                restored, exact=True)

        # 5. The services carry on with the restored key.
        crossing = len(handles(batches[:k]) & handles(batches[k:]))
        new_key = way.restored_key()
        uc.ingest(new.port, token, [encode(b, new_key) for b in batches[k:]])
        uc.ingest(ref.port, token, [encode(b, key) for b in batches[k:]])
        final = uc.snapshot(new.port, token, window)
        uc.same(f"after the remaining batches = a server given every batch ({crossing} receipt handles received "
                f"before the backup and settled after the restore joined)", uc.snapshot(ref.port, token, window),
                final, exact=True)
        (ok if crossing > 0 and key_mismatches(final) == 0 else fail)(
            f"no key_mismatch notice on the restored server ({key_mismatches(final)} timelines with one)")
        ref.stop()

        lost = started(way.reference("lost_key", token_file))
        uc.ingest(lost.port, token, [encode(b, key) for b in batches[:k]] +
                  [encode(b, secrets.token_hex(32)) for b in batches[k:]])
        n = key_mismatches(uc.snapshot(lost.port, token, window))
        (ok if n > 0 else fail)(f"control: with a new key after the restore, {n} timelines show key_mismatch")
        lost.stop()
        new.stop()
    except Exception as e:  # noqa: BLE001: report it as a failure, with the servers' last log lines
        fail(f"{type(e).__name__}: {e}")
        traceback.print_exc()
        for s in running:
            print(f"--- {s.label} log (last lines)\n{s.logs()[-2000:]}")
    finally:
        for s in running:
            s.stop()
        way.cleanup()
        if not a.keep:
            shutil.rmtree(workdir, ignore_errors=True)
    if uc.failures:
        print(f"\n{len(uc.failures)} check(s) failed")
        return 1
    print("\nbackup and restore check passed")
    return 0


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--store", choices=("sqlite", "postgres"), required=True)
    p.add_argument("--cmd", default=f"{sys.executable} -m trailbeam.server",
                   help="the trailbeam command (default: this interpreter's trailbeam.server)")
    p.add_argument("--image", help="SQLite only: run the server from this image, data in Docker volumes")
    p.add_argument("--pg-url", help="a Postgres the check may create and drop a role and databases on")
    p.add_argument("--pg-bin", help="the directory of pg_dump, pg_restore, createdb, dropdb and psql (default: PATH)")
    p.add_argument("--workdir")
    p.add_argument("--keep", action="store_true", help="keep --workdir (the logs and backups)")
    a = p.parse_args(argv)
    if a.store == "postgres" and not a.pg_url:
        p.error("--store postgres needs --pg-url")
    if a.store == "postgres" and a.image:
        p.error("the image has SQLite only")
    return run(a)


if __name__ == "__main__":
    sys.exit(main())
