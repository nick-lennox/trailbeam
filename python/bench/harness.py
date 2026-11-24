"""Trailbeam benchmark harness: seed a database, run ingest at increasing rates, time the read pages.

    python bench/harness.py --out bench-results --sizes 100k,1M,10M

For each database size, in order (each size grows the same file from the previous one):

1. **Seed** the SQLite file to the size with the load generator's message stories (``loadgen.py``),
   spread over the last ``--seed-days`` days. Seeding writes the rows ``Database.insert`` would write
   (same schema, body gate and receipt-hash join) but does the join in memory, because the server's own
   insert gets slower as the table grows (see bench/README.md) and could not reach 1M spans.
2. **Pages, idle.** Start the server (``python -m trailbeam.server``, today's code, unchanged) and time
   each read page: Messages, Messages for one queue, message detail, Duplicates, Stuck, Tenants, tenant
   search and body search.
3. **Ingest ramp.** Send live traffic (``loadgen.run``) at each ``--rates`` step, in messages/s, for
   ``--step-seconds``; record achieved spans/s, export latency, errors and the server's CPU. The ramp
   stops after the first step that is not steady (errors, less than 95% of the target, or export p99
   above ``--max-export-p99-ms``).
4. **Pages under ingest.** Time the pages again while ingest runs at the highest steady rate (or the
   lowest rate if none was steady), and record that ingest's export latency.
5. **Breakdown.** With the server stopped, time where one export's cost goes in-process: OTLP decode,
   and ``Database.insert`` with the share spent in the two join queries.

Then, once: database file growth per million spans with and without bodies (small separate files), and
the OTel SDK's BatchSpanProcessor drops against the server holding the largest database.

Results: ``results.json`` (everything), ``results.csv`` (one row per measurement) and ``summary.md``.
"""
from __future__ import annotations

import argparse
import csv
import http.client
import json
import os
import platform
import random
import shutil
import socket
import sqlite3
import subprocess
import sys
import threading
import time
from dataclasses import asdict, replace
from pathlib import Path
from typing import Optional
from urllib.parse import quote

sys.path.insert(0, str(Path(__file__).resolve().parent))

import loadgen  # noqa: E402

from trailbeam.server import body_table, summary_table  # noqa: E402
from trailbeam.server.events import HOUR_NS, KINDS as STEP_KINDS  # noqa: E402
from trailbeam.server.db import (_STEP_ADD_SQL, RECEIVE_JOIN_SQL, RECORD_FIELDS, SETTLE_JOIN_SQL,  # noqa: E402
                                 SETTLE_KINDS, Database, gate_body)

DAY_NS = 86_400 * 10 ** 9


# ---- seeding ------------------------------------------------------------------------------

class Seeder:
    """Writes generated spans the way ``Database.insert`` would, with the receipt-hash join in memory.

    Same rows as the server: the columns, the body gate (``gate_body``), each body once in ``bodies``, ``mid``/``mid_source`` and
    ``receive_row`` in both join directions. A settle span joins the receive with its hash seen earlier
    in this seeding run; generated handles are unique, so there is one candidate. Checked against
    ``Database.insert`` in tests/test_bench_harness.py.
    """

    COLS = ["row"] + RECORD_FIELDS + ["attributes", "mid", "mid_source", "body_dropped", "receive_row", "body_id"]
    MID = COLS.index("mid")
    KEEP_RECEIVES = 2_000_000  # receive hashes kept for joins; late settles come within a few batches

    def __init__(self, path: str, store_bodies=()):
        Database(path, store_bodies=store_bodies).close()  # the server's schema and migrations
        self.store_bodies = frozenset(store_bodies)
        self.conn = sqlite3.connect(path, isolation_level=None)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=OFF")  # seeding only; the server opens the file its own way
        self.next_row = (self.conn.execute("SELECT MAX(row) FROM spans").fetchone()[0] or 0) + 1
        self.receives: dict = {}   # (hmac, key_id) -> (row, message_id)
        self.waiting: dict = {}    # (hmac, key_id) -> [rows of settle spans with no receive yet]
        self.sql = f"INSERT OR IGNORE INTO spans ({', '.join(self.COLS)}) VALUES ({', '.join('?' * len(self.COLS))})"

    def write(self, records: list[dict]) -> int:
        rows, updates = [], []
        c = self.conn
        c.execute("BEGIN")
        for r in records:
            own = r.get("message_id")
            attrs, dropped = gate_body(r.get("attributes", {}), r.get("queue"), self.store_bodies)
            attrs, body = body_table.split(r["kind"], own, attrs)  # each body once, as insert stores it
            row = self.next_row
            self.next_row += 1
            mid, src, rrow = own, ("span" if own else None), None
            h = (r.get("receipt_hmac"), r.get("key_id"))
            if r["kind"] in SETTLE_KINDS:
                hit = self.receives.get(h)
                if hit is not None:
                    rrow, mid, src = hit[0], hit[1], "receipt_hash"
                else:
                    self.waiting.setdefault(h, []).append((row, own))
            elif r["kind"] == "receive":
                self.receives[h] = (row, own)
                if len(self.receives) > self.KEEP_RECEIVES:
                    self.receives.pop(next(iter(self.receives)))
                for srow, was in self.waiting.pop(h, ()):
                    updates.append((own, row, srow, was))
            rows.append([row] + [r.get(f) for f in RECORD_FIELDS]
                        + [body_table.attributes_json(attrs), mid, src, dropped, rrow,
                           None if body is None else body_table.put(c, own, body)])
        c.executemany(self.sql, rows)
        steps: dict = {}  # step_rollup, as insert adds to it (generated spans are never duplicates)
        for r in records:
            if r["kind"] in STEP_KINDS:
                k = (r["time_ns"] // HOUR_NS, r["queue"], r.get("service_name") or "", r["kind"])
                steps[k] = steps.get(k, 0) + 1
        c.executemany(_STEP_ADD_SQL, [k + (n,) for k, n in steps.items()])
        c.executemany("UPDATE spans SET mid = ?, mid_source = 'receipt_hash', receive_row = ? WHERE row = ?",
                      [u[:3] for u in updates])
        # the summary tables, as insert keeps them: each row's message, then the settle spans the joins moved
        changes = summary_table.Changes()
        for r in rows:
            changes.add(r[self.MID], r[0])
        for mid, _, srow, was in updates:
            changes.move(srow, was, mid)
        summary_table.refresh(c, changes)
        c.execute("COMMIT")
        return len(rows)

    def close(self) -> None:
        self.conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        self.conn.close()


def restore_file(src: str, dst: str) -> None:
    """Replace the SQLite file ``dst`` (no process may have it open) with a copy of ``src``."""
    for p in (dst + "-wal", dst + "-shm"):
        if os.path.exists(p):
            os.remove(p)
    copy_synced(src, dst)


def copy_synced(src: str, dst: str) -> None:
    """Copy and fsync: otherwise the kernel writes a 1.75 GB copy back during the next measurement, and the
    server's commits wait behind it (seconds-long stalls at 1M before this)."""
    shutil.copyfile(src, dst)
    fd = os.open(dst, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def span_count(path: str) -> int:
    if not os.path.exists(path):
        return 0
    c = sqlite3.connect(path)
    try:
        return c.execute("SELECT MAX(row) FROM spans").fetchone()[0] or 0  # rows are never deleted
    except sqlite3.OperationalError:
        return 0
    finally:
        c.close()


def seed(path: str, target_spans: int, profile: loadgen.Profile, *, store_bodies=(), days: float = 7.0,
         seed_no: int = 7, log=print) -> dict:
    """Grow the file at ``path`` to at least ``target_spans`` spans. Returns spans added and seconds taken."""
    have = span_count(path)
    if have >= target_spans:
        return {"added": 0, "seconds": 0.0, "spans": have}
    spm = profile.mean_spans_per_message()
    n_msgs = int((target_spans - have) / spm * 1.03) + 20  # a margin: the loop stops at the target
    now = time.time_ns()
    start, end = now - int(days * DAY_NS), now - 3_600 * 10 ** 9
    step = (end - start) / n_msgs
    i = [0]

    def clock() -> int:  # messages spread evenly over the window, oldest first
        i[0] += 1
        return start + int(i[0] * step)

    s = Seeder(path, store_bodies)
    stream = loadgen.Stream(profile, seed=seed_no * 1_000_003 + have, worker=900 + have % 97, clock=clock)
    t0, added, last = time.monotonic(), 0, 0
    for batch in stream.batches_of_messages(n_msgs):
        added += s.write(loadgen.records(batch))
        if added - last >= 1_000_000:
            last = added
            log(f"  seeded {have + added:,} spans ({added / (time.monotonic() - t0):,.0f}/s)")
        if have + added >= target_spans and not stream.pending and not stream.held:
            break
    s.close()
    return {"added": added, "seconds": round(time.monotonic() - t0, 1), "spans": have + added}


def file_bytes(path: str) -> int:
    return sum(os.path.getsize(p) for p in (path, path + "-wal") if os.path.exists(p))


# ---- the server ---------------------------------------------------------------------------

def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class Server:
    """``python -m trailbeam.server`` on a file, as a subprocess, with its CPU time from /proc."""

    durability = "full"  # --server-durability: every server the run starts gets it

    def __init__(self, db: str, store_bodies=(), log_path: Optional[str] = None, args=("--retention", "forever")):
        """``args``: more options. By default retention is off: the seeded spans go back ``--seed-days``, and
        the server's default (7d) would delete the oldest during the measurements."""
        self.port = free_port()
        cmd = [sys.executable, "-m", "trailbeam.server", "--db", db, "--port", str(self.port),
               "--log-level", "WARNING", *args]
        if self.durability != "full":
            cmd += ["--durability", self.durability]
        if store_bodies:
            cmd += ["--store-bodies", ",".join(store_bodies)]
        self.log = open(log_path or os.devnull, "ab")
        self.proc = subprocess.Popen(cmd, stdout=self.log, stderr=self.log)
        self.base = f"http://127.0.0.1:{self.port}"
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            try:
                get(self.port, "/api/none", timeout=5)  # a 404 straight away, also from servers before QV-70, whose /api/health counted the table
                return
            except OSError:
                if self.proc.poll() is not None:
                    raise RuntimeError("the server exited; see its log")
                time.sleep(0.2)
        raise RuntimeError("the server did not start")

    @property
    def otlp(self) -> str:
        return self.base + "/v1/traces"

    def cpu_s(self) -> float:
        try:
            f = Path(f"/proc/{self.proc.pid}/stat").read_text().rsplit(")", 1)[1].split()
            return (int(f[11]) + int(f[12])) / os.sysconf("SC_CLK_TCK")
        except (OSError, IndexError):
            return float("nan")

    def rss_mb(self) -> float:
        try:
            for line in Path(f"/proc/{self.proc.pid}/status").read_text().splitlines():
                if line.startswith("VmRSS:"):
                    return round(int(line.split()[1]) / 1024, 1)
        except OSError:
            pass
        return float("nan")

    def stop(self) -> None:
        self.proc.terminate()
        try:
            self.proc.wait(30)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait()
        self.log.close()


def get(port: int, path: str, timeout: float = 300) -> tuple[int, bytes]:
    c = http.client.HTTPConnection("127.0.0.1", port, timeout=timeout)
    try:
        c.request("GET", path)
        r = c.getresponse()
        return r.status, r.read()
    finally:
        c.close()


# ---- pages --------------------------------------------------------------------------------

def page_paths(db_path: str, profile: loadgen.Profile, n_messages: int = 20, seed_no: int = 3) -> dict:
    """The page URLs to time. Message detail uses ``n_messages`` random stored messages."""
    c = sqlite3.connect(db_path)
    top = c.execute("SELECT MAX(row) FROM spans").fetchone()[0] or 1
    rng = random.Random(seed_no)
    mids = []
    while len(mids) < n_messages:
        r = c.execute("SELECT mid FROM spans WHERE row = ? AND mid IS NOT NULL", (rng.randrange(1, top + 1),)).fetchone()
        if r:
            mids.append(r[0])
    c.close()
    queue = loadgen.queue_names(profile)[min(3, profile.queues - 1)]
    body_queue = loadgen.body_queue_names(profile)[0]
    return {
        "messages": ["/api/messages"],
        "messages_one_queue": [f"/api/messages?queue={queue}"],
        "message_detail": [f"/api/messages/{quote(m)}" for m in mids],
        "duplicates": ["/api/duplicates"],
        "stuck": ["/api/stuck"],
        "tenants": ["/api/tenants"],
        "tenant_search": ["/api/tenants/search?q=tenant-00"],
        # bounded (QV-76): the last day by default, at most bodies.SCAN messages per page
        "body_search": ["/api/messages?body=" + quote("body.order.status:failed")],
        "body_search_one_queue": [f"/api/messages?queue={body_queue}&body=" + quote("body.order.status:failed")],
        "body_search_no_match": ["/api/messages?body=" + quote("body.order.sku:NO-SUCH-SKU")],
    }


def time_pages(port: int, pages: dict, *, repeats: int, budget_s: float, timeout_s: float, restart=None) -> dict:
    """p50/p95/max per page. Each page runs ``repeats`` times, or fewer if it uses up ``budget_s``.

    A page that times out is not repeated. The server is still running its query, holding the lock every
    other request waits on, so ``restart()`` (if given) replaces the server and returns the new port."""
    out = {}
    for name, paths in pages.items():
        lat, errors, timeouts = [], 0, 0
        t0 = time.monotonic()
        for i in range(repeats):
            if i and time.monotonic() - t0 > budget_s:
                break
            s = time.monotonic()
            try:
                status, _ = get(port, paths[i % len(paths)], timeout=timeout_s)
                errors += status != 200
            except (TimeoutError, socket.timeout):
                timeouts += 1
                lat.append(timeout_s)
                if restart:
                    port = restart()
                break
            except OSError:
                errors += 1
                continue
            lat.append(time.monotonic() - s)
        out[name] = _lat_summary(lat) | {"errors": errors, "timeouts": timeouts}
    return out


def _lat_summary(lat: list) -> dict:
    p = loadgen.percentile
    return {"n": len(lat), "p50_ms": _ms(p(lat, 0.5)), "p95_ms": _ms(p(lat, 0.95)), "max_ms": _ms(max(lat or [0]) or None)}


def _ms(s):
    return None if s is None else round(s * 1000, 1)


class PageLoop(threading.Thread):
    """Cycles through the pages until stopped, as people using the screen during ingest would."""

    def __init__(self, port: int, pages: dict, timeout_s: float):
        super().__init__(daemon=True)
        self.port, self.pages, self.timeout_s = port, pages, timeout_s
        self.lat: dict = {k: [] for k in pages}
        self.timeouts: dict = {k: 0 for k in pages}
        self.halt = threading.Event()

    def run(self):
        i = 0
        while not self.halt.is_set():
            for name, paths in self.pages.items():
                if self.halt.is_set():
                    return
                s = time.monotonic()
                try:
                    get(self.port, paths[i % len(paths)], timeout=self.timeout_s)
                    self.lat[name].append(time.monotonic() - s)
                except OSError:
                    self.timeouts[name] += 1
            i += 1

    def result(self) -> dict:
        return {k: _lat_summary(v) | {"timeouts": self.timeouts[k]} for k, v in self.lat.items()}


# ---- ingest -------------------------------------------------------------------------------

def ingest_step(srv: Server, profile, rate: float, seconds: float, args, keep_latencies: bool = False) -> dict:
    cpu0, t0 = srv.cpu_s(), time.monotonic()
    res = loadgen.run(srv.otlp, profile, rate, seconds, concurrency=args.concurrency, processes=args.processes,
                      timeout=args.export_timeout, grace=args.export_timeout, keep_latencies=keep_latencies)
    wall = time.monotonic() - t0
    res["server_cpu_pct"] = round(100 * (srv.cpu_s() - cpu0) / wall, 1)
    res["server_rss_mb"] = srv.rss_mb()
    res["steady"] = bool(res["errors"] == 0 and res["achieved_spans_per_s"] >= 0.95 * res["target_spans_per_s"]
                         and (res["export_p99_ms"] or 0) <= args.max_export_p99_ms and res["behind_p99_s"] < 5)
    for k in ("profile", "processes", "concurrency"):
        res.pop(k, None)
    return res


def purge_under_ingest(db_path: str, snapshot: Optional[str], profile, store_bodies, args, log=print,
                       log_path: Optional[str] = None) -> dict:
    """A retention pass while ingest runs. Twice from the seeded file: ``baseline`` with retention off, then
    ``purge`` with ``--purge-retention``, whose pass starts with the server. Ingest runs in windows of
    ``--purge-window-s`` at ``--purge-spans-per-s`` until the pass has ended (or ``--purge-max-s``); the gate
    is the worst window's export p99. The export p99 over all of a case's exports is reported too
    (``all_export_p99_ms``). ``--purge-batch`` and ``--purge-duty`` set the purge's step size and share of the
    writer (the server's defaults if unset). Records the pass (from /api/health) and the file before and after."""
    spm = profile.mean_spans_per_message()
    rate = args.purge_spans_per_s / spm
    out: dict = {"retention": args.purge_retention, "target_spans_per_s": args.purge_spans_per_s,
                 "purge_batch": args.purge_batch or None, "purge_duty": args.purge_duty or None}
    tuning = ((("--purge-batch", str(args.purge_batch)) if args.purge_batch else ())
              + (("--purge-duty", str(args.purge_duty)) if args.purge_duty else ()))

    def storage() -> dict:
        c = sqlite3.connect(db_path)
        try:
            ps, pages, free, av = (c.execute(f"PRAGMA {p}").fetchone()[0]
                                   for p in ("page_size", "page_count", "freelist_count", "auto_vacuum"))
            spans = c.execute("SELECT COUNT(*) FROM spans").fetchone()[0]
        finally:
            c.close()
        return {"spans": spans, "file_mb": round(ps * pages / 1e6, 1), "free_mb": round(ps * free / 1e6, 1),
                "auto_vacuum": {0: "none", 1: "full", 2: "incremental"}[av]}

    def health(srv) -> dict:
        return json.loads(get(srv.port, "/api/health", timeout=30)[1])["config"]["retention"] or {}

    for case, extra in (("baseline", ("--retention", "forever")),
                        ("purge", ("--retention", args.purge_retention, "--purge-interval", "86400") + tuning)):
        if snapshot:
            restore_file(snapshot, db_path)
        before = storage()
        srv = Server(db_path, store_bodies, log_path, args=extra)
        windows, lat, t0 = [], [], time.monotonic()
        try:
            while True:
                w = ingest_step(srv, profile, rate, args.purge_window_s, args, keep_latencies=True)
                lat += w.pop("latencies_s")
                h = health(srv) if case == "purge" else {}
                w["pass_running"] = bool(h.get("running"))
                if case == "purge":  # the pass so far: was a long step what held up the exports?
                    so_far = h.get("running") or h.get("last_pass") or {}
                    w["longest_step_ms"], w["longest_write_ms"] = so_far.get("longest_step_ms"), so_far.get("longest_write_ms")
                windows.append(w)
                log(f"  {case}: {w['achieved_spans_per_s']} spans/s, export p99 {w['export_p99_ms']} ms, errors "
                    f"{w['errors']}" + (f", pass {'running' if w['pass_running'] else 'done'}, longest step so far "
                                       f"{w['longest_step_ms']} ms, of it under the writer lock {w['longest_write_ms']} ms" if case == "purge" else ""))
                done = case == "baseline" or (h.get("passes", 0) >= 1 and not h.get("running"))
                if done or time.monotonic() - t0 > args.purge_max_s:
                    break
            passed = h.get("last_pass") if case == "purge" else None
            unfinished = h.get("running") if case == "purge" else None  # the pass so far, if --purge-max-s cut it
        finally:
            srv.stop()
        res = {"windows": windows, "before": before, "after": storage(),
               "worst_export_p99_ms": max((w["export_p99_ms"] or 0) for w in windows),
               "all_export_p99_ms": loadgen._ms(loadgen.percentile(lat, 0.99)), "exports": len(lat),
               "windows_over_gate": sum((w["export_p99_ms"] or 0) > args.max_export_p99_ms for w in windows),
               "errors": sum(w["errors"] for w in windows)}
        if case == "purge":
            res["pass"] = passed
            res["pass_finished"] = passed is not None
            if passed is None:
                res["pass_so_far"] = unfinished
        out[case] = res
    p = out["purge"]
    out["gate_ok"] = bool(p["pass_finished"] and p["errors"] == 0 and p["worst_export_p99_ms"] <= args.max_export_p99_ms)
    return out


def scrape(port: int) -> Optional[dict]:
    """The server's /metrics as {'name{labels}': value}; None for a server without it (before QV-74)."""
    try:
        status, body = get(port, "/metrics", timeout=30)
    except OSError:
        return None
    if status != 200:
        return None
    out = {}
    for line in body.decode().splitlines():
        if line and not line.startswith("#"):
            k, _, v = line.rpartition(" ")
            out[k] = float(v)
    return out


def _delta(after: dict, before: dict, prefix: str) -> dict:
    """``{label part: after - before}`` for the series of one metric, e.g. reason="queue_full" -> 3."""
    keys = {k for k in after if k.startswith(prefix + "{") or k == prefix}
    return {k[len(prefix):].strip("{}") or "": after[k] - before.get(k, 0) for k in sorted(keys)}


def overload_step(srv: Server, db_path: str, profile, rate: float, sdk_rate: float, seconds: float, args) -> dict:
    """Ingest above the ceiling (QV-74): the load generator at ``rate`` and, alongside it, the OTel SDK at
    ``sdk_rate``, so the SDK's exporter meets a server that is already full. Then account for every span:
    the server must have stored every span it answered 200 for, and answered 429/503 with Retry-After
    instead of hanging. ``acked_not_stored`` is the silent loss on the server side; it must be 0.
    ``stored_not_acked`` is spans the server committed after the client had given up on the export (it
    timed out first); the SDK's retry of those is a duplicate, which the server skips."""
    before_rows = span_count(db_path)
    m0 = scrape(srv.port)
    box: dict = {}
    t = threading.Thread(target=lambda: box.update(raw=loadgen.run(
        srv.otlp, profile, rate, seconds, concurrency=args.overload_concurrency or args.concurrency,
        processes=args.processes,
        timeout=args.export_timeout, grace=args.export_timeout)))
    t.start()
    time.sleep(min(5.0, seconds / 4))  # the server is full before the SDK starts
    sdk = loadgen.run_sdk(srv.otlp, profile, sdk_rate, max(seconds - 5, 5), settle_s=30)
    sdk.pop("profile", None)
    t.join()
    raw = box["raw"]
    for k in ("profile", "processes", "concurrency"):
        raw.pop(k, None)
    deadline = time.monotonic() + 120  # exports still being written when the clients stopped
    m1 = scrape(srv.port)
    while m1 is not None and m1.get("trailbeam_ingest_queue_depth", 0) > 0 and time.monotonic() < deadline:
        time.sleep(0.5)
        m1 = scrape(srv.port)
    stored = span_count(db_path) - before_rows
    acked = raw["spans_ok"] + sdk["exported_spans"]
    res = {"target_msgs_per_s": rate, "sdk_msgs_per_s": sdk_rate, "seconds": seconds, "loadgen": raw, "sdk": sdk,
           "acked_spans": acked, "stored_spans": stored,
           "acked_not_stored": max(acked - stored, 0), "stored_not_acked": max(stored - acked, 0)}
    if m0 is not None and m1 is not None:
        res["server"] = {
            "requests_by_code": _delta(m1, m0, "trailbeam_ingest_requests_total"),
            "rejected_by_reason": _delta(m1, m0, "trailbeam_ingest_rejected_requests_total"),
            "spans_by_result": _delta(m1, m0, "trailbeam_ingest_spans_total"),
            "retry_after_now_s": m1.get("trailbeam_ingest_retry_after_seconds")}
        inserted = res["server"]["spans_by_result"].get('result="inserted"', 0)
        res["server_inserted_not_stored"] = max(inserted - stored, 0)  # also must be 0
    return res


def breakdown(db_path: str, profile, store_bodies, batches: int = 3) -> dict:
    """Where one export's time goes, in-process on the database file: decode, insert, and the join queries.
    Adds ``batches`` + 1 exports of spans to the file. The first, on a cold page cache, is reported on its own
    (``cold_insert_ms``) and left out of the averages."""
    from trailbeam.server.otlp import decode

    db = Database(db_path, store_bodies=store_bodies)
    timers = {"join_settle": 0.0, "join_receive": 0.0}

    def timed(name, fn):
        def wrapper(*a):
            s = time.perf_counter()
            try:
                return fn(*a)
            finally:
                timers[name] += time.perf_counter() - s
        return wrapper

    # Measurement only: the same methods, wrapped on this one instance to time them.
    db._settle_receive = timed("join_settle", db._settle_receive)
    db._join_receive = timed("join_receive", db._join_receive)
    stream = loadgen.Stream(profile, seed=4242 + span_count(db_path))
    dec = ins = 0.0
    spans = 0
    plan = db.conn.execute("EXPLAIN QUERY PLAN " + SETTLE_JOIN_SQL, ("x", "y", 0, 0)).fetchall()
    receive_plan = db.conn.execute("EXPLAIN QUERY PLAN " + RECEIVE_JOIN_SQL, ("m", 0, "x", "y")).fetchall()
    s = time.perf_counter()
    db.insert(decode(loadgen.encode(stream.batch()), "application/x-protobuf").records)
    cold = time.perf_counter() - s
    timers.update(join_settle=0.0, join_receive=0.0)
    for _ in range(batches):
        body = loadgen.encode(stream.batch())
        s = time.perf_counter()
        d = decode(body, "application/x-protobuf")
        dec += time.perf_counter() - s
        s = time.perf_counter()
        db.insert(d.records)
        ins += time.perf_counter() - s
        spans += len(d.records)
    db.close()
    per = lambda x: round(1000 * x / batches, 1)  # noqa: E731
    return {**wal_pages(db_path, profile, store_bodies, stream, batches),
            "spans_per_export": spans // batches, "decode_ms": per(dec), "insert_ms": per(ins),
            "join_settle_ms": per(timers["join_settle"]), "join_receive_ms": per(timers["join_receive"]),
            "insert_spans_per_s": round(spans / ins, 1) if ins else None, "cold_insert_ms": round(1000 * cold, 1),
            "join_settle_plan": "; ".join(r[3] for r in plan),
            "join_receive_plan": "; ".join(r[3] for r in receive_plan)}


def wal_pages(db_path: str, profile, store_bodies, stream, batches: int = 3) -> dict:
    """The pages one export writes, and its COMMIT time. The checkpoint thread is stopped, and the log is
    emptied (TRUNCATE) before each export, so the log holds that export's frames only: every page it changed,
    once. Then they are counted by the B-tree each belongs to (``dbstat``; "ptrmap" is auto-vacuum's pointer
    map, which a page split in any B-tree also writes). A COMMIT writes and fsyncs these pages, so their number
    is most of an export's time (QV-70, QV-78). Adds ``batches`` exports to the file."""
    from trailbeam.server.otlp import decode

    db = Database(db_path, store_bodies=store_bodies)
    if db._checkpointer is None:  # :memory: has no log
        db.close()
        return {}
    db._stop.set()  # measurement only: no checkpoint between COMMIT and the count
    db._checkpointer.join()
    page_size = db.conn.execute("PRAGMA page_size").fetchone()[0]
    usable = page_size - db.conn.execute("SELECT 0").fetchone()[0]  # no reserved bytes in a Trailbeam file
    ptrmap_every = usable // 5 + 1  # a pointer-map page, then the pages it maps
    commit_ms, spans, by_tree, frames = [], 0, {}, 0
    real = db.conn

    class Timed:  # measurement only: times the COMMIT insert runs on this connection
        def execute(self, sql, *a):
            if sql == "COMMIT":
                s = time.perf_counter()
                try:
                    return real.execute(sql)
                finally:
                    commit_ms.append(1000 * (time.perf_counter() - s))
            return real.execute(sql, *a)

        def __getattr__(self, k):
            return getattr(real, k)

    try:
        for _ in range(batches):
            records = decode(loadgen.encode(stream.batch()), "application/x-protobuf").records
            real.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            db.conn = Timed()
            try:
                db.insert(records)
            finally:
                db.conn = real
            spans += len(records)
            pages = set()
            with open(db_path + "-wal", "rb") as f:
                f.seek(32)  # the log header; then frames of a 24-byte header and a page
                while len(h := f.read(24)) == 24:
                    pages.add(int.from_bytes(h[:4], "big"))
                    f.seek(page_size, 1)
            frames += len(pages)
            owner = {}
            try:
                for name, pg in real.execute("SELECT name, pageno FROM dbstat"):
                    if pg in pages:
                        owner[pg] = name
            except sqlite3.OperationalError:  # SQLite built without dbstat: totals only
                pass
            for pg in pages:
                name = owner.get(pg) or ("ptrmap" if pg >= 2 and (pg - 2) % ptrmap_every == 0 else "other")
                by_tree[name] = by_tree.get(name, 0) + 1
    finally:
        db.close()
    return {"wal_pages_per_export": round(frames / batches), "wal_pages_per_span": round(frames / spans, 2),
            "commit_ms": round(sum(commit_ms) / batches, 1),
            "wal_pages_by_btree": {k: round(v / spans, 3) for k, v in sorted(by_tree.items(), key=lambda kv: -kv[1])}}


# ---- growth and SDK drops -----------------------------------------------------------------

def growth(workdir: Path, spans: int, profile) -> list[dict]:
    """File bytes per million spans for each body setting, from small separate files."""
    cases = [("no bodies", 0, False), ("1 KB bodies, stored", 1024, True), ("1 KB bodies, not stored", 1024, False),
             ("64 KB bodies, stored", 64 * 1024, True), ("256 KB bodies, stored", 256 * 1024, True)]
    out = []
    for name, size, stored in cases:
        n = spans if size <= 1024 else (20_000 if size <= 64 * 1024 else 10_000)  # big bodies: fewer spans
        path = str(workdir / f"growth-{size}-{int(stored)}.sqlite")
        for p in (path, path + "-wal", path + "-shm"):
            if os.path.exists(p):
                os.remove(p)
        prof = replace(profile, body_size=size, body_queues=0)
        r = seed(path, n, prof, store_bodies=("*",) if stored else (), log=lambda *_: None)
        b = file_bytes(path)
        out.append({"case": name, "spans": r["spans"], "file_mb": round(b / 1e6, 1),
                    "mb_per_million_spans": round(b / 1e6 / (r["spans"] / 1e6), 1)})
        for p in (path, path + "-wal", path + "-shm"):
            if os.path.exists(p):
                os.remove(p)
    return out


# ---- the run ------------------------------------------------------------------------------

def machine() -> dict:
    info = {"python": platform.python_version(), "sqlite": sqlite3.sqlite_version, "os": platform.platform(),
            "cpus": os.cpu_count()}
    try:
        for line in Path("/proc/cpuinfo").read_text().splitlines():
            if line.startswith("model name"):
                info["cpu"] = line.split(":", 1)[1].strip()
                break
        mem = Path("/proc/meminfo").read_text().split()
        info["ram_gb"] = round(int(mem[1]) / 1024 / 1024, 1)
    except OSError:
        pass
    try:
        info["commit"] = subprocess.run(["git", "rev-parse", "--short", "HEAD"], capture_output=True, text=True,
                                        cwd=Path(__file__).parent).stdout.strip()
    except OSError:
        pass
    return info


def parse_rates(text: str, spans_per_message: float) -> list[float]:
    """``--rates``: messages/s, or spans/s with an ``sp`` suffix (``1000sp`` at 3.2 spans per message is
    312.5 msg/s), so a step can sit on an SLO stated in spans/s. Sorted, smallest first: the ramp stops at the
    first step that is not steady."""
    out = []
    for r in filter(None, (x.strip() for x in text.split(","))):
        out.append(float(r[:-2]) / spans_per_message if r.endswith("sp") else float(r))
    return sorted(set(out))


def parse_size(text: str) -> int:
    t = text.strip().upper()
    mult = {"K": 1_000, "M": 1_000_000, "B": 1_000_000_000}.get(t[-1:], 1)
    return int(float(t[:-1] if t[-1:] in "KMB" else t) * mult)


def run(args) -> dict:
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    work = Path(args.workdir or out)
    work.mkdir(parents=True, exist_ok=True)
    db_path = str(work / "bench.sqlite")
    if args.fresh:
        for p in (db_path, db_path + "-wal", db_path + "-shm"):
            if os.path.exists(p):
                os.remove(p)
    profile = loadgen.profile_from(args)
    store = tuple(loadgen.body_queue_names(profile)) if profile.body_size else ()
    results = {"machine": machine(), "started": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
               "args": {k: v for k, v in vars(args).items()}, "profile": asdict(profile),
               "spans_per_message": round(profile.mean_spans_per_message(), 3), "store_bodies": list(store),
               "sizes": []}

    def save():
        (out / "results.json").write_text(json.dumps(results, indent=2))
        write_csv(results, out / "results.csv")
        (out / "summary.md").write_text(summary(results))

    def log(msg):
        print(msg, flush=True)

    skipped = {name: "--skip-pages" for name in filter(None, args.skip_pages.split(","))}
    for size in [parse_size(s) for s in args.sizes.split(",")]:
        log(f"== {size:,} spans")
        sd = seed(db_path, size, profile, store_bodies=store, days=args.seed_days, log=log)
        entry = {"target_spans": size, "spans": sd["spans"], "seed_seconds": sd["seconds"],
                 "file_mb": round(file_bytes(db_path) / 1e6, 1)}
        results["sizes"].append(entry)
        log(f"  {entry['spans']:,} spans, {entry['file_mb']:,} MB")
        if args.purge_only or args.breakdown_only:
            entry["purge_only" if args.purge_only else "breakdown_only"] = True
            pages = {}
        else:
            pages = {k: v for k, v in page_paths(db_path, profile).items() if k not in skipped}
        if skipped:
            entry["pages_skipped"] = dict(skipped)
        if not (args.purge_only or args.breakdown_only):
            # Each ingest step writes spans: at a ceiling of thousands of spans/s one ramp adds far more than a
            # 10k or 100k database holds. So every step starts from a copy of the seeded file, while copying is
            # cheap. Above --restore-max-gb the steps run on the growing file and record its size instead.
            snapshot = None
            if file_bytes(db_path) <= args.restore_max_gb * 1e9:
                snapshot = str(work / "bench.seeded.sqlite")
                copy_synced(db_path, snapshot)
            entry["steps_restore_seeded_file"] = snapshot is not None
            box = {"srv": Server(db_path, store, str(out / "server.log"))}

            def restart() -> int:
                box["srv"].stop()
                box["srv"] = Server(db_path, store, str(out / "server.log"))
                return box["srv"].port

            def restore_seeded() -> None:
                box["srv"].stop()
                if snapshot:
                    restore_file(snapshot, db_path)
                box["srv"] = Server(db_path, store, str(out / "server.log"))

            try:
                entry["pages_idle"] = time_pages(box["srv"].port, pages, repeats=args.page_repeats,
                                                 budget_s=args.page_budget_s, timeout_s=args.page_timeout_s,
                                                 restart=restart)
                for name, v in entry["pages_idle"].items():
                    if v["timeouts"]:  # it would hold the lock through every later measurement
                        skipped[name] = f"timed out ({args.page_timeout_s:g} s) at {entry['spans']:,} spans"
                pages = {k: v for k, v in pages.items() if k not in skipped}
                log("  pages idle: " + ", ".join(f"{k} {v['p95_ms']}" for k, v in entry["pages_idle"].items()))
                save()
                ramp = []
                for rate in parse_rates(args.rates, profile.mean_spans_per_message()):
                    restore_seeded()
                    before = span_count(db_path)
                    step = ingest_step(box["srv"], profile, rate, args.step_seconds, args)
                    step["db_spans_before"] = before
                    ramp.append(step)
                    log(f"  ingest {rate:g} msg/s ({step['target_spans_per_s']:g} spans/s): achieved "
                        f"{step['achieved_spans_per_s']:g} spans/s, p99 {step['export_p99_ms']} ms, errors "
                        f"{step['errors']}, cpu {step['server_cpu_pct']}%{'' if step['steady'] else '  NOT STEADY'}")
                    entry["ingest"] = ramp
                    save()
                    if not step["steady"]:
                        break  # the next restore_seeded() also drops the backlog the server is still working through
                steady = [s for s in ramp if s["steady"]]
                best = steady[-1] if steady else None
                entry["max_steady_spans_per_s"] = best["achieved_spans_per_s"] if best else 0
                entry["max_steady_msgs_per_s"] = best["target_msgs_per_s"] if best else 0
                under_rate = best["target_msgs_per_s"] if best else ramp[0]["target_msgs_per_s"]
                restore_seeded()
                entry["ingest_under_pages_db_spans_before"] = span_count(db_path)
                loop = PageLoop(box["srv"].port, pages, args.page_timeout_s)
                loop.start()
                entry["ingest_under_pages"] = ingest_step(box["srv"], profile, under_rate, args.under_load_seconds, args)
                loop.halt.set()
                loop.join(args.page_timeout_s + 5)
                entry["pages_under_ingest"] = loop.result()
                entry["pages_under_ingest_rate_msgs_per_s"] = under_rate
                log("  pages under ingest: " + ", ".join(f"{k} {v['p95_ms']}" for k, v in entry["pages_under_ingest"].items()))
                log(f"  ingest while pages load: p99 {entry['ingest_under_pages']['export_p99_ms']} ms, achieved "
                    f"{entry['ingest_under_pages']['achieved_spans_per_s']} spans/s")
                save()
                if args.overload_seconds:
                    # above the ceiling: the first rate that was not steady, times --overload-factor
                    last = ramp[-1]
                    over = last["target_msgs_per_s"] * (1 if not last["steady"] else 2) * args.overload_factor
                    restore_seeded()
                    entry["overload"] = ov = overload_step(box["srv"], db_path, profile, over, max(under_rate, 5),
                                                           args.overload_seconds, args)
                    log(f"  overload {over:g} msg/s: loadgen statuses {ov['loadgen']['statuses']}, SDK attempts "
                        f"{ov['sdk']['http_statuses']} ({ov['sdk']['exporter_retries']} retries), SDK dropped "
                        f"{ov['sdk']['dropped_spans']}; acked {ov['acked_spans']}, stored {ov['stored_spans']}, "
                        f"acked but not stored {ov['acked_not_stored']}")
                    save()
                    restore_seeded()
                if args.sdk_seconds and size == max(parse_size(s) for s in args.sizes.split(",")):
                    sdk_rate = max(under_rate * 4, 5)
                    entry["sdk"] = loadgen.run_sdk(box["srv"].otlp, profile, sdk_rate, args.sdk_seconds, settle_s=30)
                    entry["sdk"].pop("profile", None)
                    log(f"  SDK at {sdk_rate:g} msg/s: dropped {entry['sdk']['dropped_spans']} of "
                        f"{entry['sdk']['emitted_spans']} ({entry['sdk']['dropped_pct']}%)")
            finally:
                box["srv"].stop()
                if snapshot:  # the next size grows from this size's seeded file, not from what the ramp added
                    restore_file(snapshot, db_path)
                    os.remove(snapshot)
        if args.purge_retention:
            snap = None
            if file_bytes(db_path) <= args.restore_max_gb * 1e9:
                snap = str(work / "bench.seeded.sqlite")
                copy_synced(db_path, snap)
            try:
                entry["purge_under_ingest"] = purge_under_ingest(db_path, snap, profile, store, args, log,
                                                                 str(out / "server.log"))
            finally:
                if snap:
                    restore_file(snap, db_path)
                    os.remove(snap)
            pu = entry["purge_under_ingest"]
            log(f"  purge under ingest: worst export p99 {pu['purge']['worst_export_p99_ms']} ms "
                f"(baseline {pu['baseline']['worst_export_p99_ms']} ms); over all exports {pu['purge']['all_export_p99_ms']} ms "
                f"(baseline {pu['baseline']['all_export_p99_ms']} ms), gate {'ok' if pu['gate_ok'] else 'FAILED'}")
            save()
        if args.purge_only:
            continue
        entry["breakdown"] = breakdown(db_path, profile, store)
        log(f"  breakdown: {entry['breakdown']}")
        entry["file_mb_after"] = round(file_bytes(db_path) / 1e6, 1)
        save()

    if args.growth_spans:
        log("== file growth")
        results["growth"] = growth(work, args.growth_spans, profile)
        for g in results["growth"]:
            log(f"  {g['case']}: {g['mb_per_million_spans']} MB per million spans")
    results["finished"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    save()
    if args.cleanup:
        for p in (db_path, db_path + "-wal", db_path + "-shm"):
            if os.path.exists(p):
                os.remove(p)
    return results


# ---- output -------------------------------------------------------------------------------

def write_csv(results: dict, path: Path) -> None:
    rows = []
    for e in results["sizes"]:
        base = {"db_spans": e["spans"], "db_file_mb": e["file_mb"]}
        for phase in ("pages_idle", "pages_under_ingest"):
            for page, v in (e.get(phase) or {}).items():
                rows.append({**base, "measurement": phase, "name": page, "p50_ms": v["p50_ms"], "p95_ms": v["p95_ms"],
                             "max_ms": v["max_ms"], "n": v["n"], "timeouts": v.get("timeouts", 0)})
        for s in e.get("ingest") or []:
            rows.append({**base, "measurement": "ingest", "name": f"{s['target_msgs_per_s']:g} msg/s",
                         "target_spans_per_s": s["target_spans_per_s"], "achieved_spans_per_s": s["achieved_spans_per_s"],
                         "p50_ms": s["export_p50_ms"], "p99_ms": s["export_p99_ms"], "errors": s["errors"],
                         "server_cpu_pct": s["server_cpu_pct"], "steady": s["steady"]})
        if e.get("ingest_under_pages"):
            s = e["ingest_under_pages"]
            rows.append({**base, "measurement": "ingest_under_pages", "name": f"{s['target_msgs_per_s']:g} msg/s",
                         "target_spans_per_s": s["target_spans_per_s"], "achieved_spans_per_s": s["achieved_spans_per_s"],
                         "p50_ms": s["export_p50_ms"], "p99_ms": s["export_p99_ms"], "errors": s["errors"],
                         "server_cpu_pct": s["server_cpu_pct"], "steady": s["steady"]})
    for g in results.get("growth") or []:
        rows.append({"measurement": "growth", "name": g["case"], "db_spans": g["spans"], "db_file_mb": g["file_mb"],
                     "mb_per_million_spans": g["mb_per_million_spans"]})
    keys = []
    for r in rows:
        keys += [k for k in r if k not in keys]
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, keys)
        w.writeheader()
        w.writerows(rows)


def summary(results: dict) -> str:
    m = results["machine"]
    lines = ["# Trailbeam benchmark", "",
             f"Commit `{m.get('commit')}`, {m.get('cpu')} ({m.get('cpus')} threads), {m.get('ram_gb')} GB RAM, "
             f"{m.get('os')}, Python {m.get('python')}, SQLite {m.get('sqlite')}. "
             f"{results['spans_per_message']} spans per message; queues {results['profile']['queues']}, "
             f"tenants {results['profile']['tenants']}, body {results['profile']['body_size']} B. "
             f"Server durability: {results['args'].get('server_durability', 'full')}.", ""]
    sizes = results["sizes"]
    if sizes:
        lines += ["## Ingest", "", "| DB spans | file MB | max steady spans/s (msg/s) | export p99 at that rate | "
                  "server CPU | first rate not steady | DB spans at that step's start |", "|---|---|---|---|---|---|---|"]
        for e in sizes:
            ramp = e.get("ingest") or []
            steady = [s for s in ramp if s["steady"]]
            bad = next((s for s in ramp if not s["steady"]), None)
            b = steady[-1] if steady else None
            lines.append(f"| {e['spans']:,} | {e['file_mb']:,} | "
                         f"{(str(b['achieved_spans_per_s']) + ' (' + format(b['target_msgs_per_s'], 'g') + ')') if b else 'none'} | "
                         f"{b['export_p99_ms'] if b else '-'} ms | {b['server_cpu_pct'] if b else '-'}% | "
                         + (f"{bad['target_msgs_per_s']:g} msg/s: {bad['achieved_spans_per_s']} of "
                            f"{bad['target_spans_per_s']} spans/s, p99 {bad['export_p99_ms']} ms, {bad['errors']} errors"
                            if bad else "-") + " | "
                         + (f"{(bad or b)['db_spans_before']:,}" if (bad or b) and (bad or b).get("db_spans_before") else "-")
                         + ("" if e.get("steps_restore_seeded_file") else " (file not restored between steps)") + " |")
        names = list((sizes[0].get("pages_idle") or {}).keys())
        lines += ["", "## Page p95 (ms), idle / under ingest", "",
                  "| DB spans | " + " | ".join(names) + " |", "|---|" + "---|" * len(names)]
        for e in sizes:
            idle, busy = e.get("pages_idle") or {}, e.get("pages_under_ingest") or {}
            cells = []
            for n in names:
                a = idle.get(n, {}).get("p95_ms")
                b = busy.get(n, {}).get("p95_ms")
                if n in (e.get("pages_skipped") or {}):
                    cells.append("skipped")
                    continue
                t = " (timeout)" if idle.get(n, {}).get("timeouts") or busy.get(n, {}).get("timeouts") else ""
                cells.append(f"{a} / {b}{t}")
            lines.append(f"| {e['spans']:,} | " + " | ".join(cells) + " |")
        lines += ["", "## Where an export's time goes (in-process, 512-span export)", "",
                  "| DB spans | decode ms | insert ms | of which join_settle ms | join_receive ms | spans/s | cold first insert ms "
                  "| WAL pages per export (per span) | COMMIT ms | settle join plan | receive join plan |",
                  "|---|---|---|---|---|---|---|---|---|---|---|"]
        for e in sizes:
            b = e.get("breakdown")
            if b:
                lines.append(f"| {e['spans']:,} | {b['decode_ms']} | {b['insert_ms']} | {b['join_settle_ms']} | "
                             f"{b['join_receive_ms']} | {b['insert_spans_per_s']} | {b.get('cold_insert_ms', '-')} | "
                             f"{b.get('wal_pages_per_export', '-')} ({b.get('wal_pages_per_span', '-')}) | {b.get('commit_ms', '-')} | "
                             f"{b['join_settle_plan']} | "
                             f"{b.get('join_receive_plan', '-')} |")
        trees = [e for e in sizes if (e.get("breakdown") or {}).get("wal_pages_by_btree")]
        if trees:
            lines += ["", "WAL pages per span, by B-tree:", ""]
            lines += [f"- {e['spans']:,} spans: " + ", ".join(f"{k} {v}" for k, v in e["breakdown"]["wal_pages_by_btree"].items())
                      for e in trees]
        sdk = [e["sdk"] for e in sizes if e.get("sdk")]
        if sdk:
            s = sdk[-1]
            lines += ["", "## OTel SDK BatchSpanProcessor against the largest database", "",
                      f"{s['target_msgs_per_s']:g} msg/s ({s['target_spans_per_s']} spans/s): emitted {s['emitted_spans']}, "
                      f"exported {s['exported_spans']}, server received {s['received_spans']}, "
                      f"**dropped {s['dropped_spans']} ({s['dropped_pct']}%)**: {s['never_exported_spans']} never "
                      f"exported (queue full), {s['failed_export_spans']} in failed exports."]
    purges = [e for e in sizes if e.get("purge_under_ingest")]
    if purges:
        lines += ["", "## Retention pass under ingest", "",
                  "| DB spans | retention | purge batch / duty | ingest spans/s | export p99, worst window: baseline / purge | "
                  "export p99, all exports: baseline / purge | windows over gate | errors | "
                  "pass s | spans deleted | longest delete step / of it locked / reclaim step ms | space given back MB | file MB before / after | gate |",
                  "|---|---|---|---|---|---|---|---|---|---|---|---|---|---|"]
        for e in purges:
            pu = e["purge_under_ingest"]
            b, q = pu["baseline"], pu["purge"]
            ps = q.get("pass") or {}
            lines.append(
                f"| {e['spans']:,} | {pu['retention']} | {pu.get('purge_batch') or 'default'} / {pu.get('purge_duty') or 'default'} | "
                f"{pu['target_spans_per_s']:g} | {b['worst_export_p99_ms']} / "
                f"{q['worst_export_p99_ms']} ms | {b.get('all_export_p99_ms')} / {q.get('all_export_p99_ms')} ms | "
                f"{q.get('windows_over_gate')} of {len(q['windows'])} | {q['errors']} | {ps.get('seconds', 'not finished')} | "
                f"{format(ps['deleted_spans'], ',') if 'deleted_spans' in ps else '-'} | {ps.get('longest_step_ms', '-')} / {ps.get('longest_write_ms', '-')} / {ps.get('longest_reclaim_step_ms', '-')} | "
                f"{round(ps.get('reclaimed_pages', 0) * 4096 / 1e6, 1)} | {q['before']['file_mb']:,} / {q['after']['file_mb']:,} "
                f"({q['before']['auto_vacuum']}) | {'ok' if pu['gate_ok'] else 'FAILED'} |")
    over = [e for e in sizes if e.get("overload")]
    if over:
        lines += ["", "## Above the ceiling: backpressure (429/503 + Retry-After)", "",
                  "Load generator above the first rate that was not steady, with the OTel SDK alongside. "
                  "Acked = spans in a 200, as the clients saw it. Acked but not stored must be 0.", "",
                  "| DB spans | msg/s | loadgen statuses | SDK attempts by status | SDK retries | Retry-After seen "
                  "| SDK dropped | acked | stored | acked but not stored | server rejected |",
                  "|---|---|---|---|---|---|---|---|---|---|---|"]
        for e in over:
            o = e["overload"]
            rej = ", ".join(f"{k.split('=')[-1].strip(chr(34))} {v:g}"
                            for k, v in (o.get("server") or {}).get("rejected_by_reason", {}).items()) or "-"
            lines.append(f"| {e['spans']:,} | {o['target_msgs_per_s']:g} | {o['loadgen']['statuses']} | "
                         f"{o['sdk']['http_statuses']} | {o['sdk']['exporter_retries']} | "
                         f"{', '.join(o['sdk']['retry_after_values']) or '-'} | {o['sdk']['dropped_spans']} | "
                         f"{o['acked_spans']:,} | {o['stored_spans']:,} | **{o['acked_not_stored']}** | {rej} |")
    if results.get("growth"):
        lines += ["", "## File growth", "", "| case | MB per million spans |", "|---|---|"]
        lines += [f"| {g['case']} | {g['mb_per_million_spans']:,} |" for g in results["growth"]]
    return "\n".join(lines) + "\n"


def main(argv=None) -> None:
    p = argparse.ArgumentParser(description="Seed, ingest ramp and page timings for the Trailbeam server.")
    p.add_argument("--out", default="bench-results", help="directory for results.json, results.csv, summary.md")
    p.add_argument("--workdir", default=None, help="directory for the SQLite file (default: --out)")
    p.add_argument("--sizes", default="100k,1M", help="database sizes in spans, e.g. 100k,1M,10M,100M")
    p.add_argument("--rates", default="1,2,5,10,20,50,100,200,1000sp,500,1000,2000",
                   help="ingest ramp, messages/s; NNNsp is NNN spans/s (1000sp: the small-tier SLO rate)")
    p.add_argument("--step-seconds", type=float, default=30)
    p.add_argument("--under-load-seconds", type=float, default=60, help="ingest while the pages are timed")
    p.add_argument("--max-export-p99-ms", type=float, default=2000, help="a ramp step above this is not steady")
    p.add_argument("--export-timeout", type=float, default=10, help="seconds, the OTLP exporter's default")
    p.add_argument("--concurrency", type=int, default=4)
    p.add_argument("--processes", type=int, default=2)
    p.add_argument("--page-repeats", type=int, default=20)
    p.add_argument("--page-budget-s", type=float, default=60, help="stop repeating a page after this long")
    p.add_argument("--page-timeout-s", type=float, default=300,
                   help="a page that takes longer restarts the server and is skipped at larger sizes")
    p.add_argument("--skip-pages", default="", help="pages not to time, e.g. stuck,tenants")
    p.add_argument("--seed-days", type=float, default=7)
    p.add_argument("--growth-spans", type=int, default=200_000, help="0 skips the file growth measurement")
    p.add_argument("--overload-seconds", type=float, default=30,
                   help="ingest above the ceiling, to check 429/503 backpressure (QV-74); 0 skips it")
    p.add_argument("--overload-concurrency", type=int, default=16,
                   help="concurrent exports per process in the overload step. Above the ceiling a fleet of producers "
                        "has more exports in flight than the server takes at once (TRAILBEAM_INGEST_MAX_PENDING, "
                        "16): with the ramp's --concurrency the clients only slow down and the server is never full")
    p.add_argument("--overload-factor", type=float, default=2,
                   help="the overload rate: the first rate that was not steady times this")
    p.add_argument("--sdk-seconds", type=float, default=30, help="0 skips the SDK drop measurement")
    p.add_argument("--restore-max-gb", type=float, default=4,
                   help="start every ingest step from a copy of the seeded file up to this size")
    p.add_argument("--purge-retention", default="",
                   help="also time a retention pass under ingest, with this --retention (e.g. 3d: with --seed-days 7, "
                        "about half the file expires); default: skip")
    p.add_argument("--purge-spans-per-s", type=float, default=1000, help="ingest rate during the purge measurement")
    p.add_argument("--purge-window-s", type=float, default=30, help="export p99 is taken per window of this length")
    p.add_argument("--purge-batch", type=int, default=0,
                   help="messages per purge step (server --purge-batch; 0: the server's default)")
    p.add_argument("--purge-duty", type=float, default=0,
                   help="most of the writer's time the purge takes, in (0, 1] (server --purge-duty; 0: default)")
    p.add_argument("--purge-max-s", type=float, default=1800, help="stop ingest after this long even if the pass runs on")
    p.add_argument("--purge-only", action="store_true",
                   help="seed and the purge measurement only: no pages, ramp, breakdown")
    p.add_argument("--breakdown-only", action="store_true",
                   help="seed and the in-process breakdown only (the writer's ceiling and WAL pages per export): no "
                        "server, pages or ramp")
    p.add_argument("--server-durability", choices=("full", "relaxed"), default="full",
                   help="the server's --durability (relaxed: no fsync per export; default full)")
    p.add_argument("--fresh", action="store_true", help="delete the database file first")
    p.add_argument("--cleanup", action="store_true", help="delete the database file at the end")
    loadgen.add_profile_args(p)
    p.set_defaults(queues=50, tenants=50, body_size=1024, body_queues=5)
    args = p.parse_args(argv)
    Server.durability = args.server_durability
    run(args)


if __name__ == "__main__":
    main()
