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

from trailbeam.server.db import RECORD_FIELDS, SETTLE_KINDS, Database, gate_body  # noqa: E402

DAY_NS = 86_400 * 10 ** 9


# ---- seeding ------------------------------------------------------------------------------

class Seeder:
    """Writes generated spans the way ``Database.insert`` would, with the receipt-hash join in memory.

    Same rows as the server: the columns, the body gate (``gate_body``), ``mid``/``mid_source`` and
    ``receive_row`` in both join directions. A settle span joins the receive with its hash seen earlier
    in this seeding run; generated handles are unique, so there is one candidate. Checked against
    ``Database.insert`` in tests/test_bench_harness.py.
    """

    COLS = ["row"] + RECORD_FIELDS + ["attributes", "mid", "mid_source", "body_dropped", "receive_row"]
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
        for r in records:
            own = r.get("message_id")
            attrs, dropped = gate_body(r.get("attributes", {}), r.get("queue"), self.store_bodies)
            row = self.next_row
            self.next_row += 1
            mid, src, rrow = own, ("span" if own else None), None
            h = (r.get("receipt_hmac"), r.get("key_id"))
            if r["kind"] in SETTLE_KINDS:
                hit = self.receives.get(h)
                if hit is not None:
                    rrow, mid, src = hit[0], hit[1], "receipt_hash"
                else:
                    self.waiting.setdefault(h, []).append(row)
            elif r["kind"] == "receive":
                self.receives[h] = (row, own)
                if len(self.receives) > self.KEEP_RECEIVES:
                    self.receives.pop(next(iter(self.receives)))
                for srow in self.waiting.pop(h, ()):
                    updates.append((own, row, srow))
            rows.append([row] + [r.get(f) for f in RECORD_FIELDS]
                        + [json.dumps(attrs, sort_keys=True, default=str), mid, src, dropped, rrow])
        c = self.conn
        c.execute("BEGIN")
        c.executemany(self.sql, rows)
        c.executemany("UPDATE spans SET mid = ?, mid_source = 'receipt_hash', receive_row = ? WHERE row = ?", updates)
        c.execute("COMMIT")
        return len(rows)

    def close(self) -> None:
        self.conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        self.conn.close()


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

    def __init__(self, db: str, store_bodies=(), log_path: Optional[str] = None):
        self.port = free_port()
        cmd = [sys.executable, "-m", "trailbeam.server", "--db", db, "--port", str(self.port),
               "--log-level", "WARNING"]
        if store_bodies:
            cmd += ["--store-bodies", ",".join(store_bodies)]
        self.log = open(log_path or os.devnull, "ab")
        self.proc = subprocess.Popen(cmd, stdout=self.log, stderr=self.log)
        self.base = f"http://127.0.0.1:{self.port}"
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            try:
                get(self.port, "/api/none", timeout=5)  # a 404 straight away: no table scan, unlike /api/health
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
    return {
        "messages": ["/api/messages"],
        "messages_one_queue": [f"/api/messages?queue={queue}"],
        "message_detail": [f"/api/messages/{quote(m)}" for m in mids],
        "duplicates": ["/api/duplicates"],
        "stuck": ["/api/stuck"],
        "tenants": ["/api/tenants"],
        "tenant_search": ["/api/tenants/search?q=tenant-00"],
        "body_search": ["/api/messages?body=" + quote("body.order.status:failed")],
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

def ingest_step(srv: Server, profile, rate: float, seconds: float, args) -> dict:
    cpu0, t0 = srv.cpu_s(), time.monotonic()
    res = loadgen.run(srv.otlp, profile, rate, seconds, concurrency=args.concurrency, processes=args.processes,
                      timeout=args.export_timeout, grace=args.export_timeout)
    wall = time.monotonic() - t0
    res["server_cpu_pct"] = round(100 * (srv.cpu_s() - cpu0) / wall, 1)
    res["server_rss_mb"] = srv.rss_mb()
    res["steady"] = bool(res["errors"] == 0 and res["achieved_spans_per_s"] >= 0.95 * res["target_spans_per_s"]
                         and (res["export_p99_ms"] or 0) <= args.max_export_p99_ms and res["behind_p99_s"] < 5)
    for k in ("profile", "processes", "concurrency"):
        res.pop(k, None)
    return res


def breakdown(db_path: str, profile, store_bodies, batches: int = 3) -> dict:
    """Where one export's time goes, in-process on the database file: decode, insert, and the join queries.
    Adds ``batches`` exports of spans to the file."""
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
    db._join_settle = timed("join_settle", db._join_settle)
    db._join_receive = timed("join_receive", db._join_receive)
    stream = loadgen.Stream(profile, seed=4242 + span_count(db_path))
    dec = ins = 0.0
    spans = 0
    plan = db.conn.execute(
        "EXPLAIN QUERY PLAN SELECT row, message_id FROM spans WHERE kind = 'receive' AND receipt_hmac = ? "
        "AND key_id = ? ORDER BY (time_ns <= ?) DESC, CASE WHEN time_ns <= ? THEN -time_ns ELSE time_ns END "
        "LIMIT 1", ("x", "y", 0, 0)).fetchall()
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
    return {"spans_per_export": spans // batches, "decode_ms": per(dec), "insert_ms": per(ins),
            "join_settle_ms": per(timers["join_settle"]), "join_receive_ms": per(timers["join_receive"]),
            "insert_spans_per_s": round(spans / ins, 1) if ins else None,
            "join_settle_plan": "; ".join(r[3] for r in plan)}


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
        pages = {k: v for k, v in page_paths(db_path, profile).items() if k not in skipped}
        if skipped:
            entry["pages_skipped"] = dict(skipped)
        box = {"srv": Server(db_path, store, str(out / "server.log"))}

        def restart() -> int:
            box["srv"].stop()
            box["srv"] = Server(db_path, store, str(out / "server.log"))
            return box["srv"].port

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
            for rate in [float(r) for r in args.rates.split(",")]:
                step = ingest_step(box["srv"], profile, rate, args.step_seconds, args)
                ramp.append(step)
                log(f"  ingest {rate:g} msg/s ({step['target_spans_per_s']:g} spans/s): achieved "
                    f"{step['achieved_spans_per_s']:g} spans/s, p99 {step['export_p99_ms']} ms, errors "
                    f"{step['errors']}, cpu {step['server_cpu_pct']}%{'' if step['steady'] else '  NOT STEADY'}")
                entry["ingest"] = ramp
                save()
                if not step["steady"]:
                    restart()  # drop the backlog of exports the server is still working through
                    break
            steady = [s for s in ramp if s["steady"]]
            best = steady[-1] if steady else None
            entry["max_steady_spans_per_s"] = best["achieved_spans_per_s"] if best else 0
            entry["max_steady_msgs_per_s"] = best["target_msgs_per_s"] if best else 0
            under_rate = best["target_msgs_per_s"] if best else ramp[0]["target_msgs_per_s"]
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
            if args.sdk_seconds and size == max(parse_size(s) for s in args.sizes.split(",")):
                sdk_rate = max(under_rate * 4, 5)
                entry["sdk"] = loadgen.run_sdk(box["srv"].otlp, profile, sdk_rate, args.sdk_seconds, settle_s=30)
                entry["sdk"].pop("profile", None)
                log(f"  SDK at {sdk_rate:g} msg/s: dropped {entry['sdk']['dropped_spans']} of "
                    f"{entry['sdk']['emitted_spans']} ({entry['sdk']['dropped_pct']}%)")
        finally:
            box["srv"].stop()
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
             f"tenants {results['profile']['tenants']}, body {results['profile']['body_size']} B.", ""]
    sizes = results["sizes"]
    if sizes:
        lines += ["## Ingest", "", "| DB spans | file MB | max steady spans/s (msg/s) | export p99 at that rate | "
                  "server CPU | first rate not steady |", "|---|---|---|---|---|---|"]
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
                            if bad else "-") + " |")
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
                  "| DB spans | decode ms | insert ms | of which join_settle ms | join_receive ms | spans/s | settle join plan |",
                  "|---|---|---|---|---|---|---|"]
        for e in sizes:
            b = e.get("breakdown")
            if b:
                lines.append(f"| {e['spans']:,} | {b['decode_ms']} | {b['insert_ms']} | {b['join_settle_ms']} | "
                             f"{b['join_receive_ms']} | {b['insert_spans_per_s']} | {b['join_settle_plan']} |")
        sdk = [e["sdk"] for e in sizes if e.get("sdk")]
        if sdk:
            s = sdk[-1]
            lines += ["", "## OTel SDK BatchSpanProcessor against the largest database", "",
                      f"{s['target_msgs_per_s']:g} msg/s ({s['target_spans_per_s']} spans/s): emitted {s['emitted_spans']}, "
                      f"received {s['received_spans']}, **dropped {s['dropped_spans']} ({s['dropped_pct']}%)**; "
                      f"{s['dropped_queue_full']} 'Queue full' drops, {s['failed_export_warnings']} failed-export warnings."]
    if results.get("growth"):
        lines += ["", "## File growth", "", "| case | MB per million spans |", "|---|---|"]
        lines += [f"| {g['case']} | {g['mb_per_million_spans']:,} |" for g in results["growth"]]
    return "\n".join(lines) + "\n"


def main(argv=None) -> None:
    p = argparse.ArgumentParser(description="Seed, ingest ramp and page timings for the Trailbeam server.")
    p.add_argument("--out", default="bench-results", help="directory for results.json, results.csv, summary.md")
    p.add_argument("--workdir", default=None, help="directory for the SQLite file (default: --out)")
    p.add_argument("--sizes", default="100k,1M", help="database sizes in spans, e.g. 100k,1M,10M,100M")
    p.add_argument("--rates", default="1,2,5,10,20,50,100,200,500,1000,2000",
                   help="ingest ramp, messages/s")
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
    p.add_argument("--sdk-seconds", type=float, default=30, help="0 skips the SDK drop measurement")
    p.add_argument("--fresh", action="store_true", help="delete the database file first")
    p.add_argument("--cleanup", action="store_true", help="delete the database file at the end")
    loadgen.add_profile_args(p)
    p.set_defaults(queues=50, tenants=50, body_size=1024, body_queues=5)
    args = p.parse_args(argv)
    run(args)


if __name__ == "__main__":
    main()
