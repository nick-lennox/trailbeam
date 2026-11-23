"""Medium-tier run of the Postgres store (QV-82): several Trailbeam replicas on one Postgres, ingest stepped up
to a target, the pages timed under it, and retention dropping partitions while ingest runs.

    python python/bench/pg_medium.py seed --store-url postgresql://postgres@127.0.0.1:5432/bench --spans 10M
    python python/bench/pg_medium.py ramp --store-url … --spans-per-s 2000,5000,10000 --step-seconds 120
    python python/bench/pg_medium.py pages --store-url … --spans-per-s 10000 --seconds 300
    python python/bench/pg_medium.py retention --store-url … --spans-per-s 10000 --retention 6d

Seeding is pg_smoke.py's (PostgresStore.insert in this process, resumable, the harness's traffic: 50 queues, 50
tenants, 1 KB bodies on 5 queues, spread over the last ``--seed-days``). Every subcommand starts ``--replicas``
servers (``python -m trailbeam.server --store-url``) and sends each an equal share of the rate from its own load
generator, as a load balancer would; the export latencies of all of them are pooled. Pages go to the first
replica. Each subcommand appends its result to ``--out``/results.jsonl.
"""
from __future__ import annotations

import argparse
import concurrent.futures
import json
import os
import sys
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import harness  # noqa: E402
import loadgen  # noqa: E402
import pg_smoke  # noqa: E402

from trailbeam.server.postgres import PostgresStore  # noqa: E402
from trailbeam.server.retention import Retention  # noqa: E402

PROFILE = pg_smoke.PROFILE
BODIES = tuple(loadgen.body_queue_names(PROFILE))


def db_state(url: str, exact: bool = False) -> dict:
    """``spans`` is the planner's estimate unless ``exact`` or PG_MEDIUM_EXACT (a count takes seconds at 7M)."""
    import psycopg
    with psycopg.connect(url, autocommit=True) as c:
        one = lambda q: c.execute(q).fetchone()[0]  # noqa: E731
        return {
            "spans": one("SELECT count(*) FROM spans") if exact or os.environ.get("PG_MEDIUM_EXACT") else
            int(one("SELECT coalesce(sum(reltuples), 0) FROM pg_class c JOIN pg_inherits i ON i.inhrelid = c.oid "
                    "WHERE i.inhparent = 'spans'::regclass")),
            "database_gb": round(one("SELECT pg_database_size(current_database())") / 1e9, 2),
            "spans_partitions": one("SELECT count(*) FROM pg_inherits WHERE inhparent = 'spans'::regclass"),
            "postgres": one("SHOW server_version"),
            "settings": {k: one(f"SHOW {k}") for k in ("shared_buffers", "fsync", "synchronous_commit",
                                                         "max_wal_size", "effective_cache_size")},
        }


class Replicas:
    def __init__(self, url: str, n: int, out: Path, extra=("--retention", "forever")):
        self.servers = []
        try:
            for i in range(n):
                self.servers.append(harness.Server("unused.sqlite", BODIES, str(out / f"server-{i}.log"),
                                                   args=(*extra, "--store-url", url)))
        except Exception:
            self.stop()
            raise

    def cpu(self) -> list:
        return [s.cpu_s() for s in self.servers]

    def stop(self) -> None:
        for s in self.servers:
            s.stop()


def ingest(reps: Replicas, spans_per_s: float, seconds: float, a) -> dict:
    """Every replica gets spans_per_s / n from its own generator; latencies pooled."""
    n = len(reps.servers)
    rate = spans_per_s / PROFILE.mean_spans_per_message() / n
    cpu0, t0 = reps.cpu(), time.monotonic()
    with concurrent.futures.ThreadPoolExecutor(n) as pool:
        parts = list(pool.map(lambda s: loadgen.run(s.otlp, PROFILE, rate, seconds, concurrency=a.concurrency,
                                                    processes=a.processes, timeout=a.export_timeout,
                                                    grace=a.export_timeout, keep_latencies=True), reps.servers))
    wall = time.monotonic() - t0
    lat = [x for p in parts for x in p.pop("latencies_s")]
    statuses: dict = {}
    for p in parts:
        for k, v in p["statuses"].items():
            statuses[k] = statuses.get(k, 0) + v
    res = {
        "target_spans_per_s": round(sum(p["target_spans_per_s"] for p in parts), 1),
        "achieved_spans_per_s": round(sum(p["achieved_spans_per_s"] for p in parts), 1),
        "exports": sum(p["exports"] for p in parts), "errors": sum(p["errors"] for p in parts),
        "statuses": statuses,
        "export_p50_ms": loadgen._ms(loadgen.percentile(lat, 0.5)),
        "export_p99_ms": loadgen._ms(loadgen.percentile(lat, 0.99)),
        "export_max_ms": loadgen._ms(max(lat) if lat else None),
        "behind_p99_s": max(p["behind_p99_s"] for p in parts),
        "server_cpu_pct": [round(100 * (c1 - c0) / wall, 1) for c0, c1 in zip(cpu0, reps.cpu())],
        "server_rss_mb": [s.rss_mb() for s in reps.servers],
        "per_replica": [{k: p[k] for k in ("achieved_spans_per_s", "export_p50_ms", "export_p99_ms", "errors")}
                        for p in parts],
    }
    res["steady"] = bool(res["errors"] == 0 and res["achieved_spans_per_s"] >= 0.95 * res["target_spans_per_s"]
                         and (res["export_p99_ms"] or 0) <= a.max_export_p99_ms and res["behind_p99_s"] < 5)
    return res


def retention_of(text: str) -> Retention:
    """--seed-retention / --server-retention: the seed must use the servers' retention, or their new spans of a
    seeded message land in another retention class (another partition) and move rows."""
    return Retention.parse(text)


def server_args(a) -> tuple:
    return ("--retention", a.server_retention)


def pages_for(url: str, retention: Retention) -> dict:
    store = PostgresStore(url, store_bodies=BODIES, retention=retention)
    try:
        return pg_smoke.page_paths(store)
    finally:
        store.close()


def save(a, kind: str, result: dict) -> None:
    out = Path(a.out)
    rec = {"kind": kind, "at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "machine": harness.machine(),
           "args": {k: v for k, v in vars(a).items() if k != "store_url"}, **result}
    with open(out / "results.jsonl", "a") as f:
        f.write(json.dumps(rec) + "\n")
    print(json.dumps(result, indent=1), flush=True)


def _seed_slice(args: dict) -> dict:
    """One seeding process: ``spans`` more spans, messages spread evenly over [start_ns, end_ns)."""
    store = PostgresStore(args["url"], store_bodies=BODIES, retention=retention_of(args["retention"]),
                          ingest_connections=args["threads"])
    spm = PROFILE.mean_spans_per_message()
    n_msgs = int(args["spans"] / spm * 1.03) + 20
    step = (args["end_ns"] - args["start_ns"]) / n_msgs
    i = [0]

    def clock() -> int:
        i[0] += 1
        return args["start_ns"] + int(i[0] * step)

    stream = loadgen.Stream(PROFILE, seed=args["seed"], worker=args["worker"], clock=clock)
    t0, added, last = time.monotonic(), 0, 0
    try:
        with concurrent.futures.ThreadPoolExecutor(args["threads"]) as pool:
            pending = set()
            for batch in stream.batches_of_messages(n_msgs):
                pending.add(pool.submit(store.insert, loadgen.records(batch)))
                if len(pending) >= args["threads"] * 2:
                    done, pending = concurrent.futures.wait(pending, return_when=concurrent.futures.FIRST_COMPLETED)
                    added += sum(f.result()[0] for f in done)
                if added - last >= 500_000:
                    last = added
                    print(f"  [{args['worker']}] +{added:,} ({added / (time.monotonic() - t0):,.0f}/s)", flush=True)
                if added >= args["spans"] and not stream.pending and not stream.held:
                    break
            added += sum(f.result()[0] for f in concurrent.futures.as_completed(pending))
    finally:
        store.close()
    return {"added": added, "seconds": round(time.monotonic() - t0, 1)}


def cmd_seed(a) -> None:
    """``--seed-procs`` processes, each a slice of the window. Resumable: it adds what is missing, its slices
    over the whole window again (a resumed seed is denser in time, not shifted)."""
    import multiprocessing
    # counted, not estimated: right after a seed nothing has analyzed the new partitions, and the estimate was far
    # below the spans stored, so a resumed seed overshot (QV-180: asked for 4M, got 5.5M)
    have = db_state(a.store_url, exact=True)["spans"] if _has_schema(a.store_url) else 0
    target = harness.parse_size(a.spans)
    if a.add:  # --spans is what to add, not the size to reach
        target += have
    if have >= target:
        save(a, "seed", {"seed": {"added": 0, "spans": have}, "db": db_state(a.store_url)})
        return
    PostgresStore(a.store_url, retention=retention_of(a.seed_retention)).close()  # the schema, before the race
    now = time.time_ns()
    end = now - int(a.seed_end_ago_s * 10 ** 9)
    start = end - int(a.seed_days * pg_smoke.DAY_NS)
    k = a.seed_procs
    width = (end - start) // k
    jobs = [{"url": a.store_url, "threads": a.threads, "retention": a.seed_retention, "spans": (target - have) // k + 1,
             "start_ns": start + j * width, "end_ns": start + (j + 1) * width,
             "seed": 7 * 1_000_003 + have + j, "worker": 900 + (have + j) % 97} for j in range(k)]
    t0 = time.monotonic()
    with multiprocessing.get_context("spawn").Pool(k) as pool:
        parts = pool.map(_seed_slice, jobs)
    secs = time.monotonic() - t0
    added = sum(p["added"] for p in parts)
    save(a, "seed", {"seed": {"added": added, "seconds": round(secs, 1), "spans_per_s": round(added / secs),
                              "procs": k, "threads": a.threads}, "db": db_state(a.store_url)})


def _has_schema(url: str) -> bool:
    import psycopg
    with psycopg.connect(url, autocommit=True) as c:
        return c.execute("SELECT to_regclass('spans') IS NOT NULL").fetchone()[0]


def cmd_ramp(a) -> None:
    before = db_state(a.store_url)
    steps = []
    reps = Replicas(a.store_url, a.replicas, Path(a.out), extra=server_args(a))
    try:
        for r in [float(x) for x in a.spans_per_s.split(",")]:
            s = ingest(reps, r, a.step_seconds, a)
            print(f"  {r:,.0f} spans/s: achieved {s['achieved_spans_per_s']:,.0f}, p50 {s['export_p50_ms']} "
                  f"p99 {s['export_p99_ms']} ms, errors {s['errors']}, cpu {s['server_cpu_pct']}", flush=True)
            steps.append(s)
            if not s["steady"]:
                break
            time.sleep(a.pause)
    finally:
        reps.stop()
    save(a, "ramp", {"db_before": before, "db_after": db_state(a.store_url), "replicas": a.replicas, "steps": steps})


def cmd_pages(a) -> None:
    before = db_state(a.store_url)
    pages = pages_for(a.store_url, retention_of(a.server_retention))
    reps = Replicas(a.store_url, a.replicas, Path(a.out), extra=server_args(a))
    try:
        port = reps.servers[0].port
        idle = harness.time_pages(port, pages, repeats=a.page_repeats, budget_s=a.page_budget_s, timeout_s=60)
        loop = harness.PageLoop(port, pages, 60)
        loop.start()
        ing = ingest(reps, float(a.spans_per_s), a.seconds, a)
        loop.halt.set()
        loop.join()
        under = loop.result()
    finally:
        reps.stop()
    save(a, "pages", {"db_before": before, "pages_idle": idle, "ingest": ing, "pages_under_ingest": under})


def cmd_retention(a) -> None:
    """Servers run with --retention a.retention: the oldest seeded days are past it, so the replica holding the
    retention lease drops their partitions while ingest runs."""
    before = db_state(a.store_url)
    # Hold the retention lease while the replicas start (their first pass runs at start and does nothing without
    # it), and let go --hold-lease-s into the ingest: the drop then happens under load, not before it.
    holder = PostgresStore(a.store_url, retention=retention_of(a.retention))
    if not holder.holds_retention_lease():
        holder.close()
        raise SystemExit("another process holds the retention lease")
    reps = Replicas(a.store_url, a.replicas, Path(a.out),
                    extra=("--retention", a.retention, "--purge-interval", a.purge_interval))
    released = threading.Timer(a.hold_lease_s, holder.close)
    samples = []
    halt = threading.Event()

    def watch():
        while not halt.wait(5):
            try:
                samples.append({"t": round(time.monotonic() - t0, 1), **{k: v for k, v in db_state(a.store_url).items()
                                                                       if k in ("spans_partitions", "database_gb")}})
            except Exception as e:  # noqa: BLE001
                samples.append({"error": str(e)[:200]})

    t0 = time.monotonic()
    w = threading.Thread(target=watch, daemon=True)
    w.start()
    released.start()
    try:
        ing = ingest(reps, float(a.spans_per_s), a.seconds, a)
    finally:
        released.cancel()
        holder.close()
        halt.set()
        w.join()
        reps.stop()
    save(a, "retention", {"db_before": before, "db_after": db_state(a.store_url), "ingest": ing,
                          "lease_released_at_s": a.hold_lease_s,
                          "partitions_over_time": samples})


def main(argv=None) -> None:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("command", choices=("seed", "ramp", "pages", "retention", "state"))
    p.add_argument("--store-url", required=True)
    p.add_argument("--out", default="bench-results/pg-medium")
    p.add_argument("--replicas", type=int, default=2)
    p.add_argument("--spans", default="1M", help="seed: database size to reach")
    p.add_argument("--seed-days", type=float, default=7)
    p.add_argument("--seed-end-ago-s", type=float, default=3600, help="seed: the window ends this long ago")
    p.add_argument("--add", action="store_true", help="seed: --spans is what to add, not the size to reach")
    p.add_argument("--seed-retention", default="forever",
                   help="seed: the retention the seed's spans are placed by; use the servers' (--server-retention)")
    p.add_argument("--server-retention", default="forever", help="ramp, pages: the servers' --retention")
    p.add_argument("--hold-lease-s", type=float, default=60,
                   help="retention: seconds into the ingest before the replicas may take the retention lease")
    p.add_argument("--threads", type=int, default=4, help="seed: concurrent inserts per process")
    p.add_argument("--seed-procs", type=int, default=6, help="seed: processes")
    p.add_argument("--spans-per-s", default="10000", help="ramp: comma-separated steps")
    p.add_argument("--step-seconds", type=float, default=120)
    p.add_argument("--pause", type=float, default=10, help="ramp: seconds between steps")
    p.add_argument("--seconds", type=float, default=300, help="pages, retention: ingest duration")
    p.add_argument("--page-repeats", type=int, default=20)
    p.add_argument("--page-budget-s", type=float, default=60)
    p.add_argument("--retention", default="6d")
    p.add_argument("--purge-interval", default="5")
    p.add_argument("--concurrency", type=int, default=4, help="concurrent exports per generator process")
    p.add_argument("--processes", type=int, default=2, help="generator processes per replica")
    p.add_argument("--export-timeout", type=float, default=10)
    p.add_argument("--max-export-p99-ms", type=float, default=2000)
    a = p.parse_args(argv)
    Path(a.out).mkdir(parents=True, exist_ok=True)
    if a.command == "state":
        print(json.dumps(db_state(a.store_url), indent=1))
        return
    {"seed": cmd_seed, "ramp": cmd_ramp, "pages": cmd_pages, "retention": cmd_retention}[a.command](a)


if __name__ == "__main__":
    os.environ.setdefault("PYTHONUNBUFFERED", "1")
    main()
