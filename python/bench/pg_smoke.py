"""A short smoke run of the Postgres store (QV-80): seed a database to a size, then run ingest at a rate through
the server (``--store-url``) while the list pages are timed. Prints and saves export p50/p99 and page p50/p95.

    python python/bench/pg_smoke.py --store-url postgresql://postgres@127.0.0.1:5432/bench --spans 1M \
        --spans-per-s 1000 --seconds 120 --out bench-results/pg-smoke

Same traffic as harness.py (50 queues, 50 tenants, 1 KB bodies on 5 queues, messages spread over the last
``--seed-days``). Seeding goes through PostgresStore.insert in this process (``--threads`` exporters at once),
so the seeded rows are what the server would have stored; it resumes where a previous run stopped, so a seed
that is too long for one go can be done in parts. The full medium-tier measurement is QV-82's.
"""
from __future__ import annotations

import argparse
import concurrent.futures
import json
import os
import random
import sys
import time
from pathlib import Path
from urllib.parse import quote

sys.path.insert(0, str(Path(__file__).resolve().parent))
import harness  # noqa: E402
import loadgen  # noqa: E402

from trailbeam.server.postgres import PostgresStore  # noqa: E402
from trailbeam.server.retention import Retention  # noqa: E402

DAY_NS = 86_400 * 10 ** 9
PROFILE = loadgen.Profile(queues=50, tenants=50, body_size=1024, body_queues=5)
RETENTION = Retention(None, {})  # the server runs with --retention forever: the seeded week must stay


def stored_spans(store: PostgresStore) -> int:
    return sum(store.counts()["spans"].values())


def seed(store: PostgresStore, target: int, days: float, threads: int, log=print) -> dict:
    have = stored_spans(store)
    if have >= target:
        return {"added": 0, "seconds": 0.0, "spans": have}
    spm = PROFILE.mean_spans_per_message()
    n_msgs = int((target - have) / spm * 1.03) + 20
    now = time.time_ns()
    start, end = now - int(days * DAY_NS), now - 3_600 * 10 ** 9
    step = (end - start) / n_msgs
    i = [0]

    def clock() -> int:
        i[0] += 1
        return start + int(i[0] * step)

    stream = loadgen.Stream(PROFILE, seed=7 * 1_000_003 + have, worker=900 + have % 97, clock=clock)
    t0, added, last = time.monotonic(), 0, 0
    with concurrent.futures.ThreadPoolExecutor(threads) as pool:
        pending = set()
        for batch in stream.batches_of_messages(n_msgs):
            pending.add(pool.submit(store.insert, loadgen.records(batch)))
            if len(pending) >= threads * 2:
                done, pending = concurrent.futures.wait(pending, return_when=concurrent.futures.FIRST_COMPLETED)
                added += sum(f.result()[0] for f in done)
            if added - last >= 100_000:
                last = added
                log(f"  seeded {have + added:,} spans ({added / (time.monotonic() - t0):,.0f}/s)")
            if have + added >= target and not stream.pending and not stream.held:
                break
        added += sum(f.result()[0] for f in concurrent.futures.as_completed(pending))
    return {"added": added, "seconds": round(time.monotonic() - t0, 1), "spans": have + added}


def page_paths(store: PostgresStore, n_messages: int = 20) -> dict:
    mids = [m for m in store.recent_messages(5000)]
    rng = random.Random(3)
    picks = rng.sample(mids, min(n_messages, len(mids)))
    queue = loadgen.queue_names(PROFILE)[3]
    body_queue = loadgen.body_queue_names(PROFILE)[0]
    return {
        "messages": ["/api/messages"],
        "messages_one_queue": [f"/api/messages?queue={queue}"],
        "message_detail": [f"/api/messages/{quote(m)}" for m in picks],
        "duplicates": ["/api/duplicates"],
        "stuck": ["/api/stuck"],
        "tenants": ["/api/tenants"],
        "tenant_search": ["/api/tenants/search?q=tenant-00"],
        "body_search": ["/api/messages?body=" + quote("body.order.status:failed")],
        "body_search_one_queue": [f"/api/messages?queue={body_queue}&body=" + quote("body.order.status:failed")],
    }


def main(argv=None) -> None:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--store-url", required=True)
    p.add_argument("--spans", default="1M")
    p.add_argument("--spans-per-s", type=float, default=1000)
    p.add_argument("--seconds", type=float, default=120)
    p.add_argument("--seed-days", type=float, default=7)
    p.add_argument("--threads", type=int, default=4, help="concurrent inserts while seeding")
    p.add_argument("--seed-only", action="store_true", help="seed (or resume seeding), then stop")
    p.add_argument("--no-page-load", action="store_true", help="ingest alone, no pages timed during it")
    p.add_argument("--out", default="bench-results/pg-smoke")
    p.add_argument("--concurrency", type=int, default=4)
    p.add_argument("--processes", type=int, default=2)
    p.add_argument("--export-timeout", type=float, default=10)
    p.add_argument("--max-export-p99-ms", type=float, default=2000)
    a = p.parse_args(argv)
    out = Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    bodies = tuple(loadgen.body_queue_names(PROFILE))
    target = harness.parse_size(a.spans)

    store = PostgresStore(a.store_url, store_bodies=bodies, retention=RETENTION)
    sd = seed(store, target, a.seed_days, a.threads)
    print(f"seeded: {sd}", flush=True)
    if a.seed_only:
        store.close()
        return
    pages = page_paths(store)
    with store.reads.connection() as c:
        db_mb = round(c.execute("SELECT pg_database_size(current_database()) AS b").fetchone()["b"] / 1e6, 1)
        version = c.execute("SHOW server_version").fetchone()["server_version"]
        settings = {r["name"]: r["setting"] for r in c.execute(
            "SELECT name, setting FROM pg_settings WHERE name IN ('fsync', 'synchronous_commit', 'shared_buffers')")}
    store.close()

    srv = harness.Server("unused.sqlite", bodies, str(out / "server.log"),
                         args=("--retention", "forever", "--store-url", a.store_url))
    try:
        idle = {} if a.no_page_load else harness.time_pages(srv.port, pages, repeats=10, budget_s=30, timeout_s=60)
        loop = harness.PageLoop(srv.port, {} if a.no_page_load else pages, 60)
        loop.start()
        rate = a.spans_per_s / PROFILE.mean_spans_per_message()
        ingest = harness.ingest_step(srv, PROFILE, rate, a.seconds, a)
        loop.halt.set()
        loop.join()
        under = loop.result()
    finally:
        srv.stop()
    result = {"machine": harness.machine(), "postgres": version, "postgres_settings": settings,
              "seed": sd, "database_mb": db_mb, "ingest": ingest, "pages_idle": idle, "pages_under_ingest": under,
              "args": vars(a) | {"store_url": "(redacted)"}}
    (out / "results.json").write_text(json.dumps(result, indent=2))
    print(json.dumps({"ingest": {k: ingest.get(k) for k in ("target_spans_per_s", "achieved_spans_per_s",
                                                            "export_p50_ms", "export_p99_ms", "errors", "steady",
                                                            "server_cpu_pct")},
                      "pages_under_ingest": {k: (v["p50_ms"], v["p95_ms"], v["n"], v["timeouts"])
                                             for k, v in under.items()},
                      "pages_idle_p95": {k: v.get("p95_ms") for k, v in idle.items()},
                      "database_mb": db_mb, "postgres": version, "settings": settings}, indent=1))


if __name__ == "__main__":
    os.environ.setdefault("PYTHONUNBUFFERED", "1")
    main()
