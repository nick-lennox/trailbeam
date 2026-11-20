"""Tenants page latency on one store, before and after a change (QV-128).

Seeds harness traffic (50 queues, 50 tenants, 1 KB bodies on 5 queues, messages spread over the last 7 days)
into a SQLite file or a Postgres schema, then times the Tenants page (queries.tenants, what /api/tenants
answers) for the last hour, the last day (the page's default) and the last 7 days, ending at the newest span, so
two runs on the same data ask the same. Prints a table and writes JSON with each range's answer, so a run
with the old code and one with the new code on the same data can be compared:

    PYTHONPATH=<old checkout>/python/src python python/bench/tenants_page.py --db /tmp/t.sqlite --spans 1M --out before.json
    python python/bench/tenants_page.py --db /tmp/t.sqlite --out after.json --compare before.json

The second run opens the store with the new code, so it also migrates it (the time is reported). Counts must
be equal; p95 within the contract's bound (contract/tenants.md). ``--insert N`` then times N exports of live
traffic, one at a time, to see what keeping the rollup adds to ingest; it adds spans, so run it on a copy.
"""
from __future__ import annotations

import argparse
import json
import statistics
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import harness  # noqa: E402
import loadgen  # noqa: E402

from trailbeam.server import queries  # noqa: E402

PROFILE = loadgen.Profile(queues=50, tenants=50, body_size=1024, body_queues=5)
STORE_BODIES = loadgen.queue_names(PROFILE)[:5]
HOUR_MS = 3_600_000
RANGES = {"last hour": HOUR_MS, "last day": 24 * HOUR_MS, "last 7 days": 7 * 24 * HOUR_MS}


def open_store(args):
    t0 = time.monotonic()
    if args.store_url:
        import psycopg
        from psycopg.conninfo import make_conninfo

        from trailbeam.server.postgres import PostgresStore
        with psycopg.connect(args.store_url, autocommit=True) as c:
            c.execute(f"CREATE SCHEMA IF NOT EXISTS {args.schema}")
        s = PostgresStore(make_conninfo(args.store_url, options=f"-c search_path={args.schema}"),
                          store_bodies=STORE_BODIES)
    else:
        from trailbeam.server.db import Database
        s = Database(args.db, store_bodies=STORE_BODIES)
    return s, time.monotonic() - t0


def seed(s, args) -> dict:
    if args.store_url:
        import pg_smoke
        return pg_smoke.seed(s, args.spans, 7.0, 4)
    s.close()
    out = harness.seed(args.db, args.spans, PROFILE, store_bodies=STORE_BODIES)
    return out


def run(args) -> dict:
    s, open_s = open_store(args)
    migrated = list(getattr(s, "migrated", []) or [])
    seeded = None
    if args.spans:
        seeded = seed(s, args)
        if not args.store_url:
            s, _ = open_store(args)
    newest_ms = s.newest_messages(1)[0][0] // 1_000_000
    to_ms = newest_ms + 60_000
    out = {"store": "postgres" if args.store_url else "sqlite", "open_s": round(open_s, 2), "migrated": migrated,
           "seeded": seeded, "spans": sum(s.counts()["spans"].values()) if hasattr(s, "counts") else None,
           "ranges": {}}
    for name, width in RANGES.items():
        frm = to_ms - width
        queries.tenants(s, 0, 900, frm, to_ms)  # warm
        lat, ans = [], None
        for _ in range(args.repeats):
            t0 = time.perf_counter()
            ans = queries.tenants(s, 0, 900, frm, to_ms)
            lat.append((time.perf_counter() - t0) * 1000)
        lat.sort()
        p95 = lat[max(0, -(-95 * len(lat) // 100) - 1)]
        out["ranges"][name] = {"from_ms": frm, "to_ms": to_ms, "p50_ms": round(statistics.median(lat), 1),
                               "p95_ms": round(p95, 1), "max_ms": round(lat[-1], 1), "answer": ans}
        print(f"{out['store']:8} {name:12} p50 {statistics.median(lat):8.1f} ms  p95 {p95:8.1f} ms  "
              f"max {lat[-1]:8.1f} ms  tenants {len(ans['rows'])}  sends {ans['total_sent']:,}", flush=True)
    if args.insert:
        # live traffic after the pages: exports of ~512 spans, timed one at a time (what the rollup adds to ingest)
        stream, lat, spans = loadgen.Stream(PROFILE, seed=4242, worker=77), [], 0
        batches = stream.batches_of_messages(args.insert * 512 // int(PROFILE.mean_spans_per_message()))
        for batch in batches:
            recs = loadgen.records(batch)
            t0 = time.perf_counter()
            s.insert(recs)
            lat.append((time.perf_counter() - t0) * 1000)
            spans += len(recs)
        lat.sort()
        out["insert"] = {"exports": len(lat), "spans": spans, "p50_ms": round(statistics.median(lat), 1),
                         "p95_ms": round(lat[max(0, -(-95 * len(lat) // 100) - 1)], 1),
                         "spans_per_s": round(spans / (sum(lat) / 1000))}
        print(f"{out['store']:8} insert       {out['insert']}", flush=True)
    s.close()
    return out


def compare(after: dict, before: dict, bound: float = 0.01) -> list:
    """Differences in the answers: counts must be equal, p95 within ``bound`` of the old (exact) one."""
    problems = []
    for name, r in after["ranges"].items():
        a, b = r["answer"], before["ranges"][name]["answer"]
        for k in ("total_sent", "untagged_sent"):
            if a[k] != b[k]:
                problems.append(f"{name}: {k} {a[k]} != {b[k]}")
        rows_a = {x["tenant"]: x for x in a["rows"]}
        rows_b = {x["tenant"]: x for x in b["rows"]}
        if rows_a.keys() != rows_b.keys():
            problems.append(f"{name}: tenants differ")
            continue
        for t, x in rows_a.items():
            y = rows_b[t]
            for k in ("sent", "share", "first_deliveries", "dead_lettered"):
                if x[k] != y[k]:
                    problems.append(f"{name} {t}: {k} {x[k]} != {y[k]}")
            p, q = x["p95_in_queue_ms"], y["p95_in_queue_ms"]
            if (p is None) != (q is None) or (q is not None and abs(p - q) > bound * q):
                problems.append(f"{name} {t}: p95 {p} vs {q}")
    return problems


def main(argv=None) -> None:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--db", help="SQLite file")
    p.add_argument("--store-url", help="Postgres URL (then --schema)")
    p.add_argument("--schema", default="tenants_bench")
    p.add_argument("--spans", type=harness.parse_size, default=0, help="seed to this many spans first (e.g. 1M)")
    p.add_argument("--repeats", type=int, default=30)
    p.add_argument("--insert", type=int, default=0, help="then time this many exports of ~512 spans (changes the data)")
    p.add_argument("--out", required=True)
    p.add_argument("--compare", help="a previous run's JSON: check the answers match")
    args = p.parse_args(argv)
    if bool(args.db) == bool(args.store_url):
        p.error("give one of --db and --store-url")
    out = run(args)
    if args.compare:
        out["compare"] = compare(out, json.loads(Path(args.compare).read_text()))
        print("answers match" if not out["compare"] else "\n".join(out["compare"]))
    Path(args.out).write_text(json.dumps(out, indent=1, default=str) + "\n")


if __name__ == "__main__":
    main()
