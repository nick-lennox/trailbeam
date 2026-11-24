"""/api/events latency on one store (QV-394).

Seeds harness traffic (50 queues, 50 tenants, 1 KB bodies on 5 queues, messages spread over the last 7 days, as
tenants_page.py) into a SQLite file or a Postgres schema, then times events.page, what /api/events answers, for
the last day and the last 7 days, ending at the newest span: the first page (rows and histogram) unfiltered, by
queue, by service, by kind, by message, and the next page after a cursor. Prints a table and writes JSON:

    python python/bench/events_page.py --db /tmp/e.sqlite --spans 1M --out events.json
    python python/bench/events_page.py --store-url postgresql://... --schema bench --spans 1M --out events-pg.json

``--insert N`` then times N exports of live traffic, one at a time, to compare ingest with and without a change.
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
import tenants_page  # noqa: E402

from trailbeam.server import events  # noqa: E402

HOUR_MS = 3_600_000
RANGES = {"last day": 24 * HOUR_MS, "last 7 days": 7 * 24 * HOUR_MS}


def queries_for(s, frm: int, to: int) -> dict:
    """The queries timed in each range. The queue, service and message are the first page's newest row's, so they
    exist; a queue is one of 50, a service one of the profile's few."""
    top = events.page(s, frm, to, 1)["events"][0]
    first = events.page(s, frm, to, 100)
    return {
        "first page": {},
        "by queue": {"queue": top["queue"]},
        "by service": {"service": top["service_name"]},
        "deletes and visibility changes": {"kinds": ("delete", "change_visibility")},
        "Lambda outcomes": {"kinds": ("outcome",)},
        "by message": {"message_id": top["message_id"]},
        "next page": {"cursor": first["next_cursor"]},
    }


def run(args) -> dict:
    s, open_s = tenants_page.open_store(args)
    seeded = None
    if args.spans:
        seeded = tenants_page.seed(s, args)
        if not args.store_url:
            s, _ = tenants_page.open_store(args)
    newest_ms = s.newest_messages(1)[0][0] // 1_000_000
    to_ms = newest_ms + 60_000
    out = {"store": "postgres" if args.store_url else "sqlite", "open_s": round(open_s, 2), "seeded": seeded,
           "spans": sum(s.counts()["spans"].values()), "ranges": {}}
    print(f"{out['store']}: {out['spans']:,} spans", flush=True)
    for name, width in RANGES.items():
        frm = to_ms - width
        res = out["ranges"][name] = {"from_ms": frm, "to_ms": to_ms, "queries": {}}
        for qname, kw in queries_for(s, frm, to_ms).items():
            events.page(s, frm, to_ms, 100, **kw)  # warm
            lat, ans = [], None
            for _ in range(args.repeats):
                t0 = time.perf_counter()
                ans = events.page(s, frm, to_ms, 100, **kw)
                lat.append((time.perf_counter() - t0) * 1000)
            lat.sort()
            p95 = lat[max(0, -(-95 * len(lat) // 100) - 1)]
            total = ans["histogram"]["total"] if ans["histogram"] else None
            res["queries"][qname] = {"params": {k: list(v) if isinstance(v, tuple) else v for k, v in kw.items()},
                                     "p50_ms": round(statistics.median(lat), 1), "p95_ms": round(p95, 1),
                                     "max_ms": round(lat[-1], 1), "rows": len(ans["events"]), "total": total}
            print(f"{out['store']:8} {name:12} {qname:32} p50 {statistics.median(lat):8.1f} ms  p95 {p95:8.1f} ms  "
                  f"rows {len(ans['events']):4}  steps {total if total is not None else '-':>10}", flush=True)
    if args.insert:
        stream, lat, spans = loadgen.Stream(tenants_page.PROFILE, seed=4242, worker=77), [], 0
        for batch in stream.batches_of_messages(args.insert * 512 // int(tenants_page.PROFILE.mean_spans_per_message())):
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


def main(argv=None) -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--db", default="/tmp/trailbeam-events-bench.sqlite")
    ap.add_argument("--store-url", help="a Postgres URL: bench a PostgresStore in --schema instead of SQLite")
    ap.add_argument("--schema", default="events_bench")
    ap.add_argument("--spans", type=harness.parse_size, default=0, help="seed the store to this many spans first")
    ap.add_argument("--repeats", type=int, default=20)
    ap.add_argument("--insert", type=int, default=0, help="then time this many ~512-span exports")
    ap.add_argument("--out", help="write the results as JSON here")
    args = ap.parse_args(argv)
    out = run(args)
    if args.out:
        Path(args.out).write_text(json.dumps(out, indent=1) + "\n")


if __name__ == "__main__":
    main()
