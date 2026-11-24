"""Queue page latency on one store (QV-363): ``/api/flows`` and ``/api/queues/{queue}/series``.

Seeds the traffic tenants_page.py seeds (harness traffic: 50 queues, 50 tenants, 1 KB bodies on 5 queues,
messages spread over the last 7 days) into a SQLite file or a Postgres schema, then times the two endpoints
in-process (``App.api``: the store reads and the JSON-ready answer, not HTTP) for the last hour, the last day
(the target: under 300 ms) and the last 7 days, ending at the newest span. The series is the busiest queue's
(by name, as the page asks), at the page's default step, with and without ``split=version``. Prints a table and
writes JSON:

    python python/bench/queue_page.py --db /tmp/q.sqlite --spans 1M --out sqlite.json
    python python/bench/queue_page.py --store-url postgresql://postgres@127.0.0.1:5432/postgres --schema qp \\
        --spans 1M --out pg.json

The seed is kept, so a second run on the same file or schema reuses it.
"""
from __future__ import annotations

import argparse
import json
import statistics
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import tenants_page  # noqa: E402  (the same store, seed and traffic)

from trailbeam.server.server import App  # noqa: E402

RANGES = tenants_page.RANGES


def _time(call, repeats: int) -> tuple[dict, object]:
    call()  # warm
    lat, ans = [], None
    for _ in range(repeats):
        t0 = time.perf_counter()
        ans = call()
        lat.append((time.perf_counter() - t0) * 1000)
    lat.sort()
    p95 = lat[max(0, -(-95 * len(lat) // 100) - 1)]
    return {"p50_ms": round(statistics.median(lat), 1), "p95_ms": round(p95, 1), "max_ms": round(lat[-1], 1)}, ans


def run(args) -> dict:
    s, open_s = tenants_page.open_store(args)
    migrated = list(getattr(s, "migrated", []) or [])
    seeded = None
    if args.spans:
        seeded = tenants_page.seed(s, args)
        if not args.store_url:
            s, _ = tenants_page.open_store(args)
    app = App(s, max_receive_counts={})
    to_ms = s.newest_messages(1)[0][0] // 1_000_000 + 60_000
    store = "postgres" if args.store_url else "sqlite"
    out = {"store": store, "open_s": round(open_s, 2), "migrated": migrated, "seeded": seeded,
           "spans": sum(s.counts()["spans"].values()), "ranges": {}}
    for name, width in RANGES.items():
        q = {"from_ms": str(to_ms - width), "to_ms": str(to_ms)}
        flows_t, flows = _time(lambda: app.api("flows", q), args.repeats)
        busiest = flows["flows"][0]["name"]
        series_t, series = _time(lambda: app.api(f"queues/{busiest}/series", q), args.repeats)
        split_t, _ = _time(lambda: app.api(f"queues/{busiest}/series", {**q, "split": "version"}), args.repeats)
        out["ranges"][name] = {
            "from_ms": to_ms - width, "to_ms": to_ms, "queues": len(flows["flows"]), "queue": busiest,
            "sends": sum(f["sent"] for f in flows["flows"]), "buckets": len(series["series"]),
            "step_s": series["step_s"], "flows": flows_t, "series": series_t, "series_by_version": split_t}
        r = out["ranges"][name]
        print(f"{store:8} {name:12} flows p50 {flows_t['p50_ms']:7.1f} p95 {flows_t['p95_ms']:7.1f} ms | "
              f"series p50 {series_t['p50_ms']:7.1f} p95 {series_t['p95_ms']:7.1f} ms | "
              f"by version p95 {split_t['p95_ms']:7.1f} ms | {r['queues']} queues, {r['sends']:,} sends, "
              f"{busiest}: {r['buckets']} buckets of {r['step_s']} s", flush=True)
    s.close()
    return out


def main(argv=None) -> None:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--db", help="SQLite file")
    p.add_argument("--store-url", help="Postgres URL (then --schema)")
    p.add_argument("--schema", default="queue_page_bench")
    p.add_argument("--spans", type=tenants_page.harness.parse_size, default=0,
                   help="seed to this many spans first (e.g. 1M)")
    p.add_argument("--repeats", type=int, default=20)
    p.add_argument("--out", required=True)
    args = p.parse_args(argv)
    if bool(args.db) == bool(args.store_url):
        p.error("give one of --db and --store-url")
    Path(args.out).write_text(json.dumps(run(args), indent=1, default=str) + "\n")


if __name__ == "__main__":
    main()
