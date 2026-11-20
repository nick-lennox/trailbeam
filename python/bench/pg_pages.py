"""Page latency and locks on a Postgres store with many places (QV-116).

Seeds harness traffic (pg_smoke.py's: 50 queues, 50 tenants, bodies on 5 queues) over ``--days`` into a schema
of its own, with buckets narrow enough to make ``--places`` places, then times every store call a page makes
that walks across all of time, and counts the lock entries it holds at once (each statement run in a
transaction of its own that lists the connection's locks before it ends). Prints a table and writes JSON with
a digest of each call's answer, so two runs on one database (before and after a change) can be checked to
answer the same:

    python python/bench/pg_pages.py --store-url postgresql://postgres@127.0.0.1:5432/postgres \
        --schema pages400 --places 400 --spans 200k --out before.json
    python python/bench/pg_pages.py ... --schema pages400 --places 400 --out after.json --compare before.json

The seed is kept in the schema, so the second run reuses it.
"""
from __future__ import annotations

import argparse
import contextlib
import hashlib
import json
import statistics
import sys
import time
from pathlib import Path

import psycopg
from psycopg.conninfo import make_conninfo

sys.path.insert(0, str(Path(__file__).resolve().parent))
import harness  # noqa: E402
import loadgen  # noqa: E402
import pg_smoke  # noqa: E402

from trailbeam.server.postgres import PostgresStore  # noqa: E402

DAY_NS = 86_400 * 10 ** 9


def calls(s: PostgresStore) -> dict:
    """name -> a call, fixed by the seeded data (not by the clock), so two runs ask the same."""
    newest = s.newest_messages(1)[0][0]
    first = s.newest_messages(50)
    mid_way = s.newest_messages(1, to_ns=newest - (newest - _oldest(s)) // 2)[0]
    queue, tenant = loadgen.queue_names(pg_smoke.PROFILE)[3], "tenant-007"
    return {
        "messages": lambda: s.recent_messages(51),
        "messages_page_2": lambda: s.recent_messages(51, after=first[-1]),
        "messages_half_way_back": lambda: s.recent_messages(51, after=mid_way),
        "messages_queue": lambda: s.recent_messages(51, queue),
        "messages_tenant": lambda: s.recent_messages(51, tenant=tenant),
        "messages_last_hour": lambda: s.recent_messages(51, from_ns=newest - 3_600 * 10 ** 9),
        "body_search_chunk_half_way_back": lambda: s.newest_messages(500, after=mid_way),
        "duplicates": lambda: s.duplicate_messages(51),
        "stuck": lambda: s.stuck_messages(newest - 600 * 10 ** 9, 51),
        "stuck_queue": lambda: s.stuck_messages(newest - 600 * 10 ** 9, 51, queue),
        "queue_messages": lambda: sorted(s.queue_messages(queue)),
        "tenant_activity_last_day": lambda: sorted(
            (a.tenant, a.sent, a.dead_lettered, sorted(a.waits))
            for a in s.tenant_activity((newest - DAY_NS) // 10 ** 6, newest // 10 ** 6)),
        "tenant_search": lambda: s.find_tenants("tenant-00", 8),
        "tenant_search_last_day": lambda: s.find_tenants("tenant-0", 8, from_ns=newest - DAY_NS),
        "counts": s.counts,
        "last_row": s.last_row,
    }


def _oldest(s: PostgresStore) -> int:
    with s.reads.connection() as c:
        return c.execute("SELECT MIN(bucket) AS b FROM places").fetchone()["b"] * s.bucket_ns


@contextlib.contextmanager
def counting_locks(s: PostgresStore, peaks: list):
    """While open, every read statement runs in a transaction of its own and records the lock entries held."""
    pool_connection = s.reads.connection

    class Rows(list):
        def fetchall(self):
            return list(self)

        def fetchone(self):
            return self[0] if self else None

    class Counted:
        def __init__(self, conn):
            self._conn = conn

        def __getattr__(self, name):
            return getattr(self._conn, name)

        def execute(self, query, params=None):
            idle = self._conn.info.transaction_status == psycopg.pq.TransactionStatus.IDLE
            with self._conn.transaction() if idle else contextlib.nullcontext():
                cur = self._conn.execute(query, params)
                rows = cur.fetchall() if cur.description else []
                peaks.append(self._conn.execute(
                    "SELECT COUNT(*) AS n FROM pg_locks WHERE pid = pg_backend_pid() AND locktype <> 'advisory'"
                ).fetchone()["n"])
            return Rows(rows)

    @contextlib.contextmanager
    def counted(*a, **kw):
        with pool_connection(*a, **kw) as conn:
            yield Counted(conn)

    s.reads.connection = counted
    try:
        yield
    finally:
        del s.reads.connection


def main(argv=None) -> None:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--store-url", required=True)
    p.add_argument("--schema", required=True)
    p.add_argument("--places", type=int, default=400)
    p.add_argument("--days", type=float, default=7)
    p.add_argument("--spans", default="200k")
    p.add_argument("--runs", type=int, default=7)
    p.add_argument("--threads", type=int, default=4)
    p.add_argument("--out", required=True)
    p.add_argument("--compare", help="a previous run's JSON: its answers must be these")
    a = p.parse_args(argv)
    with psycopg.connect(a.store_url, autocommit=True) as c:
        c.execute(f"CREATE SCHEMA IF NOT EXISTS {a.schema}")
    url = make_conninfo(a.store_url, options=f"-c search_path={a.schema}")
    store = PostgresStore(url, store_bodies=tuple(loadgen.body_queue_names(pg_smoke.PROFILE)),
                          retention=pg_smoke.RETENTION, bucket_ns=int(a.days * DAY_NS / a.places))
    try:
        print("seed:", pg_smoke.seed(store, harness.parse_size(a.spans), a.days, a.threads), flush=True)
        with store.reads.connection() as c:
            n_places = c.execute("SELECT COUNT(*) AS n FROM places").fetchone()["n"]
        out = {"places": n_places, "page_places": getattr(store, "PAGE_PLACES", None), "calls": {}}
        for name, call in calls(store).items():
            try:
                peaks: list = []
                with counting_locks(store, peaks):
                    answer = call()
                ms = []
                for _ in range(a.runs):
                    t0 = time.perf_counter()
                    call()
                    ms.append((time.perf_counter() - t0) * 1000)
                digest = hashlib.sha256(json.dumps(answer, sort_keys=True, default=str).encode()).hexdigest()[:16]
                r = {"p50_ms": round(statistics.median(ms), 2), "max_ms": round(max(ms), 2),
                     "statements": len(peaks), "most_locks": max(peaks, default=0), "answer": digest}
            except psycopg.Error as e:
                r = {"error": str(e).splitlines()[0]}
            out["calls"][name] = r
            print(f"{name:34} {json.dumps(r)}", flush=True)
    finally:
        store.close()
    Path(a.out).write_text(json.dumps(out, indent=1))
    if a.compare:
        before = json.loads(Path(a.compare).read_text())["calls"]
        differ = [n for n, r in out["calls"].items() if n in before and r.get("answer") != before[n].get("answer")]
        print("answers differ:" if differ else "answers: all the same as", differ or a.compare)
        if differ:
            sys.exit(1)


if __name__ == "__main__":
    main()
