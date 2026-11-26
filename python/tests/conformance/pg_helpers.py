"""A Postgres store in a schema of its own, for tests: TRAILBEAM_TEST_POSTGRES_URL names the database."""
import itertools
import os
import uuid

import psycopg
from psycopg.conninfo import make_conninfo

from trailbeam.server.postgres import PostgresStore

_n = itertools.count()


def new_schema() -> str:
    """A fresh, empty schema; returns a conninfo whose search_path is it."""
    url = os.environ["TRAILBEAM_TEST_POSTGRES_URL"]
    name = f"tb_{uuid.uuid4().hex[:12]}_{next(_n)}"
    with psycopg.connect(url, autocommit=True) as c:
        c.execute(f"CREATE SCHEMA {name}")
    return make_conninfo(url, options=f"-c search_path={name}")


def drop_schema(conninfo: str) -> None:
    """Drop the schema: its place partitions a few at a time first, each in its own transaction, then the rest.
    One ``DROP SCHEMA ... CASCADE`` locks every partition, index, TOAST table and type in one transaction, and a
    test with many places (one-second buckets) fills a stock Postgres's lock table (QV-107)."""
    name = conninfo.split("search_path=")[1].split()[0].strip("'")
    with psycopg.connect(os.environ["TRAILBEAM_TEST_POSTGRES_URL"], autocommit=True) as c:
        parts = [r[0] for r in c.execute(
            """SELECT ch.relname FROM pg_inherits i JOIN pg_class ch ON ch.oid = i.inhrelid
               WHERE ch.relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = %s) AND ch.relkind = 'r'
               ORDER BY 1""", (name,)).fetchall()]
        for k in range(0, len(parts), 12):
            c.execute("DROP TABLE IF EXISTS " + ", ".join(f"{name}.{t}" for t in parts[k:k + 12]))
        c.execute(f"DROP SCHEMA IF EXISTS {name} CASCADE")


class _Store(PostgresStore):
    def close(self):
        super().close()
        drop_schema(self.url)


def postgres_store(**kw) -> PostgresStore:
    return _Store(new_schema(), **kw)


def replica_report(stores, failed=None) -> str:
    """What a failed replica test needs to tell a wrong stored state from a wrong read (QV-552): the stored state
    read on a fresh connection (each summary row, whether its message's lookup row is there, a message with two
    summary rows, spans of a message with none), then, for each replica, its lists and counts and what each of its
    pooled read connections sees in every place, against the fresh connection. ``failed``: the replica whose
    snapshot differed."""
    import contextlib

    import psycopg_pool

    url = stores[0].url
    out = []
    per_place = """SELECT p.rclass, p.bucket,
                          (SELECT count(*) FROM messages m WHERE m.rclass = p.rclass AND m.bucket = p.bucket) AS messages,
                          (SELECT count(*) FROM mid_places l WHERE l.rclass = p.rclass AND l.bucket = p.bucket) AS mid_places,
                          (SELECT count(*) FROM spans s WHERE s.rclass = p.rclass AND s.bucket = p.bucket) AS spans
                   FROM places p ORDER BY p.bucket, p.rclass"""
    with psycopg.connect(url, autocommit=True) as c:
        summaries = c.execute("SELECT mid, rclass, bucket, strays_json FROM messages ORDER BY mid, bucket").fetchall()
        looked = set(c.execute("SELECT mid, rclass, bucket FROM mid_places").fetchall())
        spans = {r[0] for r in c.execute("SELECT DISTINCT mid FROM spans WHERE mid IS NOT NULL").fetchall()}
        want = {(r[0], r[1]): r[2:] for r in c.execute(per_place).fetchall()}
    mids = [r[0] for r in summaries]
    out.append("stored (fresh connection): summary rows " + ", ".join(f"{m}@{rc}/{b} strays {s}"
                                                                      for m, rc, b, s in summaries))
    problems = [f"{m}@{rc}/{b}: summary row without its mid_places row" for m, rc, b, _ in summaries
                if (m, rc, b) not in looked]
    problems += [f"{m}: {mids.count(m)} summary rows" for m in sorted(set(mids)) if mids.count(m) > 1]
    problems += [f"{m}: spans but no summary row" for m in sorted(spans - set(mids))]
    out.append("stored state: " + ("; ".join(problems) if problems else "consistent"))
    for i, s in enumerate(stores):
        tag = f"replica {i}{' (failed)' if s is failed else ''}"
        try:
            listed = sorted(s.recent_messages(1000))
            missing = [m for m in sorted(set(mids)) if not s.message_rows(m)]
            out.append(f"{tag}: counts {s.counts()['messages']}, listed {listed}, no rows for {missing}")
        except Exception as e:  # noqa: BLE001 (a report: say what failed and go on)
            out.append(f"{tag}: reads failed: {e!r}")
        try:
            with contextlib.ExitStack() as held:  # every pooled read connection at once (the pool may open more)
                conns = [held.enter_context(s.reads.connection(timeout=5)) for _ in range(s.reads.max_size)]
                differ = []
                for k, c in enumerate(conns):
                    got = {(r["rclass"], r["bucket"]): (r["messages"], r["mid_places"], r["spans"])
                           for r in c.execute(per_place).fetchall()}
                    diff = {p: (got.get(p), w) for p, w in want.items() if got.get(p) != w}
                    if diff:
                        pid = c.execute("SELECT pg_backend_pid() AS pid").fetchone()["pid"]
                        differ.append(f"connection {k} (pid {pid}), place: ((messages, mid_places, spans) seen, "
                                      f"stored): {diff}")
                out.append(f"{tag}: its {len(conns)} read connections " + (
                    "see in places what a fresh one sees" if not differ else "differ: " + "; ".join(differ)))
        except (psycopg.Error, psycopg_pool.PoolTimeout) as e:
            out.append(f"{tag}: could not check its read connections: {e!r}")
    return "\n".join(out)
