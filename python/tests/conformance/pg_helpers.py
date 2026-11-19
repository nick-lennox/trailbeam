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
