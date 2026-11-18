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
    name = conninfo.split("search_path=")[1].split()[0].strip("'")
    with psycopg.connect(os.environ["TRAILBEAM_TEST_POSTGRES_URL"], autocommit=True) as c:
        c.execute(f"DROP SCHEMA IF EXISTS {name} CASCADE")


class _Store(PostgresStore):
    def close(self):
        super().close()
        drop_schema(self.url)


def postgres_store(**kw) -> PostgresStore:
    return _Store(new_schema(), **kw)
