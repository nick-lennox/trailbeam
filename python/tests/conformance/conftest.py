"""The backends the conformance suite runs against.

``BACKENDS`` maps a name to a factory ``(tmp_path, store_bodies) -> Store`` giving an empty store. SQLite runs
twice, as the server's tests do: ``:memory:`` (reads share the writer) and a WAL file (separate read
connections). Another backend (one not in this repository, such as the hosted app's) runs the same suite by
naming its factory in TRAILBEAM_CONFORMANCE_STORE, ``package.module:function``; ``pytest tests/conformance``.

Postgres (postgres.py) runs too when TRAILBEAM_TEST_POSTGRES_URL names a database the tests may create schemas
in (CI: a service container). Each store gets a schema of its own, and buckets of one second, so retention's
moves and drops run on these few minutes of spans.
"""
import importlib
import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

from trailbeam.server.db import SqliteStore  # noqa: E402

BACKENDS = {
    "sqlite-memory": lambda tmp_path, store_bodies: SqliteStore(":memory:", store_bodies=store_bodies),
    "sqlite-file": lambda tmp_path, store_bodies: SqliteStore(str(tmp_path / "store.sqlite"), store_bodies=store_bodies),
}
if os.environ.get("TRAILBEAM_TEST_POSTGRES_URL"):
    from pg_helpers import postgres_store  # noqa: E402

    BACKENDS["postgres"] = lambda tmp_path, store_bodies: postgres_store(store_bodies=store_bodies, bucket_ns=10 ** 9)
if os.environ.get("TRAILBEAM_CONFORMANCE_STORE"):
    _module, _, _name = os.environ["TRAILBEAM_CONFORMANCE_STORE"].partition(":")
    BACKENDS[os.environ["TRAILBEAM_CONFORMANCE_STORE"]] = getattr(importlib.import_module(_module), _name)


@pytest.fixture(params=sorted(BACKENDS))
def new_store(request, tmp_path):
    """Call with store_bodies (and optionally a sub-directory name) for an empty store; closed after the test."""
    made = []

    def make(store_bodies=frozenset(), name="a"):
        d = tmp_path / name
        d.mkdir()
        s = BACKENDS[request.param](d, frozenset(store_bodies))
        made.append(s)
        return s
    yield make
    for s in made:
        s.close()
