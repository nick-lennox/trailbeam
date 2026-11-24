"""The tables of people and tokens (account_tables.py; SQLite schema 15, Postgres schema 8), on both stores:
emails unique ignoring case, what removing a person removes, and retention and --vacuum leaving them alone.
Upgrades from every older version are in test_migrations.py and test_postgres.py.

The Postgres cases run when TRAILBEAM_TEST_POSTGRES_URL names a database, as in test_postgres.py."""
import os
import sqlite3
import sys
from pathlib import Path

import pytest

from spans import SEC, T0, Consumer, to_proto
from trailbeam.server import account_tables
from trailbeam.server.db import Database
from trailbeam.server.otlp import decode
from trailbeam.server.retention import Purger, Retention

ROOT = Path(__file__).resolve().parents[2]
PG = os.environ.get("TRAILBEAM_TEST_POSTGRES_URL")
DAY = 86_400
w = Consumer()


class Sqlite:
    name = "sqlite"
    param = "?"

    def __init__(self, tmp_path):
        self.path = str(tmp_path / "trailbeam.sqlite")
        self.store = Database(self.path)

    def sql(self, q: str, args=()) -> list:
        c = sqlite3.connect(self.path)
        c.execute("PRAGMA foreign_keys=ON")
        try:
            with c:
                return [tuple(r) for r in c.execute(q, args).fetchall()]
        finally:
            c.close()

    def tables(self) -> set:
        return {r[0] for r in self.sql("SELECT name FROM sqlite_master WHERE type = 'table'")}

    integrity = sqlite3.IntegrityError

    def close(self):
        self.store.close()


class Postgres:
    name = "postgres"
    param = "%s"

    def __init__(self, tmp_path):
        sys.path.insert(0, str(ROOT / "tests" / "conformance"))
        from pg_helpers import postgres_store
        import psycopg

        self.store = postgres_store()
        self.integrity = psycopg.errors.UniqueViolation

    def sql(self, q: str, args=()) -> list:
        with self.store.ingest.connection() as c:
            cur = c.execute(q.replace("?", "%s"), args)
            return [tuple(r.values()) for r in cur.fetchall()] if cur.description else []

    def tables(self) -> set:
        return {r[0] for r in self.sql("SELECT tablename FROM pg_tables WHERE schemaname = current_schema()")}

    def close(self):
        self.store.close()


@pytest.fixture(params=["sqlite", pytest.param("postgres", marks=pytest.mark.skipif(
    not PG, reason="TRAILBEAM_TEST_POSTGRES_URL is not set"))])
def store(request, tmp_path):
    s = (Sqlite if request.param == "sqlite" else Postgres)(tmp_path)
    yield s
    s.close()


def add_user(s, email: str, role: str = "admin") -> int:
    s.sql("INSERT INTO users (email, name, role, password_hash, created_at_ms, updated_at_ms) "
          "VALUES (?, ?, ?, 'scrypt$15$8$3$c2FsdA$aGFzaA', 1, 1)", (email, email.split("@")[0], role))
    return s.sql("SELECT id FROM users WHERE email = ?", (email,))[0][0]


def add_everything(s, user: int) -> None:
    s.sql("INSERT INTO sessions (id_sha256, user_id, created_at_ms, expires_at_ms, last_seen_ms) "
          "VALUES (?, ?, 1, 2, 1)", (f"s{user}", user))
    s.sql("INSERT INTO personal_tokens (user_id, name, token_sha256, prefix, created_at_ms) VALUES (?, 'ci', ?, 'tbp_abcd', 1)",
          (user, f"p{user}"))
    s.sql("INSERT INTO ingest_tokens (name, token_sha256, prefix, created_at_ms, created_by) "
          "VALUES ('orders-api prod', ?, 'tbi_abcd', 1, ?)", (f"i{user}", user))
    s.sql("INSERT INTO setup_codes (code_sha256, kind, user_id, created_at_ms, expires_at_ms) VALUES (?, 'reset', ?, 1, 2)",
          (f"c{user}", user))
    s.sql("INSERT INTO oidc_identities (issuer, subject, user_id, email, created_at_ms) "
          "VALUES ('https://id.example.com', ?, ?, 'x@example.com', 1)", (f"sub{user}", user))


def counts(s) -> dict:
    return {t: s.sql(f"SELECT COUNT(*) FROM {t}")[0][0] for t in account_tables.TABLES}


def test_a_new_store_has_the_tables_empty(store):
    assert set(account_tables.TABLES) <= store.tables()
    assert counts(store) == {t: 0 for t in account_tables.TABLES}


def test_emails_are_unique_ignoring_case_and_found_ignoring_case(store):
    uid = add_user(store, "Ada@Example.com")
    with pytest.raises(store.integrity):
        add_user(store, "ada@example.COM")
    # SQLite: the column's NOCASE collation; Postgres: lower(email), the expression the unique index is on
    where = "email = ?" if store.name == "sqlite" else "lower(email) = lower(?)"
    assert store.sql(f"SELECT id, email FROM users WHERE {where}", ("ADA@example.com",)) == [(uid, "Ada@Example.com")]
    add_user(store, "grace@example.com", role="member")
    assert counts(store)["users"] == 2


def test_removing_a_person_removes_their_sessions_tokens_and_codes_and_keeps_the_ingest_tokens(store):
    ada, grace = add_user(store, "ada@example.com"), add_user(store, "grace@example.com", "member")
    add_everything(store, ada)
    add_everything(store, grace)
    store.sql("DELETE FROM users WHERE id = ?", (grace,))
    assert counts(store) == {"users": 1, "sessions": 1, "personal_tokens": 1, "setup_codes": 1, "ingest_tokens": 2,
                             "oidc_identities": 1, "oidc_logins": 0}
    # a service's token outlives whoever made it: revoking it is a decision of its own
    assert sorted(store.sql("SELECT token_sha256, created_by FROM ingest_tokens")) == [(f"i{ada}", ada), (f"i{grace}", None)]


def test_token_hashes_are_unique(store):
    add_everything(store, add_user(store, "ada@example.com"))
    with pytest.raises(store.integrity):
        store.sql("INSERT INTO ingest_tokens (name, token_sha256, prefix, created_at_ms) VALUES ('again', ?, 'tbi_x', 1)",
                  (store.sql("SELECT token_sha256 FROM ingest_tokens")[0][0],))


def test_retention_leaves_people_and_tokens_alone(store):
    add_everything(store, add_user(store, "ada@example.com"))
    before = counts(store)
    now = T0 + 10 * DAY * SEC
    spans = [w.send(T0, "old"), w.receive(T0 + SEC, "old", "h-old"), w.delete(T0 + 2 * SEC, "h-old")]
    store.store.insert(decode(to_proto(spans).SerializeToString(), "application/x-protobuf").records)
    r = Purger(store.store, Retention(DAY, {}), now=lambda: now, pause_s=0).run_pass()
    assert r["deleted_spans"] == 3
    assert counts(store) == before


def test_vacuum_keeps_people_and_tokens(tmp_path):
    s = Sqlite(tmp_path)
    try:
        add_everything(s, add_user(s, "ada@example.com"))
        before = counts(s)
        s.store.vacuum()
        assert counts(s) == before
    finally:
        s.close()


def test_a_memory_store_starts_with_no_one():
    """``--db :memory:`` keeps people and tokens only while the process runs (README, "Users and tokens")."""
    for _ in range(2):
        db = Database(":memory:")
        try:
            assert db.query("SELECT COUNT(*) AS n FROM users")[0]["n"] == 0
            db.conn.execute("INSERT INTO users (email, name, role, created_at_ms, updated_at_ms) "
                            "VALUES ('ada@example.com', 'Ada', 'admin', 1, 1)")
        finally:
            db.close()
