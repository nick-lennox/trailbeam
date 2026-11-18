"""The Postgres schema (PostgresStore, postgres.py): versions, the ordered migrations, and the per-place partitions.

Versioning follows migrations.py (the SQLite schema): an integer version, a ``schema_version`` table with one row
per version the database has been at, every migration after the database's version run in order, each in its
own transaction, and a database at a newer version than ``LATEST`` refused (``SchemaTooNew``). The migrations run
under a session advisory lock, so two servers starting at once against one database don't both migrate.
Adding a migration: append a ``Migration``, update ``CREATE`` to the schema it produces, extend
tests/server/test_postgres.py. Never edit a migration that has been on ``main``.

**Places.** Every table holding spans or what is derived from them is partitioned by ``(rclass, bucket)``,
a *place*: ``rclass`` the retention class (the retention of the queue, in seconds; FOREVER_CLASS for forever;
0 when the store was given no retention), ``bucket`` a span time bucket (``time_ns // G``). One place is one
partition of each of ``spans``, ``messages``, ``message_queues``, ``message_tenants``, ``tenant_events`` and
``bodies``, created when a span first needs it (``place_ddl``) and removed whole by retention (postgres.py).
``places`` lists them; ``place_queues`` the queues each holds; ``strays`` the messages with rows in a place
other than their own (postgres.py, "Placement").

Text keys that pages sort or compare by are ``COLLATE "C"``: byte order, as SQLite compares, so ties and
cursors come out the same on both.
"""
from __future__ import annotations

import time
from dataclasses import dataclass

from .. import __version__
from .migrations import SchemaError, SchemaTooNew

# Partitioned tables: one partition of each per place.
PARTITIONED = ("spans", "messages", "message_queues", "message_tenants", "tenant_events", "bodies")

CREATE = """
CREATE TABLE IF NOT EXISTS places (
    rclass BIGINT NOT NULL,
    bucket BIGINT NOT NULL,
    state TEXT NOT NULL DEFAULT 'live',  -- live | dropping (retention is removing it; ingest writes elsewhere)
    swept TEXT,                          -- JSON [queue, ...]: the expired queues of its last sweep (postgres.py)
    PRIMARY KEY (rclass, bucket)
);
CREATE TABLE IF NOT EXISTS place_queues (
    rclass BIGINT NOT NULL,
    bucket BIGINT NOT NULL,
    queue TEXT COLLATE "C" NOT NULL,     -- a queue with a span, or a message, in the place
    PRIMARY KEY (rclass, bucket, queue)
);
CREATE TABLE IF NOT EXISTS strays (
    rclass BIGINT NOT NULL,              -- the place the rows are in
    bucket BIGINT NOT NULL,
    mid TEXT COLLATE "C" NOT NULL,       -- a message placed elsewhere with rows here
    PRIMARY KEY (rclass, bucket, mid)
);
CREATE SEQUENCE IF NOT EXISTS span_rows;
CREATE SEQUENCE IF NOT EXISTS body_ids;
CREATE TABLE IF NOT EXISTS spans (
    rclass BIGINT NOT NULL,
    bucket BIGINT NOT NULL,
    "row" BIGINT NOT NULL DEFAULT nextval('span_rows'),
    trace_id TEXT NOT NULL,
    span_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    time_ns BIGINT NOT NULL,
    start_ns BIGINT,
    service_name TEXT,
    source TEXT, operation TEXT, queue TEXT COLLATE "C" NOT NULL, queue_url TEXT,
    message_id TEXT COLLATE "C",
    receipt_hmac TEXT, key_id TEXT,
    receive_count BIGINT, sent_ts_ms BIGINT, first_receive_ts_ms BIGINT,
    dlq_source_arn TEXT,
    result TEXT, error_code TEXT, visibility_timeout BIGINT,
    outcome TEXT, error_type TEXT, batch_entry_id TEXT,
    client_id TEXT, pid BIGINT, thread_name TEXT,
    faas_name TEXT, faas_invocation_id TEXT,
    order_scope TEXT, order_issued BIGINT, order_returned BIGINT,
    attributes TEXT NOT NULL,
    mid TEXT COLLATE "C",
    mid_source TEXT,
    receive_row BIGINT,
    body_dropped TEXT,
    body_id BIGINT,
    UNIQUE (rclass, bucket, trace_id, span_id)
) PARTITION BY RANGE (rclass, bucket);
CREATE INDEX IF NOT EXISTS spans_row ON spans ("row");
CREATE INDEX IF NOT EXISTS spans_mid ON spans (mid, time_ns);
CREATE INDEX IF NOT EXISTS spans_hash ON spans (receipt_hmac, key_id);
CREATE INDEX IF NOT EXISTS spans_receive_row ON spans (receive_row) WHERE receive_row IS NOT NULL;
CREATE INDEX IF NOT EXISTS spans_unjoined ON spans (queue)
    WHERE receive_row IS NULL AND kind IN ('delete', 'change_visibility');
CREATE INDEX IF NOT EXISTS spans_body ON spans (mid) WHERE body_id IS NOT NULL;
CREATE TABLE IF NOT EXISTS messages (
    rclass BIGINT NOT NULL,
    bucket BIGINT NOT NULL,
    mid TEXT COLLATE "C" NOT NULL,
    first_ns BIGINT NOT NULL,
    first_clock TEXT NOT NULL,
    last_ns BIGINT NOT NULL,
    source_queue TEXT NOT NULL,
    current_queue TEXT NOT NULL,
    in_dlq BIGINT NOT NULL,
    moved BIGINT NOT NULL,
    source_receive_count BIGINT NOT NULL,
    deliveries BIGINT NOT NULL,
    successes BIGINT NOT NULL,
    consumers TEXT NOT NULL,
    final_delete BIGINT NOT NULL,
    lambda_ok BIGINT NOT NULL,
    lambda_wait_ns BIGINT,
    tenant TEXT COLLATE "C",
    dup_received BIGINT NOT NULL,
    dup_succeeded BIGINT NOT NULL,
    stuck_first_ns BIGINT,
    stuck_sqs_clock BIGINT NOT NULL,
    stuck_open BIGINT NOT NULL,
    queues_json TEXT NOT NULL,
    tenants_json TEXT NOT NULL,
    events_json TEXT NOT NULL,
    strays_json TEXT NOT NULL DEFAULT '[]',  -- JSON [[rclass, bucket], ...]: other places with rows of it
    UNIQUE (rclass, bucket, mid)
) PARTITION BY RANGE (rclass, bucket);
CREATE INDEX IF NOT EXISTS messages_mid ON messages (mid);
CREATE INDEX IF NOT EXISTS messages_last ON messages (last_ns DESC, mid);
CREATE INDEX IF NOT EXISTS messages_duplicates ON messages (last_ns DESC, mid) WHERE dup_received <> 0 OR dup_succeeded <> 0;
CREATE INDEX IF NOT EXISTS messages_stuck ON messages (stuck_first_ns, mid)
    WHERE stuck_open <> 0 AND stuck_first_ns IS NOT NULL;
CREATE INDEX IF NOT EXISTS messages_strays ON messages (mid) WHERE strays_json <> '[]';
CREATE TABLE IF NOT EXISTS message_queues (
    rclass BIGINT NOT NULL,
    bucket BIGINT NOT NULL,
    queue TEXT COLLATE "C" NOT NULL,
    last_ns BIGINT NOT NULL,
    mid TEXT COLLATE "C" NOT NULL
) PARTITION BY RANGE (rclass, bucket);
CREATE INDEX IF NOT EXISTS message_queues_key ON message_queues (queue, last_ns DESC, mid);
CREATE TABLE IF NOT EXISTS message_tenants (
    rclass BIGINT NOT NULL,
    bucket BIGINT NOT NULL,
    tenant TEXT COLLATE "C" NOT NULL,
    last_ns BIGINT NOT NULL,
    mid TEXT COLLATE "C" NOT NULL,
    tenant_lower TEXT COLLATE "C" NOT NULL,
    named_ns BIGINT NOT NULL
) PARTITION BY RANGE (rclass, bucket);
CREATE INDEX IF NOT EXISTS message_tenants_key ON message_tenants (tenant, last_ns DESC, mid);
CREATE INDEX IF NOT EXISTS message_tenants_search ON message_tenants (tenant_lower, tenant, named_ns);
CREATE TABLE IF NOT EXISTS tenant_events (
    rclass BIGINT NOT NULL,
    bucket BIGINT NOT NULL,
    at_ms BIGINT NOT NULL,
    mid TEXT COLLATE "C" NOT NULL,
    seq BIGINT NOT NULL,
    kind TEXT NOT NULL,
    tenant TEXT COLLATE "C",
    wait_ms BIGINT
) PARTITION BY RANGE (rclass, bucket);
CREATE INDEX IF NOT EXISTS tenant_events_key ON tenant_events (at_ms, mid, seq);
CREATE TABLE IF NOT EXISTS bodies (
    rclass BIGINT NOT NULL,
    bucket BIGINT NOT NULL,
    id BIGINT NOT NULL DEFAULT nextval('body_ids'),
    mid TEXT COLLATE "C" NOT NULL,
    sha BYTEA NOT NULL,
    body BYTEA NOT NULL                  -- trailbeam.body as the span carried it, UTF-8 (surrogatepass)
) PARTITION BY RANGE (rclass, bucket);
CREATE INDEX IF NOT EXISTS bodies_mid ON bodies (mid, sha);
CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER PRIMARY KEY,
    applied_at_ms BIGINT,
    name TEXT NOT NULL,
    trailbeam_version TEXT
);
"""


@dataclass(frozen=True)
class Migration:
    version: int
    name: str
    statements: tuple


# Version 1 is the first Postgres schema. Later versions go here, one at a time.
MIGRATIONS: tuple = ()
LATEST = 1
MIGRATION_LOCK = 0x7472_6169_6c62_6d01  # pg_advisory_lock key: "trailbm" + 1, held while the schema is opened


def statements(script: str) -> list[str]:
    """A script as single statements: ``--`` comments removed, then split at semicolons (no statement here
    has one in a string)."""
    text = "\n".join(line.split("--", 1)[0] for line in script.splitlines())
    return [s.strip() for s in text.split(";") if s.strip()]


def open_schema(conn) -> list[int]:
    """Bring the database to ``LATEST``. ``conn``: a psycopg connection in autocommit. Returns the versions
    migrated to (empty for a new or current database). Raises SchemaError, SchemaTooNew."""
    done: list[int] = []
    conn.execute("SELECT pg_advisory_lock(%s)", (MIGRATION_LOCK,))
    try:
        while True:
            with conn.transaction():
                version = current_version(conn)
                if version is None:
                    for sql in statements(CREATE):
                        conn.execute(sql)
                    _record(conn, LATEST, "create")
                    return done
                if version == LATEST:
                    return done
                if version > LATEST:
                    raise SchemaTooNew(
                        f"the database is at Postgres schema version {version}, written by a newer Trailbeam; this "
                        f"one ({__version__}) knows versions up to {LATEST}. Run the newer Trailbeam. Refusing to "
                        "start, so nothing is lost")
                m = MIGRATIONS[version - 1]
                for sql in m.statements:
                    conn.execute(sql)
                _record(conn, m.version, m.name)
                done.append(m.version)
    finally:
        conn.execute("SELECT pg_advisory_unlock(%s)", (MIGRATION_LOCK,))


def current_version(conn):
    """The schema version; None for a database (schema) with no Trailbeam tables."""
    tables = {r[0] for r in conn.execute(
        "SELECT tablename FROM pg_tables WHERE schemaname = current_schema()").fetchall()}
    if "schema_version" in tables:
        v = conn.execute("SELECT MAX(version) FROM schema_version").fetchone()[0]
        if v is None:
            raise SchemaError("the schema_version table is empty")
        return v
    if tables & {"spans", "messages", "places"}:
        raise SchemaError("the database has Trailbeam tables but no schema version")
    return None


def _record(conn, version: int, name: str) -> None:
    conn.execute("INSERT INTO schema_version (version, applied_at_ms, name, trailbeam_version) VALUES (%s, %s, %s, %s)",
                 (version, time.time_ns() // 1_000_000, name, __version__))


def partition_name(table: str, rclass: int, bucket: int) -> str:
    return f"{table}_p{rclass}_{'m' if bucket < 0 else ''}{abs(bucket)}"


def place_ddl(rclass: int, bucket: int) -> list[str]:
    """The partitions of one place, one per partitioned table."""
    return [f"CREATE TABLE IF NOT EXISTS {partition_name(t, rclass, bucket)} PARTITION OF {t} "
            f"FOR VALUES FROM ({int(rclass)}, {int(bucket)}) TO ({int(rclass)}, {int(bucket) + 1})"
            for t in PARTITIONED]
