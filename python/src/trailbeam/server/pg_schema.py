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

**Lookups** (version 2, QV-113). ``mid_places`` and ``hash_places`` are not partitioned: they say which places
may hold rows of a message ID, and spans of a receipt hash. A statement that names its places is pruned when it
is planned, and locks those places' partitions only; one that doesn't (a message ID or a hash across all places)
locks every place's partitions and indexes until its transaction ends (postgres.py, "Locks"). They are written
with the rows, may name a place that no longer holds any (a message moved on, a sweep deleted it), and lose a
place's entries when retention drops it.

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
CREATE TABLE IF NOT EXISTS mid_places (
    mid TEXT COLLATE "C" NOT NULL,       -- a message with rows (spans, summary rows, bodies) in the place
    rclass BIGINT NOT NULL,
    bucket BIGINT NOT NULL,
    PRIMARY KEY (mid, rclass, bucket)
);
CREATE INDEX IF NOT EXISTS mid_places_place ON mid_places (rclass, bucket);
CREATE TABLE IF NOT EXISTS hash_places (
    receipt_hmac TEXT NOT NULL,          -- a receive or settle span with this receipt hash is in the place
    key_id TEXT NOT NULL,
    rclass BIGINT NOT NULL,
    bucket BIGINT NOT NULL,
    PRIMARY KEY (receipt_hmac, key_id, rclass, bucket)
);
CREATE INDEX IF NOT EXISTS hash_places_place ON hash_places (rclass, bucket);
CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER PRIMARY KEY,
    applied_at_ms BIGINT,
    name TEXT NOT NULL,
    trailbeam_version TEXT
);
"""


@dataclass(frozen=True)
class Migration:
    """``statements`` run in one transaction. ``per_place``, when given, then run once for every place, each place
    in a transaction of its own (``%(rclass)s``, ``%(bucket)s``: the place), so that the migration locks one
    place's partitions at a time; the version is recorded after the last. Both must be safe to run again: a
    migration stopped part way is run again from the start."""
    version: int
    name: str
    statements: tuple
    per_place: tuple = ()


# Version 1 is the first Postgres schema. Later versions go here, one at a time.
MIGRATIONS: tuple = (
    Migration(2, "lookup tables: the places of a message ID and of a receipt hash (QV-113)", (
        """CREATE TABLE IF NOT EXISTS mid_places (mid TEXT COLLATE "C" NOT NULL, rclass BIGINT NOT NULL,
           bucket BIGINT NOT NULL, PRIMARY KEY (mid, rclass, bucket))""",
        "CREATE INDEX IF NOT EXISTS mid_places_place ON mid_places (rclass, bucket)",
        """CREATE TABLE IF NOT EXISTS hash_places (receipt_hmac TEXT NOT NULL, key_id TEXT NOT NULL,
           rclass BIGINT NOT NULL, bucket BIGINT NOT NULL, PRIMARY KEY (receipt_hmac, key_id, rclass, bucket))""",
        "CREATE INDEX IF NOT EXISTS hash_places_place ON hash_places (rclass, bucket)",
    ), per_place=(
        """INSERT INTO mid_places (mid, rclass, bucket)
           SELECT mid, %(rclass)s, %(bucket)s FROM spans
               WHERE rclass = %(rclass)s AND bucket = %(bucket)s AND mid IS NOT NULL
           UNION SELECT mid, %(rclass)s, %(bucket)s FROM messages WHERE rclass = %(rclass)s AND bucket = %(bucket)s
           UNION SELECT mid, %(rclass)s, %(bucket)s FROM bodies WHERE rclass = %(rclass)s AND bucket = %(bucket)s
           ON CONFLICT DO NOTHING""",
        """INSERT INTO hash_places (receipt_hmac, key_id, rclass, bucket)
           SELECT DISTINCT receipt_hmac, key_id, %(rclass)s, %(bucket)s FROM spans
               WHERE rclass = %(rclass)s AND bucket = %(bucket)s AND receipt_hmac IS NOT NULL AND key_id IS NOT NULL
                 AND kind IN ('receive', 'delete', 'change_visibility')
           ON CONFLICT DO NOTHING""",
    )),
)
LATEST = 2
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
                if not m.per_place:
                    _record(conn, m.version, m.name)
                    done.append(m.version)
                    continue
            places = conn.execute("SELECT rclass, bucket FROM places ORDER BY bucket, rclass").fetchall()
            for rclass, bucket in places:
                with conn.transaction():
                    for sql in m.per_place:
                        conn.execute(sql, {"rclass": rclass, "bucket": bucket})
            with conn.transaction():
                if current_version(conn) == m.version - 1:
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


# ---- the lock table (QV-107) ----------------------------------------------------------------------------------
#
# A statement on a partitioned table whose WHERE can't prune by place (a message ID, a receipt hash, a page
# across all of time) locks every partition and every partition's index until its transaction ends: about one
# lock per table and index of each place, per connection. Those locks live in Postgres's shared lock table,
# sized at server start (src/backend/storage/lmgr/lock.c NLOCKENTS and postinit.c InitializeMaxBackends, REL_14_STABLE):
# max_locks_per_transaction x (max_connections + autovacuum_max_workers + 1 + max_worker_processes
# + max_wal_senders + max_prepared_transactions) locked objects, and twice that (object, backend) pairs. When it
# is full, any statement fails with "out of shared memory". On a stock server (64 x 122 = 7,808 objects) the
# lock count of Trailbeam's own statements grows with the number of places, so the check below says what
# setting a store's places and pools need.

LOCK_SETTINGS = ("max_locks_per_transaction", "max_connections", "autovacuum_max_workers", "max_worker_processes",
                 "max_wal_senders", "max_prepared_transactions")


@dataclass(frozen=True)
class LockNeed:
    """What one replica can lock at once, against what the server has room for. ``settings``: LOCK_SETTINGS,
    as ints; ``relations_per_place``: the partitioned tables and indexes (one partition of each per place);
    ``connections``: this replica's connections that can run a page at once (the read pool: since
    QV-113 pages are the only statements that lock every place)."""
    places: int
    relations_per_place: int
    connections: int
    settings: dict

    @property
    def backends(self) -> int:
        s = self.settings
        return (s["max_connections"] + s["autovacuum_max_workers"] + 1 + s["max_worker_processes"]
                + s["max_wal_senders"] + s["max_prepared_transactions"])

    @property
    def objects(self) -> int:
        """Relations one page may hold a lock on at once: every table and index of every place."""
        return self.places * self.relations_per_place

    @property
    def holders(self) -> int:
        """(relation, connection) pairs, with every read connection of the replica in a page at once."""
        return self.objects * self.connections

    @property
    def required(self) -> int:
        """The least max_locks_per_transaction with room for both (ignoring Postgres's 10 % margin)."""
        b = self.backends
        return max(-(-self.objects // b), -(-self.holders // (2 * b)), 1)

    @property
    def short(self) -> bool:
        return self.required > self.settings["max_locks_per_transaction"]

    def message(self) -> str:
        have = self.settings["max_locks_per_transaction"]
        return (f"Postgres lock table: this replica's statements can lock up to {self.holders:,} tables and indexes "
                f"at once ({self.places} places x {self.relations_per_place} tables and indexes x "
                f"{self.connections} read connections), but max_locks_per_transaction = {have} leaves room for "
                f"{2 * have * self.backends:,} ({2 * have} x {self.backends} server processes). Under concurrent "
                f"pages, statements (exports too) fail with \"out of shared memory\". Set max_locks_per_transaction to "
                f"at least {self.required} (and restart Postgres); with more replicas on this database, multiply by "
                f"their number; it grows as places are added (README: Postgres lock table)")


def lock_need(conn, connections: int) -> LockNeed:
    """LockNeed for the current schema's places and the server's settings. ``conn``: any psycopg connection."""
    rows = conn.execute("SELECT name, setting FROM pg_settings WHERE name = ANY(%s)", (list(LOCK_SETTINGS),)).fetchall()
    settings = {(r["name"] if isinstance(r, dict) else r[0]): int(r["setting"] if isinstance(r, dict) else r[1])
                for r in rows}
    places = _one(conn.execute("SELECT COUNT(*) FROM places").fetchone())
    per_place = _one(conn.execute(
        "SELECT COUNT(*) FROM pg_class WHERE relnamespace = current_schema()::regnamespace AND relkind IN ('p', 'I')"
    ).fetchone())
    return LockNeed(places, per_place, connections, settings)


def _one(row):
    return next(iter(row.values())) if isinstance(row, dict) else row[0]
