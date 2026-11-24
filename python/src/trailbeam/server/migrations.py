"""SQLite schema versions and the ordered upgrade migrations that run when the server opens a file.

The schema version is an integer. ``schema_version`` has one row per version the file has been at, with
when it got there and the Trailbeam version that put it there; the file's version is the highest. Version 5
and earlier kept the version in ``meta.schema_version`` only. That row is still written, so a Trailbeam
older than this module refuses a file it cannot read, instead of opening it.

On open (``open_schema``):

- a new file gets the current schema in one step (``CREATE``), at ``LATEST``;
- a file at an older version runs every migration after its version, in order, each in its own
  transaction: a failure, a crash or a kill leaves the file at the last version that completed, and the next
  start carries on from there;
- a file at a newer version than ``LATEST`` is refused (``SchemaTooNew``): this code would read it wrongly.

Adding a migration: append a ``Migration`` with the next version, update ``CREATE`` to the schema it
produces, and extend ``tests/server/test_migrations.py`` (a file at every older version must upgrade to the
same schema as ``CREATE``). Never edit a migration that has been on ``main``: some file has run it.
"""
from __future__ import annotations

import json
import sqlite3
import time
from dataclasses import dataclass
from typing import Callable, Optional

from .. import __version__
from . import account_tables, body_table, summary_table


class SchemaError(RuntimeError):
    """The file's schema can't be used by this version of Trailbeam."""


class SchemaTooNew(SchemaError):
    pass


@dataclass(frozen=True)
class Migration:
    version: int       # the version the file is at after this migration
    name: str
    statements: tuple  # SQL statements, run in order in one transaction
    run: Optional[Callable[[sqlite3.Connection], object]] = None  # then this, in the same transaction


def statements(script: str) -> list[str]:
    """A script as single statements. ``executescript`` is not used: before Python 3.12 it commits first."""
    out, buf = [], ""
    for line in script.splitlines(keepends=True):
        buf += line
        if sqlite3.complete_statement(buf):
            out.append(buf.strip())
            buf = ""
    if buf.strip():
        raise ValueError(f"incomplete SQL statement: {buf.strip()[:80]!r}")
    return out


# The spans table, under a name: migration 11 builds it as spans_new and renames it. ``{added}``: the columns
# added after 11 (SPANS_ADDED), which 11's copy does not have yet.
SPANS_TABLE = """
CREATE TABLE IF NOT EXISTS {name} (
    row INTEGER PRIMARY KEY,
    trace_id TEXT NOT NULL,
    span_id TEXT NOT NULL,
    kind TEXT NOT NULL,              -- send | receive | delete | change_visibility | outcome
    time_ns INTEGER NOT NULL,        -- span end: when the SQS response arrived (contract section 2)
    start_ns INTEGER,                -- span start: just before the request was sent; NULL = not observed
    service_name TEXT,
    source TEXT, operation TEXT, queue TEXT NOT NULL, queue_url TEXT,
    message_id TEXT,                 -- messaging.message.id as carried by the span
    receipt_hmac TEXT, key_id TEXT,
    receive_count INTEGER, sent_ts_ms INTEGER, first_receive_ts_ms INTEGER,
    dlq_source_arn TEXT,             -- receives from a dead-letter queue: the queue SQS moved it from (3.1)
    result TEXT, error_code TEXT, visibility_timeout INTEGER,
    outcome TEXT, error_type TEXT, batch_entry_id TEXT,
    client_id TEXT, pid INTEGER, thread_name TEXT,
    faas_name TEXT, faas_invocation_id TEXT,
    order_scope TEXT, order_issued INTEGER, order_returned INTEGER,  -- call order in one process (contract 3)
    attributes TEXT NOT NULL,        -- every span attribute, as JSON
    mid TEXT,                        -- the message this row belongs to (see db.py)
    mid_source TEXT,                 -- 'span' | 'receipt_hash' | NULL (settle span not joined yet)
    receive_row INTEGER REFERENCES spans(row),  -- settle spans: the delivery whose handle was used
    body_dropped TEXT,               -- 'not_opted_in': the span carried a body, not stored (db.py)
    -- body_id: its trailbeam.body, stored once in bodies (body_table.py)
    body_id INTEGER{added}
);
"""
# The indexes of spans (schema 11). Each leads with time or is partial, so most of a new span's entries go where
# the newest spans' are, at the right edge of the B-tree: an export of a few hundred spans changes a few pages
# of it, not a page per span (QV-78). spans_identity is the duplicate check (an exporter retry sends the same
# span again, end time included). The receipt hash is indexed only where a join looks for it: receives, and
# settle spans not joined yet (db.py joins a settle span before it is inserted). There is no index on mid: a
# message's spans are the rows in its summary's rows_json (summary_table.py).
SPANS_INDEXES = """
CREATE UNIQUE INDEX IF NOT EXISTS spans_identity ON spans (time_ns, trace_id, span_id);
CREATE INDEX IF NOT EXISTS spans_receive_hash ON spans (receipt_hmac, key_id) WHERE kind = 'receive';
CREATE INDEX IF NOT EXISTS spans_unjoined_hash ON spans (receipt_hmac, key_id)
    WHERE kind IN ('delete', 'change_visibility') AND receive_row IS NULL;
CREATE INDEX IF NOT EXISTS spans_kind_time ON spans (kind, time_ns);
CREATE INDEX IF NOT EXISTS spans_receive_row ON spans (receive_row) WHERE receive_row IS NOT NULL;
"""
SPANS_COLUMNS = ("row, trace_id, span_id, kind, time_ns, start_ns, service_name, source, operation, queue, queue_url, "
                 "message_id, receipt_hmac, key_id, receive_count, sent_ts_ms, first_receive_ts_ms, dlq_source_arn, "
                 "result, error_code, visibility_timeout, outcome, error_type, batch_entry_id, client_id, pid, "
                 "thread_name, faas_name, faas_invocation_id, order_scope, order_issued, order_returned, attributes, "
                 "mid, mid_source, receive_row, body_dropped, body_id")  # those of schema 11, which migration 11 copies
# The columns added after schema 11, each by its own migration: service_version (16), the resource's
# service.version, NULL if not set.
SPANS_ADDED = """,
    service_version TEXT             -- the resource's service.version (QV-362)"""

# The test spans of trailbeam doctor (contract section 17): an ID and when this server stored it. Not spans, so no
# page, count or retention pass reads them; Store.record_probes keeps them at least a day.
PROBES_CREATE = """
CREATE TABLE IF NOT EXISTS probes (id TEXT PRIMARY KEY, received_ns INTEGER NOT NULL);
"""

# The schema a new file gets: the result of every migration below.
CREATE = SPANS_TABLE.format(name="spans", added=SPANS_ADDED) + SPANS_INDEXES + """
CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
""" + """
CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER PRIMARY KEY,
    applied_at_ms INTEGER,           -- when the file reached this version; NULL = not recorded (before 6)
    name TEXT NOT NULL,              -- the migration's name; 'create' for a file made at this version
    trailbeam_version TEXT           -- the Trailbeam that ran it; NULL for versions reached before 6
);
""" + summary_table.CREATE + body_table.CREATE + PROBES_CREATE + account_tables.SQLITE_CREATE

# Schema 1 was the first released (the CREATE of version 1 is in tests/server/test_migrations.py).
# Schema 1 had no start_ns. Its rows came from contract v1 spans, stamped only when the response arrived,
# so their start is not known: they are migrated with start_ns NULL. Schema 2 had no call-order columns;
# its rows came from contract v2 spans, which carry none, so they are migrated with NULLs. Schema 3 had no
# dlq_source_arn; no package emitted the attribute before contract 3.1, so its rows are migrated with NULL.
# Schema 4 had no body_dropped. Its rows were stored with every attribute, bodies included, so NULL
# (nothing dropped) is true of them; a schema 4 file can hold bodies for any queue a package captured.
# Schema 5 is Trailbeam 0.1. Schema 6 adds the schema_version table; it changes no span data.
# Schema 7 adds the summary tables the list pages read (summary_table.py) and builds them from the spans: one
# pass over the file in message order, about a minute per million messages (QV-73).
# Schema 8 indexes receive_row, the foreign key from settle spans to their receive. SQLite checks a foreign key
# on every delete of a parent row by looking the child up: without this index that is a scan of the whole
# table per deleted receive (QV-72: 65 s to delete 20 messages at 1.6M spans), so retention could not delete.
# Partial: most rows (sends, receives, outcomes, unjoined settles) have none. About 1 s to build per 1M spans.
# Schema 9 rebuilds the summary tables without an index on mid in message_queues, message_tenants and
# tenant_events: each was a B-tree with a random key per message, and together a third of the pages one export
# wrote (QV-73). Their rows are found from the message's lists in messages (summary_table.py). The tables are
# dropped and built again from the spans, as in 7 before.
# Schema 10 stores each body once (body_table.py): the bodies move out of the send and receive spans'
# attributes into the bodies table, one row per distinct body of a message, and the spans point at it. One pass
# over the stored bodies; the file only shrinks after a vacuum (trailbeam --vacuum), as with retention.
# Schema 11 (QV-78) cuts the pages an export writes: at 1M spans, a 512-span export wrote 2,318 pages at 9.
# - messages gets rows_json, its spans' row IDs, and spans loses spans_mid: the summary tables are dropped and
#   built again from the spans (summary_table.backfill, by spans_mid, which is still there), as in 9 before.
# - spans is rebuilt with indexes whose new entries go to the right edge of each B-tree (SPANS_INDEXES):
#   UNIQUE (trace_id, span_id) becomes spans_identity (time_ns, trace_id, span_id), and spans_hash becomes two
#   partial indexes, on receives and on unjoined settle spans. An inline UNIQUE can't be dropped, so the table
#   is copied, row IDs and all, and every index built again (body_table's spans_body too). That needs free
#   disk for a second copy of the table.
# Freshly built indexes are full, so for a while after the upgrade an insert splits more pages than it will later.
# Schema 12 (QV-128) adds tenant_rollup, the hourly rollup the Tenants page reads (rollup.py), and a partial index
# on the tenant events of repeat dead-letter moves. Messages moved to a dead-letter queue more than once get those
# events (their summary is rewritten from their spans; rare), then the rollup is built from every message's
# summary row: one pass over messages, no span read.
# Schema 13 (QV-148) adds probes: the test spans trailbeam doctor sends (contract section 17), kept apart from the
# spans so they never show as messages. It changes no span data.
# Schema 14 (QV-246) changes no tables. A send SQS deduplicated on a FIFO queue (timeline.deduplicated_sends) is no
# longer a ``sent`` tenant event, so the summaries of messages with more than one sent event and a FIFO queue are
# rewritten from their spans, which takes their extra sends out of the Tenants rollup too (refresh applies the
# rollup's deltas). Rare: other messages are not read past their summary row.
# Schema 15 (QV-225) adds the tables of people and tokens (account_tables.py), empty. It changes no span data. A
# Trailbeam before it (0.3.0 and earlier) refuses the file from then on.
# Schema 16 (QV-362) adds spans.service_version, the resource's service.version (the build that sent the span). Rows
# stored before it are migrated with NULL: the version was not kept. Their summaries are left as they are: a
# consumer without a version is listed as [service_name, client_id], as before (summary.py).
_REBUILD_SPANS = (statements(SPANS_TABLE.format(name="spans_new", added=""))
                  + [f"INSERT INTO spans_new ({SPANS_COLUMNS}) SELECT {SPANS_COLUMNS} FROM spans ORDER BY row",
                     "DROP TABLE spans",
                     "ALTER TABLE spans_new RENAME TO spans"]
                  + statements(SPANS_INDEXES)
                  + [q for q in statements(body_table.CREATE) if q.lstrip().upper().startswith("CREATE INDEX")])


def _to_11(conn: sqlite3.Connection) -> None:
    for t in summary_table.TABLES:
        conn.execute(f"DROP TABLE IF EXISTS {t}")
    for sql in statements(summary_table.CREATE):
        conn.execute(sql)
    summary_table.backfill(conn)
    for sql in _REBUILD_SPANS:
        conn.execute(sql)


def _to_12(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.row_factory = sqlite3.Row
    again = summary_table.Changes()
    for r in cur.execute("SELECT mid, events_json FROM messages WHERE moved"):
        if sum(e[1] == "dead_lettered" for e in json.loads(r["events_json"])) > 1:
            again.added[r["mid"]] = set()
    if again:
        summary_table.refresh(conn, again)
    summary_table.rebuild_rollup(conn)


def _to_16(conn: sqlite3.Connection) -> None:
    """Adds spans.service_version, unless the file has it: SQLite has no ADD COLUMN IF NOT EXISTS, and a file
    whose recorded version was set back by hand (the tests of the earlier migrations) has the column already."""
    if "service_version" not in {r[1] for r in conn.execute("PRAGMA table_info(spans)")}:
        conn.execute("ALTER TABLE spans ADD COLUMN service_version TEXT")


def _to_14(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.row_factory = sqlite3.Row
    again = summary_table.Changes()
    for r in cur.execute("SELECT mid, queues_json, events_json FROM messages WHERE queues_json LIKE '%.fifo%'"):
        if (any(q.endswith(".fifo") for q in json.loads(r["queues_json"]))
                and sum(e[1] == "sent" for e in json.loads(r["events_json"])) > 1):
            again.added[r["mid"]] = set()
    if again:
        summary_table.refresh(conn, again)


MIGRATIONS = (
    Migration(2, "spans.start_ns", ("ALTER TABLE spans ADD COLUMN start_ns INTEGER",)),
    Migration(3, "spans call order", ("ALTER TABLE spans ADD COLUMN order_scope TEXT",
                                      "ALTER TABLE spans ADD COLUMN order_issued INTEGER",
                                      "ALTER TABLE spans ADD COLUMN order_returned INTEGER")),
    Migration(4, "spans.dlq_source_arn", ("ALTER TABLE spans ADD COLUMN dlq_source_arn TEXT",)),
    Migration(5, "spans.body_dropped", ("ALTER TABLE spans ADD COLUMN body_dropped TEXT",)),
    Migration(6, "schema_version table", (
        """CREATE TABLE schema_version (version INTEGER PRIMARY KEY, applied_at_ms INTEGER,
               name TEXT NOT NULL, trailbeam_version TEXT)""",
        # the version the file was at, so its history starts where this table does
        """INSERT INTO schema_version (version, applied_at_ms, name)
           SELECT CAST(value AS INTEGER), NULL, 'before schema_version' FROM meta WHERE key = 'schema_version'""",
    )),
    # 7 and 9 create the tables, empty: 11 replaces them and builds them, so an upgrade builds them once. (9 built
    # them until 11 was added; a file that ran it is rebuilt by 11 all the same.)
    Migration(7, "message summary tables", tuple(statements(summary_table.CREATE))),
    Migration(8, "spans_receive_row index", (
        "CREATE INDEX spans_receive_row ON spans (receive_row) WHERE receive_row IS NOT NULL",)),
    Migration(9, "summary tables keyed for the pages", tuple(
        [f"DROP TABLE IF EXISTS {t}" for t in summary_table.TABLES] + statements(summary_table.CREATE))),
    Migration(10, "bodies stored once", tuple(
        ["ALTER TABLE spans ADD COLUMN body_id INTEGER"] + statements(body_table.CREATE)),
        run=body_table.move_inline),
    Migration(11, "spans indexes at the right edge", (), run=_to_11),
    Migration(12, "hourly tenant rollup", tuple(statements(summary_table.ROLLUP_CREATE)), run=_to_12),
    Migration(13, "probes (trailbeam doctor)", tuple(statements(PROBES_CREATE))),
    Migration(14, "FIFO deduplicated sends out of the tenant events", (), run=_to_14),
    Migration(15, "users, sessions and tokens", tuple(statements(account_tables.SQLITE_CREATE))),
    Migration(16, "spans.service_version", (), run=_to_16),
)
LATEST = MIGRATIONS[-1].version
VERSION_TABLE_FROM = 6  # the first version with the schema_version table
FIRST = MIGRATIONS[0].version - 1  # the oldest version a file can be at
assert [m.version for m in MIGRATIONS] == list(range(FIRST + 1, LATEST + 1)), "migrations go up one at a time"


def current_version(conn: sqlite3.Connection) -> Optional[int]:
    """The file's schema version; None for a file Trailbeam has never opened (no ``meta``, no ``spans``)."""
    tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}
    if "schema_version" in tables:
        v = conn.execute("SELECT MAX(version) FROM schema_version").fetchone()[0]
        if v is None:
            raise SchemaError("the schema_version table is empty")
        return v
    row = conn.execute("SELECT value FROM meta WHERE key = 'schema_version'").fetchone() if "meta" in tables else None
    if row is not None:
        try:
            return int(row[0])
        except ValueError:
            raise SchemaError(f"schema version {row[0]!r} is not a number") from None
    if tables & {"spans", "meta"}:
        # Trailbeam before 6 created meta, then the tables, then the version row, each committed on its own:
        # a start killed in between leaves no version. With no spans stored there is nothing to lose.
        if tables <= {"spans", "meta"} and ("spans" not in tables or
                                             conn.execute("SELECT 1 FROM spans LIMIT 1").fetchone() is None):
            return None
        raise SchemaError("the file has Trailbeam tables but no schema version")
    if tables:
        raise SchemaError(f"not a Trailbeam database (tables: {', '.join(sorted(tables))})")
    return None


def open_schema(conn: sqlite3.Connection, path: str = "") -> list[int]:
    """Bring the file to ``LATEST``: create it, or run the migrations it is missing, one transaction each.
    Returns the versions migrated to (empty for a new or current file). ``conn`` must be in autocommit
    (isolation_level None). Raises SchemaError (SchemaTooNew for a newer file) and leaves the file as it was."""
    done: list[int] = []
    version: Optional[int] = None
    try:
        while True:
            version = _locked_version(conn)
            try:
                if version is None:
                    conn.execute("DROP TABLE IF EXISTS spans")  # current_version: absent, or empty
                    for sql in statements(CREATE):
                        conn.execute(sql)
                    _record(conn, LATEST, "create")
                elif version != LATEST:
                    _check(version)
                    m = MIGRATIONS[version - FIRST]  # the one to version + 1
                    for sql in m.statements:
                        conn.execute(sql)
                    if m.run is not None:
                        m.run(conn)
                    _record(conn, m.version, m.name)
                    done.append(m.version)
                conn.execute("COMMIT")
            except BaseException:
                if conn.in_transaction:
                    conn.execute("ROLLBACK")
                raise
            if version is None or version == LATEST:
                return done
    except SchemaError as e:
        raise type(e)((f"{path}: " if path else "") + str(e)) from None
    except sqlite3.Error as e:
        at = f" (the file is at schema version {version})" if version is not None else ""
        raise SchemaError(f"{path + ': ' if path else ''}opening the schema failed{at}: {e}") from e


def _locked_version(conn: sqlite3.Connection) -> Optional[int]:
    """Start a write transaction, so no other process changes the schema, and read the version in it."""
    conn.execute("BEGIN IMMEDIATE")
    try:
        return current_version(conn)
    except BaseException:
        conn.execute("ROLLBACK")
        raise


def _check(version: int) -> None:
    if version > LATEST:
        raise SchemaTooNew(f"the database is at schema version {version}, written by a newer Trailbeam; this one "
                           f"({__version__}) knows versions up to {LATEST}. Run the newer Trailbeam, or a copy of "
                           "the file from before it was upgraded. Refusing to start, so nothing is lost")
    if version < FIRST:
        raise SchemaError(f"schema version {version} is older than any Trailbeam release ({FIRST})")


def _record(conn: sqlite3.Connection, version: int, name: str) -> None:
    if version >= VERSION_TABLE_FROM:
        conn.execute("INSERT INTO schema_version (version, applied_at_ms, name, trailbeam_version) VALUES (?, ?, ?, ?)",
                     (version, time.time_ns() // 1_000_000, name, __version__))
    conn.execute("INSERT INTO meta (key, value) VALUES ('schema_version', ?) "
                 "ON CONFLICT (key) DO UPDATE SET value = excluded.value", (str(version),))
