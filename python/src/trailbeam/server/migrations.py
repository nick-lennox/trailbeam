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

import sqlite3
import time
from dataclasses import dataclass
from typing import Callable, Optional

from .. import __version__
from . import summary_table


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


# The spans table, under a name: migration 10 builds it as spans_new and renames it.
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
    body_dropped TEXT                -- 'not_opted_in': the span carried a body, not stored (db.py)
);
"""
# The indexes of spans (schema 10). Every one but spans_mid leads with time or is partial, so a new span's
# entries go where the newest spans' are, at the right edge of each B-tree: an export of a few hundred spans
# changes a few pages of each, not a page per span (QV-78). spans_identity is the duplicate check (an exporter
# retry sends the same span again, end time included). The receipt hash is indexed only where a join looks
# for it: receives, and settle spans not joined yet (db.py joins a settle span before it is inserted).
SPANS_INDEXES = """
CREATE UNIQUE INDEX IF NOT EXISTS spans_identity ON spans (time_ns, trace_id, span_id);
CREATE INDEX IF NOT EXISTS spans_mid ON spans (mid, time_ns);
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
                 "mid, mid_source, receive_row, body_dropped")

# The schema a new file gets: the result of every migration below.
CREATE = SPANS_TABLE.format(name="spans") + SPANS_INDEXES + """
CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
""" + """
CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER PRIMARY KEY,
    applied_at_ms INTEGER,           -- when the file reached this version; NULL = not recorded (before 6)
    name TEXT NOT NULL,              -- the migration's name; 'create' for a file made at this version
    trailbeam_version TEXT           -- the Trailbeam that ran it; NULL for versions reached before 6
);
""" + summary_table.CREATE

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
# Schema 10 rebuilds spans with indexes whose new entries go to the right edge of each B-tree (SPANS_INDEXES):
# UNIQUE (trace_id, span_id) becomes spans_identity (time_ns, trace_id, span_id), and spans_hash becomes two
# partial indexes, on receives and on unjoined settle spans. At 1M spans a 512-span export wrote 2,318 pages
# before and 1,866 after (QV-78). An inline UNIQUE can't be dropped, so the table is copied, row IDs and all, and
# every index built again: about 3 minutes per 1M spans, with free disk for a second copy of the table. Freshly
# built indexes are full, so for a while after the upgrade an insert splits more pages than it will later.
_REBUILD_SPANS = (statements(SPANS_TABLE.format(name="spans_new"))
                  + [f"INSERT INTO spans_new ({SPANS_COLUMNS}) SELECT {SPANS_COLUMNS} FROM spans ORDER BY row",
                     "DROP TABLE spans",
                     "ALTER TABLE spans_new RENAME TO spans"]
                  + statements(SPANS_INDEXES))
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
    # 7 creates the tables, empty: 9 replaces them and builds them, so an upgrade from 6 or older builds once.
    Migration(7, "message summary tables", tuple(statements(summary_table.CREATE))),
    Migration(8, "spans_receive_row index", (
        "CREATE INDEX spans_receive_row ON spans (receive_row) WHERE receive_row IS NOT NULL",)),
    Migration(9, "summary tables keyed for the pages", tuple(
        [f"DROP TABLE IF EXISTS {t}" for t in summary_table.TABLES] + statements(summary_table.CREATE)),
        run=summary_table.backfill),
    Migration(10, "spans indexes at the right edge", tuple(_REBUILD_SPANS)),
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
