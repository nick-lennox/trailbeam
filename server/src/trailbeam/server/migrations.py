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

from . import __version__
from . import account_tables, alerts_store, body_table, dashboards_store, s3, summary_table


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
    kind TEXT NOT NULL,              -- send | receive | delete | change_visibility | outcome | ... (db.SPAN_KINDS)
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
# The SNS fan-out indexes (schema 20, QV-313, contract section 21): publishes by SNS message ID, and publishes and
# receives by the attributes that join them. Partial, so a span without SNS keys has no entry in them; the publish
# ID and envelope message ID stay in ``attributes`` (no column), and these expressions are how the queries in db.py
# name them. Not in SPANS_INDEXES, which migration 11 builds.
SNS_PID = """json_extract(attributes, '$."trailbeam.sns.publish_id"')"""
SNS_MSG = """json_extract(attributes, '$."trailbeam.sns.message_id"')"""
SNS_INDEXES = f"""
CREATE INDEX IF NOT EXISTS spans_publish_msg ON spans (message_id) WHERE kind = 'publish';
CREATE INDEX IF NOT EXISTS spans_sns_pid ON spans ({SNS_PID})
    WHERE kind IN ('publish', 'receive') AND {SNS_PID} IS NOT NULL;
CREATE INDEX IF NOT EXISTS spans_receive_sns ON spans ({SNS_MSG}) WHERE kind = 'receive' AND {SNS_MSG} IS NOT NULL;
"""
# Schema 22 (QV-306): RabbitMQ's settle kinds reject and channel_closed (otlp.py) join their delivery as delete and
# change_visibility join their receive, so the unjoined settle spans' index covers them. The kinds are written out:
# they are this migration's, not whatever store.SETTLE_KINDS says later. Not in SPANS_INDEXES, which migration 11 builds.
# Schema 23 (QV-489, contract section 25): EventBridge PutEvents entries by event ID, and receives by the event ID their
# body names (section 23.1). Partial, like SNS_INDEXES; the receive's event ID stays in ``attributes``.
EB_EVENT_ID = """json_extract(attributes, '$."trailbeam.eventbridge.event_id"')"""
EB_INDEXES = f"""
CREATE INDEX IF NOT EXISTS spans_put_event_msg ON spans (message_id) WHERE kind = 'put_event';
CREATE INDEX IF NOT EXISTS spans_receive_eb ON spans ({EB_EVENT_ID}) WHERE kind = 'receive' AND {EB_EVENT_ID} IS NOT NULL;
"""
# Schema 24 (QV-511, contract section 27): S3 uploads by bucket, object key and time, and receives whose body is an S3
# notification with the object key (section 23.2), by bucket and event time. Partial, like EB_INDEXES.
S3_BUCKET = """json_extract(attributes, '$."trailbeam.s3.bucket"')"""
S3_TIME = """json_extract(attributes, '$."trailbeam.s3.event_time_ms"')"""
S3_KEY = """json_extract(attributes, '$."trailbeam.s3.object_key"')"""
S3_UPLOAD_INDEX = """
CREATE INDEX IF NOT EXISTS spans_s3_upload ON spans (queue, message_id, time_ns) WHERE kind = 's3_upload';
"""
S3_INDEXES = S3_UPLOAD_INDEX + f"""CREATE INDEX IF NOT EXISTS spans_receive_s3 ON spans ({S3_BUCKET}, {S3_TIME}) WHERE kind = 'receive' AND {S3_KEY} IS NOT NULL;
"""
# Schema 25 (QV-563): the receives' S3 notifications by bucket, URL-decoded object key and event time, one row per
# receive span, as Postgres's s3_receives (pg_schema.py). The key is URL-encoded in ``attributes``, so no index on
# them can find one key's receives: spans_receive_s3 found the bucket's in the window and the key was compared after,
# so a busy bucket's could fall past the query's limit. The row goes with its span (ON DELETE CASCADE).
S3_RECEIVES = """
CREATE TABLE IF NOT EXISTS s3_receives (
    row INTEGER PRIMARY KEY REFERENCES spans(row) ON DELETE CASCADE,  -- the receive span
    bucket TEXT NOT NULL,
    object_key TEXT NOT NULL,        -- URL-decoded (s3.notification)
    event_time_ms INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS s3_receives_key ON s3_receives (bucket, object_key, event_time_ms);
"""
S3_RECEIVE_SQL = "INSERT OR REPLACE INTO s3_receives (row, bucket, object_key, event_time_ms) VALUES (?, ?, ?, ?)"


def s3_receive(row: int, attributes) -> Optional[tuple]:
    """The s3_receives row of a receive span with these attributes (a dict or its JSON), or None: S3_RECEIVE_SQL's
    values."""
    n = s3.notification({"attributes": attributes})
    return None if n is None else (row, n.bucket, n.key, n.event_time_ms)
UNJOINED_INDEX_22 = """
DROP INDEX IF EXISTS spans_unjoined_hash;
CREATE INDEX IF NOT EXISTS spans_unjoined_hash ON spans (receipt_hmac, key_id)
    WHERE kind IN ('delete', 'change_visibility', 'reject', 'channel_closed') AND receive_row IS NULL;
"""
SPANS_COLUMNS = ("row, trace_id, span_id, kind, time_ns, start_ns, service_name, source, operation, queue, queue_url, "
                 "message_id, receipt_hmac, key_id, receive_count, sent_ts_ms, first_receive_ts_ms, dlq_source_arn, "
                 "result, error_code, visibility_timeout, outcome, error_type, batch_entry_id, client_id, pid, "
                 "thread_name, faas_name, faas_invocation_id, order_scope, order_issued, order_returned, attributes, "
                 "mid, mid_source, receive_row, body_dropped, body_id")  # those of schema 11, which migration 11 copies
# The columns added after schema 11, each by its own migration: service_version (16), the resource's
# service.version, NULL if not set; seq (29), when the span reached this server (store.next_seq), NULL on rows
# stored before it.
SPANS_ADDED = """,
    service_version TEXT,            -- the resource's service.version (QV-362)
    seq INTEGER                      -- arrival on the server's clock: ms * 1000 plus a counter (QV-593)"""

# Schema 19 (QV-394): the steps /api/events counts, per hour, queue, service and kind (events.py, db.py). Kept
# exact: ingest adds the spans it stores, retention takes out the ones it deletes. Purges are not steps.
STEP_ROLLUP_CREATE = """
CREATE TABLE IF NOT EXISTS step_rollup (
    hour INTEGER NOT NULL,               -- time_ns // 3,600 s
    queue TEXT NOT NULL,
    service TEXT NOT NULL,               -- the span's service.name, '' for none
    kind TEXT NOT NULL,                  -- events.KINDS
    n INTEGER NOT NULL,                  -- the spans
    PRIMARY KEY (hour, queue, service, kind)
) WITHOUT ROWID;
"""
STEP_ROLLUP_BACKFILL = """INSERT INTO step_rollup (hour, queue, service, kind, n)
SELECT time_ns / 3600000000000, queue, COALESCE(service_name, ''), kind, COUNT(*) FROM spans
 WHERE kind IN ('send', 'receive', 'delete', 'change_visibility', 'outcome')
 GROUP BY 1, 2, 3, 4"""

# The test spans of trailbeam doctor (contract section 17): an ID and when this server stored it. Not spans, so no
# page, count or retention pass reads them; Store.record_probes keeps them at least a day.
PROBES_CREATE = """
CREATE TABLE IF NOT EXISTS probes (id TEXT PRIMARY KEY, received_ns INTEGER NOT NULL);
"""

# The schema a new file gets: the result of every migration below.
CREATE = SPANS_TABLE.format(name="spans", added=SPANS_ADDED) + SPANS_INDEXES + SNS_INDEXES + UNJOINED_INDEX_22 + EB_INDEXES + S3_UPLOAD_INDEX + S3_RECEIVES + """
CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
""" + """
CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER PRIMARY KEY,
    applied_at_ms INTEGER,           -- when the file reached this version; NULL = not recorded (before 6)
    name TEXT NOT NULL,              -- the migration's name; 'create' for a file made at this version
    trailbeam_version TEXT           -- the Trailbeam that ran it; NULL for versions reached before 6
);
""" + summary_table.CREATE + body_table.CREATE + PROBES_CREATE + account_tables.SQLITE_CREATE + f"""
{account_tables.SQLITE_ADD_PROXY};
""" + STEP_ROLLUP_CREATE + account_tables.SQLITE_OIDC + alerts_store.SQLITE_CREATE + dashboards_store.SQLITE_CREATE + account_tables.SQLITE_PASSKEYS + alerts_store.SQLITE_OUTBOX

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
# Schema 17 (QV-363) adds queue_events, the queue page's events by queue and time (queues.py), and
# messages.qevents_json, the list of a message's rows in it. Both are built from every message's spans: one pass
# over messages, reading each message's spans by row ID, as migration 12 does with the summaries.
# Schema 18 (QV-227) adds users.proxy_linked_at_ms: which people may sign in through a proxy (proxy_auth.py). NULL
# for everyone stored before it, so no local account can be signed in to through the proxy until an admin links it.
# Schema 19 (QV-394) adds step_rollup (STEP_ROLLUP_CREATE), built from the spans in one pass: GROUP BY over every
# span, about a second per million. It changes no span data. It rebuilds the table rather than adding to it, so a file
# whose recorded version is older than its tables (a restore, or a test rolling the number back) still ends exact.
# Schema 20 (QV-313) adds the SNS fan-out indexes (SNS_INDEXES). Publish spans are rows of spans (kind 'publish', no
# mid); no file before it has any, and no receive before it carries the SNS attributes, so the indexes are built
# from one scan of spans and start empty. It changes no span data.
# Schema 21 (QV-228) adds the two single sign-on tables (account_tables.py: oidc_logins, oidc_identities), empty.
# Schema 22 (QV-306) takes RabbitMQ spans (contract/span-contract-amqp.md, schema version 4): the unjoined settle
# spans' index is built again with its two new settle kinds (UNJOINED_INDEX_22), from one scan of spans. No file
# before it holds a RabbitMQ span (a Trailbeam before 4.0 rejects them), and an SQS message's key is its message ID
# as before (systems.py), so it changes no span data and no summary.
# Schema 23 (QV-489) adds the EventBridge indexes (EB_INDEXES). put_event spans are rows of spans (kind 'put_event',
# no mid); no file before it has any, so the first index starts empty. Receives since 3.5 carry the event ID, so
# the second is built from one scan of spans. It changes no span data.
# Schema 24 (QV-511) adds the S3 upload indexes (S3_INDEXES). s3_upload spans are rows of spans (kind 's3_upload', no
# mid); no file before it has any, so the first index starts empty. Receives since 3.5 carry the S3 fields, the key
# only on queues whose body is captured, so the second is built from one scan of spans. It changes no span data.
# Schema 25 (QV-563) adds s3_receives (S3_RECEIVES), built from the receives spans_receive_s3 finds (the key decoded
# in Python, one pass over the S3 receives only), then drops spans_receive_s3, which nothing reads any more. It
# changes no span data.
# Schema 26 (QV-650) adds the alert tables (alerts_store.py: rules, channels, state, checks and leases), empty. It
# changes no span data.
# Schema 27 (QV-652) adds the dashboard tables (dashboards_store.py: dashboards and their widgets), empty. It changes
# no span data.
# Schema 28 (QV-665) adds the passkey tables (account_tables.py: passkeys, passkey_challenges), empty. It changes no
# span data.
# Schema 29 (QV-593) adds spans.seq, when the span reached this server (store.next_seq), for triage: a regression or
# "ignore until N more" counts the failures that arrived after the resolve or ignore, on the server's clock, never
# the reporting host's. Rows stored before it keep NULL: when they arrived is not known, so none of them ever counts
# as a new failure (as on the hosted app). Adding a nullable column rewrites no row. No index: triage reads the seq
# of spans it has already read by message.
# Schema 30 (QV-674) adds alert_outbox (alerts_store.py: the changes a check is sending), empty. It changes no span
# data.
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


def _to_17(conn: sqlite3.Connection) -> None:
    # A file built by migration 7, 9 or 11 under this code has the column and the table already (they create the
    # summary tables as summary_table.CREATE is now). SQLite has no ADD COLUMN IF NOT EXISTS.
    if "qevents_json" not in {r[1] for r in conn.execute("PRAGMA table_info(messages)")}:
        conn.execute("ALTER TABLE messages ADD COLUMN qevents_json TEXT NOT NULL DEFAULT '[]'")
    for sql in statements(summary_table.QUEUE_CREATE):
        conn.execute(sql)
    summary_table.rebuild_queue_events(conn)


def _to_18(conn: sqlite3.Connection) -> None:
    """Adds users.proxy_linked_at_ms, unless the file has it (as _to_16)."""
    if account_tables.PROXY_COLUMN not in {r[1] for r in conn.execute("PRAGMA table_info(users)")}:
        conn.execute(account_tables.SQLITE_ADD_PROXY)


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


def _to_29(conn: sqlite3.Connection) -> None:
    """Adds spans.seq, unless the file has it (as _to_16)."""
    if "seq" not in {r[1] for r in conn.execute("PRAGMA table_info(spans)")}:
        conn.execute("ALTER TABLE spans ADD COLUMN seq INTEGER")


def _to_25(conn: sqlite3.Connection) -> None:
    for sql in statements(S3_RECEIVES):
        conn.execute(sql)
    # spans_receive_s3 is there at 24; a file whose recorded version was set back by hand may not have it
    have = conn.execute("SELECT 1 FROM sqlite_master WHERE type = 'index' AND name = 'spans_receive_s3'").fetchone()
    by = "INDEXED BY spans_receive_s3 " if have else ""
    cur = conn.execute(f"SELECT row, attributes FROM spans {by}WHERE kind = 'receive' AND {S3_KEY} IS NOT NULL")
    while True:
        part = cur.fetchmany(1000)
        if not part:
            break
        conn.executemany(S3_RECEIVE_SQL, [v for v in (s3_receive(r, a) for r, a in part) if v is not None])
    conn.execute("DROP INDEX IF EXISTS spans_receive_s3")


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
    Migration(17, "queue events (the queue page)", (), run=_to_17),
    Migration(18, "users.proxy_linked_at_ms", (), run=_to_18),
    Migration(19, "hourly step rollup (/api/events)", tuple(statements(STEP_ROLLUP_CREATE))
              + ("DELETE FROM step_rollup", STEP_ROLLUP_BACKFILL)),
    Migration(20, "SNS fan-out indexes", tuple(statements(SNS_INDEXES))),
    Migration(21, "OpenID Connect sign-ins and identities", tuple(statements(account_tables.SQLITE_OIDC))),
    Migration(22, "RabbitMQ settle kinds in the unjoined settle index", tuple(statements(UNJOINED_INDEX_22))),
    Migration(23, "EventBridge PutEvents indexes", tuple(statements(EB_INDEXES))),
    Migration(24, "S3 upload indexes", tuple(statements(S3_INDEXES))),
    Migration(25, "S3 receives by decoded object key", (), run=_to_25),
    Migration(26, "alert rules, channels, state and checks", tuple(statements(alerts_store.SQLITE_CREATE))),
    Migration(27, "dashboards and their widgets", tuple(statements(dashboards_store.SQLITE_CREATE))),
    Migration(28, "passkeys and their challenges", tuple(statements(account_tables.SQLITE_PASSKEYS))),
    Migration(29, "spans.seq: when each span reached the server", (), run=_to_29),
    Migration(30, "alert outbox", tuple(statements(alerts_store.SQLITE_OUTBOX))),
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
