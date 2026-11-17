"""SQLite storage for Trailbeam spans, and the receipt-hash join of settle spans to receives.

Every Trailbeam span is one row in ``spans``. ``mid`` is the message the row belongs to:

- send, receive, outcome: the span's own ``messaging.message.id``;
- delete, change_visibility: the message ID of the ``trailbeam receive`` with the same receipt-handle hash
  and key ID (``mid_source = 'receipt_hash'``, and ``receive_row`` points at that receive). If no such
  receive has arrived yet, the settle span's own message ID, if it has one (``mid_source = 'span'``).

The join runs in both directions at insert time, because spans arrive in any order: a delete can be
exported before the receive it belongs to (different processes, different export batches). The span
contract (section 8) makes the hash join the one that must work; the message ID on a settle span is
an optimisation.

Message bodies (contract section 15) are kept only for the queues named in ``store_bodies``
(``TRAILBEAM_STORE_BODIES``), off by default. A package exports a body only for queues it was told to
capture; this is the second gate, on the server, so turning capture on in a package does not by itself
put bodies on disk. For any other queue the ``trailbeam.body*`` attributes that carry or describe the
body are removed before the row is written, and ``body_dropped`` records why.
"""
from __future__ import annotations

import contextlib
import json
import sqlite3
import threading
from typing import Iterable, Iterator, Optional

from .otlp import RECORD_FIELDS

SETTLE_KINDS = ("delete", "change_visibility")

# The body and the attributes that describe it (contract 15.2). trailbeam.body.capture_status is kept:
# it is the package's reason, carries no message content, and the Body tab needs it either way.
BODY_ATTRIBUTES = ("trailbeam.body", "trailbeam.body.format", "trailbeam.body.redacted_count",
                   "trailbeam.body.envelope")
BODY_DROPPED_NOT_OPTED_IN = "not_opted_in"  # spans.body_dropped: the queue is not in store_bodies

SCHEMA = """
CREATE TABLE IF NOT EXISTS spans (
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
    mid TEXT,                        -- the message this row belongs to (see module docstring)
    mid_source TEXT,                 -- 'span' | 'receipt_hash' | NULL (settle span not joined yet)
    receive_row INTEGER REFERENCES spans(row),  -- settle spans: the delivery whose handle was used
    body_dropped TEXT,               -- 'not_opted_in': the span carried a body, not stored (module docstring)
    UNIQUE (trace_id, span_id)
);
CREATE INDEX IF NOT EXISTS spans_mid ON spans (mid, time_ns);
CREATE INDEX IF NOT EXISTS spans_hash ON spans (receipt_hmac, key_id);
CREATE INDEX IF NOT EXISTS spans_kind_time ON spans (kind, time_ns);
CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
"""
SCHEMA_VERSION = "5"

# Schema 1 had no start_ns. Its rows came from contract v1 spans, stamped only when the response arrived,
# so their start is not known: they are migrated with start_ns NULL. Schema 2 had no call-order columns;
# its rows came from contract v2 spans, which carry none, so they are migrated with NULLs. Schema 3 had no
# dlq_source_arn; no package emitted the attribute before contract 3.1, so its rows are migrated with NULL.
# Schema 4 had no body_dropped. Its rows were stored with every attribute, bodies included, so NULL
# (nothing dropped) is true of them; a schema 4 file can hold bodies for any queue a package captured.
_MIGRATIONS = {
    "1": ("2", ["ALTER TABLE spans ADD COLUMN start_ns INTEGER"]),
    "2": ("3", ["ALTER TABLE spans ADD COLUMN order_scope TEXT", "ALTER TABLE spans ADD COLUMN order_issued INTEGER",
                "ALTER TABLE spans ADD COLUMN order_returned INTEGER"]),
    "3": ("4", ["ALTER TABLE spans ADD COLUMN dlq_source_arn TEXT"]),
    "4": ("5", ["ALTER TABLE spans ADD COLUMN body_dropped TEXT"]),
}

# The two receipt-hash joins run once per settle or receive span, inside the insert transaction, so they
# must be index lookups at every size. INDEXED BY pins spans_hash: the server never runs ANALYZE, and without
# statistics SQLite's planner picks spans_kind_time (kind=?) for the settle join, which scans every stored
# receive (QV-68: 99.95% of insert time at 10M spans). With INDEXED BY the plan cannot change with the data,
# the statistics or the SQLite version; if the index were ever missing the statement fails loudly instead of
# scanning. A hash matches one delivery's receive and its settle spans, so the lookup returns a few rows.
SETTLE_JOIN_SQL = """SELECT row, message_id FROM spans INDEXED BY spans_hash
   WHERE kind = 'receive' AND receipt_hmac = ? AND key_id = ?
   ORDER BY (time_ns <= ?) DESC,
            CASE WHEN time_ns <= ? THEN -time_ns ELSE time_ns END
   LIMIT 1"""
RECEIVE_JOIN_SQL = """UPDATE spans INDEXED BY spans_hash SET mid = ?, mid_source = 'receipt_hash', receive_row = ?
   WHERE kind IN ('delete', 'change_visibility') AND receive_row IS NULL
     AND receipt_hmac = ? AND key_id = ?"""


def parse_queue_list(text: str) -> frozenset:
    """``orders, payments`` -> {"orders", "payments"}. ``*`` stands for every queue, as in the packages."""
    return frozenset(filter(None, (q.strip() for q in (text or "").split(","))))


def gate_body(attributes: dict, queue: Optional[str], store_bodies: frozenset) -> tuple[dict, Optional[str]]:
    """The attributes to store, and the body_dropped value: None if nothing was dropped."""
    if "*" in store_bodies or queue in store_bodies:
        return attributes, None
    if not any(k in attributes for k in BODY_ATTRIBUTES):
        return attributes, None
    return {k: v for k, v in attributes.items() if k not in BODY_ATTRIBUTES}, BODY_DROPPED_NOT_OPTED_IN


class Database:
    """Thread-safe. One writer connection, serialised by ``lock``; on a file, reads use separate read-only
    connections and never take that lock.

    The file is in WAL mode, so a reader works on the last committed snapshot while the writer appends to
    the log: a page does not wait for an export's transaction, and an export does not wait for a page.
    Read connections are pooled (``MAX_IDLE_READERS`` kept open), so a request does not open the file.
    ``:memory:`` has no file to share, so there every read goes through the writer connection and its lock.
    """

    MAX_IDLE_READERS = 8

    def __init__(self, path: str = ":memory:", *, store_bodies: Iterable[str] = ()):
        self.path = path
        self.store_bodies = frozenset(store_bodies)  # queues whose bodies are kept; empty = none (default)
        self.lock = threading.RLock()
        self.conn = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
        self.conn.row_factory = sqlite3.Row
        self.shared_reads = path != ":memory:"  # separate read connections (WAL); False: reads take self.lock
        self._idle_readers: list[sqlite3.Connection] = []
        self._readers_lock = threading.Lock()
        self._closed = False
        with self.lock:
            if self.shared_reads:
                mode = self.conn.execute("PRAGMA journal_mode=WAL").fetchone()[0]
                if mode.lower() != "wal":
                    raise RuntimeError(f"{path}: SQLite could not switch the file to WAL mode (got {mode})")
            self.conn.execute("PRAGMA foreign_keys=ON")
            self.conn.execute("CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
            cur = self.conn.execute("SELECT value FROM meta WHERE key = 'schema_version'").fetchone()
            version = cur[0] if cur is not None else None
            while version is not None and version != SCHEMA_VERSION:
                if version not in _MIGRATIONS:
                    raise RuntimeError(f"{path}: database schema {version}, this version needs {SCHEMA_VERSION}")
                version, statements = _MIGRATIONS[version]
                for sql in statements:
                    self.conn.execute(sql)
                self.conn.execute("UPDATE meta SET value = ? WHERE key = 'schema_version'", (version,))
            self.conn.executescript(SCHEMA)
            if cur is None:
                self.conn.execute("INSERT INTO meta VALUES ('schema_version', ?)", (SCHEMA_VERSION,))

    def close(self) -> None:
        with self._readers_lock:
            self._closed = True
            idle, self._idle_readers = self._idle_readers, []
        for c in idle:
            c.close()
        with self.lock:
            self.conn.close()

    @contextlib.contextmanager
    def reader(self) -> Iterator[sqlite3.Connection]:
        """A connection for reads. On a file: a pooled read-only connection, without the writer lock."""
        if not self.shared_reads:
            with self.lock:
                yield self.conn
            return
        with self._readers_lock:
            c = self._idle_readers.pop() if self._idle_readers else None
        if c is None:
            c = sqlite3.connect(self.path, check_same_thread=False, isolation_level=None, timeout=30)
            c.row_factory = sqlite3.Row
            c.execute("PRAGMA query_only = ON")
        try:
            yield c
        except BaseException:
            c.close()  # it may be mid-statement; a new one is cheap
            raise
        with self._readers_lock:
            if not self._closed and len(self._idle_readers) < self.MAX_IDLE_READERS:
                self._idle_readers.append(c)
                return
        c.close()

    # ---- writes ---------------------------------------------------------------------------

    def insert(self, records: Iterable[dict]) -> tuple[int, int]:
        """Insert records (from otlp.decode) in one transaction. Returns (inserted, duplicates).

        A span already stored (same trace and span ID, e.g. an exporter retry) is skipped.
        """
        inserted = dupes = 0
        cols = RECORD_FIELDS + ["attributes", "mid", "mid_source", "body_dropped"]
        sql = f"INSERT OR IGNORE INTO spans ({', '.join(cols)}) VALUES ({', '.join('?' * len(cols))})"
        with self.lock:
            c = self.conn
            c.execute("BEGIN IMMEDIATE")
            try:
                for r in records:
                    own = r.get("message_id")
                    attrs, dropped = gate_body(r.get("attributes", {}), r.get("queue"), self.store_bodies)
                    values = [r.get(f) for f in RECORD_FIELDS] + [
                        json.dumps(attrs, sort_keys=True, default=str), own, "span" if own else None, dropped]
                    cur = c.execute(sql, values)
                    if cur.rowcount == 0:
                        dupes += 1
                        continue
                    inserted += 1
                    row = cur.lastrowid
                    if r["kind"] in SETTLE_KINDS:
                        self._join_settle(row, r)
                    elif r["kind"] == "receive":
                        self._join_receive(row, r)
                c.execute("COMMIT")
            except BaseException:
                c.execute("ROLLBACK")
                raise
        return inserted, dupes

    def _join_settle(self, row: int, r: dict) -> None:
        """Join a new settle span to the receive that returned its receipt handle."""
        # SQS issues a new receipt handle for every delivery, so there is normally one match. If an
        # emulator reuses handles, take the latest receive at or before the settle, else the first.
        rec = self.conn.execute(SETTLE_JOIN_SQL, (r["receipt_hmac"], r["key_id"], r["time_ns"], r["time_ns"])).fetchone()
        if rec is not None:
            self.conn.execute("UPDATE spans SET mid = ?, mid_source = 'receipt_hash', receive_row = ? WHERE row = ?",
                              (rec["message_id"], rec["row"], row))

    def _join_receive(self, row: int, r: dict) -> None:
        """Join settle spans that arrived before this receive."""
        self.conn.execute(RECEIVE_JOIN_SQL, (r["message_id"], row, r["receipt_hmac"], r["key_id"]))

    # ---- reads ----------------------------------------------------------------------------

    def message_rows(self, message_id: str) -> list[sqlite3.Row]:
        """Every row of one message, in time order."""
        with self.reader() as c:
            return c.execute("SELECT * FROM spans WHERE mid = ? ORDER BY time_ns, row", (message_id,)).fetchall()

    def query(self, sql: str, params: tuple = ()) -> list[sqlite3.Row]:
        """A read-only query (on a file, a write fails: read connections are query_only)."""
        with self.reader() as c:
            return c.execute(sql, params).fetchall()

    def counts(self) -> dict:
        """Exact counts. Each one reads the whole table: seconds at 10M spans."""
        with self.reader() as c:
            kinds = {k: n for k, n in c.execute("SELECT kind, COUNT(*) FROM spans GROUP BY kind")}
            unjoined = c.execute(
                "SELECT COUNT(*) FROM spans WHERE kind IN ('delete', 'change_visibility') AND receive_row IS NULL"
            ).fetchone()[0]
            messages = c.execute("SELECT COUNT(DISTINCT mid) FROM spans WHERE mid IS NOT NULL").fetchone()[0]
        return {"spans": kinds, "messages": messages, "unjoined_settles": unjoined}
