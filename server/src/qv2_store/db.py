"""SQLite storage for qv2 spans, and the receipt-hash join of settle spans to receives.

Every qv2 span is one row in ``spans``. ``mid`` is the message the row belongs to:

- send, receive, outcome: the span's own ``messaging.message.id``;
- delete, change_visibility: the message ID of the ``qv2 receive`` with the same receipt-handle hash
  and key ID (``mid_source = 'receipt_hash'``, and ``receive_row`` points at that receive). If no such
  receive has arrived yet, the settle span's own message ID, if it has one (``mid_source = 'span'``).

The join runs in both directions at insert time, because spans arrive in any order: a delete can be
exported before the receive it belongs to (different processes, different export batches). The span
contract (section 8) makes the hash join the one that must work; the message ID on a settle span is
an optimisation.
"""
from __future__ import annotations

import json
import sqlite3
import threading
from typing import Iterable, Optional

from .otlp import RECORD_FIELDS

SETTLE_KINDS = ("delete", "change_visibility")

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
    UNIQUE (trace_id, span_id)
);
CREATE INDEX IF NOT EXISTS spans_mid ON spans (mid, time_ns);
CREATE INDEX IF NOT EXISTS spans_hash ON spans (receipt_hmac, key_id);
CREATE INDEX IF NOT EXISTS spans_kind_time ON spans (kind, time_ns);
CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
"""
SCHEMA_VERSION = "4"
# Schema 1 had no start_ns. Its rows came from contract v1 spans, stamped only when the response arrived,
# so their start is not known: they are migrated with start_ns NULL. Schema 2 had no call-order columns;
# its rows came from contract v2 spans, which carry none, so they are migrated with NULLs. Schema 3 had no
# dlq_source_arn; no package emitted the attribute before contract 3.1, so its rows are migrated with NULL.
_MIGRATIONS = {
    "1": ("2", ["ALTER TABLE spans ADD COLUMN start_ns INTEGER"]),
    "2": ("3", ["ALTER TABLE spans ADD COLUMN order_scope TEXT", "ALTER TABLE spans ADD COLUMN order_issued INTEGER",
                "ALTER TABLE spans ADD COLUMN order_returned INTEGER"]),
    "3": ("4", ["ALTER TABLE spans ADD COLUMN dlq_source_arn TEXT"]),
}


class Store:
    """Thread-safe: one connection, serialised by a lock. Enough for 0.1's single container."""

    def __init__(self, path: str = ":memory:"):
        self.path = path
        self.lock = threading.RLock()
        self.conn = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
        self.conn.row_factory = sqlite3.Row
        with self.lock:
            if path != ":memory:":
                self.conn.execute("PRAGMA journal_mode=WAL")
            self.conn.execute("PRAGMA foreign_keys=ON")
            self.conn.execute("CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
            cur = self.conn.execute("SELECT value FROM meta WHERE key = 'schema_version'").fetchone()
            version = cur[0] if cur is not None else None
            while version is not None and version != SCHEMA_VERSION:
                if version not in _MIGRATIONS:
                    raise RuntimeError(f"{path}: store schema {version}, this version needs {SCHEMA_VERSION}")
                version, statements = _MIGRATIONS[version]
                for sql in statements:
                    self.conn.execute(sql)
                self.conn.execute("UPDATE meta SET value = ? WHERE key = 'schema_version'", (version,))
            self.conn.executescript(SCHEMA)
            if cur is None:
                self.conn.execute("INSERT INTO meta VALUES ('schema_version', ?)", (SCHEMA_VERSION,))

    def close(self) -> None:
        with self.lock:
            self.conn.close()

    # ---- writes ---------------------------------------------------------------------------

    def insert(self, records: Iterable[dict]) -> tuple[int, int]:
        """Insert records (from otlp.decode) in one transaction. Returns (inserted, duplicates).

        A span already stored (same trace and span ID, e.g. an exporter retry) is skipped.
        """
        inserted = dupes = 0
        cols = RECORD_FIELDS + ["attributes", "mid", "mid_source"]
        sql = f"INSERT OR IGNORE INTO spans ({', '.join(cols)}) VALUES ({', '.join('?' * len(cols))})"
        with self.lock:
            c = self.conn
            c.execute("BEGIN IMMEDIATE")
            try:
                for r in records:
                    own = r.get("message_id")
                    values = [r.get(f) for f in RECORD_FIELDS] + [
                        json.dumps(r.get("attributes", {}), sort_keys=True, default=str),
                        own, "span" if own else None]
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
        rec = self.conn.execute(
            """SELECT row, message_id FROM spans
               WHERE kind = 'receive' AND receipt_hmac = ? AND key_id = ?
               ORDER BY (time_ns <= ?) DESC,
                        CASE WHEN time_ns <= ? THEN -time_ns ELSE time_ns END
               LIMIT 1""",
            (r["receipt_hmac"], r["key_id"], r["time_ns"], r["time_ns"])).fetchone()
        if rec is not None:
            self.conn.execute("UPDATE spans SET mid = ?, mid_source = 'receipt_hash', receive_row = ? WHERE row = ?",
                              (rec["message_id"], rec["row"], row))

    def _join_receive(self, row: int, r: dict) -> None:
        """Join settle spans that arrived before this receive."""
        self.conn.execute(
            """UPDATE spans SET mid = ?, mid_source = 'receipt_hash', receive_row = ?
               WHERE kind IN ('delete', 'change_visibility') AND receive_row IS NULL
                 AND receipt_hmac = ? AND key_id = ?""",
            (r["message_id"], row, r["receipt_hmac"], r["key_id"]))

    # ---- reads ----------------------------------------------------------------------------

    def message_rows(self, message_id: str) -> list[sqlite3.Row]:
        """Every row of one message, in time order."""
        with self.lock:
            return self.conn.execute(
                "SELECT * FROM spans WHERE mid = ? ORDER BY time_ns, row", (message_id,)).fetchall()

    def query(self, sql: str, params: tuple = ()) -> list[sqlite3.Row]:
        with self.lock:
            return self.conn.execute(sql, params).fetchall()

    def counts(self) -> dict:
        with self.lock:
            kinds = {k: n for k, n in self.conn.execute("SELECT kind, COUNT(*) FROM spans GROUP BY kind")}
            unjoined = self.conn.execute(
                "SELECT COUNT(*) FROM spans WHERE kind IN ('delete', 'change_visibility') AND receive_row IS NULL"
            ).fetchone()[0]
            messages = self.conn.execute("SELECT COUNT(DISTINCT mid) FROM spans WHERE mid IS NOT NULL").fetchone()[0]
        return {"spans": kinds, "messages": messages, "unjoined_settles": unjoined}
