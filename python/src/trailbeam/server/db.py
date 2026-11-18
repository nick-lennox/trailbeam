"""SQLite storage for Trailbeam spans (``SqliteStore``, the default Store), and the receipt-hash join of
settle spans to receives. The interface, and what each method must return, is store.py.

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

from . import migrations
from .otlp import RECORD_FIELDS
from .store import (SETTLE_KINDS, TENANT_ATTR, TENANT_KINDS, TENANT_MAX_CHARS, Store,
                    StuckCandidate)

# The body and the attributes that describe it (contract 15.2). trailbeam.body.capture_status is kept:
# it is the package's reason, carries no message content, and the Body tab needs it either way.
BODY_ATTRIBUTES = ("trailbeam.body", "trailbeam.body.format", "trailbeam.body.redacted_count",
                   "trailbeam.body.envelope")
BODY_DROPPED_NOT_OPTED_IN = "not_opted_in"  # spans.body_dropped: the queue is not in store_bodies

# The schema, its version and the upgrades from older files are in migrations.py.

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


class SqliteStore(Store):
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
        try:
            # First the schema: a file this version refuses (migrations.SchemaTooNew) is left as it was.
            self.migrated = migrations.open_schema(self.conn, path)  # the versions upgraded to on this open
            if self.shared_reads:
                mode = self.conn.execute("PRAGMA journal_mode=WAL").fetchone()[0]
                if mode.lower() != "wal":
                    raise RuntimeError(f"{path}: SQLite could not switch the file to WAL mode (got {mode})")
            self.conn.execute("PRAGMA foreign_keys=ON")
        except BaseException:
            self.conn.close()
            raise

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

    def delete_messages_before(self, cutoff_ns: int, queue: Optional[str] = None, limit: int = 10_000) -> int:
        qsql, qp = _queue_filter(queue)
        with self.lock:
            c = self.conn
            c.execute("BEGIN IMMEDIATE")
            try:
                n = c.execute(
                    f"""DELETE FROM spans WHERE mid IN (
                            SELECT s.mid FROM spans s WHERE s.mid IS NOT NULL {qsql}
                            GROUP BY s.mid HAVING MAX(s.time_ns) < ? LIMIT ?)""", qp + (cutoff_ns, limit)).rowcount
                n += c.execute(
                    f"""DELETE FROM spans WHERE mid IS NULL AND kind IN {SETTLE_KINDS!r} AND time_ns < ?
                        {"AND queue = ?" if queue else ""}""", (cutoff_ns,) + ((queue,) if queue else ())).rowcount
                c.execute("COMMIT")
            except BaseException:
                c.execute("ROLLBACK")
                raise
        return n

    # ---- reads ----------------------------------------------------------------------------

    def message_rows(self, message_id: str) -> list[sqlite3.Row]:
        """Every row of one message, in time order."""
        with self.reader() as c:
            return c.execute("SELECT * FROM spans WHERE mid = ? ORDER BY time_ns, row", (message_id,)).fetchall()

    def query(self, sql: str, params: tuple = ()) -> list[sqlite3.Row]:
        """A read-only query (on a file, a write fails: read connections are query_only). SQLite only: not
        part of Store, so the server doesn't use it; tests and the benchmark do."""
        with self.reader() as c:
            return c.execute(sql, params).fetchall()

    def recent_messages(self, limit: int, queue: Optional[str] = None, tenant: Optional[str] = None) -> list[str]:
        qsql, qp = _queue_filter(queue)
        tsql, tp = _tenant_filter(tenant)
        rows = self.query(
            f"""SELECT s.mid, MAX(s.time_ns) AS last FROM spans s WHERE s.mid IS NOT NULL {qsql}{tsql}
                GROUP BY s.mid ORDER BY last DESC, s.mid LIMIT ?""", qp + tp + (limit,))
        return [r["mid"] for r in rows]

    def duplicate_messages(self, limit: int, queue: Optional[str] = None, reason: Optional[str] = None) -> list[str]:
        qsql, qp = _queue_filter(queue)
        having = {"received_more_than_once": "receives > 1", "succeeded_more_than_once": "successes > 1",
                  None: "receives > 1 OR successes > 1"}[reason]
        rows = self.query(
            f"""SELECT s.mid,
                       SUM(s.kind = 'receive') AS receives,
                       COUNT(DISTINCT CASE WHEN s.kind = 'delete' AND s.result = 'ok' THEN s.receive_row END)
                       + COUNT(DISTINCT CASE WHEN s.kind = 'outcome' AND s.outcome = 'success'
                                             THEN s.faas_invocation_id END) AS successes,
                       MAX(s.time_ns) AS last
                FROM spans s WHERE s.mid IS NOT NULL {qsql}
                GROUP BY s.mid HAVING {having}
                ORDER BY last DESC, s.mid LIMIT ?""", qp + (limit,))
        return [r["mid"] for r in rows]

    def stuck_messages(self, cutoff_ns: int, limit: int, queue: Optional[str] = None) -> list[StuckCandidate]:
        qsql, qp = _queue_filter(queue)
        rows = self.query(
            f"""WITH latest AS (
                    SELECT mid, row AS rrow, faas_invocation_id AS inv FROM (
                        SELECT mid, row, faas_invocation_id,
                               ROW_NUMBER() OVER (PARTITION BY mid ORDER BY time_ns DESC, row DESC) AS rn
                        FROM spans WHERE kind = 'receive')
                    WHERE rn = 1),
                seen AS (
                    SELECT s.mid, MIN(CASE WHEN s.kind = 'send' THEN s.time_ns END) AS send_ns,
                           MIN(CASE WHEN s.kind = 'receive' THEN s.sent_ts_ms END) * 1000000 AS sqs_ns,
                           MIN(s.time_ns) AS any_ns
                    FROM spans s WHERE s.mid IS NOT NULL AND s.kind IN ('send', 'receive') {qsql}
                    GROUP BY s.mid),
                first AS (
                    SELECT mid, COALESCE(send_ns, sqs_ns, any_ns) AS first_ns,
                           send_ns IS NULL AND sqs_ns IS NOT NULL AS on_sqs_clock
                    FROM seen)
                SELECT f.mid, f.first_ns, f.on_sqs_clock FROM first f LEFT JOIN latest l ON l.mid = f.mid
                WHERE f.first_ns < ?
                  AND NOT EXISTS (SELECT 1 FROM spans d WHERE d.kind = 'delete' AND d.result = 'ok'
                                  AND d.receive_row = l.rrow)
                  AND NOT EXISTS (SELECT 1 FROM spans o WHERE o.kind = 'outcome' AND o.outcome = 'success'
                                  AND o.mid = f.mid AND o.faas_invocation_id = l.inv)
                ORDER BY f.first_ns, f.mid LIMIT ?""", qp + (cutoff_ns, limit))
        return [StuckCandidate(r["mid"], r["first_ns"], bool(r["on_sqs_clock"])) for r in rows]

    def active_messages(self, from_ns: int, to_ns: int, limit: int) -> list[str]:
        rows = self.query(
            """SELECT mid, MAX(time_ns) AS last FROM spans WHERE mid IS NOT NULL AND time_ns >= ? AND time_ns < ?
               GROUP BY mid ORDER BY last DESC, mid LIMIT ?""", (from_ns, to_ns, limit))
        return [r["mid"] for r in rows]

    def queue_messages(self, queue: str) -> set[str]:
        return {r["mid"] for r in self.query("SELECT DISTINCT mid FROM spans WHERE queue = ? AND mid IS NOT NULL",
                                             (queue,))}

    def find_tenants(self, text: str, limit: int) -> list[str]:
        if not text:
            return []
        like = "".join("\\" + c if c in "\\%_" else c for c in _ascii_lower(text)) + "%"
        rows = self.query(
            f"""SELECT tenant, MAX(time_ns) AS last FROM (
                    SELECT {_TENANT_SQL} AS tenant, time_ns FROM spans WHERE kind IN {TENANT_KINDS!r})
                WHERE tenant IS NOT NULL AND LOWER(tenant) LIKE ? ESCAPE '\\'
                GROUP BY tenant ORDER BY LOWER(tenant) = ? DESC, last DESC, tenant LIMIT ?""",
            (like, _ascii_lower(text), limit))
        return [r["tenant"] for r in rows]

    def body_rows(self) -> list[sqlite3.Row]:
        return self.query(
            """SELECT row, kind, mid, queue, attributes,
                      (SELECT MAX(s2.time_ns) FROM spans s2 WHERE s2.mid = s.mid) AS last
               FROM spans s
               WHERE kind IN ('send', 'receive') AND mid IS NOT NULL
                 AND json_type(attributes, '$."trailbeam.body"') = 'text'
               ORDER BY mid, kind <> 'send', row""")

    def last_row(self) -> int:
        """The highest row ID stored, 0 if none: one step down the primary key."""
        with self.reader() as c:
            return c.execute("SELECT MAX(row) FROM spans").fetchone()[0] or 0

    def counts(self) -> dict:
        """Exact counts (``/api/stats``). Each one reads the whole table: seconds at 10M spans."""
        with self.reader() as c:
            kinds = {k: n for k, n in c.execute("SELECT kind, COUNT(*) FROM spans GROUP BY kind")}
            unjoined = c.execute(
                "SELECT COUNT(*) FROM spans WHERE kind IN ('delete', 'change_visibility') AND receive_row IS NULL"
            ).fetchone()[0]
            messages = c.execute("SELECT COUNT(DISTINCT mid) FROM spans WHERE mid IS NOT NULL").fetchone()[0]
        return {"spans": kinds, "messages": messages, "unjoined_settles": unjoined}


Database = SqliteStore  # the name everything used before store.py


def _queue_filter(queue: Optional[str]) -> tuple[str, tuple]:
    """A WHERE fragment for spans aliased s: the message has a span on the queue."""
    if not queue:
        return "", ()
    return " AND EXISTS (SELECT 1 FROM spans q WHERE q.mid = s.mid AND q.queue = ?)", (queue,)


# a span's tenant, in SQL: a non-empty string, cut to 256 characters
_TENANT_SQL = (f"""CASE WHEN json_type(attributes, '$."{TENANT_ATTR}"') = 'text'
                         AND json_extract(attributes, '$."{TENANT_ATTR}"') <> ''
                       THEN substr(json_extract(attributes, '$."{TENANT_ATTR}"'), 1, {TENANT_MAX_CHARS}) END""")


def _tenant_filter(tenant: Optional[str]) -> tuple[str, tuple]:
    """A WHERE fragment for spans aliased s: the message has a span naming the tenant."""
    if not tenant:
        return "", ()
    return (f" AND EXISTS (SELECT 1 FROM spans tn WHERE tn.mid = s.mid AND tn.kind IN {TENANT_KINDS!r}"
            f" AND {_TENANT_SQL.replace('attributes', 'tn.attributes')} = ?)", (tenant[:TENANT_MAX_CHARS],))


def _ascii_lower(s: str) -> str:
    return s.translate(_ASCII_LOWER)


_ASCII_LOWER = str.maketrans("ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz")
