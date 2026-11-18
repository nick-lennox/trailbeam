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

from . import migrations, summary_table
from .otlp import RECORD_FIELDS
from .store import (SETTLE_KINDS, TENANT_MAX_CHARS, Store,
                    StuckCandidate, TenantActivity)

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
# The messages the receive join takes settle spans from (their own message ID, or none), so their summaries
# are rewritten too. Same rows as RECEIVE_JOIN_SQL, by the same index.
RECEIVE_JOIN_FROM_SQL = """SELECT DISTINCT mid FROM spans INDEXED BY spans_hash
   WHERE kind IN ('delete', 'change_visibility') AND receive_row IS NULL
     AND receipt_hmac = ? AND key_id = ? AND mid IS NOT NULL"""
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

        A span already stored (same trace and span ID, e.g. an exporter retry) is skipped. In the same
        transaction, the summary of every message a new span or a join touched is rewritten (summary_table.py).
        """
        inserted = dupes = 0
        touched: set = set()
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
                        touched.add(self._join_settle(row, r) or own)
                    elif r["kind"] == "receive":
                        touched.update(self._join_receive(row, r))
                        touched.add(own)
                    else:
                        touched.add(own)
                summary_table.refresh(c, touched)
                c.execute("COMMIT")
            except BaseException:
                c.execute("ROLLBACK")
                raise
        return inserted, dupes

    def _join_settle(self, row: int, r: dict) -> Optional[str]:
        """Join a new settle span to the receive that returned its receipt handle. Returns the message joined
        to, None if no receive has the hash yet."""
        # SQS issues a new receipt handle for every delivery, so there is normally one match. If an
        # emulator reuses handles, take the latest receive at or before the settle, else the first.
        rec = self.conn.execute(SETTLE_JOIN_SQL, (r["receipt_hmac"], r["key_id"], r["time_ns"], r["time_ns"])).fetchone()
        if rec is not None:
            self.conn.execute("UPDATE spans SET mid = ?, mid_source = 'receipt_hash', receive_row = ? WHERE row = ?",
                              (rec["message_id"], rec["row"], row))
            return rec["message_id"]
        return None

    def _join_receive(self, row: int, r: dict) -> list[str]:
        """Join settle spans that arrived before this receive. Returns the messages they belonged to."""
        key = (r["receipt_hmac"], r["key_id"])
        was = [m for (m,) in self.conn.execute(RECEIVE_JOIN_FROM_SQL, key)]
        self.conn.execute(RECEIVE_JOIN_SQL, (r["message_id"], row) + key)
        return was

    def delete_messages_before(self, cutoff_ns: int, queue: Optional[str] = None, limit: int = 10_000) -> int:
        with self.lock:
            c = self.conn
            c.execute("BEGIN IMMEDIATE")
            try:
                if queue:
                    mids = [m for (m,) in c.execute(
                        """SELECT q.mid FROM message_queues q JOIN messages m ON m.mid = q.mid
                           WHERE q.queue = ? AND q.last_ns < ? LIMIT ?""", (queue, cutoff_ns, limit))]
                else:
                    mids = [m for (m,) in c.execute(
                        "SELECT mid FROM messages WHERE last_ns < ? ORDER BY last_ns LIMIT ?", (cutoff_ns, limit))]
                n = sum(c.execute("DELETE FROM spans WHERE mid = ?", (m,)).rowcount for m in mids)
                summary_table.forget(c, mids)
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

    def recent_messages(self, limit: int, queue: Optional[str] = None, tenant: Optional[str] = None, *,
                        from_ns: Optional[int] = None, to_ns: Optional[int] = None,
                        after: Optional[tuple] = None) -> list[str]:
        if tenant:
            src, p = "message_tenants INDEXED BY message_tenants_last WHERE tenant = ?", [tenant[:TENANT_MAX_CHARS]]
            if queue:
                src += " AND EXISTS (SELECT 1 FROM message_queues q WHERE q.mid = s.mid AND q.queue = ?)"
                p.append(queue)
        elif queue:
            src, p = "message_queues INDEXED BY message_queues_last WHERE queue = ?", [queue]
        else:
            src, p = "messages INDEXED BY messages_last WHERE 1", []
        return self._newest_first(src, p, limit, from_ns, to_ns, after)

    def duplicate_messages(self, limit: int, queue: Optional[str] = None, reason: Optional[str] = None, *,
                           from_ns: Optional[int] = None, to_ns: Optional[int] = None,
                           after: Optional[tuple] = None) -> list[str]:
        src = "messages INDEXED BY messages_duplicates WHERE (dup_received OR dup_succeeded)"
        src += {None: "", "received_more_than_once": " AND dup_received",
                "succeeded_more_than_once": " AND dup_succeeded"}[reason]
        p: list = []
        if queue:
            src += " AND EXISTS (SELECT 1 FROM message_queues q WHERE q.mid = s.mid AND q.queue = ?)"
            p.append(queue)
        return self._newest_first(src, p, limit, from_ns, to_ns, after)

    def _newest_first(self, src: str, p: list, limit: int, from_ns, to_ns, after) -> list[str]:
        """``src``: a summary table (alias s) with last_ns and mid, and its WHERE, indexed by (last_ns DESC, mid)."""
        where, p = "", list(p)
        if from_ns is not None:
            where += " AND s.last_ns >= ?"
            p.append(from_ns)
        if to_ns is not None:
            where += " AND s.last_ns < ?"
            p.append(to_ns)
        if after is not None:
            where += " AND (s.last_ns < ? OR (s.last_ns = ? AND s.mid > ?))"
            p += [after[0], after[0], after[1]]
        table, _, rest = src.partition(" WHERE ")
        rows = self.query(f"SELECT s.mid FROM {table.replace(' INDEXED', ' s INDEXED', 1)} WHERE {rest}{where} "
                          "ORDER BY s.last_ns DESC, s.mid LIMIT ?", tuple(p) + (limit,))
        return [r["mid"] for r in rows]

    def stuck_messages(self, cutoff_ns: int, limit: int, queue: Optional[str] = None, *,
                       from_ns: Optional[int] = None, after: Optional[tuple] = None) -> list[StuckCandidate]:
        where, p = "", [cutoff_ns]
        if from_ns is not None:
            where += " AND s.stuck_first_ns >= ?"
            p.append(from_ns)
        if after is not None:
            where += " AND (s.stuck_first_ns > ? OR (s.stuck_first_ns = ? AND s.mid > ?))"
            p += [after[0], after[0], after[1]]
        if queue:
            where += " AND EXISTS (SELECT 1 FROM message_queues q WHERE q.mid = s.mid AND q.queue = ?)"
            p.append(queue)
        rows = self.query(
            f"""SELECT s.mid, s.stuck_first_ns, s.stuck_sqs_clock FROM messages s INDEXED BY messages_stuck
                WHERE s.stuck_open AND s.stuck_first_ns IS NOT NULL AND s.stuck_first_ns < ?{where}
                ORDER BY s.stuck_first_ns, s.mid LIMIT ?""", tuple(p) + (limit,))
        return [StuckCandidate(r["mid"], r["stuck_first_ns"], bool(r["stuck_sqs_clock"])) for r in rows]

    def summaries(self, message_ids: list[str]) -> list[sqlite3.Row]:
        if not message_ids:
            return []
        out: dict = {}
        with self.reader() as c:
            for i in range(0, len(message_ids), 500):
                part = message_ids[i:i + 500]
                for r in c.execute(f"SELECT * FROM messages WHERE mid IN ({', '.join('?' * len(part))})", part):
                    out[r["mid"]] = r
        return [out[m] for m in message_ids if m in out]

    def queue_messages(self, queue: str) -> set[str]:
        return {r["mid"] for r in self.query("SELECT mid FROM message_queues INDEXED BY message_queues_last "
                                             "WHERE queue = ?", (queue,))}

    def tenant_activity(self, from_ms: int, to_ms: int) -> list[TenantActivity]:
        with self.reader() as c:
            totals = c.execute(
                """SELECT tenant, SUM(kind = 'sent') AS sent,
                          COUNT(DISTINCT CASE WHEN kind = 'dead_lettered' THEN mid END) AS dead_lettered
                   FROM tenant_events INDEXED BY tenant_events_at WHERE at_ms BETWEEN ? AND ?
                   GROUP BY tenant""", (from_ms, to_ms)).fetchall()
            waits: dict = {}
            for t, w in c.execute(
                    """SELECT tenant, wait_ms FROM tenant_events INDEXED BY tenant_events_at
                       WHERE at_ms BETWEEN ? AND ? AND kind = 'first_delivery'""", (from_ms, to_ms)):
                waits.setdefault(t, []).append(w)
        return [TenantActivity(r["tenant"], r["sent"], r["dead_lettered"], waits.get(r["tenant"], []))
                for r in totals]

    def find_tenants(self, text: str, limit: int, *, from_ns: Optional[int] = None, to_ns: Optional[int] = None,
                     queue: Optional[str] = None) -> list[str]:
        """Skips through the distinct tenants that start with the text (message_tenants_search), then one
        index lookup each for the newest span naming it in the range: the cost is the number of tenants
        matching, not of messages."""
        if not text:
            return []
        lo = _ascii_lower(text)
        hi = lo[:-1] + chr(ord(lo[-1]) + 1) if ord(lo[-1]) < 0x10FFFF else None
        where, p = "", []
        if from_ns is not None:
            where += " AND named_ns >= ?"
            p.append(from_ns)
        if to_ns is not None:
            where += " AND named_ns < ?"
            p.append(to_ns)
        if queue:
            where += " AND EXISTS (SELECT 1 FROM message_queues q WHERE q.mid = t.mid AND q.queue = ?)"
            p.append(queue)
        found = []
        with self.reader() as c:
            key = (lo, "")
            while True:
                nxt = c.execute(
                    f"""SELECT tenant_lower, tenant FROM message_tenants INDEXED BY message_tenants_search
                        WHERE (tenant_lower, tenant) > (?, ?) {"AND tenant_lower < ?" if hi else ""}
                        ORDER BY tenant_lower, tenant LIMIT 1""", key + ((hi,) if hi else ())).fetchone()
                if nxt is None or not nxt[0].startswith(lo):
                    break
                key = (nxt[0], nxt[1])
                newest = c.execute(
                    f"""SELECT named_ns FROM message_tenants t INDEXED BY message_tenants_search
                        WHERE tenant_lower = ? AND tenant = ?{where} ORDER BY named_ns DESC LIMIT 1""",
                    key + tuple(p)).fetchone()
                if newest is not None:
                    found.append((nxt[0] != lo, -newest[0], nxt[1]))
        return [t for *_, t in sorted(found)[:limit]]

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


_ascii_lower = summary_table.ascii_lower
