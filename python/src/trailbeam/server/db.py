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
import logging
import sqlite3
import threading
import time
from typing import Iterable, Iterator, Optional

from . import migrations, summary_table
from .otlp import RECORD_FIELDS
from .store import (SETTLE_KINDS, TENANT_MAX_CHARS, Cutoffs, PurgeStep, Store,
                    StuckCandidate, TenantActivity)

# The body and the attributes that describe it (contract 15.2). trailbeam.body.capture_status is kept:
# it is the package's reason, carries no message content, and the Body tab needs it either way.
BODY_ATTRIBUTES = ("trailbeam.body", "trailbeam.body.format", "trailbeam.body.redacted_count",
                   "trailbeam.body.envelope")
BODY_DROPPED_NOT_OPTED_IN = "not_opted_in"  # spans.body_dropped: the queue is not in store_bodies

# The schema, its version and the upgrades from older files are in migrations.py.

# The two receipt-hash joins run once per settle or receive span, inside the insert transaction, so they
# must be index lookups at every size. INDEXED BY pins the hash index: the server never runs ANALYZE, and without
# statistics SQLite's planner picks spans_kind_time (kind=?) for the settle join, which scans every stored
# receive (QV-68: 99.95% of insert time at 10M spans). With INDEXED BY the plan cannot change with the data,
# the statistics or the SQLite version; if the index were ever missing the statement fails loudly instead of
# scanning. A hash matches one delivery's receive and its settle spans, so the lookup returns a few rows.
# The two hash indexes are partial (migrations.SPANS_INDEXES): spans_receive_hash holds receives only, and
# spans_unjoined_hash settle spans with no receive yet. A settle span is joined before its row is inserted, so
# one that finds its receive never has an entry in spans_unjoined_hash (QV-78: a page fewer per span).
SETTLE_JOIN_SQL = """SELECT row, message_id FROM spans INDEXED BY spans_receive_hash
   WHERE kind = 'receive' AND receipt_hmac = ? AND key_id = ?
   ORDER BY (time_ns <= ?) DESC,
            CASE WHEN time_ns <= ? THEN -time_ns ELSE time_ns END
   LIMIT 1"""
# The settle spans the receive join takes, and the message each was in (its own message ID, or none), so the
# summaries of both messages are rewritten. Same rows as RECEIVE_JOIN_SQL, by the same index.
RECEIVE_JOIN_FROM_SQL = """SELECT row, mid FROM spans INDEXED BY spans_unjoined_hash
   WHERE kind IN ('delete', 'change_visibility') AND receive_row IS NULL
     AND receipt_hmac = ? AND key_id = ?"""
RECEIVE_JOIN_SQL = """UPDATE spans INDEXED BY spans_unjoined_hash SET mid = ?, mid_source = 'receipt_hash', receive_row = ?
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

    Checkpoints (copying the log back into the file) run on a thread of their own (``_checkpoint_loop``),
    not in the writer's COMMIT. SQLite's automatic checkpoint runs inside the COMMIT that takes the log past
    1,000 pages, and with the summary tables one 512-span export writes about 2,600: every COMMIT
    checkpointed, and that was most of its time. The thread runs a PASSIVE checkpoint every
    ``CHECKPOINT_INTERVAL_S``. That never waits for the writer or a reader, and it writes a page changed by
    many exports once. ``synchronous`` stays FULL, so a COMMIT is still on disk when ``insert`` returns.
    """

    MAX_IDLE_READERS = 8
    GROUP_MAX_SPANS = 4096  # spans one transaction takes from the queue (insert): 8 exports of 512
    CHECKPOINT_INTERVAL_S = 1.0
    WAL_SIZE_LIMIT = 64 * 1024 * 1024  # journal_size_limit: the log is cut back to this when it restarts

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
        self._stop = threading.Event()
        self._checkpointer: Optional[threading.Thread] = None
        self._queue: list = []  # exports waiting for the writer (insert: group commit)
        self._queue_lock = threading.Lock()
        self.write_stats = {"transactions": 0, "exports": 0}  # exports / transactions: the group commit's gain
        try:
            # A new file is created with incremental vacuum, so retention can give space back (reclaim_space).
            # SQLite only takes this before the first table is created; on an existing file it does nothing.
            self.conn.execute("PRAGMA auto_vacuum = INCREMENTAL")
            # First the schema: a file this version refuses (migrations.SchemaTooNew) is left as it was.
            self.migrated = migrations.open_schema(self.conn, path)  # the versions upgraded to on this open
            if self.shared_reads:
                mode = self.conn.execute("PRAGMA journal_mode=WAL").fetchone()[0]
                if mode.lower() != "wal":
                    raise RuntimeError(f"{path}: SQLite could not switch the file to WAL mode (got {mode})")
                self.conn.execute("PRAGMA wal_autocheckpoint = 0")
                self.conn.execute(f"PRAGMA journal_size_limit = {self.WAL_SIZE_LIMIT}")
            self.conn.execute("PRAGMA foreign_keys=ON")
        except BaseException:
            self.conn.close()
            raise
        if self.shared_reads:
            self._checkpointer = threading.Thread(target=self._checkpoint_loop, name="trailbeam-checkpoint",
                                                  daemon=True)
            self._checkpointer.start()

    def _checkpoint_loop(self) -> None:
        c = sqlite3.connect(self.path, check_same_thread=False, isolation_level=None)
        try:
            while not self._stop.wait(self.CHECKPOINT_INTERVAL_S):
                self.checkpoint(c)
        finally:
            c.close()

    def checkpoint(self, conn: Optional[sqlite3.Connection] = None) -> tuple:
        """One PASSIVE checkpoint: (busy, frames in the log, frames checkpointed), as SQLite reports them."""
        try:
            if conn is not None:
                return tuple(conn.execute("PRAGMA wal_checkpoint(PASSIVE)").fetchone())
            with self.lock:
                return tuple(self.conn.execute("PRAGMA wal_checkpoint(PASSIVE)").fetchone())
        except sqlite3.Error as e:  # e.g. the file went away under a test; the next round tries again
            logging.getLogger(__name__).warning("checkpoint failed: %s", e)
            return (1, -1, -1)

    def close(self) -> None:
        self._stop.set()
        if self._checkpointer is not None:
            self._checkpointer.join()
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
        """Insert records (from otlp.decode) atomically. Returns (inserted, duplicates).

        A span already stored (same trace ID, span ID and end time, e.g. an exporter retry) is skipped (the
        spans_identity index). A settle span is joined to its receive before it is inserted. In the same
        transaction, the summary of every message a new span or a join touched is rewritten (summary_table.py).

        Group commit: concurrent calls share one transaction. Each call queues its export, then waits for the
        writer lock; whichever thread gets it writes every export queued by then (up to ``GROUP_MAX_SPANS``),
        each in a SAVEPOINT of its own, and commits once. An export that fails is rolled back to its savepoint
        and only its caller gets the error. Every call returns only after the COMMIT that holds its spans, so an
        acknowledged export is on disk (``synchronous = FULL``), as before. One COMMIT, and one fsync, then
        serves several exports, and a message several of them touch has its summary rewritten once (QV-78).
        """
        job = _Export(list(records))
        with self._queue_lock:
            self._queue.append(job)
        with self.lock:
            if not job.done:  # else a writer before us took it, and committed it
                self._write_group()
        if job.error is not None:
            raise job.error
        return job.result

    def _write_group(self) -> None:
        """Write the queued exports in one transaction; the caller holds ``lock``."""
        with self._queue_lock:
            group, spans = [], 0
            while self._queue and (not group or spans + len(self._queue[0].records) <= self.GROUP_MAX_SPANS):
                group.append(self._queue.pop(0))
                spans += len(group[-1].records)
        c = self.conn
        changes = summary_table.Changes()
        try:
            c.execute("BEGIN IMMEDIATE")
            try:
                for job in group:
                    c.execute("SAVEPOINT export")
                    mine: list = []
                    try:
                        job.result = self._insert_rows(job.records, mine)
                    except Exception as e:  # this export only: the others in the group still commit
                        c.execute("ROLLBACK TO export")
                        job.error = e
                    else:
                        for row, old, new in mine:
                            changes.move(row, old, new)
                    c.execute("RELEASE export")
                summary_table.refresh(c, changes)
                c.execute("COMMIT")
            except BaseException:
                c.execute("ROLLBACK")
                raise
        except BaseException as e:  # nothing of the group was stored: every export in it fails
            for job in group:
                job.result, job.error = None, e
            raise
        finally:
            self.write_stats["transactions"] += 1
            self.write_stats["exports"] += len(group)
            for job in group:
                job.done = True

    def _insert_rows(self, records: list, moves: list) -> tuple[int, int]:
        """One export's rows and joins, in the open transaction. Appends to ``moves`` each row that joined a
        message, as (row, message it was in or None, message it is in): summary_table.Changes.move."""
        inserted = dupes = 0
        c = self.conn
        for r in records:
            own = r.get("message_id")
            mid, mid_source, receive_row = own, "span" if own else None, None
            if r["kind"] in SETTLE_KINDS:  # joined first, so the row is written once, joined
                rec = self._settle_receive(r)
                if rec is not None:
                    mid, mid_source, receive_row = rec["message_id"], "receipt_hash", rec["row"]
            attrs, dropped = gate_body(r.get("attributes", {}), r.get("queue"), self.store_bodies)
            values = [r.get(f) for f in RECORD_FIELDS] + [
                json.dumps(attrs, sort_keys=True, default=str), mid, mid_source, receive_row, dropped]
            cur = c.execute(_INSERT_SQL, values)
            if cur.rowcount == 0:
                dupes += 1
                continue
            inserted += 1
            moves.append((cur.lastrowid, None, mid))
            if r["kind"] == "receive":
                moves.extend((row, was, mid) for row, was in self._join_receive(cur.lastrowid, r))
        return inserted, dupes

    def _settle_receive(self, r: dict) -> Optional[sqlite3.Row]:
        """The receive that returned a settle span's receipt handle (its row and message ID), None if no receive
        has the hash yet. Looked up before the settle span is inserted."""
        # SQS issues a new receipt handle for every delivery, so there is normally one match. If an
        # emulator reuses handles, take the latest receive at or before the settle, else the first.
        return self.conn.execute(SETTLE_JOIN_SQL, (r["receipt_hmac"], r["key_id"], r["time_ns"], r["time_ns"])).fetchone()

    def _join_receive(self, row: int, r: dict) -> list[tuple]:
        """Join settle spans that arrived before this receive. Returns (row, message it was in) for each."""
        key = (r["receipt_hmac"], r["key_id"])
        was = [(j, m) for j, m in self.conn.execute(RECEIVE_JOIN_FROM_SQL, key)]
        if was:
            self.conn.execute(RECEIVE_JOIN_SQL, (r["message_id"], row) + key)
        return was

    def purge_step(self, cutoffs: Cutoffs, resume: Optional[str] = None, limit: int = 100) -> PurgeStep:
        """Store.purge_step. A pass walks span time, oldest first, in windows of about ``limit`` messages, up
        to the latest cutoff: ``resume`` is ``t<window start>:<window width>`` (ns). Walking time, not message
        IDs, keeps one step's rows close together in the file (they were written at about the same time), so a
        step changes few pages (QV-72: half the write time of a step over random messages, half the WAL). And a
        span that arrived late with an old time is found by its time.

        Each step reads on a read connection, without the writer lock: the spans in the window (spans_kind_time,
        every kind), then which of their messages are expired. The write deletes those messages and the window's
        expired unjoined settle spans, each checked again in the write transaction."""
        latest = _latest(cutoffs)
        cut, cut_p = _cutoff_sql(cutoffs)
        qcut, _ = _cutoff_sql(cutoffs, "value")  # the same cutoffs, of a queue in a message's queues_json
        if resume is None:
            with self.reader() as c:
                firsts = [c.execute("SELECT MIN(time_ns) FROM spans INDEXED BY spans_kind_time WHERE kind = ?",
                                    (k,)).fetchone()[0] for k in SPAN_KINDS]
            firsts = [t for t in firsts if t is not None]
            if not firsts or min(firsts) >= latest:
                return PurgeStep(0, 0, None)
            start, width = min(firsts), PURGE_FIRST_WINDOW_NS
        else:
            start, width = (int(x) for x in resume[1:].split(":"))
        end = min(start + width, latest)
        with self.reader() as c:
            # INDEXED BY: the time range on every kind is the walk; a plan that scanned would read the table.
            rows = c.execute(
                f"""SELECT row, mid, kind, time_ns < {cut} AS old FROM spans INDEXED BY spans_kind_time
                    WHERE kind IN {SPAN_KINDS!r} AND time_ns >= ? AND time_ns < ?""", cut_p + (start, end)).fetchall()
            mids = sorted({r["mid"] for r in rows if r["mid"] is not None})
            expired = [r["mid"] for r in c.execute(_EXPIRED_SQL.format(cut=qcut), (json.dumps(mids),) + cut_p)] if mids else []
            unjoined = [r["row"] for r in rows if r["mid"] is None and r["kind"] in SETTLE_KINDS and r["old"]]
            if self.shared_reads and (expired or unjoined):
                _prefetch_delete(c, expired, unjoined)
        n, write_ms = 0, 0.0
        if expired or unjoined:
            with self.lock:
                w0 = time.monotonic()
                n = self._delete(expired, unjoined, qcut, cut_p)
                write_ms = (time.monotonic() - w0) * 1000
        found = len(mids) + len(unjoined)
        if end >= latest:
            return PurgeStep(n, found, None, write_ms)
        # the next window: about ``limit`` messages, if they come at the rate this one had
        width = max(1, int(width * min(4.0, max(0.25, limit / max(found, 1)))))
        return PurgeStep(n, found, f"t{end}:{width}", write_ms)

    def _delete(self, expired: list, unjoined: list, qcut: str, cut_p: tuple) -> int:
        """purge_step's write, in one transaction; the caller holds ``lock``. Each message is checked again:
        a span that came in since the read can have made it newer. Its summary rows go with its spans
        (summary_table.forget), so the list pages never show a message that is gone. Returns spans deleted."""
        c = self.conn
        c.execute("BEGIN IMMEDIATE")
        try:
            gone = [m for (m,) in c.execute(_EXPIRED_SQL.format(cut=qcut), (json.dumps(expired),) + cut_p)] if expired else []
            # the spans first: their rows are in the summaries forget deletes
            n = c.execute(f"DELETE FROM spans WHERE row IN ({_ROWS_OF_SQL})", (json.dumps(gone),)).rowcount
            summary_table.forget(c, gone)
            # joined since the read: it belongs to a message now, which goes by the message's time
            n += c.execute("DELETE FROM spans WHERE row IN (SELECT value FROM json_each(?)) AND mid IS NULL",
                           (json.dumps(unjoined),)).rowcount
            c.execute("COMMIT")
        except BaseException:
            c.execute("ROLLBACK")
            raise
        return n

    def reclaim_space(self, max_pages: int) -> int:
        """``PRAGMA incremental_vacuum``: move up to ``max_pages`` free pages to the end of the file and cut
        them off. Only on a file with ``auto_vacuum = INCREMENTAL``: every file created at schema 8 or later, and
        older ones after ``vacuum()``. On any other file deleted spans' pages stay free inside it, and new spans
        reuse them, so the file stops growing but does not shrink."""
        with self.lock:
            c = self.conn
            if c.execute("PRAGMA auto_vacuum").fetchone()[0] != AUTO_VACUUM_INCREMENTAL:
                return 0
            before = c.execute("PRAGMA freelist_count").fetchone()[0]
            if before:
                c.execute(f"PRAGMA incremental_vacuum({int(max_pages)})").fetchall()  # it frees a page per step
            return before - c.execute("PRAGMA freelist_count").fetchone()[0]

    def vacuum(self) -> None:
        """Rewrite the whole file (``VACUUM``) with ``auto_vacuum = INCREMENTAL``, so that retention can give
        space back from then on. Needs free disk for a second copy of the file, and holds the writer lock until
        it is done: run it with the server stopped (``trailbeam --vacuum``)."""
        with self.lock:
            self.conn.execute("PRAGMA auto_vacuum = INCREMENTAL")
            self.conn.execute("VACUUM")

    def storage(self) -> dict:
        """The file's size and free space, in pages: SQLite only (the benchmark and ``--vacuum`` report it)."""
        with self.lock:
            c = self.conn
            return {"page_size": c.execute("PRAGMA page_size").fetchone()[0],
                    "pages": c.execute("PRAGMA page_count").fetchone()[0],
                    "free_pages": c.execute("PRAGMA freelist_count").fetchone()[0],
                    "auto_vacuum": {0: "none", 1: "full", 2: "incremental"}[c.execute("PRAGMA auto_vacuum").fetchone()[0]]}

    # ---- reads ----------------------------------------------------------------------------

    def message_rows(self, message_id: str) -> list[sqlite3.Row]:
        """Every row of one message, in time order."""
        with self.reader() as c:
            return summary_table.rows_of(c, message_id)

    def query(self, sql: str, params: tuple = ()) -> list[sqlite3.Row]:
        """A read-only query (on a file, a write fails: read connections are query_only). SQLite only: not
        part of Store, so the server doesn't use it; tests and the benchmark do."""
        with self.reader() as c:
            return c.execute(sql, params).fetchall()

    def recent_messages(self, limit: int, queue: Optional[str] = None, tenant: Optional[str] = None, *,
                        from_ns: Optional[int] = None, to_ns: Optional[int] = None,
                        after: Optional[tuple] = None) -> list[str]:
        if tenant:
            src, p = "message_tenants s NOT INDEXED WHERE s.tenant = ?", [tenant[:TENANT_MAX_CHARS]]
            if queue:
                src += " AND EXISTS (SELECT 1 FROM messages m, json_each(m.queues_json) WHERE m.mid = s.mid AND value = ?)"
                p.append(queue)
        elif queue:
            src, p = "message_queues s NOT INDEXED WHERE s.queue = ?", [queue]
        else:
            src, p = "messages s INDEXED BY messages_last WHERE 1", []
        return self._newest_first(src, p, limit, from_ns, to_ns, after)

    def duplicate_messages(self, limit: int, queue: Optional[str] = None, reason: Optional[str] = None, *,
                           from_ns: Optional[int] = None, to_ns: Optional[int] = None,
                           after: Optional[tuple] = None) -> list[str]:
        src = "messages s INDEXED BY messages_duplicates WHERE (dup_received OR dup_succeeded)"
        src += {None: "", "received_more_than_once": " AND dup_received",
                "succeeded_more_than_once": " AND dup_succeeded"}[reason]
        p: list = []
        if queue:
            src += " AND EXISTS (SELECT 1 FROM json_each(s.queues_json) WHERE value = ?)"
            p.append(queue)
        return self._newest_first(src, p, limit, from_ns, to_ns, after)

    def _newest_first(self, src: str, p: list, limit: int, from_ns, to_ns, after) -> list[str]:
        """``src``: a summary table, as s, with last_ns and mid, and its WHERE; read in (last_ns DESC, mid) order
        by the index or primary key it names (INDEXED BY, or NOT INDEXED for the primary key)."""
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
        rows = self.query(f"SELECT s.mid FROM {table} WHERE {rest}{where} "
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
            where += " AND EXISTS (SELECT 1 FROM json_each(s.queues_json) WHERE value = ?)"
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
        return {r["mid"] for r in self.query("SELECT mid FROM message_queues NOT INDEXED WHERE queue = ?", (queue,))}

    def tenant_activity(self, from_ms: int, to_ms: int) -> list[TenantActivity]:
        with self.reader() as c:
            totals = c.execute(
                """SELECT tenant, SUM(kind = 'sent') AS sent,
                          COUNT(DISTINCT CASE WHEN kind = 'dead_lettered' THEN mid END) AS dead_lettered
                   FROM tenant_events NOT INDEXED WHERE at_ms BETWEEN ? AND ?
                   GROUP BY tenant""", (from_ms, to_ms)).fetchall()
            waits: dict = {}
            for t, w in c.execute(
                    """SELECT tenant, wait_ms FROM tenant_events NOT INDEXED
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
            where += " AND EXISTS (SELECT 1 FROM messages m, json_each(m.queues_json) WHERE m.mid = t.mid AND value = ?)"
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
                      (SELECT m.last_ns FROM messages m WHERE m.mid = s.mid) AS last
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
            messages = c.execute("SELECT COUNT(*) FROM messages").fetchone()[0]  # one summary per message
        return {"spans": kinds, "messages": messages, "unjoined_settles": unjoined}


Database = SqliteStore  # the name everything used before store.py


class _Export:
    """One insert call's export, queued for the writer (SqliteStore.insert)."""
    __slots__ = ("records", "result", "error", "done")

    def __init__(self, records: list):
        self.records, self.result, self.error, self.done = records, None, None, False


_INSERT_COLUMNS = RECORD_FIELDS + ["attributes", "mid", "mid_source", "receive_row", "body_dropped"]
_INSERT_SQL = f"INSERT OR IGNORE INTO spans ({', '.join(_INSERT_COLUMNS)}) VALUES ({', '.join('?' * len(_INSERT_COLUMNS))})"

AUTO_VACUUM_INCREMENTAL = 2  # PRAGMA auto_vacuum
SPAN_KINDS = ("send", "receive", "delete", "change_visibility", "outcome")  # every kind a row can have
def _prefetch_delete(c: sqlite3.Connection, mids: list, rows: list) -> None:
    """Read every page purge_step's delete of these messages and unjoined rows will change: the rows, an entry
    in each index of spans (the UNIQUE one too), the foreign-key lookup of their settle spans, and the summary
    rows (summary_table.prefetch). On a read connection, without the writer lock: the delete then finds them in
    the page cache. Without it the first steps of a pass at 1M spans held the lock for 3 to 4.5 s each, reading
    pages of week-old messages from disk, and exports waited behind them (QV-72 bench). Changes nothing."""
    cols = "SELECT row, trace_id, span_id, kind, time_ns, receipt_hmac, key_id, receive_row FROM spans"
    found = (c.execute(f"{cols} WHERE row IN ({_ROWS_OF_SQL})", (json.dumps(mids),)).fetchall()
             + c.execute(f"{cols} WHERE row IN (SELECT value FROM json_each(?))", (json.dumps(rows),)).fetchall())
    for r in found:
        c.execute("SELECT 1 FROM spans INDEXED BY spans_identity WHERE time_ns = ? AND trace_id = ? AND span_id = ?",
                  (r["time_ns"], r["trace_id"], r["span_id"])).fetchall()
        c.execute("SELECT 1 FROM spans INDEXED BY spans_kind_time WHERE kind = ? AND time_ns = ? AND row = ?",
                  (r["kind"], r["time_ns"], r["row"])).fetchall()
        if r["receipt_hmac"] is not None and r["kind"] == "receive":
            c.execute("SELECT 1 FROM spans INDEXED BY spans_receive_hash WHERE kind = 'receive' AND receipt_hmac = ? "
                      "AND key_id IS ? AND row = ?", (r["receipt_hmac"], r["key_id"], r["row"])).fetchall()
        elif r["receipt_hmac"] is not None and r["receive_row"] is None:
            c.execute("SELECT 1 FROM spans INDEXED BY spans_unjoined_hash WHERE kind IN ('delete', 'change_visibility') "
                      "AND receive_row IS NULL AND receipt_hmac = ? AND key_id IS ? AND row = ?",
                      (r["receipt_hmac"], r["key_id"], r["row"])).fetchall()
        # the foreign key: deleting a row looks for settle spans that point at it
        c.execute("SELECT 1 FROM spans INDEXED BY spans_receive_row WHERE receive_row = ?", (r["row"],)).fetchall()
    summary_table.prefetch(c, mids)


PURGE_FIRST_WINDOW_NS = 60 * 10 ** 9  # the first window of a pass; later ones adapt (purge_step)
# The messages of a JSON list whose newest span is before the cutoff of every queue they have a span on, from
# their summaries (last_ns, queues_json): spans has no index on mid (schema 10).
_EXPIRED_SQL = """SELECT mid FROM messages WHERE mid IN (SELECT value FROM json_each(?))
                  AND last_ns < (SELECT MIN({cut}) FROM json_each(messages.queues_json))"""
# The span rows of the messages of a JSON list (their summaries' rows_json).
_ROWS_OF_SQL = """SELECT r.value FROM messages m, json_each(m.rows_json) r
                  WHERE m.mid IN (SELECT value FROM json_each(?))"""
_KEPT = -(2 ** 63)  # the cutoff of a queue kept forever, in SQL: no span ends before it


def _cutoff_sql(cutoffs: Cutoffs, column: str = "queue") -> tuple[str, tuple]:
    """A SQL expression for the cutoff of the queue in ``column`` (a row's ``queue``), and its parameters."""
    if not cutoffs.queues:
        return "?", (_KEPT if cutoffs.default is None else cutoffs.default,)
    params: list = []
    for q, c in cutoffs.queues.items():
        params += [q, _KEPT if c is None else c]
    params.append(_KEPT if cutoffs.default is None else cutoffs.default)
    return f"(CASE {column} {' '.join(['WHEN ? THEN ?'] * len(cutoffs.queues))} ELSE ? END)", tuple(params)


def _latest(cutoffs: Cutoffs) -> int:
    """The latest cutoff of any queue: no span after it has expired."""
    return max(_KEPT if c is None else c for c in (cutoffs.default, *cutoffs.queues.values()))


_ascii_lower = summary_table.ascii_lower
