"""Postgres storage for Trailbeam (``PostgresStore``): bring-your-own, selected by ``TRAILBEAM_STORE_URL=postgres://…``.

The same Store as SQLite (store.py; db.py is the reference), with the same answers: the conformance suite and
``contract/fixtures`` run against both. The driver (psycopg 3, ``pip install "trailbeam[postgres]"``) is
imported here only, and this module only when the store URL is a Postgres one, so a SQLite install needs
nothing new. The schema and its versions are pg_schema.py. Supported: Postgres 14 and later (CI: 14 and 16);
14 is the first with ``DETACH PARTITION … CONCURRENTLY``, which retention needs (below).

**Ingest** (``insert``): one transaction per export, its statements batched (arrays and pipelined
``executemany``), never a commit per statement and never a ``DELETE``. In it, as in SQLite: the spans (an
exporter's retry skipped by the unique key), the receipt-hash join in both directions, and the summary rows of
every message a span or a join touched (summary_table.py's tables, the same rules). Concurrent exports are
serialised only where they meet: a transaction-scoped advisory lock per receipt hash and per message, taken in
one sorted order, so the join and a message's summary see each other's spans. A settle span carries no message
ID, so the messages its join reaches are locked after the join (without waiting; when one is held, the export is
retried with it locked from the start). The locks are the database's, so this holds across replicas: several
Trailbeam servers on one Postgres join and summarise exactly as one does (QV-81). Exports and reads use
separate connection pools.

**Placement.** Every table is partitioned by place, ``(rclass, bucket)`` (pg_schema.py), with buckets of
``bucket_ns`` (G, a day by default). A span is placed by its own time and its queue's retention class. A
message's summary rows and bodies are placed by its newest span's bucket and the longest retention class among
its queues, and move when that changes. Rows of a message in another place than its own (it crossed a bucket,
it moved to a dead-letter queue kept longer, a span came late) are recorded in ``strays``: rare, and small.

**Retention** (``purge_step``) removes whole places, never rows from ingest. A message expires once the bucket
of its newest span has ended before the cutoff of every queue it has a span on (``retention_granularity_ns``):
with daily buckets, ``7d`` keeps a message 7 to 8 days. One step takes one place, oldest bucket first:

1. *Drop*, when every queue in the place is past its cutoff. In short transactions: the place is marked
   ``dropping`` (ingest writes to the next bucket instead, and waits rather than change a row there); the
   place's stray rows move to their message's place; the rows other places hold of the place's messages are
   deleted (a message goes whole); then each table's partition is detached (``CONCURRENTLY``: no lock that
   stops exports or pages) and dropped.
2. *Sweep*, only for a place that mixes queues with different cutoffs (the retention changed after its spans
   were stored, or the store was given none): the expired messages are deleted in batches of ``limit``,
   outside ingest. A place is swept again only when more of its queues have expired.

With the retention given to the store (as the server does), every place has one retention class, so a pass is
drops only. With several replicas on one database, one runs retention at a time: the one holding the retention
lease, a session advisory lock that Postgres releases when that replica dies. Give every replica the same
retention configuration.

**Locks** (QV-113). A statement on a partitioned table locks, until its transaction ends, every partition (and
its indexes) that it can't rule out when it is planned. So no statement of an export, a by-message read or
retention looks a message ID or a receipt hash up across all places: the lookup tables (``mid_places``,
``hash_places``, pg_schema.py) give the places that may hold its rows, and the statement names each of them
(``_each``: one copy a place, the place a literal). Statements that name their place by a parameter
(``rclass = %s AND bucket = %s``) are pruned at plan time only in a custom plan, so every connection sets
``plan_cache_mode = force_custom_plan``: psycopg prepares a statement run often, and Postgres's generic plan of
it would lock every partition. What an export locks is then the partitions of the places its messages and
hashes are in, however many places the store has (85 lock entries for a four-span export joining across two
places, with 10 places or 400).

**Pages** (QV-116): the lists of messages, stuck messages, tenants, a queue's messages, ``counts`` and
``last_row`` read across all of time. They walk the places a batch at a time (``PAGE_PLACES`` a statement, each
named as above), each statement in its own transaction, so a page holds one batch's locks at a time however
many places there are. The newest-first lists start at the newest bucket and stop once the page is full and the
next place can't hold a row newer than the page's last: a place's rows are no newer than its bucket's end
(``_top_ns``). A ``from`` time skips the places that end before it. The stuck list (by ``stuck_first_ns``,
which no bucket bounds), the tenant search and activity, a queue's messages, ``counts`` and ``last_row`` read
every place, a batch at a time. Each batch reads its own snapshot, not one for the whole page. So a message
that moves to a newer place while a page is read (a span of it came) is listed once if both batches see it,
and not at all if the batch with its new place ran before it moved and the one with its old place after. The
next page doesn't list it either: it is newer than the cursor now, as when the span comes between two pages.

**New places** (QV-118). ``CREATE TABLE … PARTITION OF`` locks the parent ACCESS EXCLUSIVE: it waits for every
open transaction that has read it (a ``pg_dump`` for its whole run, and every running export), and every export
queues behind the request, which with the exports' message locks can form a cycle. So a place's partitions are
made as tables of their own and then attached (``pg_schema.place_ddl``), which locks the parent SHARE UPDATE
EXCLUSIVE, conflicting only with other DDL and ANALYZE of the parent. That still waits at most
``DDL_LOCK_TIMEOUT_MS``. Retention's pass creates the places of today and the next ``PRECREATE_AHEAD``
buckets, so a pass that can't is retried at the next one long before the place is needed; an export that needs
a place that isn't there (a span more than that late or early) and can't create it in ``DDL_ATTEMPTS`` tries
is answered busy (WriterBusy: 503 with Retry-After), nothing stored. A new place's partitions also have no planner statistics
until autoanalyze reaches them, and with none Postgres estimates ``rclass = c AND bucket = b`` at 1 row of
the partition, whatever it holds, so the export's statements pick plans for an empty table (a hash of the
whole partition for the settle UPDATE). Once this replica has written ``ANALYZE_AFTER_SPANS`` spans to a place
whose partitions have never been analysed, it analyses them, after that export commits.

**Locks** (QV-107). Locks are held in Postgres's shared lock table, whose size is fixed by
``max_locks_per_transaction``. A page's statement locks at most ``PAGE_PLACES`` places, and an export or a
by-message read only its messages' places, so what that setting must be no longer grows with the places.
``lock_need`` (pg_schema.py) works it out, checked at start and as places are added, and a shortfall is
logged as a warning saying what to set. The table is shared, so whatever fills it fails exports too; a full lock
table is reported as that, on ingest and on reads, not as an unreachable database.
"""
from __future__ import annotations

import contextlib
import hashlib
import json
import logging
import threading
import time
from collections import Counter
from typing import Iterable, Optional

import psycopg
import psycopg_pool
from psycopg import errors, sql
from psycopg.rows import dict_row

from . import body_table, pg_schema, summary
from .db import gate_body
from .otlp import RECORD_FIELDS
from .store import (SETTLE_KINDS, TENANT_MAX_CHARS, Cutoffs, ExportAbandoned, PurgeStep, Store,
                    StoreUnavailable, StuckCandidate, TenantActivity, WriterBusy)
from .summary_table import COLUMNS, FIELDS, LISTS, ascii_lower

log = logging.getLogger("trailbeam.server.postgres")

DAY_NS = 86_400 * 10 ** 9
FOREVER_CLASS = 2 ** 62  # rclass of a queue kept forever; 0: no retention was given to the store
URL_SCHEMES = ("postgres://", "postgresql://")

# Every column of a span row, as SQLite's spans table has them (store.py "Rows").
SPAN_COLUMNS = (["row"] + [f for f in RECORD_FIELDS] +
                ["attributes", "mid", "mid_source", "receive_row", "body_dropped", "body_id"])
_INT = {"time_ns", "start_ns", "receive_count", "sent_ts_ms", "first_receive_ts_ms", "visibility_timeout", "pid",
        "order_issued", "order_returned", "body_id"}
_INSERT_COLUMNS = ["rclass", "bucket"] + list(RECORD_FIELDS) + ["attributes", "mid", "mid_source", "body_dropped", "body_id"]


def _q(c: str) -> str:
    return '"row"' if c == "row" else c


_SPAN_SELECT = ", ".join(f"s.{_q(c)}" for c in SPAN_COLUMNS)


class _NeedPlaces(Exception):
    def __init__(self, places):
        super().__init__(f"{len(places)} place(s) to create")
        self.places = set(places)


class _NeedLocks(Exception):
    """The join touched messages whose lock the export does not hold, and another export holds one: retry with
    them locked from the start (in the one sorted order)."""
    def __init__(self, keys):
        super().__init__(f"{len(keys)} more message lock(s)")
        self.keys = set(keys)


class _PlaceBusy(Exception):
    """A row the export must change is in a place retention is dropping: wait for the drop, then retry."""


def _key(text: str) -> int:
    """A 64-bit advisory lock key."""
    return int.from_bytes(hashlib.blake2b(text.encode("utf-8", "surrogatepass"), digest_size=8).digest(), "big",
                          signed=True)


def _body_bytes(text: str) -> bytes:
    return text.encode("utf-8", "surrogatepass")


def _each(places, query: str, params=(), alias: str = "", **aliases: str) -> tuple[str, list]:
    """``query`` once per place, ``{p}`` its WHERE term naming the place (columns of ``alias``), as one UNION ALL,
    and the parameters of every copy. The place is a literal, so each copy is pruned to its place's partition
    when it is planned, in a generic plan too (module docstring, "Locks"). Each of ``aliases`` is another such
    term: ``q="q"`` makes ``{q}`` name the place by the columns of ``q`` (a subquery on another table of the same
    place: a correlated ``q.rclass = s.rclass`` is pruned only when run). Not one statement with several places:
    Postgres doesn't prune ``(rclass = 0 AND bucket = 1) OR (rclass = 0 AND bucket = 2)`` on a two-column range
    key (it takes out ``rclass = 0`` and can't prune by ``bucket`` alone), nor a constant FALSE. So never call it
    with no places: skip the statement."""
    places = sorted(set(places))
    assert places, "a statement naming no places locks them all: skip it"
    def term(alias, c, b):
        a = f"{alias}." if alias else ""
        return f"{a}rclass = {int(c)} AND {a}bucket = {int(b)}"
    parts = [query.format(p=term(alias, c, b), **{k: term(v, c, b) for k, v in aliases.items()}) for c, b in places]
    return " UNION ALL ".join(f"({q})" for q in parts), list(params) * len(places)


def _mid_places(conn, mids) -> dict:
    """{mid: {place, ...}}: the places that may hold rows of these messages (mid_places)."""
    out: dict = {}
    mids = sorted({m for m in mids if m is not None})
    if mids:
        for r in conn.execute("SELECT mid, rclass, bucket FROM mid_places WHERE mid = ANY(%s)", (mids,)).fetchall():
            out.setdefault(r["mid"], set()).add((r["rclass"], r["bucket"]))
    return out


def _note_mids(conn, rows) -> None:
    """mid_places rows for these (mid, rclass, bucket)."""
    rows = sorted({r for r in rows if r[0] is not None})
    if rows:
        conn.execute("""INSERT INTO mid_places (mid, rclass, bucket)
                        SELECT * FROM unnest(%s::text[], %s::bigint[], %s::bigint[]) ON CONFLICT DO NOTHING""",
                     [[r[k] for r in rows] for k in range(3)])


class PostgresStore(Store):
    """Thread-safe: an ingest pool and a read pool (``ingest_connections``, ``read_connections``).

    ``retention``: retention.Retention (or anything with ``default`` and ``queues`` in seconds, None forever),
    the retention classes spans are placed by. None: one class; retention then sweeps places whose queues have
    different cutoffs instead of dropping them. ``bucket_ns``: the bucket width G (the granularity)."""

    INGEST_ATTEMPTS = 50
    DDL_LOCK_TIMEOUT_MS = 1_000  # the longest a partition's DDL waits for its lock ("New places")
    DDL_ATTEMPTS = 3             # tries an export makes at a missing place's DDL before it is answered busy
    PRECREATE_AHEAD = 2          # buckets after the current one that a retention pass creates
    ANALYZE_AFTER_SPANS = 1_000  # spans this replica writes to a never-analysed place before it analyses it
    cancels_abandoned_exports = True
    WATCH_INTERVAL_S = 0.2  # how often an export's client is checked (``insert(abandoned=...)``)

    def __init__(self, url: str, *, store_bodies: Iterable[str] = (), retention=None, bucket_ns: int = DAY_NS,
                 ingest_connections: int = 4, read_connections: int = 8, statement_timeout_ms: int = 60_000):
        if bucket_ns < 1:
            raise ValueError("bucket_ns must be at least 1")
        self.url = url
        self.store_bodies = frozenset(store_bodies)
        self.retention = retention
        self.bucket_ns = int(bucket_ns)
        self.retention_granularity_ns = self.bucket_ns
        self._lock_connections = read_connections  # pages: the statements that lock the most places ("Locks")
        self._lock_warned_at: Optional[float] = None
        self.lock_need: Optional[pg_schema.LockNeed] = None
        with psycopg.connect(url, autocommit=True) as c:
            self.migrated = pg_schema.open_schema(c)
            self.server_version = c.info.server_version
            self._check_locks(c)
            # the retention lease's advisory lock key: one per schema, so two Trailbeams in one database don't share it
            self._lease_key = _key("retention lease:" + c.execute("SELECT current_schema()").fetchone()[0])

        def ingest_conf(conn):
            conn.execute(f"SET statement_timeout = {int(statement_timeout_ms)}")
            conn.execute("SET lock_timeout = 0")
            conn.execute("SET plan_cache_mode = force_custom_plan")  # "Locks" above

        def read_conf(conn):
            conn.execute(f"SET statement_timeout = {int(statement_timeout_ms)}")
            conn.execute("SET default_transaction_read_only = on")
            conn.execute("SET plan_cache_mode = force_custom_plan")

        kw = {"autocommit": True, "row_factory": dict_row}
        self.ingest = psycopg_pool.ConnectionPool(url, min_size=1, max_size=ingest_connections, kwargs=kw,
                                                  configure=ingest_conf, name="trailbeam-ingest", open=True)
        self.reads = psycopg_pool.ConnectionPool(url, min_size=1, max_size=read_connections, kwargs=kw,
                                                 configure=read_conf, name="trailbeam-read", open=True)
        self._admin_lock = threading.Lock()
        self._known_queues: set = set()      # (rclass, bucket, queue) rows of place_queues written
        self._written: Counter = Counter()   # place -> spans written, until it is analysed or known to be
        self._analysed: set = set()          # places whose partitions have statistics (or were just analysed)
        self._to_analyse: set = set()        # places due for ANALYZE, after the export that made them due
        self._cache_lock = threading.Lock()
        self._tx = threading.local()         # per export: place_queues rows written, cached after the commit
        self._watched: dict = {}             # exports whose client is checked: key -> [conn, abandoned, cancelled]
        self._watch_lock = threading.Lock()
        self._lease = None                   # the connection holding the retention lease's lock, while this replica holds it
        self._lease_lock = threading.Lock()
        self._stop = threading.Event()
        self._watcher = threading.Thread(target=self._watch_loop, name="trailbeam-pg-abandoned", daemon=True)
        self._watcher.start()

    def close(self) -> None:
        self._stop.set()
        self._watcher.join()
        self._release_lease()
        self.ingest.close()
        self.reads.close()

    # ---- placement ----------------------------------------------------------------------------

    def queue_class(self, queue: Optional[str]) -> int:
        r = self.retention
        if r is None:
            return 0
        queues = getattr(r, "queues", None) or {}
        s = queues[queue] if queue in queues else r.default
        return FOREVER_CLASS if s is None else int(s)

    def bucket(self, time_ns: int) -> int:
        return int(time_ns) // self.bucket_ns

    def _ensure_places(self, places, wait_s: Optional[float] = None) -> None:
        """Create the partitions of these places, outside any export's transaction (DDL locks the parent). Each
        statement waits at most DDL_LOCK_TIMEOUT_MS for its locks: errors.LockNotAvailable past it, with the
        places made so far kept. ``wait_s``: the longest to wait for this replica's other place creation
        (TimeoutError past it); None, as long as it takes."""
        if not self._admin_lock.acquire(timeout=-1 if wait_s is None else wait_s):
            raise TimeoutError("another export of this replica is creating places")
        try:
            self._create_places(places)
        finally:
            self._admin_lock.release()

    def _create_places(self, places) -> None:
        with psycopg.connect(self.url, autocommit=True) as c:
            c.execute(f"SET lock_timeout = {int(self.DDL_LOCK_TIMEOUT_MS)}")
            for rclass, bucket in sorted(places):
                for table, name, create, attach in pg_schema.place_ddl(rclass, bucket):
                    if self._is_partition(c, table, name):
                        continue
                    try:
                        c.execute(create)
                    except (errors.UniqueViolation, errors.DuplicateTable):
                        pass  # another server made it at the same moment
                    try:
                        c.execute(attach)
                    except errors.WrongObjectType:
                        pass  # another server attached it at the same moment ("is already a partition")
                c.execute("INSERT INTO places (rclass, bucket) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                          (rclass, bucket))
            self._check_locks(c, throttle_s=self.LOCK_WARNING_INTERVAL_S)

    @staticmethod
    def _is_partition(c, table: str, name: str) -> bool:
        return c.execute(
            """SELECT 1 FROM pg_inherits i JOIN pg_class ch ON ch.oid = i.inhrelid JOIN pg_class pa ON pa.oid = i.inhparent
               WHERE ch.relname = %s AND pa.relname = %s
                 AND ch.relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = current_schema())""",
            (name, table)).fetchone() is not None

    LOCK_WARNING_INTERVAL_S = 3600.0  # a shortfall found as places are added is logged at most this often

    def _check_locks(self, conn, throttle_s: float = 0.0) -> None:
        """Compare what this replica's statements can lock with the server's lock table (pg_schema.LockNeed),
        and warn when it is short. Never raises: the check must not stop a store that works under light load."""
        try:
            need = pg_schema.lock_need(conn, self._lock_connections, self.PAGE_PLACES)
        except psycopg.Error as e:
            log.info("could not check max_locks_per_transaction: %s", e)
            return
        self.lock_need = need
        if not need.short:
            return
        now = time.monotonic()
        if throttle_s and self._lock_warned_at is not None and now - self._lock_warned_at < throttle_s:
            return
        self._lock_warned_at = now
        log.warning("%s", need.message())

    def _lock_table_full(self, e: BaseException) -> Optional[str]:
        """For a "out of shared memory" error from the lock table: what to say about it; else None."""
        if not isinstance(e, errors.OutOfMemory) or "max_locks_per_transaction" not in (e.diag.message_hint or ""):
            return None
        need = self.lock_need
        return ("Postgres's lock table is full (out of shared memory: too many partitions locked at once). "
                + (need.message() if need is not None and need.short else
                   "Raise max_locks_per_transaction (README: Postgres lock table)"))

    @staticmethod
    def _lock_places(conn, places) -> dict:
        """Lock (FOR SHARE) and read the state of these places: {place: state}; a missing place is absent.
        Retention takes the rows FOR UPDATE to mark a place ``dropping``, so it waits for this transaction."""
        places = sorted(set(places))
        if not places:
            return {}
        rows = conn.execute(
            """SELECT rclass, bucket, state FROM places
               WHERE (rclass, bucket) IN (SELECT * FROM unnest(%s::bigint[], %s::bigint[]))
               ORDER BY rclass, bucket FOR SHARE""",
            ([p[0] for p in places], [p[1] for p in places])).fetchall()
        return {(r["rclass"], r["bucket"]): r["state"] for r in rows}

    def _resolve(self, conn, wanted: dict) -> dict:
        """``wanted``: {key: (rclass, bucket)} -> {key: live place}, locked. A bucket being dropped is replaced
        by the next one (a span late enough to land there is kept a bucket longer); a missing one raises
        _NeedPlaces."""
        out, todo = {}, dict(wanted)
        while todo:
            states = self._lock_places(conn, set(todo.values()))
            missing = {p for p in todo.values() if p not in states}
            if missing:
                raise _NeedPlaces(missing)
            nxt = {}
            for k, p in todo.items():
                if states[p] == "live":
                    out[k] = p
                else:
                    nxt[k] = (p[0], p[1] + 1)
            todo = nxt
        return out

    def _require_live(self, conn, places) -> None:
        """Places holding rows this export changes: all must be live (else _PlaceBusy)."""
        states = self._lock_places(conn, places)
        for p in set(places):
            if states.get(p) != "live":
                raise _PlaceBusy(p)

    def _note_queues(self, conn, pairs) -> None:
        """place_queues rows for these (rclass, bucket, queue), each written once per process."""
        with self._cache_lock:
            new = sorted(set(pairs) - self._known_queues)
        if not new:
            return
        conn.cursor().executemany(
            "INSERT INTO place_queues (rclass, bucket, queue) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING", new)
        self._tx.noted += new  # cached once the export commits

    # ---- writes ---------------------------------------------------------------------------

    def _watch_loop(self) -> None:
        """Cancel the database work of exports whose client has gone (rule 5 of QV-69): the client will retry
        the export, so finishing it only adds load. One thread for every export."""
        while not self._stop.wait(self.WATCH_INTERVAL_S):
            with self._watch_lock:
                for w in self._watched.values():
                    if w[2]:
                        continue
                    try:
                        gone = w[1]()
                    except Exception:  # noqa: BLE001
                        gone = False
                    if gone:
                        w[2] = True
                        try:
                            (getattr(w[0], "cancel_safe", None) or w[0].cancel)()
                        except Exception as e:  # noqa: BLE001
                            log.debug("cancel failed: %s", e)

    INGEST_CONNECTION_WAIT_S = 10.0  # for a pooled connection, before an export is answered StoreUnavailable

    def _ingest_saturated(self) -> bool:
        """Every ingest connection open and in use: busy, not unreachable (an unreachable database leaves the
        pool short of connections, as it can't open new ones)."""
        st = self.ingest.get_stats()
        return st.get("pool_size", 0) >= self.ingest.max_size and st.get("pool_available", 0) == 0

    def ping(self, timeout_s: float = 2.0) -> None:
        try:
            with self.reads.connection(timeout=timeout_s) as c:
                c.execute("SELECT 1")
        except (psycopg.OperationalError, psycopg_pool.PoolTimeout) as e:
            raise StoreUnavailable(f"Postgres is unreachable: {e}") from None

    def insert(self, records: Iterable[dict], *, wait_s: Optional[float] = None, timings: Optional[dict] = None,
               abandoned=None) -> tuple[int, int]:
        """Store.insert. ``abandoned``: see Store.cancels_abandoned_exports; checked every WATCH_INTERVAL_S
        while the export's transaction runs, which is then cancelled (ExportAbandoned, nothing stored).
        StoreUnavailable when the database can't be reached (nothing stored: the client sends it again).

        ``wait_s`` bounds the wait for a pooled ingest connection (``ingest_connections`` of them: the
        exports this replica writes at once). Past it, with every connection in use by other exports, raise
        WriterBusy, nothing written; with connections missing from the pool (it can't open them), the
        database is unreachable: StoreUnavailable. Only the first connection is bounded: an export retried
        after a lock conflict has nothing committed either way, but is already this replica's turn."""
        try:
            result = self._insert_records(records, abandoned, wait_s, timings)
        except (psycopg.OperationalError, psycopg_pool.PoolTimeout) as e:
            if isinstance(e, errors.QueryCanceled):
                raise  # a statement timeout: the database is there, the export is too slow
            full = self._lock_table_full(e)
            if full is not None:  # the database is there, and the export may pass when it is sent again
                log.error("export not stored: %s", full)
                raise StoreUnavailable(full) from None
            raise StoreUnavailable(f"Postgres is unreachable: {e}") from None
        if self._to_analyse:
            self._analyse_due()
        return result

    def _insert_records(self, records, abandoned, wait_s=None, timings=None) -> tuple[int, int]:
        t0 = time.perf_counter()
        t1 = None  # when the first connection came
        records = list(records)  # an exporter's iterator that fails part way stores nothing
        if not records:
            if timings is not None:
                timings["wait_s"] = timings["write_s"] = 0.0
            return 0, 0
        prepared = []
        for r in records:
            own = r.get("message_id")
            attrs, dropped = gate_body(r.get("attributes", {}), r.get("queue"), self.store_bodies)
            attrs, body = body_table.split(r["kind"], own, attrs)
            prepared.append((r, own, body_table.attributes_json(attrs), dropped, body))
        delay = 0.01
        more_locks: set = set()  # message locks a previous attempt's join found it needs
        for attempt in range(self.INGEST_ATTEMPTS):
            if abandoned is not None and abandoned():
                raise ExportAbandoned()
            bounded = wait_s is not None and t1 is None
            try:
                with self.ingest.connection(timeout=min(wait_s, self.INGEST_CONNECTION_WAIT_S) if bounded
                                            else self.INGEST_CONNECTION_WAIT_S) as conn:
                    if t1 is None:
                        t1 = time.perf_counter()
                    watch = [conn, abandoned, False]
                    if abandoned is not None:
                        with self._watch_lock:
                            self._watched[id(watch)] = watch
                    try:
                        self._tx.noted, self._tx.written = [], Counter()
                        with conn.transaction():
                            result = self._insert(conn, prepared, more_locks)
                    except psycopg.Error:
                        if watch[2]:
                            raise ExportAbandoned() from None
                        raise
                    finally:
                        if abandoned is not None:
                            with self._watch_lock:
                                del self._watched[id(watch)]
                            if watch[2]:
                                conn.close()  # a cancel may still be on its way: never reuse the connection
                    with self._cache_lock:
                        self._known_queues.update(self._tx.noted)
                        for p, n in self._tx.written.items():
                            if p not in self._analysed:
                                self._written[p] += n
                                if self._written[p] >= self.ANALYZE_AFTER_SPANS:
                                    del self._written[p]
                                    self._analysed.add(p)
                                    self._to_analyse.add(p)
                    if timings is not None:
                        timings["wait_s"] = t1 - t0
                        timings["write_s"] = time.perf_counter() - t1
                    return result
            except psycopg_pool.PoolTimeout:
                if bounded and self._ingest_saturated():
                    raise WriterBusy(f"every ingest connection was busy for {wait_s:g} s") from None
                raise
            except _NeedPlaces as e:
                self._ensure_places_for_ingest(e.places)
            except _NeedLocks as e:
                more_locks |= e.keys
            except _PlaceBusy:
                time.sleep(delay)
                delay = min(delay * 2, 0.5)
            except (errors.DeadlockDetected, errors.SerializationFailure):
                time.sleep(delay)
            except errors.CheckViolation:
                pass  # "no partition of relation found for row": its place was dropped meanwhile; _resolve sees it
        raise RuntimeError(f"insert did not complete after {self.INGEST_ATTEMPTS} attempts")

    def _insert(self, conn, prepared, more_locks=frozenset()) -> tuple[int, int]:
        keys = set(more_locks)
        for r, own, *_ in prepared:
            if own:
                keys.add(_key("m:" + own))
            if r["kind"] == "receive" or r["kind"] in SETTLE_KINDS:
                keys.add(_key(f"h:{r.get('receipt_hmac')}:{r.get('key_id')}"))
        conn.execute("SELECT pg_advisory_xact_lock(k) FROM unnest(%s::bigint[]) AS k", (sorted(keys),))

        want = {i: (self.queue_class(r.get("queue")), self.bucket(r["time_ns"])) for i, (r, *_) in enumerate(prepared)}
        places = self._resolve(conn, want)

        # bodies: one row per (message, text), wherever the message's other rows are (refresh moves it)
        body_ids: dict = {}
        wanted_bodies = {}
        for i, (r, own, _, _, body) in enumerate(prepared):
            if body is not None:
                wanted_bodies.setdefault((own, hashlib.sha256(_body_bytes(body)).digest()), (i, body))
        if wanted_bodies:
            ks = list(wanted_bodies)
            known = set().union(*_mid_places(conn, {k[0] for k in ks}).values())
            if known:
                for row in conn.execute(*_each(
                        known, "SELECT mid, sha, id FROM bodies WHERE {p} "
                               "AND (mid, sha) IN (SELECT * FROM unnest(%s::text[], %s::bytea[]))",
                        [[k[0] for k in ks], [k[1] for k in ks]])).fetchall():
                    body_ids.setdefault((row["mid"], bytes(row["sha"])), row["id"])
            new = [k for k in ks if k not in body_ids]
            if new:
                ids = conn.execute("SELECT nextval('body_ids') AS id FROM generate_series(1, %s)", (len(new),)).fetchall()
                rows = []
                for k, idrow in zip(new, ids):
                    i, body = wanted_bodies[k]
                    body_ids[k] = idrow["id"]
                    rows.append((places[i][0], places[i][1], idrow["id"], k[0], k[1], _body_bytes(body)))
                conn.cursor().executemany(
                    "INSERT INTO bodies (rclass, bucket, id, mid, sha, body) VALUES (%s, %s, %s, %s, %s, %s)", rows)
                _note_mids(conn, [(r[3], r[0], r[1]) for r in rows])

        cols = {c: [] for c in _INSERT_COLUMNS}
        for i, (r, own, attrs, dropped, body) in enumerate(prepared):
            cols["rclass"].append(places[i][0])
            cols["bucket"].append(places[i][1])
            for f in RECORD_FIELDS:
                cols[f].append(r.get(f))
            cols["attributes"].append(attrs)
            cols["mid"].append(own)
            cols["mid_source"].append("span" if own else None)
            cols["body_dropped"].append(dropped)
            cols["body_id"].append(None if body is None else
                                   body_ids[(own, hashlib.sha256(_body_bytes(body)).digest())])
        types = ["bigint[]" if c in _INT or c in ("rclass", "bucket") else "text[]" for c in _INSERT_COLUMNS]
        names = ", ".join(_INSERT_COLUMNS)
        stmt = (f"INSERT INTO spans ({names}) SELECT {names} FROM unnest("
                + ", ".join(f"%s::{t}" for t in types)
                + f") WITH ORDINALITY AS u({names}, ord) ORDER BY ord "
                  "ON CONFLICT DO NOTHING RETURNING \"row\", trace_id, span_id")
        got = conn.execute(stmt, [cols[c] for c in _INSERT_COLUMNS]).fetchall()
        new_rows = {(g["trace_id"], g["span_id"]): g["row"] for g in got}
        inserted, dupes, order = 0, 0, []
        for i, (r, *_rest) in enumerate(prepared):
            k = (r["trace_id"], r["span_id"])
            if k in new_rows:
                order.append((i, new_rows.pop(k)))
                inserted += 1
            else:
                dupes += 1
        if not order:
            return 0, dupes
        self._tx.written.update(places[i] for i, _ in order)
        _note_mids(conn, [(prepared[i][1], places[i][0], places[i][1]) for i, _ in order])
        hashed = sorted({(prepared[i][0].get("receipt_hmac"), prepared[i][0].get("key_id")) + places[i] for i, _ in order
                         if (prepared[i][0]["kind"] == "receive" or prepared[i][0]["kind"] in SETTLE_KINDS)
                         and prepared[i][0].get("receipt_hmac") is not None and prepared[i][0].get("key_id") is not None})
        if hashed:
            conn.execute("""INSERT INTO hash_places (receipt_hmac, key_id, rclass, bucket)
                            SELECT * FROM unnest(%s::text[], %s::text[], %s::bigint[], %s::bigint[])
                            ON CONFLICT DO NOTHING""", [[h[k] for h in hashed] for k in range(4)])

        j0 = time.perf_counter()
        touched, updates = self._join(conn, prepared, places, order)
        # A settle span joined by its receipt hash touches a message the export has no span of (a settle carries
        # no message ID): lock it too before its summary is rewritten, or an export of that message's own spans,
        # at the same moment on this or another replica, and this one each rewrite it without the other's span.
        # Taken without waiting (waiting here, out of the sorted order, could deadlock): when another export
        # holds one, this attempt is rolled back and the next takes it with the others, in order.
        extra = sorted({_key("m:" + m) for m in touched} - keys)
        if extra and not conn.execute("SELECT bool_and(pg_try_advisory_xact_lock(k)) AS ok FROM unnest(%s::bigint[]) AS k",
                                      (extra,)).fetchone()["ok"]:
            raise _NeedLocks(extra)
        by_place: dict = {}
        for u in updates:
            by_place.setdefault((u[0], u[1]), []).append(u)
        for (uc, ub), us in sorted(by_place.items()):  # one statement a place, so that it is pruned when planned
            conn.execute(
                """UPDATE spans s SET mid = u.mid, mid_source = 'receipt_hash', receive_row = u.rr
                   FROM unnest(%s::bigint[], %s::text[], %s::bigint[]) AS u("row", mid, rr)
                   WHERE s.rclass = %s AND s.bucket = %s AND s."row" = u."row" """,
                [[u[2] for u in us], [u[3] for u in us], [u[4] for u in us], uc, ub])
        _note_mids(conn, [(u[3], u[0], u[1]) for u in updates])
        on_join = self.on_join
        if on_join is not None:
            # one batched join for the whole export: each joined span gets its share of the time
            kinds = ["receive" if prepared[i][0]["kind"] == "receive" else "settle" for i, _ in order
                     if prepared[i][0]["kind"] == "receive" or prepared[i][0]["kind"] in SETTLE_KINDS]
            if kinds:
                share = (time.perf_counter() - j0) / len(kinds)
                for k in kinds:
                    on_join(k, share)
        self._note_queues(conn, {(places[i][0], places[i][1], prepared[i][0]["queue"]) for i, _ in order})
        self._refresh(conn, touched)
        return inserted, dupes

    def _ensure_places_for_ingest(self, places) -> None:
        """An export's missing places, tried DDL_ATTEMPTS times ("New places"). Still not made: other DDL (or
        an ANALYZE of a parent) holds a lock that attaching needs. Answer busy (WriterBusy: 503), nothing
        stored; the exporter sends it again."""
        for attempt in range(self.DDL_ATTEMPTS):
            try:
                self._ensure_places(places, wait_s=2 * self.DDL_LOCK_TIMEOUT_MS / 1000)
                return
            except (errors.LockNotAvailable, TimeoutError):
                pass
        log.warning("ingest: could not create the partitions of %d new place(s) in %d tries of %d ms: another "
                    "transaction (a schema change?) holds a lock on the tables; answering busy",
                    len(places), self.DDL_ATTEMPTS, self.DDL_LOCK_TIMEOUT_MS)
        raise WriterBusy("could not create a new day's partitions now: another transaction holds a lock on "
                         "the tables")

    def _analyse_due(self) -> None:
        """ANALYZE the partitions of the places due ("New places"), unless they have statistics already (autoanalyze,
        or another replica). Outside every export's transaction; a failure is logged, and autoanalyze does it."""
        with self._cache_lock:
            due, self._to_analyse = sorted(self._to_analyse), set()
        try:
            with psycopg.connect(self.url, autocommit=True) as c:
                c.execute(f"SET lock_timeout = {int(self.DDL_LOCK_TIMEOUT_MS)}")
                for rclass, bucket in due:
                    names = [pg_schema.partition_name(t, rclass, bucket) for t in pg_schema.PARTITIONED]
                    if c.execute("""SELECT 1 FROM pg_stats WHERE schemaname = current_schema() AND tablename = %s
                                    AND attname = 'bucket'""", (names[0],)).fetchone() is not None:
                        continue
                    t0 = time.perf_counter()
                    try:
                        c.execute("ANALYZE " + ", ".join(names))
                    except (errors.LockNotAvailable, errors.UndefinedTable):
                        continue  # being analysed (or dropped) already
                    log.info("analysed the new partitions of place (%d, %d) in %.0f ms", rclass, bucket,
                             (time.perf_counter() - t0) * 1000)
        except psycopg.Error as e:
            log.warning("could not analyse new partitions (autoanalyze will): %s", e)

    def _join(self, conn, prepared, places, order):
        """The receipt-hash join, in the export's order, exactly as SQLite runs it span by span (db.py). Returns
        the messages touched and the settle rows to update: (rclass, bucket, row, mid, receive_row)."""
        hashes = {(prepared[i][0].get("receipt_hmac"), prepared[i][0].get("key_id")) for i, _ in order
                  if prepared[i][0]["kind"] == "receive" or prepared[i][0]["kind"] in SETTLE_KINDS}
        hashes = sorted(h for h in hashes if h[0] is not None and h[1] is not None)
        mine = {row for _, row in order}
        receives: dict = {}
        unjoined: dict = {}
        if hashes:
            a, b = [h[0] for h in hashes], [h[1] for h in hashes]
            known = {(r["rclass"], r["bucket"]) for r in conn.execute(
                """SELECT DISTINCT rclass, bucket FROM hash_places
                   WHERE (receipt_hmac, key_id) IN (SELECT * FROM unnest(%s::text[], %s::text[]))""", (a, b)).fetchall()}
            q, qp = _each(known,  # never empty: it has the places of this export's own spans
                          """SELECT "row", message_id, time_ns, receipt_hmac, key_id, kind, mid, rclass, bucket,
                                    receive_row
                             FROM spans WHERE {p}
                               AND (receipt_hmac, key_id) IN (SELECT * FROM unnest(%s::text[], %s::text[]))
                               AND (kind = 'receive' OR (kind IN ('delete', 'change_visibility') AND receive_row IS NULL))""",
                          [a, b])
            for x in conn.execute(f'SELECT * FROM ({q}) u ORDER BY "row"', qp).fetchall():
                if x["row"] in mine:
                    continue
                h = (x["receipt_hmac"], x["key_id"])
                if x["kind"] == "receive":
                    receives.setdefault(h, []).append((x["time_ns"], x["row"], x["message_id"]))
                else:
                    unjoined.setdefault(h, []).append((x["rclass"], x["bucket"], x["row"], x["mid"]))
        touched: set = set()
        updates: dict = {}  # row -> (rclass, bucket, row, mid, receive_row)
        busy = set()
        for i, row in order:
            r, own = prepared[i][0], prepared[i][1]
            h = (r.get("receipt_hmac"), r.get("key_id"))
            if r["kind"] in SETTLE_KINDS:
                cands = receives.get(h, []) if None not in h else []
                if cands:
                    t = r["time_ns"]
                    # SQLite: ORDER BY (time_ns <= t) DESC, CASE WHEN time_ns <= t THEN -time_ns ELSE time_ns END
                    best = min(cands, key=lambda c: (0 if c[0] <= t else 1, -c[0] if c[0] <= t else c[0], c[1]))
                    updates[row] = (places[i][0], places[i][1], row, best[2], best[1])
                    touched.add(best[2])
                else:
                    if None not in h:
                        unjoined.setdefault(h, []).append((places[i][0], places[i][1], row, own))
                    touched.add(own)
            elif r["kind"] == "receive":
                if None not in h:
                    for (rc, bk, srow, smid) in unjoined.pop(h, []):
                        if smid is not None:
                            touched.add(smid)
                        prev = updates.get(srow)
                        updates[srow] = (rc, bk, srow, r.get("message_id"), row)
                        if prev is None and srow not in mine:
                            busy.add((rc, bk))
                    receives.setdefault(h, []).append((r["time_ns"], row, r.get("message_id")))
                touched.add(own)
            else:
                touched.add(own)
        if busy:
            self._require_live(conn, busy)
        return {m for m in touched if m is not None}, list(updates.values())

    def _refresh(self, conn, mids) -> None:
        """Rewrite the summary rows of these messages from their spans (summary_table.refresh's rules), in the
        export's transaction; place them, and record their strays."""
        mids = sorted(set(mids))
        if not mids:
            return
        known = _mid_places(conn, mids)  # with the places of this export's spans: it wrote them above
        where = set().union(*known.values())
        rows_by: dict = {}
        q, qp = _each(where, f"SELECT {_SPAN_SELECT}, s.rclass, s.bucket FROM spans s WHERE {{p}} AND s.mid = ANY(%s)",
                      [mids], "s")
        for r in conn.execute(f'SELECT * FROM ({q}) u ORDER BY mid, time_ns, "row"', qp).fetchall():
            rows_by.setdefault(r["mid"], []).append(r)
        old_by = {r["mid"]: r for r in conn.execute(*_each(
            where, f"SELECT rclass, bucket, strays_json, {', '.join(COLUMNS)} FROM messages WHERE {{p}} AND mid = ANY(%s)",
            [mids])).fetchall()}
        # every place this changes rows in must be live
        self._require_live(conn, {(o["rclass"], o["bucket"]) for o in old_by.values()})
        news = {m: summary.summarize(rows_by[m]) for m in mids if m in rows_by}
        want = {m: (max(self.queue_class(q) for q in s["queues"]), self.bucket(s["last_ns"])) for m, s in news.items()}
        place = self._resolve(conn, want)

        cur = conn.cursor()
        ins_msg, del_rows, upd_cols = [], [], []
        add_q, add_t, add_e, upd_q, upd_t = [], [], [], [], []
        strays_new, bodies_move, noted = [], [], set()
        for m in mids:
            new, old = news.get(m), old_by.get(m)
            if new is None:
                continue  # not reachable from insert (a touched message has the span that touched it)
            pc, pb = place[m]
            lists = _lists(new)
            stray_places = sorted({(r["rclass"], r["bucket"]) for r in rows_by[m]} - {(pc, pb)})
            strays_json = json.dumps([list(p) for p in stray_places])
            noted.update((pc, pb, q) for q in new["queues"])
            if any(r["body_id"] is not None for r in rows_by[m]):
                bodies_move += [(pc, pb, oc, ob, m) for oc, ob in sorted(known.get(m, ())) if (oc, ob) != (pc, pb)]
            values = [int(v) if isinstance(v, bool) else v for v in (new[f] for f in FIELDS)] + list(lists)
            if old is not None and (old["rclass"], old["bucket"]) != (pc, pb):
                del_rows.append((old["rclass"], old["bucket"], m))
                old = None
            if old is None:
                ins_msg.append([pc, pb] + values + [strays_json])
                add_q += [(pc, pb, q, new["last_ns"], m) for q in new["queues"]]
                add_t += [(pc, pb, t, new["last_ns"], m, ascii_lower(t), ns) for t, ns in new["tenants"].items()]
                add_e += [(pc, pb) + e for e in _events(new, json.loads(lists[2]))]
            else:
                changed = [(c, v) for c, v in zip(COLUMNS, values) if old[c] != v]
                if old["strays_json"] != strays_json:
                    changed.append(("strays_json", strays_json))
                if changed:
                    upd_cols.append((pc, pb, m, changed))
                last, was = new["last_ns"], old["last_ns"]
                if last != was or lists[0] != old["queues_json"]:
                    del_rows.append((pc, pb, m, "q"))
                    add_q += [(pc, pb, q, last, m) for q in new["queues"]]
                if last != was or lists[1] != old["tenants_json"]:
                    del_rows.append((pc, pb, m, "t"))
                    add_t += [(pc, pb, t, last, m, ascii_lower(t), ns) for t, ns in new["tenants"].items()]
                if lists[2] != old["events_json"] or old["tenant"] != new["tenant"]:
                    del_rows.append((pc, pb, m, "e"))
                    add_e += [(pc, pb) + e for e in _events(new, json.loads(lists[2]))]
            strays_new += [(sc, sb, m) for sc, sb in stray_places]

        # Rewriting a message's list rows replaces them (they are keyed by last_ns): the old ones go in the same
        # transaction. Summary upkeep, not retention; the rows are the message's own few.
        whole = [d for d in del_rows if len(d) == 3]
        for table in ("messages", "message_queues", "message_tenants", "tenant_events"):
            if whole:
                cur.executemany(f"DELETE FROM {table} WHERE rclass = %s AND bucket = %s AND mid = %s", whole)
        for code, table in (("q", "message_queues"), ("t", "message_tenants"), ("e", "tenant_events")):
            part = [d[:3] for d in del_rows if len(d) == 4 and d[3] == code]
            if part:
                cur.executemany(f"DELETE FROM {table} WHERE rclass = %s AND bucket = %s AND mid = %s", part)
        for pc, pb, m, changed in upd_cols:
            cur.execute(sql.SQL("UPDATE messages SET {} WHERE rclass = %s AND bucket = %s AND mid = %s").format(
                sql.SQL(", ").join(sql.SQL("{} = %s").format(sql.Identifier(c)) for c, _ in changed)),
                [v for _, v in changed] + [pc, pb, m])
        if ins_msg:
            mcols = ["rclass", "bucket"] + list(COLUMNS) + ["strays_json"]
            cur.executemany(f"INSERT INTO messages ({', '.join(mcols)}) VALUES ({', '.join(['%s'] * len(mcols))})",
                            ins_msg)
        if add_q:
            cur.executemany("INSERT INTO message_queues (rclass, bucket, queue, last_ns, mid) VALUES (%s, %s, %s, %s, %s)",
                            add_q)
        if add_t:
            cur.executemany("""INSERT INTO message_tenants (rclass, bucket, tenant, last_ns, mid, tenant_lower, named_ns)
                               VALUES (%s, %s, %s, %s, %s, %s, %s)""", add_t)
        if add_e:
            cur.executemany("""INSERT INTO tenant_events (rclass, bucket, at_ms, mid, seq, kind, tenant, wait_ms)
                               VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""", add_e)
        if bodies_move:
            cur.executemany("UPDATE bodies SET rclass = %s, bucket = %s WHERE rclass = %s AND bucket = %s AND mid = %s",
                            bodies_move)
        _note_mids(conn, [(m, place[m][0], place[m][1]) for m in mids if m in news])
        if strays_new:
            cur.executemany("INSERT INTO strays (rclass, bucket, mid) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
                            sorted(set(strays_new)))
        self._note_queues(conn, noted)

    # ---- retention ------------------------------------------------------------------------

    # Several replicas on one database: one runs retention at a time. The lease is a session advisory lock held on
    # a connection of its own; this replica keeps it while it lives, and Postgres lets it go when the replica
    # (or its connection) dies, so another takes over at its next pass. Every replica keeps calling purge_step:
    # without the lease, a step does nothing.

    def holds_retention_lease(self) -> bool:
        """Take the retention lease if no replica holds it; True while this one does."""
        with self._lease_lock:
            if self._lease is not None:
                try:
                    self._lease.execute("SELECT 1")
                    return True
                except psycopg.Error:
                    log.warning("lost the retention lease (its connection failed); another replica may take it")
                    self._drop_lease_conn()
            try:
                c = psycopg.connect(self.url, autocommit=True)
            except psycopg.Error as e:
                log.warning("retention: could not connect for the lease: %s", e)
                return False
            try:
                if c.execute("SELECT pg_try_advisory_lock(%s)", (self._lease_key,)).fetchone()[0]:
                    self._lease = c
                    log.info("retention: this replica runs retention (it holds the lease)")
                    return True
            except psycopg.Error as e:
                log.warning("retention: could not take the lease: %s", e)
            c.close()
            return False

    @property
    def retention_lease_held(self) -> bool:
        """This replica held the retention lease at its last purge step (for /api/health)."""
        return self._lease is not None

    def _drop_lease_conn(self) -> None:
        try:
            self._lease.close()
        except Exception:  # noqa: BLE001
            pass
        self._lease = None

    def _release_lease(self) -> None:
        with self._lease_lock:
            if self._lease is not None:
                self._drop_lease_conn()  # closing the session releases the lock

    def purge_step(self, cutoffs: Cutoffs, resume: Optional[str] = None, limit: int = 100) -> PurgeStep:
        """Store.purge_step, by place (module docstring). ``resume``: ``<bucket>:<rclass>`` of the next place to
        look at, and for a sweep in progress ``:<last_ns>:<mid>`` of the last message it looked at. Only on the
        replica holding the retention lease (holds_retention_lease); on another, a step does nothing and ends the
        pass."""
        if not self.holds_retention_lease():
            return PurgeStep(0, 0, None)
        if resume is None:
            self._precreate()
            start, sweep_after = (-(2 ** 63), -(2 ** 63)), None
        else:
            parts = resume.split(":", 3)
            start = (int(parts[0]), int(parts[1]))
            sweep_after = (int(parts[2]), parts[3]) if len(parts) == 4 else None
        with self._reading() as c:
            places = c.execute(
                """SELECT rclass, bucket, state, swept FROM places WHERE (bucket, rclass) >= (%s, %s)
                   ORDER BY bucket, rclass LIMIT 1000""", start).fetchall()
            for p in places:
                pc, pb = p["rclass"], p["bucket"]
                end = (pb + 1) * self.bucket_ns
                if end > _latest(cutoffs):
                    return PurgeStep(0, 0, None)  # this and every later bucket ends after every cutoff
                queues = [r["queue"] for r in c.execute(
                    "SELECT queue FROM place_queues WHERE rclass = %s AND bucket = %s", (pc, pb)).fetchall()]
                expired = {q for q in queues if _expired(cutoffs.of(q), end)}
                if p["state"] == "dropping" or expired == set(queues):
                    n, examined, w = self._drop(pc, pb, expired)
                    return PurgeStep(n, examined, f"{pb}:{pc + 1}", w)
                swept = set(json.loads(p["swept"])) if p["swept"] else set()
                if expired and (sweep_after is not None or not expired <= swept):
                    n, examined, done_key, w = self._sweep(pc, pb, expired, sweep_after, limit)
                    if done_key is None:
                        with self.ingest.connection() as w_conn:
                            w_conn.execute("UPDATE places SET swept = %s WHERE rclass = %s AND bucket = %s",
                                           (json.dumps(sorted(expired)), pc, pb))
                        return PurgeStep(n, examined, f"{pb}:{pc + 1}", w)
                    return PurgeStep(n, examined, f"{pb}:{pc}:{done_key[0]}:{done_key[1]}", w)
                sweep_after = None
            if len(places) == 1000:
                last = places[-1]
                return PurgeStep(0, 0, f"{last['bucket']}:{last['rclass'] + 1}")
        return PurgeStep(0, 0, None)

    def _precreate(self) -> None:
        """Places for now and the next PRECREATE_AHEAD buckets, for every class the configuration names, made
        ahead of the spans so that ingest rarely creates a partition ("New places"). Places it can't create now
        are tried again at the next pass, days before ingest needs them."""
        now = self.bucket(time.time_ns())
        classes = {self.queue_class(None)}
        r = self.retention
        if r is not None:
            classes |= {self.queue_class(q) for q in (getattr(r, "queues", None) or {})}
        try:
            self._ensure_places({(c, b) for c in classes for b in range(now, now + self.PRECREATE_AHEAD + 1)})
        except errors.LockNotAvailable:
            log.info("retention: another transaction (a schema change?) held the tables for %d ms; the next places will "
                     "be created at the next pass", self.DDL_LOCK_TIMEOUT_MS)
        except psycopg.Error as e:  # e.g. no CREATE privilege: ingest makes them when spans come
            log.warning("could not create the next places ahead of time: %s", e)

    def _drop(self, pc: int, pb: int, expired: set) -> tuple[int, int, float]:
        """Remove one place whole. Returns (spans deleted, messages and unjoined settles examined, ms of writes)."""
        t0 = time.monotonic()
        with self.ingest.connection() as c:
            with c.transaction():
                st = c.execute("SELECT state FROM places WHERE rclass = %s AND bucket = %s FOR UPDATE",
                               (pc, pb)).fetchone()
                if st is None:
                    return 0, 0, 0.0
                c.execute("UPDATE places SET state = 'dropping' WHERE rclass = %s AND bucket = %s", (pc, pb))
            # From here ingest neither writes to the place nor changes a row in it (it waits: _PlaceBusy). The
            # messages changed below are locked as ingest locks them, so neither sees half of the other's change.
            with c.transaction():
                examined = c.execute(
                    "SELECT (SELECT COUNT(*) FROM messages WHERE rclass = %s AND bucket = %s) + "
                    "(SELECT COUNT(*) FROM spans WHERE rclass = %s AND bucket = %s AND mid IS NULL) AS n",
                    (pc, pb, pc, pb)).fetchone()["n"]
                strays = [x["mid"] for x in c.execute(
                    "SELECT mid FROM strays WHERE rclass = %s AND bucket = %s", (pc, pb)).fetchall()]
                spread = [x["mid"] for x in c.execute(
                    "SELECT mid FROM messages WHERE rclass = %s AND bucket = %s AND strays_json <> '[]'",
                    (pc, pb)).fetchall()]
                c.execute("SELECT pg_advisory_xact_lock(k) FROM unnest(%s::bigint[]) AS k",
                          (sorted({_key("m:" + m) for m in strays + spread}),))
                # 1. rows here of messages placed elsewhere: to their message's place
                homes, found = set().union(*_mid_places(c, strays).values()) - {(pc, pb)}, []
                if homes:
                    found = c.execute(*_each(homes, "SELECT mid, rclass, bucket, strays_json FROM messages "
                                                    "WHERE {p} AND mid = ANY(%s)", [strays])).fetchall()
                for x in found:
                    c.execute("""INSERT INTO hash_places (receipt_hmac, key_id, rclass, bucket)
                                 SELECT DISTINCT receipt_hmac, key_id, %s::bigint, %s::bigint FROM spans
                                 WHERE rclass = %s AND bucket = %s AND mid = %s
                                   AND receipt_hmac IS NOT NULL AND key_id IS NOT NULL
                                   AND kind IN ('receive', 'delete', 'change_visibility')
                                 ON CONFLICT DO NOTHING""", (x["rclass"], x["bucket"], pc, pb, x["mid"]))
                    c.execute("UPDATE spans SET rclass = %s, bucket = %s WHERE mid = %s AND rclass = %s AND bucket = %s",
                              (x["rclass"], x["bucket"], x["mid"], pc, pb))
                    c.execute("UPDATE messages SET strays_json = %s WHERE rclass = %s AND bucket = %s AND mid = %s",
                              (json.dumps([q for q in json.loads(x["strays_json"]) if q != [pc, pb]]),
                               x["rclass"], x["bucket"], x["mid"]))
                    c.execute("""INSERT INTO place_queues (rclass, bucket, queue)
                                 SELECT %s, %s, value FROM json_array_elements_text(
                                     (SELECT queues_json FROM messages WHERE rclass = %s AND bucket = %s AND mid = %s)::json)
                                 ON CONFLICT DO NOTHING""",
                              (x["rclass"], x["bucket"], x["rclass"], x["bucket"], x["mid"]))
                # 2. rows elsewhere of messages placed here go with them: a message goes whole
                n = 0
                for x in c.execute("SELECT mid, strays_json FROM messages WHERE rclass = %s AND bucket = %s "
                                   "AND mid = ANY(%s)", (pc, pb, spread)).fetchall():
                    for sc, sb in json.loads(x["strays_json"]):
                        if (sc, sb) == (pc, pb):
                            continue
                        n += c.execute("DELETE FROM spans WHERE rclass = %s AND bucket = %s AND mid = %s",
                                       (sc, sb, x["mid"])).rowcount
                        c.execute("DELETE FROM strays WHERE rclass = %s AND bucket = %s AND mid = %s", (sc, sb, x["mid"]))
                n += c.execute("SELECT COUNT(*) AS n FROM spans WHERE rclass = %s AND bucket = %s",
                               (pc, pb)).fetchone()["n"]
            # 3. the partitions themselves, then the place's lookup entries (in batches: as many as its messages)
            for t in pg_schema.PARTITIONED:
                self._detach_and_drop(c, t, pc, pb)
            for t in ("mid_places", "hash_places"):
                while c.execute(f"""DELETE FROM {t} WHERE ctid = ANY(ARRAY(
                                        SELECT ctid FROM {t} WHERE rclass = %s AND bucket = %s LIMIT %s))""",
                                (pc, pb, self.LOOKUP_DELETE_BATCH)).rowcount:
                    pass
            with c.transaction():
                c.execute("DELETE FROM strays WHERE rclass = %s AND bucket = %s", (pc, pb))
                c.execute("DELETE FROM place_queues WHERE rclass = %s AND bucket = %s", (pc, pb))
                c.execute("DELETE FROM places WHERE rclass = %s AND bucket = %s", (pc, pb))
        with self._cache_lock:
            self._known_queues = {k for k in self._known_queues if (k[0], k[1]) != (pc, pb)}
            self._analysed.discard((pc, pb))
            self._written.pop((pc, pb), None)
        return n, examined, (time.monotonic() - t0) * 1000

    LOOKUP_DELETE_BATCH = 10_000  # lookup rows deleted a transaction when a place is dropped

    def _detach_and_drop(self, c, table: str, pc: int, pb: int) -> None:
        name = pg_schema.partition_name(table, pc, pb)
        attached = c.execute(
            """SELECT i.inhdetachpending AS pending FROM pg_inherits i
               JOIN pg_class ch ON ch.oid = i.inhrelid JOIN pg_class pa ON pa.oid = i.inhparent
               WHERE ch.relname = %s AND pa.relname = %s
                 AND ch.relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = current_schema())""",
            (name, table)).fetchone()
        if attached is not None:
            how = "FINALIZE" if attached["pending"] else "CONCURRENTLY"
            c.execute(f"ALTER TABLE {table} DETACH PARTITION {name} {how}")
        c.execute(f"DROP TABLE IF EXISTS {name}")

    def _sweep(self, pc: int, pb: int, expired: set, after, limit: int):
        """Delete, in one short transaction, the expired messages among the next ``limit`` of a place that mixes
        queues with different cutoffs. Returns (spans deleted, examined, the last (last_ns, mid) looked at or None
        when the place is done, ms)."""
        t0 = time.monotonic()
        with self.ingest.connection() as c:
            with c.transaction():
                self._require_live(c, {(pc, pb)})
                c.execute("SELECT 1 FROM places WHERE rclass = %s AND bucket = %s FOR UPDATE", (pc, pb))
                p = [pc, pb]
                where = ""
                if after is not None:
                    where = " AND (last_ns, mid) > (%s, %s)"
                    p += list(after)
                msgs = c.execute(
                    f"""SELECT mid, last_ns, queues_json, strays_json FROM messages WHERE rclass = %s AND bucket = %s{where}
                        ORDER BY last_ns, mid LIMIT %s""", p + [limit]).fetchall()
                gone = [m for m in msgs if set(json.loads(m["queues_json"])) <= expired]
                n = 0
                if gone:
                    ids = [m["mid"] for m in gone]
                    other = {tuple(x) for m in gone for x in json.loads(m["strays_json"])} - {(pc, pb)}
                    self._require_live(c, other)
                    for kc, kb in sorted(set().union(*_mid_places(c, ids).values())):
                        n += c.execute("DELETE FROM spans WHERE rclass = %s AND bucket = %s AND mid = ANY(%s)",
                                       (kc, kb, ids)).rowcount
                    for t in ("messages", "message_queues", "message_tenants", "tenant_events", "bodies"):
                        c.execute(f"DELETE FROM {t} WHERE rclass = %s AND bucket = %s AND mid = ANY(%s)", (pc, pb, ids))
                    c.execute("DELETE FROM strays WHERE mid = ANY(%s)", (ids,))
                unjoined = 0
                if len(msgs) < limit:  # the messages are done: the place's unjoined settle spans, by their own queue
                    unjoined = c.execute(
                        """DELETE FROM spans WHERE rclass = %s AND bucket = %s AND mid IS NULL AND receive_row IS NULL
                           AND kind IN ('delete', 'change_visibility') AND queue = ANY(%s)""",
                        (pc, pb, sorted(expired))).rowcount
                    n += unjoined
        done = None if len(msgs) < limit else (msgs[-1]["last_ns"], msgs[-1]["mid"])
        return n, len(msgs) + unjoined, done, (time.monotonic() - t0) * 1000

    # ---- reads ----------------------------------------------------------------------------

    @contextlib.contextmanager
    def _reading(self):
        """A read connection; a full lock table is logged as that before the error goes on."""
        with self.reads.connection() as c:
            try:
                yield c
            except errors.OutOfMemory as e:
                full = self._lock_table_full(e)
                if full is not None:
                    log.error("read failed: %s", full)
                raise

    def _by_message(self, mids, query) -> list:
        """``query(places)``: (SQL, parameters) reading rows of ``mids`` in these places (``_each``); run in one
        snapshot with the lookup that gives the places, so that a message moving meanwhile is seen whole."""
        with self.reads.connection() as c, c.transaction():
            c.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            places = set().union(*_mid_places(c, mids).values())
            return c.execute(*query(places)).fetchall() if places else []

    def message_rows(self, message_id: str) -> list[dict]:
        def query(places):
            spans, sp = _each(places, f"SELECT {_SPAN_SELECT} FROM spans s WHERE {{p}} AND s.mid = %s", [message_id], "s")
            bodies, bp = _each(places, "SELECT mid, id, body FROM bodies WHERE {p} AND mid = %s", [message_id])
            return (f"""SELECT s.*, b.body AS _body FROM ({spans}) s LEFT JOIN ({bodies}) b ON b.mid = s.mid AND b.id = s.body_id
                        ORDER BY s.time_ns, s."row" """, sp + bp)
        out = []
        for r in self._by_message([message_id], query):
            body = r.pop("_body")
            if body is not None:
                attrs = json.loads(r["attributes"])
                attrs[body_table.BODY] = bytes(body).decode("utf-8", "surrogatepass")
                r["attributes"] = body_table.attributes_json(attrs)
            out.append(r)
        return out

    # Pages walk the places (module docstring, "Pages"): a batch of PAGE_PLACES places a statement, each statement
    # its own transaction, so a page holds the locks of one batch at a time however many places the store has.
    PAGE_PLACES = 8

    def _places_from(self, c, from_ns: Optional[int] = None) -> list:
        """The places, newest bucket first, without those whose rows are all older than ``from_ns``."""
        q, p = "SELECT rclass, bucket FROM places", ()
        if from_ns is not None:
            q, p = q + " WHERE bucket >= %s", (self.bucket(from_ns),)
        return [(r["rclass"], r["bucket"]) for r in c.execute(q + " ORDER BY bucket DESC, rclass", p).fetchall()]

    def _top_ns(self, place) -> int:
        """The newest ``last_ns`` a summary row in this place can have. A message is placed by its newest span's
        bucket, or a later one when that bucket is being dropped (``_resolve``), never an earlier one; so are its
        message_queues, message_tenants and tenant_events rows, whose times are no newer than ``last_ns``."""
        return (place[1] + 1) * self.bucket_ns - 1

    def _batches(self, places):
        for k in range(0, len(places), self.PAGE_PLACES):
            yield places[k:k + self.PAGE_PLACES]

    def _walk_first(self, branch: str, params: list, key, limit: int, places_from=None, top=None) -> list:
        """The first ``limit`` rows by ``key`` of ``branch`` (one place's query: ``{p}`` and ``{q}`` name the
        place, it sorts and limits its own rows) across the places, a batch a statement. ``top(row)``: when
        given, rows come newest first, and the walk stops once the page is full and no row of the next batch can
        come before the page's last. A message seen twice (it moved between two statements) counts once."""
        if limit <= 0:
            return []
        page: list = []
        with self._reading() as c:
            for batch in self._batches(self._places_from(c, places_from)):
                if top is not None and len(page) >= limit and self._top_ns(batch[0]) < top(page[-1]):
                    break
                q, qp = _each(batch, branch, params + [limit], "s", q="q")
                rows = c.execute(f"SELECT * FROM ({q}) u", qp).fetchall()
                seen, page = set(), sorted(page + rows, key=key)
                page = [r for r in page if not (r["mid"] in seen or seen.add(r["mid"]))][:limit]
        return page

    def _newest_first(self, src: str, p: list, limit: int, from_ns, to_ns, after) -> list:
        where, p = "", list(p)
        if from_ns is not None:
            where += " AND s.last_ns >= %s"
            p.append(from_ns)
        if to_ns is not None:
            where += " AND s.last_ns < %s"
            p.append(to_ns)
        if after is not None:
            where += " AND (s.last_ns < %s OR (s.last_ns = %s AND s.mid > %s))"
            p += [after[0], after[0], after[1]]
        table, _, rest = src.partition(" WHERE ")
        branch = (f"SELECT s.last_ns, s.mid FROM {table} WHERE {{p}} AND ({rest}){where} "
                  "ORDER BY s.last_ns DESC, s.mid LIMIT %s")
        return self._walk_first(branch, p, lambda r: (-r["last_ns"], r["mid"]), limit, from_ns,
                                top=lambda r: r["last_ns"])

    _ON_QUEUE = (" AND EXISTS (SELECT 1 FROM message_queues q WHERE {q} AND q.queue = %s AND q.last_ns = s.last_ns"
                 " AND q.mid = s.mid)")

    def recent_messages(self, limit: int, queue: Optional[str] = None, tenant: Optional[str] = None, *,
                        from_ns: Optional[int] = None, to_ns: Optional[int] = None,
                        after: Optional[tuple] = None) -> list[str]:
        return [r["mid"] for r in self._newest_first(*self._recent_src(queue, tenant), limit, from_ns, to_ns, after)]

    def _recent_src(self, queue: Optional[str], tenant: Optional[str]) -> tuple[str, list]:
        """The summary table, and its WHERE, that recent_messages and newest_messages walk."""
        if tenant:
            src, p = "message_tenants s WHERE s.tenant = %s", [tenant[:TENANT_MAX_CHARS]]
            if queue:
                src += self._ON_QUEUE
                p.append(queue)
        elif queue:
            src, p = "message_queues s WHERE s.queue = %s", [queue]
        else:
            src, p = "messages s WHERE TRUE", []
        return src, p

    def duplicate_messages(self, limit: int, queue: Optional[str] = None, reason: Optional[str] = None, *,
                           from_ns: Optional[int] = None, to_ns: Optional[int] = None,
                           after: Optional[tuple] = None) -> list[str]:
        src = "messages s WHERE (s.dup_received <> 0 OR s.dup_succeeded <> 0)"
        src += {None: "", "received_more_than_once": " AND s.dup_received <> 0",
                "succeeded_more_than_once": " AND s.dup_succeeded <> 0"}[reason]
        p: list = []
        if queue:
            src += self._ON_QUEUE
            p.append(queue)
        return [r["mid"] for r in self._newest_first(src, p, limit, from_ns, to_ns, after)]

    def stuck_messages(self, cutoff_ns: int, limit: int, queue: Optional[str] = None, *,
                       from_ns: Optional[int] = None, after: Optional[tuple] = None) -> list[StuckCandidate]:
        # Oldest first by stuck_first_ns, which a place's bucket doesn't bound (a message received long ago and
        # kept on its consumer is placed by its newest span; an SQS send time is SQS's clock): every place is read.
        where, p = "", [cutoff_ns]
        if from_ns is not None:
            where += " AND s.stuck_first_ns >= %s"
            p.append(from_ns)
        if after is not None:
            where += " AND (s.stuck_first_ns > %s OR (s.stuck_first_ns = %s AND s.mid > %s))"
            p += [after[0], after[0], after[1]]
        if queue:
            where += self._ON_QUEUE
            p.append(queue)
        rows = self._walk_first(
            f"""SELECT s.mid, s.stuck_first_ns, s.stuck_sqs_clock FROM messages s
                WHERE {{p}} AND s.stuck_open <> 0 AND s.stuck_first_ns IS NOT NULL AND s.stuck_first_ns < %s{where}
                ORDER BY s.stuck_first_ns, s.mid LIMIT %s""", p, lambda r: (r["stuck_first_ns"], r["mid"]), limit)
        return [StuckCandidate(r["mid"], r["stuck_first_ns"], bool(r["stuck_sqs_clock"])) for r in rows]

    def summaries(self, message_ids: list[str]) -> list[dict]:
        if not message_ids:
            return []
        out = {r["mid"]: r for r in self._by_message(message_ids, lambda places: _each(
            places, f"SELECT {', '.join(COLUMNS)} FROM messages WHERE {{p}} AND mid = ANY(%s)", [list(message_ids)]))}
        return [out[m] for m in message_ids if m in out]

    def _walk_all(self, query: str, params=(), from_ns: Optional[int] = None):
        """``query`` (``{p}`` names the place) over the places, a batch a statement: the rows of every batch."""
        with self._reading() as c:
            for batch in self._batches(self._places_from(c, from_ns)):
                yield from c.execute(*_each(batch, query, params)).fetchall()

    def queue_messages(self, queue: str) -> set[str]:
        return {r["mid"] for r in self._walk_all("SELECT mid FROM message_queues WHERE {p} AND queue = %s", [queue])}

    def tenant_activity(self, from_ms: int, to_ms: int) -> list[TenantActivity]:
        # A message's events are all in its place, so a count of distinct messages adds up over places.
        sent: dict = {}
        dead: dict = {}
        waits: dict = {}
        for r in self._walk_all(
                """SELECT tenant, SUM(CASE WHEN kind = 'sent' THEN 1 ELSE 0 END) AS sent,
                          COUNT(DISTINCT CASE WHEN kind = 'dead_lettered' THEN mid END) AS dead_lettered,
                          NULL AS wait_ms
                   FROM tenant_events WHERE {p} AND at_ms BETWEEN %s AND %s GROUP BY tenant
                   UNION ALL
                   SELECT tenant, NULL, NULL, wait_ms FROM tenant_events
                   WHERE {p} AND at_ms BETWEEN %s AND %s AND kind = 'first_delivery'""",
                [from_ms, to_ms, from_ms, to_ms], from_ms * 10 ** 6):
            t = r["tenant"]
            if r["sent"] is not None:
                sent[t] = sent.get(t, 0) + int(r["sent"])
                dead[t] = dead.get(t, 0) + int(r["dead_lettered"])
            else:
                waits.setdefault(t, []).append(r["wait_ms"])
        return [TenantActivity(t, sent[t], dead[t], waits.get(t, [])) for t in sent]

    def find_tenants(self, text: str, limit: int, *, from_ns: Optional[int] = None, to_ns: Optional[int] = None,
                     queue: Optional[str] = None) -> list[str]:
        if not text:
            return []
        lo = ascii_lower(text)
        hi = lo[:-1] + chr(ord(lo[-1]) + 1) if ord(lo[-1]) < 0x10FFFF else None
        below, below_p = (" AND t.tenant_lower < %s", [hi]) if hi else ("", [])
        where, p = "", []
        if from_ns is not None:
            where += " AND t.named_ns >= %s"
            p.append(from_ns)
        if to_ns is not None:
            where += " AND t.named_ns < %s"
            p.append(to_ns)
        if queue:
            where += (" AND EXISTS (SELECT 1 FROM message_queues q WHERE {p} AND q.queue = %s"
                      " AND q.last_ns = t.last_ns AND q.mid = t.mid)")
            p.append(queue)
        # In each place: its tenants from ``lo`` on, one index step each (a recursive skip scan), and the newest
        # time each was named within the filters.
        branch = ("WITH RECURSIVE d AS ((SELECT t.tenant_lower, t.tenant FROM message_tenants t"
                  " WHERE {p} AND (t.tenant_lower, t.tenant) > (%s, '')" + below +
                  " ORDER BY t.tenant_lower, t.tenant LIMIT 1)"
                  " UNION ALL SELECT n.tenant_lower, n.tenant FROM d CROSS JOIN LATERAL ("
                  "SELECT t.tenant_lower, t.tenant FROM message_tenants t"
                  " WHERE {p} AND (t.tenant_lower, t.tenant) > (d.tenant_lower, d.tenant)" + below +
                  " ORDER BY t.tenant_lower, t.tenant LIMIT 1) n WHERE starts_with(n.tenant_lower, %s))"
                  " SELECT d.tenant_lower, d.tenant, (SELECT t.named_ns FROM message_tenants t"
                  " WHERE {p} AND t.tenant_lower = d.tenant_lower AND t.tenant = d.tenant" + where +
                  " ORDER BY t.named_ns DESC LIMIT 1) AS newest FROM d WHERE starts_with(d.tenant_lower, %s)")
        newest: dict = {}
        for r in self._walk_all(branch, [lo] + below_p + below_p + [lo] + p + [lo], from_ns):
            if r["newest"] is not None:
                k = (r["tenant_lower"], r["tenant"])
                newest[k] = max(newest.get(k, r["newest"]), r["newest"])
        return [t for *_, t in sorted((k[0] != lo, -ns, k[1]) for k, ns in newest.items())[:limit]]

    def newest_messages(self, limit: int, queue: Optional[str] = None, tenant: Optional[str] = None, *,
                        from_ns: Optional[int] = None, to_ns: Optional[int] = None,
                        after: Optional[tuple] = None) -> list[tuple[int, str]]:
        return [(r["last_ns"], r["mid"])
                for r in self._newest_first(*self._recent_src(queue, tenant), limit, from_ns, to_ns, after)]

    def body_spans(self, message_ids: list[str]) -> list[dict]:
        if not message_ids:
            return []
        out = []
        def query(places):
            spans, sp = _each(places, """SELECT "row", kind, mid, queue, attributes, body_id FROM spans
                                         WHERE {p} AND mid = ANY(%s) AND body_id IS NOT NULL""", [list(message_ids)])
            bodies, bp = _each(places, "SELECT mid, id, body FROM bodies WHERE {p} AND mid = ANY(%s)", [list(message_ids)])
            return (f"""SELECT s."row", s.kind, s.mid, s.queue, b.body, s.attributes FROM ({spans}) s
                        JOIN ({bodies}) b ON b.mid = s.mid AND b.id = s.body_id""", sp + bp)
        for r in self._by_message(message_ids, query):
            attrs = r.pop("attributes")
            r["body"] = bytes(r["body"]).decode("utf-8", "surrogatepass")
            r["format"] = json.loads(attrs).get("trailbeam.body.format")
            out.append(r)
        return out

    def last_row(self) -> int:
        # rows are numbered in the order they were stored, not by their time, so every place is read
        return max((r["n"] for r in self._walk_all('SELECT MAX("row") AS n FROM spans WHERE {p}') if r["n"]),
                   default=0)

    def counts(self) -> dict:
        kinds: dict = {}
        unjoined = messages = 0
        for r in self._walk_all(
                """SELECT kind, COUNT(*) AS n FROM spans WHERE {p} GROUP BY kind
                   UNION ALL SELECT '', COUNT(*) FROM spans
                   WHERE {p} AND kind IN ('delete', 'change_visibility') AND receive_row IS NULL
                   UNION ALL SELECT NULL, COUNT(*) FROM messages WHERE {p}"""):
            if r["kind"] is None:
                messages += r["n"]
            elif r["kind"] == "":
                unjoined += r["n"]
            else:
                kinds[r["kind"]] = kinds.get(r["kind"], 0) + r["n"]
        return {"spans": kinds, "messages": messages, "unjoined_settles": unjoined}

def _lists(s) -> tuple:
    return (json.dumps(s["queues"]), json.dumps(s["tenants"], sort_keys=True),
            json.dumps(sorted(list(e) for e in s["tenant_events"])))


def _events(s, events: list) -> list:
    return [(at, s["mid"], i, kind, s["tenant"], wait) for i, (at, kind, wait) in enumerate(events)]


def _expired(cutoff: Optional[int], bucket_end: int) -> bool:
    return cutoff is not None and bucket_end <= cutoff


def _latest(cutoffs: Cutoffs) -> int:
    """The latest cutoff of any queue (a bucket ending after it has nothing expired); forever: below every time."""
    return max(-(2 ** 63) if c is None else c for c in (cutoffs.default, *cutoffs.queues.values()))


def is_postgres_url(url: Optional[str]) -> bool:
    return bool(url) and url.startswith(URL_SCHEMES)
