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

**Tenant rollup** (QV-128, rollup.py). What a message adds to the Tenants page per hour and tenant is
kept in ``tenant_rollup``, in the message's place: an export that rewrites a message's summary takes its old
contribution out of the place it was in and adds the new one to the place it is in now, and a sweep takes out
what it deletes; a drop removes the place's rollup with the rest. A tenant's current hour is touched by nearly
every export, so a single row per (place, hour, tenant) would make concurrent exports for one tenant wait
for each other's commit, across replicas too. Each row belongs to one connection instead (``writer``, its
backend PID): only that connection's transactions, one at a time, write it, so no export waits on another's
rollup row. A key has as many rows as connections wrote it (the ingest pools of every replica, and a few more as
connections are replaced); the page adds them up in SQL. Rows built by the version 4 migration have writer 0.

**Locks** (QV-113). A statement on a partitioned table locks, until its transaction ends, every partition (and
its indexes) that it can't rule out when it is planned. So no statement of an export, a by-message read or
retention looks a message ID or a receipt hash up across all places: the lookup tables (``mid_places``,
``hash_places``, pg_schema.py; "Lookups" below) give the places that may hold its rows, and the statement names each of them
(``_each``: one copy a place, the place a literal). Statements that name their place by a parameter
(``rclass = %s AND bucket = %s``) are pruned at plan time only in a custom plan, so every connection sets
``plan_cache_mode = force_custom_plan``: psycopg prepares a statement run often, and Postgres's generic plan of
it would lock every partition. What an export locks is then the partitions of the places its messages and
hashes are in, however many places the store has (85 lock entries for a four-span export joining across two
places, with 10 places or 400).

**Lookups** (QV-127). The lookup tables are partitioned by place, so they no longer grow as one index with all
of history; but every export looks up keys that are new (a send's message ID, a receive's hash), and only
reading every place's partition would show such a key is in none of them. So the places are two kinds. *Hot*:
from ``hot_buckets`` before the current bucket on (and the places the export writes to), read by name in the
export's transaction. *Sealed*: older, their keys in one Bloom filter (lookup_filter.py), read only for the keys
the filter says may be there (about 1 in 15,000 of the others, at 20 bits a key), newest first, a batch a
statement on a read connection: outside the export's transaction, so it holds none of their locks, and safe
because the export holds the advisory lock of every key it looks up, which every writer of that key takes. An
old place not sealed yet (before the next retention pass) is read for every key, the same way. A message's
lookup stops at its *home*, the place of its summary row, whose ``strays_json`` names every other place with
spans of it; pages note the home of each message they list, so its summary is read without a lookup.

Retention's pass seals the places past the hot ones (``maintain``, also when every queue is kept forever): it marks them, so that a lookup row written
to one from then on (a late span) also goes to the late tables, which every lookup reads; adds their keys to the
filter, or builds it anew, larger, once it would hold more than its capacity (which also drops the keys of
places gone); and stores it as the next version. Each replica loads the newest at start and every
``FILTER_CHECK_S``; until it has loaded one, a place sealed in it is read as if it were not sealed.

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
``DDL_LOCK_TIMEOUT_MS``. Places are made by one connection at a time, of any replica, under a session advisory
lock: ATTACHes to one parent wait for each other anyway, and the exports that need a new place at the same
moment queue there for up to ``PLACE_WAIT_MS``, not each with ``DDL_LOCK_TIMEOUT_MS``, and find made what the
others made. Retention's pass creates the places of today and the next ``PRECREATE_AHEAD``
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
from collections import Counter, OrderedDict
from typing import Iterable, Optional

import psycopg
import psycopg_pool
from psycopg import errors, sql
from psycopg.rows import dict_row

from . import body_table, pg_schema, rollup, summary
from .lookup_filter import LookupFilter, hash_key, mid_key
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


def _per_place(jobs, query: str) -> tuple[str, list]:
    """``query`` once per place with that place's own parameters, as one UNION ALL: ``jobs`` is [(place,
    params), ...], ``{p}`` the WHERE term naming the place (a literal, as ``_each``)."""
    parts, params = [], []
    for (c, b), ps in jobs:
        parts.append("(" + query.format(p=f"rclass = {int(c)} AND bucket = {int(b)}") + ")")
        params += list(ps)
    return " UNION ALL ".join(parts), params


class PostgresStore(Store):
    """Thread-safe: an ingest pool and a read pool (``ingest_connections``, ``read_connections``).

    ``retention``: retention.Retention (or anything with ``default`` and ``queues`` in seconds, None forever),
    the retention classes spans are placed by. None: one class; retention then sweeps places whose queues have
    different cutoffs instead of dropping them. ``bucket_ns``: the bucket width G (the granularity).
    ``hot_buckets``: places more than this many buckets before the current one are sealed ("Lookups")."""

    INGEST_ATTEMPTS = 50
    DDL_LOCK_TIMEOUT_MS = 1_000  # the longest a partition's DDL waits for its lock ("New places")
    PLACE_WAIT_MS = 5_000        # the longest to wait while another connection makes places ("New places")
    DDL_ATTEMPTS = 3             # tries an export makes at a missing place's DDL before it is answered busy
    PRECREATE_AHEAD = 2          # buckets after the current one that a retention pass creates
    ANALYZE_AFTER_SPANS = 1_000  # spans this replica writes to a never-analysed place before it analyses it
    cancels_abandoned_exports = True
    WATCH_INTERVAL_S = 0.2  # how often an export's client is checked (``insert(abandoned=...)``)

    def __init__(self, url: str, *, store_bodies: Iterable[str] = (), retention=None, bucket_ns: int = DAY_NS,
                 ingest_connections: int = 4, read_connections: int = 8, statement_timeout_ms: int = 60_000,
                 hot_buckets: int = 2):
        if bucket_ns < 1:
            raise ValueError("bucket_ns must be at least 1")
        self.url = url
        self.store_bodies = frozenset(store_bodies)
        self.retention = retention
        self.bucket_ns = int(bucket_ns)
        self.retention_granularity_ns = self.bucket_ns
        if hot_buckets < 0:
            raise ValueError("hot_buckets must be at least 0")
        self.hot_buckets = int(hot_buckets)
        self._filter: Optional[LookupFilter] = None  # the newest lookup filter loaded ("Lookups")
        self._filter_checked = time.monotonic()
        self._hints: OrderedDict = OrderedDict()  # mid -> its home as a page last listed it ("Lookups")
        self._hints_lock = threading.Lock()
        self._lock_connections = read_connections  # pages: the statements that lock the most places ("Locks")
        self._lock_warned_at: Optional[float] = None
        self.lock_need: Optional[pg_schema.LockNeed] = None
        with psycopg.connect(url, autocommit=True) as c:
            self.migrated = pg_schema.open_schema(c)
            self.server_version = c.info.server_version
            self._check_locks(c)
            # the retention lease's advisory lock key: one per schema, so two Trailbeams in one database don't share it
            schema = c.execute("SELECT current_schema()").fetchone()[0]
            self._lease_key = _key("retention lease:" + schema)
            self._places_key = _key("places:" + schema)  # held while a connection makes places ("New places")
            self._filter_key = _key("lookup filter:" + schema)  # held while a replica seals places ("Lookups")

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
        try:
            with self.reads.connection() as c:
                self._load_filter(c)
        except psycopg.Error as e:  # lookups read every place until a filter is loaded: slower, still exact
            log.warning("could not load the lookup filter: %s", e)
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

    def _ensure_places(self, places) -> None:
        """Create the partitions of these places, outside any export's transaction (DDL locks the parent). Each
        statement waits at most DDL_LOCK_TIMEOUT_MS for its locks, and all of it at most PLACE_WAIT_MS for another
        connection (of this replica or another) that is making places: errors.LockNotAvailable past either, with
        the places made so far kept."""
        self._create_places(places)

    def _create_places(self, places) -> None:
        with psycopg.connect(self.url, autocommit=True) as c:
            # One connection, of any replica, makes places at a time: ATTACH locks the parent against other
            # ATTACHes anyway, and the many that need one new place at once would queue there, each with only
            # DDL_LOCK_TIMEOUT_MS. Here they wait up to PLACE_WAIT_MS, and stop waiting once others have made
            # all their places. Without it, the one that loses a race fails CREATE on the table's row type in
            # pg_type (IF NOT EXISTS doesn't cover a race) or ATTACHes what the other attached, behind the
            # exports writing to it.
            c.execute(f"SET lock_timeout = {int(self.DDL_LOCK_TIMEOUT_MS)}")
            places = sorted(places)
            deadline = time.monotonic() + self.PLACE_WAIT_MS / 1000
            while not c.execute("SELECT pg_try_advisory_lock(%s)", (self._places_key,)).fetchone()[0]:
                if self._places_made(c, places):
                    return
                if time.monotonic() > deadline:
                    raise errors.LockNotAvailable(
                        f"another connection was making places for {self.PLACE_WAIT_MS} ms")
                time.sleep(0.02)
            try:
                for rclass, bucket in places:
                    for table, name, create, attach in pg_schema.place_ddl(rclass, bucket):
                        if self._is_partition(c, table, name):
                            continue
                        try:
                            c.execute(create)
                        except (errors.UniqueViolation, errors.DuplicateTable, errors.DuplicateObject):
                            pass  # a replica without the lock above (an older one) made it at the same moment
                        try:
                            c.execute(attach)
                        except errors.WrongObjectType:
                            pass  # ... or attached it ("is already a partition")
                    c.execute("INSERT INTO places (rclass, bucket) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                              (rclass, bucket))
            finally:
                with contextlib.suppress(psycopg.Error):  # a lost session has released it; keep the error
                    c.execute("SELECT pg_advisory_unlock(%s)", (self._places_key,))
            self._check_locks(c, throttle_s=self.LOCK_WARNING_INTERVAL_S)

    @staticmethod
    def _places_made(c, places: list) -> bool:
        """Whether every one of these places is in ``places``: its row is written once all its partitions are."""
        return c.execute(
            """SELECT count(*) FROM places p JOIN unnest(%s::bigint[], %s::bigint[]) AS w(rclass, bucket)
                 ON (p.rclass, p.bucket) = (w.rclass, w.bucket)""",
            ([r for r, _ in places], [b for _, b in places])).fetchone()[0] == len(places)

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

    def _lock_places(self, conn, places) -> dict:
        """Lock (FOR SHARE) and read the state of these places: {place: state}; a missing place is absent.
        Retention takes the rows FOR UPDATE to mark a place ``dropping``, and sealing to mark it sealed, so they
        wait for this transaction. An export notes the places, which its lookups then read ("Lookups")."""
        places = sorted(set(places))
        if not places:
            return {}
        locked = getattr(self._tx, "locked", None)
        if locked is not None:
            locked.update(places)
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

    # ---- lookups (QV-127) ---------------------------------------------------------------------

    def _hot_from(self) -> int:
        """The oldest bucket that is not sealed: places from it on are read for every key ("Lookups")."""
        return self.bucket(time.time_ns()) - self.hot_buckets

    def _near(self) -> set:
        """The places this export has locked so far: those it writes rows to, so its lookups read them in its
        own transaction, which sees its own rows."""
        return set(getattr(self._tx, "locked", None) or ())

    def _split(self, conn, near) -> tuple[set, list]:
        """(near, far): the places a lookup reads on ``conn`` (the given ones, and every place from
        ``_hot_from`` on), and the others, newest first, each with whether the loaded filter covers it."""
        hot, f = self._hot_from(), self._filter
        v = f.version if f is not None else 0
        near, far = set(near), []
        exporting = getattr(self._tx, "locked", None) is not None
        rows = getattr(self._tx, "places", None) if exporting else None  # in an export: read once an attempt
        if rows is None:
            rows = conn.execute("SELECT rclass, bucket, sealed_in FROM places").fetchall()
            if exporting:
                self._tx.places = rows
        for r in rows:
            p = (r["rclass"], r["bucket"])
            if p[1] >= hot:
                near.add(p)
            elif p not in near:
                far.append((p, r["sealed_in"] is not None and 0 < r["sealed_in"] <= v))
        far.sort(key=lambda x: (-x[0][1], x[0][0]))
        return near, far

    @contextlib.contextmanager
    def _walker(self, conn, walk):
        """The connection a lookup reads the far places on: ``walk()``'s, or ``conn`` when walk is None."""
        if walk is None:
            yield conn
        else:
            with walk() as c:
                yield c

    @staticmethod
    def _homes(c, hits: dict) -> dict:
        """{mid: places} for the messages of ``hits`` ({mid: places}) whose summary row is in one of those
        places: that place, its *home*, and the places of its ``strays_json``, which are every other place that
        holds spans of it ("Placement"; its summary rows and bodies are all at home). So once a message's home
        is found, its other places need no lookup."""
        where = set().union(*hits.values()) if hits else set()
        if not where:
            return {}
        out = {}
        for r in c.execute(*_each(where, "SELECT mid, rclass, bucket, strays_json FROM messages "
                                         "WHERE {p} AND mid = ANY(%s)", [sorted(hits)])).fetchall():
            home = (r["rclass"], r["bucket"])
            if home in hits.get(r["mid"], ()):
                out[r["mid"]] = {home} | {tuple(x) for x in json.loads(r["strays_json"])}
        return out

    def _locate_mids(self, conn, mids, near=(), walk=None, homed: Optional[set] = None) -> dict:
        """{mid: {place, ...}}: every place holding rows of these messages (with, maybe, places that no longer
        do). ``conn``: the transaction it reads in, with the places ``near`` and the recent ones; ``walk``: a
        function giving the connection it reads the older places on, one batch a statement, so that their locks
        aren't held by ``conn``'s transaction (``conn`` itself when None). A message is looked for in a sealed
        place only when the filter may hold it, and in no more places once its home is found. ``homed``, when
        given, gets the messages whose home was found."""
        mids = sorted({m for m in mids if m is not None})
        homed = set() if homed is None else homed
        if not mids:
            return {}
        near, far = self._split(conn, near)
        # the near places with their summary rows (one statement: a home found here needs no more), and the late rows
        hits: dict = {}
        homes: dict = {}
        q, qp = ("SELECT mid, rclass, bucket, NULL AS strays_json FROM mid_places_late l WHERE mid = ANY(%s)", [mids])
        if near:
            nq, nqp = _each(near, "SELECT l.mid, l.rclass, l.bucket, m.strays_json FROM mid_places l "
                                  "LEFT JOIN messages m ON {m} AND m.mid = l.mid WHERE {p} AND l.mid = ANY(%s)",
                            [mids], "l", m="m")
            q, qp = f"{nq} UNION ALL ({q})", nqp + qp
        for r in conn.execute(q, qp).fetchall():
            p = (r["rclass"], r["bucket"])
            hits.setdefault(r["mid"], set()).add(p)
            if r["strays_json"] is not None:
                homes[r["mid"]] = {p} | {tuple(x) for x in json.loads(r["strays_json"])}
        late = {m: ps - near for m, ps in hits.items() if m not in homes and ps - near}
        homes.update(self._homes(conn, late))  # a home among the late rows' places (rare: sealed places)
        out = {m: set(ps) for m, ps in hits.items()}
        for m, ps in homes.items():
            out[m] |= ps
        homed.update(homes)
        left = set(mids) - set(homes)
        if not left or not far:
            return out
        f = self._filter
        maybe = {m for m in left if f is not None and mid_key(m) in f}
        if all(sealed for _, sealed in far) and not maybe:
            return out
        with self._walker(conn, walk) as c:
            for k in range(0, len(far), self.PAGE_PLACES):
                jobs = [(p, [sorted(left & maybe if sealed else left)]) for p, sealed in far[k:k + self.PAGE_PLACES]]
                jobs = [j for j in jobs if j[1][0]]
                if not jobs:
                    continue
                found: dict = {}
                for r in c.execute(*_per_place(jobs, "SELECT mid, rclass, bucket FROM mid_places "
                                                     "WHERE {p} AND mid = ANY(%s)")).fetchall():
                    found.setdefault(r["mid"], set()).add((r["rclass"], r["bucket"]))
                for m, ps in found.items():
                    out.setdefault(m, set()).update(ps)
                for m, ps in self._homes(c, found).items():
                    out[m] |= ps
                    left.discard(m)
                    homed.add(m)
                if not left:
                    break
        return out

    def _locate_hashes(self, conn, hashes, near=(), walk=None) -> set:
        """The places holding receive or settle spans of these receipt hashes ((receipt_hmac, key_id)), as
        ``_locate_mids`` finds them, but read in every far place (unsealed) or every sealed one whose filter may
        hold the hash: a hash has no summary row saying where the rest is."""
        hashes = sorted(set(hashes))
        if not hashes:
            return set()
        near, far = self._split(conn, near)
        a, b = [h[0] for h in hashes], [h[1] for h in hashes]
        sub = "(receipt_hmac, key_id) IN (SELECT * FROM unnest(%s::text[], %s::text[]))"
        q, qp = f"SELECT rclass, bucket FROM hash_places_late WHERE {sub}", [a, b]
        if near:
            nq, nqp = _each(near, "SELECT rclass, bucket FROM hash_places WHERE {p} AND " + sub, [a, b])
            q, qp = f"{nq} UNION ALL ({q})", nqp + qp
        places = {(r["rclass"], r["bucket"]) for r in conn.execute(q, qp).fetchall()}
        f = self._filter
        maybe = [h for h in hashes if f is not None and hash_key(*h) in f]
        jobs = [(p, maybe if sealed else hashes) for p, sealed in far]
        jobs = [(p, [[h[0] for h in hs], [h[1] for h in hs]]) for p, hs in jobs if hs]
        if jobs:
            with self._walker(conn, walk) as c:
                for k in range(0, len(jobs), self.PAGE_PLACES):
                    places |= {(r["rclass"], r["bucket"]) for r in c.execute(*_per_place(
                        jobs[k:k + self.PAGE_PLACES], "SELECT rclass, bucket FROM hash_places WHERE {p} AND " + sub
                    )).fetchall()}
        return places

    def _note_mids(self, conn, rows) -> None:
        """mid_places rows for these (mid, rclass, bucket); a new one for a sealed place goes to
        mid_places_late too. Each place's row in ``places`` is locked FOR SHARE by the caller, so its
        ``sealed_in`` doesn't change before the commit (sealing waits)."""
        rows = sorted({r for r in rows if r[0] is not None})
        if rows:
            conn.execute("""WITH new AS (INSERT INTO mid_places (mid, rclass, bucket)
                                SELECT * FROM unnest(%s::text[], %s::bigint[], %s::bigint[]) ON CONFLICT DO NOTHING
                                RETURNING mid, rclass, bucket)
                            INSERT INTO mid_places_late (mid, rclass, bucket)
                            SELECT n.mid, n.rclass, n.bucket FROM new n
                            JOIN places p ON p.rclass = n.rclass AND p.bucket = n.bucket AND p.sealed_in IS NOT NULL
                            ON CONFLICT DO NOTHING""", [[r[k] for r in rows] for k in range(3)])

    def _note_hashes(self, conn, rows) -> None:
        """hash_places rows for these (receipt_hmac, key_id, rclass, bucket), as ``_note_mids``."""
        rows = sorted({r for r in rows if r[0] is not None and r[1] is not None})
        if rows:
            conn.execute("""WITH new AS (INSERT INTO hash_places (receipt_hmac, key_id, rclass, bucket)
                                SELECT * FROM unnest(%s::text[], %s::text[], %s::bigint[], %s::bigint[])
                                ON CONFLICT DO NOTHING RETURNING receipt_hmac, key_id, rclass, bucket)
                            INSERT INTO hash_places_late (receipt_hmac, key_id, rclass, bucket)
                            SELECT n.receipt_hmac, n.key_id, n.rclass, n.bucket FROM new n
                            JOIN places p ON p.rclass = n.rclass AND p.bucket = n.bucket AND p.sealed_in IS NOT NULL
                            ON CONFLICT DO NOTHING""", [[r[k] for r in rows] for k in range(4)])

    # Sealing: the replica holding the retention lease, at the start of each pass (maintain).

    FILTER_CHUNK_BYTES = 4 << 20  # bytes of the filter per lookup_filter row
    FILTER_CHECK_S = 30.0         # how often a replica looks for a newer filter than it has

    def _seal_due(self) -> int:
        """Seal the places older than ``hot_buckets`` ("Lookups"): mark them (a lookup row written to them from
        then on goes to the late tables too), add their lookup keys to the filter, or build it anew, twice the
        size of what it must hold, when they would fill it (which also drops the keys of places gone), and store
        it as the next version, which marks them sealed in it. Returns the places sealed."""
        hot = self._hot_from()
        with self.ingest.connection() as c:
            due = [(r["rclass"], r["bucket"]) for r in c.execute(
                """SELECT rclass, bucket FROM places WHERE bucket < %s AND state = 'live'
                   AND (sealed_in IS NULL OR sealed_in = 0) ORDER BY bucket, rclass""", (hot,)).fetchall()]
            if not due:
                return 0
            if not c.execute("SELECT pg_try_advisory_lock(%s) AS ok", (self._filter_key,)).fetchone()["ok"]:
                return 0  # another replica is sealing (one that held the lease a moment ago)
            try:
                t0 = time.monotonic()
                cols = [[p[0] for p in due], [p[1] for p in due]]
                with c.transaction():  # waits for the exports writing to them (they hold their rows FOR SHARE)
                    c.execute("""SELECT 1 FROM places WHERE (rclass, bucket) IN (SELECT * FROM unnest(%s::bigint[],
                                 %s::bigint[])) ORDER BY rclass, bucket FOR UPDATE""", cols)
                    c.execute("""UPDATE places SET sealed_in = 0 WHERE sealed_in IS NULL
                                 AND (rclass, bucket) IN (SELECT * FROM unnest(%s::bigint[], %s::bigint[]))""", cols)
                stored = self._load_filter(c)
                counts = {p: self._lookup_rows(c, p) for p in due}
                new = sum(counts.values())
                if stored is None or stored.full(new):
                    sealed = [(r["rclass"], r["bucket"]) for r in c.execute(
                        "SELECT rclass, bucket FROM places WHERE sealed_in > 0 ORDER BY bucket, rclass").fetchall()]
                    held = c.execute("SELECT COALESCE(SUM(lookup_keys), 0) AS n FROM places WHERE sealed_in > 0"
                                     ).fetchone()["n"]
                    f, sources = LookupFilter.sized(2 * (held + new)), sealed + due
                else:  # a copy: lookups on this replica go on with the stored one meanwhile
                    f = LookupFilter(stored.m, stored.k, capacity=stored.capacity, keys=stored.keys, bits=stored.bits)
                    sources = due
                for p in sources:
                    self._add_keys(c, f, p)
                self._store_filter(c, f, due, counts)
                log.info("sealed %d place(s) into lookup filter version %d (%d keys of %d, %.1f MB) in %.1f s",
                         len(due), f.version, f.keys, f.capacity, len(f.bits) / 1e6, time.monotonic() - t0)
            finally:
                with contextlib.suppress(psycopg.Error):
                    c.execute("SELECT pg_advisory_unlock(%s)", (self._filter_key,))
        return len(due)

    @staticmethod
    def _lookup_rows(c, place) -> int:
        return c.execute("SELECT (SELECT COUNT(*) FROM mid_places WHERE rclass = %s AND bucket = %s) + "
                         "(SELECT COUNT(*) FROM hash_places WHERE rclass = %s AND bucket = %s) AS n",
                         place + place).fetchone()["n"]

    @staticmethod
    def _add_keys(c, f: LookupFilter, place) -> None:
        with c.transaction():
            with c.cursor(name="trailbeam_seal_mids") as cur:
                cur.itersize = 20_000
                cur.execute("SELECT mid FROM mid_places WHERE rclass = %s AND bucket = %s", place)
                for r in cur:
                    f.add(mid_key(r["mid"]))
            with c.cursor(name="trailbeam_seal_hashes") as cur:
                cur.itersize = 20_000
                cur.execute("SELECT receipt_hmac, key_id FROM hash_places WHERE rclass = %s AND bucket = %s", place)
                for r in cur:
                    f.add(hash_key(r["receipt_hmac"], r["key_id"]))

    def _store_filter(self, c, f: LookupFilter, due, counts) -> None:
        """Store ``f`` as the next version, mark ``due`` sealed in it, and remove the older versions."""
        n = self.FILTER_CHUNK_BYTES
        with c.transaction():
            v = c.execute("SELECT COALESCE(MAX(version), 0) + 1 AS v FROM lookup_filters").fetchone()["v"]
            c.execute("""INSERT INTO lookup_filters (version, m, k, capacity, keys, made_at_ms)
                         VALUES (%s, %s, %s, %s, %s, %s)""", (v, f.m, f.k, f.capacity, f.keys, time.time_ns() // 10 ** 6))
            c.cursor().executemany("INSERT INTO lookup_filter (version, chunk, bits) VALUES (%s, %s, %s)",
                                   [(v, i, bytes(f.bits[o:o + n])) for i, o in enumerate(range(0, len(f.bits), n))])
            c.cursor().executemany(
                "UPDATE places SET sealed_in = %s, lookup_keys = %s WHERE rclass = %s AND bucket = %s AND sealed_in = 0",
                [(v, counts[p], p[0], p[1]) for p in due])
            c.execute("DELETE FROM lookup_filter WHERE version < %s", (v,))
            c.execute("DELETE FROM lookup_filters WHERE version < %s", (v,))
        f.version = v
        self._filter = f

    def _load_filter(self, c) -> Optional[LookupFilter]:
        """The newest stored filter, loaded unless this replica has it already (None: none stored yet)."""
        f = self._filter
        newest = c.execute("SELECT MAX(version) AS v FROM lookup_filters").fetchone()["v"]
        if newest is None or (f is not None and f.version >= newest):
            return f if newest is not None else None
        with c.transaction():
            c.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            meta = c.execute("SELECT version, m, k, capacity, keys FROM lookup_filters ORDER BY version DESC LIMIT 1"
                             ).fetchone()
            chunks = c.execute("SELECT bits FROM lookup_filter WHERE version = %s ORDER BY chunk",
                               (meta["version"],)).fetchall()
        f = LookupFilter(meta["m"], meta["k"], capacity=meta["capacity"], keys=meta["keys"], version=meta["version"],
                         bits=b"".join(bytes(r["bits"]) for r in chunks))
        self._filter = f
        return f

    def _check_filter(self) -> None:
        """Load a newer filter if another replica stored one (in the watcher thread, every FILTER_CHECK_S)."""
        try:
            with self.reads.connection(timeout=5) as c:
                self._load_filter(c)
        except (psycopg.Error, psycopg_pool.PoolTimeout) as e:
            log.info("could not check for a newer lookup filter: %s", e)

    # ---- writes ---------------------------------------------------------------------------

    def _watch_loop(self) -> None:
        """Cancel the database work of exports whose client has gone (rule 5 of QV-69): the client will retry
        the export, so finishing it only adds load. One thread for every export."""
        while not self._stop.wait(self.WATCH_INTERVAL_S):
            if time.monotonic() - self._filter_checked > self.FILTER_CHECK_S:
                self._filter_checked = time.monotonic()
                self._check_filter()
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
        try:
            return self._insert_attempts(records, abandoned, wait_s, timings)
        finally:  # this thread's next read is not in an export ("Lookups": _split, _near)
            self._tx.locked = self._tx.places = None

    def _insert_attempts(self, records, abandoned, wait_s=None, timings=None) -> tuple[int, int]:
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
                        self._tx.noted, self._tx.written, self._tx.locked = [], Counter(), set()
                        self._tx.places = None
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
            known = set().union(*self._locate_mids(conn, {k[0] for k in ks}, self._near(),
                                                   self.reads.connection).values())
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
                self._note_mids(conn, [(r[3], r[0], r[1]) for r in rows])

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
        self._note_mids(conn, [(prepared[i][1], places[i][0], places[i][1]) for i, _ in order])
        hashed = sorted({(prepared[i][0].get("receipt_hmac"), prepared[i][0].get("key_id")) + places[i] for i, _ in order
                         if (prepared[i][0]["kind"] == "receive" or prepared[i][0]["kind"] in SETTLE_KINDS)
                         and prepared[i][0].get("receipt_hmac") is not None and prepared[i][0].get("key_id") is not None})
        self._note_hashes(conn, hashed)

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
        self._note_mids(conn, [(u[3], u[0], u[1]) for u in updates])
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
                self._ensure_places(places)
                return
            except errors.LockNotAvailable:
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
            known = self._locate_hashes(conn, hashes, self._near(), self.reads.connection)
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
        known = self._locate_mids(conn, mids, self._near(), self.reads.connection)  # with this export's own rows
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
        deltas = rollup.Deltas()
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
            deltas.change(None if old is None else (old["rclass"], old["bucket"]),
                          None if old is None else rollup.of_summary(old), (pc, pb), rollup.of_summary(new))
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
        _apply_rollup(conn, deltas)
        self._note_mids(conn, [(m, place[m][0], place[m][1]) for m in mids if m in news])
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

    maintains = True

    def maintain(self) -> None:
        """Store.maintain, at the start of every retention pass, also when every queue is kept forever: on the
        replica holding the retention lease, create the next places ("New places") and seal the places past the
        hot ones ("Lookups"). Sealing is what keeps an export from reading every old place, so it must not wait
        for something to expire."""
        if not self.holds_retention_lease():
            return
        self._precreate()
        try:
            self._seal_due()
        except psycopg.Error as e:  # lookups stay exact unsealed, only slower: the next pass tries again
            log.warning("retention: could not seal the older places' lookups: %s", e)

    def purge_step(self, cutoffs: Cutoffs, resume: Optional[str] = None, limit: int = 100) -> PurgeStep:
        """Store.purge_step, by place (module docstring). ``resume``: ``<bucket>:<rclass>`` of the next place to
        look at, and for a sweep in progress ``:<last_ns>:<mid>`` of the last message it looked at. Only on the
        replica holding the retention lease (holds_retention_lease); on another, a step does nothing and ends the
        pass."""
        if not self.holds_retention_lease():
            return PurgeStep(0, 0, None)
        if resume is None:
            start, sweep_after = (-(2 ** 63), -(2 ** 63)), None
        else:
            parts = resume.split(":", 3)
            start = (int(parts[0]), int(parts[1]))
            sweep_after = (int(parts[2]), parts[3]) if len(parts) == 4 else None
        # Choose the step on a read connection, then give it back before the step: a drop's and a sweep's
        # lookups may need one of their own ("Lookups").
        step = None
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
                    step = ("drop", pc, pb, expired)
                    break
                swept = set(json.loads(p["swept"])) if p["swept"] else set()
                if expired and (sweep_after is not None or not expired <= swept):
                    step = ("sweep", pc, pb, expired)
                    break
                sweep_after = None
        if step is None:
            if len(places) == 1000:
                last = places[-1]
                return PurgeStep(0, 0, f"{last['bucket']}:{last['rclass'] + 1}")
            return PurgeStep(0, 0, None)
        kind, pc, pb, expired = step
        if kind == "drop":
            n, examined, w = self._drop(pc, pb, expired)
            return PurgeStep(n, examined, f"{pb}:{pc + 1}", w)
        n, examined, done_key, w = self._sweep(pc, pb, expired, sweep_after, limit)
        if done_key is None:
            with self.ingest.connection() as w_conn:
                w_conn.execute("UPDATE places SET swept = %s WHERE rclass = %s AND bucket = %s",
                               (json.dumps(sorted(expired)), pc, pb))
            return PurgeStep(n, examined, f"{pb}:{pc + 1}", w)
        return PurgeStep(n, examined, f"{pb}:{pc}:{done_key[0]}:{done_key[1]}", w)

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
                homes = set().union(*self._locate_mids(c, strays, {(pc, pb)}, self.reads.connection).values())
                homes, found = homes - {(pc, pb)}, []
                if homes:
                    found = c.execute(*_each(homes, "SELECT mid, rclass, bucket, strays_json FROM messages "
                                                    "WHERE {p} AND mid = ANY(%s)", [strays])).fetchall()
                self._lock_places(c, {(x["rclass"], x["bucket"]) for x in found})  # their sealed_in holds still
                for x in found:
                    self._note_hashes(c, [(h["receipt_hmac"], h["key_id"], x["rclass"], x["bucket"]) for h in c.execute(
                        """SELECT DISTINCT receipt_hmac, key_id FROM spans WHERE rclass = %s AND bucket = %s AND mid = %s
                             AND receipt_hmac IS NOT NULL AND key_id IS NOT NULL
                             AND kind IN ('receive', 'delete', 'change_visibility')""", (pc, pb, x["mid"])).fetchall()])
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
            # 3. the partitions themselves, its lookups' with them
            for t in pg_schema.PARTITIONED:
                self._detach_and_drop(c, t, pc, pb)
            with c.transaction():
                c.execute("DELETE FROM mid_places_late WHERE rclass = %s AND bucket = %s", (pc, pb))
                c.execute("DELETE FROM hash_places_late WHERE rclass = %s AND bucket = %s", (pc, pb))
                c.execute("DELETE FROM strays WHERE rclass = %s AND bucket = %s", (pc, pb))
                c.execute("DELETE FROM place_queues WHERE rclass = %s AND bucket = %s", (pc, pb))
                c.execute("DELETE FROM places WHERE rclass = %s AND bucket = %s", (pc, pb))
        with self._cache_lock:
            self._known_queues = {k for k in self._known_queues if (k[0], k[1]) != (pc, pb)}
            self._analysed.discard((pc, pb))
            self._written.pop((pc, pb), None)
        return n, examined, (time.monotonic() - t0) * 1000

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
                    f"""SELECT mid, last_ns, queues_json, strays_json, tenant, events_json
                        FROM messages WHERE rclass = %s AND bucket = %s{where}
                        ORDER BY last_ns, mid LIMIT %s""", p + [limit]).fetchall()
                gone = [m for m in msgs if set(json.loads(m["queues_json"])) <= expired]
                n = 0
                if gone:
                    ids = [m["mid"] for m in gone]
                    other = {tuple(x) for m in gone for x in json.loads(m["strays_json"])} - {(pc, pb)}
                    self._require_live(c, other)
                    for kc, kb in sorted(set().union(*self._locate_mids(c, ids, {(pc, pb)},
                                                                        self.reads.connection).values())):
                        n += c.execute("DELETE FROM spans WHERE rclass = %s AND bucket = %s AND mid = ANY(%s)",
                                       (kc, kb, ids)).rowcount
                    for t in ("messages", "message_queues", "message_tenants", "tenant_events", "bodies"):
                        c.execute(f"DELETE FROM {t} WHERE rclass = %s AND bucket = %s AND mid = ANY(%s)", (pc, pb, ids))
                    deltas = rollup.Deltas()
                    for m in gone:
                        deltas.add((pc, pb), rollup.of_summary(m), -1)
                    _apply_rollup(c, deltas)
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

    BY_MESSAGE_ATTEMPTS = 3

    def _by_message(self, mids, query) -> list:
        """``query(places)``: (SQL, parameters) reading rows of ``mids`` in these places (``_each``). The lookup
        runs first, each statement in a transaction of its own (so an old message's walk holds no more locks than
        a page does), then the query in one snapshot, which checks that the places still hold every message it
        found whole: a message's home and strays (``_homes``), and no home lost. One that moved meanwhile (a span
        came) is looked up again."""
        mids = sorted(set(mids))
        rows: list = []
        with self._reading() as c:
            for attempt in range(self.BY_MESSAGE_ATTEMPTS):
                homed: set = set()
                located = self._locate_mids(c, mids, self._hinted(mids), None, homed)
                places = set().union(*located.values()) if located else set()
                if not places:
                    return []
                with c.transaction():
                    c.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
                    rows = c.execute(*query(places)).fetchall()
                    now = c.execute(*_each(places, "SELECT mid, rclass, bucket, strays_json FROM messages "
                                                   "WHERE {p} AND mid = ANY(%s)", [mids])).fetchall()
                whole = {r["mid"] for r in now if {tuple(x) for x in json.loads(r["strays_json"])} <= places}
                if homed <= whole and len(whole) == len({r["mid"] for r in now}):
                    return rows
        return rows

    HINTS = 100_000  # message homes remembered from the pages listed

    def _remember(self, rows) -> None:
        """Note the home of each message a page listed (its list rows are at its home), so that the page's
        summaries find them without a walk (``_locate_mids``: a hint is a near place, checked like any other)."""
        with self._hints_lock:
            for r in rows:
                self._hints[r["mid"]] = (r["rclass"], r["bucket"])
                self._hints.move_to_end(r["mid"])
            while len(self._hints) > self.HINTS:
                self._hints.popitem(last=False)

    def _hinted(self, mids) -> set:
        with self._hints_lock:
            return {self._hints[m] for m in mids if m in self._hints}

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
        self._remember(page)
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
        branch = (f"SELECT s.last_ns, s.mid, s.rclass, s.bucket FROM {table} WHERE {{p}} AND ({rest}){where} "
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
            f"""SELECT s.mid, s.stuck_first_ns, s.stuck_sqs_clock, s.rclass, s.bucket FROM messages s
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
        """The rollup for the whole hours in the range, the tenant events of the partial hours at its ends, and the
        repeat dead-letter moves (rollup.py), a batch of places a statement."""
        hours, raw = rollup.split(from_ms, to_ms)
        rows, events, again = [], [], []
        if hours:
            # counts added up in SQL; the sketches here, faster than jsonb_each and a GROUP BY (QV-128)
            totals: dict = {}
            waits: dict = {}
            for r in self._walk_all(
                    """SELECT tenant, SUM(sent) AS sent, SUM(first_deliveries) AS firsts,
                              SUM(dead_letter_moves) AS moves, NULL::jsonb AS waits
                       FROM tenant_rollup WHERE {p} AND hour BETWEEN %s AND %s GROUP BY tenant
                       UNION ALL
                       SELECT tenant, NULL, NULL, NULL, waits FROM tenant_rollup WHERE {p} AND hour BETWEEN %s AND %s""",
                    list(hours) * 2, from_ms * 10 ** 6):
                t = r["tenant"]
                if r["waits"] is None:
                    c = totals.setdefault(t, [0, 0, 0])
                    c[0] += int(r["sent"])
                    c[1] += int(r["firsts"])
                    c[2] += int(r["moves"])
                else:
                    into = waits.setdefault(t, {})
                    for k, n in r["waits"].items():
                        into[k] = into.get(k, 0) + n
            rows = [(t, c[0], c[1], c[2], waits.get(t, {})) for t, c in totals.items()]
        for a, b in raw:
            events += [(r["tenant"], r["kind"], r["wait_ms"]) for r in self._walk_all(
                "SELECT tenant, kind, wait_ms FROM tenant_events WHERE {p} AND at_ms BETWEEN %s AND %s", [a, b],
                a * 10 ** 6)]
        again = [r["tenant"] for r in self._walk_all(
            """SELECT tenant FROM tenant_events WHERE {p} AND kind = 'dead_lettered_again'
               AND at_ms BETWEEN %s AND %s AND at_ms - wait_ms >= %s""", [from_ms, to_ms, from_ms], from_ms * 10 ** 6)]
        return rollup.tenant_page(rows, events, again)

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

def _apply_rollup(conn, deltas: "rollup.Deltas") -> None:
    """Add the deltas to tenant_rollup ("Tenant rollup"): to this connection's own rows, ``writer`` its backend's
    PID, which no other transaction writes, so concurrent exports touching one tenant's hour never wait for each
    other. Rows are read, added to in Python and written back (or deleted, when they come to nothing), one
    statement of each a place."""
    items = deltas.items()
    if not items:
        return
    me = conn.info.backend_pid
    by_place: dict = {}
    for (place, h, t), d in items:
        by_place.setdefault(place, []).append((h, t, d))
    cur = conn.cursor()
    for (pc, pb), keys in sorted(by_place.items()):
        have = {(r["hour"], r["tenant"]): r for r in conn.execute(
            """SELECT hour, tenant, sent, first_deliveries, dead_letter_moves, waits FROM tenant_rollup
               WHERE rclass = %s AND bucket = %s AND writer = %s
                 AND (hour, tenant) IN (SELECT * FROM unnest(%s::bigint[], %s::text[]))""",
            (pc, pb, me, [k[0] for k in keys], [k[1] for k in keys])).fetchall()}
        put, gone = [], []
        for h, t, d in keys:
            r = have.get((h, t))
            a = rollup.Agg(r["sent"], r["first_deliveries"], r["dead_letter_moves"], rollup.Sketch(
                r["waits"])) if r else rollup.Agg()
            a.merge(d)
            if a:
                put.append((pc, pb, h, t, me) + a.row())
            elif r:
                gone.append((pc, pb, h, t, me))
        if put:
            cur.executemany(
                """INSERT INTO tenant_rollup (rclass, bucket, hour, tenant, writer, sent, first_deliveries,
                   dead_letter_moves, waits) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                   ON CONFLICT (rclass, bucket, hour, tenant, writer) DO UPDATE SET sent = EXCLUDED.sent,
                   first_deliveries = EXCLUDED.first_deliveries, dead_letter_moves = EXCLUDED.dead_letter_moves,
                   waits = EXCLUDED.waits""", put)
        if gone:
            cur.executemany("""DELETE FROM tenant_rollup WHERE rclass = %s AND bucket = %s AND hour = %s
                               AND tenant = %s AND writer = %s""", gone)


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
