"""The storage interface: what the server needs from wherever spans are kept.

The server reaches storage only through a ``Store``. Two backends implement it today: SQLite (db.py,
``SqliteStore``), the default, and Postgres (postgres.py, ``PostgresStore``), selected by
``TRAILBEAM_STORE_URL=postgres://…`` (``--store-url``). Every backend must pass the conformance suite in
``python/tests/conformance`` (the same answers, from the same spans, as SQLite); Postgres runs in it when
``TRAILBEAM_TEST_POSTGRES_URL`` is set.

What a Store does, and what stays out of it: a Store keeps span rows, joins settle spans to their receives,
keeps one summary per message (summary.summarize of its rows, rewritten whenever a span of it is stored,
joined or deleted), and answers the questions below with message IDs, rows and summaries. It doesn't build
timelines, list items, the Tenants page or Body tabs: queries.py, summary.py, timeline.py and bodies.py do
that in Python, so those rules exist once whatever the backend.

**Rows.** A row is a mapping from column name to value with every column of the SQLite ``spans`` table
(migrations.CREATE): the record fields of otlp.decode, ``attributes`` (the span's attributes as JSON text,
sorted keys, bodies removed per ``store_bodies``), and these, which the Store sets:

- ``row``: an integer ID, unique in the store, increasing in the order spans were stored. The timeline uses it
  to break ties between equal times, and settle spans point at their receive with it.
- ``mid``: the message the row belongs to. Send, receive and outcome spans: their own message ID. Settle
  spans (delete, change_visibility): the message ID of the receive with the same receipt-handle hash and key
  ID, whichever of the two arrived first (``mid_source = 'receipt_hash'``, ``receive_row`` = that receive's
  ``row``); until that receive arrives, the settle span's own message ID if it has one (``mid_source =
  'span'``), else NULL. If several receives share a hash, the latest at or before the settle span, else the
  earliest after it.
- ``body_dropped``: ``'not_opted_in'`` when the span carried a body for a queue not in ``store_bodies``.

**Messages.** Only rows with a ``mid`` belong to a message. A message's time is its spans' ``time_ns``.

**Purges** (contract section 18, revision 3.3). A ``purge`` record (a ``PurgeQueue`` call) belongs to no message:
SQS does not say which messages it deleted. A Store keeps it with its queue and times (``purges``) and drops it by
its own time against its queue's cutoff, like an unjoined settle span; what it means for a message is purge.py.
"""
from __future__ import annotations

import abc
from typing import Any, Callable, Iterable, Mapping, NamedTuple, Optional

# Why a store without probes rejects them (contract section 17: the reason is "probe span not stored: " this).
NO_PROBES = "this store does not keep probes"

Row = Mapping[str, Any]
Summary = Mapping[str, Any]  # summary.summarize, as stored

SETTLE_KINDS = ("delete", "change_visibility")

# Tenants (contract/tenants.md): a span's tenant is its trailbeam.tenant attribute when that is a non-empty
# string, cut to TENANT_MAX_CHARS, on send, receive and outcome spans only.
TENANT_ATTR = "trailbeam.tenant"
TENANT_KINDS = ("send", "receive", "outcome")
TENANT_MAX_CHARS = 256

DUPLICATE_REASONS = ("received_more_than_once", "succeeded_more_than_once")


class ExportAbandoned(Exception):
    """Store.insert stopped because the client that sent the export went away (``abandoned``): nothing stored."""


class StoreUnavailable(Exception):
    """The store can't be reached (its database is down, or no connection came in time): nothing was stored. The
    server answers 503 with Retry-After, so the exporter (or a load balancer, on another replica) tries again."""


class StoreFull(StoreUnavailable):
    """The store's disk is full: nothing was stored. The server answers 503 (``store_full``) with Retry-After, so the
    exporter sends the export again, and it is stored once space is freed. Not a 500, which exporters drop."""


class WriterBusy(Exception):
    """``Store.insert`` gave up waiting to write within ``wait_s``; nothing was written (the server answers 503)."""


class StuckCandidate(NamedTuple):
    message_id: str
    first_ns: int        # first seen (Store.stuck_messages)
    on_sqs_clock: bool   # first_ns is SQS's SentTimestamp, not a send span's time


class TenantActivity(NamedTuple):
    tenant: Optional[str]
    sent: int            # sends
    dead_lettered: int   # messages moved to a dead-letter queue
    waits: Any           # first deliveries' times in queue, ms: a rollup.Sketch (its n is the first deliveries)


class Cutoffs(NamedTuple):
    """Retention cutoffs on span time (``time_ns``): a span that ended before its queue's cutoff has expired.
    ``None``: kept forever."""
    default: Optional[int]
    queues: Mapping[str, Optional[int]] = {}

    def of(self, queue: str) -> Optional[int]:
        return self.queues[queue] if queue in self.queues else self.default

    def keeps_everything(self) -> bool:
        return self.default is None and all(c is None for c in self.queues.values())


class PurgeStep(NamedTuple):
    deleted: int             # spans deleted
    examined: int            # messages and unjoined settle spans looked at
    resume: Optional[str]    # for the next step; None: the pass is complete
    write_ms: float = 0.0    # of which under the writer lock


class Store(abc.ABC):
    """Thread-safe: the server calls it from one thread per request, ingest and reads at once."""

    store_bodies: frozenset  # queues whose bodies are kept (``*``: every queue); empty: none
    # Set by the server: called as on_join("settle" | "receive", seconds) after each receipt-hash join at insert
    # (the /metrics join histogram). None: not measured.
    on_join: Optional[Callable[[str, float], None]] = None

    # Retention's granularity, in ns of span time (purge_step). 1: exact, a message expires as soon as its newest
    # span is older than the cutoff (SQLite). G > 1: the store drops whole time buckets ``[k*G, (k+1)*G)``, and a
    # message expires once the bucket of its newest span has ended before the cutoff, at ``(t // G + 1) * G``: with
    # daily buckets a 7-day retention keeps a message 7 to 8 days. Never shorter than the retention asked for.
    retention_granularity_ns: int = 1

    # ---- writes ------------------------------------------------------------------------------

    # True: insert takes ``abandoned``, a function saying whether the client that sent the export has gone, and
    # stops the work in the database when it has, raising ExportAbandoned (the server passes it then).
    cancels_abandoned_exports: bool = False

    @abc.abstractmethod
    def insert(self, records: Iterable[dict], *, wait_s: Optional[float] = None,
               timings: Optional[dict] = None) -> tuple[int, int]:
        """Store records (otlp.decode) atomically and join them (module docstring). A span already stored,
        resent as it was, is skipped: that is an exporter retry, end time included. What else counts as the same
        span is the store's (SQLite: same trace ID, span ID and end time; Postgres: same trace and span ID in
        the same day bucket). Returns (inserted, duplicates).

        Backpressure (backpressure.py): ``wait_s`` bounds how long to wait before the write starts (SQLite: the
        writer lock; Postgres: a pooled ingest connection); past it raise ``WriterBusy`` with nothing written
        (the server answers 503 writer_behind). A store whose database can't be reached raises
        ``StoreUnavailable`` instead, also with nothing written (503 store_unreachable); one whose disk is full
        raises ``StoreFull`` (503 store_full). Never return before the
        records are durable as far as the backend promises: the server answers 200 on return. ``timings``, if
        given, gets ``wait_s`` (time waited) and ``write_s`` (the write itself) in seconds. Each receipt-hash
        join of an inserted receive or settle span is reported to ``on_join`` (a store that joins a whole
        export at once reports each span's share). A store with ``cancels_abandoned_exports`` also takes
        ``abandoned``."""

    @abc.abstractmethod
    def purge_step(self, cutoffs: "Cutoffs", resume: Optional[str] = None, limit: int = 100) -> "PurgeStep":
        """Retention: one step of a pass that deletes expired spans. A pass is the steps from ``resume=None``,
        each given the previous step's ``resume``, until one returns ``resume=None``. A step looks at about
        ``limit`` messages and deletes the expired ones in one short write, so ingest and pages never wait long
        behind it; between steps the caller pauses.

        Expired, a message: its newest span ended before the cutoff of every queue it has a span on (with
        ``retention_granularity_ns`` G > 1: the end of that span's bucket, ``(time_ns // G + 1) * G``, is at or
        before it). So the
        longest retention among its queues wins, and one queue kept forever keeps the message. A message is
        deleted whole or not at all: no timeline is cut in half, and no settle span loses its receive. A settle
        span that belongs to no message (no ``mid``): its own ``time_ns`` against its own queue's cutoff, the same way.

        Spans that change a message between steps are taken into account: a step deletes a message only if it
        is still expired in the write. ``resume`` is opaque to the caller."""

    #: True: ``maintain`` does work, so the purge runs its passes even when every queue is kept forever.
    maintains = False

    def maintain(self) -> None:
        """Housekeeping retention's pass does first, whether or not anything can expire (so also when every
        queue is kept forever). Raises on failure; the pass logs it and goes on. Default: nothing."""

    def reclaim_space(self, max_pages: int) -> int:
        """Give up to ``max_pages`` of the space deleted spans left back to the file system. Returns the pages
        given back; 0 when there are none, or the backend reclaims space by itself."""
        return 0

    # ---- one message -------------------------------------------------------------------------

    @abc.abstractmethod
    def message_rows(self, message_id: str) -> list[Row]:
        """Every row whose ``mid`` is the message ID, by (time_ns, row)."""

    # ---- lists of messages: IDs, at most ``limit`` --------------------------------------------
    # Each reads the per-message summaries (summary.py), which the Store keeps up to date as spans arrive.
    # ``queue``: only messages with a span on that queue. "By newest span, latest first": the message's
    # highest time_ns (its summary's ``last_ns``), descending; ties by message ID, ascending.
    # ``from_ns``/``to_ns``: only messages whose newest span is in [from_ns, to_ns), each bound optional.
    # ``after``: the (last_ns, message ID) of the last message of the previous page: only messages after it in
    # the list's order (keyset pagination: a page costs the same however deep it is).

    @abc.abstractmethod
    def recent_messages(self, limit: int, queue: Optional[str] = None, tenant: Optional[str] = None, *,
                        from_ns: Optional[int] = None, to_ns: Optional[int] = None,
                        after: Optional[tuple] = None) -> list[str]:
        """By newest span, latest first. ``tenant``: only messages with a span naming it (cut to
        TENANT_MAX_CHARS)."""

    @abc.abstractmethod
    def duplicate_messages(self, limit: int, queue: Optional[str] = None, reason: Optional[str] = None, *,
                           from_ns: Optional[int] = None, to_ns: Optional[int] = None,
                           after: Optional[tuple] = None) -> list[str]:
        """Messages with more than one receive (``received_more_than_once``) or more than one success
        (``succeeded_more_than_once``), either if ``reason`` is None; by newest span, latest first. A success
        is a delivery with an accepted delete (``result = 'ok'``, counted once per ``receive_row``) or a
        distinct ``faas_invocation_id`` with a ``success`` outcome."""

    @abc.abstractmethod
    def stuck_messages(self, cutoff_ns: int, limit: int, queue: Optional[str] = None, *,
                       from_ns: Optional[int] = None, after: Optional[tuple] = None) -> list[StuckCandidate]:
        """Messages first seen before ``cutoff_ns`` (and at or after ``from_ns``) with no final delete, oldest
        first. ``after``: the (first_ns, message ID) of the previous page's last message.

        First seen: the earliest send span's time, else the earliest receive's SentTimestamp (``sent_ts_ms``,
        on SQS's clock: ``on_sqs_clock``), else the earliest send or receive time. Messages with neither a send
        nor a receive are not listed. Ties: by message ID. Final:
        an accepted delete whose ``receive_row`` is the message's latest receive (by time_ns, then row), or a
        ``success`` outcome with that receive's ``faas_invocation_id``.

        Purged messages are not listed (purge.py): those with a purge record of ``result = 'ok'`` on their summary's
        ``current_queue`` whose ``start_ns`` is after the summary's ``last_ns``. Possibly purged ones are."""

    def purges(self, queues) -> list[Row]:
        """The purge records (contract section 18) stored for these queues, every column of purge.COLUMNS, by
        (time_ns, row); ``row`` is unique among purges. Few: one per PurgeQueue call. Not abstract, so a store
        written before revision 3.3 still loads: by default, none."""
        return []

    @abc.abstractmethod
    def summaries(self, message_ids: list[str]) -> list[Summary]:
        """The summaries (summary.summarize: every field in summary.FIELDS) of these messages, in the order
        given; a message with no spans is left out."""

    @abc.abstractmethod
    def queue_messages(self, queue: str) -> set[str]:
        """Every message with a span on the queue."""

    # ---- aggregates --------------------------------------------------------------------------

    @abc.abstractmethod
    def tenant_activity(self, from_ms: int, to_ms: int) -> list[TenantActivity]:
        """What the Tenants page counts in [from_ms, to_ms], both included: per message tenant (None for messages
        with none), the summaries' tenant_events in the range added up. Tenants with nothing in the range are
        left out; any order."""

    @abc.abstractmethod
    def find_tenants(self, text: str, limit: int, *, from_ns: Optional[int] = None, to_ns: Optional[int] = None,
                     queue: Optional[str] = None) -> list[str]:
        """Tenants whose name starts with ``text``, ignoring ASCII case only: an exact (ASCII case-insensitive)
        match first, then by the newest span naming each, latest first, then name. Empty text: none.
        ``from_ns``/``to_ns``: only tenants a message names whose newest span naming them is in [from_ns,
        to_ns); ``queue``: only messages with a span on that queue."""

    @abc.abstractmethod
    def counts(self) -> dict:
        """Exact: ``{"spans": {kind: n}, "messages": n, "unjoined_settles": n}``, kinds with no spans left out.
        Unjoined: settle spans with no ``receive_row``."""

    def file_sizes(self) -> Optional[dict]:
        """Bytes on local disk by file (SQLite: ``main``, ``wal``) for /metrics; None when the data isn't in
        local files (``:memory:``, a server-side database)."""
        return None

    @abc.abstractmethod
    def last_row(self) -> int:
        """The highest ``row`` stored, 0 if none. Cheap at any size (the health check)."""

    def last_span_ns(self) -> Optional[int]:
        """The time (``time_ns``, its end) of the span stored last, None if none: what ``/api/health`` reports as
        the newest span, for trailbeam doctor. Cheap at any size. Not abstract, so a store written before it still
        loads: by default, None (not known)."""
        return None

    # ---- probes (trailbeam doctor, contract section 17) ---------------------------------------

    # How long a probe is kept at least: record_probes deletes older ones when it writes. Probes are not spans: no page, count, rollup or
    # retention pass reads them.
    PROBE_KEEP_NS = 86_400 * 10 ** 9

    def record_probes(self, probe_ids: list[str], received_ns: int, *, wait_s: Optional[float] = None) -> None:
        """Store the probes of one export (at most otlp.MAX_PROBES_PER_EXPORT) with the time this server stored
        them, replacing ones of the same ID, and delete probes older than ``PROBE_KEEP_NS``. One write transaction, on the same writer exports use (``wait_s``, WriterBusy
        and StoreUnavailable as in insert), so a probe stored says that exports can be. A store that does not keep
        probes raises NotImplementedError (the default): the server then rejects the probe span, saying so."""
        raise NotImplementedError(NO_PROBES)

    def probe(self, probe_id: str) -> Optional[int]:
        """When the probe was stored (``received_ns``), None if it is not."""
        return None

    def ping(self, timeout_s: float = 2.0) -> None:
        """Raise StoreUnavailable unless the store answers within about ``timeout_s`` (the readiness check,
        ``/api/ready``). A local file is always there: by default, nothing to check."""

    # ---- bodies ------------------------------------------------------------------------------

    @abc.abstractmethod
    def newest_messages(self, limit: int, queue: Optional[str] = None, tenant: Optional[str] = None, *,
                        from_ns: Optional[int] = None, to_ns: Optional[int] = None,
                        after: Optional[tuple] = None) -> list[tuple[int, str]]:
        """(last_ns, mid) of up to ``limit`` messages, as recent_messages orders and filters them (``tenant``
        included): the walk body search reads bodies along (bodies.search)."""

    @abc.abstractmethod
    def body_spans(self, message_ids: list[str]) -> list[Row]:
        """The send and receive rows of these messages that carry a stored body, with the columns row, kind,
        mid, queue, ``body`` (the text) and ``format`` (trailbeam.body.format, or None). Any order. Indexed:
        the cost is the messages asked for, not the bodies stored."""

    # ---- lifecycle ---------------------------------------------------------------------------

    @abc.abstractmethod
    def close(self) -> None:
        ...
