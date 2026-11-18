"""The storage interface: what the server needs from wherever spans are kept.

The server reaches storage only through a ``Store``. SQLite (db.py, ``SqliteStore``) is the default and
the only one today; Postgres and ClickHouse backends will implement this interface, and every backend must
pass the conformance suite in ``python/tests/conformance`` (the same answers, from the same spans, as SQLite).

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
"""
from __future__ import annotations

import abc
from typing import Any, Iterable, Mapping, NamedTuple, Optional

Row = Mapping[str, Any]
Summary = Mapping[str, Any]  # summary.summarize, as stored

SETTLE_KINDS = ("delete", "change_visibility")

# Tenants (contract/tenants.md): a span's tenant is its trailbeam.tenant attribute when that is a non-empty
# string, cut to TENANT_MAX_CHARS, on send, receive and outcome spans only.
TENANT_ATTR = "trailbeam.tenant"
TENANT_KINDS = ("send", "receive", "outcome")
TENANT_MAX_CHARS = 256

DUPLICATE_REASONS = ("received_more_than_once", "succeeded_more_than_once")


class StuckCandidate(NamedTuple):
    message_id: str
    first_ns: int        # first seen (Store.stuck_messages)
    on_sqs_clock: bool   # first_ns is SQS's SentTimestamp, not a send span's time


class TenantActivity(NamedTuple):
    tenant: Optional[str]
    sent: int            # sends
    dead_lettered: int   # messages moved to a dead-letter queue
    waits: list          # first deliveries' times in queue, ms


class Store(abc.ABC):
    """Thread-safe: the server calls it from one thread per request, ingest and reads at once."""

    store_bodies: frozenset  # queues whose bodies are kept (``*``: every queue); empty: none

    # ---- writes ------------------------------------------------------------------------------

    @abc.abstractmethod
    def insert(self, records: Iterable[dict]) -> tuple[int, int]:
        """Store records (otlp.decode) atomically and join them (module docstring). A span already stored,
        same trace and span ID, is skipped. Returns (inserted, duplicates)."""

    @abc.abstractmethod
    def delete_messages_before(self, cutoff_ns: int, queue: Optional[str] = None, limit: int = 10_000) -> int:
        """Retention: delete every span of up to ``limit`` messages whose newest span is before ``cutoff_ns``,
        and settle spans that belong to no message and ended before it. Whole messages, so no timeline is cut
        in half. ``queue``: only messages with a span on that queue, and unjoined settle spans on it. Returns
        the number of spans deleted; call again until it returns 0."""

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
        ``success`` outcome with that receive's ``faas_invocation_id``."""

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

    @abc.abstractmethod
    def last_row(self) -> int:
        """The highest ``row`` stored, 0 if none. Cheap at any size (the health check)."""

    # ---- bodies ------------------------------------------------------------------------------

    @abc.abstractmethod
    def body_rows(self) -> list[Row]:
        """Send and receive rows of a message whose ``attributes`` hold a text ``trailbeam.body``, with the
        columns row, kind, mid, queue, attributes and ``last`` (the message's newest time_ns); by mid, sends
        first, then row. bodies.search filters and matches them."""

    # ---- lifecycle ---------------------------------------------------------------------------

    @abc.abstractmethod
    def close(self) -> None:
        ...
