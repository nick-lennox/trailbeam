"""``GET /api/events`` (QV-394): the recorded steps in a time window, one per stored span, newest first, and their
counts per time bucket for a histogram. The Event explorer reads it, here and in the hosted app.

A step is a span as it was recorded: a send, a receive, a delete, a visibility change or a Lambda outcome. Nothing
is inferred (a timeline's ``visibility_timeout_expired`` or ``lambda_invocation_ended_without_outcome`` steps are
not here), and PurgeQueue calls, which belong to no message, are left out. ``message_id`` is the message the step
belongs to, as a timeline places it (db.py): a delete or visibility change takes the message of the receive whose
receipt handle it used, so it is null for one whose receive hasn't arrived and that carries no message ID itself.

The page is read in SQL by the store (``Store.step_rows``), newest first by ``(time_ns, trace_id, span_id)``, which
is unique (an exporter retry sends the same span with the same end time, and the store keeps it once), so a cursor
names one place in the order and pages don't overlap or skip. The counts are added up in SQL too
(``Store.step_counts``), so neither reads more than a page of rows into Python however long the window is. With
``message_id`` the store reads that message's rows by its index (``Store.message_rows``) and the filters run here:
a message has a few rows.

The hosted app's engine answers the same query (trailbeam-app lib/app/engine/events.ts) and its parity test checks
that both give the same rows and counts on the same spans."""
from __future__ import annotations

import base64
import binascii
from typing import Optional

from .queries import CursorError
from .timeline import iso

KINDS = ("send", "receive", "delete", "change_visibility", "outcome")
# Bucket widths for the histogram, in seconds: the smallest that gives at most MAX_BUCKETS buckets for the window,
# else the fewest whole days that do. Buckets start at multiples of the width since the Unix epoch (UTC), so two windows that overlap
# share their buckets' edges.
BUCKET_S = (60, 300, 900, 3_600, 10_800, 21_600, 86_400)
MAX_BUCKETS = 200
NS_PER_MS = 1_000_000
INT64_MAX = 2 ** 63 - 1


class EventQueryError(ValueError):
    pass


def parse_kinds(text: Optional[str]) -> Optional[tuple]:
    """``kind=send,receive``: a comma-separated list of KINDS, in KINDS order; None (every kind) when empty."""
    if text is None or not text.strip():
        return None
    got = {k.strip() for k in text.split(",") if k.strip()}
    bad = sorted(got - set(KINDS))
    if bad:
        raise EventQueryError(f"kind must be one or more of {', '.join(KINDS)}, separated by commas; "
                              f"not {bad[0]!r}")
    return tuple(k for k in KINDS if k in got)


def bucket_ms(from_ms: int, to_ms: int) -> int:
    """The histogram's bucket width for the window [from_ms, to_ms], both included."""
    span_ms = to_ms - from_ms + 1
    for s in BUCKET_S:
        if -(-span_ms // (s * 1000)) + 1 <= MAX_BUCKETS:  # +1: a window that doesn't start on an edge spills over
            return s * 1000
    day = BUCKET_S[-1] * 1000
    return day * -(-span_ms // (day * (MAX_BUCKETS - 1)))


def encode_cursor(time_ns: int, trace_id: str, span_id: str) -> str:
    return base64.urlsafe_b64encode(f"{time_ns}:{trace_id}:{span_id}".encode()).decode().rstrip("=")


def decode_cursor(text: Optional[str]) -> Optional[tuple]:
    if not text:
        return None
    try:
        ns, trace_id, span_id = base64.urlsafe_b64decode(text + "=" * (-len(text) % 4)).decode().split(":")
        return int(ns), trace_id, span_id
    except (ValueError, binascii.Error, UnicodeDecodeError):
        raise CursorError("cursor is not one this server returned") from None


def _key(r) -> tuple:
    return r["time_ns"], r["trace_id"], r["span_id"]


def step(r) -> dict:
    """One row of ``events``. Every field is always there, null when the span didn't carry it."""
    start = r["start_ns"]
    return {
        "time": iso(r["time_ns"]),
        "time_unix_nano": str(r["time_ns"]),
        "start_unix_nano": None if start is None else str(start),
        "kind": r["kind"],
        "message_id": r["mid"],
        "queue": r["queue"],
        "service_name": r["service_name"],
        "service_version": r["service_version"],
        "result": r["result"],
        "error_code": r["error_code"],
        "outcome": r["outcome"],
        "error_type": r["error_type"],
        "receive_count": r["receive_count"],
        "visibility_timeout": r["visibility_timeout"],
        "trace_id": r["trace_id"],
        "span_id": r["span_id"],
    }


def _matches(r, from_ns: int, to_ns: int, queue, service, kinds) -> bool:
    return (from_ns <= r["time_ns"] < to_ns and r["kind"] in (kinds or KINDS)
            and (queue is None or r["queue"] == queue) and (service is None or r["service_name"] == service))


def page(db, from_ms: int, to_ms: int, limit: int, *, queue: Optional[str] = None, service: Optional[str] = None,
         kinds: Optional[tuple] = None, message_id: Optional[str] = None, cursor: Optional[str] = None) -> dict:
    """The answer of ``/api/events`` for [from_ms, to_ms], both included. The histogram comes with the first page
    only (no ``cursor``): it is the same for every page of the query, and is the costlier half."""
    after = decode_cursor(cursor)
    # nanoseconds past 2^63 (from about year 2262) can't be stored: clamp, so a window reaching that far just ends
    from_ns, to_ns = min(from_ms * NS_PER_MS, INT64_MAX), min((to_ms + 1) * NS_PER_MS, INT64_MAX)
    width = bucket_ms(from_ms, to_ms)
    width_ns = width * NS_PER_MS
    if message_id is not None:
        mine = [r for r in db.message_rows(message_id) if _matches(r, from_ns, to_ns, queue, service, kinds)]
        mine.sort(key=_key, reverse=True)
        rows = [r for r in mine if after is None or _key(r) < after][:limit + 1]
        counts: dict = {}
        if after is None:
            for r in mine:
                k = (r["time_ns"] // width_ns, r["kind"])
                counts[k] = counts.get(k, 0) + 1
        counted = [(b, k, n) for (b, k), n in counts.items()]
    else:
        rows = db.step_rows(from_ns, to_ns, limit + 1, queue=queue, service=service, kinds=kinds, after=after)
        counted = [] if after is not None else db.step_counts(from_ns, to_ns, width_ns, queue=queue,
                                                              service=service, kinds=kinds)
    more = len(rows) > limit
    rows = rows[:limit]
    return {
        "from_ms": from_ms,
        "to_ms": to_ms,
        "events": [step(r) for r in rows],
        "next_cursor": encode_cursor(*_key(rows[-1])) if more else None,
        "histogram": None if after is not None else histogram(counted, from_ms, to_ms, width),
    }


def histogram(counted, from_ms: int, to_ms: int, width_ms: int) -> dict:
    """``counted``: (bucket index, kind, steps), the bucket index being time_ns // (width_ms ms). Every bucket of the
    window is listed, oldest first, with a zero for each kind it has none of. The first and last bucket count only
    the steps inside the window."""
    first, last = from_ms // width_ms, to_ms // width_ms
    buckets = [{"start_ms": i * width_ms, **{k: 0 for k in KINDS}} for i in range(first, last + 1)]
    totals = {k: 0 for k in KINDS}
    for b, kind, n in counted:
        n = int(n)
        buckets[int(b) - first][kind] += n
        totals[kind] += n
    return {"bucket_ms": width_ms, "buckets": buckets, "totals": totals, "total": sum(totals.values())}
