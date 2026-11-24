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

The first page counts first, then reads rows only where the counts found them (QV-468): only the kinds with a
step in the window, and only the newest buckets that hold a page of them. Without that, a filter that matches
little (a queue, a service, a kind) walks every step of the window that doesn't match before it has a page, and
one that matches nothing walks all of them. The rows are the same either way.

The hosted app's engine answers the same query (trailbeam-app lib/app/engine/events.ts) and its parity test checks
that both give the same rows and counts on the same spans."""
from __future__ import annotations

import base64
import binascii
import time
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


# The hourly rollup of steps (step_rollup: db.py, QV-394; postgres.py, QV-468): the steps per (hour, queue, service,
# kind), kept exact at ingest and by retention. A histogram whose buckets are whole hours reads the hours wholly
# inside the window from it, so a 7-day histogram adds up about 168 hours of rollup rows instead of counting every
# span. A partial hour at either end is read the cheaper way, as rollup.plan reads the Tenants page's (QV-224): its
# own spans, or the hour's rollup rows less the spans of the rest of the hour. A window ending now takes its current
# hour from the rollup, less the spans newer than its end (next to none), instead of counting every span of that
# hour: about 125 ms of a 7-day page after a heavy hour (trailbeam-app docs/bench/hosted-events.md). A service of
# '' in the rollup is a span with none (the API never asks for '').
HOUR_NS = 3_600 * 10 ** 9


def hour_split(from_ns: int, to_ns: int, bucket_ns: int, now_ns: Optional[int] = None) -> Optional[tuple]:
    """For buckets of whole hours: ([(from_ns, to_ns, sign)] spans to count (sign 1) or take out (-1), (first hour,
    end hour) of the rollup to add, or None), so the two add up to [from_ns, to_ns). Each hour is in one bucket, so a
    span taken out is in the bucket its rollup hour is. None when the buckets aren't whole hours: count it all from
    the spans. A partial hour comes from the rollup when the part of it in the window is longer than the rest of
    it; only time up to ``now_ns`` (default: now) counts in the lengths, since later there are next to no spans."""
    if bucket_ns % HOUR_NS or from_ns >= to_ns:
        return None
    now_ns = time.time_ns() if now_ns is None else now_ns

    def past(a, b):  # how much of [a, b) is not in the future
        return max(0, min(b, now_ns) - a)

    first, last = from_ns // HOUR_NS, (to_ns - 1) // HOUR_NS
    hours, ranges = [], []
    for h in sorted({first, last}):
        lo, hi = h * HOUR_NS, (h + 1) * HOUR_NS
        a, b = max(from_ns, lo), min(to_ns, hi)
        rest = [(x, y) for x, y in ((lo, a), (b, hi)) if x < y]
        if hi > INT64_MAX or (rest and past(a, b) <= sum(past(x, y) for x, y in rest)):
            ranges.append((a, b, 1))  # its own spans (an hour past 2^63 ns can't be named in SQL)
            continue
        hours.append(h)
        ranges += [(x, y, -1) for x, y in rest]
    if last - first >= 2:
        hours += [first + 1, last - 1]  # the whole hours between
    return ranges, ((min(hours), max(hours) + 1) if hours else None)


def rows_range(counted, from_ns: int, to_ns: int, bucket_ns: int, need: int) -> Optional[tuple]:
    """The part of [from_ns, to_ns) that holds the newest ``need`` steps the counts found: from the newest bucket
    with a step back to the bucket where ``need`` is reached (or the window's start). None: no step at all."""
    by: dict = {}
    for b, _, n in counted:
        by[int(b)] = by.get(int(b), 0) + int(n)
    found = sorted((b for b, n in by.items() if n > 0), reverse=True)
    if not found:
        return None
    hi, seen, lo = min(to_ns, (found[0] + 1) * bucket_ns), 0, from_ns
    for b in found:
        seen += by[b]
        if seen >= need:
            lo = max(from_ns, b * bucket_ns)
            break
    return lo, hi


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
    elif after is None:
        counted = db.step_counts(from_ns, to_ns, width_ns, queue=queue, service=service, kinds=kinds)
        found = {k for _, k, n in counted if n}
        ks = tuple(k for k in (kinds or KINDS) if k in found)
        where = rows_range(counted, from_ns, to_ns, width_ns, limit + 1) if ks else None
        rows = [] if where is None else db.step_rows(*where, limit + 1, queue=queue, service=service, kinds=ks)
        if where is not None and len(rows) <= limit and where[0] > from_ns:
            # the counts and the rows are two reads: retention took steps out between them. Read the whole window.
            rows = db.step_rows(from_ns, to_ns, limit + 1, queue=queue, service=service, kinds=kinds)
    else:
        rows = db.step_rows(from_ns, to_ns, limit + 1, queue=queue, service=service, kinds=kinds, after=after)
        counted = []
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
