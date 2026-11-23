"""The queries behind the screens: one message's timeline, duplicate deliveries, stuck messages, tenants.

Storage is reached through the Store interface (store.py), which returns message IDs, rows and per-message
summaries (summary.py). List pages are built from the summaries; only ``message`` builds a full timeline. The
pages and the Tenants page are built here, the same for every backend.

Pagination: a list page returns at most ``limit`` items and, when there are more, ``next_cursor``, which
gives the next page when passed back as ``cursor``. A cursor is the sort key of the page's last item, so a
page costs the same however deep it is, and spans arriving meanwhile don't shift later pages."""
from __future__ import annotations

import base64
import binascii
import math
from datetime import datetime, timedelta, timezone
from typing import NamedTuple, Optional

from . import bodies, rollup, summary, timeline
from .store import Store
from .summary import message_tenant, span_tenant  # noqa: F401  (part of this module's interface)

MAX_LIMIT = 1000


def message(db: Store, message_id: str, now_ns: int, lambda_timeout_s: int,
            max_receive_counts: Optional[dict] = None) -> Optional[dict]:
    """``max_receive_counts``: queue name -> maxReceiveCount, as configured (``--max-receive-count``)."""
    rows = db.message_rows(message_id)
    if not rows:
        return None
    return timeline.build(rows, now_ns, lambda_timeout_s, max_receive_counts, db.purges({r["queue"] for r in rows}))


class Page(NamedTuple):
    items: list
    next_cursor: Optional[str]  # None: this is the last page


class CursorError(ValueError):
    pass


def encode_cursor(ns: int, mid: str) -> str:
    return base64.urlsafe_b64encode(f"{ns}:{mid}".encode()).decode().rstrip("=")


def decode_cursor(text: Optional[str]) -> Optional[tuple]:
    if not text:
        return None
    try:
        ns, _, mid = base64.urlsafe_b64decode(text + "=" * (-len(text) % 4)).decode().partition(":")
        return int(ns), mid
    except (ValueError, binascii.Error, UnicodeDecodeError):
        raise CursorError("cursor is not one this server returned") from None


def _purges(db: Store, sums) -> list:
    """The purges (contract section 18) of the queues these summaries' messages are on: one read per page."""
    return db.purges({s["current_queue"] for s in sums})


def _items(db: Store, mids: list, now_ns: int, lambda_timeout_s: int, mrc: Optional[dict]) -> list[dict]:
    sums = db.summaries(mids)
    purges = _purges(db, sums)
    return [summary.item(s, now_ns, lambda_timeout_s, mrc, purges) for s in sums]


def _page(items: list, limit: int, key) -> Page:
    """``items``: up to limit + 1 (one more tells whether there is a next page)."""
    if len(items) <= limit:
        return Page(items, None)
    return Page(items[:limit], encode_cursor(*key(items[limit - 1])))


def _newest_first(db: Store, mids: list, limit: int, now_ns: int, lambda_timeout_s: int, mrc) -> Page:
    sums = db.summaries(mids)
    page = _page(sums, limit, lambda s: (s["last_ns"], s["mid"]))
    purges = _purges(db, page.items)
    return Page([summary.item(s, now_ns, lambda_timeout_s, mrc, purges) for s in page.items], page.next_cursor)


def recent_page(db: Store, now_ns: int, lambda_timeout_s: int, limit: int, queue: Optional[str] = None,
                mrc: Optional[dict] = None, tenant: Optional[str] = None, *, from_ns: Optional[int] = None,
                to_ns: Optional[int] = None, cursor: Optional[str] = None) -> Page:
    """The most recently active messages; ``tenant``: only those with a span naming it (contract/tenants.md).
    ``from_ns``/``to_ns``: last seen in [from_ns, to_ns)."""
    mids = db.recent_messages(limit + 1, queue, tenant, from_ns=from_ns, to_ns=to_ns, after=decode_cursor(cursor))
    return _newest_first(db, mids, limit, now_ns, lambda_timeout_s, mrc)


def recent(db: Store, now_ns: int, lambda_timeout_s: int, limit: int, queue: Optional[str] = None,
           mrc: Optional[dict] = None, tenant: Optional[str] = None) -> list[dict]:
    """The first page of ``recent_page``'s items."""
    return recent_page(db, now_ns, lambda_timeout_s, limit, queue, mrc, tenant).items


class BodySearch(NamedTuple):
    page: Page
    scanned: int               # messages whose body was read
    oldest_ns: Optional[int]   # last seen of the oldest of them: the search covered from there to to_ns


def body_search_page(db: Store, now_ns: int, lambda_timeout_s: int, limit: int, queue: Optional[str],
                     terms: list[dict], mrc: Optional[dict] = None, *, from_ns: Optional[int] = None,
                     to_ns: Optional[int] = None, cursor: Optional[str] = None,
                     scan: int = bodies.SCAN, tenant: Optional[str] = None) -> BodySearch:
    """Messages last seen in [from_ns, to_ns) whose kept body matches the body.* terms (bodies.py), most recently
    seen first. A page reads at most ``scan`` messages: ``next_cursor`` goes on from the last one read, so a page
    can have fewer than ``limit`` items, or none, and still a next one. None: the range is searched to its end.
    ``tenant``: only messages with a span naming it, as on ``recent_page``."""
    r = bodies.search(db, terms, db.store_bodies, limit=limit, queue=queue, tenant=tenant, from_ns=from_ns,
                      to_ns=to_ns, after=decode_cursor(cursor), scan=scan)
    return BodySearch(Page(_items(db, r.mids, now_ns, lambda_timeout_s, mrc),
                           encode_cursor(*r.after) if r.after else None), r.scanned, r.oldest_ns)


def body_search(db: Store, now_ns: int, lambda_timeout_s: int, limit: int, queue: Optional[str],
                terms: list[dict], mrc: Optional[dict] = None) -> list[dict]:
    """The first page of ``body_search_page``, over all history."""
    return body_search_page(db, now_ns, lambda_timeout_s, limit, queue, terms, mrc).page.items


def _with_reasons(item: dict) -> dict:
    item["reasons"] = (["received_more_than_once"] if item["deliveries"] > 1 else []) + \
                      (["succeeded_more_than_once"] if item["successful_deliveries"] > 1 else [])
    return item


def duplicates_page(db: Store, now_ns: int, lambda_timeout_s: int, limit: int, queue: Optional[str] = None,
                    reason: Optional[str] = None, mrc: Optional[dict] = None, *, from_ns: Optional[int] = None,
                    to_ns: Optional[int] = None, cursor: Optional[str] = None) -> Page:
    """Messages received more than once, or that succeeded more than once, most recently active first.

    A success is a delivery that ended in an accepted delete (counted once per delivery, however many
    times its handle was deleted) or a Lambda ``success`` outcome.
    """
    mids = db.duplicate_messages(limit + 1, queue, reason, from_ns=from_ns, to_ns=to_ns, after=decode_cursor(cursor))
    page = _newest_first(db, mids, limit, now_ns, lambda_timeout_s, mrc)
    return Page([_with_reasons(i) for i in page.items], page.next_cursor)


def duplicates(db: Store, now_ns: int, lambda_timeout_s: int, limit: int,
               queue: Optional[str] = None, reason: Optional[str] = None, mrc: Optional[dict] = None) -> list[dict]:
    return duplicates_page(db, now_ns, lambda_timeout_s, limit, queue, reason, mrc).items


def stuck_page(db: Store, now_ns: int, lambda_timeout_s: int, older_than_s: int, limit: int,
               queue: Optional[str] = None, mrc: Optional[dict] = None, *, from_ns: Optional[int] = None,
               cursor: Optional[str] = None) -> Page:
    """Messages first seen more than ``older_than_s`` ago (and at or after ``from_ns``) that have no final
    delete, oldest first.

    Final means an accepted delete whose receipt handle is the one from the message's latest receive
    (span contract section 4.3), or a Lambda ``success`` outcome for the latest receive. First seen is
    as in :func:`timeline.first_seen_ns`: the send time on the producer host's clock when the send was
    observed, and only otherwise SQS's SentTimestamp, on SQS's clock. For those messages the cutoff and
    ``age_s`` compare SQS's clock with this server's, and each such item says so (``age_across_clocks``).

    Messages a recorded PurgeQueue call removed are not listed (state ``purged``, contract section 18); those
    that were on the queue during a purge are, with ``purge.status`` ``possibly_purged``.
    """
    cutoff = now_ns - older_than_s * 1_000_000_000
    found = db.stuck_messages(cutoff, limit + 1, queue, from_ns=from_ns, after=decode_cursor(cursor))
    sums = {s["mid"]: s for s in db.summaries([r.message_id for r in found])}
    purges = _purges(db, sums.values())
    out = []
    for r in found:
        if r.message_id not in sums:
            continue
        item = summary.item(sums[r.message_id], now_ns, lambda_timeout_s, mrc, purges)
        item["age_s"] = round((now_ns - r.first_ns) / 1e9, 3)
        # True when first seen is SQS's SentTimestamp: the age is this server's clock minus SQS's, so skew
        # between the two clocks is in it.
        item["age_across_clocks"] = r.on_sqs_clock
        out.append((item, r))
    page = _page(out, limit, lambda x: (x[1].first_ns, x[1].message_id))
    return Page([i for i, _ in page.items], page.next_cursor)


def stuck(db: Store, now_ns: int, lambda_timeout_s: int, older_than_s: int, limit: int,
          queue: Optional[str] = None, mrc: Optional[dict] = None) -> list[dict]:
    return stuck_page(db, now_ns, lambda_timeout_s, older_than_s, limit, queue, mrc).items


# ---- tenants (contract/tenants.md) -------------------------------------------------------------

_NS_PER_MS = 1_000_000
_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def _ms(ns) -> int:
    return int(ns) // _NS_PER_MS


def _iso_ms(text: str) -> int:
    # timeline.iso: 2025-11-14T00:26:40.123456Z, always UTC with microseconds
    dt = datetime.strptime(text, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc)
    return (dt - _EPOCH) // timedelta(milliseconds=1)  # exact: no float on the way


def p95(values: list) -> Optional[int]:
    """Nearest-rank p95: the value at position ceil(0.95 n), counting from 1. None for no values."""
    if not values:
        return None
    s = sorted(values)
    return s[max(0, math.ceil(0.95 * len(s)) - 1)]


def tenant_counts(timeline_events: list, tenant: Optional[str], from_ms: int, to_ms: int) -> dict:
    """What one message adds to the Tenants page: its sends, first deliveries' times in queue and
    whether it was dead-lettered, each only when its time is in [from_ms, to_ms]. A send SQS deduplicated
    enqueued nothing, so it is not counted."""
    sends = [e for e in timeline_events if e["type"] == "sent"]
    first_send = _ms(sends[0]["end_unix_nano"]) if sends else None
    out = {"tenant": tenant, "sent": 0, "waits": [], "dead_lettered": False}
    for e in timeline_events:
        if e["type"] == "sent":
            if not e.get("deduplicated") and from_ms <= _ms(e["end_unix_nano"]) <= to_ms:
                out["sent"] += 1
        elif e["type"] == "received":
            at = _ms(e["end_unix_nano"])
            if e.get("delivery") != 1 or e.get("receive_count") != 1 or not from_ms <= at <= to_ms:
                continue
            sent_at = first_send if first_send is not None else e.get("sent_timestamp_ms")
            if isinstance(sent_at, int):
                out["waits"].append(max(0, at - sent_at))
        elif e["type"] == "moved_to_dead_letter_queue":
            to = (e.get("window") or {}).get("to")
            if to and from_ms <= _iso_ms(to) <= to_ms:
                out["dead_lettered"] = True
    return out


def tenant_stats(per_message: list, capped: bool = False) -> dict:
    """The Tenants page (contract/tenants.md) from each message's tenant_counts, or from totals per tenant
    (Store.tenant_activity) in the same shape, with ``dead_lettered`` a count of messages. ``waits`` is a list of
    times in queue or a rollup.Sketch of them; p95 comes from the sketch either way (within rollup.BOUND)."""
    by: dict = {}
    total = untagged = 0
    for m in per_message:
        total += m["sent"]
        if m["tenant"] is None:
            untagged += m["sent"]
            continue
        waits = m["waits"] if isinstance(m["waits"], rollup.Sketch) else rollup.Sketch.of(m["waits"])
        if not (m["sent"] or waits or m["dead_lettered"]):
            continue
        g = by.setdefault(m["tenant"], {"sent": 0, "waits": rollup.Sketch(), "dead_lettered": 0})
        g["sent"] += m["sent"]
        g["waits"].merge(waits)
        g["dead_lettered"] += int(m["dead_lettered"])  # a message's flag, or a count of messages
    rows = [{"tenant": t, "sent": g["sent"], "share": g["sent"] / total if total else 0,
             "p95_in_queue_ms": g["waits"].p95(), "first_deliveries": g["waits"].n,
             "dead_lettered": g["dead_lettered"]} for t, g in by.items()]
    rows.sort(key=lambda r: (-r["sent"], -r["dead_lettered"], r["tenant"]))
    return {"total_sent": total, "untagged_sent": untagged, "capped": capped, "rows": rows}


def tenants(db: Store, now_ns: int, lambda_timeout_s: int, from_ms: int, to_ms: int,
            mrc: Optional[dict] = None) -> dict:
    """The Tenants page for [from_ms, to_ms], both included (contract/tenants.md), from every message: the
    store's hourly rollup of the summaries' tenant events (rollup.py), so no timeline is built and nothing is
    capped. ``capped`` is always false; it stays in the response for clients of the capped page. ``now_ns``,
    the Lambda timeout and ``mrc`` don't change what the page counts."""
    per_tenant = [{"tenant": a.tenant, "sent": a.sent, "waits": a.waits, "dead_lettered": a.dead_lettered}
                  for a in db.tenant_activity(from_ms, to_ms)]
    out = tenant_stats(per_tenant, capped=False)
    return {"from_ms": from_ms, "to_ms": to_ms, **out}


def find_tenants(db: Store, text: str, limit: int = 8, *, from_ms: Optional[int] = None, to_ms: Optional[int] = None,
                 queue: Optional[str] = None) -> list[str]:
    """Tenants whose name starts with the text, ignoring ASCII case; an exact match first (contract/tenants.md).
    ``from_ms``/``to_ms``: named in [from_ms, to_ms], both included; ``queue``: by messages on that queue."""
    if not text:
        return []
    return db.find_tenants(text, limit, from_ns=None if from_ms is None else from_ms * _NS_PER_MS,
                           to_ns=None if to_ms is None else (to_ms + 1) * _NS_PER_MS, queue=queue)
