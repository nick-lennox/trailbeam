"""What a RabbitMQ message's stored spans say, for its timeline and summary (contract/span-contract-amqp.md A6 to A10).

A RabbitMQ row (otlp.py) keeps the span's attributes; the facts the timeline needs beyond the row's columns are
read from them here: the receive count's source (A7), the ``x-death`` copy (A8), the publish's flags, the close's
initiator and reply. ``facts`` turns a stored row into a dict with them, once per row.

The rules that differ from SQS:

- **A copy per queue** (A6). A fanout publish to two queues is one message with two copies. A copy is done when its
  latest delivery was acked (``basic.ack`` accepted), refused (``requeue = false``) or the message was observed
  moving off its queue to a dead-letter queue (``x-death``).
- **Redeliveries** (A10). No visibility timeout exists: a later delivery on the same queue with a higher receive
  count (or, with no count, ``redelivered = true``) is a return to the queue, whose cause is what the spans of the
  earlier delivery show (``cause_of``).
- **x-delivery-limit** (A7). It is checked against ``x-delivery-count``, which counts only failed returns (a
  ``basic.reject`` with requeue, a closed channel), not ``basic.nack`` with requeue since RabbitMQ 4.3.
  ``failed_returns`` is the count the source queue's copy has reached; the queue dead-letters the message when it
  goes above the limit.
"""
from __future__ import annotations

import json
from typing import Optional

from .systems import RABBITMQ, system_of

# A12 attribute names
COUNT_SOURCE = "trailbeam.receive_count.source"
ROUTING_KEY = "rabbitmq.destination.routing_key"
DELIVERY_COUNT = "trailbeam.amqp.delivery_count"
ACQUIRED_COUNT = "trailbeam.amqp.acquired_count"
CHANNEL = "trailbeam.amqp.channel.id"
CONSUMER_TAG = "trailbeam.amqp.consumer_tag"
FIRST_DELIVERY = "first_delivery"

# The source of an observed move to a dead-letter queue on RabbitMQ (A8), in the API and on the screen.
DLQ_SOURCE = "amqp_x_death"
DLQ_REPORTED_BY = "reported by RabbitMQ (the x-death header on the delivery from the dead-letter queue)"

# What each x-death reason means (https://www.rabbitmq.com/docs/dlx).
DEAD_LETTER_REASONS = {
    "rejected": "a consumer refused it with basic.reject or basic.nack with requeue = false",
    "expired": "its time to live (TTL) ran out",
    "maxlen": "the queue was over its length limit (x-max-length)",
    "delivery_limit": "it was returned to the queue more times than x-delivery-limit allows",
}

# The causes of a return to the queue (A10), as the API's ``cause``.
REQUEUED, CHANNEL_CLOSED, CONSUMER_TIMEOUT, CONSUMER_CANCELLED, NOT_OBSERVED = (
    "requeued", "channel_closed", "consumer_timeout", "consumer_cancelled", "not_observed")
# A broker channel close with this text is a consumer timeout (A4.6).
CONSUMER_TIMEOUT_TEXT = "PRECONDITION_FAILED - delivery acknowledgement on channel"
UNKNOWN_TAG_TEXT = "PRECONDITION_FAILED - unknown delivery tag"

# The requeue loop warning (A10): this many basic.nack requeues of one copy. Since 4.3 they never reach
# x-delivery-limit, whose default is 20; 3 is a loop, not a retry.
REQUEUE_LOOP_MIN = 3


def is_rabbitmq(rows) -> bool:
    """Whether these stored rows are a RabbitMQ message. Rows without a message ID (the Postgres version-7
    upgrade reads only kind, queue, time and row of FIFO sends) were stored before RabbitMQ, so are SQS."""
    if not rows:
        return False
    try:
        mid = rows[0]["mid"]
    except (IndexError, KeyError):
        return False
    return system_of(mid) == RABBITMQ


def attributes(r) -> dict:
    try:
        raw = r["attributes"]
    except (IndexError, KeyError):
        return {}
    if isinstance(raw, dict):
        return raw
    try:
        v = json.loads(raw) if raw else {}
    except (TypeError, ValueError):
        return {}
    return v if isinstance(v, dict) else {}


def facts(r) -> dict:
    """A stored row as a dict, with ``amqp`` (its attributes) and ``count_source`` (A7) added."""
    d = {k: r[k] for k in r.keys()}
    a = attributes(r)
    d["amqp"] = a
    d["count_source"] = a.get(COUNT_SOURCE) if d.get("receive_count") is not None else None
    return d


def _int(v) -> Optional[int]:
    if isinstance(v, bool):
        return None
    if isinstance(v, int):
        return v
    if isinstance(v, str) and v.lstrip("-").isdigit():
        return int(v)
    return None


def dead_letter(r) -> Optional[dict]:
    """The x-death copy on a delivery (A8), or None: source queue, reason, count, time and first queue/reason."""
    a = r["amqp"]
    q, reason = a.get("trailbeam.dead_letter.source_queue"), a.get("trailbeam.dead_letter.reason")
    if not isinstance(q, str) or not q or not isinstance(reason, str) or not reason:
        return None
    out = {"source_queue": q, "reason": reason}
    for k in ("count", "time_ms"):
        v = _int(a.get(f"trailbeam.dead_letter.{k}"))
        if v is not None:
            out[k] = v
    for k in ("exchange", "first_queue", "first_reason"):
        v = a.get(f"trailbeam.dead_letter.{k}")
        if isinstance(v, str) and v:
            out[k] = v
    return out


def dlq_source(r) -> Optional[str]:
    """The queue a delivery reports the message was dead-lettered from, when that is another queue (A8)."""
    d = dead_letter(r) if r["kind"] == "receive" else None
    return d["source_queue"] if d is not None and d["source_queue"] != r["queue"] else None


def same_count_source(a, b) -> bool:
    """Whether two deliveries' receive counts can be compared (A9): from the same source, or one is a first
    delivery (count 1, below any other)."""
    sa, sb = a.get("count_source"), b.get("count_source")
    return sa == sb or FIRST_DELIVERY in (sa, sb)


def redelivery_of(p, r) -> bool:
    """Whether delivery ``r`` is a later delivery of the copy ``p`` delivered on the same queue (A10): a higher
    comparable receive count, or with no count, ``redelivered = true``. A second first delivery (count 1, not
    redelivered) is a second message published under the same ID, not a redelivery."""
    if p["receive_count"] is not None and r["receive_count"] is not None and same_count_source(p, r):
        return r["receive_count"] > p["receive_count"]
    return r["receive_count"] is None and r["amqp"].get("trailbeam.amqp.redelivered") is True


def is_consumer_timeout_close(r) -> bool:
    text = r["amqp"].get("trailbeam.amqp.close.reply_text")
    return isinstance(text, str) and text.startswith(CONSUMER_TIMEOUT_TEXT)


def settles_of(rows, p) -> list:
    return [x for x in rows if x["receive_row"] == p["row"] and x["kind"] in
            ("delete", "change_visibility", "reject", "channel_closed")]


def broker_cancel_of(p, cancels) -> Optional[dict]:
    """The broker's basic.cancel of the consumer that got delivery ``p``, at or after it (A4.5), or None."""
    a = p["amqp"]
    tag, channel = a.get(CONSUMER_TAG), a.get(CHANNEL)
    if not tag:
        return None
    for c in cancels:
        ca = attributes(c)
        if (ca.get("trailbeam.amqp.cancelled_by") == "broker" and ca.get(CONSUMER_TAG) == tag
                and ca.get(CHANNEL) == channel and c["queue_url"] == p["queue_url"] and c["time_ns"] >= p["time_ns"]):
            return c
    return None


def cause_of(rows, p, cancels=()) -> tuple[str, Optional[dict], str]:
    """(cause, the span that shows it or None, reason) of the return to the queue after delivery ``p`` (A10)."""
    settles = settles_of(rows, p)
    req = [x for x in settles if x["kind"] == "change_visibility" and x["result"] == "ok"]
    if req:
        x = req[-1]
        op = x["amqp"].get("messaging.operation.name", "basic.nack")
        return REQUEUED, x, (f"The consumer requeued the message with {op} (requeue = true), so RabbitMQ put it "
                             f"back in the queue at once.")
    closed = [x for x in settles if x["kind"] == "channel_closed"]
    if closed:
        x = closed[-1]
        xa = x["amqp"]
        who = xa.get("trailbeam.amqp.close.initiator")
        text = xa.get("trailbeam.amqp.close.reply_text")
        code = xa.get("trailbeam.amqp.close.reply_code")
        said = f" with {code} {text}" if code is not None and text else (f" with {text}" if text else "")
        if who == "broker" and is_consumer_timeout_close(x):
            return CONSUMER_TIMEOUT, x, (f"The consumer did not ack in time: the broker closed the channel{said} "
                                         f"(its consumer_timeout), and RabbitMQ returned every unacked delivery "
                                         f"of that channel to the queue.")
        by = {"client": "the application closed it", "broker": "the broker closed it",
              "network": "the connection was lost"}.get(who, "it closed")
        return CHANNEL_CLOSED, x, (f"The channel of this delivery closed before it was acked ({by}{said}), and "
                                   f"RabbitMQ returned the message to the queue. The close is when the client saw "
                                   f"it; the broker may have returned the message later.")
    if not any(x["kind"] in ("delete", "reject") for x in settles):
        c = broker_cancel_of(p, cancels)
        if c is not None:
            return CONSUMER_CANCELLED, None, (
                "The broker cancelled the consumer that held this delivery (basic.cancel from the broker), with no "
                "ack before. On a quorum queue that is a consumer timeout (consumer_timeout) or a deleted queue; "
                "the cancel does not say which. RabbitMQ returned the unacked delivery to the queue.")
    return NOT_OBSERVED, None, ("RabbitMQ delivered the message again, so it went back to the queue, but no span "
                                "shows why: no requeue, no closed channel and no cancelled consumer was observed.")


def failed_returns(rows, source_queue: str) -> int:
    """The x-delivery-count the copy on ``source_queue`` has reached (A7): its latest delivery's header (0 when
    absent), plus 1 if that delivery was then returned by a failure (a reject with requeue, or its channel
    closing). ``basic.nack`` with requeue does not count (RabbitMQ 4.3 and later)."""
    on = [r for r in rows if r["kind"] == "receive" and r["queue"] == source_queue]
    if not on:
        return 0
    last = on[-1]
    n = _int(last["amqp"].get(DELIVERY_COUNT)) or 0
    for x in settles_of(rows, last):
        if x["kind"] == "channel_closed" or (
                x["kind"] == "change_visibility" and x["amqp"].get("messaging.operation.name") == "basic.reject"):
            return n + 1
    return n


def delivery_limit_reached(count: int, limit: Optional[int]) -> bool:
    """The queue dead-letters (or drops) a message whose x-delivery-count goes above x-delivery-limit."""
    return bool(limit) and count > limit


def copies(rows, moved_from: set) -> dict:
    """queue -> (latest delivery, how the copy ended: "acked", "rejected", "moved" or None while open)."""
    latest: dict = {}
    for r in rows:
        if r["kind"] == "receive":
            latest[r["queue"]] = r
    out = {}
    for q, last in latest.items():
        ended = None
        settles = settles_of(rows, last)
        if any(x["kind"] == "delete" and x["result"] == "ok" for x in settles):
            ended = "acked"
        elif q in moved_from:
            ended = "moved"
        elif any(x["kind"] == "reject" and x["result"] == "ok" for x in settles):
            ended = "rejected"
        out[q] = (last, ended)
    return out


def unroutable(rows) -> bool:
    """Every publish of a message no queue took came back with basic.return, and no delivery was seen."""
    sends = sum(1 for r in rows if r["kind"] == "send")
    returns = sum(1 for r in rows if r["kind"] == "return")
    return returns > 0 and returns >= sends and not any(r["kind"] == "receive" for r in rows)


def done(rows, moved_from: set) -> tuple[bool, bool]:
    """(final, left): ``final`` when every copy was acked or moved to a dead-letter queue (the message's work is
    done: state "deleted"); ``left`` when every copy left its queue (acked, moved or refused), or the message was
    unroutable. A message that ``left`` is never stuck."""
    cs = copies(rows, moved_from)
    if not cs:
        return False, unroutable(rows)
    ends = [e for _, e in cs.values()]
    return all(e in ("acked", "moved") for e in ends), all(e is not None for e in ends)
