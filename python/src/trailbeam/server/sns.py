"""SNS fan-out (contract section 20, revision 3.4): which recorded publishes a message's receives join.

A ``trailbeam publish`` span (kind ``publish``) is stored apart from messages, like a purge: one SNS publish reaches
each subscribed queue as a different SQS message, so it belongs to none of them. Its row has the SNS message ID in
``message_id``, the topic name in ``queue`` and every attribute in ``attributes``; its ``mid`` is always NULL.

A receive **joins** a publish (section 20.5) through one of two keys, read from the receive's attributes:

- the **envelope key** ``(trailbeam.sns.topic_arn, trailbeam.sns.message_id)``, equal to the publish's
  ``(aws.sns.topic.arn, messaging.message.id)``: raw delivery off;
- the **publish ID** ``trailbeam.sns.publish_id``, equal to the publish's: raw delivery on. Not used for a message
  that has a ``send`` span: SNS makes no send a package could see, so that message's publish ID was copied by a
  process that re-sent it.

What a join shows is worked out when the message is read (queries.message): the timeline gets the joined
publishes as observed steps, each with the SQS messages it reached; a message whose receives name a topic but join
no publish gets "Arrived via SNS" (timeline.py).
"""
from __future__ import annotations

import json
import re
from typing import Iterable, Mapping, NamedTuple, Optional

KIND = "publish"
OPERATIONS = ("Publish", "PublishBatch")

# On the publish span (20.1)
TOPIC_ARN = "aws.sns.topic.arn"
PUBLISH_ID = "trailbeam.sns.publish_id"  # on the publish span and on the receive (20.3)
PUBLISH_ID_SKIPPED = "trailbeam.sns.publish_id_skipped"
# On the receive (20.3)
ENVELOPE_MESSAGE_ID = "trailbeam.sns.message_id"
ENVELOPE_TOPIC_ARN = "trailbeam.sns.topic_arn"
ENVELOPE_TIMESTAMP_MS = "trailbeam.sns.timestamp_ms"

# How a receive joined a publish, in the API (``joined_by``).
BY_ENVELOPE = "envelope_message_id"
BY_PUBLISH_ID = "publish_id"

# The publish row's columns: what Store.publishes returns (purge.COLUMNS, plus the message ID and batch entry).
COLUMNS = ("row", "trace_id", "span_id", "start_ns", "time_ns", "service_name", "source", "operation", "queue",
           "message_id", "batch_entry_id", "client_id", "pid", "thread_name", "order_scope", "order_issued",
           "order_returned", "attributes")

# The packages' checks (20.3), applied again here: a key the server joins on is one a package could have written.
_PUBLISH_ID_RE = re.compile(r"[A-Za-z0-9-]{1,64}")
_MESSAGE_ID_RE = re.compile(r"[A-Za-z0-9-]{1,100}")
_TOPIC_ARN_RE = re.compile(r"arn:[A-Za-z0-9-]+:sns:[A-Za-z0-9-]+:[0-9]{12}:[A-Za-z0-9_.-]{1,256}")

# A message's receives can join many publishes only by accident (a re-used publish ID); a publish can reach many
# queues. Both lists are capped so one message's timeline stays one bounded read.
MAX_PUBLISHES = 20
MAX_REACHED = 100


class Keys(NamedTuple):
    envelope: frozenset  # {(topic_arn, sns_message_id)}
    publish_ids: frozenset

    def __bool__(self) -> bool:
        return bool(self.envelope or self.publish_ids)


NO_KEYS = Keys(frozenset(), frozenset())


def valid_topic_arn(v) -> bool:
    return isinstance(v, str) and _TOPIC_ARN_RE.fullmatch(v) is not None


def topic_name(arn: str) -> str:
    return arn.rsplit(":", 1)[-1]


def attributes(r) -> dict:
    """A stored row's span attributes (``attributes``, JSON), or {}."""
    try:
        raw = r["attributes"]
    except (IndexError, KeyError):
        return {}
    if isinstance(raw, Mapping):
        return dict(raw)
    try:
        v = json.loads(raw) if raw else {}
    except (TypeError, ValueError):
        return {}
    return v if isinstance(v, dict) else {}


def _publish_id(v) -> Optional[str]:
    return v if isinstance(v, str) and _PUBLISH_ID_RE.fullmatch(v) else None


def receive_keys(r) -> tuple[Optional[tuple], Optional[str]]:
    """(envelope key or None, publish ID or None) of one receive row."""
    a = attributes(r)
    mid, arn = a.get(ENVELOPE_MESSAGE_ID), a.get(ENVELOPE_TOPIC_ARN)
    env = (arn, mid) if isinstance(mid, str) and _MESSAGE_ID_RE.fullmatch(mid) and valid_topic_arn(arn) else None
    return env, _publish_id(a.get(PUBLISH_ID))


def publish_keys(p) -> tuple[Optional[tuple], Optional[str]]:
    """(envelope key, publish ID or None) of one publish row."""
    a = attributes(p)
    arn = a.get(TOPIC_ARN)
    env = (arn, p["message_id"]) if valid_topic_arn(arn) and p["message_id"] else None
    return env, _publish_id(a.get(PUBLISH_ID))


def message_keys(rows: Iterable) -> Keys:
    """The keys a message's receives join publishes by. No publish IDs for a message with a send (20.5)."""
    rows = list(rows)
    env, pids = set(), set()
    for r in rows:
        if r["kind"] != "receive":
            continue
        e, p = receive_keys(r)
        if e is not None:
            env.add(e)
        if p is not None:
            pids.add(p)
    if any(r["kind"] == "send" for r in rows):
        pids = set()
    return Keys(frozenset(env), frozenset(pids))


def keys_of_publish(p) -> Keys:
    e, pid = publish_keys(p)
    return Keys(frozenset([e] if e else []), frozenset([pid] if pid else []))


def join_of(keys: Keys, p) -> list[str]:
    """How a message with ``keys`` joins publish row ``p``: a list of BY_ENVELOPE / BY_PUBLISH_ID, empty if not."""
    e, pid = publish_keys(p)
    out = []
    if e is not None and e in keys.envelope:
        out.append(BY_ENVELOPE)
    if pid is not None and pid in keys.publish_ids:
        out.append(BY_PUBLISH_ID)
    return out


def joined(keys: Keys, publishes: Iterable) -> list[tuple[Mapping, list[str]]]:
    """The publishes a message with ``keys`` joins, each with how, by (time_ns, row). ``publishes``: any rows."""
    out = [(p, j) for p in publishes if (j := join_of(keys, p))]
    return sorted(out, key=lambda x: (x[0]["time_ns"], x[0]["row"]))[:MAX_PUBLISHES]


def envelope_timestamp_ms(r) -> Optional[int]:
    v = attributes(r).get(ENVELOPE_TIMESTAMP_MS)
    if isinstance(v, str) and v.lstrip("-").isdigit():
        v = int(v)
    return v if isinstance(v, int) and not isinstance(v, bool) and v >= 0 else None


def delivery_ms(receives: list) -> Optional[int]:
    """SNS delivery time (20.5): the first receive's SentTimestamp minus its envelope Timestamp, in ms, both on AWS
    clocks; None without both. ``receives``: the message's receives, in time order."""
    if not receives:
        return None
    first = min(receives, key=lambda r: (r["receive_count"] or 0, r["time_ns"], r["row"]))
    ts = envelope_timestamp_ms(first)
    if ts is None or first["sent_ts_ms"] is None:
        return None
    return first["sent_ts_ms"] - ts


def arrived_from(receives: list) -> Optional[tuple[Mapping, str]]:
    """(the earliest receive carrying an envelope topic, that topic's ARN), or None."""
    for r in sorted(receives, key=lambda r: (r["time_ns"], r["row"])):
        e, _ = receive_keys(r)
        if e is not None:
            return r, e[0]
    return None


def as_row(p: Mapping) -> dict:
    """A publish row in the shape of a span row (store.py), for the timeline's order: kind ``publish``, no message,
    and a ``row`` below every span's and purge's (-(2^40) - row), so it never equals another step's."""
    out = {c: None for c in ("queue_url", "receipt_hmac", "key_id", "receive_count", "sent_ts_ms",
                             "first_receive_ts_ms", "dlq_source_arn", "result", "error_code", "visibility_timeout",
                             "outcome", "error_type", "faas_name", "faas_invocation_id", "mid", "mid_source",
                             "receive_row", "body_dropped", "body_id")}
    out.update({c: p[c] for c in COLUMNS})
    out["kind"] = KIND
    out["row"] = -(1 << 40) - int(p["row"])
    return out
