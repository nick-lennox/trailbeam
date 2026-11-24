"""EventBridge PutEvents (contract section 25, revision 3.6): which recorded PutEvents entries a message's receives join.

A ``trailbeam put_event`` span (kind ``put_event``) is one entry of a PutEvents call, accepted or failed. It is stored
apart from messages, like an SNS publish (sns.py): one event can match several rules and reach several queues, each as
a different SQS message, so it belongs to none of them. Its row has the event bus's name in ``queue``, the ``EventId``
in ``message_id`` (NULL for a failed entry) and every attribute in ``attributes``; its ``mid`` is always NULL.

A receive **joins** an entry (section 25.3) when its ``trailbeam.eventbridge.event_id`` (section 23.1, read by
origin.py) equals the entry's ``message_id`` and the entry's result is ``ok``. Nothing else is a key: the event ID is
a UUID EventBridge makes, and the board's QV-314 decision (section 23.5) is what lets a receive read it on every queue.

What a join shows is worked out when the message is read (queries.message): each joined entry is an observed step
with the SQS messages it reached, and it replaces section 23.3's inferred "Arrived via an EventBridge rule" step.
"""
from __future__ import annotations

import re
from typing import Iterable, Mapping, Optional

from . import origin as origin_mod
from . import sns as sns_mod

KIND = "put_event"
OPERATION = "PutEvents"

ENTRY_INDEX = "trailbeam.eventbridge.entry_index"
EVENT_BUS = "trailbeam.eventbridge.event_bus"
SOURCE = "trailbeam.eventbridge.source"
DETAIL_TYPE = "trailbeam.eventbridge.detail_type"
TIME_GIVEN = "trailbeam.eventbridge.time_given"

BY_EVENT_ID = "event_id"  # how a receive joined an entry, in the API (``joined_by``)

# The row's columns: an SNS publish's (sns.COLUMNS; ``batch_entry_id`` always NULL), with the entry's result.
COLUMNS = sns_mod.COLUMNS + ("result", "error_code")

# The packages' checks (25.1), applied again: a key the server joins on is one a package could have written.
EVENT_ID_RE = re.compile(r"[A-Za-z0-9-]{1,64}")
BUS_RE = re.compile(r"[.\-_A-Za-z0-9]{1,256}")

# Caps, as for SNS: a message joins several entries only by accident (a body re-sent with a reused ID).
MAX_ENTRIES = sns_mod.MAX_PUBLISHES
MAX_REACHED = sns_mod.MAX_REACHED


def valid_event_id(v) -> bool:
    return isinstance(v, str) and EVENT_ID_RE.fullmatch(v) is not None


def valid_bus(v) -> bool:
    return isinstance(v, str) and BUS_RE.fullmatch(v) is not None


def receive_event_id(r) -> Optional[str]:
    """The event ID one receive row joins entries by, or None: only a body that matched section 23.1 as a whole."""
    o = origin_mod.origin(r)
    if o is None or o["kind"] != origin_mod.EVENTBRIDGE:
        return None
    eid = o["event_id"]
    return eid if valid_event_id(eid) else None


def message_event_ids(rows: Iterable) -> frozenset:
    """The event IDs a message's receives join entries by."""
    return frozenset(e for r in rows if r["kind"] == "receive" and (e := receive_event_id(r)) is not None)


def joins(event_ids, p) -> bool:
    """Whether a message with ``event_ids`` joins entry row ``p``: an accepted entry with one of those IDs."""
    return p["result"] == "ok" and p["message_id"] in event_ids


def joined(event_ids, entries: Iterable) -> list[Mapping]:
    """The entries a message with ``event_ids`` joins, by (time_ns, row). ``entries``: any rows."""
    out = [p for p in entries if joins(event_ids, p)]
    return sorted(out, key=lambda p: (p["time_ns"], p["row"]))[:MAX_ENTRIES]


def delivery_ms(receives: list) -> Optional[int]:
    """EventBridge delivery time (25.3): the first receive's SentTimestamp minus the event's ``time``, in ms, both
    on AWS clocks unless the sender gave the entry a Time; None without both. ``receives``: the message's receives."""
    first = None
    for r in sorted(receives, key=lambda r: (r["receive_count"] or 0, r["time_ns"], r["row"])):
        if receive_event_id(r) is not None:
            first = r
            break
    if first is None or first["sent_ts_ms"] is None:
        return None
    ts = origin_mod.origin(first)["event_time_ms"]
    return None if ts is None else first["sent_ts_ms"] - ts


def event_time_ms(receives: list) -> Optional[int]:
    for r in sorted(receives, key=lambda r: (r["time_ns"], r["row"])):
        if receive_event_id(r) is not None:
            return origin_mod.origin(r)["event_time_ms"]
    return None


def as_row(p: Mapping) -> dict:
    """An entry row in the shape of a span row (store.py), for the timeline's order: kind ``put_event``, no message,
    and a ``row`` below every span's, purge's and SNS publish's (-(2^42) - row), so it never equals another step's."""
    out = sns_mod.as_row(p)
    out.update(kind=KIND, result=p["result"], error_code=p["error_code"])
    out["row"] = -(1 << 42) - int(p["row"])
    return out
