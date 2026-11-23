"""What a recorded ``PurgeQueue`` call means for a message (contract section 18, revision 3.3).

A purge span is stored per queue, not per message: SQS does not say which messages it deleted. Whether a purge
applies to a message is worked out when the message is read, from the message's summary (its newest step's end,
``last_ns``, and the queue it is on, ``current_queue``) and the queue's purges (``Store.purges``). The same rule
is used by the timeline, the list pages and the Stuck page's filter in each store.

- **Purged:** a purge that SQS accepted (``result = ok``) on the message's queue started after the message's
  newest step ended (the ``clock`` source of section 9a). If several do, the earliest.
- **Possibly purged:** not purged, and the newest step ended inside a purge's *window*: from the purge's start to
  ``WINDOW_NS`` after its end. SQS says a purge takes up to 60 seconds and may delete messages sent meanwhile.

Only a message with no final delete and no delete inferred from a Lambda outcome can be either (``APPLIES_TO``).
"""
from __future__ import annotations

from typing import Iterable, Mapping, NamedTuple, Optional

# SQS: the purge "takes up to 60 seconds" (PurgeQueue API reference, checked 2026-11-23).
WINDOW_NS = 60 * 1_000_000_000

PURGED = "purged"
POSSIBLY_PURGED = "possibly_purged"

# The states (summary.state / timeline.build) a purge can change: neither deleted nor deleted by Lambda.
APPLIES_TO = ("not_deleted", "processing")

# The purge row's columns, beyond those every stored span has: what Store.purges returns.
COLUMNS = ("row", "trace_id", "span_id", "start_ns", "time_ns", "service_name", "source", "operation", "queue",
           "queue_url", "result", "error_code", "client_id", "pid", "thread_name", "order_scope", "order_issued",
           "order_returned", "attributes")


class Verdict(NamedTuple):
    status: str     # PURGED or POSSIBLY_PURGED
    purge: Mapping  # the purge row it comes from


def _key(p: Mapping) -> tuple:
    return (p["start_ns"], p["time_ns"], p["row"])


def classify(last_ns: int, queue: str, purges: Iterable[Mapping]) -> Optional[Verdict]:
    """The verdict for a message whose newest step ended at ``last_ns`` and that is on ``queue``, or None.
    ``purges``: any purge rows; those of other queues and failed ones are skipped."""
    ok = sorted((p for p in purges if p["queue"] == queue and p["result"] == "ok" and p["start_ns"] is not None),
                key=_key)
    for p in ok:
        if p["start_ns"] > last_ns:
            return Verdict(PURGED, p)
    for p in ok:
        if p["start_ns"] <= last_ns <= p["time_ns"] + WINDOW_NS:
            return Verdict(POSSIBLY_PURGED, p)
    return None


def as_row(p: Mapping) -> dict:
    """A purge row in the shape of a span row (store.py), for the timeline's order: kind ``purge``, no message, and
    a negative ``row`` so it never equals a span's."""
    out = {c: None for c in ("message_id", "receipt_hmac", "key_id", "receive_count", "sent_ts_ms",
                             "first_receive_ts_ms", "dlq_source_arn", "visibility_timeout", "outcome", "error_type",
                             "batch_entry_id", "faas_name", "faas_invocation_id", "mid", "mid_source",
                             "receive_row", "body_dropped", "body_id")}
    out.update({c: p[c] for c in COLUMNS})
    out["kind"] = "purge"
    out["row"] = -int(p["row"])
    return out


def describe(v: Verdict) -> dict:
    """The ``purge`` object of a message in the API (list items and the timeline)."""
    from .timeline import iso  # here, not at the top: timeline imports this module
    p = v.purge
    return {"status": v.status, "queue": p["queue"], "start": iso(p["start_ns"]), "end": iso(p["time_ns"]),
            "window_end": iso(p["time_ns"] + WINDOW_NS),
            "span": {"trace_id": p["trace_id"], "span_id": p["span_id"]}}
