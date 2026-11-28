"""What a recorded ``PurgeQueue`` call means for a message (contract section 18, revision 3.3).

A purge span is stored per queue, not per message: SQS does not say which messages it deleted. Whether a purge
applies to a message is worked out when the message is read, from the message's summary (its newest step's end,
``last_ns``, and the key of the queue it is on, ``current_key``) and the queue's purges (``Store.purges``). The
same rule is used by the timeline, the list pages and the Stuck page's filter in each store.

**Which queue.** A purge is of the queue its span names, by queue key (queues.key: account, region and name), not
by name: two queues called ``orders`` in different accounts or regions are two queues (QV-725). A key that says
less (``orders``, ``000000000000//orders``) is the one queue it fits that says more, when there is exactly one, as
queues.resolve puts it: ``keyed`` gives each purge the stored keys that are its queue (``keys``).

- **Purged:** a purge that SQS accepted (``result = ok``) on the message's queue started after the message's
  newest step ended (the ``clock`` source of section 9a). If several do, the earliest.
- **Possibly purged:** not purged, and the newest step ended inside a purge's *window*: from the purge's start to
  ``WINDOW_NS`` after its end. SQS says a purge takes up to 60 seconds and may delete messages sent meanwhile.

Only a message with no final delete and no delete inferred from a Lambda outcome can be either (``APPLIES_TO``).
"""
from __future__ import annotations

from typing import Iterable, Mapping, NamedTuple, Optional

from . import queues

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


def key_of(p: Mapping) -> str:
    """The key of the queue a purge row names (queues.key)."""
    return queues.key(p["queue"], p["queue_url"])


def _keys(p: Mapping) -> frozenset:
    """The queue keys a purge is of: those ``keyed`` gave it, else its own."""
    if isinstance(p, dict) and "keys" in p:
        return p["keys"]
    return frozenset((key_of(p),))


def keyed(db, purges: Iterable[Mapping], keys: Iterable[str] = ()) -> list:
    """``purges`` as dicts, each with ``keys``: every stored queue key that is the queue it names (module
    docstring). ``keys``: more keys to place, those of the messages being read. A store's queue keys are read only
    when there is a purge SQS accepted; a store without them (Store.queue_keys) places the given keys alone."""
    purges = [dict(p) for p in purges]
    ok = [p for p in purges if p["result"] == "ok"]
    if not ok:
        return purges
    names = {p["queue"] for p in ok}
    try:
        stored = db.queue_keys()
    except NotImplementedError:
        stored = []
    known = set(keys) | {key_of(p) for p in ok} | set(stored)
    # only keys of these names can be one of these queues, so _top works over few keys
    places = {k: pl for k, pl in ((k, queues.place_of_key(k)) for k in known) if pl.name in names}
    top = {k: queues._top(k, places) or k for k in places}
    for p in purges:
        own = top.get(key_of(p), key_of(p))
        p["keys"] = frozenset(k for k, t in top.items() if t == own) | {key_of(p)}
    return purges


def read(db, names: Iterable[str], keys: Iterable[str] = ()) -> list:
    """The purges of the queues called ``names`` (Store.purges), ``keyed``: what a message page or a list page
    reads. ``keys``: the queue keys of the messages read."""
    return keyed(db, db.purges(set(names)), keys)


def stuck_starts(db, name: Optional[str] = None) -> list:
    """For the Stuck list's filter in each store: (queue key, the latest start of a purge SQS accepted on it), for
    every purge, or those of queues called ``name``. A message whose ``current_key`` is one of them and whose
    ``last_ns`` is before that start is purged (``classify``: some purge started after its newest step). Few:
    one per purged queue."""
    latest: dict = {}
    for p in keyed(db, db.purges(None if name is None else {name})):
        if p["result"] != "ok" or p["start_ns"] is None:
            continue
        for k in p["keys"]:
            latest[k] = max(latest.get(k, p["start_ns"]), p["start_ns"])
    return sorted(latest.items())


def classify(last_ns: int, queue_key: str, purges: Iterable[Mapping]) -> Optional[Verdict]:
    """The verdict for a message whose newest step ended at ``last_ns`` and that is on the queue whose key is
    ``queue_key`` (queues.key), or None. ``purges``: any purge rows, best ``keyed``; those of other queues and
    failed ones are skipped."""
    ok = sorted((p for p in purges if p["result"] == "ok" and p["start_ns"] is not None and queue_key in _keys(p)),
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
