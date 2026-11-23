"""Per-message summaries: what the list pages show, worked out once per message when its spans change.

A Store keeps one summary per message (``summarize``), next to its spans, and replaces it whenever a span of
the message is stored, joined or deleted. The list pages (Messages, one queue, one tenant, Duplicates, Stuck,
Tenants and tenant search) read summaries with indexed range scans; only the message detail page builds a
full timeline (timeline.py). Like the timeline, this is plain Python over ``Store.message_rows``, so the
rules exist once whatever the backend.

A summary says what ``timeline.build`` would say about the message, except the three things that depend on
when and how it is asked: ``state`` "processing" (a Lambda receive with no outcome, for at most the Lambda
timeout), ``dead_letter_queue_move`` "not_seen_on_dlq_yet" (needs the configured maxReceiveCount), and whether
a PurgeQueue call removed it (``state`` "purged" and ``purge``: purge.py, from the queue's purges, which can
arrive after the message's spans). ``item`` adds those from the ``lambda_wait_ns``, ``source_receive_count``,
``last_ns`` and ``current_queue`` the summary keeps.
tests/server/test_summary.py checks ``item(summarize(rows))`` against the timeline on every dataset.

The fields (every one is a column of the SQLite ``messages`` table, migrations.CREATE):

- ``mid``; ``first_ns`` and ``first_clock`` (timeline.first_seen_ns); ``last_ns``, the newest span's time.
- ``source_queue``, ``current_queue``, ``in_dlq`` and ``moved`` (an observed move to a dead-letter queue,
  contract 9b), ``source_receive_count`` (the highest receive count on the source queue, 0 if none).
- ``deliveries``, ``successes`` (timeline.successful_deliveries), ``consumers`` (JSON: the receives'
  [service_name, client_id] pairs, sorted as the list pages show them).
- ``final_delete`` (an accepted delete of the latest receive's handle), ``lambda_ok`` (the latest receive is
  a Lambda's and its invocation succeeded), ``lambda_wait_ns`` (the latest receive is a Lambda's with no
  outcome: its time, else NULL).
- ``tenant``: the message's tenant (contract/tenants.md), NULL if none.
- Duplicates: ``dup_received`` (more than one receive), ``dup_succeeded`` (more than one success: accepted
  deletes counted once per delivery, plus distinct invocation IDs with a ``success`` outcome).
- Stuck: ``stuck_first_ns`` and ``stuck_sqs_clock`` (first seen as Store.stuck_messages defines it; NULL for a
  message with neither a send nor a receive, which is never stuck), ``stuck_open`` (no final delete and no
  success for the latest receive's invocation).

And three lists, kept in their own tables so they can be indexed:

- ``queues``: every queue the message has a span on;
- ``tenants``: {tenant: the newest time a span of the message named it} (the tenant filter and search);
- ``tenant_events``: what the message adds to the Tenants page, as (time in ms, kind, time in queue in ms):
  ``sent`` per send span, ``first_delivery`` for a first delivery with a time in queue, ``dead_lettered``
  per observed move to a dead-letter queue, at the receive that reported it (contract/tenants.md), and
  ``dead_lettered_again`` for each move after the first, with the time since the move before it in place of a
  time in queue: the hourly rollup (rollup.py) needs it to count dead-lettered messages, not moves (QV-128).
"""
from __future__ import annotations

import json
from typing import Any, Mapping, Optional

from . import purge, timeline
from .store import TENANT_ATTR, TENANT_KINDS, TENANT_MAX_CHARS

Summary = Mapping[str, Any]

# the columns of a summary, in the order of the messages table
FIELDS = ("mid", "first_ns", "first_clock", "last_ns", "source_queue", "current_queue", "in_dlq", "moved",
          "source_receive_count", "deliveries", "successes", "consumers", "final_delete", "lambda_ok",
          "lambda_wait_ns", "tenant", "dup_received", "dup_succeeded", "stuck_first_ns", "stuck_sqs_clock",
          "stuck_open")

SENT, FIRST_DELIVERY, DEAD_LETTERED, DEAD_LETTERED_AGAIN = "sent", "first_delivery", "dead_lettered", "dead_lettered_again"
_NS_PER_MS = 1_000_000


def span_tenant(row) -> Optional[str]:
    """The tenant a stored span names, or None: only send, receive and outcome spans name one."""
    if row["kind"] not in TENANT_KINDS:
        return None
    v = json.loads(row["attributes"]).get(TENANT_ATTR)
    return v[:TENANT_MAX_CHARS] if isinstance(v, str) and v else None


def message_tenant(rows) -> Optional[str]:
    """A message's tenant: its earliest send's, else its earliest receive's or outcome's."""
    ordered = sorted(rows, key=lambda r: (r["kind"] != "send", r["time_ns"], r["trace_id"], r["span_id"]))
    return next((t for t in map(span_tenant, ordered) if t is not None), None)


def summarize(rows) -> dict:
    """The summary of one message from every row of it (Store.message_rows), any order."""
    rows = sorted(rows, key=lambda r: (r["time_ns"], r["row"]))
    receives = [r for r in rows if r["kind"] == "receive"]
    sends = [r for r in rows if r["kind"] == "send"]
    latest = receives[-1] if receives else None
    source_queue = sends[0]["queue"] if sends else (receives[0]["queue"] if receives else rows[0]["queue"])
    current_queue = latest["queue"] if latest else source_queue
    outcomes = {r["faas_invocation_id"]: r for r in rows if r["kind"] == "outcome"}  # as timeline.build

    # observed moves to a dead-letter queue (timeline._dlq_moves): (source, DLQ) -> the receive reporting it
    reports: dict = {}
    for r in receives:
        if r["dlq_source_arn"]:
            frm = r["dlq_source_arn"].rsplit(":", 1)[-1]
            if frm != r["queue"]:
                key = (r["receive_count"], r["time_ns"], r["row"])
                if (frm, r["queue"]) not in reports or key < reports[(frm, r["queue"])][0]:
                    reports[(frm, r["queue"])] = (key, r)
    dlqs = {q for _, q in reports}

    final_delete = latest is not None and any(
        r["kind"] == "delete" and r["result"] == "ok" and r["receive_row"] == latest["row"] for r in rows)
    lambda_ok = (latest is not None and latest["source"] == "lambda"
                 and outcomes.get(latest["faas_invocation_id"]) is not None
                 and outcomes[latest["faas_invocation_id"]]["outcome"] == "success")
    lambda_wait = (latest["time_ns"] if latest is not None and latest["source"] == "lambda"
                   and latest["faas_invocation_id"] not in outcomes else None)
    consumers = sorted({(r["service_name"], r["client_id"]) for r in receives}, key=str)
    first_ns, first_clock = timeline.first_seen_ns(rows)

    # Duplicates and Stuck, as Store.duplicate_messages and Store.stuck_messages define them
    ok_deliveries = {r["receive_row"] for r in rows if r["kind"] == "delete" and r["result"] == "ok"
                     and r["receive_row"] is not None}
    ok_invocations = {r["faas_invocation_id"] for r in rows if r["kind"] == "outcome" and r["outcome"] == "success"
                      and r["faas_invocation_id"] is not None}
    sr = [r for r in rows if r["kind"] in ("send", "receive")]
    if sr:
        sqs = [r["sent_ts_ms"] for r in receives if r["sent_ts_ms"] is not None]
        if sends:
            stuck_first, on_sqs = min(r["time_ns"] for r in sends), False
        elif sqs:
            stuck_first, on_sqs = min(sqs) * _NS_PER_MS, True
        else:
            stuck_first, on_sqs = min(r["time_ns"] for r in sr), False
    else:
        stuck_first, on_sqs = None, False
    latest_inv = latest["faas_invocation_id"] if latest is not None else None
    stuck_open = not final_delete and not (latest_inv is not None and latest_inv in ok_invocations)

    named: dict = {}
    for r in rows:
        t = span_tenant(r)
        if t is not None:
            named[t] = max(named.get(t, r["time_ns"]), r["time_ns"])

    events = [(r["time_ns"] // _NS_PER_MS, SENT, None) for r in sends]
    if receives and receives[0]["receive_count"] == 1:
        first = receives[0]
        at = first["time_ns"] // _NS_PER_MS
        sent_at = sends[0]["time_ns"] // _NS_PER_MS if sends else first["sent_ts_ms"]
        if isinstance(sent_at, int):
            events.append((at, FIRST_DELIVERY, max(0, at - sent_at)))
    moves = sorted((r["time_ns"], r["row"]) for _, r in reports.values())
    events += [(t // _NS_PER_MS, DEAD_LETTERED, None) for t, _ in moves]
    # every move after the first, with the time since the one before it: the rollup counts distinct messages
    events += [(b // _NS_PER_MS, DEAD_LETTERED_AGAIN, b // _NS_PER_MS - a // _NS_PER_MS)
               for (a, _), (b, _) in zip(moves, moves[1:])]

    return {
        "mid": rows[0]["mid"],
        "first_ns": first_ns,
        "first_clock": first_clock,
        "last_ns": rows[-1]["time_ns"],
        "source_queue": source_queue,
        "current_queue": current_queue,
        "in_dlq": current_queue in dlqs,
        "moved": bool(reports),
        "source_receive_count": max((r["receive_count"] for r in receives if r["queue"] == source_queue), default=0),
        "deliveries": len(receives),
        "successes": timeline.successful_deliveries(rows),
        "consumers": json.dumps(consumers),
        "final_delete": final_delete,
        "lambda_ok": lambda_ok,
        "lambda_wait_ns": lambda_wait,
        "tenant": message_tenant(rows),
        "dup_received": len(receives) > 1,
        "dup_succeeded": len(ok_deliveries) + len(ok_invocations) > 1,
        "stuck_first_ns": stuck_first,
        "stuck_sqs_clock": on_sqs,
        "stuck_open": stuck_open,
        "queues": sorted({r["queue"] for r in rows}),
        "tenants": named,
        "tenant_events": events,
    }


def state(s: Summary, now_ns: int, lambda_timeout_s: int, purges=()) -> str:
    """timeline.build's ``state``. ``purges``: purge rows (Store.purges) of the message's queue, if any."""
    if s["final_delete"]:
        return "deleted"
    if s["lambda_ok"]:
        return "deleted_inferred"
    v = purge.classify(s["last_ns"], s["current_queue"], purges)
    if v is not None and v.status == purge.PURGED:
        return purge.PURGED
    if s["lambda_wait_ns"] is not None and now_ns - s["lambda_wait_ns"] <= lambda_timeout_s * 1_000_000_000:
        return "processing"
    return "not_deleted"


def item(s: Summary, now_ns: int, lambda_timeout_s: int, max_receive_counts: Optional[dict] = None,
         purges=()) -> dict:
    """A message in a list page, as the API returns it: the fields timeline.build gives, from the summary.
    ``purges``: purge rows (Store.purges) that include those of the message's ``current_queue``."""
    st = state(s, now_ns, lambda_timeout_s, purges)
    mrc = (max_receive_counts or {}).get(s["source_queue"])
    if s["moved"]:
        dlq_move = "observed"
    elif mrc and s["source_receive_count"] >= mrc and st not in ("deleted", "deleted_inferred", purge.PURGED):
        dlq_move = "not_seen_on_dlq_yet"
    else:
        dlq_move = None
    return {
        "message_id": s["mid"],
        "state": st,
        "source_queue": s["source_queue"],
        "current_queue": s["current_queue"],
        "in_dead_letter_queue": bool(s["in_dlq"]),
        "dead_letter_queue_move": dlq_move,
        "deliveries": s["deliveries"],
        "successful_deliveries": s["successes"],
        "consumers": [{"service_name": a, "client_id": b} for a, b in json.loads(s["consumers"])],
        "first_seen": timeline.iso(s["first_ns"]),
        "first_seen_clock": s["first_clock"],
        "last_seen": timeline.iso(s["last_ns"]),
        **purge_of(s, st, purges),
    }


def purge_of(s: Summary, st: str, purges) -> dict:
    """``{"purge": purge.describe(...)}`` for a message in state ``st`` that a purge applies to, else ``{}``: the
    field is left out when there is no purge to tell of."""
    if st != purge.PURGED and st not in purge.APPLIES_TO:
        return {}
    v = purge.classify(s["last_ns"], s["current_queue"], purges)
    return {} if v is None else {"purge": purge.describe(v)}
