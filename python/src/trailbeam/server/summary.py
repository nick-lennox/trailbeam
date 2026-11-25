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
  contract 9b; on RabbitMQ from ``x-death``), ``source_receive_count`` (the highest receive count on the source
  queue, 0 if none; on RabbitMQ the x-delivery-count its copy reached, rabbitmq.failed_returns, which
  x-delivery-limit is checked against).
- ``deliveries``, ``successes`` (timeline.successful_deliveries), ``consumers`` (JSON: the receives'
  [service_name, client_id, service_version] triples, sorted as the list pages show them; a summary written
  before schema 16 (QV-362) has [service_name, client_id] pairs, read as a version of null).
- ``final_delete`` (an accepted delete of the latest receive's handle), ``lambda_ok`` (the latest receive is
  a Lambda's and its invocation succeeded), ``lambda_wait_ns`` (the latest receive is a Lambda's with no
  outcome: its time, else NULL).
- ``tenant``: the message's tenant (contract/tenants.md), NULL if none.
- Duplicates: ``dup_received`` (more than one receive), ``dup_succeeded`` (more than one success: accepted
  deletes counted once per delivery, plus distinct invocation IDs with a ``success`` outcome). A RabbitMQ message
  (systems.py) is counted per queue copy, its fan-out not being a duplicate: more than one delivery, or more than
  one accepted ack, on one queue; ``final_delete`` holds when every copy was acked or moved to a dead-letter queue,
  and ``stuck_open`` when a copy is still open (rabbitmq.done: a refused copy, or an unroutable message, is not).
- Stuck: ``stuck_first_ns`` and ``stuck_sqs_clock`` (first seen as Store.stuck_messages defines it; NULL for a
  message with neither a send nor a receive, which is never stuck), ``stuck_open`` (no final delete and no
  success for the latest receive's invocation).

And these lists, kept in their own tables so they can be indexed:

- ``queues``: every queue the message has a span on;
- ``tenants``: {tenant: the newest time a span of the message named it} (the tenant filter and search);
- ``tenant_events``: what the message adds to the Tenants page, as (time in ms, kind, time in queue in ms):
  ``sent`` per send span but those SQS deduplicated (timeline.deduplicated_sends), ``first_delivery`` for a first delivery with a time in queue, ``dead_lettered``
  per observed move to a dead-letter queue, at the receive that reported it (contract/tenants.md), and
  ``dead_lettered_again`` for each move after the first, with the time since the move before it in place of a
  time in queue: the hourly rollup (rollup.py) needs it to count dead-lettered messages, not moves (QV-128);
- ``queue_events``: what the message adds to each queue's page (queues.py), as (time in ms, kind, queue key,
  service, lambda, value) (QV-363).
"""
from __future__ import annotations

import json
from typing import Any, Mapping, Optional

from . import purge, queues, rabbitmq, systems, timeline
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
    amqp = rabbitmq.is_rabbitmq(rows)
    if amqp:
        rows = [rabbitmq.facts(r) for r in rows]
    receives = [r for r in rows if r["kind"] == "receive"]
    sends = [r for r in rows if r["kind"] == "send"]
    latest = receives[-1] if receives else None
    source_queue = timeline.source_queue_of(rows[0]["mid"], sends, receives, rows[0]["queue"])
    current_queue = latest["queue"] if latest else source_queue
    outcomes = {r["faas_invocation_id"]: r for r in rows if r["kind"] == "outcome"}  # as timeline.build

    # observed moves to a dead-letter queue (timeline._dlq_moves): (source, DLQ) -> the receive reporting it
    reports: dict = {}
    # RabbitMQ keys both queues as copies (rabbitmq.copy_of): (virtual host, queue)
    for r in receives:
        if amqp:
            frm, here = rabbitmq.dlq_source_copy(r), rabbitmq.copy_of(r)
        else:
            frm, here = (r["dlq_source_arn"].rsplit(":", 1)[-1] if r["dlq_source_arn"] else None), r["queue"]
        if frm is not None:
            if frm != here:
                key = (r["receive_count"] or 0, r["time_ns"], r["row"])
                if (frm, here) not in reports or key < reports[(frm, here)][0]:
                    reports[(frm, here)] = (key, r)
    if amqp:  # an x-death naming a queue whose latest delivery is held or acked is not a move (A8)
        reports = {k: v for k, v in reports.items() if rabbitmq.move_ruled_out(rows, k[0]) is None}
    dlqs = {q for _, q in reports}
    in_dlq = (rabbitmq.copy_of(latest) if latest is not None else None) in dlqs if amqp else current_queue in dlqs

    final_delete = latest is not None and any(
        r["kind"] == "delete" and r["result"] == "ok" and r["receive_row"] == latest["row"] for r in rows)
    lambda_ok = (latest is not None and latest["source"] == "lambda"
                 and outcomes.get(latest["faas_invocation_id"]) is not None
                 and outcomes[latest["faas_invocation_id"]]["outcome"] == "success")
    lambda_wait = (latest["time_ns"] if latest is not None and latest["source"] == "lambda"
                   and latest["faas_invocation_id"] not in outcomes else None)
    consumers = sorted({(r["service_name"], r["client_id"], timeline.span_version(r)) for r in receives}, key=str)
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
    dup_received = len(receives) > 1
    dup_succeeded = len(ok_deliveries) + len(ok_invocations) > 1
    if systems.system_of(rows[0]["mid"]) == systems.RABBITMQ:
        # One copy per queue (contract/span-contract-amqp.md A6): a fanout publish to two queues is one message
        # with two copies, not a duplicate. Duplicates and Stuck are per copy: received more than once on one
        # queue, acked more than once on one queue, or a copy whose latest delivery has no accepted ack. A queue
        # is (virtual host, queue) (A3): one ID used in two virtual hosts is two copies too.
        copies: dict = {}
        for r in receives:
            copies.setdefault(rabbitmq.copy_of(r), []).append(r)
        ok_by_queue: dict = {}
        by_row = {r["row"]: r for r in receives}
        for d in ok_deliveries:
            if d in by_row:
                ok_by_queue.setdefault(rabbitmq.copy_of(by_row[d]), set()).add(d)
        dup_received = any(len(c) > 1 for c in copies.values())
        dup_succeeded = any(len(c) > 1 for c in ok_by_queue.values())
        # Done and stuck per copy (rabbitmq.done): every copy acked or moved to a dead-letter queue is the
        # timeline's "deleted"; a copy refused with requeue = false, or an unroutable message, left its queue and is
        # not stuck.
        final_delete, left = rabbitmq.done(rows, {frm for frm, _ in reports})
        stuck_open = not left

    named: dict = {}
    for r in rows:
        t = span_tenant(r)
        if t is not None:
            named[t] = max(named.get(t, r["time_ns"]), r["time_ns"])

    deduplicated = timeline.deduplicated_sends(rows)  # enqueued nothing: not a message sent (QV-246)
    events = [(r["time_ns"] // _NS_PER_MS, SENT, None) for r in sends if r["row"] not in deduplicated]
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
        "in_dlq": in_dlq,
        "moved": bool(reports),
        "source_receive_count": (rabbitmq.failed_returns(rows, rabbitmq.copy_of(receives[0]) if receives else None)
                                 if amqp else
                                 max((r["receive_count"] or 0 for r in receives if r["queue"] == source_queue), default=0)),
        "deliveries": len(receives),
        "successes": timeline.successful_deliveries(rows),
        "consumers": json.dumps(consumers),
        "final_delete": final_delete,
        "lambda_ok": lambda_ok,
        "lambda_wait_ns": lambda_wait,
        "tenant": message_tenant(rows),
        "dup_received": dup_received,
        "dup_succeeded": dup_succeeded,
        "stuck_first_ns": stuck_first,
        "stuck_sqs_clock": on_sqs,
        "stuck_open": stuck_open,
        "queues": sorted({r["queue"] for r in rows}),
        "tenants": named,
        "tenant_events": events,
        "queue_events": queues.events(rows, deduplicated),
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
    elif systems.system_of(s["mid"]) == systems.RABBITMQ:
        # mrc is the queue's x-delivery-limit, checked against the x-delivery-count its copy reached (rabbitmq.py)
        dlq_move = "not_seen_on_dlq_yet" if (rabbitmq.delivery_limit_reached(s["source_receive_count"], mrc)
                                            and st != "deleted") else None
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
        "consumers": [{"service_name": c[0], "client_id": c[1], "service_version": c[2] if len(c) > 2 else None}
                      for c in json.loads(s["consumers"])],
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
