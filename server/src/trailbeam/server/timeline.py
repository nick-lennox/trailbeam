"""Per-message timelines, built from the stored rows of one message (https://docs.trailbeam.app/reference/api).

Observed events come from Trailbeam spans. Each is an interval (contract section 2): ``start`` just before the
SQS request was sent (``None`` if not observed, as for a Lambda receive), ``end`` when the response came.
Inferred events (contract section 9) have no span and are always marked ``inferred: true``, with a time
window instead of a time. The move to a dead-letter queue has no span either, but is observed: SQS reports
it on the dead-letter receive (section 9b), so it is ``inferred: false`` with ``source: sqs_dlq_source``,
and is placed like an inferred event, by its anchors.

Order (contract section 9a). An observed event is shown before another only when an observed fact proves
it: the clocks, the call order inside one process, SQS's receive count, or SQS's own timestamps
(:mod:`.order`). Each proven edge carries its source(s), in the API and on the screen. Events are never
re-ordered by what caused what. Events are listed in an order consistent with every proven edge, and cut
into **groups** wherever everything before the cut is proven before everything after it; a group of more
than one event is shown as "concurrent, order unknown" (some of its pairs may still be ordered, and say
so). Each inferred event is placed by the observed events it was inferred from (its anchors), never by
a time of its own, and is ordered against another event only through those anchors.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Optional

from . import eventbridge as eb_mod
from . import s3 as s3_mod
from . import order as order_mod
from . import origin as origin_mod
from . import purge as purge_mod
from . import queues as queues_mod
from . import rabbitmq as amqp_mod
from . import sns as sns_mod
from . import systems
from .otlp import AMQP_KINDS, DEFAULT_KEY_ID

DEFAULT_LAMBDA_TIMEOUT_S = 900  # the longest Lambda timeout; Trailbeam does not know the real one


def iso(ns: Optional[int]) -> Optional[str]:
    if ns is None:
        return None
    dt = datetime.fromtimestamp(ns // 1_000_000_000, tz=timezone.utc).replace(microsecond=(ns // 1000) % 1_000_000)
    return dt.isoformat(timespec="microseconds").replace("+00:00", "Z")


CONCURRENT_LABEL = "concurrent, order unknown"
# The source of an observed move to a dead-letter queue (contract section 9b), in the API and on the screen.
DLQ_SOURCE = "sqs_dlq_source"
DLQ_REPORTED_BY = "reported by SQS (DeadLetterQueueSourceArn on the DLQ receive)"
# A FIFO send SQS deduplicated (QV-246): it returned the MessageId of the earlier send and enqueued nothing.
DEDUPLICATED_REASON = "SQS returned the same MessageId: this send was deduplicated and enqueued nothing"


def deduplicated_sends(rows) -> set:
    """The row IDs of the sends SQS deduplicated. A FIFO queue (name ending ``.fifo``) accepts a second send
    with the same MessageDeduplicationId within its 5-minute window and returns the first send's MessageId, but
    enqueues nothing; a standard queue never returns one MessageId twice. So every send of a message on a FIFO
    queue after its first there (by end time, then row) was deduplicated. RabbitMQ deduplicates nothing: a
    publish's ``queue`` is its exchange, and a second publish with one ID is a second message (A6)."""
    if amqp_mod.is_rabbitmq(rows):
        return set()
    seen: set = set()
    out: set = set()
    for r in sorted((r for r in rows if r["kind"] == "send"), key=lambda r: (r["time_ns"], r["row"])):
        q = r["queue"] or ""
        if q.endswith(".fifo") and q in seen:
            out.add(r["row"])
        seen.add(q)
    return out


def _time(start_ns: Optional[int], end_ns: int) -> dict:
    """An observed event's times. ``time`` is the end, kept from the 0.1 API for simple clients."""
    return {"time": iso(end_ns), "time_unix_nano": str(end_ns),
            "start": iso(start_ns), "start_unix_nano": None if start_ns is None else str(start_ns),
            "end": iso(end_ns), "end_unix_nano": str(end_ns), "start_observed": start_ns is not None}


def _start(r) -> Optional[int]:
    return r["start_ns"]


def span_version(r) -> Optional[str]:
    """The resource's service.version the span was stored with (QV-362); None if not set, or on a row without the
    column (a purge row of the Postgres store, which keeps purges in a table of their own)."""
    try:
        return r["service_version"]
    except (IndexError, KeyError):
        return None


def consumer(r) -> dict:
    keys = ("service_name", "client_id", "source", "pid", "thread_name", "faas_name", "faas_invocation_id")
    out = {k: r[k] for k in keys if r[k] is not None}
    version = span_version(r)
    if version is not None:
        out["service_version"] = version
    return out


def _span(r) -> dict:
    return {"trace_id": r["trace_id"], "span_id": r["span_id"]}


def _ticks(r) -> Optional[dict]:
    """The call-order facts of one span (contract section 2a), so that a client can recompute every
    ``process_order`` edge itself. None if the span carries no call order (schema version 2)."""
    if r["order_scope"] is None:
        return None
    return {"scope": r["order_scope"], "issued": r["order_issued"], "returned": r["order_returned"]}


def _error_message(r) -> Optional[str]:
    """The thrown error's message on an outcome span (contract 15.5): redacted by the package, and only there
    for queues with error text turned on. Read from the stored attributes, so rows stored before this field was
    shown have it too."""
    try:
        raw = r["attributes"]
    except (IndexError, KeyError):
        return None
    try:
        v = json.loads(raw).get("trailbeam.error_message") if raw else None
    except (TypeError, ValueError, AttributeError):
        return None
    return v if isinstance(v, str) and v else None


def _inferred(type_: str, frm: Optional[int], to: Optional[int], reason: str, **extra) -> dict:
    return {"type": type_, "inferred": True, "time": None,
            "window": {"from": iso(frm), "to": iso(to)}, "reason": reason, **extra}


def source_queue_of(mid: Optional[str], sends: list, receives: list, fallback: str) -> str:
    """The queue a message was sent to: its first send's, else its first receive's. A RabbitMQ message is sent to
    an exchange, not a queue (contract/span-contract-amqp.md A5): its first delivery's queue."""
    if systems.system_of(mid) == systems.RABBITMQ:
        return receives[0]["queue"] if receives else (sends[0]["queue"] if sends else fallback)
    return sends[0]["queue"] if sends else (receives[0]["queue"] if receives else fallback)


def current_key_of(sends: list, receives: list, rows: list) -> str:
    """The queue key (queues.key) of the queue a message is on: its latest receive's, else its first send's, else
    its first row's, which is where its current queue's name comes from (contract section 18; QV-725)."""
    r = receives[-1] if receives else (sends[0] if sends else rows[0])
    return queues_mod.key(r["queue"], r["queue_url"])


def build(rows: list, now_ns: int, lambda_timeout_s: int = DEFAULT_LAMBDA_TIMEOUT_S,
          max_receive_counts: Optional[dict] = None, purges=(), publishes=(), cancels=(), put_events=(),
          s3_uploads=()) -> dict:
    """``rows``: every stored row of one message (Store.message_rows), any order. ``max_receive_counts``:
    queue name -> the maxReceiveCount of its redrive policy, as configured (spans don't carry it). ``purges``:
    purge rows (Store.purges) that include those of the message's current queue (contract section 18).
    ``publishes``: the SNS publishes this message's receives join (contract section 21.5), as queries.message gives
    them: each ``{"publish": row, "joined_by": [...], "reached": [...], "reached_truncated": bool}``.
    ``put_events``: the EventBridge PutEvents entries they join (contract section 25.3), each ``{"entry": row,
    "joined_by": "event_id", "reached": [...], "reached_truncated": bool}``. ``s3_uploads``: the S3 uploads they
    join (contract section 27.3), each ``{"upload": row, "joined_by": "bucket_key_time", "reached": [...],
    "reached_truncated": bool}``.

    A RabbitMQ message (contract/span-contract-amqp.md, rabbitmq.py): ``max_receive_counts`` gives each queue's
    x-delivery-limit, and ``cancels`` the stored consumer cancels (Store.consumer_cancels) of its queues, which show
    when the broker cancelled the consumer holding a delivery (A10)."""
    rows = sorted(rows, key=lambda r: (r["time_ns"], r["row"]))
    mid = rows[0]["mid"]
    fallback_queue = rows[0]["queue"]
    amqp = systems.system_of(mid) == systems.RABBITMQ
    if amqp:
        return _build_amqp([amqp_mod.facts(r) for r in rows], now_ns, max_receive_counts, cancels)
    rows = [r for r in rows if r["kind"] not in AMQP_KINDS]  # an SQS message has none
    receives = [r for r in rows if r["kind"] == "receive"]
    sends = [r for r in rows if r["kind"] == "send"]
    by_row = {r["row"]: i + 1 for i, r in enumerate(receives)}  # receive row -> delivery number
    latest = receives[-1] if receives else None
    source_queue = source_queue_of(mid, sends, receives, fallback_queue)
    outcomes = {(r["faas_invocation_id"]): r for r in rows if r["kind"] == "outcome"}
    # Proven order between the observed rows (row list index = position in ``rows``).
    order = order_mod.Order(rows)
    idx = {r["row"]: i for i, r in enumerate(rows)}
    deduplicated = deduplicated_sends(rows)

    def proven(a, b) -> bool:
        return order.before(idx[a["row"]], idx[b["row"]])

    observed: list[dict] = []  # one event per row, in the order of ``rows``
    # Inferred events: (after row, before row, rank, event). Either row may be None.
    inferred: list[tuple[Optional[int], Optional[int], int, dict]] = []

    def infer(after, before, rank: int, ev: dict) -> None:
        inferred.append((after["row"] if after is not None else None,
                         before["row"] if before is not None else None, rank, ev))

    # ---- observed -------------------------------------------------------------------------
    for r in rows:
        k = r["kind"]
        base = {"inferred": False, **_time(_start(r), r["time_ns"]), "group": None,
                "queue": r["queue"], "consumer": consumer(r), "span": _span(r), "process_order": _ticks(r)}
        if k == "send":
            ev = {"type": "sent", **base, **({"batch_entry_id": r["batch_entry_id"]} if r["batch_entry_id"] else {})}
            if r["row"] in deduplicated:
                ev["deduplicated"] = True
                ev["reason"] = DEDUPLICATED_REASON
        elif k == "receive":
            ev = {"type": "received", **base, "delivery": by_row[r["row"]], "receive_count": r["receive_count"]}
            if r["sent_ts_ms"] is not None:
                ev["sent_timestamp"] = iso(r["sent_ts_ms"] * 1_000_000)
                ev["sent_timestamp_ms"] = r["sent_ts_ms"]
            if r["first_receive_ts_ms"] is not None:
                ev["first_receive_timestamp"] = iso(r["first_receive_ts_ms"] * 1_000_000)
                ev["first_receive_timestamp_ms"] = r["first_receive_ts_ms"]
            if r["dlq_source_arn"]:  # a receive on a dead-letter queue: the queue SQS moved it from (3.1, 9b)
                ev["dlq_source_arn"] = r["dlq_source_arn"]
        elif k in ("delete", "change_visibility"):
            ev = {"type": "deleted" if k == "delete" else "visibility_changed", **base,
                  "delivery": by_row.get(r["receive_row"]), "result": r["result"]}
            if r["error_code"]:
                ev["error_code"] = r["error_code"]
            if r["batch_entry_id"]:
                ev["batch_entry_id"] = r["batch_entry_id"]
            if k == "change_visibility":
                ev["visibility_timeout_s"] = r["visibility_timeout"]
            else:
                ev["status"] = _delete_status(r, latest)
        elif k == "outcome":
            ev = {"type": "lambda_outcome", **base, "outcome": r["outcome"]}
            if r["error_type"]:
                ev["error_type"] = r["error_type"]
            if r["outcome"] != "success" and (text := _error_message(r)) is not None:
                ev["error_message"] = text
            rec = next((x for x in receives if x["faas_invocation_id"] == r["faas_invocation_id"]), None)
            if rec is not None:
                ev["delivery"] = by_row[rec["row"]]
            if r["outcome"] == "success":
                infer(r, None, 0, _inferred(
                    "deleted_by_lambda", r["time_ns"], None,
                    "The handler succeeded for this record, so Lambda's event source mapping deletes the "
                    "message. That delete is not observed.", delivery=ev.get("delivery"), queue=r["queue"]))
        else:  # pragma: no cover - otlp.decode only produces the kinds above
            raise AssertionError(k)
        observed.append(ev)

    # ---- inferred between deliveries ----------------------------------------------------------
    prev_on_queue: dict[str, dict] = {}
    for r in receives:
        q = r["queue"]
        p = prev_on_queue.get(q)
        if p is None and q == source_queue and (r["receive_count"] or 0) > 1:  # None: no count (RabbitMQ, A7)
            s0 = sends[0] if sends else None
            infer(s0, r, 2, _inferred(
                "deliveries_not_observed", _start(s0) if s0 else None, r["time_ns"],
                f"The first observed receive has receive count {r['receive_count']}, so "
                f"{r['receive_count'] - 1} earlier receive(s) were not observed (a consumer without Trailbeam, "
                f"or lost spans).", count=r["receive_count"] - 1, queue=q))
        if p is not None and None not in (r["receive_count"], p["receive_count"]) and r["receive_count"] > p["receive_count"]:
            gap = r["receive_count"] - p["receive_count"] - 1
            if gap > 0:
                infer(p, r, 2, _inferred(
                    "deliveries_not_observed", _start(p), r["time_ns"],
                    f"Receive count went from {p['receive_count']} to {r['receive_count']}, so {gap} "
                    f"receive(s) in between were not observed.", count=gap, queue=q))
            # A visibility change SQS accepted for delivery p, proven to be before r, reset the timer
            # while the message was in flight, so the expiry came after it: it is an anchor too. Of
            # several, the latest proven one (one that no other such change is proven to follow).
            cmv = [x for x in rows if x["kind"] == "change_visibility" and x["receive_row"] == p["row"]
                   and x["result"] == "ok" and proven(x, r)]
            last = [x for x in cmv if not any(proven(x, y) for y in cmv if y is not x)]
            a = max(last, key=lambda x: (x["start_ns"] or 0, x["time_ns"], x["row"])) if last else p
            infer(a, r, 3, _inferred(
                "visibility_timeout_expired", _start(a), r["time_ns"],
                _expiry_reason(rows, p), queue=q, after_delivery=by_row[p["row"]],
                before_delivery=by_row[r["row"]]))
        prev_on_queue[q] = r

    # ---- the move to a dead-letter queue: observed when SQS reports it (contract section 9b) ----------
    # Never inferred from the message ID alone. SQS gives the move no time: it is placed after the last
    # receive on the queue it came from and before the dead-letter receive that reports it.
    moves = _dlq_moves(receives, sends, proven)
    for a, b, frm in moves:
        infer(a, b, 1, {
            "type": "moved_to_dead_letter_queue", "inferred": False, "source": DLQ_SOURCE, "time": None,
            "window": {"from": iso(_start(a)) if a is not None else None, "to": iso(b["time_ns"])},
            "reported_by": DLQ_REPORTED_BY, "source_arn": b["dlq_source_arn"],
            "reported_on_delivery": by_row[b["row"]], "from_queue": frm, "to_queue": b["queue"],
            "reason": f"SQS reported this move: the receive on {b['queue']} carries DeadLetterQueueSourceArn "
                      f"naming {frm}. SQS gives no time for the move; it happened after the last receive on "
                      f"{frm} and before that receive."})

    # ---- inferred: Lambda invocations without an outcome ------------------------------------------
    timeout_ns = lambda_timeout_s * 1_000_000_000
    pending_lambda = False
    for r in receives:
        if r["source"] != "lambda" or r["faas_invocation_id"] in outcomes:
            continue
        if now_ns - r["time_ns"] > timeout_ns:
            infer(r, None, 0, _inferred(
                "lambda_invocation_ended_without_outcome", r["time_ns"], r["time_ns"] + timeout_ns,
                f"A Lambda receive with no outcome after {lambda_timeout_s} s: the invocation most likely "
                f"timed out or ran out of memory.", delivery=by_row[r["row"]], queue=r["queue"],
                faas_invocation_id=r["faas_invocation_id"]))
        elif r is latest:
            pending_lambda = True

    # ---- summary ----------------------------------------------------------------------------
    final = [e for e in observed if e["type"] == "deleted" and e.get("status") == "final"]
    lambda_ok = latest is not None and latest["source"] == "lambda" and \
        outcomes.get(latest["faas_invocation_id"]) is not None and \
        outcomes[latest["faas_invocation_id"]]["outcome"] == "success"
    if final:
        state = "deleted"
    elif lambda_ok:
        state = "deleted_inferred"
    elif pending_lambda:
        state = "processing"
    else:
        state = "not_deleted"
    current_queue = latest["queue"] if latest else source_queue

    # ---- a PurgeQueue call on the message's queue (contract section 18) ---------------------------
    # The purge is observed and placed like any observed step, by the proven order; that it removed this
    # message is what SQS's documentation says a purge does, and the event says so.
    verdict = (purge_mod.classify(rows[-1]["time_ns"], current_key_of(sends, receives, rows), purges)
               if state in purge_mod.APPLIES_TO else None)
    steps = rows
    if verdict is not None:
        prow = purge_mod.as_row(verdict.purge)
        steps = rows + [prow]
        observed.append(_purge_event(prow, verdict.status))
        order = order_mod.Order(steps)
        idx = {r["row"]: i for i, r in enumerate(steps)}
        if verdict.status == purge_mod.PURGED:
            state = purge_mod.PURGED

    # ---- SNS fan-out (contract section 21.5) --------------------------------------------------------
    # A joined publish is an observed step, placed by the proven order (clock and process_order only: SNS's and
    # SQS's timestamps are never compared with a host clock). Without one, a receive whose envelope names a topic
    # shows that SNS delivered it, placed before that receive only, unless a send of the message was seen: then a
    # process re-sent an envelope-shaped body, and SNS delivered nothing (QV-514).
    delivery = sns_mod.delivery_ms(receives)
    if publishes:
        prows = [sns_mod.as_row(x["publish"]) for x in publishes]
        steps = steps + prows
        observed.extend(_publish_event(prow, x, delivery) for prow, x in zip(prows, publishes))
        order = order_mod.Order(steps)
        idx = {r["row"]: i for i, r in enumerate(steps)}
    elif put_events:
        # EventBridge PutEvents (contract section 25.3): a joined entry is observed, placed like an SNS publish, and
        # replaces section 23.3's inferred arrival.
        erows = [eb_mod.as_row(x["entry"]) for x in put_events]
        steps = steps + erows
        eb_delivery, eb_time = eb_mod.delivery_ms(receives), eb_mod.event_time_ms(receives)
        observed.extend(_put_event_event(erow, x, eb_delivery, eb_time) for erow, x in zip(erows, put_events))
        order = order_mod.Order(steps)
        idx = {r["row"]: i for i, r in enumerate(steps)}
    elif s3_uploads:
        # S3 uploads (contract section 27.3): a joined upload is observed, placed like an SNS publish, and replaces
        # section 23.3's inferred "S3 object created" step.
        urows = [s3_mod.as_row(x["upload"]) for x in s3_uploads]
        steps = steps + urows
        dwell = s3_mod.delivery_ms(receives)
        observed.extend(_s3_upload_event(urow, x, receives, dwell) for urow, x in zip(urows, s3_uploads))
        order = order_mod.Order(steps)
        idx = {r["row"]: i for i, r in enumerate(steps)}
    elif not sends and (arrived := sns_mod.arrived_from(receives)) is not None:
        r, arn = arrived
        infer(None, r, 1, _arrived_event(r, arn, by_row[r["row"]], delivery))
    elif not sends and (came := origin_mod.arrived_from(receives)) is not None:
        # EventBridge rule or S3 notification (contract section 23.3): inferred from the body's shape, so not
        # shown when a send of the message was seen (a process re-sent the body) or SNS delivered it.
        r, o = came
        infer(None, r, 1, _origin_event(r, o, by_row[r["row"]]))

    evs, n_groups, order_doc = _arrange(steps, observed, inferred, order, idx)
    dlqs = {b["queue"] for _, b, _ in moves}
    mrc = (max_receive_counts or {}).get(source_queue)
    src_count = max((r["receive_count"] or 0 for r in receives if r["queue"] == source_queue), default=0)
    if moves:
        dlq_move = "observed"
    elif mrc and src_count >= mrc and state not in ("deleted", "deleted_inferred", purge_mod.PURGED):
        dlq_move = "not_seen_on_dlq_yet"
    else:
        dlq_move = None
    groups = _groups(evs, n_groups)
    first_ns, first_clock = first_seen_ns(rows)
    return {
        "message_id": mid,
        "state": state,
        "source_queue": source_queue,
        "current_queue": current_queue,
        "in_dead_letter_queue": current_queue in dlqs,
        "dead_letter_queue_move": dlq_move,
        "send_observed": bool(sends),
        "deliveries": len(receives),
        "successful_deliveries": successful_deliveries(rows),
        "first_seen": iso(first_ns),
        "first_seen_clock": first_clock,
        "last_seen": iso(rows[-1]["time_ns"]),
        **({} if verdict is None else {"purge": purge_mod.describe(verdict)}),
        "concurrent_groups": sum(g["concurrent"] for g in groups),
        "groups": groups,
        "events": evs,
        "order": order_doc,
        "notices": _notices(rows, receives, order) + ([_not_seen_notice(source_queue, src_count, mrc)]
                                                       if dlq_move == "not_seen_on_dlq_yet" else []),
    }


PURGE_SOURCE = "purge_queue"  # the queue-level step a recorded PurgeQueue call is (contract section 18)


def _purge_event(prow, status: str) -> dict:
    """The observed PurgeQueue call on the message's queue, as a step of its timeline."""
    when = iso(prow["start_ns"])
    if status == purge_mod.PURGED:
        removal = "per_sqs_documentation"
        reason = (f"PurgeQueue on {prow['queue']} started at {when}, after this message's last observed step "
                  f"ended. SQS deletes every message in the queue when a purge starts, including messages in "
                  f"flight; the deletion of this message is not observed, it is what SQS's documentation says a "
                  f"purge does.")
    else:
        removal = "possible"
        reason = (f"PurgeQueue on {prow['queue']} started at {when}, and this message's last observed step "
                  f"ended during the purge or within 60 s after it. SQS may delete messages sent or received "
                  f"while a purge runs (up to 60 s), or may not: Trailbeam can't tell whether this one was.")
    return {"type": "queue_purged", "inferred": False, "source": PURGE_SOURCE, **_time(prow["start_ns"], prow["time_ns"]),
            "group": None, "queue": prow["queue"], "consumer": consumer(prow), "span": _span(prow),
            "process_order": _ticks(prow), "result": prow["result"], "removal": removal, "reason": reason}


PUBLISH_SOURCE = "sns_publish"    # a recorded SNS publish this message's receives join (contract section 21.5)
SNS_ENVELOPE_SOURCE = "sns_envelope"  # SNS delivered the message: its receive's envelope names the topic


def _publish_event(prow, joined: dict, delivery: Optional[int]) -> dict:
    """A recorded SNS publish this message's receives join, as a step of its timeline."""
    a = sns_mod.attributes(prow)
    arn = a.get(sns_mod.TOPIC_ARN)
    topic = sns_mod.topic_name(arn)
    by = joined["joined_by"]
    how = ("the SNS message ID in the envelope of its receive" if by == [sns_mod.BY_ENVELOPE] else
           "the trailbeam.publish_id attribute its receive carries" if by == [sns_mod.BY_PUBLISH_ID] else
           "the SNS message ID in its envelope and the trailbeam.publish_id attribute")
    ev = {"type": "published", "inferred": False, "source": PUBLISH_SOURCE, **_time(prow["start_ns"], prow["time_ns"]),
          "group": None, "queue": topic, "consumer": consumer(prow), "span": _span(prow),
          "process_order": _ticks(prow), "topic": topic, "topic_arn": arn, "sns_message_id": prow["message_id"],
          "joined_by": by, "reached": joined["reached"], "reached_truncated": joined["reached_truncated"],
          "reason": f"Published to topic {topic}. This message is joined to the publish by {how}."}
    if isinstance(a.get(sns_mod.PUBLISH_ID), str):
        ev["publish_id"] = a[sns_mod.PUBLISH_ID]
    if prow["batch_entry_id"]:
        ev["batch_entry_id"] = prow["batch_entry_id"]
    if delivery is not None and sns_mod.BY_ENVELOPE in by:
        ev["sns_delivery_ms"] = delivery
    return ev


PUT_EVENTS_SOURCE = "eventbridge_put_events"  # a recorded PutEvents entry this message's receives join (section 25.3)


def _put_event_event(erow, joined: dict, delivery: Optional[int], event_time: Optional[int]) -> dict:
    """A recorded EventBridge PutEvents entry this message's receives join, as a step of its timeline."""
    a = sns_mod.attributes(erow)
    bus = erow["queue"]
    given = a.get(eb_mod.TIME_GIVEN) is True
    ev = {"type": "put_event", "inferred": False, "source": PUT_EVENTS_SOURCE, **_time(erow["start_ns"], erow["time_ns"]),
          "group": None, "queue": bus, "consumer": consumer(erow), "span": _span(erow),
          "process_order": _ticks(erow), "event_bus": bus, "event_id": erow["message_id"],
          "joined_by": joined["joined_by"], "reached": joined["reached"],
          "reached_truncated": joined["reached_truncated"], "event_time_given": given,
          "reason": f"Put on EventBridge bus {bus} by PutEvents. This message is joined to the entry by the event ID in "
                    f"the body of its receive."}
    for name, key in ((eb_mod.SOURCE, "eventbridge_source"), (eb_mod.DETAIL_TYPE, "detail_type"),
                      (eb_mod.EVENT_BUS, "event_bus_given"), (eb_mod.ENTRY_INDEX, "entry_index")):
        if a.get(name) is not None and not isinstance(a.get(name), bool):
            ev[key] = a[name]
    if event_time is not None:
        ev["event_time"] = iso(event_time * 1_000_000)
        ev["event_time_ms"] = event_time
    if delivery is not None:  # SQS's SentTimestamp minus the event's time: AWS clocks unless the sender gave a Time
        ev["eventbridge_delivery_ms"] = delivery
    return ev


S3_UPLOAD_SOURCE = "s3_upload"  # a recorded S3 upload this message's receives join (section 27.3)


def _s3_upload_event(urow, joined: dict, receives: list, dwell: Optional[int]) -> dict:
    """A recorded S3 upload this message's receives join, as a step of its timeline."""
    a = sns_mod.attributes(urow)
    bucket, op = urow["queue"], urow["operation"]
    ev = {"type": "s3_upload", "inferred": False, "source": S3_UPLOAD_SOURCE, **_time(urow["start_ns"], urow["time_ns"]),
          "group": None, "queue": bucket, "consumer": consumer(urow), "span": _span(urow),
          "process_order": _ticks(urow), "bucket": bucket, "operation": op, "object_key": urow["message_id"],
          "s3_event_name": s3_mod.EVENT_NAMES[op], "joined_by": joined["joined_by"], "reached": joined["reached"],
          "reached_truncated": joined["reached_truncated"],
          "reason": f"Uploaded to S3 bucket {bucket} by {op}. This message is joined to the upload by the bucket, the "
                    f"object key and the event time in the S3 notification its receive got."}
    for name, key in ((s3_mod.ETAG, "etag"), (s3_mod.VERSION_ID, "version_id")):
        if isinstance(a.get(name), str):
            ev[key] = a[name]
    times = [n.event_time_ms for r in receives if (n := s3_mod.notification(r)) is not None
             and n.bucket == bucket and n.key == urow["message_id"]]
    if times:
        ev["event_time"] = iso(min(times) * 1_000_000)
        ev["event_time_ms"] = min(times)
    if dwell is not None:  # SQS's SentTimestamp minus the record's eventTime: both AWS clocks
        ev["s3_delivery_ms"] = dwell
    return ev


def _arrived_event(r, arn: str, delivery_no: int, delivery: Optional[int]) -> dict:
    """SNS delivered this message, and no recorded publish joins it (the publisher runs no Trailbeam package)."""
    topic = sns_mod.topic_name(arn)
    e, _ = sns_mod.receive_keys(r)
    ts = sns_mod.envelope_timestamp_ms(r)
    ev = {"type": "arrived_via_sns", "inferred": False, "source": SNS_ENVELOPE_SOURCE, "time": None,
          "window": {"from": None, "to": iso(r["time_ns"])}, "topic": topic, "topic_arn": arn,
          "sns_message_id": e[1], "reported_on_delivery": delivery_no,
          "reason": f"The SNS envelope of receive {delivery_no} names topic {topic}: SNS delivered this message to "
                    f"{r['queue']}. The publish is not recorded (its publisher runs no Trailbeam package, or its "
                    f"span was lost), so the step has no time of its own; it came before that receive."}
    if ts is not None:
        ev["sns_timestamp"] = iso(ts * 1_000_000)
        ev["sns_timestamp_ms"] = ts
    if delivery is not None:
        ev["sns_delivery_ms"] = delivery
    return ev


EVENTBRIDGE_SOURCE = "eventbridge_event"  # the receive's body is an EventBridge event (contract section 23.1)
S3_SOURCE = "s3_event"  # the receive's body is an S3 event notification (contract section 23.2)


def _origin_event(r, o: dict, delivery_no: int) -> dict:
    """The inferred first step of a message an EventBridge rule or an S3 notification sent (contract section 23.3)."""
    ts = o["event_time_ms"]
    extra = {k: v for k, v in o.items() if k not in ("kind", "event_time_ms") and v is not None}
    if o["kind"] == origin_mod.EVENTBRIDGE:
        type_, src = "arrived_via_eventbridge", EVENTBRIDGE_SOURCE
        why = (f"The body of receive {delivery_no} is an EventBridge event from source {o['eventbridge_source']}, "
               f"detail-type {o['detail_type']}: an EventBridge rule sent it to {r['queue']}. The event does not "
               f"name the rule.")
    else:
        type_, src = "s3_event", S3_SOURCE
        if o["test_event"]:
            why = (f"The body of receive {delivery_no} is the test message S3 sends to {r['queue']} when a "
                   f"notification configuration on bucket {o['bucket']} is saved.")
        else:
            what = f"object {o['object_key']} in bucket {o['bucket']}" if o.get("object_key") else f"bucket {o['bucket']}"
            why = (f"The body of receive {delivery_no} is an S3 event notification, {o['s3_event_name']} on {what}: "
                   f"S3 sent it to {r['queue']}.")
    ev = _inferred(type_, None, r["time_ns"], why + " Inferred from the body's shape (anything can send such a "
                   "body), so the step has no time of its own; it came before that receive.",
                   source=src, queue=r["queue"], reported_on_delivery=delivery_no, **extra)
    if ts is not None:
        ev["event_time"] = iso(ts * 1_000_000)
        ev["event_time_ms"] = ts
        if r["sent_ts_ms"] is not None:  # both AWS clocks, as SNS's delivery time (contract 21.5)
            ev["delivery_ms"] = r["sent_ts_ms"] - ts
    return ev


def _arrange(rows: list, observed: list[dict], inferred: list, order: "order_mod.Order",
             idx: dict) -> tuple[list[dict], int, dict]:
    """Put observed and inferred events in display order, number their groups, give each event an ``id``
    and its ``after`` list, and build the API's ``order`` object (see the module docstring)."""
    n = len(rows)
    keys: list[tuple] = []
    for r in rows:  # unproven order: by end time, so the listing is stable (it means nothing)
        keys.append((r["time_ns"], -1 if r["start_ns"] is None else r["start_ns"], r["row"], 0, 0))
    anchors: list[tuple] = []
    for seq, (after, before, rank, ev) in enumerate(inferred):
        a = idx[after] if after is not None else None
        b = idx[before] if before is not None else None
        if a is not None and b is not None and (a == b or order.before(b, a)):
            b = None  # the anchors contradict the proven order: keep only the earlier one
        anchors.append((a, b))
        if b is not None:  # just before its later anchor, when nothing proven says otherwise
            keys.append(keys[b][:3] + (-10 + rank, seq))
        elif a is not None:  # just after its earlier anchor
            keys.append(keys[a][:3] + (1 + rank, seq))
        else:  # pragma: no cover - every inferred event has an anchor
            keys.append((0, 0, 0, rank, seq))
    seq_nodes, group, concurrent = order_mod.layout(n, order, anchors, keys)

    events = observed + [ev for _, _, _, ev in inferred]
    pos = {node: p for p, node in enumerate(seq_nodes)}
    ids = [f"e{p + 1}" for p in range(len(seq_nodes))]
    out: list[dict] = []
    for p, node in enumerate(seq_nodes):
        ev = {"id": ids[p], **events[node], "group": group[p]}
        if node < n:
            ev["after"] = [_after_entry(rows, events, order, i, node, ids[pos[i]]) for i in
                           sorted(order.immediate(node), key=lambda i: pos[i])]
        else:
            a, b = anchors[node - n]
            if ev.get("source") in (EVENTBRIDGE_SOURCE, S3_SOURCE):  # contract section 23.3
                src = "inferred_from"
                why_a = ""
                what = ("an EventBridge event" if ev["source"] == EVENTBRIDGE_SOURCE else
                        "S3's test message" if ev.get("test_event") else "an S3 event notification")
                why_b = (f"Inferred from {_label(events[b])}: its body is {what}, sent before that receive."
                         if b is not None else "")
            elif ev["inferred"]:
                src = "inferred_from"
                why_a = f"Inferred from {_label(events[a])}: it came after it." if a is not None else ""
                why_b = f"Inferred from {_label(events[b])}: it came before it." if b is not None else ""
            elif ev["source"] == SNS_ENVELOPE_SOURCE:  # arrived via SNS (contract section 21.5)
                src = SNS_ENVELOPE_SOURCE
                why_a = ""
                why_b = (f"The SNS envelope of {_label(events[b])} names topic {ev['topic']}: SNS delivered the "
                         f"message before that receive.") if b is not None else ""
            elif ev["source"] == amqp_mod.DLQ_SOURCE:  # the move x-death reports (span-contract-amqp.md A8)
                src = amqp_mod.DLQ_SOURCE
                frm = ev["from_queue"]
                why_a = (f"RabbitMQ dead-letters a message off {frm} only after its deliveries there, and reported "
                         f"the move later, so it came after {_label(events[a])}.") if a is not None else ""
                why_b = (f"RabbitMQ reported the move on {_label(events[b])}: its x-death header names "
                         f"{frm}.") if b is not None else ""
            else:  # the observed move to a dead-letter queue (contract section 9b)
                src = DLQ_SOURCE
                frm = ev["from_queue"]
                why_a = (f"SQS moves a message off {frm} only after its receives there, and reported the move "
                         f"later, so it came after {_label(events[a])}.") if a is not None else ""
                why_b = (f"SQS reported the move on {_label(events[b])}: DeadLetterQueueSourceArn on that "
                         f"receive names {frm}.") if b is not None else ""
            ev["after"] = [] if a is None else [{
                "event": ids[pos[a]], "type": events[a]["type"], "sources": [src], "source": src, "why": why_a}]
            if b is not None:
                ev["before"] = {"event": ids[pos[b]], "type": events[b]["type"], "sources": [src],
                                "source": src, "why": why_b}
        out.append(ev)
    edges = []
    for (i, j), sources in sorted(order.edges.items(), key=lambda e: (pos[e[0][0]], pos[e[0][1]])):
        edges.append({"from": ids[pos[i]], "to": ids[pos[j]], "sources": sources})
    used = {src: sum(src in e["sources"] for e in edges) for src in order_mod.SOURCES}
    conflicts = [{"from": ids[pos[i]], "to": ids[pos[j]], "sources": _sources_of(rows, i, j)}
                 for i, j in order.conflicts]
    doc = {"edges": edges, "edges_by_source": used, "conflicts": conflicts}
    return out, len(concurrent), doc


def _sources_of(rows, i, j) -> list[str]:
    return order_mod._edge_sources(rows[i], rows[j])


def _label(ev: dict) -> str:
    t = ev["type"]
    d = ev.get("delivery")
    of = f" of delivery {d}" if d is not None else ""
    if t == "sent":
        if "exchange" in ev:  # a RabbitMQ publish
            return "the publish" if ev.get("publish", 1) == 1 else f"publish {ev['publish']}"
        return "the send"
    if t == "received":
        if "redelivered" in ev:  # a RabbitMQ delivery
            return f"delivery {d}" if d is not None else "a delivery"
        return f"receive {d}" if d is not None else "a receive"
    if t in ("acked", "requeued", "rejected"):
        return {"acked": "the ack", "requeued": "the requeue", "rejected": "the refusal"}[t] + of
    if t == "channel_closed":
        return "the channel close" + of
    if t == "returned":
        return "the return"
    if t == "deleted":
        return "the delete" + of
    if t == "visibility_changed":
        return "the visibility change" + of
    if t == "lambda_outcome":
        return "the Lambda outcome" + of
    if t == "queue_purged":
        return "the purge of the queue"
    if t == "published":
        return f"the publish to topic {ev['topic']}"
    return t.replace("_", " ")


def _ms(ms: int) -> str:
    return iso(ms * 1_000_000)


def _after_entry(rows, events, order, i: int, j: int, ev_id: str) -> dict:
    """Why observed event j is shown after observed event i, in plain words, per proving source."""
    a, b = rows[i], rows[j]
    who = _label(events[i])
    why = []
    sources = order.edges[(i, j)]
    for src in sources:
        if src == "clock":
            why.append(f"{who.capitalize()} ended ({iso(a['time_ns'])}) before this started ({iso(b['start_ns'])}), "
                       f"on the hosts' clocks.")
        elif src == "process_order":
            why.append(f"Same process: {who} returned before this call was issued.")
        elif src == "receive_count" and _is_amqp_row(a):
            why.append(f"The receive counts RabbitMQ gave ({_count_from(a)} and {_count_from(b)}) show {who} "
                       f"(receive count {a['receive_count']}) came before this one (receive count "
                       f"{b['receive_count']}).")
        elif src == "receive_count":
            why.append(f"SQS's receive count shows {who} (receive count {a['receive_count']}) came before this "
                       f"one (receive count {b['receive_count']}).")
        elif src == "sqs_timestamps":
            why.append(f"SQS's timestamps show this message was sent ({_ms(b['sent_ts_ms'])}) before its first "
                       f"receive ({_ms(b['first_receive_ts_ms'])}).")
    return {"event": ev_id, "type": events[i]["type"], "sources": sources, "source": sources[0], "why": " ".join(why)}


def _is_amqp_row(r) -> bool:
    return isinstance(r, dict) and "count_source" in r


def _count_from(r) -> str:
    return "redelivered = false" if r["count_source"] == amqp_mod.FIRST_DELIVERY else f"the {r['count_source']} header"


def _groups(evs: list[dict], n: int) -> list[dict]:
    out = []
    for g in range(n):
        members = [e for e in evs if e.get("group") == g]
        obs = [e for e in members if not e["inferred"]]
        spans = [e for e in obs if "end_unix_nano" in e]  # the observed move has no time of its own
        starts = [int(e["start_unix_nano"]) for e in spans if e["start_unix_nano"] is not None]
        concurrent = len(members) > 1  # an inferred member counts: it is not ordered against the rest
        ordered = sum(1 for e in members for x in e.get("after", []) if x["event"] in {m["id"] for m in members})
        out.append({"group": g, "concurrent": concurrent, "label": CONCURRENT_LABEL if concurrent else None,
                    "observed_events": len(obs), "inferred_events": len(members) - len(obs),
                    "ordered_inside": ordered,
                    "start": iso(min(starts)) if spans and len(starts) == len(spans) else None,
                    "end": iso(max(int(e["end_unix_nano"]) for e in spans)) if spans else None})
    return out


def _queue_of_arn(arn: str) -> str:
    return arn.rsplit(":", 1)[-1]


def _dlq_moves(receives: list, sends: list, proven, source_of=None, key=None) -> list[tuple]:
    """(earlier anchor or None, the reporting receive, source queue name) per observed move (section 9b).

    One move per (source queue, dead-letter queue) pair, reported by its earliest dead-letter receive
    that carries DeadLetterQueueSourceArn. The earlier anchor is the last receive on the source queue
    (one no other receive there is proven to follow, and not proven to come after the report), or the
    send on it if no receive there was observed. ``source_of``: receive -> the queue it reports the message was
    moved from, or None (default: SQS's DeadLetterQueueSourceArn; RabbitMQ: rabbitmq.dlq_source_copy, from x-death).
    ``key``: row -> the key of its queue, in the terms ``source_of`` gives (default: the queue name; RabbitMQ:
    rabbitmq.copy_of, (virtual host, queue)). The source queue returned is in those terms."""
    key = key or (lambda r: r["queue"])
    out = []
    pairs: dict[tuple[str, str], list] = {}
    for r in receives:
        frm = source_of(r) if source_of is not None else (
            _queue_of_arn(r["dlq_source_arn"]) if r["dlq_source_arn"] else None)
        if frm is not None and frm != key(r):
            pairs.setdefault((frm, key(r)), []).append(r)
    for (frm, _), reports in pairs.items():
        first = [x for x in reports if not any(proven(y, x) for y in reports if y is not x)]
        b = min(first, key=lambda x: (x["receive_count"] or 0, x["time_ns"], x["row"]))
        on_src = [x for x in receives if key(x) == frm and not proven(b, x)]
        last = [x for x in on_src if not any(proven(x, y) for y in on_src if y is not x)]
        if last:
            a = max(last, key=lambda x: (x["receive_count"] or 0, x["time_ns"], x["row"]))
        else:
            a = next((x for x in sends if key(x) == frm and not proven(b, x)), None)
        out.append((a, b, frm))
    return out


def _not_seen_notice(queue: str, count: int, mrc: int) -> dict:
    return {"code": "dlq_move_not_seen", "message":
            f"Receive count reached maxReceiveCount; not seen on the DLQ yet. The last receive on {queue} had "
            f"receive count {count}, and {queue}'s maxReceiveCount is {mrc} as configured, so unless that receive "
            f"deletes the message, SQS moves it to its dead-letter queue instead of delivering it again. This is "
            f"inferred from the configured value, and that receive may still be in progress. No receive from the "
            f"dead-letter queue has "
            f"reported the move yet, so no move is shown. Trailbeam does not receive from the dead-letter queue to "
            f"check: a receive changes the message's state."}


def _delete_status(r, latest) -> str:
    if r["result"] != "ok":
        return "failed"
    if r["receive_row"] is None:
        return "unmatched_handle"
    if latest is not None and r["receive_row"] == latest["row"]:
        return "final"
    return "stale_handle"


def _expiry_reason(rows, p) -> str:
    vis = [r for r in rows if r["kind"] == "change_visibility" and r["receive_row"] == p["row"] and r["result"] == "ok"]
    if vis:
        t = vis[-1]["visibility_timeout"]
        if t == 0:
            return "ChangeMessageVisibility set the visibility timeout to 0, which made the message visible again at once."
        return f"The visibility timeout, last changed to {t} s, expired before the message was deleted."
    stale = [r for r in rows if r["kind"] == "delete" and r["receive_row"] == p["row"] and r["result"] == "ok"]
    if stale:
        return ("The visibility timeout expired before the delete: SQS accepted the delete, but the message "
                "was received again, so the delete came too late to remove it.")
    return "The visibility timeout expired before the message was deleted, so SQS delivered it again."


def successful_deliveries(rows) -> int:
    """Deliveries that ended in an accepted delete or a Lambda success."""
    ok = {r["receive_row"] for r in rows if r["kind"] == "delete" and r["result"] == "ok" and r["receive_row"]}
    ok_inv = {r["faas_invocation_id"] for r in rows if r["kind"] == "outcome" and r["outcome"] == "success"}
    return len(ok) + len(ok_inv)


# The clock a "first seen" time is on (QV-32 section 4). The two are never compared with each other.
HOST_CLOCK = "host"  # the clock of the host that made the call (a Trailbeam span's end)
SQS_CLOCK = "sqs"    # SQS's own clock: SentTimestamp, in milliseconds


def first_seen_ns(rows) -> tuple[int, str]:
    """(time, clock) the message was first seen. The send's time, on the producer host's clock, when the
    send was observed; otherwise the earliest SentTimestamp a receive carries, on SQS's clock; otherwise the
    earliest span, on its host's clock. Never the minimum across the two clocks."""
    sends = [r["time_ns"] for r in rows if r["kind"] == "send"]
    if sends:
        return min(sends), HOST_CLOCK
    sqs = [r["sent_ts_ms"] for r in rows if r["kind"] == "receive" and r["sent_ts_ms"] is not None]
    if sqs:
        return min(sqs) * 1_000_000, SQS_CLOCK
    return min(r["time_ns"] for r in rows), HOST_CLOCK


def _notices(rows, receives, order=None) -> list[dict]:
    out = []
    if order is not None and order.conflicts:
        out.append({"code": "order_conflict", "message":
                    "The observed facts contradict each other about the order of some steps (for example, a "
                    "host clock that jumped), so no order is shown between those steps. The conflicting edges "
                    "are listed in order.conflicts."})
    if any(r["key_id"] == DEFAULT_KEY_ID for r in rows):
        out.append({"code": "default_key", "message":
                    "These spans use the package's built-in receipt-hash key, which anyone can read in the source. "
                    "Generate one key with `openssl rand -hex 32` and set it as `TRAILBEAM_RECEIPT_HASH_KEY` on "
                    "every service that uses these queues."})
    unmatched = [r for r in rows if r["kind"] in ("delete", "change_visibility") and r["receive_row"] is None]
    if unmatched:
        rkeys = {r["key_id"] for r in receives}
        if receives and any(r["key_id"] not in rkeys for r in unmatched):
            out.append({"code": "key_mismatch", "message":
                        "A delete or visibility change for this message used a different receipt-hash key than "
                        "its receives (trailbeam.receipt_handle.key_id differs), so it cannot be joined to a delivery. "
                        "Use the same TRAILBEAM_RECEIPT_HASH_KEY everywhere."})
        else:
            out.append({"code": "unmatched_handle", "message":
                        "A delete or visibility change carries this message ID, but no stored receive has its "
                        "receipt-handle hash, so it is not attached to a delivery."})
    if not any(r["kind"] == "send" for r in rows):
        out.append({"code": "send_not_observed", "message":
                    "No trailbeam send span for this message: the producer is not instrumented, or the send is older "
                    "than the stored data."})
    return out


# ---- RabbitMQ (contract/span-contract-amqp.md A4 to A10; rabbitmq.py) ------------------------------------------
# The steps of a RabbitMQ message, by stored kind. A publish is "sent", a delivery "received"; the settle steps
# have their own types, because AMQP 0-9-1 has no delete and no visibility timeout.
AMQP_SETTLE_TYPES = {"delete": "acked", "change_visibility": "requeued", "reject": "rejected",
                     "channel_closed": "channel_closed"}


def _op(r) -> Optional[str]:
    v = r["amqp"].get("messaging.operation.name")
    return v if isinstance(v, str) else None


def _routing_key(ev: dict, a: dict) -> None:
    """The routing key (A5), or, when the package did not export it (body capture off, revision 4.1), its keyed
    hash: equal hashes mean equal keys under one receipt-hash key."""
    if isinstance(a.get(amqp_mod.ROUTING_KEY), str) and a[amqp_mod.ROUTING_KEY]:
        ev["routing_key"] = a[amqp_mod.ROUTING_KEY]
    elif isinstance(a.get(amqp_mod.ROUTING_KEY_HMAC), str) and a[amqp_mod.ROUTING_KEY_HMAC]:
        ev["routing_key_hmac"] = a[amqp_mod.ROUTING_KEY_HMAC]


def _amqp_event(r, base: dict, by_row: dict, n_publish: int, latest_on: dict, seen: list) -> dict:
    """The observed event of one RabbitMQ row. ``n_publish``: which publish of the message this is (sends only);
    ``latest_on``: queue -> its latest delivery; ``seen``: every delivery of the message."""
    k, a = r["kind"], r["amqp"]
    ev: dict
    if k == "send":
        ev = {"type": "sent", **base, "exchange": r["queue"], "operation": _op(r), "result": r["result"],
              "confirmed": a.get("trailbeam.amqp.confirmed") is True, "mandatory": a.get("trailbeam.amqp.mandatory") is True,
              "publish": n_publish}
        _routing_key(ev, a)
        if r["error_code"]:
            ev["error_code"] = r["error_code"]
        if a.get("trailbeam.amqp.message_id.source") == "package":
            ev["message_id_source"] = "package"
        if n_publish > 1:
            ev["published_again"] = True
            ev["reason"] = (f"Published again with the same message_id (publish {n_publish}). RabbitMQ does not "
                            f"deduplicate, so this is a second message in the queue under the same ID, most "
                            f"likely a publisher retry.")
        elif not ev["confirmed"]:
            ev["reason"] = ("Published on a channel without publisher confirms: the frame was written, and nothing "
                            "says the broker got the message.")
        return ev
    if k == "return":
        ev = {"type": "returned", **base, "exchange": r["queue"], "operation": _op(r),
              "reply_code": a.get("trailbeam.amqp.reply_code")}
        _routing_key(ev, a)
        text = a.get("trailbeam.amqp.reply_text")
        if isinstance(text, str) and text:
            ev["reply_text"] = text
        rk = f" with routing key {ev['routing_key']}" if "routing_key" in ev else ""
        ev["reason"] = (f"Unroutable: exchange {r['queue']} has no queue for this message{rk}, so the broker "
                        f"returned it to the publisher with basic.return ({ev['reply_code']}"
                        f"{' ' + text if isinstance(text, str) and text else ''}) and enqueued nothing.")
        return ev
    if k == "receive":
        ev = {"type": "received", **base, "delivery": by_row[r["row"]], "receive_count": r["receive_count"],
              "operation": _op(r), "redelivered": a.get("trailbeam.amqp.redelivered") is True}
        if r["count_source"] is not None:
            ev["receive_count_source"] = r["count_source"]
        for name, key in ((amqp_mod.CONSUMER_TAG, "consumer_tag"), ("trailbeam.amqp.exchange", "exchange")):
            if isinstance(a.get(name), str) and a[name]:
                ev[key] = a[name]
        _routing_key(ev, a)
        for name, key in ((amqp_mod.DELIVERY_COUNT, "x_delivery_count"), (amqp_mod.ACQUIRED_COUNT, "x_acquired_count")):
            v = amqp_mod._int(a.get(name))
            if v is not None:
                ev[key] = v
        dl = amqp_mod.dead_letter(r)
        if dl is not None:
            ev["dead_letter"] = dl
        return ev
    # a settle or close of one delivery
    rec = by_row.get(r["receive_row"])
    ev = {"type": AMQP_SETTLE_TYPES[k], **base, "delivery": rec, "operation": _op(r), "result": r["result"]}
    if r["error_code"]:
        ev["error_code"] = r["error_code"]
    if isinstance(a.get("trailbeam.amqp.multiple"), bool):
        ev["multiple"] = a["trailbeam.amqp.multiple"]
    if k == "delete":
        ev["status"] = _ack_status(r, latest_on)
    elif k == "change_visibility":
        ev["requeue"] = True
    elif k == "reject":
        ev["requeue"] = False
        # seen again: delivered again on its queue, or on a dead-letter queue that reports the move off it
        here = amqp_mod.copy_of(r)
        seen_again = any((amqp_mod.copy_of(x) == here and x["time_ns"] > r["time_ns"])
                         or amqp_mod.dlq_source_copy(x) == here for x in seen if x["row"] != r["receive_row"])
        ev["reason"] = (f"Refused by the consumer with {ev['operation']} (requeue = false). RabbitMQ dead-letters the "
                        f"message if its queue has a dead-letter exchange, and drops it otherwise"
                        + ("." if seen_again else "; it has not been seen since."))
    else:
        who = a.get("trailbeam.amqp.close.initiator")
        ev["initiator"] = who
        for name, key in (("trailbeam.amqp.close.reply_code", "reply_code"), ("trailbeam.amqp.close.reply_text", "reply_text")):
            if a.get(name) is not None:
                ev[key] = a[name]
        text = ev.get("reply_text") if isinstance(ev.get("reply_text"), str) else ""
        if who == "broker" and amqp_mod.is_consumer_timeout_close(r):
            ev["consumer_timeout"] = True
            ev["reason"] = ("The broker closed the channel because this delivery was not acked within its "
                            "consumer_timeout. RabbitMQ returns the channel's unacked deliveries to the queue.")
        elif text.startswith(amqp_mod.UNKNOWN_TAG_TEXT):
            ev["reason"] = ("The broker closed the channel because a settle named a delivery tag it did not know (an "
                            "ack or nack sent twice, or on the wrong channel). Trailbeam can't tell which settle it "
                            "refused. RabbitMQ returns the channel's unacked deliveries to the queue.")
        else:
            by = {"client": "The application closed the channel", "broker": "The broker closed the channel",
                  "network": "The connection was lost"}.get(who, "The channel closed")
            ev["reason"] = (f"{by} while this delivery was not settled"
                            + (f" ({ev.get('reply_code')} {text})" if text else "")
                            + ". RabbitMQ returns the channel's unacked deliveries to the queue.")
    return ev


def _ack_status(r, latest_on: dict) -> str:
    """As _delete_status, per copy: an ack of the latest delivery on its queue is final."""
    if r["result"] != "ok":
        return "failed"
    if r["receive_row"] is None:
        return "unmatched_handle"
    if any(x["row"] == r["receive_row"] for x in latest_on.values()):
        return "final"
    return "stale_handle"


def _build_amqp(rows: list, now_ns: int, delivery_limits: Optional[dict], cancels) -> dict:
    """timeline.build for a RabbitMQ message: ``rows`` are rabbitmq.facts rows, by (time_ns, row)."""
    mid = rows[0]["mid"]
    receives = [r for r in rows if r["kind"] == "receive"]
    sends = [r for r in rows if r["kind"] == "send"]
    by_row = {r["row"]: i + 1 for i, r in enumerate(receives)}
    latest = receives[-1] if receives else None
    source_queue = source_queue_of(mid, sends, receives, rows[0]["queue"])
    # a copy is (virtual host, queue), amqp_mod.copy_of (A3, A6): queues of one name in two vhosts are two copies
    copy_of = amqp_mod.copy_of
    source_copy = copy_of(receives[0]) if receives else None
    order = order_mod.Order(rows)
    idx = {r["row"]: i for i, r in enumerate(rows)}

    def proven(a, b) -> bool:
        return order.before(idx[a["row"]], idx[b["row"]])

    inferred: list[tuple[Optional[int], Optional[int], int, dict]] = []

    def infer(after, before, rank: int, ev: dict) -> None:
        inferred.append((after["row"] if after is not None else None,
                         before["row"] if before is not None else None, rank, ev))

    # ---- deliveries of one copy: returns to the queue and their causes (A10) -------------------------------
    prev_on_queue: dict[tuple, dict] = {}
    for r in receives:
        q = r["queue"]
        p = prev_on_queue.get(copy_of(r))
        dl = amqp_mod.dead_letter(r)
        if p is None and copy_of(r) == source_copy and (r["receive_count"] or 0) > 1 and dl is None:
            s0 = sends[0] if sends else None
            infer(s0, r, 2, _inferred(
                "deliveries_not_observed", _start(s0) if s0 else None, r["time_ns"],
                f"The first observed delivery has receive count {r['receive_count']} (from {r['count_source']}), so "
                f"{r['receive_count'] - 1} earlier delivery(s) were not observed (a consumer without Trailbeam, or "
                f"lost spans).", count=r["receive_count"] - 1, queue=q))
        if p is not None and amqp_mod.redelivery_of(p, r):
            if (None not in (p["receive_count"], r["receive_count"]) and p["count_source"] == r["count_source"]
                    or p["count_source"] == amqp_mod.FIRST_DELIVERY and r["receive_count"] is not None):
                gap = r["receive_count"] - p["receive_count"] - 1
                if gap > 0:
                    infer(p, r, 2, _inferred(
                        "deliveries_not_observed", _start(p), r["time_ns"],
                        f"Receive count went from {p['receive_count']} to {r['receive_count']}, so {gap} "
                        f"delivery(s) in between were not observed.", count=gap, queue=q))
            cause, shown_by, why = amqp_mod.cause_of(rows, p, cancels)
            # The return is inferred from the requeue, a call the consumer made, so it comes after that call. A
            # close is when the client saw it, not when the broker returned the message (A4.6), so that return is
            # placed after the delivery only.
            a = shown_by if cause == amqp_mod.REQUEUED else p
            infer(a, r, 3, _inferred(
                "returned_to_queue", _start(a), r["time_ns"], why, queue=q, cause=cause,
                cause_observed=cause != amqp_mod.NOT_OBSERVED, after_delivery=by_row[p["row"]],
                before_delivery=by_row[r["row"]]))
        # otherwise a second first delivery: a second message published under the same ID, not a return
        prev_on_queue[copy_of(r)] = r

    # ---- observed steps ------------------------------------------------------------------------------
    latest_on = {}
    for r in receives:
        latest_on[copy_of(r)] = r
    observed: list[dict] = []
    n_publish = 0
    for r in rows:
        base = {"inferred": False, **_time(_start(r), r["time_ns"]), "group": None,
                "queue": r["queue"], "consumer": consumer(r), "span": _span(r), "process_order": _ticks(r)}
        if r["kind"] == "send":
            n_publish += 1
        if r["kind"] == "cancel":  # pragma: no cover - a cancel belongs to no message (otlp.py)
            continue
        observed.append(_amqp_event(r, base, by_row, n_publish, latest_on, receives))

    # ---- routed to N queues (A5): from the queues it was delivered on, a lower bound --------------------
    moves = _dlq_moves(receives, sends, proven, source_of=amqp_mod.dlq_source_copy, key=copy_of)
    # an x-death naming a queue whose latest delivery is held or acked is not a move: x-death is publisher-set (A8)
    forged, kept = [], []
    for m in moves:
        held = amqp_mod.move_ruled_out(rows, m[2])
        if held is None:
            kept.append(m)
        else:
            forged.append(amqp_mod.forged_move_notice(rows, m[1], held))
    moves = kept
    dlqs = {copy_of(b) for _, b, _ in moves}
    # the queues of the first publish's virtual host: an exchange routes only within its own
    vhost0 = copy_of(sends[0])[0] if sends else None
    routed_copies = []
    for r in receives:
        c = copy_of(r)
        if c[0] == vhost0 and c not in dlqs and c not in routed_copies:
            routed_copies.append(c)
    routed = [q for _, q in routed_copies]
    if sends and routed:
        s0 = sends[0]
        first = next(r for r in receives if copy_of(r) == routed_copies[0])
        n = len(routed)
        infer(s0, first if n == 1 else None, 1, _inferred(
            "routed", _start(s0), first["time_ns"],
            f"Exchange {s0['queue']} routed this message to {n} queue{'s' if n > 1 else ''}: {', '.join(routed)}. "
            f"Counted from the queues it was delivered on, so it is a lower bound: a queue that no consumer with "
            f"Trailbeam reads is not seen. Each queue holds its own copy, which is not a duplicate.",
            exchange=s0["queue"], queues=routed, count=n, lower_bound=True))

    # ---- the move to a dead-letter queue, as x-death reports it (A8) -------------------------------------
    for a, b, (_, frm) in moves:
        dl = amqp_mod.dead_letter(b)
        what = amqp_mod.DEAD_LETTER_REASONS.get(dl["reason"], f"reason {dl['reason']}")
        when = ""
        ev = {"type": "moved_to_dead_letter_queue", "inferred": False, "source": amqp_mod.DLQ_SOURCE, "time": None,
              "window": {"from": iso(_start(a)) if a is not None else None, "to": iso(b["time_ns"])},
              "reported_by": amqp_mod.DLQ_REPORTED_BY, "reported_on_delivery": by_row[b["row"]],
              "from_queue": frm, "to_queue": b["queue"], "dead_letter_reason": dl["reason"]}
        if "count" in dl:
            ev["dead_letter_count"] = dl["count"]
        if "time_ms" in dl:
            ev["first_dead_lettered"] = _ms(dl["time_ms"])
            ev["first_dead_lettered_ms"] = dl["time_ms"]
            when = (f" x-death says it was first dead-lettered from {frm} for this reason at "
                    f"{_ms(dl['time_ms'])}, to the second, on the broker's clock.")
        ev["reason"] = (f"RabbitMQ reported this move: the delivery on {b['queue']} carries an x-death header naming "
                        f"{frm}, with reason {dl['reason']} ({what}).{when} It happened after the last delivery on "
                        f"{frm} and before that delivery.")
        infer(a, b, 1, ev)

    # ---- state: per copy (A6) ---------------------------------------------------------------------------
    moved_from = {frm for _, _, frm in moves}
    final, _left = amqp_mod.done(rows, moved_from)
    state = "deleted" if final else "not_deleted"
    current_queue = latest["queue"] if latest else source_queue
    in_dlq = latest is not None and copy_of(latest) in dlqs

    evs, n_groups, order_doc = _arrange(rows, observed, inferred, order, idx)
    groups = _groups(evs, n_groups)
    limit = (delivery_limits or {}).get(source_queue)
    failed = amqp_mod.failed_returns(rows, source_copy)
    if moves:
        dlq_move = "observed"
    elif amqp_mod.delivery_limit_reached(failed, limit) and state != "deleted":
        dlq_move = "not_seen_on_dlq_yet"
    else:
        dlq_move = None
    first_ns, first_clock = first_seen_ns(rows)
    notices = _amqp_notices(rows, sends, order) + forged
    if dlq_move == "not_seen_on_dlq_yet":
        notices.append({"code": "dlq_move_not_seen", "message":
                        f"x-delivery-limit passed; not seen on a dead-letter queue yet. The copy on {source_queue} "
                        f"was returned to the queue by a failure {failed} time(s) (its x-delivery-count), and "
                        f"{source_queue}'s x-delivery-limit is {limit} as configured, so RabbitMQ dead-letters the "
                        f"message if the queue has a dead-letter exchange, and drops it otherwise. This is inferred "
                        f"from the configured value; no delivery from a dead-letter queue has reported the move."})
    return {
        "message_id": mid,
        "system": systems.RABBITMQ,
        "state": state,
        "source_queue": source_queue,
        "current_queue": current_queue,
        "in_dead_letter_queue": in_dlq,
        "dead_letter_queue_move": dlq_move,
        "send_observed": bool(sends),
        "publishes": len(sends),
        "deliveries": len(receives),
        "successful_deliveries": successful_deliveries(rows),
        "copies": [{"queue": q, "vhost": v, "deliveries": sum(1 for r in receives if copy_of(r) == (v, q)),
                    "ended": e} for (v, q), (_, e) in amqp_mod.copies(rows, moved_from).items()],
        "first_seen": iso(first_ns),
        "first_seen_clock": first_clock,
        "last_seen": iso(rows[-1]["time_ns"]),
        "concurrent_groups": sum(g["concurrent"] for g in groups),
        "groups": groups,
        "events": evs,
        "order": order_doc,
        "notices": notices,
    }


def _amqp_notices(rows, sends, order) -> list[dict]:
    out = []
    if order.conflicts:
        out.append({"code": "order_conflict", "message":
                    "The observed facts contradict each other about the order of some steps (for example, a "
                    "host clock that jumped), so no order is shown between those steps. The conflicting edges "
                    "are listed in order.conflicts."})
    if len(sends) > 1:
        out.append({"code": "published_twice", "message":
                    f"Published {len(sends)} times with the same message_id. RabbitMQ does not deduplicate: each "
                    f"publish is its own message in the queue, so the work can be done once per publish. This is "
                    f"most often a publisher retry after a lost confirm."})
    by_queue: dict = {}  # per copy: (virtual host, queue)
    for r in rows:
        if r["kind"] == "change_visibility" and r["result"] == "ok" and _op(r) == "basic.nack":
            c = amqp_mod.copy_of(r)
            by_queue[c] = by_queue.get(c, 0) + 1
    for (v, q), n in sorted(by_queue.items(), key=lambda kv: (str(kv[0][0]), kv[0][1])):
        if n >= amqp_mod.REQUEUE_LOOP_MIN:
            out.append({"code": "requeue_loop", "queue": q, "vhost": v, "requeues": n, "message":
                        f"Requeued {n} times on {q} with basic.nack (requeue = true). On a quorum queue since "
                        f"RabbitMQ 4.3, basic.nack does not count toward x-delivery-limit, so the queue never "
                        f"dead-letters the message for it, and a consumer that keeps failing receives it again and "
                        f"again. To end the loop after some attempts, refuse it with requeue = false (it goes to the "
                        f"dead-letter exchange, if there is one), or use basic.reject, which counts toward the limit."})
    if not sends:
        out.append({"code": "send_not_observed", "message":
                    "No trailbeam publish span for this message: the publisher is not instrumented, or the publish "
                    "is older than the stored data."})
    return out
