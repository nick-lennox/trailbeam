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

from . import order as order_mod
from .otlp import DEFAULT_KEY_ID

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
    queue after its first there (by end time, then row) was deduplicated."""
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


def consumer(r) -> dict:
    keys = ("service_name", "client_id", "source", "pid", "thread_name", "faas_name", "faas_invocation_id")
    return {k: r[k] for k in keys if r[k] is not None}


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


def build(rows: list, now_ns: int, lambda_timeout_s: int = DEFAULT_LAMBDA_TIMEOUT_S,
          max_receive_counts: Optional[dict] = None) -> dict:
    """``rows``: every stored row of one message (Store.message_rows), any order. ``max_receive_counts``:
    queue name -> the maxReceiveCount of its redrive policy, as configured (spans don't carry it)."""
    rows = sorted(rows, key=lambda r: (r["time_ns"], r["row"]))
    mid = rows[0]["mid"]
    receives = [r for r in rows if r["kind"] == "receive"]
    sends = [r for r in rows if r["kind"] == "send"]
    by_row = {r["row"]: i + 1 for i, r in enumerate(receives)}  # receive row -> delivery number
    latest = receives[-1] if receives else None
    source_queue = sends[0]["queue"] if sends else (receives[0]["queue"] if receives else rows[0]["queue"])
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
        if p is None and q == source_queue and r["receive_count"] > 1:
            s0 = sends[0] if sends else None
            infer(s0, r, 2, _inferred(
                "deliveries_not_observed", _start(s0) if s0 else None, r["time_ns"],
                f"The first observed receive has receive count {r['receive_count']}, so "
                f"{r['receive_count'] - 1} earlier receive(s) were not observed (a consumer without Trailbeam, "
                f"or lost spans).", count=r["receive_count"] - 1, queue=q))
        if p is not None and r["receive_count"] > p["receive_count"]:
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

    evs, n_groups, order_doc = _arrange(rows, observed, inferred, order, idx)

    # ---- summary ----------------------------------------------------------------------------
    final = [e for e in evs if e["type"] == "deleted" and e.get("status") == "final"]
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
    dlqs = {b["queue"] for _, b, _ in moves}
    mrc = (max_receive_counts or {}).get(source_queue)
    src_count = max((r["receive_count"] for r in receives if r["queue"] == source_queue), default=0)
    if moves:
        dlq_move = "observed"
    elif mrc and src_count >= mrc and state not in ("deleted", "deleted_inferred"):
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
        "concurrent_groups": sum(g["concurrent"] for g in groups),
        "groups": groups,
        "events": evs,
        "order": order_doc,
        "notices": _notices(rows, receives, order) + ([_not_seen_notice(source_queue, src_count, mrc)]
                                                       if dlq_move == "not_seen_on_dlq_yet" else []),
    }


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
            if ev["inferred"]:
                src = "inferred_from"
                why_a = f"Inferred from {_label(events[a])}: it came after it." if a is not None else ""
                why_b = f"Inferred from {_label(events[b])}: it came before it." if b is not None else ""
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
        return "the send"
    if t == "received":
        return f"receive {d}" if d is not None else "a receive"
    if t == "deleted":
        return "the delete" + of
    if t == "visibility_changed":
        return "the visibility change" + of
    if t == "lambda_outcome":
        return "the Lambda outcome" + of
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
        elif src == "receive_count":
            why.append(f"SQS's receive count shows {who} (receive count {a['receive_count']}) came before this "
                       f"one (receive count {b['receive_count']}).")
        elif src == "sqs_timestamps":
            why.append(f"SQS's timestamps show this message was sent ({_ms(b['sent_ts_ms'])}) before its first "
                       f"receive ({_ms(b['first_receive_ts_ms'])}).")
    return {"event": ev_id, "type": events[i]["type"], "sources": sources, "source": sources[0], "why": " ".join(why)}


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


def _dlq_moves(receives: list, sends: list, proven) -> list[tuple]:
    """(earlier anchor or None, the reporting receive, source queue name) per observed move (section 9b).

    One move per (source queue, dead-letter queue) pair, reported by its earliest dead-letter receive
    that carries DeadLetterQueueSourceArn. The earlier anchor is the last receive on the source queue
    (one no other receive there is proven to follow, and not proven to come after the report), or the
    send on it if no receive there was observed."""
    out = []
    pairs: dict[tuple[str, str], list] = {}
    for r in receives:
        if r["dlq_source_arn"]:
            frm = _queue_of_arn(r["dlq_source_arn"])
            if frm != r["queue"]:
                pairs.setdefault((frm, r["queue"]), []).append(r)
    for (frm, _), reports in pairs.items():
        first = [x for x in reports if not any(proven(y, x) for y in reports if y is not x)]
        b = min(first, key=lambda x: (x["receive_count"], x["time_ns"], x["row"]))
        on_src = [x for x in receives if x["queue"] == frm and not proven(b, x)]
        last = [x for x in on_src if not any(proven(x, y) for y in on_src if y is not x)]
        if last:
            a = max(last, key=lambda x: (x["receive_count"], x["time_ns"], x["row"]))
        else:
            a = next((x for x in sends if x["queue"] == frm and not proven(b, x)), None)
        out.append((a, b, frm))
    return out


def _not_seen_notice(queue: str, count: int, mrc: int) -> dict:
    return {"code": "dlq_move_not_seen", "message":
            f"Receive count exceeded maxReceiveCount; not seen on the DLQ yet. The last receive on {queue} had "
            f"receive count {count}, and {queue}'s maxReceiveCount is {mrc}, so SQS moves the message to its "
            f"dead-letter queue instead of delivering it again. No receive from the dead-letter queue has "
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
