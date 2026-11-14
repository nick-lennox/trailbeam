"""Per-message timelines, built from the stored rows of one message (docs/store-api.md).

Observed events come from qv2 spans. Each is an interval (contract section 2): ``start`` just before the
SQS request was sent (``None`` if not observed, as for a Lambda receive), ``end`` when the response came.
Inferred events (contract section 9) have no span and are always marked ``inferred: true``, with a time
window instead of a time.

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

from datetime import datetime, timezone
from typing import Optional

from . import order as order_mod
from .otlp import DEFAULT_KEY_ID

DEFAULT_LAMBDA_TIMEOUT_S = 900  # the longest Lambda timeout; the store does not know the real one


def iso(ns: Optional[int]) -> Optional[str]:
    if ns is None:
        return None
    dt = datetime.fromtimestamp(ns // 1_000_000_000, tz=timezone.utc).replace(microsecond=(ns // 1000) % 1_000_000)
    return dt.isoformat(timespec="microseconds").replace("+00:00", "Z")


CONCURRENT_LABEL = "concurrent, order unknown"


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


def _inferred(type_: str, frm: Optional[int], to: Optional[int], reason: str, **extra) -> dict:
    return {"type": type_, "inferred": True, "time": None,
            "window": {"from": iso(frm), "to": iso(to)}, "reason": reason, **extra}


def build(rows: list, now_ns: int, lambda_timeout_s: int = DEFAULT_LAMBDA_TIMEOUT_S) -> dict:
    """``rows``: every stored row of one message (Store.message_rows), any order."""
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
                "queue": r["queue"], "consumer": consumer(r), "span": _span(r)}
        if k == "send":
            ev = {"type": "sent", **base, **({"batch_entry_id": r["batch_entry_id"]} if r["batch_entry_id"] else {})}
        elif k == "receive":
            ev = {"type": "received", **base, "delivery": by_row[r["row"]], "receive_count": r["receive_count"]}
            if r["sent_ts_ms"] is not None:
                ev["sent_timestamp"] = iso(r["sent_ts_ms"] * 1_000_000)
            if r["first_receive_ts_ms"] is not None:
                ev["first_receive_timestamp"] = iso(r["first_receive_ts_ms"] * 1_000_000)
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
    prev = None
    for r in receives:
        q = r["queue"]
        p = prev_on_queue.get(q)
        if p is None and q == source_queue and r["receive_count"] > 1:
            s0 = sends[0] if sends else None
            infer(s0, r, 2, _inferred(
                "deliveries_not_observed", _start(s0) if s0 else None, r["time_ns"],
                f"The first observed receive has receive count {r['receive_count']}, so "
                f"{r['receive_count'] - 1} earlier receive(s) were not observed (a consumer without qv2, "
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
        if prev is not None and q != prev["queue"]:
            to_dlq = q != source_queue
            infer(prev, r, 1, _inferred(
                "moved_to_dead_letter_queue" if to_dlq else "moved_back_to_source_queue",
                _start(prev), r["time_ns"],
                f"The message ID was received on {q} after {prev['queue']}. SQS keeps the message ID "
                f"when it moves a message, so it was moved between those two receives; exactly when is "
                f"not known.", from_queue=prev["queue"], to_queue=q))
        prev_on_queue[q] = r
        prev = r

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
    groups = _groups(evs, n_groups)
    return {
        "message_id": mid,
        "state": state,
        "source_queue": source_queue,
        "current_queue": current_queue,
        "in_dead_letter_queue": current_queue != source_queue,
        "send_observed": bool(sends),
        "deliveries": len(receives),
        "successful_deliveries": successful_deliveries(rows),
        "first_seen": iso(first_seen_ns(rows)),
        "last_seen": iso(rows[-1]["time_ns"]),
        "concurrent_groups": sum(g["concurrent"] for g in groups),
        "groups": groups,
        "events": evs,
        "order": order_doc,
        "notices": _notices(rows, receives, order),
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
            ev["after"] = [] if a is None else [{
                "event": ids[pos[a]], "type": events[a]["type"], "sources": ["inferred_from"],
                "source": "inferred_from", "why": f"Inferred from {_label(events[a])}: it came after it."}]
            if b is not None:
                ev["before"] = {"event": ids[pos[b]], "type": events[b]["type"], "sources": ["inferred_from"],
                                "source": "inferred_from",
                                "why": f"Inferred from {_label(events[b])}: it came before it."}
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
        starts = [int(e["start_unix_nano"]) for e in obs if e["start_unix_nano"] is not None]
        concurrent = len(members) > 1  # an inferred member counts: it is not ordered against the rest
        ordered = sum(1 for e in members for x in e.get("after", []) if x["event"] in {m["id"] for m in members})
        out.append({"group": g, "concurrent": concurrent, "label": CONCURRENT_LABEL if concurrent else None,
                    "observed_events": len(obs), "inferred_events": len(members) - len(obs),
                    "ordered_inside": ordered,
                    "start": iso(min(starts)) if obs and len(starts) == len(obs) else None,
                    "end": iso(max(int(e["end_unix_nano"]) for e in obs)) if obs else None})
    return out


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


def first_seen_ns(rows) -> int:
    ts = [r["time_ns"] for r in rows if r["kind"] == "send"]
    ts += [r["sent_ts_ms"] * 1_000_000 if r["sent_ts_ms"] is not None else r["time_ns"]
           for r in rows if r["kind"] == "receive"]
    return min(ts) if ts else min(r["time_ns"] for r in rows)


def _notices(rows, receives, order=None) -> list[dict]:
    out = []
    if order is not None and order.conflicts:
        out.append({"code": "order_conflict", "message":
                    "The observed facts contradict each other about the order of some steps (for example, a "
                    "host clock that jumped), so no order is shown between those steps. The conflicting edges "
                    "are listed in order.conflicts."})
    if any(r["key_id"] == DEFAULT_KEY_ID for r in rows):
        out.append({"code": "default_key", "message":
                    "Some spans hash receipt handles with the published default key. Set QV2_RECEIPT_HASH_KEY "
                    "to the same secret in every service."})
    unmatched = [r for r in rows if r["kind"] in ("delete", "change_visibility") and r["receive_row"] is None]
    if unmatched:
        rkeys = {r["key_id"] for r in receives}
        if receives and any(r["key_id"] not in rkeys for r in unmatched):
            out.append({"code": "key_mismatch", "message":
                        "A delete or visibility change for this message used a different receipt-hash key than "
                        "its receives (qv2.receipt_handle.key_id differs), so it cannot be joined to a delivery. "
                        "Use the same QV2_RECEIPT_HASH_KEY everywhere."})
        else:
            out.append({"code": "unmatched_handle", "message":
                        "A delete or visibility change carries this message ID, but no stored receive has its "
                        "receipt-handle hash, so it is not attached to a delivery."})
    if not any(r["kind"] == "send" for r in rows):
        out.append({"code": "send_not_observed", "message":
                    "No qv2 send span for this message: the producer is not instrumented, or the send is older "
                    "than the stored data."})
    return out
