"""Per-message timelines, built from the stored rows of one message (docs/store-api.md).

Observed events come from qv2 spans. Inferred events (contract section 9) have no span and are always
marked ``inferred: true``, with a time window instead of a time.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from .otlp import DEFAULT_KEY_ID

DEFAULT_LAMBDA_TIMEOUT_S = 900  # the longest Lambda timeout; the store does not know the real one


def iso(ns: Optional[int]) -> Optional[str]:
    if ns is None:
        return None
    dt = datetime.fromtimestamp(ns // 1_000_000_000, tz=timezone.utc).replace(microsecond=(ns // 1000) % 1_000_000)
    return dt.isoformat(timespec="microseconds").replace("+00:00", "Z")


def _time(ns: int) -> dict:
    return {"time": iso(ns), "time_unix_nano": str(ns)}


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

    events: list[tuple[tuple, dict]] = []  # (sort key, event)

    def add(sort_ns: int, order: int, ev: dict):
        events.append(((sort_ns, order, len(events)), ev))

    # ---- observed -------------------------------------------------------------------------
    for r in rows:
        k = r["kind"]
        base = {"inferred": False, **_time(r["time_ns"]), "queue": r["queue"], "consumer": consumer(r), "span": _span(r)}
        if k == "send":
            add(r["time_ns"], 0, {"type": "sent", **base,
                                  **({"batch_entry_id": r["batch_entry_id"]} if r["batch_entry_id"] else {})})
        elif k == "receive":
            ev = {"type": "received", **base, "delivery": by_row[r["row"]], "receive_count": r["receive_count"]}
            if r["sent_ts_ms"] is not None:
                ev["sent_timestamp"] = iso(r["sent_ts_ms"] * 1_000_000)
            add(r["time_ns"], 0, ev)
        elif k in ("delete", "change_visibility"):
            rec_row = r["receive_row"]
            delivery = by_row.get(rec_row)
            ev = {"type": "deleted" if k == "delete" else "visibility_changed", **base,
                  "delivery": delivery, "result": r["result"]}
            if r["error_code"]:
                ev["error_code"] = r["error_code"]
            if r["batch_entry_id"]:
                ev["batch_entry_id"] = r["batch_entry_id"]
            if k == "change_visibility":
                ev["visibility_timeout_s"] = r["visibility_timeout"]
            else:
                ev["status"] = _delete_status(r, latest)
            # Order a settle after the receive it settles, even with clock skew between hosts.
            rec_t = next((x["time_ns"] for x in receives if x["row"] == rec_row), r["time_ns"])
            add(max(r["time_ns"], rec_t), 1, ev)
        elif k == "outcome":
            ev = {"type": "lambda_outcome", **base, "outcome": r["outcome"]}
            if r["error_type"]:
                ev["error_type"] = r["error_type"]
            rec = next((x for x in receives if x["faas_invocation_id"] == r["faas_invocation_id"]), None)
            if rec is not None:
                ev["delivery"] = by_row[rec["row"]]
            add(r["time_ns"], 2, ev)
            if r["outcome"] == "success":
                add(r["time_ns"], 3, _inferred(
                    "deleted_by_lambda", r["time_ns"], None,
                    "The handler succeeded for this record, so Lambda's event source mapping deletes the "
                    "message. That delete is not observed.", delivery=ev.get("delivery"), queue=r["queue"]))

    # ---- inferred between deliveries ----------------------------------------------------------
    prev_on_queue: dict[str, dict] = {}
    prev = None
    for r in receives:
        q = r["queue"]
        p = prev_on_queue.get(q)
        if p is None and q == source_queue and r["receive_count"] > 1:
            add(r["time_ns"], -2, _inferred(
                "deliveries_not_observed", sends[0]["time_ns"] if sends else None, r["time_ns"],
                f"The first observed receive has receive count {r['receive_count']}, so "
                f"{r['receive_count'] - 1} earlier receive(s) were not observed (a consumer without qv2, "
                f"or lost spans).", count=r["receive_count"] - 1, queue=q))
        if p is not None and r["receive_count"] > p["receive_count"]:
            gap = r["receive_count"] - p["receive_count"] - 1
            if gap > 0:
                add(r["time_ns"], -2, _inferred(
                    "deliveries_not_observed", p["time_ns"], r["time_ns"],
                    f"Receive count went from {p['receive_count']} to {r['receive_count']}, so {gap} "
                    f"receive(s) in between were not observed.", count=gap, queue=q))
            add(r["time_ns"], -1, _inferred(
                "visibility_timeout_expired", p["time_ns"], r["time_ns"],
                _expiry_reason(rows, p), queue=q, after_delivery=by_row[p["row"]],
                before_delivery=by_row[r["row"]]))
        if prev is not None and q != prev["queue"]:
            to_dlq = q != source_queue
            add(r["time_ns"], -3, _inferred(
                "moved_to_dead_letter_queue" if to_dlq else "moved_back_to_source_queue",
                prev["time_ns"], r["time_ns"],
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
            add(r["time_ns"] + timeout_ns, 0, _inferred(
                "lambda_invocation_ended_without_outcome", r["time_ns"], r["time_ns"] + timeout_ns,
                f"A Lambda receive with no outcome after {lambda_timeout_s} s: the invocation most likely "
                f"timed out or ran out of memory.", delivery=by_row[r["row"]], queue=r["queue"],
                faas_invocation_id=r["faas_invocation_id"]))
        elif r is latest:
            pending_lambda = True

    events.sort(key=lambda e: e[0])
    evs = [e for _, e in events]

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
        "events": evs,
        "notices": _notices(rows, receives),
    }


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


def _notices(rows, receives) -> list[dict]:
    out = []
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
