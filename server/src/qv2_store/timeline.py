"""Per-message timelines, built from the stored rows of one message (docs/store-api.md).

Observed events come from qv2 spans. Each is an interval (contract section 2): ``start`` just before the
SQS request was sent (``None`` if not observed, as for a Lambda receive), ``end`` when the response came.
Inferred events (contract section 9) have no span and are always marked ``inferred: true``, with a time
window instead of a time.

Order (contract section 9a). Observed events are put in **groups**: the connected components of "their
intervals overlap", where an unknown start counts as minus infinity. Every event of a group ended before
any event of the next group started, so the order of groups is proven by the clocks; inside a group it
is not, and a group of more than one event is shown as "concurrent, order unknown". Events are never
re-ordered by what caused what. Each inferred event is placed by the observed events it was inferred
from (its anchors), never by a time of its own, and is never ordered against an observed event it was
not inferred from: if one may have happened on either side of it, they share a group (see ``_place``).
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


CONCURRENT_LABEL = "concurrent, order unknown"


def _time(start_ns: Optional[int], end_ns: int) -> dict:
    """An observed event's times. ``time`` is the end, kept from the 0.1 API for simple clients."""
    return {"time": iso(end_ns), "time_unix_nano": str(end_ns),
            "start": iso(start_ns), "start_unix_nano": None if start_ns is None else str(start_ns),
            "end": iso(end_ns), "end_unix_nano": str(end_ns), "start_observed": start_ns is not None}


def _start(r) -> Optional[int]:
    return r["start_ns"]


def group_rows(rows: list) -> dict:
    """Row number -> group number (0, 1, ...), by the rule in the module docstring.

    Sort by start (unknown first), sweep, and open a new group only when a start is strictly after
    the latest end seen so far: then every earlier event ended before it started.
    """
    order = sorted(rows, key=lambda r: (-1 if r["start_ns"] is None else r["start_ns"], r["time_ns"], r["row"]))
    out: dict = {}
    g, max_end = -1, None
    for r in order:
        start = r["start_ns"]
        if max_end is None or (start is not None and start > max_end):
            g += 1
            max_end = r["time_ns"]
        else:
            max_end = max(max_end, r["time_ns"])
        out[r["row"]] = g
    return out


def consumer(r) -> dict:
    keys = ("service_name", "client_id", "source", "pid", "thread_name", "faas_name", "faas_invocation_id")
    return {k: r[k] for k in keys if r[k] is not None}


def _span(r) -> dict:
    return {"trace_id": r["trace_id"], "span_id": r["span_id"]}


def _inferred(type_: str, frm: Optional[int], to: Optional[int], reason: str, **extra) -> dict:
    return {"type": type_, "inferred": True, "time": None,
            "window": {"from": iso(frm), "to": iso(to)}, "reason": reason, **extra, "_window_ns": (frm, to)}


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
    group = group_rows(rows)

    observed: list[tuple[tuple, dict]] = []  # (sort key, event)
    # Inferred events: (after row, before row, rank, event). Either row may be None.
    inferred: list[tuple[Optional[int], Optional[int], int, dict]] = []

    def infer(after, before, rank: int, ev: dict) -> None:
        inferred.append((after["row"] if after is not None else None,
                         before["row"] if before is not None else None, rank, ev))

    # ---- observed -------------------------------------------------------------------------
    for r in rows:
        k = r["kind"]
        base = {"inferred": False, **_time(_start(r), r["time_ns"]), "group": group[r["row"]],
                "queue": r["queue"], "consumer": consumer(r), "span": _span(r)}
        if k == "send":
            ev = {"type": "sent", **base, **({"batch_entry_id": r["batch_entry_id"]} if r["batch_entry_id"] else {})}
        elif k == "receive":
            ev = {"type": "received", **base, "delivery": by_row[r["row"]], "receive_count": r["receive_count"]}
            if r["sent_ts_ms"] is not None:
                ev["sent_timestamp"] = iso(r["sent_ts_ms"] * 1_000_000)
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
            continue
        # Inside a group the order is not known; list by end time so the listing is stable.
        observed.append(((group[r["row"]], r["time_ns"], -1 if _start(r) is None else _start(r), r["row"]), ev))

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
            # A visibility change SQS accepted for delivery p, proven (by the clocks) to be before r, reset
            # the timer while the message was in flight, so the expiry came after it: it is an anchor too.
            cmv = [x for x in rows if x["kind"] == "change_visibility" and x["receive_row"] == p["row"]
                   and x["result"] == "ok" and x["start_ns"] is not None and _start(r) is not None
                   and x["time_ns"] < _start(r)]
            a = max(cmv, key=lambda x: (x["start_ns"], x["row"])) if cmv else p
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

    observed.sort(key=lambda e: e[0])
    evs, n_groups = _place(observed, inferred)

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
        "notices": _notices(rows, receives),
    }


def _place(observed: list, inferred: list) -> tuple[list[dict], int]:
    """Observed events in group order, with each inferred event inserted so that it is never ordered
    against an observed event it was not inferred from (contract section 9a).

    An inferred event happened inside its window (``from``: the start of its earlier anchor, ``to``: the
    end of its later anchor; a missing bound is unbounded). An observed event that is not an anchor and
    whose interval meets the window may have happened before or after it, so the inferred event must be
    in one group with every such event. If they are in different groups, those groups (and any between)
    are merged into one: their order against the inferred event is unknown, so the order between them
    cannot be shown either without ordering the inferred event against one of them. Then:

    - some non-anchor event meets the window: inside that event's group, just before the later anchor if
      it is in that group, else at the group's end;
    - none does, and the later anchor's group holds more than one event, or both anchors: inside that
      group, just before the later anchor;
    - none does, otherwise: just before the later anchor's group, between groups (``group`` null);
    - only an earlier anchor: just after it, inside its group if that group is concurrent, else after
      the group.
    """
    evs = [ev for _, ev in observed]
    rows = [key[3] for key, _ in observed]
    pos = {row: i for i, row in enumerate(rows)}
    lo_of = [None if ev["start_unix_nano"] is None else int(ev["start_unix_nano"]) for ev in evs]
    hi_of = [int(ev["end_unix_nano"]) for ev in evs]

    # Which observed events each inferred event cannot be ordered against.
    meets: list[list[int]] = []
    for after, before, _, ev in inferred:
        frm, to = ev.pop("_window_ns")
        anchors = {pos[x] for x in (after, before) if x is not None}
        meets.append([i for i in range(len(evs)) if i not in anchors
                      and (to is None or lo_of[i] is None or lo_of[i] <= to)
                      and (frm is None or hi_of[i] >= frm)])

    # Merge groups so that each inferred event's set lies in one group; groups stay contiguous.
    n = max((e["group"] for e in evs), default=-1) + 1
    joined = [False] * n  # joined[g]: group g is merged with group g - 1
    for m in meets:
        if m:
            gs = [evs[i]["group"] for i in m]
            for g in range(min(gs) + 1, max(gs) + 1):
                joined[g] = True
    renum, g2 = [], -1
    for g in range(n):
        g2 += 0 if (g and joined[g]) else 1
        renum.append(g2)
    if any(joined):
        for e in evs:
            e["group"] = renum[e["group"]]
        order = sorted(range(len(evs)), key=lambda i: (evs[i]["group"],) + observed[i][0][1:])
        evs = [evs[i] for i in order]
        rows = [rows[i] for i in order]
        meet_rows = [{observed[i][0][3] for i in m} for m in meets]
        pos = {row: i for i, row in enumerate(rows)}
    else:
        meet_rows = [{rows[i] for i in m} for m in meets]

    first: dict = {}
    size: dict = {}
    for i, ev in enumerate(evs):
        first.setdefault(ev["group"], i)
        size[ev["group"]] = size.get(ev["group"], 0) + 1
    slots: list[tuple[int, int, int, dict]] = []  # (insert before index, rank, seq, event)
    for seq, ((after, before, rank, ev), m) in enumerate(zip(inferred, meet_rows)):
        gb = evs[pos[before]]["group"] if before is not None else None
        ga = evs[pos[after]]["group"] if after is not None else None
        if m:
            g = evs[pos[next(iter(m))]]["group"]
            idx = pos[before] if gb == g else first[g] + size[g]
        elif before is not None:
            if ga == gb or size[gb] > 1:
                idx, g = pos[before], gb
            else:
                idx, g = first[gb], None
        else:
            if size[ga] > 1:
                idx, g = pos[after] + 1, ga
            else:
                idx, g = first[ga] + size[ga], None
        slots.append((idx, rank, seq, {**ev, "group": g}))
    slots.sort(key=lambda s: s[:3])
    out: list[dict] = []
    j = 0
    for i, ev in enumerate(evs + [None]):
        while j < len(slots) and slots[j][0] == i:
            out.append(slots[j][3])
            j += 1
        if ev is not None:
            out.append(ev)
    n_groups = (max((e["group"] for e in evs), default=-1) + 1)
    return out, n_groups


def _groups(evs: list[dict], n: int) -> list[dict]:
    out = []
    for g in range(n):
        members = [e for e in evs if e.get("group") == g]
        obs = [e for e in members if not e["inferred"]]
        starts = [int(e["start_unix_nano"]) for e in obs if e["start_unix_nano"] is not None]
        concurrent = len(members) > 1  # an inferred member counts: it is not ordered against the rest
        out.append({"group": g, "concurrent": concurrent, "label": CONCURRENT_LABEL if concurrent else None,
                    "observed_events": len(obs), "inferred_events": len(members) - len(obs),
                    "start": iso(min(starts)) if len(starts) == len(obs) else None,
                    "end": iso(max(int(e["end_unix_nano"]) for e in obs))})
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
