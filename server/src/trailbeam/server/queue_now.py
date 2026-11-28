"""``GET /api/queues/{queue}/now`` (QV-579): "In ‹queue› now" from spans alone. These are the instrumented messages
in a queue right now, each visible or in flight, and what left it in the past minute.

This is the hosted app's ``loadInQueue`` and ``placeOf`` (trailbeam-app ``lib/app/in-queue.ts``), in its
``InQueueView`` shape and camelCase, so the shared "In ‹queue› now" view reads either. With AWS reads off
(aws_settings.py, the default), or on but not for this queue, the server makes no AWS call, so the answer differs from
the hosted one in three ways:

- ``totals`` and ``uninstrumented`` are null. Only SQS knows what the queue holds beyond the instrumented messages.
- The visibility timeout is SQS's default of 30 s (``config.visibilityKnown`` is false), unless an accepted
  ChangeMessageVisibility on the latest receive set another one. The queue's own setting is not read.
- The retention period is SQS's longest, 14 days, because the queue's own ``MessageRetentionPeriod`` is not read
  either. A message older than that has left SQS whatever its spans say. The server's own retention usually ends
  sooner.

With AWS reads on for the queue (QV-589), ``view`` is given what GetQueueAttributes said (``aws``, aws_figures'
settings) and SQS's counts (``totals``), as the hosted app gives them: the queue's own visibility timeout, retention
period, maxReceiveCount (over the server's ``--max-receive-count``), dead-letter queue and ARN, and the totals with
what the spans don't account for.

Trailbeam never calls ReceiveMessage (that would hide the messages it returned), so nothing here looks inside the
queue. It is worked out from the spans:

- In the queue: a message with a send or receive on this queue and no accepted delete of its latest receive and no
  Lambda success for it (the Stuck list's candidates, ``Store.stuck_messages``), whose timeline is not deleted or
  purged and whose current queue is this one (a message received on its dead-letter queue has left).
- In flight or visible: a message is in flight until its visibility timeout runs out. That is counted from its
  latest receive, or from an accepted ChangeMessageVisibility on that receive with the timeout it set. After that it
  is visible again. A message never received is visible.
- Not seen on the DLQ yet (contract 9b): a message received maxReceiveCount times (``max_receive_count`` in the
  queue's config) whose last timeout ran out. SQS moves it on the next receive attempt, so it is listed apart and
  not as in the queue. The move is inferred and has no time.
- ``deleted``: the accepted deletes and Lambda successes on this queue in the past minute.
- ``seenOnDlq``: the moves SQS reported in the past hour (``dlq_seen`` queue events, queues.py), as receives on the
  dead-letter queue naming this queue as the source.

Each listed message carries its events as the Event explorer has them (timeline_events.py), for its sequence
drawing. A message's kept body is left out: the drawing doesn't show it, and the message page does."""
from __future__ import annotations

from typing import Optional

from . import queries, queues as queues_mod, rabbitmq
from .timeline_events import events_of_timeline, handler_failed, span_metas

NS_PER_MS = 1_000_000
SQS_DEFAULT_VISIBILITY_S = 30
SQS_MAX_RETENTION_S = 14 * 86_400
LIST_LIMIT = 1000
DELETED_LIMIT = 1000  # steps read for the past minute's deletes
NO_SEND = "No send observed"


def not_deleted_text(n: int, default_timeout: bool, why: str = "no delete of it was stored") -> str:
    """The hosted ``notDeletedText`` (lib/app/not-deleted.ts): a receive not deleted in time, said as inferred."""
    tail = (f"; the timeout used is SQS's default {SQS_DEFAULT_VISIBILITY_S} s, as the queue's setting was not read"
            if default_timeout else "")
    return f"Inferred: receive {n} was not deleted before its visibility timeout ran out ({why}{tail})"


def _ms(ns: int) -> int:
    return ns // NS_PER_MS


def place_of(rows: list, t: dict, queue: str, now_ms: int, cfg: dict, events: list) -> Optional[dict]:
    """A message's place in ``queue`` from its rows and timeline: ``{"held": …}``, ``{"notSeen": …}``, or None when it
    is not in this queue now. ``cfg``: the queue's ``config`` (visibilityTimeoutS, retentionS, maxReceiveCount)."""
    if not rows or t["state"] in ("deleted", "deleted_inferred", "purged") or t["current_queue"] != queue:
        return None
    s = sorted(rows, key=lambda r: (r["time_ns"], r["row"]))
    sends = [r for r in s if r["kind"] == "send"]
    receives = [r for r in s if r["kind"] == "receive" and r["queue"] == queue]
    last = receives[-1] if receives else None
    if sends:
        sent_ms = _ms(sends[0]["time_ns"])
    elif last is not None and last["sent_ts_ms"] is not None:
        sent_ms = last["sent_ts_ms"]
    else:
        sent_ms = _ms(s[0]["time_ns"])
    retention_s = cfg["retentionS"] if cfg["retentionS"] is not None else SQS_MAX_RETENTION_S
    if sent_ms + retention_s * 1000 < now_ms:
        return None  # SQS deleted it at the end of its retention period
    base = {"id": str(s[0]["mid"]), "producer": (sends[0]["service_name"] or "") if sends else "",
            "consumer": (last["service_name"] or "") if last is not None else "", "sentMs": sent_ms, "events": events}
    if last is None:
        return {"held": {**base, "consumer": "", "receiveCount": 0, "state": "visible", "lastReceiveMs": None,
                         "untilMs": None, "leftS": 0, "used": 0, "timeoutS": None}}

    # the timer of the latest receive: the queue's timeout, or the last accepted visibility change on it
    changes = [r for r in s if r["kind"] == "change_visibility" and r["receive_row"] == last["row"]
               and r["result"] == "ok" and r["visibility_timeout"] is not None]
    change = changes[-1] if changes else None
    default_vt = cfg["visibilityTimeoutS"] is None
    vt = SQS_DEFAULT_VISIBILITY_S if default_vt else cfg["visibilityTimeoutS"]
    from_ms = _ms(change["time_ns"]) if change else _ms(last["time_ns"])
    timeout_s = change["visibility_timeout"] if change else vt
    until_ms = from_ms + timeout_s * 1000
    rc = last["receive_count"] if last["receive_count"] is not None else len(receives)

    def error_of(r) -> Optional[str]:
        # worded by outcome, as the timeline words it: a reported batch item failure is not a handler that threw
        inv = r["faas_invocation_id"]
        f = next((o for o in s if o["kind"] == "outcome" and inv and o["faas_invocation_id"] == inv
                  and o["outcome"] != "success"), None)
        return handler_failed(f["error_type"], None, f["outcome"]) if f is not None else None

    prev = receives[-2] if len(receives) > 1 else None
    if now_ms < until_ms:
        used = min(1.0, max(0.0, (now_ms - from_ms) / max(1, timeout_s * 1000)))
        err = error_of(last)
        if err is None and prev is not None:
            err = error_of(prev) or not_deleted_text(
                prev["receive_count"] if prev["receive_count"] is not None else len(receives) - 1, default_vt)
        held = {**base, "receiveCount": rc, "state": "in_flight", "lastReceiveMs": _ms(last["time_ns"]),
                "untilMs": until_ms, "leftS": max(0, -((now_ms - until_ms) // 1000)), "used": _js(used),
                "timeoutS": timeout_s}
        if err is not None:
            held["lastError"] = err
        return {"held": held}
    mrc = cfg["maxReceiveCount"]
    if mrc and rc >= mrc:
        return {"notSeen": {**base, "timedOutMs": until_ms, "receiveCount": rc}}
    return {"held": {**base, "receiveCount": rc, "state": "visible", "lastReceiveMs": _ms(last["time_ns"]),
                     "untilMs": None, "leftS": 0, "used": 0, "timeoutS": timeout_s,
                     "lastError": error_of(last) or not_deleted_text(rc, default_vt)}}


def _js(x: float):
    """A number as JSON.stringify writes it: a whole float as an integer."""
    return int(x) if float(x).is_integer() else x


def summarize(held: list, not_seen: list, *, now_ms: int, config: dict, capped: bool, deleted: list,
              seen_on_dlq: int, totals: Optional[dict] = None) -> dict:
    """The hosted ``summarize``. ``totals``: SQS's counts ({visible, inFlight, delayed}), None when not read."""
    by_producer: dict = {}
    for m in held:
        k = m["producer"] or NO_SEND
        by_producer[k] = by_producer.get(k, 0) + 1
    instrumented = {"visible": sum(1 for m in held if m["state"] == "visible"),
                    "inFlight": sum(1 for m in held if m["state"] == "in_flight")}
    t = totals
    tot = ({"visible": t["visible"], "inFlight": t["inFlight"], "delayed": t.get("delayed")}
           if t and t.get("visible") is not None and t.get("inFlight") is not None else None)
    # totals minus instrumented, never below zero; over: the spans account for more than SQS holds
    unins = ({"visible": max(0, tot["visible"] - instrumented["visible"]),
              "inFlight": max(0, tot["inFlight"] - instrumented["inFlight"]),
              "over": instrumented["visible"] > tot["visible"] or instrumented["inFlight"] > tot["inFlight"]}
             if tot else None)
    return {
        "nowMs": now_ms, "config": {**config, "visibilityKnown": config["visibilityTimeoutS"] is not None},
        "held": held, "capped": capped,
        "instrumented": instrumented,
        "totals": tot, "uninstrumented": unins,
        "notSeenOnDlq": not_seen, "seenOnDlq": seen_on_dlq, "deleted": deleted,
        # most first; a stable sort keeps the first seen first among equal counts, as Array.prototype.sort does
        "producers": sorted(({"name": k, "n": n} for k, n in by_producer.items()), key=lambda p: -p["n"]),
    }


def _held_of(db, keys: set, name: str, cutoff_ns: int, from_ns: int, limit: int) -> list:
    """The oldest ``limit`` of the Stuck list's candidates first seen in [from_ns, cutoff_ns) that are on the queue
    whose stored keys are ``keys``, as (message ID, its rows), oldest first. The store filters by name only, so a
    queue of this name in another account or region is skipped here, a page at a time, and its messages don't take
    this queue's places in the limit (nor turn ``capped`` on). RabbitMQ messages are skipped the same way."""
    out: list = []
    after = None
    while len(out) < limit:
        page = db.stuck_messages(cutoff_ns, limit, name, from_ns=from_ns, after=after)
        rows_of = db.messages_rows([c.message_id for c in page])
        for c in page:
            rows = rows_of.get(c.message_id)
            if not rows or rabbitmq.is_rabbitmq(rows):
                continue
            # a queue of this name in another account or region is another queue
            if any(queues_mod.key(r["queue"], r["queue_url"]) in keys for r in rows
                   if r["queue"] == name and r["kind"] in ("send", "receive")):
                out.append((c.message_id, rows))
        if len(page) < limit:
            break
        after = (page[-1].first_ns, page[-1].message_id)
    return out[:limit]


def _steps_of(db, keys: set, name: str, from_ns: int, to_ns: int, limit: int = DELETED_LIMIT) -> list:
    """The newest ``limit`` deletes and Lambda outcomes in [from_ns, to_ns) on the queue whose stored keys are
    ``keys``, newest first. The store filters by name only, so the steps of a queue of this name in another account
    or region are skipped here, a page at a time, and don't take this queue's places in the limit."""
    out: list = []
    after = None
    while len(out) < limit:
        page = db.step_rows(from_ns, to_ns, limit, queue=name, kinds=("delete", "outcome"), after=after)
        out += [r for r in page if queues_mod.key(r["queue"], r["queue_url"]) in keys]
        if len(page) < limit:
            break
        after = (page[-1]["time_ns"], page[-1]["trace_id"], page[-1]["span_id"])
    return out[:limit]


def config_of(settings: Optional[dict], mrc: Optional[int], lambda_timeout_s: int) -> dict:
    """The queue's ``config``: from GetQueueAttributes (``settings``, aws_figures.settings_from) when read, else
    unknown but for the server's maxReceiveCount (``mrc``). When read, the queue's redrive policy is the one that
    applies: a queue without one has no maxReceiveCount, whatever the server's setting."""
    s = settings or {}
    return {"visibilityTimeoutS": s.get("visibilityTimeoutS"), "retentionS": s.get("retentionS"),
            "maxReceiveCount": s.get("maxReceiveCount") if settings is not None else mrc, "dlq": s.get("dlqName"),
            "arn": s.get("arn"),
            "lambdaTimeoutS": lambda_timeout_s}


def view(db, want: list, place, now_ns: int, lambda_timeout_s: int, max_receive_counts: Optional[dict] = None,
         *, limit: int = LIST_LIMIT, settings: Optional[dict] = None, totals: Optional[dict] = None) -> dict:
    """The view of the queue whose stored keys are ``want`` (queues.resolve) and whose place is ``place``.
    ``settings`` and ``totals``: what SQS said about the queue (the module's docstring), None when not read."""
    name = place.name
    keys = set(want)
    now_ms = _ms(now_ns)
    config = config_of(settings, (max_receive_counts or {}).get(name), lambda_timeout_s)
    if settings is not None:  # the timelines go by the queue's own redrive policy too
        max_receive_counts = {k: v for k, v in (max_receive_counts or {}).items() if k != name}
        if config["maxReceiveCount"]:
            max_receive_counts[name] = config["maxReceiveCount"]
    retention_s = min(config["retentionS"] if config["retentionS"] is not None else SQS_MAX_RETENTION_S,
                      SQS_MAX_RETENTION_S)
    since_ns = now_ns - retention_s * 1_000_000_000
    found = _held_of(db, keys, name, now_ns + 1, since_ns, limit + 1)
    capped = len(found) > limit
    held, not_seen = [], []
    for mid, rows in found[:limit]:
        t = queries.message(db, mid, now_ns, lambda_timeout_s, max_receive_counts, rows=rows)
        if t is None:
            continue
        urls: dict = {}
        for r in rows:
            if r["queue_url"] and r["queue"] not in urls:
                urls[r["queue"]] = r["queue_url"]
        p = place_of(rows, t, name, now_ms, config, events_of_timeline(t, urls=urls, metas=span_metas(rows)))
        if p and "held" in p:
            held.append(p["held"])
        elif p:
            not_seen.append(p["notSeen"])

    # what left by a delete in the past minute: accepted deletes and Lambda successes on this queue
    gone = _steps_of(db, keys, name, now_ns - 60_000 * NS_PER_MS, now_ns + 1)
    deleted = [{"id": r["mid"], "atMs": _ms(r["time_ns"])} for r in reversed(gone)
               if (r["kind"] == "delete" and r["result"] == "ok") or (r["kind"] == "outcome" and r["outcome"] == "success")]
    # moves SQS reported in the past hour
    seen = sum(1 for e in db.queue_events(want, now_ms - 3_600_000, now_ms) if e[1] == "dlq_seen")
    return summarize(held, not_seen, now_ms=now_ms, config=config, capped=capped, deleted=deleted, seen_on_dlq=seen,
                     totals=totals)


__all__ = ["view", "config_of", "place_of", "summarize", "not_deleted_text", "SQS_DEFAULT_VISIBILITY_S",
           "SQS_MAX_RETENTION_S"]
