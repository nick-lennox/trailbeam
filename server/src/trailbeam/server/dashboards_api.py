"""The dashboards JSON API (QV-652, the second part of QV-591: dashboards in self-hosted Trailbeam).

The hosted app's dashboard server actions in Python (lib/dashboards/actions.ts and data.ts: make, save, star, lock,
duplicate and delete a dashboard), with the same checks and the same error texts, plus one widget at a time and the
figures the widgets draw, worked out here by the measures engine (measures.py, QV-632) so a client need not run it.

Routes (``ROUTES``), all under ``/api/dashboards``, JSON in and out, records with the hosted app's field names
(dashboards_store.py):

- ``GET    /``                            the dashboards, starred first, then the latest changed
- ``POST   /``                            {name, def?, imported?, time?}: ``def`` a definition, or ``imported`` one as
                                          JSON text (the hosted app's Import); without either an empty dashboard
- ``GET    /{id}``                        one dashboard
- ``PATCH  /{id}``                        any of {name, def, time, starred, version}; a locked dashboard takes only a star
- ``POST   /{id}/lock``                   {lock: true or false}
- ``POST   /{id}/duplicate``              a copy named "<name> copy"
- ``DELETE /{id}``                        refused while it is locked
- ``POST   /{id}/widgets``                {widget}: added at the end of the definition
- ``PATCH  /{id}/widgets/{wid}``          {widget}: replaces the widget with that ID (its ID stays)
- ``DELETE /{id}/widgets/{wid}``
- ``GET    /{id}/measures``               what every widget draws over a time range (``measures``, below)
- ``GET    /{id}/widgets/{wid}/measures`` what that widget draws

Every definition is checked with measures.check_def (each widget with measures.problem) before it is stored, so a
widget added or changed alone is held to the same checks and the same size limit as a whole definition. A widget
change reads the dashboard, changes its definition and writes it back only if no one changed the dashboard in
between (the store's ``version``); after three such races it answers 409.

**Who may do what** (as the hosted app, on open-source Trailbeam's people and roles, access.py): anyone the server
lets in at MEMBER makes, changes, duplicates and deletes dashboards, and anyone who may read the API reads them. A
locked dashboard changes (name, definition, widgets, delete) only after it is unlocked; whoever locked it, or an
admin, may unlock it. Before accounts exist, whoever is let in is the operator and may. A dashboard's owner is its
maker (``createdBy``, ``createdById``); locks are compared by person ID, never by name (QV-257).

**Measures.** ``?range=`` a preset (measures.PRESETS), or ``?from=&to=`` in ms (the end excluded, at most 30 days);
without either, the dashboard's ``time``, else the past hour. A widget with its own ``time`` keeps it, as on the
hosted dashboard. ``?n=`` the buckets of a series (60, the hosted dashboard's; 1 to 200). ``?data=1`` adds the
EventData the figures were worked out from. The EventData is read from the store (``event_data``): the events of
the messages with spans in the range, the most recently active first, up to EVENT_MESSAGES of them, each message's
rows through the timeline (timeline.py), as the hosted app's lib/app/live-events.ts does. A range that ends before
now is read on its own, up to its end, so the messages since do not crowd it out (QV-685). Self-hosted Trailbeam
does not read AWS, so ``queueLive`` gives every queue ``status: "not_configured"``: queue measures (depth, age,
headroom) have no figures and say why (measures.no_depth). Infinity and NaN are the strings "Infinity", "-Infinity"
and "NaN", as in contract/fixtures/measures-parity.json, because JSON has no such numbers.

**What a measures request may cost** (QV-684): a definition holds at most MAX_WIDGETS widgets, MAX_QUERIES queries a
widget and MAX_COLUMNS columns a queue table (_checked); a query's lines are the largest MAX_LINES and one
"Other (k)" line for the rest; and once a dashboard's answer holds MEASURES_BYTES of JSON or has taken
MEASURES_SECONDS, the widgets left are ``skipped``, to be asked for one at a time.

The routes answer only when ``TRAILBEAM_DASHBOARDS`` is on (1, true, yes or on), so nothing half done shows by
default; the dashboard screens are later parts of QV-591.
"""
from __future__ import annotations

import json
import math
import os
import re
import time as _time
import uuid
from collections.abc import Callable
from datetime import datetime
from typing import Any

from . import measures as M
from . import timeline
from .alerts_api import ApiProblem
from .dashboards_store import DashboardStore, DashboardTables, MemoryDashboards
from .queues import _body_size, place_of_key

ENV = "TRAILBEAM_DASHBOARDS"
#: the preset IDs a range or a dashboard's time can name
PRESET_IDS = [str(p["id"]) for p in M.PRESETS]
NAME_MAX = 80
#: pasted JSON is refused before parsing once it is far past what check_def allows (actions.ts MAX_IMPORT_CHARS)
MAX_IMPORT_CHARS = 4 * M.MAX_DEF_BYTES
#: how many messages' events a measures read takes at most (the hosted app's EVENT_MESSAGES)
EVENT_MESSAGES = 2000
#: the longest range a measures read covers: spans are kept 30 days (the hosted app's EVENT_DAYS)
MAX_RANGE_MS = 30 * M.DAY
#: a range that ends before now also reads the messages whose newest span is up to this long after its end, so a
#: message still being worked at the end (a receive just before, its delete just after) keeps its events in the
#: range: the store orders and bounds messages by their newest span only. Read only with what EVENT_MESSAGES
#: leaves after the range's own messages, newest first, so when more than that were active in this time after
#: the end, the ones read are the newest of them, not the straddlers, and the answer says ``capped`` (QV-685)
STRADDLE_MS = 15 * M.MIN
#: the events a widget's answer carries at most (event lists, distributions, heatmaps)
MAX_EVENTS = 500
N_DEFAULT = 60
WRITE_TRIES = 3
# What one measures request may cost (QV-684). A definition's 256 KB holds some 1,600 widgets, and a group-by on a
# field with a value per message (message_id) makes a line per message: without these one GET took minutes of CPU
# and gigabytes. measures.check_def has none of them, as the hosted app's checks have none; _checked adds them.
#: widgets on one dashboard (the hosted app's starter dashboards have up to 12)
MAX_WIDGETS = 50
#: queries on one widget (the hosted app's starters use up to 3)
MAX_QUERIES = 5
#: a queue table's columns: each measure once
MAX_COLUMNS = 19
#: lines per query, the largest first; the rest are one "Other (k)" line (measures.other_line). The hosted
#: dashboard draws the top 5 and Other on a chart and the top 10 on a top list, so both are drawn from these.
MAX_LINES = 10
#: once a dashboard's answer holds this much JSON, or has taken this long, the widgets left are skipped: each comes
#: back with ``skipped`` (SKIPPED) and no figures, to be asked for on its own (/widgets/{wid}/measures)
MEASURES_BYTES = 8 * 1024 * 1024
MEASURES_SECONDS = 5.0

NO_NAME = "Give the dashboard a name."
TOO_BIG = f"That dashboard is too large. A dashboard can be at most {M.MAX_DEF_BYTES // 1024} KB."
NOT_JSON = "That is not valid JSON."
LOCKED = "This dashboard is locked. Unlock it to change it."
LOCKED_DELETE = "This dashboard is locked. Unlock it before deleting it."
GONE = "This dashboard no longer exists."
WIDGET_GONE = "That widget is no longer on this dashboard."
BUSY = "Someone else changed this dashboard at the same moment. Reload it and try again."
SKIPPED = ("This widget's figures did not fit in the dashboard's answer. Ask for this widget on its own: "
           "GET /api/dashboards/{id}/widgets/{wid}/measures.")
NOT_QUEUE_LIVE = ("No data. This server does not read queue figures from AWS, so depth, age and headroom have "
                  "none here.")

QUEUE_WIDGETS = ("queue_table", "status_grid")
EVENT_WIDGETS = ("event_list", "distribution", "heatmap")


# ── EventData from the store ─────────────────────────────────────────────────────────────────────────────────────

_NS = 1_000_000
_OUTCOME_WORDS = {"failed_exception": "The handler threw", "failed_reported": "Reported in batchItemFailures",
                  "failed_invalid_response": "Invalid batchItemFailures response"}


def handler_failed(type_: str | None, message: str | None, outcome: str | None) -> str:
    """A failed handler's one line (live-events.ts handlerFailed)."""
    lead = (outcome and _OUTCOME_WORDS.get(outcome)) or "The handler failed"
    if type_:
        return f"{lead}: {type_}{f': {message}' if message else ''}"
    return f"{lead}: {message}" if message else lead


def _ms_of(ns: Any) -> int:
    return int(ns) // _NS


def _iso_ms(text: Any) -> float:
    if not isinstance(text, str):
        return math.nan
    try:
        return datetime.fromisoformat(text).timestamp() * 1000
    except ValueError:
        return math.nan


def _row(r, key: str):
    try:
        return r[key]
    except (IndexError, KeyError):
        return None


def events_of(rows: list, now_ns: int, lambda_timeout_s: int = timeline.DEFAULT_LAMBDA_TIMEOUT_S,
              max_receive_counts: dict | None = None) -> list[dict]:
    """The Event explorer's rows of one message (the hosted app's Ev), from its stored rows: live-events.ts eventsOf
    without its AWS-side steps (CloudTrail, function logs), which self-hosted Trailbeam does not read. A RabbitMQ
    message's ack is a delete and its reject or requeue a processing error."""
    if not rows:
        return []
    t = timeline.build(list(rows), now_ns, lambda_timeout_s, max_receive_counts)
    mid = str(t["message_id"])
    evs = t["events"]
    by_span = {(_row(r, "trace_id"), _row(r, "span_id")): r for r in rows}
    tenant = next((v for r in rows if (v := _row(r, "tenant"))), None)

    def svc(e: dict) -> str:
        return (e.get("consumer") or {}).get("service_name") or ""

    def row_of(e: dict):
        sp = e.get("span") or {}
        return by_span.get((sp.get("trace_id"), sp.get("span_id")))

    sends = [e for e in evs if e["type"] == "sent"]
    receives = [e for e in evs if e["type"] == "received"]
    outcomes = [e for e in evs if e["type"] == "lambda_outcome"]
    producer = svc(sends[0]) if sends else ""

    def receive_of(delivery):
        return next((x for x in receives if x.get("delivery") == delivery), None)

    def consumer_of(delivery) -> str:
        r = receive_of(delivery)
        return svc(r) if r else svc(receives[-1]) if receives else ""

    first_send = _ms_of(sends[0]["end_unix_nano"]) if sends else None
    out: list[dict] = []

    def handler_ms(r, start, end):
        if r:
            return max(0, end - _ms_of(r["end_unix_nano"]))
        return end - start if start is not None and end > start else None

    def push(e: dict, kind: str, ms: float, **fields) -> None:
        if not math.isfinite(ms):
            return
        queue = str(e.get("queue") or t.get("current_queue") or "")
        error = kind in ("processing_error", "dead_letter") or fields.get("status") == "error"
        own = row_of(e)
        attrs = {"message.ApproximateReceiveCount": e.get("receive_count") if kind == "receive"
                 and (e.get("receive_count") or 0) > 0 else None,
                 "message.tenant": tenant,
                 "message.SentTimestamp": e.get("sent_timestamp_ms"),
                 "message.ApproximateFirstReceiveTimestamp": e.get("first_receive_timestamp_ms")}
        made: dict = {"key": f"{mid}:{e.get('id')}", "ms": int(ms), "event": kind, "status": "error" if error else "ok",
                      "queue": queue, "messageId": mid, "producer": producer, "detail": "", "source": "package",
                      "hasPage": True, "live": True}
        if tenant:
            made["tenant"] = tenant
        if kind == "send" and own is not None and (size := _body_size(own)) is not None:
            made["sizeBytes"] = size
        made["attrs"] = {k: v for k, v in attrs.items() if v is not None}
        made.update({k: v for k, v in fields.items() if v is not None})
        out.append(made)

    for e in evs:
        end = _ms_of(e["end_unix_nano"]) if e.get("end_unix_nano") else math.nan
        start = _ms_of(e["start_unix_nano"]) if e.get("start_unix_nano") else None
        ty = e["type"]
        if ty == "sent":
            push(e, "send", end, service=svc(e), consumer=consumer_of(1),
                 call="SendMessageBatch" if e.get("batch_entry_id") else "SendMessage",
                 **({"deduplicated": True, "detail": str(e.get("reason") or "")} if e.get("deduplicated") else {}))
        elif ty == "received":
            sent_at = first_send if first_send is not None else e.get("sent_timestamp_ms")
            wait = max(0, end - sent_at) if e.get("delivery") == 1 and e.get("receive_count") == 1 \
                and sent_at is not None else None
            lam = (e.get("consumer") or {}).get("source") == "lambda"
            push(e, "receive", end, service=svc(e), consumer=svc(e), receiveCount=e.get("receive_count"), waitMs=wait,
                 call="Lambda trigger" if lam else "ReceiveMessage",
                 detail="Received from the dead-letter queue" if e.get("dlq_source_arn") else "")
        elif ty in ("deleted", "acked"):
            r = receive_of(e.get("delivery"))
            took = max(0, start - _ms_of(r["end_unix_nano"])) if r and start is not None else None
            failed = e.get("result") not in ("ok", None)
            call = ("DeleteMessageBatch" if e.get("batch_entry_id") else "DeleteMessage") if ty == "deleted" else "ack"
            code = e.get("error_code")
            detail = (f"{call} failed{f': {code}' if code else ''}" if failed
                      else "Deleted with the receipt handle of an earlier receive" if e.get("status") == "stale_handle"
                      else "No observed receive returned this receipt handle" if e.get("status") == "unmatched_handle"
                      else "")
            push(e, "delete", end, service=svc(e), consumer=svc(e), receiveCount=r.get("receive_count") if r else None,
                 durationMs=took, detail=detail, status="error" if failed else "ok", call=call)
        elif ty in ("rejected", "requeued"):
            r = receive_of(e.get("delivery"))
            took = max(0, start - _ms_of(r["end_unix_nano"])) if r and start is not None else None
            push(e, "processing_error", end, service=svc(e), consumer=svc(e),
                 receiveCount=r.get("receive_count") if r else None, durationMs=took,
                 detail="Rejected by the consumer" if ty == "rejected" else "Requeued by the consumer")
        elif ty == "lambda_outcome":
            if e.get("outcome") == "success":
                continue  # its delete is the inferred deleted_by_lambda step
            r = receive_of(e.get("delivery"))
            push(e, "processing_error", end, service=svc(e), consumer=svc(e),
                 receiveCount=r.get("receive_count") if r else None, durationMs=handler_ms(r, start, end),
                 detail=handler_failed(e.get("error_type"), e.get("error_message"), e.get("outcome")))
        elif ty == "deleted_by_lambda":
            at = _iso_ms((e.get("window") or {}).get("from"))
            o = next((x for x in outcomes if x.get("outcome") == "success" and x.get("delivery") == e.get("delivery")
                      and _ms_of(x["end_unix_nano"]) == at), None) or \
                next((x for x in outcomes if x.get("outcome") == "success" and x.get("delivery") == e.get("delivery")),
                     None)
            r = receive_of(e.get("delivery"))
            took = handler_ms(r, _ms_of(o["start_unix_nano"]) if o.get("start_unix_nano") else None,
                              _ms_of(o["end_unix_nano"])) if o else None
            push(e, "delete", at, service=svc(r) if r else "", consumer=svc(r) if r else "",
                 receiveCount=r.get("receive_count") if r else None, durationMs=took,
                 detail="The handler succeeded, so the Lambda trigger deleted the message. That delete is inferred",
                 source="inferred", call="Inferred delete")
        elif ty == "lambda_invocation_ended_without_outcome":
            r = receive_of(e.get("delivery"))
            push(e, "processing_error", _iso_ms((e.get("window") or {}).get("to")), service=svc(r) if r else "",
                 consumer=svc(r) if r else "", receiveCount=r.get("receive_count") if r else None,
                 detail="No outcome within the Lambda timeout: the invocation most likely timed out or ran out of memory",
                 source="inferred")
        elif ty == "visibility_timeout_expired":
            before = receive_of(e.get("after_delivery"))
            push(e, "visibility_expired", _iso_ms((e.get("window") or {}).get("to")), service="sqs",
                 consumer=svc(before) if before else "", receiveCount=before.get("receive_count") if before else None,
                 detail=f"Receive {e.get('after_delivery')} was not deleted in time, so the message became visible "
                        "again. Inferred from the next receive", source="inferred")
        elif ty == "moved_to_dead_letter_queue":
            dlq = receive_of(e.get("reported_on_delivery"))
            n = e.get("reported_on_delivery")
            push({**e, "queue": e.get("from_queue")}, "dead_letter", _iso_ms((e.get("window") or {}).get("to")),
                 service="sqs", consumer=consumer_of(n - 1 if isinstance(n, int) else None),
                 receiveCount=dlq.get("receive_count") if dlq else None,
                 detail=f"Moved from {e.get('from_queue')} to {e.get('to_queue')}. SQS reported it on the receive "
                        f"from {e.get('to_queue')}")
    return out


def event_data(db, now_ms: int, since_ms: int, lambda_timeout_s: int = timeline.DEFAULT_LAMBDA_TIMEOUT_S,
               max_receive_counts: dict | None = None, max_messages: int = EVENT_MESSAGES,
               to_ms: int | None = None) -> dict:
    """The measures engine's EventData from a store: the events in [since_ms, end) of the messages with spans in
    that range, the most recently active first, up to ``max_messages`` (``capped`` when more had), newest event
    first (live-events.ts loadEvents). ``end``: ``to_ms`` (excluded) if it is given and before now, else just after
    ``now_ms``. A range that ends before now reads its own messages first and then, with what the cap leaves, the
    ones last active up to STRADDLE_MS after it (QV-685): so a past day is not crowded out by the messages since.
    ``queueLive``: every queue "not_configured" (the module's docstring)."""
    now_ns = now_ms * _NS
    end_ms = now_ms + 1 if to_ms is None else min(to_ms, now_ms + 1)
    mids = db.recent_messages(max_messages + 1, from_ns=since_ms * _NS, to_ns=end_ms * _NS)
    capped = len(mids) > max_messages
    mids = mids[:max_messages]
    if not capped and end_ms <= now_ms:  # and the messages still at work at its end, newest span soon after it
        left = max_messages - len(mids)
        after = db.recent_messages(left + 1, from_ns=end_ms * _NS,
                                   to_ns=min(end_ms + STRADDLE_MS, now_ms + 1) * _NS)
        capped = len(after) > left
        mids = after[:left] + mids  # most recently active first
    rows = db.messages_rows(mids)
    found: list[tuple[dict, int]] = []
    for mid in mids:
        for e in events_of(rows.get(mid) or [], now_ns, lambda_timeout_s, max_receive_counts):
            if since_ms <= e["ms"] < end_ms:
                found.append((e, len(found)))
    found.sort(key=lambda x: (-x[0]["ms"], -x[1]))
    events = [e for e, _ in found]
    try:  # the queues a filter can pick from: the events' and every queue the store has seen (liveEventData)
        known = {place_of_key(k).name for k in db.queue_keys()}
    except NotImplementedError:
        known = set()
    queues = sorted({e["queue"] for e in events} | known)
    return {"live": True, "events": events, "now": now_ms, "queues": queues, "capped": capped,
            "queueLive": {q: {"status": "not_configured", "detail": NOT_QUEUE_LIVE} for q in queues}}


# ── the figures a widget draws ───────────────────────────────────────────────────────────────────────────────────

def plain(v):
    """Infinity and NaN as strings, so JSON holds them (as contract/fixtures/make-measures-parity.py writes them)."""
    if isinstance(v, float) and not math.isfinite(v):
        return "NaN" if math.isnan(v) else ("Infinity" if v > 0 else "-Infinity")
    if isinstance(v, dict):
        return {k: plain(x) for k, x in v.items()}
    if isinstance(v, (list, tuple)):
        return [plain(x) for x in v]
    return v


def _queue_ids(w: dict) -> list:
    if w.get("type") == "queue_table":
        return list(w.get("columns") or M.TABLE_COLUMNS)
    return [q.get("measure") for q in w.get("queries") or [] if isinstance(q, dict)]


def widget_measures(w: dict, variables: list, time: dict, data: dict, n: int = N_DEFAULT,
                    no_depth_text: str | None = None) -> dict:
    """What one widget draws, worked out as the hosted dashboard's widgets work it out (components/dashboards/
    widgets.tsx) over the widget's own time range if it has one, else ``time``:

    - ``queries``: per query, its measure's lines (measures.series_for with the query's group-by, ``n`` buckets):
      the largest MAX_LINES, then the rest as one line "Other (k)" with ``others``: k (measures.other_line); and
      whether the measure has no figure (``depthMissing``) or no history (``noHistory``);
    - ``buckets``: the events per bucket, ok and failed, for a timeseries of the events measure;
    - ``events``: the matching events (up to MAX_EVENTS; ``eventsCapped``), for event lists, distributions and
      heatmaps;
    - ``queues``: per queue the filter leaves, its health and each column's figure, for queue tables and status grids;
    - ``alert``: the widget's alert state now (measures.alert_of), or None;
    - ``queueOnlyTerms``: the filter terms a queue measure can't apply; ``noDepth``: why queue measures have no figure
      (``no_depth_text`` when given and no queue has figures: a server that reads none says so even with no queues).
    Not plain(): the caller makes it JSON."""
    t = M.preset(w["time"], data["now"]) if w.get("time") else time
    f = M.effective_filter(w, variables)
    out: dict = {"id": w.get("id"), "type": w.get("type"), "title": w.get("title"), "filter": f, "time": t}
    qs = []
    for q in w.get("queries") or []:
        m = M.measure(q.get("measure"))
        if m is None:
            continue
        missing = bool(M.depth_missing(m, data))
        qs.append({"measure": m.id, "label": m.label, "unit": m.unit, "groupBy": q.get("group_by") or None,
                   "depthMissing": missing, "noHistory": M.no_history(m, data, t),
                   "lines": [] if missing else M.series_for(m, f, q.get("group_by") or None, t, n, data,
                                                            top=MAX_LINES)})
    out["queries"] = qs
    if w.get("type") == "timeseries" and qs and qs[0]["measure"] == "events":
        out["buckets"] = M.event_buckets(f, t, n, data)
    if w.get("type") in EVENT_WIDGETS:
        evs = M.events_for(f, t, data)
        out["events"], out["eventsCapped"] = evs[:MAX_EVENTS], len(evs) > MAX_EVENTS
    if w.get("type") in QUEUE_WIDGETS:
        cols = [m for m in (M.measure(c) for c in (_queue_ids(w) if w["type"] == "queue_table" else [])) if m]
        rows = []
        for name in M.queues_for(M.parse(f), data.get("queues") or []):
            vals = {}
            for m in cols:
                if m.queue:
                    vals[m.id] = M.queue_value(m, name, data, t)
                else:
                    s = M.series_for(m, f'queue:"{name}"', None, t, 2, data)
                    vals[m.id] = s[0]["now"] if s else 0
            rows.append({"name": name, "tone": M.queue_tone(name, data), "values": vals})
        out["queues"] = rows
    a = M.alert_of(w, variables, data)
    out["alert"] = None if a is None else {k: a[k] for k in ("tone", "value", "limit", "queue")}
    ids = _queue_ids(w)
    queueish = w.get("type") == "status_grid" or any((m := M.measure(i)) is not None and m.queue for i in ids)
    out["queueOnlyTerms"] = M.queue_only_terms(f) if queueish else []
    missing = any((m := M.measure(i)) is not None and M.depth_missing(m, data) for i in ids)
    some = any(q.get("status") == "ok" for q in (data.get("queueLive") or {}).values())
    out["noDepth"] = None if not missing else no_depth_text if no_depth_text and not some else M.no_depth(data)
    return out


# ── requests ─────────────────────────────────────────────────────────────────────────────────────────────────────

#: (method, path template, handler, who may: "read" or "member")
ROUTES: tuple[tuple[str, str, str, str], ...] = (
    ("GET", "/api/dashboards", "list_dashboards", "read"),
    ("POST", "/api/dashboards", "create", "member"),
    ("GET", "/api/dashboards/{id}", "get", "read"),
    ("PATCH", "/api/dashboards/{id}", "save", "member"),
    ("POST", "/api/dashboards/{id}/lock", "lock", "member"),
    ("POST", "/api/dashboards/{id}/duplicate", "duplicate", "member"),
    ("DELETE", "/api/dashboards/{id}", "delete", "member"),
    ("POST", "/api/dashboards/{id}/widgets", "add_widget", "member"),
    ("PATCH", "/api/dashboards/{id}/widgets/{wid}", "change_widget", "member"),
    ("DELETE", "/api/dashboards/{id}/widgets/{wid}", "remove_widget", "member"),
    ("GET", "/api/dashboards/{id}/measures", "measures", "read"),
    ("GET", "/api/dashboards/{id}/widgets/{wid}/measures", "measures", "read"),
)
_SEG = "([^/]{1,200})"
_RX = tuple((m, re.compile("^" + re.escape(t).replace(re.escape("{id}"), _SEG).replace(re.escape("{wid}"), _SEG)
                           + "$"), h, w) for m, t, h, w in ROUTES)


def route(method: str, path: str) -> tuple[str, tuple, str] | None:
    """The dashboards route of a request: (handler, its path IDs, "read" or "member"), or None."""
    from urllib.parse import unquote

    for m, rx, h, w in _RX:
        if m == method:
            hit = rx.match(path)
            if hit:
                return h, tuple(unquote(g) for g in hit.groups()), w
    return None


def is_dashboards_path(path: str) -> bool:
    return path == "/api/dashboards" or path.startswith("/api/dashboards/")


def _by(who) -> str:
    """Who made or changed something, as stored and shown: the person's name, else their email, else how the
    request was let in."""
    return getattr(who, "name", None) or getattr(who, "email", None) or getattr(who, "via", None) or "none"


def _by_id(who) -> str | None:
    uid = getattr(who, "user_id", None)
    return None if uid is None else str(uid)


def _one(q: dict, key: str) -> str | None:
    v = q.get(key)
    if isinstance(v, list):
        v = v[0] if v else None
    return v if isinstance(v, str) else None


def _clean_name(v: Any) -> str:
    return v.strip()[:NAME_MAX] if isinstance(v, str) else ""


def _check_time(v: Any) -> str | None:
    if v is None:
        return None
    if not isinstance(v, str) or v not in PRESET_IDS:
        raise ApiProblem(400, f'"time" is a preset: {", ".join(PRESET_IDS)}, or null.')
    return v


def _checked(raw: Any) -> dict:
    """measures.check_def, then what one measures request may cost (MAX_WIDGETS, MAX_QUERIES, MAX_COLUMNS)."""
    c = M.check_def(raw)
    if "error" in c:
        raise ApiProblem(400, c["error"])
    d = c["def"]
    if len(d["widgets"]) > MAX_WIDGETS:
        raise ApiProblem(400, f"That dashboard has {len(d['widgets'])} widgets. A dashboard can have at most "
                              f"{MAX_WIDGETS}.")
    for i, w in enumerate(d["widgets"]):
        if len(w.get("queries") or []) > MAX_QUERIES:
            raise ApiProblem(400, f"Widget {i + 1}: a widget can have at most {MAX_QUERIES} queries.")
        if w.get("type") == "queue_table" and len(w.get("columns") or []) > MAX_COLUMNS:
            raise ApiProblem(400, f"Widget {i + 1}: a queue table can have at most {MAX_COLUMNS} columns.")
    return d


class DashboardsApi:
    """The handlers over one store, for one workspace (self-hosted Trailbeam has one).

    ``data(since_ms, now_ms, to_ms)``: the EventData the measures read (event_data on the server's store); ``to_ms``
    None for a range that ends now. ``can_unlock``:
    whether a request's Who may lift someone else's lock (an admin, or anyone before accounts exist); the default
    lets anyone. ``now``: ms. ``no_depth_text``: why queue measures have no figures, when the data never has any
    (widget_measures)."""

    def __init__(self, store: DashboardStore, workspace: str, data: Callable[[int, int, int | None], dict],
                 can_unlock: Callable[[Any], bool] = lambda who: True,
                 now: Callable[[], int] = lambda: _time.time_ns() // _NS, no_depth_text: str | None = None):
        self.store, self.ws, self.data, self.can_unlock, self.now = store, workspace, data, can_unlock, now
        self.no_depth_text = no_depth_text
        #: seconds, for MEASURES_SECONDS
        self.clock: Callable[[], float] = _time.monotonic

    def call(self, method: str, path: str, body: dict | None, who, base: str | None = None) -> tuple[int, dict]:
        """(status, JSON body) for a dashboards route. ``body``: the JSON body, or for a GET the query (name ->
        value, or a list of values). Raises ApiProblem (alerts_api's: 404 for no such route, 400 for a refused
        change, 409 for a lost race)."""
        r = route(method, path)
        if r is None:
            raise ApiProblem(404, f"no such endpoint: {method} {path}")
        handler, ids, _ = r
        status, obj = getattr(self, handler)(*ids, body if isinstance(body, dict) else {}, who)
        return status, plain(obj)

    def _get(self, did: str) -> dict:
        d = self.store.get_dashboard(self.ws, did)
        if d is None:
            raise ApiProblem(404, GONE)
        return d

    def _new(self, name: str, def_: dict, who, time: str | None = None, starred: bool = False) -> dict:
        at = self.now()
        d: dict[str, Any] = {"id": uuid.uuid4().hex[:8], "name": name, "def": def_, "time": time, "starred": starred,
             "lockedBy": None, "lockedById": None, "createdBy": _by(who), "createdById": _by_id(who),
             "updatedBy": _by(who), "createdAt": at, "updatedAt": at}
        while not self.store.add_dashboard(self.ws, d):  # an 8-character ID taken already: another one
            d["id"] = uuid.uuid4().hex[:8]
        return self._get(d["id"])

    # ---- dashboards -------------------------------------------------------------------------------------------

    def list_dashboards(self, q: dict, who) -> tuple[int, dict]:
        return 200, {"dashboards": self.store.list_dashboards(self.ws), "canUnlock": self.can_unlock(who)}

    def get(self, did: str, q: dict, who) -> tuple[int, dict]:
        return 200, {"dashboard": self._get(did)}

    def create(self, body: dict, who) -> tuple[int, dict]:
        """createDashboardAction: a name, then an imported definition (JSON text), a definition, or an empty one.
        The hosted app's starters are not here: a definition can be any of them."""
        name = _clean_name(body.get("name"))
        if not name:
            raise ApiProblem(400, NO_NAME)
        def_: dict = {"variables": [], "sections": [], "widgets": []}
        imported = body.get("imported")
        if isinstance(imported, str) and imported:
            if len(imported) > MAX_IMPORT_CHARS:
                raise ApiProblem(400, TOO_BIG)
            try:
                raw = json.loads(imported)
            except ValueError:
                raise ApiProblem(400, NOT_JSON) from None
            def_ = _checked(raw)
        elif body.get("def") is not None:
            def_ = _checked(body["def"])
        return 201, {"dashboard": self._new(name, def_, who, _check_time(body.get("time")))}

    def save(self, did: str, body: dict, who) -> tuple[int, dict]:
        """saveDashboardAction and starDashboardAction: a name, a definition and a time range change only while
        it is unlocked; a star always. ``version``: change it only if it is still at that version (409 if not)."""
        fields: dict = {}
        if "name" in body:
            if not _clean_name(body["name"]):
                raise ApiProblem(400, NO_NAME)
            fields["name"] = _clean_name(body["name"])
        if "def" in body:
            fields["def"] = _checked(body["def"])
        if "time" in body:
            fields["time"] = _check_time(body["time"])
        guarded = bool(fields)
        if guarded:
            fields.update(updatedBy=_by(who), updatedAt=self.now())
        if "starred" in body:
            if not isinstance(body["starred"], bool):
                raise ApiProblem(400, '"starred" is true or false.')
            fields["starred"] = body["starred"]
        if not fields:
            return 200, {"dashboard": self._get(did)}
        version = body.get("version")
        if version is not None and (not isinstance(version, int) or isinstance(version, bool)):
            raise ApiProblem(400, '"version" is a whole number.')
        out, d = self.store.update_dashboard(self.ws, did, fields, unlocked=guarded, version=version)
        return 200, {"dashboard": self._outcome(out, d)}

    def _outcome(self, out: str, d: dict | None) -> dict:
        if out == "missing":
            raise ApiProblem(404, GONE)
        if out == "locked":
            raise ApiProblem(400, LOCKED)
        if out == "conflict" or d is None:
            raise ApiProblem(409, BUSY)
        return d

    def lock(self, did: str, body: dict, who) -> tuple[int, dict]:
        """lockDashboardAction: locking a locked one changes nothing; unlocking someone else's lock needs an admin
        (compared by person ID; a lock without one only an admin lifts)."""
        lock = body.get("lock")
        if not isinstance(lock, bool):
            raise ApiProblem(400, '"lock" is true or false.')
        d = self._get(did)
        if lock:
            got = self.store.lock_dashboard(self.ws, did, _by(who), _by_id(who), self.now())
            if got is None:
                raise ApiProblem(404, GONE)
            return 200, {"dashboard": got, "lockedBy": got["lockedBy"]}
        if d["lockedBy"] is not None and (d["lockedById"] is None or d["lockedById"] != _by_id(who)) \
                and not self.can_unlock(who):
            raise ApiProblem(403, f"{d['lockedBy']} locked this dashboard. They, or an admin, can unlock it.")
        out, after = self.store.update_dashboard(self.ws, did, {"lockedBy": None, "lockedById": None})
        return 200, {"dashboard": self._outcome(out, after), "lockedBy": None}

    def duplicate(self, did: str, body: dict, who) -> tuple[int, dict]:
        d = self._get(did)
        return 201, {"dashboard": self._new(f"{d['name']} copy"[:NAME_MAX], d["def"], who, d.get("time"))}

    def delete(self, did: str, body: dict, who) -> tuple[int, dict]:
        out = self.store.remove_dashboard(self.ws, did, unlocked=True)
        if out == "locked":
            raise ApiProblem(400, LOCKED_DELETE)
        return 200, {"removed": out == "ok"}

    # ---- one widget at a time -----------------------------------------------------------------------------------

    def _change_def(self, did: str, who, change: Callable[[list], list]) -> dict:
        """Reads the dashboard, changes its widgets, checks the definition and stores it if no one changed the
        dashboard in between; WRITE_TRIES times at most."""
        for _ in range(WRITE_TRIES):
            d = self._get(did)
            if d["lockedBy"] is not None:
                raise ApiProblem(400, LOCKED)
            def_ = _checked({**d["def"], "widgets": change(list(d["def"]["widgets"]))})
            out, after = self.store.update_dashboard(self.ws, did, {"def": def_, "updatedBy": _by(who),
                                                                    "updatedAt": self.now()},
                                                     unlocked=True, version=d["version"])
            if out != "conflict":
                return self._outcome(out, after)
        raise ApiProblem(409, BUSY)

    @staticmethod
    def _widget(body: dict) -> dict:
        w = body.get("widget")
        if not isinstance(w, dict):
            raise ApiProblem(400, 'Send the widget as "widget", a JSON object.')
        return w

    def add_widget(self, did: str, body: dict, who) -> tuple[int, dict]:
        """The widget at the end of the definition; without a text "id" it gets one."""
        w = dict(self._widget(body))
        if not isinstance(w.get("id"), str) or not w["id"]:
            w["id"] = uuid.uuid4().hex[:8]
        return 201, {"dashboard": self._change_def(did, who, lambda ws: ws + [w]), "widget": w}

    def change_widget(self, did: str, wid: str, body: dict, who) -> tuple[int, dict]:
        """Replaces the first widget with that ID; the widget keeps its ID."""
        w = {**self._widget(body), "id": wid}

        def change(ws: list) -> list:
            i = next((i for i, x in enumerate(ws) if isinstance(x, dict) and x.get("id") == wid), None)
            if i is None:
                raise ApiProblem(404, WIDGET_GONE)
            return ws[:i] + [w] + ws[i + 1:]

        return 200, {"dashboard": self._change_def(did, who, change), "widget": w}

    def remove_widget(self, did: str, wid: str, body: dict, who) -> tuple[int, dict]:
        def change(ws: list) -> list:
            kept = [x for x in ws if not (isinstance(x, dict) and x.get("id") == wid)]
            if len(kept) == len(ws):
                raise ApiProblem(404, WIDGET_GONE)
            return kept

        return 200, {"dashboard": self._change_def(did, who, change)}

    # ---- measures -------------------------------------------------------------------------------------------

    def time_of(self, q: dict, d: dict, now: int) -> dict:
        """The range a measures request names: ?range=, ?from=&to=, else the dashboard's time, else the past hour."""
        rng, frm, to = _one(q, "range"), _one(q, "from"), _one(q, "to")
        if rng is not None:
            if rng not in PRESET_IDS:
                raise ApiProblem(400, f"range is a preset: {', '.join(PRESET_IDS)}.")
            return M.preset(rng, now)
        if frm is not None or to is not None:
            if frm is None or to is None or not frm.isdigit() or not to.isdigit():
                raise ApiProblem(400, "from and to are both milliseconds since the epoch.")
            a, b = int(frm), int(to)
            if b <= a:
                raise ApiProblem(400, "to must be after from.")
            if b - a > MAX_RANGE_MS:
                raise ApiProblem(400, "A range can be at most 30 days.")
            return {"from": a, "to": b}
        return M.preset(d.get("time") or "1h", now)

    def measures(self, did: str, *rest) -> tuple[int, dict]:
        """``GET /{id}/measures`` (rest: query, who) and ``GET /{id}/widgets/{wid}/measures`` (rest: widget ID,
        query, who): {dashboard, time, now, capped, widgets: [widget_measures]} (and ``data`` with ?data=1)."""
        wid = rest[0] if len(rest) == 3 else None
        q = rest[-2]
        d = self._get(did)
        widgets = [w for w in d["def"]["widgets"] if isinstance(w, dict)]
        if wid is not None:
            widgets = [w for w in widgets if w.get("id") == wid][:1]
            if not widgets:
                raise ApiProblem(404, WIDGET_GONE)
        n_text = _one(q, "n")
        n = N_DEFAULT
        if n_text is not None:
            if not n_text.isdigit() or not 1 <= int(n_text) <= 200:
                raise ApiProblem(400, "n is a whole number from 1 to 200.")
            n = int(n_text)
        # a definition stored before the caps, or past them some other way, is answered for its first MAX_WIDGETS
        widgets = widgets[:MAX_WIDGETS]
        now = self.now()
        t = self.time_of(q, d, now)
        # the ranges that end now (presets, a widget's own time) are read once, from the earliest; a range that
        # ends before now is read on its own, up to its end, so the messages since do not crowd it out (QV-685)
        past = t["to"] < now
        own = [M.preset(w["time"], now)["from"] for w in widgets if w.get("time")]
        live_from = min(own + ([] if past else [t["from"]]), default=None)
        live = None if live_from is None else self.data(int(live_from), now, None)
        data = self.data(int(t["from"]), now, int(t["to"])) if past else live
        start, size, done = self.clock(), 0, []
        for w in widgets:
            if done and (size >= MEASURES_BYTES or self.clock() - start >= MEASURES_SECONDS):
                done.append({"id": w.get("id"), "type": w.get("type"), "title": w.get("title"),
                             "skipped": SKIPPED.format(id=d["id"], wid=w.get("id"))})
                continue
            wm = plain(widget_measures(w, d["def"]["variables"], t, live if w.get("time") else data, n,
                                       self.no_depth_text))
            size += len(json.dumps(wm, separators=(",", ":")))
            done.append(wm)
        capped = any(x.get("capped", False) for x in (data, live) if x is not None)
        out: dict = {"dashboard": d["id"], "time": t, "now": data["now"], "capped": capped,
                     "skipped": sum(1 for w in done if "skipped" in w), "widgets": done}
        if _one(q, "data") in ("1", "true"):
            out["data"] = data
        return 200, out


# ── in the server ────────────────────────────────────────────────────────────────────────────────────────────────

#: self-hosted Trailbeam's one workspace, as alerts_api.WORKSPACE names it
WORKSPACE = "local"


def enabled(env=None) -> bool:
    return (os.environ if env is None else env).get(ENV, "").strip().lower() in ("1", "true", "yes", "on")


def for_app(app, env=None) -> DashboardsApi | None:
    """The API for server.App on its store's tables, or None (the routes are 404) while TRAILBEAM_DASHBOARDS is off.
    A store without the tables (a test double) keeps dashboards in memory."""
    if not enabled(env):
        return None
    db = app.db
    store: DashboardStore = DashboardTables(db) if hasattr(db, "reader") or hasattr(db, "reads") \
        else MemoryDashboards()

    def can_unlock(who) -> bool:
        return app.access.mode() != "accounts" or getattr(who, "role", None) == "admin"

    def data(since_ms: int, now_ms: int, to_ms: int | None) -> dict:
        return event_data(app.db, now_ms, since_ms, app.lambda_timeout_s, app.max_receive_counts, to_ms=to_ms)

    return DashboardsApi(store, WORKSPACE, data, can_unlock=can_unlock, now=lambda: app.now() // _NS,
                         no_depth_text=NOT_QUEUE_LIVE)
