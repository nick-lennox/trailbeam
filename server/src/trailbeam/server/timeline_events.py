"""``GET /api/timeline-events`` (QV-580): the Event explorer's events in a time window, made from each message's
timeline, so they include what a timeline infers: an expired visibility timeout, a move to the dead-letter queue,
the Lambda trigger's delete after a successful handler, and a Lambda invocation that never reported an outcome.

``/api/events`` (events.py) lists the recorded steps, one per span, and nothing inferred. This lists what the hosted
app's explorer lists: ``events_of_timeline`` is a port of ``eventsOfTimeline`` in @trailbeam/ui
(ui/src/model/events.ts), the function the hosted app runs on its own timelines, and both must make the same events of
the same timeline (ui/test/fixtures/viewmodel-parity.json and timeline-events-parity.json; python/tests/server/
test_timeline_events.py and ui/test/timeline-events.test.ts check that). A change to one is a change to the other.

    send                one per observed SendMessage
    receive             one per observed receive, with its receive count and, on the first delivery, how long the
                        message waited in the queue
    delete              an observed DeleteMessage, or (inferred) the Lambda trigger's delete after a successful
                        invocation
    processing_error    a Lambda invocation whose outcome span says it failed, or (inferred) one that never reported
                        an outcome within the Lambda timeout
    visibility_expired  inferred: a later receive of the same message proves the timeout ran out
    dead_letter         the move SQS reported on a receive from the dead-letter queue

The fields are the shared ``Ev`` type's, in its camelCase, so the shared view reads them as they come. A field the
TypeScript leaves undefined is left out here too.

Which messages: those with a stored span in the window, or up to the Lambda timeout before it (an invocation with no
outcome is reported at its receive plus the timeout), most recently active first, at most ``limit`` of them
(``capped`` says when there were more). Their events in the window, both ends included, newest first. A message's
kept body (contract 15.4) goes on the event whose span carried it, for queues in ``TRAILBEAM_STORE_BODIES``, as on the
Body tab: anyone the server lets in can read it. Bodies stop at ``MAX_BODY_CHARS`` for the page (``bodies_capped``).
"""
from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from typing import Iterable, Optional
from urllib.parse import unquote, urlsplit

from . import bodies, queries

NS_PER_MS = 1_000_000
INT64_MAX = 2 ** 63 - 1
DEFAULT_MESSAGES = 1000
MAX_MESSAGES = 2000
MAX_BODY_CHARS = 4_000_000  # as the hosted app's loadEvents
TENANT_MAX_CHARS = 256
_PAGE = 1000  # spans read per step when finding the window's messages

EVENT_KINDS = ("send", "receive", "delete", "processing_error", "visibility_expired", "dead_letter", "filtered")

CALL_LAMBDA_RECEIVE = "ReceiveMessage (Lambda trigger)"
CALL_LAMBDA_DELETE = "DeleteMessageBatch (Lambda trigger)"
CALL_INFERRED_DELETE = "Delete (inferred)"

OUTCOME_WORDS = {
    "failed_exception": "The handler threw",
    "failed_reported": "Reported in batchItemFailures",
    "failed_invalid_response": "Invalid batchItemFailures response",
}


def handler_failed(type_: Optional[str] = None, message: Optional[str] = None, outcome: Optional[str] = None) -> str:
    """A failed handler's one line: the exception class, and its message when the queue has error text on."""
    lead = (outcome and OUTCOME_WORDS.get(outcome)) or "The handler failed"
    if type_:
        return f"{lead}: {type_}{f': {message}' if message else ''}"
    return f"{lead}: {message}" if message else lead


# ---- where a queue lives -----------------------------------------------------------------------------------------

_REGION = re.compile(r"(?:^|\.)((?:[a-z]{2}(?:-gov|-iso[a-z]?)?)-[a-z]+-\d+)(?=\.|$)")
_ACCOUNT = re.compile(r"^\d{12}$")


def parse_queue_url(url: Optional[str]) -> Optional[dict]:
    """{name, account, region} of an aws.sqs.queue.url (parseQueueUrl): the region from the host, the account from
    the path, and the older, VPC endpoint, China and local-emulator forms of it."""
    if not url:
        return None
    try:
        u = urlsplit(url)
        host = (u.hostname or "").lower()
    except ValueError:
        return None
    if not u.scheme or not u.netloc:
        return None
    parts = [p for p in u.path.split("/") if p]
    if not parts:
        return None
    name = parts[-1]
    before = parts[-2] if len(parts) > 1 else None
    account = before if before and _ACCOUNT.match(before) else None
    m = _REGION.search(host)
    region = m.group(1) if m else ("us-east-1" if host == "queue.amazonaws.com" else None)
    return {"name": unquote(name), "account": account, "region": region}


# ---- what the spans said beyond the timeline (contract 3.2) ------------------------------------------------------

def _attrs(r) -> dict:
    a = r["attributes"]
    return json.loads(a) if isinstance(a, str) else (a or {})


def _s(v) -> Optional[str]:
    return v if isinstance(v, str) and v != "" else None


def span_metas(rows: Iterable) -> list[dict]:
    """The shared SpanMeta of each of a message's rows, in its camelCase: what eventsOfTimeline reads of them."""
    out = []
    for r in rows:
        a = _attrs(r)
        tenant = _s(a.get("trailbeam.tenant"))
        out.append({
            "kind": r["kind"], "traceId": r["trace_id"], "spanId": r["span_id"],
            "tenant": tenant[:TENANT_MAX_CHARS] if tenant else None,
            "tenantSource": _s(a.get("trailbeam.tenant.source")) if tenant else None,
            "bodySize": bodies._int(a.get("messaging.message.body.size")),
            "groupId": _s(a.get("trailbeam.message_group_id")),
            "sequenceNumber": _s(a.get("trailbeam.sequence_number")),
            "errorMessage": _s(a.get("trailbeam.error_message")),
            "batchSize": bodies._int(a.get("trailbeam.batch.size")),
            "batchIndex": bodies._int(a.get("trailbeam.batch.index")),
        })
    return out


def meta_summary(metas: list[dict]) -> dict:
    """A message's tenant (and where it was read from), FIFO group and sequence number, and body size: the send's
    value wins (metaSummary)."""
    ordered = [m for m in metas if m["kind"] == "send"] + [m for m in metas if m["kind"] != "send"]

    def first(k):
        return next((m[k] for m in ordered if m[k] is not None), None)

    tenant_of = next((m for m in ordered if m["tenant"] is not None), None)
    return {"tenant": tenant_of["tenant"] if tenant_of else None,
            "tenantSource": tenant_of["tenantSource"] if tenant_of else None,
            "groupId": first("groupId"), "sequenceNumber": first("sequenceNumber"), "bodySize": first("bodySize")}


# ---- one message's events, from its timeline ---------------------------------------------------------------------

def _ms(ns) -> int:
    """Nanoseconds (an integer or its string) as whole milliseconds, as the TypeScript's BigInt division."""
    n = int(str(ns))
    return n // NS_PER_MS if n >= 0 else -((-n) // NS_PER_MS)


_ISO = re.compile(r"^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.(\d+))?Z$")


def _iso_ms(v) -> Optional[int]:
    """An ISO 8601 UTC time as milliseconds, as Date.parse reads the timeline's times (digits after the third
    dropped); None for anything else (NaN there)."""
    m = _ISO.match(v) if isinstance(v, str) else None
    if not m:
        return None
    d = datetime(*(int(m.group(i)) for i in range(1, 7)), tzinfo=timezone.utc)
    return int(d.timestamp()) * 1000 + int(((m.group(7) or "") + "000")[:3])


def _num(v) -> bool:
    return isinstance(v, (int, float)) and not isinstance(v, bool)


def _js_str(v) -> str:
    return "null" if v is None else str(v)


def _int_or_none(v) -> Optional[int]:
    return None if v is None else int(v)


def _lead_float(v) -> Optional[float]:
    """parseFloat: the number at the start of a string (a function log's Duration, "1234.56 ms")."""
    m = re.match(r"\s*([+-]?(?:\d+\.?\d*|\.\d+)(?:[eE][+-]?\d+)?)", v)
    return float(m.group(1)) if m else None


def events_of_timeline(t: dict, *, urls: Optional[dict] = None, places: Optional[dict] = None,
                       metas: Optional[list] = None, body: Optional[dict] = None,
                       trail_send: Optional[dict] = None) -> list[dict]:
    """The events of one message, from its timeline (``GET /api/messages/{id}``): eventsOfTimeline, in Python.

    ``urls``: each queue's URL by name. ``places``: {account, region} by queue name, before ``urls``. ``metas``: the
    message's span_metas. ``body``: {value, traceId, spanId}, the kept body and the span that carried it.
    ``trail_send``: a send only CloudTrail saw (the hosted app's), {queue, timeMs, call, producer, byService}."""
    mid = str(t["message_id"])
    evs = t.get("events") or []
    urls = urls or {}
    metas = metas or []

    def svc(e) -> str:
        c = e.get("consumer") or {}
        v = c.get("service_name")
        return "" if v is None else v

    sends = [e for e in evs if e.get("type") == "sent"]
    receives = [e for e in evs if e.get("type") == "received"]
    outcomes = [e for e in evs if e.get("type") == "lambda_outcome"]
    ct_send = None if sends else trail_send
    producer = svc(sends[0]) if sends else ct_send["producer"] if ct_send else ""

    def receive_of(delivery):
        return next((x for x in receives if x.get("delivery") == delivery), None)

    def consumer_of(delivery) -> str:
        r = receive_of(delivery)
        return svc(r) if r else svc(receives[-1]) if receives else ""

    first_send = _ms(sends[0]["end_unix_nano"]) if sends and sends[0].get("end_unix_nano") else None

    about = meta_summary(metas)
    span_meta = {f"{m['traceId']}:{m['spanId']}": m for m in metas}

    def span_of(e) -> str:
        sp = e.get("span") or {}
        return f"{sp['trace_id']}:{sp.get('span_id')}" if sp.get("trace_id") else ""

    out: list[dict] = []
    ev_span: list[str] = []

    def push(e, ev: str, ms: Optional[int], **fields):
        if ms is None:
            return
        queue = _js_str(e.get("queue") if e.get("queue") is not None else t.get("current_queue"))
        place = (places or {}).get(queue) or parse_queue_url(urls.get(queue))
        error = ev in ("processing_error", "dead_letter") or fields.get("status") == "error"
        own = span_meta.get(span_of(e))
        attrs = {
            "message.ApproximateReceiveCount": int(e["receive_count"]) if ev == "receive" and _num(e.get("receive_count")) and e["receive_count"] > 0 else None,
            "message.tenant": about["tenant"],
            "message.tenantSource": about["tenantSource"] if about["tenant"] else None,
            "message.MessageGroupId": about["groupId"],
            "message.SequenceNumber": about["sequenceNumber"],
            "message.BodySize": about["bodySize"],
            "message.SentTimestamp": e.get("sent_timestamp_ms") if _num(e.get("sent_timestamp_ms")) else None,
            "message.ApproximateFirstReceiveTimestamp": e.get("first_receive_timestamp_ms") if _num(e.get("first_receive_timestamp_ms")) else None,
            "batch.size": own["batchSize"] if ev == "receive" and own else None,
            "batch.index": own["batchIndex"] if ev == "receive" and own else None,
            "queue.account": place["account"] if place else None,
            "queue.region": place["region"] if place else None,
        }
        made = {
            "key": f"{mid}:{e.get('id')}", "ms": ms, "event": ev, "status": "error" if error else "ok", "queue": queue,
            "messageId": mid, "producer": producer, "detail": "", "source": "package", "hasPage": True, "live": True,
            **({"tenant": about["tenant"]} if about["tenant"] else {}),
            **({"sizeBytes": own["bodySize"]} if ev == "send" and own and own["bodySize"] is not None else {}),
            "attrs": {k: v for k, v in attrs.items() if v is not None},
            **fields,
        }
        out.append({k: v for k, v in made.items() if v is not None})
        ev_span.append(span_of(e))

    def handler_ms(r, start, end):
        if r:
            return max(0, end - _ms(r["end_unix_nano"]))
        return end - start if start is not None and end > start else None

    def count(r):
        return _int_or_none(r.get("receive_count")) if r else None

    if ct_send:
        by = "the AWS service " if ct_send.get("byService") else ""
        push({"id": "ct-send", "queue": ct_send["queue"]}, "send", ct_send["timeMs"], service=producer,
             consumer=consumer_of(1), source="cloudtrail", call=ct_send.get("call"),
             detail=f"Recorded by CloudTrail (whole seconds). Sent by {by}{producer}")

    for e in evs:
        end = _ms(e["end_unix_nano"]) if e.get("end_unix_nano") else None
        start = _ms(e["start_unix_nano"]) if e.get("start_unix_nano") else None
        typ = e.get("type")
        if typ == "sent":
            extra = {"deduplicated": True, "detail": _js_str(e.get("reason")) if e.get("reason") is not None else ""} \
                if e.get("deduplicated") else {}
            push(e, "send", end, service=svc(e), consumer=consumer_of(1),
                 call="SendMessageBatch" if e.get("batch_entry_id") else "SendMessage", **extra)
        elif typ == "received":
            sent_at = first_send if first_send is not None else (
                e.get("sent_timestamp_ms") if _num(e.get("sent_timestamp_ms")) else None)
            wait = max(0, end - sent_at) if (e.get("delivery") == 1 and e.get("receive_count") == 1
                                             and sent_at is not None and end is not None) else None
            lam = (e.get("consumer") or {}).get("source") == "lambda"
            push(e, "receive", end, service=svc(e), consumer=svc(e), receiveCount=_int_or_none(e.get("receive_count")),
                 waitMs=wait, call=CALL_LAMBDA_RECEIVE if lam else "ReceiveMessage",
                 detail="Received from the dead-letter queue" if e.get("dlq_source_arn") else "")
        elif typ == "deleted":
            r = receive_of(e.get("delivery"))
            took = max(0, start - _ms(r["end_unix_nano"])) if r and start is not None else None
            failed = e.get("result") != "ok"
            call = "DeleteMessageBatch" if e.get("batch_entry_id") else "DeleteMessage"
            if failed:
                detail = f"{call} failed{': ' + e['error_code'] if e.get('error_code') else ''}"
            elif e.get("status") == "stale_handle":
                detail = "Deleted with the receipt handle of an earlier receive"
            elif e.get("status") == "unmatched_handle":
                detail = "No observed receive returned this receipt handle"
            else:
                detail = ""
            push(e, "delete", end, service=svc(e), consumer=svc(e), receiveCount=count(r), durationMs=took,
                 detail=detail, status="error" if failed else "ok", call=call)
        elif typ == "lambda_outcome":
            if e.get("outcome") == "success":
                continue  # its delete is the inferred deleted_by_lambda step
            r = receive_of(e.get("delivery"))
            own = span_meta.get(span_of(e))
            message = own["errorMessage"] if own and own["errorMessage"] is not None else e.get("error_message")
            push(e, "processing_error", end, service=svc(e), consumer=svc(e), receiveCount=count(r),
                 durationMs=handler_ms(r, start, end) if end is not None else None,
                 detail=handler_failed(e.get("error_type"), message, e.get("outcome")))
        elif typ == "deleted_by_lambda":
            at = _iso_ms(e["window"]["from"])
            ok = [x for x in outcomes if x.get("outcome") == "success" and x.get("delivery") == e.get("delivery")]
            o = next((x for x in ok if _ms(x["end_unix_nano"]) == at), ok[0] if ok else None)
            r = receive_of(e.get("delivery"))
            took = handler_ms(r, _ms(o["start_unix_nano"]) if o.get("start_unix_nano") else None,
                              _ms(o["end_unix_nano"])) if o else None
            push(e, "delete", at, service=svc(r) if r else "", consumer=svc(r) if r else "", receiveCount=count(r),
                 durationMs=took, source="inferred", call=CALL_INFERRED_DELETE,
                 detail="The handler succeeded, so the Lambda trigger deleted the message. That delete is inferred")
        elif typ == "lambda_invocation_ended_without_outcome":
            r = receive_of(e.get("delivery"))
            push(e, "processing_error", _iso_ms(e["window"]["to"]), service=svc(r) if r else "",
                 consumer=svc(r) if r else "", receiveCount=count(r), source="inferred",
                 detail="No outcome within the Lambda timeout: the invocation most likely timed out or ran out of memory")
        elif typ == "visibility_timeout_expired":
            before = receive_of(e.get("after_delivery"))
            push(e, "visibility_expired", _iso_ms(e["window"]["to"]), service="sqs",
                 consumer=svc(before) if before else "", receiveCount=count(before), source="inferred",
                 detail=f"Receive {_js_str(e.get('after_delivery'))} was not deleted in time, so the message became "
                        "visible again. Inferred from the next receive")
        elif typ == "aws_step":  # the hosted app's AWS records; kept so the two functions stay the same
            at = _iso_ms(e.get("time"))
            fn = e.get("function_name") if isinstance(e.get("function_name"), str) else ""
            r = receive_of(e["delivery"]) if e.get("delivery") else None
            common = {"service": fn or "lambda", "consumer": fn, "detail": _js_str(e.get("summary")) if e.get("summary") is not None else "",
                      "receiveCount": count(r)}
            kind = e.get("aws_kind")
            if kind == "filter_drop":
                push(e, "filtered", at, **common, source="cloudtrail", status="error")
            elif kind in ("trigger_delete", "second_function"):
                push(e, "delete", at, **common, source="cloudtrail", call=CALL_LAMBDA_DELETE)
            elif kind in ("timeout", "out_of_memory"):
                d = (e.get("record") or {}).get("Duration")
                push(e, "processing_error", at, **common, source="function_logs",
                     durationMs=_lead_float(d) if isinstance(d, str) else None)
        elif typ == "moved_to_dead_letter_queue":
            dlq_receive = receive_of(e.get("reported_on_delivery"))
            push(e, "dead_letter", _iso_ms(e["window"]["to"]), queue=_js_str(e.get("from_queue")), service="sqs",
                 consumer=consumer_of(int(e["reported_on_delivery"]) - 1), receiveCount=count(dlq_receive),
                 detail=f"Moved from {e.get('from_queue')} to {e.get('to_queue')}. SQS reported it on the receive "
                        f"from {e.get('to_queue')}")
    # the body goes on the event whose span carried the kept copy, else the message's first event
    if body and out:
        at = f"{body['traceId']}:{body['spanId']}"
        i = next((i for i, s in enumerate(ev_span) if s == at), 0)
        out[i]["body"] = body["value"]
    return out


# ---- the window's events -----------------------------------------------------------------------------------------

def window_messages(db, from_ns: int, to_ns: int, limit: int) -> tuple[list[str], bool]:
    """(message IDs, capped): up to ``limit`` messages with a span in [from_ns, to_ns), most recently active first.
    Walks the window's spans newest first (Store.step_rows), a page at a time, so it reads about as many spans as the
    messages it returns have in the window."""
    mids: list[str] = []
    seen: set = set()
    after = None
    while True:
        rows = db.step_rows(from_ns, to_ns, _PAGE, after=after)
        for r in rows:
            m = r["mid"]
            if m is not None and m not in seen:
                if len(mids) == limit:
                    return mids, True
                seen.add(m)
                mids.append(m)
        if len(rows) < _PAGE:
            return mids, False
        last = rows[-1]
        after = (last["time_ns"], last["trace_id"], last["span_id"])


def page(db, from_ms: int, to_ms: int, now_ns: int, lambda_timeout_s: int, max_receive_counts: Optional[dict] = None,
         *, limit: int = DEFAULT_MESSAGES, max_body_chars: int = MAX_BODY_CHARS) -> dict:
    """The answer of ``/api/timeline-events`` for [from_ms, to_ms], both included."""
    return answer(inputs(db, from_ms, to_ms, now_ns, lambda_timeout_s, max_receive_counts, limit=limit,
                         max_body_chars=max_body_chars), from_ms, to_ms)


def inputs(db, from_ms: int, to_ms: int, now_ns: int, lambda_timeout_s: int,
           max_receive_counts: Optional[dict] = None, *, limit: int = DEFAULT_MESSAGES,
           max_body_chars: int = MAX_BODY_CHARS) -> dict:
    """What events_of_timeline is given for each message of the window: {messages: [{timeline, urls, metas, body}],
    capped, bodies_capped}. The parity fixture records these, so the TypeScript can be given the same."""
    lead_ns = lambda_timeout_s * 1_000_000_000
    # the store takes 64-bit integers: a window to about year 2262 and beyond is cut there (as events.py does)
    mids, capped = window_messages(db, min(INT64_MAX, max(0, from_ms * NS_PER_MS - lead_ns)),
                                   min(INT64_MAX, (to_ms + 1) * NS_PER_MS), limit)
    rows_of = db.messages_rows(mids)
    budget, bodies_capped = max_body_chars, False
    out = []
    for mid in mids:
        rows = rows_of.get(mid)
        if not rows:
            continue
        t = queries.message(db, mid, now_ns, lambda_timeout_s, max_receive_counts, rows=rows)
        if t is None:
            continue
        urls: dict = {}
        for r in rows:
            if r["queue_url"] and r["queue"] not in urls:
                urls[r["queue"]] = r["queue_url"]
        body = None
        kept = bodies.kept_body(rows, db.store_bodies)
        if kept is not None:
            a = _attrs(kept)
            text = a["trailbeam.body"]
            if len(text) <= budget:
                budget -= len(text)
                body = {"value": bodies.parsed_body(text, _s(a.get("trailbeam.body.format"))),
                        "traceId": kept["trace_id"], "spanId": kept["span_id"]}
            else:
                bodies_capped = True
        out.append({"timeline": t, "urls": urls, "metas": span_metas(rows), "body": body})
    return {"messages": out, "capped": capped, "bodies_capped": bodies_capped}


def answer(i: dict, from_ms: int, to_ms: int) -> dict:
    """The page made of ``inputs``: every message's events in [from_ms, to_ms], newest first; events at the same time
    keep their timeline's order, reversed (as the hosted app's loadEvents orders them)."""
    found = []
    for m in i["messages"]:
        for e in events_of_timeline(m["timeline"], urls=m["urls"], metas=m["metas"], body=m["body"]):
            if from_ms <= e["ms"] <= to_ms:
                found.append(e)
    events = [e for _, e in sorted(enumerate(found), key=lambda x: (-x[1]["ms"], -x[0]))]
    return {"from_ms": from_ms, "to_ms": to_ms, "events": events, "messages": len(i["messages"]),
            "capped": i["capped"], "bodies_capped": i["bodies_capped"],
            "queues": sorted({e["queue"] for e in events})}


__all__ = ["events_of_timeline", "page", "inputs", "answer", "parse_queue_url", "span_metas", "meta_summary",
           "handler_failed", "EVENT_KINDS"]
