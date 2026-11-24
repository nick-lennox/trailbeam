"""Compare the spans a Trailbeam package exported with what the contract says it must export.

The checker knows nothing about any language. Its two inputs are:

- the **ground truth** a driver wrote (``SCENARIO.md``, "Ground-truth file"): every SQS call the driver
  made, in order, with what SQS returned, including the raw receipt handles; and
- the **spans** the OpenTelemetry Collector wrote to a file (OTLP JSON, one export request per line).

From the truth it builds the spans the contract (``contract/span-contract.md``) requires, recomputes each
receipt-handle HMAC with the run's key, and matches them against the exported Trailbeam spans of the run.
Every difference becomes a ``Problem`` (an error) or a warning (a "should" of the contract), and
``render`` turns them into a report a person can read.

Standard library only, so it runs anywhere a Python 3.9+ interpreter does.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import re
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Iterable, Optional

TRUTH_VERSION = 1
SCHEMA_VERSION = 3
SCOPE = "trailbeam"
KEY_ID_MESSAGE = "trailbeam key id"

# Section 1: span name, trailbeam.kind and messaging.operation.type for each SQS action.
OPERATIONS = {
    "SendMessage": ("trailbeam send", "sqs.send", "send", False),
    "SendMessageBatch": ("trailbeam send", "sqs.send", "send", True),
    "ReceiveMessage": ("trailbeam receive", "sqs.receive", "receive", False),
    "DeleteMessage": ("trailbeam delete", "sqs.settle", "settle", False),
    "DeleteMessageBatch": ("trailbeam delete", "sqs.settle", "settle", True),
    "ChangeMessageVisibility": ("trailbeam change_visibility", "sqs.settle", "settle", False),
    "ChangeMessageVisibilityBatch": ("trailbeam change_visibility", "sqs.settle", "settle", True),
    "PurgeQueue": ("trailbeam purge", "sqs.purge", "purge", False),
}
SETTLE_OPS = {"DeleteMessage", "DeleteMessageBatch", "ChangeMessageVisibility", "ChangeMessageVisibilityBatch"}

# Every trailbeam.* attribute the contract defines. Anything else in the namespace is reported, so a
# misspelt attribute (which Trailbeam would silently ignore) fails the kit instead of passing it.
KNOWN_TRAILBEAM_ATTRIBUTES = {
    "trailbeam.schema_version", "trailbeam.kind", "trailbeam.source",
    "trailbeam.order.scope", "trailbeam.order.issued", "trailbeam.order.returned", "trailbeam.start_observed",
    "trailbeam.batch_entry_id", "trailbeam.receive_count",
    "trailbeam.receipt_handle.hmac", "trailbeam.receipt_handle.key_id",
    "trailbeam.sent_timestamp_ms", "trailbeam.first_receive_timestamp_ms", "trailbeam.aws_trace_header",
    "trailbeam.dead_letter_queue_source_arn", "trailbeam.queue.arn",
    "trailbeam.visibility_timeout", "trailbeam.result", "trailbeam.error_code",
    "trailbeam.outcome", "trailbeam.error_type", "trailbeam.error_message",
    "trailbeam.body", "trailbeam.body.capture_status", "trailbeam.body.format", "trailbeam.body.redacted_count",
    "trailbeam.body.offloaded_size", "trailbeam.body.envelope",
    "trailbeam.tenant", "trailbeam.tenant.source", "trailbeam.message_group_id", "trailbeam.sequence_number",
    "trailbeam.batch.size", "trailbeam.batch.index", "trailbeam.batch.reported_failure_count",
}

# Section 4.2: system attributes the client hooks must ask for. ElasticMQ returns these three on every
# message when asked, so their absence from a response means the package did not add them.
ALWAYS_RETURNED_SYSTEM_ATTRIBUTES = ("ApproximateReceiveCount", "SentTimestamp", "ApproximateFirstReceiveTimestamp")
# The scenario's dead-letter queue is named <run>-dlq; a message received there must carry its source ARN.
DLQ_SUFFIX = "-dlq"


def receipt_hmac(key: str, handle: str) -> str:
    return hmac.new(key.encode("utf-8"), handle.encode("utf-8"), hashlib.sha256).hexdigest()[:32]


def key_id(key: str) -> str:
    return receipt_hmac(key, KEY_ID_MESSAGE)[:8]


def short(value: Any, width: int = 60) -> str:
    text = json.dumps(value) if not isinstance(value, str) or value == "" else value
    return text if len(text) <= width else text[: width - 1] + "…"


# -- reading the Collector's file --------------------------------------------------------------------


@dataclass
class Span:
    name: str
    scope: str
    scope_version: Optional[str]
    kind: Any
    trace_id: str
    span_id: str
    parent_span_id: str
    start: int
    end: int
    attributes: dict  # key -> (otlp type, python value)
    resource: dict

    def get(self, key: str) -> Any:
        v = self.attributes.get(key)
        return v[1] if v else None

    def label(self) -> str:
        bits = [self.name]
        for k, label in (("messaging.operation.name", "op"), ("messaging.message.id", "message"),
                         ("trailbeam.batch_entry_id", "entry")):
            if self.get(k) is not None:
                bits.append(f"{label}={self.get(k)}")
        return " ".join(bits)


def _value(v: dict) -> tuple[str, Any]:
    if "stringValue" in v:
        return "string", v["stringValue"]
    if "intValue" in v:
        return "int", int(v["intValue"])
    if "boolValue" in v:
        return "bool", bool(v["boolValue"])
    if "doubleValue" in v:
        return "double", float(v["doubleValue"])
    if "arrayValue" in v:
        return "array", [_value(x)[1] for x in v["arrayValue"].get("values", [])]
    if "kvlistValue" in v:
        return "kvlist", {x["key"]: _value(x.get("value", {}))[1] for x in v["kvlistValue"].get("values", [])}
    if "bytesValue" in v:
        return "bytes", v["bytesValue"]
    return "empty", None


def _attrs(items: Iterable[dict]) -> dict:
    return {a["key"]: _value(a.get("value", {})) for a in items or []}


def read_spans(lines: Iterable[str]) -> list[Span]:
    """Parse the Collector file exporter's output (``format: json``): one ExportTraceServiceRequest per line."""
    spans = []
    for line in lines:
        line = line.strip()
        if not line:
            continue
        doc = json.loads(line)
        for rs in doc.get("resourceSpans", []):
            resource = {k: v[1] for k, v in _attrs(rs.get("resource", {}).get("attributes")).items()}
            for ss in rs.get("scopeSpans", []):
                scope = ss.get("scope", {}) or {}
                for s in ss.get("spans", []):
                    spans.append(Span(
                        name=s.get("name", ""), scope=scope.get("name", ""), scope_version=scope.get("version"),
                        kind=s.get("kind"), trace_id=s.get("traceId", ""), span_id=s.get("spanId", ""),
                        parent_span_id=s.get("parentSpanId", "") or "",
                        start=int(s.get("startTimeUnixNano", 0) or 0), end=int(s.get("endTimeUnixNano", 0) or 0),
                        attributes=_attrs(s.get("attributes")), resource=resource))
    return spans


def read_truth(lines: Iterable[str]) -> tuple[dict, list[dict]]:
    rows = [json.loads(line) for line in lines if line.strip()]
    if not rows or rows[0].get("truth") != TRUTH_VERSION:
        raise ValueError(f"not a version {TRUTH_VERSION} ground-truth file: the first line must be the header "
                         f'{{"truth": {TRUTH_VERSION}, ...}} (SCENARIO.md)')
    header, calls = rows[0], rows[1:]
    for i, c in enumerate(calls, 1):
        c.setdefault("call", i)
        if c.get("op") not in OPERATIONS:
            raise ValueError(f"ground truth call {i}: unknown op {c.get('op')!r}")
    return header, calls


# -- what the contract expects -----------------------------------------------------------------------


@dataclass
class Expected:
    call: dict
    op: str
    key: tuple
    attrs: dict  # attribute -> expected value (must)
    absent: set = field(default_factory=set)  # attributes that must not be there
    should: dict = field(default_factory=dict)  # attribute -> expected value (should: warn when missing)
    what: str = ""
    span: Optional[Span] = None


def _queue_name(url: str) -> str:
    return url.rstrip("/").rsplit("/", 1)[-1]


def build_expected(calls: list[dict], key: str, capture: bool = False) -> list[Expected]:
    kid = key_id(key)
    out: list[Expected] = []
    received: dict[str, str] = {}  # raw handle -> message id, from earlier receives in this process

    def body_attrs(body: Optional[str]) -> dict:
        if body is None:
            return {}
        a = {"messaging.message.body.size": len(body.encode("utf-8"))}
        if capture:
            a.update({"trailbeam.body.capture_status": "captured", "trailbeam.body": body})
        else:
            a["trailbeam.body.capture_status"] = "off"
        return a

    for c in calls:
        op = c["op"]
        if op == "SendMessage":
            if c.get("ok"):
                out.append(Expected(c, op, (op, c["message_id"]),
                                    {"messaging.message.id": c["message_id"], **body_attrs(c.get("body"))},
                                    what=f"message {c['message_id']}"))
        elif op == "SendMessageBatch":
            for e in c.get("entries", []):
                if e.get("ok"):
                    out.append(Expected(c, op, ("send", e["message_id"]),
                                        {"messaging.message.id": e["message_id"], "trailbeam.batch_entry_id": e["id"],
                                         **body_attrs(e.get("body"))},
                                        what=f"entry {e['id']}, message {e['message_id']}"))
        elif op == "ReceiveMessage":
            for m in c.get("messages", []):
                h = m["receipt_handle"]
                attrs_ = m.get("attributes", {})
                a = {"messaging.message.id": m["message_id"],
                     "trailbeam.receipt_handle.hmac": receipt_hmac(key, h), "trailbeam.receipt_handle.key_id": kid,
                     **body_attrs(m.get("body"))}
                should = {}
                if "ApproximateReceiveCount" in attrs_:
                    a["trailbeam.receive_count"] = int(attrs_["ApproximateReceiveCount"])
                if "SentTimestamp" in attrs_:
                    should["trailbeam.sent_timestamp_ms"] = int(attrs_["SentTimestamp"])
                if "DeadLetterQueueSourceArn" in attrs_:
                    a["trailbeam.dead_letter_queue_source_arn"] = attrs_["DeadLetterQueueSourceArn"]
                if "ApproximateFirstReceiveTimestamp" in attrs_:
                    # "may" in the contract: checked only when present (see check_span)
                    should["?trailbeam.first_receive_timestamp_ms"] = int(attrs_["ApproximateFirstReceiveTimestamp"])
                out.append(Expected(c, op, ("receive", m["message_id"], a["trailbeam.receipt_handle.hmac"]), a,
                                    should=should, what=f"message {m['message_id']}, receive count "
                                    f"{attrs_.get('ApproximateReceiveCount', '?')}"))
                received[h] = m["message_id"]
        elif op in SETTLE_OPS:
            batch = OPERATIONS[op][3]
            entries = c.get("entries", []) if batch else [c]
            for e in entries:
                h = e["receipt_handle"]
                a = {"trailbeam.receipt_handle.hmac": receipt_hmac(key, h), "trailbeam.receipt_handle.key_id": kid,
                     "trailbeam.result": "ok" if e.get("ok") else "failed"}
                absent, should = {"trailbeam.receipt_handle"}, {}
                if batch:
                    a["trailbeam.batch_entry_id"] = e["id"]
                if op.startswith("ChangeMessageVisibility"):
                    a["trailbeam.visibility_timeout"] = int(e["visibility_timeout"])
                if not e.get("ok"):
                    a["trailbeam.error_code"] = e.get("error_code") or True  # True: any non-empty string
                else:
                    absent.add("trailbeam.error_code")
                if h in received:
                    should["messaging.message.id"] = received[h]
                else:
                    absent.add("messaging.message.id")
                what = (f"entry {e['id']}, " if batch else "") + (
                    f"message {received[h]}" if h in received else "a handle this process did not receive")
                out.append(Expected(c, op, (op, a["trailbeam.receipt_handle.hmac"], e.get("id")), a,
                                    absent=absent, should=should, what=what))
        elif op == "PurgeQueue":
            a = {"trailbeam.result": "ok" if c.get("ok") else "failed"}
            absent = {"messaging.message.id", "trailbeam.receipt_handle.hmac"}
            if not c.get("ok"):
                a["trailbeam.error_code"] = c.get("error_code") or True
            out.append(Expected(c, op, (op, c["queue_url"], c["call"]), a, absent=absent, what="the purge"))
    return out


def span_key(s: Span, call_numbers: dict) -> Optional[tuple]:
    op = s.get("messaging.operation.name")
    if op in ("SendMessage", "SendMessageBatch"):
        return ("send", s.get("messaging.message.id")) if op == "SendMessageBatch" else (op, s.get("messaging.message.id"))
    if op == "ReceiveMessage":
        return ("receive", s.get("messaging.message.id"), s.get("trailbeam.receipt_handle.hmac"))
    if op in SETTLE_OPS:
        return (op, s.get("trailbeam.receipt_handle.hmac"), s.get("trailbeam.batch_entry_id"))
    if op == "PurgeQueue":
        return None  # matched by queue and order below
    return None


# -- the check ---------------------------------------------------------------------------------------


@dataclass
class Problem:
    where: str  # "call 4 DeleteMessageBatch, entry d2" or "run"
    text: str
    level: str = "error"  # or "warning"
    rule: str = ""  # contract section


@dataclass
class Report:
    run: str
    variant: str
    driver: str
    expected: int
    matched: int
    spans: int
    problems: list

    @property
    def errors(self):
        return [p for p in self.problems if p.level == "error"]

    @property
    def warnings(self):
        return [p for p in self.problems if p.level == "warning"]

    @property
    def ok(self) -> bool:
        return not self.errors

    def as_json(self) -> dict:
        return {"run": self.run, "variant": self.variant, "driver": self.driver, "ok": self.ok,
                "expected_spans": self.expected, "matched_spans": self.matched, "trailbeam_spans": self.spans,
                "problems": [p.__dict__ for p in self.problems]}


def _where(e: Expected) -> str:
    c = e.call
    q = _queue_name(c.get("queue_url", "")) if c.get("queue_url") else ""
    return f"call {c['call']} {e.op} on {q}" + (f", {e.what}" if e.what else "")


def _type_name(v: Any) -> str:
    return {bool: "bool", int: "int", str: "string"}.get(type(v), type(v).__name__)


INT_ATTRS = {"process.pid", "trailbeam.schema_version", "trailbeam.order.issued", "trailbeam.order.returned",
             "trailbeam.receive_count", "trailbeam.sent_timestamp_ms", "trailbeam.first_receive_timestamp_ms",
             "trailbeam.visibility_timeout", "messaging.message.body.size", "trailbeam.body.redacted_count",
             "trailbeam.body.offloaded_size", "trailbeam.batch.size", "trailbeam.batch.index",
             "trailbeam.batch.reported_failure_count"}
BOOL_ATTRS = {"trailbeam.start_observed"}


def check_span(e: Expected, s: Span, problems: list) -> None:
    where = _where(e)
    c = e.call
    op = e.op
    name, kind, op_type, _ = OPERATIONS[op]

    def err(text, rule=""):
        problems.append(Problem(where, text, "error", rule))

    def warn(text, rule=""):
        problems.append(Problem(where, text, "warning", rule))

    def expect(attr, want, rule):
        if attr not in s.attributes:
            err(f"{attr}: missing (expected {short(want)})", rule)
            return
        t, got = s.attributes[attr]
        if want is True:  # any non-empty string
            if t != "string" or not got:
                err(f"{attr}: expected a non-empty string, got {t} {short(got)}", rule)
            return
        if t != _type_name(want):
            err(f"{attr}: expected {_type_name(want)} {short(want)}, got {t} {short(got)}", rule)
        elif got != want:
            err(f"{attr}: expected {short(want)}, got {short(got)}", rule)

    if s.name != name:
        err(f"span name: expected {name!r}, got {s.name!r}", "1")
    if s.kind not in (1, "SPAN_KIND_INTERNAL"):
        err(f"span kind: expected INTERNAL, got {s.kind!r}", "2")
    if not s.scope_version:
        err("instrumentation scope version: missing (must be the package version)", "2")
    common = {"trailbeam.schema_version": SCHEMA_VERSION, "trailbeam.kind": kind, "trailbeam.source": "client",
              "messaging.system.name": "aws.sqs", "messaging.operation.type": op_type,
              "messaging.operation.name": op, "messaging.destination.name": _queue_name(c["queue_url"]),
              "aws.sqs.queue.url": c["queue_url"]}
    for k, v in common.items():
        expect(k, v, "3")
    for k in ("messaging.client.id", "trailbeam.order.scope"):
        expect(k, True, "3" if k == "messaging.client.id" else "2a")
    for k in ("process.pid", "trailbeam.order.issued", "trailbeam.order.returned"):
        if k not in s.attributes:
            err(f"{k}: missing", "3")
        elif s.attributes[k][0] != "int":
            err(f"{k}: expected int, got {s.attributes[k][0]} {short(s.attributes[k][1])}", "3")
    inst = s.resource.get("service.instance.id")
    if s.get("messaging.client.id") and inst and inst != s.get("messaging.client.id"):
        err(f"messaging.client.id {s.get('messaging.client.id')!r} is not the resource's service.instance.id {inst!r}", "5")
    if not inst:
        err("resource service.instance.id: missing", "5")
    if "trailbeam.start_observed" in s.attributes:
        err("trailbeam.start_observed: must be omitted on client spans (the start was observed)", "3")
    issued, returned = s.get("trailbeam.order.issued"), s.get("trailbeam.order.returned")
    if isinstance(issued, int) and isinstance(returned, int) and not issued < returned:
        err(f"call order: issued {issued} is not before returned {returned}", "2a")
    if not s.start or not s.end or s.end < s.start:
        err(f"times: start {s.start}, end {s.end}: the span must have both, and end at or after its start", "2")
    if not s.trace_id or not s.span_id:
        err("trace ID or span ID missing", "4.5")

    for k, v in e.attrs.items():
        expect(k, v, "4")
    for k in e.absent:
        if k in s.attributes:
            err(f"{k}: must not be set here, got {short(s.get(k))}", "4")
    for k, v in e.should.items():
        optional = k.startswith("?")
        k = k.lstrip("?")
        if k not in s.attributes:
            if not optional:
                warn(f"{k}: missing (should be {short(v)})", "4")
        elif s.get(k) != v or s.attributes[k][0] != _type_name(v):
            err(f"{k}: expected {_type_name(v)} {short(v)}, got {s.attributes[k][0]} {short(s.get(k))}", "4")

    checked = set(e.attrs) | {k.lstrip("?") for k in e.should} | set(common) | {
        "process.pid", "trailbeam.order.issued", "trailbeam.order.returned"}
    for k, (t, v) in s.attributes.items():
        if k.startswith("trailbeam.") and k not in KNOWN_TRAILBEAM_ATTRIBUTES:
            err(f"{k}: not an attribute of the contract (misspelt? Trailbeam ignores it)", "3")
        if (k.startswith("trailbeam.") or k.startswith("messaging.") or k.startswith("aws.")) and (v == "" or v == -1):
            err(f"{k}: sent as {short(v)}; an attribute without a value must be omitted", "3")
        if k in checked:
            continue  # its type was reported with its value above
        if k in INT_ATTRS and t != "int":
            err(f"{k}: expected int, got {t} {short(v)}", "3")
        if k in BOOL_ATTRS and t != "bool":
            err(f"{k}: expected bool, got {t} {short(v)}", "3")


def check(header: dict, calls: list[dict], spans: list[Span], *, run: str, key: str, variant: str = "",
          parent: str = "any", capture: bool = False) -> Report:
    """Check the spans of one run (resource ``service.name`` = ``run``) against its ground truth.

    ``parent``: ``any`` (not checked), ``exported`` (every Trailbeam span has a parent that was exported in
    the same trace: the stock SDK span) or ``unexported`` (every Trailbeam span has a parent, and the run
    exported no span of another scope: the application's sampler dropped them, Trailbeam's did not).
    """
    problems: list[Problem] = []
    run_spans = [s for s in spans if s.resource.get("service.name") == run]
    ours = [s for s in run_spans if s.scope == SCOPE]
    others = [s for s in run_spans if s.scope != SCOPE]
    if not ours:
        names = sorted({str(s.resource.get("service.name")) for s in spans if s.scope == SCOPE})
        problems.append(Problem("run", f"no span with instrumentation scope {SCOPE!r} and service.name {run!r} "
                                f"was exported" + (f" (Trailbeam spans of other services: {', '.join(names)})" if names
                                                   else " (no Trailbeam span at all reached the Collector)"),
                                rule="2, 5"))

    expected = build_expected(calls, key, capture=capture)

    # Scenario coverage: the driver must have exercised what the kit claims to check.
    problems += coverage(calls)

    # Section 4.2: the receive hooks must ask for the system attributes.
    for c in calls:
        if c["op"] != "ReceiveMessage":
            continue
        for m in c.get("messages", []):
            missing = [a for a in ALWAYS_RETURNED_SYSTEM_ATTRIBUTES if a not in m.get("attributes", {})]
            if _queue_name(c["queue_url"]).endswith(DLQ_SUFFIX) and "DeadLetterQueueSourceArn" not in m.get("attributes", {}):
                missing.append("DeadLetterQueueSourceArn")
            if missing:
                problems.append(Problem(
                    f"call {c['call']} ReceiveMessage on {_queue_name(c['queue_url'])}, message {m['message_id']}",
                    f"SQS's response has no {', '.join(missing)}: the package did not add them to "
                    "MessageSystemAttributeNames", rule="4.2"))
                break
            asked = [a for a in c.get("asked_system_attributes", []) if a != "All"]
            lost = [a for a in asked if a not in m.get("attributes", {})]
            if lost:
                problems.append(Problem(
                    f"call {c['call']} ReceiveMessage on {_queue_name(c['queue_url'])}",
                    f"the driver asked for {', '.join(lost)} but SQS did not return it: the package removed "
                    "names the caller asked for", rule="4.2"))
                break

    # Match spans to expectations.
    by_key: dict[tuple, list[Span]] = defaultdict(list)
    purges: list[Span] = []
    unmatched: list[Span] = []
    for s in sorted(ours, key=lambda x: (x.start, x.get("trailbeam.order.issued") or 0)):
        k = span_key(s, {})
        if s.get("messaging.operation.name") == "PurgeQueue":
            purges.append(s)
        elif k is None:
            unmatched.append(s)
        else:
            by_key[k].append(s)
    for e in expected:
        if e.op == "PurgeQueue":
            cand = [p for p in purges if p.get("aws.sqs.queue.url") == e.call["queue_url"]]
            if cand:
                e.span = cand[0]
                purges.remove(cand[0])
            continue
        lst = by_key.get(e.key)
        if lst:
            e.span = lst.pop(0)
    for lst in by_key.values():
        unmatched += lst
    unmatched += purges

    # A span with a wrong HMAC or message ID matches no expectation exactly. Pair what is left by a looser
    # identity (operation, message ID of a receive, batch entry ID), in call order, so the report says
    # "hmac: expected X, got Y" on the right call instead of "missing" here and "unexpected" there.
    expected_keys = {e.key for e in expected}
    loose: dict[tuple, list[Span]] = defaultdict(list)
    for sp in sorted(unmatched, key=lambda x: (x.start, x.get("trailbeam.order.issued") or 0)):
        if span_key(sp, {}) not in expected_keys:  # a duplicate stays a duplicate
            loose[_loose_key(sp.get("messaging.operation.name"), sp.get("messaging.message.id"),
                             sp.get("trailbeam.batch_entry_id"))].append(sp)
    for e in expected:
        if e.span is None and e.op != "PurgeQueue":
            lst = loose.get(_loose_key(e.op, e.attrs.get("messaging.message.id"), e.attrs.get("trailbeam.batch_entry_id")))
            if lst:
                e.span = lst.pop(0)
                unmatched.remove(e.span)

    kid = key_id(key)
    kids = {sp.get("trailbeam.receipt_handle.key_id") for sp in ours if sp.get("trailbeam.receipt_handle.key_id")}
    if kids and kid not in kids:
        problems.append(Problem("run", f"every receipt-handle hash was made with another key (key ID "
                                f"{', '.join(sorted(map(str, kids)))}; the run's TRAILBEAM_RECEIPT_HASH_KEY has ID {kid}): "
                                "the package did not use the key it was given, so every HMAC below differs", rule="7"))

    for e in expected:
        if e.span is None:
            ident = _expected_identity(e)
            problems.append(Problem(_where(e), f"missing span: no {OPERATIONS[e.op][0]!r} was exported"
                                    + (f" {ident}" if ident else ""), rule="1, 4"))
        else:
            check_span(e, e.span, problems)

    # One span per message: anything left over is a duplicate or a span no call explains.
    for s in unmatched:
        k = span_key(s, {})
        if k in expected_keys:
            text = "duplicate span: a second span for the same message/handle in one call (one span per message)"
        else:
            text = "unexpected span: no call in the ground truth explains it"
            if s.get("messaging.operation.name") == "ReceiveMessage" and s.get("messaging.message.id"):
                text += (" (if the message is right, its receipt-handle HMAC is wrong: "
                         f"got {s.get('trailbeam.receipt_handle.hmac')})")
            elif s.get("messaging.operation.name") in SETTLE_OPS:
                text += f" (its receipt-handle HMAC {s.get('trailbeam.receipt_handle.hmac')} matches no handle the driver sent)"
        problems.append(Problem(f"span {s.label()}", text, rule="2"))

    problems += check_calls(expected)
    problems += check_secrets(ours, calls, key)
    problems += check_parents(ours, others, parent)

    return Report(run=run, variant=variant, driver=str(header.get("driver", "?")), expected=len(expected),
                  matched=sum(1 for e in expected if e.span), spans=len(ours), problems=problems)


def _loose_key(op, message_id, entry_id) -> tuple:
    return (op, message_id if op == "ReceiveMessage" else None,
            entry_id if op in OPERATIONS and OPERATIONS[op][3] else None)


def _expected_identity(e: Expected) -> str:
    bits = []
    for k in ("messaging.message.id", "trailbeam.batch_entry_id", "trailbeam.receipt_handle.hmac"):
        if k in e.attrs:
            bits.append(f"{k} = {e.attrs[k]}")
    return ("with " + ", ".join(bits)) if bits else ""


def coverage(calls: list[dict]) -> list[Problem]:
    """What the scenario promises. A driver that skipped a step would otherwise pass by checking less."""
    have = defaultdict(int)
    for c in calls:
        have[c["op"]] += 1
        if c["op"] == "ReceiveMessage":
            for m in c.get("messages", []):
                a = m.get("attributes", {})
                if int(a.get("ApproximateReceiveCount", 1) or 1) >= 2:
                    have["redelivery"] += 1
            if _queue_name(c["queue_url"]).endswith(DLQ_SUFFIX) and c.get("messages"):
                have["dlq receive"] += 1
        if c["op"].endswith("Batch") and any(not e.get("ok") for e in c.get("entries", [])):
            have["failed batch entry"] += 1
        if c["op"] in SETTLE_OPS and not c["op"].endswith("Batch") and not c.get("ok"):
            have["failed call"] += 1
    need = list(OPERATIONS) + ["redelivery", "dlq receive", "failed batch entry", "failed call"]
    missing = [n for n in need if not have[n]]
    if not missing:
        return []
    return [Problem("scenario", f"the ground truth has no {', '.join(missing)}: the driver did not run the whole "
                    "scenario (SCENARIO.md), so the run proves less than the kit claims", rule="kit")]


def check_calls(expected: list[Expected]) -> list[Problem]:
    """Section 2 and 2a: spans of one call share its interval and ticks; sequential calls are ordered."""
    problems = []
    calls: dict[int, list[Expected]] = defaultdict(list)
    for e in expected:
        if e.span is not None:
            calls[e.call["call"]].append(e)
    scopes = {e.span.get("trailbeam.order.scope") for es in calls.values() for e in es}
    if len(scopes) > 1:
        problems.append(Problem("run", f"{len(scopes)} different trailbeam.order.scope values in one process: "
                                f"{', '.join(sorted(map(str, scopes)))}", rule="2a"))
    prev = None
    for n in sorted(calls):
        es = calls[n]
        first = es[0].span
        where = f"call {n} {es[0].op}"
        for e in es[1:]:
            s = e.span
            for label, a, b in (("trailbeam.order.issued", first.get("trailbeam.order.issued"), s.get("trailbeam.order.issued")),
                                ("trailbeam.order.returned", first.get("trailbeam.order.returned"), s.get("trailbeam.order.returned")),
                                ("start time", first.start, s.start), ("end time", first.end, s.end)):
                if a != b:
                    problems.append(Problem(where, f"spans of one call differ in {label}: {a} and {b} "
                                            "(every span of a call carries that call's interval and ticks)", rule="2, 2a"))
                    break
        if prev is not None:
            pn, ps = prev
            r, i = ps.get("trailbeam.order.returned"), first.get("trailbeam.order.issued")
            if isinstance(r, int) and isinstance(i, int) and not r < i:
                problems.append(Problem(where, f"call order: call {pn} returned at tick {r}, but this call, made after "
                                        f"it, was issued at tick {i}; the ticks must say {r} < {i}", rule="2a"))
            if ps.end and first.start and ps.end > first.start:
                problems.append(Problem(where, f"clock: call {pn} ended at {ps.end}, after this later call started at "
                                        f"{first.start} (the interval must contain the call)", rule="2"))
        prev = (n, first)
    return problems


def check_secrets(ours: list[Span], calls: list[dict], key: str) -> list[Problem]:
    """Section 7: no receipt handle and no key may leave the process."""
    # Only handles SQS returned are secrets; the scenario's invalid handles are made up by the driver.
    handles = {m["receipt_handle"] for c in calls for m in c.get("messages", [])}
    problems = []
    for s in ours:
        texts = [str(v) for _, v in s.attributes.values()] + [str(v) for v in s.resource.values()]
        blob = "\n".join(texts)
        if key and key in blob:
            problems.append(Problem(f"span {s.label()}", "the receipt-hash key appears in an exported attribute", rule="7"))
        if any(h in blob for h in handles):
            problems.append(Problem(f"span {s.label()}", "a raw receipt handle appears in an exported attribute", rule="7"))
    return problems


def check_parents(ours: list[Span], others: list[Span], parent: str) -> list[Problem]:
    if parent == "any":
        return []
    problems = []
    exported = {(s.trace_id, s.span_id) for s in others}
    no_parent = [s for s in ours if not s.parent_span_id]
    if no_parent:
        problems.append(Problem("run", f"{len(no_parent)} of {len(ours)} Trailbeam spans have no parent; with the "
                                "application's stock SDK instrumentation on, each must be a child of the stock span "
                                f"(first: {no_parent[0].label()})", rule="2"))
    if parent == "exported":
        lost = [s for s in ours if s.parent_span_id and (s.trace_id, s.parent_span_id) not in exported]
        if lost:
            problems.append(Problem("run", f"{len(lost)} of {len(ours)} Trailbeam spans have a parent that was not "
                                    f"exported in the same trace (first: {lost[0].label()})", rule="2"))
    elif parent == "unexported":
        if others:
            scopes = sorted({s.scope for s in others})
            problems.append(Problem("run", f"the application's sampler was set to 0, yet {len(others)} spans of other "
                                    f"scopes were exported ({', '.join(scopes)}): this run did not test sampling",
                                    rule="kit"))
    return problems


# -- the readable report -----------------------------------------------------------------------------


def render(report: Report) -> str:
    head = (f"{report.driver} / {report.variant or 'default'} (run {report.run}): "
            f"{'PASS' if report.ok else 'FAIL'}, {report.matched} of {report.expected} expected spans found, "
            f"{report.spans} Trailbeam spans exported")
    counts = []
    if report.errors:
        counts.append(f"{len(report.errors)} problem{'s' if len(report.errors) != 1 else ''}")
    if report.warnings:
        counts.append(f"{len(report.warnings)} warning{'s' if len(report.warnings) != 1 else ''}")
    lines = [head + (f" ({', '.join(counts)})" if counts else "")]
    groups: dict[str, list[Problem]] = defaultdict(list)
    order = []
    for p in report.problems:
        if p.where not in groups:
            order.append(p.where)
        groups[p.where].append(p)

    def sort_key(w):
        m = re.match(r"call (\d+)", w)
        return (0, int(m.group(1))) if m else (1 if w.startswith("span") else -1, 0)

    for w in sorted(order, key=sort_key):
        lines.append(f"  {w}")
        for p in groups[w]:
            mark = "-" if p.level == "error" else "~"
            rule = f"  [contract {p.rule}]" if p.rule and p.rule != "kit" else ("  [kit]" if p.rule == "kit" else "")
            lines.append(f"    {mark} {p.text}{rule}")
    if report.problems:
        lines.append("  (- must: fails the run; ~ should: a warning)")
    return "\n".join(lines)
