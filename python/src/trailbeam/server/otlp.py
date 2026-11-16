"""Decode OTLP/HTTP trace requests (protobuf or JSON) into Trailbeam span records (docs/span-contract.md).

Only spans with instrumentation scope ``trailbeam`` are read (contract section 1). They are classified by
``trailbeam.kind`` and ``messaging.operation.name``; span names are never parsed.
"""
from __future__ import annotations

import base64
import json
from dataclasses import dataclass, field
from typing import Any, Iterable, Optional

from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest

SCOPE_NAME = "trailbeam"
SCHEMA_VERSION = 3
# Version 2 spans are still read: they only lack the call-order attributes (trailbeam.order.*), so their
# steps are ordered by the other sources alone. Version 1 spans are rejected (contract section 12).
ACCEPTED_SCHEMA_VERSIONS = (2, 3)
DEFAULT_KEY_ID = "b5e633f7"  # key ID of the published default key, contract section 7

DELETE_OPS = {"DeleteMessage", "DeleteMessageBatch"}
VISIBILITY_OPS = {"ChangeMessageVisibility", "ChangeMessageVisibilityBatch"}
OUTCOMES = {"success", "failed_reported", "failed_invalid_response", "failed_exception"}

# Span attribute -> record field, with the expected Python type.
_FIELDS = {
    "trailbeam.source": ("source", str),
    "messaging.operation.name": ("operation", str),
    "messaging.destination.name": ("queue", str),
    "aws.sqs.queue.url": ("queue_url", str),
    "messaging.message.id": ("message_id", str),
    "trailbeam.receipt_handle.hmac": ("receipt_hmac", str),
    "trailbeam.receipt_handle.key_id": ("key_id", str),
    "trailbeam.receive_count": ("receive_count", int),
    "trailbeam.sent_timestamp_ms": ("sent_ts_ms", int),
    "trailbeam.first_receive_timestamp_ms": ("first_receive_ts_ms", int),
    "trailbeam.dead_letter_queue_source_arn": ("dlq_source_arn", str),  # contract 3.1, section 9b
    "trailbeam.result": ("result", str),
    "trailbeam.error_code": ("error_code", str),
    "trailbeam.visibility_timeout": ("visibility_timeout", int),
    "trailbeam.outcome": ("outcome", str),
    "trailbeam.error_type": ("error_type", str),
    "trailbeam.batch_entry_id": ("batch_entry_id", str),
    "messaging.client.id": ("client_id", str),
    "process.pid": ("pid", int),
    "thread.name": ("thread_name", str),
    "faas.name": ("faas_name", str),
    "faas.invocation_id": ("faas_invocation_id", str),
    "trailbeam.order.scope": ("order_scope", str),
    "trailbeam.order.issued": ("order_issued", int),
    "trailbeam.order.returned": ("order_returned", int),
}
# time_ns is the span's end (contract section 2: the response arrived); start_ns its start, or None if
# the start was not observed (trailbeam.start_observed = false, a Lambda receive).
RECORD_FIELDS = ["trace_id", "span_id", "kind", "start_ns", "time_ns", "service_name"] + [f for f, _ in _FIELDS.values()]

# Fields each event kind must carry to be usable (contract section 4).
_REQUIRED = {
    "send": ("queue", "message_id"),
    "receive": ("queue", "message_id", "receive_count", "receipt_hmac", "key_id"),
    "delete": ("queue", "receipt_hmac", "key_id", "result"),
    "change_visibility": ("queue", "receipt_hmac", "key_id", "result"),
    "outcome": ("queue", "message_id", "outcome"),
}


@dataclass
class Decoded:
    records: list[dict] = field(default_factory=list)
    ignored: int = 0  # spans of other instrumentation scopes; not an error
    rejected: list[str] = field(default_factory=list)  # one reason per rejected Trailbeam span


class DecodeError(ValueError):
    pass


def decode(body: bytes, content_type: str) -> Decoded:
    ct = (content_type or "").split(";")[0].strip().lower()
    if ct == "application/json":
        try:
            doc = json.loads(body)
        except ValueError as e:
            raise DecodeError(f"invalid JSON: {e}") from None
        if not isinstance(doc, dict):
            raise DecodeError("invalid JSON: expected an object")
        return _from_spans(_json_spans(doc))
    if ct in ("application/x-protobuf", "application/protobuf", ""):
        try:
            req = ExportTraceServiceRequest.FromString(body)
        except Exception as e:  # protobuf raises DecodeError, a subclass of Exception
            raise DecodeError(f"invalid protobuf: {e}") from None
        return _from_spans(_proto_spans(req))
    raise DecodeError(f"unsupported Content-Type {content_type!r}")


# ---- protobuf -------------------------------------------------------------------------------

def _proto_value(v) -> Any:
    which = v.WhichOneof("value")
    if which == "array_value":
        return [_proto_value(x) for x in v.array_value.values]
    if which == "kvlist_value":
        return {kv.key: _proto_value(kv.value) for kv in v.kvlist_value.values}
    if which == "bytes_value":
        return base64.b64encode(v.bytes_value).decode()
    return getattr(v, which) if which else None


def _proto_spans(req) -> Iterable[tuple]:
    for rs in req.resource_spans:
        resource = {kv.key: _proto_value(kv.value) for kv in rs.resource.attributes}
        for ss in rs.scope_spans:
            for s in ss.spans:
                attrs = {kv.key: _proto_value(kv.value) for kv in s.attributes}
                yield (ss.scope.name, resource, s.trace_id.hex(), s.span_id.hex(),
                       s.start_time_unix_nano, s.end_time_unix_nano, attrs)


# ---- JSON (OTLP/JSON: lowerCamelCase keys, hex trace and span IDs, 64-bit ints as strings) ---

def _get(d: dict, camel: str, snake: str, default=None):
    return d.get(camel, d.get(snake, default))


def _json_value(v: dict) -> Any:
    if not isinstance(v, dict):
        return None
    for camel, snake in (("stringValue", "string_value"), ("boolValue", "bool_value"),
                         ("doubleValue", "double_value"), ("bytesValue", "bytes_value")):
        if camel in v or snake in v:
            return _get(v, camel, snake)
    if "intValue" in v or "int_value" in v:
        return int(_get(v, "intValue", "int_value"))
    if "arrayValue" in v or "array_value" in v:
        return [_json_value(x) for x in (_get(v, "arrayValue", "array_value") or {}).get("values", [])]
    if "kvlistValue" in v or "kvlist_value" in v:
        return _json_attrs((_get(v, "kvlistValue", "kvlist_value") or {}).get("values", []))
    return None


def _json_attrs(kvs) -> dict:
    return {kv.get("key"): _json_value(kv.get("value") or {}) for kv in kvs or [] if isinstance(kv, dict)}


def _json_spans(doc: dict) -> Iterable[tuple]:
    try:
        for rs in _get(doc, "resourceSpans", "resource_spans", []) or []:
            resource = _json_attrs((rs.get("resource") or {}).get("attributes"))
            for ss in _get(rs, "scopeSpans", "scope_spans", []) or []:
                scope = (ss.get("scope") or {}).get("name", "")
                for s in ss.get("spans", []) or []:
                    yield (scope, resource, str(_get(s, "traceId", "trace_id", "")).lower(),
                           str(_get(s, "spanId", "span_id", "")).lower(),
                           int(_get(s, "startTimeUnixNano", "start_time_unix_nano", 0) or 0),
                           int(_get(s, "endTimeUnixNano", "end_time_unix_nano", 0) or 0),
                           _json_attrs(s.get("attributes")))
    except (AttributeError, TypeError, ValueError) as e:
        raise DecodeError(f"invalid OTLP/JSON: {e}") from None


# ---- span -> record -------------------------------------------------------------------------

def _event_kind(attrs: dict) -> Optional[str]:
    kind = attrs.get("trailbeam.kind")
    if kind == "sqs.send":
        return "send"
    if kind == "sqs.receive":
        return "receive"
    if kind == "sqs.outcome":
        return "outcome"
    if kind == "sqs.settle":
        op = attrs.get("messaging.operation.name")
        if op in DELETE_OPS:
            return "delete"
        if op in VISIBILITY_OPS:
            return "change_visibility"
    return None


def to_record(resource: dict, trace_id: str, span_id: str, start_ns: int, end_ns: int,
              attrs: dict) -> tuple[Optional[dict], str]:
    """Returns (record, "") or (None, reason)."""
    if attrs.get("trailbeam.schema_version") not in ACCEPTED_SCHEMA_VERSIONS:
        return None, (f"trailbeam.schema_version {attrs.get('trailbeam.schema_version')!r} is not one of "
                      f"{', '.join(map(str, ACCEPTED_SCHEMA_VERSIONS))}")
    kind = _event_kind(attrs)
    if kind is None:
        return None, (f"unknown trailbeam.kind {attrs.get('trailbeam.kind')!r} / messaging.operation.name "
                      f"{attrs.get('messaging.operation.name')!r}")
    if not trace_id or not span_id or not start_ns or not end_ns:
        return None, "span without trace ID, span ID, start time or end time"
    if end_ns < start_ns:
        return None, "span ends before it starts"
    observed = attrs.get("trailbeam.start_observed") is not False
    rec = {"trace_id": trace_id, "span_id": span_id, "kind": kind, "time_ns": int(end_ns),
           "start_ns": int(start_ns) if observed else None,
           "service_name": resource.get("service.name") if isinstance(resource.get("service.name"), str) else None}
    for attr, (name, typ) in _FIELDS.items():
        v = attrs.get(attr)
        if typ is int and isinstance(v, str) and v.lstrip("-").isdigit():
            v = int(v)
        rec[name] = v if isinstance(v, typ) and not isinstance(v, bool) and v != "" else None
    missing = [f for f in _REQUIRED[kind] if rec[f] is None]
    if missing:
        return None, f"{kind} span without {', '.join(missing)}"
    if kind in ("delete", "change_visibility") and rec["result"] not in ("ok", "failed"):
        return None, f"trailbeam.result {rec['result']!r} is not ok or failed"
    if kind == "outcome" and rec["outcome"] not in OUTCOMES:
        return None, f"unknown trailbeam.outcome {rec['outcome']!r}"
    rec["attributes"] = attrs
    return rec, ""


def _from_spans(spans: Iterable[tuple]) -> Decoded:
    out = Decoded()
    for scope, resource, trace_id, span_id, start_ns, end_ns, attrs in spans:
        if scope != SCOPE_NAME:
            out.ignored += 1
            continue
        rec, reason = to_record(resource, trace_id, span_id, start_ns, end_ns, attrs)
        if rec is None:
            out.rejected.append(reason)
        else:
            out.records.append(rec)
    return out
