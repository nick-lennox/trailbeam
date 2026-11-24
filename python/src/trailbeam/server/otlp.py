"""Decode OTLP/HTTP trace requests (protobuf or JSON) into Trailbeam span records (contract/span-contract.md).

Only spans with instrumentation scope ``trailbeam`` are read (contract section 1). They are classified by
``trailbeam.kind`` and ``messaging.operation.name``; span names are never parsed.

**RabbitMQ** (schema version 4, contract/span-contract-amqp.md). An ``amqp.*`` span is read by ``_amqp_record``
into a record of the same fields, with the step its kind maps to (A2) as its kind:

| ``trailbeam.kind`` | record kind |
|---|---|
| ``amqp.publish`` | ``send`` (``queue``: the exchange) |
| ``amqp.return`` | ``return`` (``queue``: the exchange) |
| ``amqp.deliver`` | ``receive`` |
| ``amqp.ack`` | ``delete`` |
| ``amqp.nack``, ``amqp.reject`` with ``requeue = true`` | ``change_visibility``, ``visibility_timeout`` 0: released |
| ``amqp.nack``, ``amqp.reject`` with ``requeue = false`` | ``reject``: refused by the consumer |
| ``amqp.cancel`` | ``cancel`` (belongs to no message: a consumer, not a delivery) |
| ``amqp.channel_closed`` | ``channel_closed`` |

``message_id`` is the message's key (systems.py), ``~rabbitmq~<ID>``, None when the publisher set no
``message_id`` (A6: such spans are kept and counted, and belong to no message). A delivery's settle and close
spans join it as SQS settles join their receive, through ``receipt_hmac`` and ``key_id``: for RabbitMQ those hold
the delivery's (``trailbeam.amqp.channel.id``, ``rabbitmq.message.delivery_tag``) and ``AMQP_KEY_ID``, not a hash
(A15). ``queue_url`` is the queue's (or exchange's) place, ``amqp_place``, so the queues of two virtual hosts are
two queue keys (queues.key). ``receive_count`` is set only with a known ``trailbeam.receive_count.source`` (A7, A12).
"""
from __future__ import annotations

import base64
import json
import re
from dataclasses import dataclass, field
from typing import Any, Iterable, Optional
from urllib.parse import quote

from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest

from .sns import valid_topic_arn
from .systems import RABBITMQ, SQS, message_key

SCOPE_NAME = "trailbeam"
SCHEMA_VERSION = 3
# Version 2 spans are still read: they only lack the call-order attributes (trailbeam.order.*), so their
# steps are ordered by the other sources alone. Version 1 spans are rejected (contract section 12). Version 4
# (contract/span-contract-amqp.md A1) means for SQS and SNS spans what 3 means; amqp.* spans must be 4.
ACCEPTED_SCHEMA_VERSIONS = (2, 3, 4)
AMQP_SCHEMA_VERSION = 4
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
# service_name and service_version are the resource's service.name and service.version (the deployed build, QV-362).
RECORD_FIELDS = (["trace_id", "span_id", "kind", "start_ns", "time_ns", "service_name", "service_version"]
                 + [f for f, _ in _FIELDS.values()])

# Fields each event kind must carry to be usable (contract section 4).
_REQUIRED = {
    "send": ("queue", "message_id"),
    "receive": ("queue", "message_id", "receive_count", "receipt_hmac", "key_id"),
    "delete": ("queue", "receipt_hmac", "key_id", "result"),
    "change_visibility": ("queue", "receipt_hmac", "key_id", "result"),
    "outcome": ("queue", "message_id", "outcome"),
    "purge": ("queue", "result"),  # revision 3.3, section 18: no message ID, no receipt hash
    "publish": ("queue", "message_id"),  # revision 3.4, section 21.1: the topic name and the SNS message ID
}

# ---- RabbitMQ (contract/span-contract-amqp.md) ----
AMQP_KEY_ID = "amqp"  # key_id of a RabbitMQ delivery's join key: (channel ID, delivery tag), not a hash (A15)
# kind -> the messaging.operation.name values it may have (A4)
AMQP_OPERATIONS = {
    "amqp.publish": ("basic.publish",),
    "amqp.return": ("basic.return",),
    "amqp.deliver": ("basic.deliver", "basic.get"),
    "amqp.ack": ("basic.ack",),
    "amqp.nack": ("basic.nack",),
    "amqp.reject": ("basic.reject",),
    "amqp.cancel": ("basic.cancel",),
    "amqp.channel_closed": ("channel.close", "connection.close", "connection.lost"),
}
_DEST, _VHOST, _CHANNEL, _TAG = ("messaging.destination.name", "trailbeam.amqp.vhost", "trailbeam.amqp.channel.id",
                                 "rabbitmq.message.delivery_tag")
# kind -> its required attributes (A12), in the table's order
AMQP_REQUIRED = {
    "amqp.publish": (_DEST, _VHOST, "trailbeam.amqp.confirmed", "trailbeam.amqp.mandatory", "trailbeam.result"),
    "amqp.return": (_DEST, _VHOST, "trailbeam.amqp.reply_code"),
    "amqp.deliver": (_DEST, _VHOST, _CHANNEL, _TAG, "trailbeam.amqp.redelivered"),
    "amqp.ack": (_DEST, _VHOST, _CHANNEL, _TAG, "trailbeam.amqp.multiple", "trailbeam.result"),
    "amqp.nack": (_DEST, _VHOST, _CHANNEL, _TAG, "trailbeam.amqp.multiple", "trailbeam.result",
                  "trailbeam.amqp.requeue"),
    "amqp.reject": (_DEST, _VHOST, _CHANNEL, _TAG, "trailbeam.amqp.requeue", "trailbeam.result"),
    "amqp.cancel": (_DEST, _VHOST, _CHANNEL, "trailbeam.amqp.consumer_tag", "trailbeam.amqp.cancelled_by"),
    "amqp.channel_closed": (_DEST, _VHOST, _CHANNEL, _TAG, "trailbeam.amqp.close.initiator"),
}
AMQP_BOOLS = frozenset({"trailbeam.amqp.confirmed", "trailbeam.amqp.mandatory", "trailbeam.amqp.redelivered",
                        "trailbeam.amqp.multiple", "trailbeam.amqp.requeue"})
AMQP_INTS = frozenset({_TAG, "trailbeam.amqp.reply_code"})
AMQP_ENUMS = {
    "trailbeam.amqp.cancelled_by": ("client", "broker"),
    "trailbeam.amqp.close.initiator": ("client", "broker", "network"),
    "trailbeam.receive_count.source": ("x-acquired-count", "x-delivery-count", "first_delivery"),
}
# The record kinds only RabbitMQ spans have (the module docstring's table).
AMQP_KINDS = ("return", "reject", "cancel", "channel_closed")
# The fields an amqp span sets as an SQS span does; the rest are worked out (_amqp_record) or None.
_AMQP_FIELDS = ("source", "operation", "queue", "result", "error_code", "client_id", "pid", "thread_name",
                "order_scope", "order_issued", "order_returned")


# The test span of trailbeam doctor (contract section 17): stored as a probe, never as a span or a message.
PROBE_KIND = "trailbeam.probe"
PROBE_ID = "trailbeam.probe.id"
PROBE_ID_CHARS = frozenset("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_")
PROBE_ID_LENGTH = (8, 64)
# An export carries at most this many probes; the server rejects the rest (each its own write otherwise).
MAX_PROBES_PER_EXPORT = 8
# The fixed starts of the reasons a probe is rejected with (contract section 17), which trailbeam doctor tells apart.
# A server of 0.3 or before answers "unknown trailbeam.kind 'trailbeam.probe'" instead.
PROBE_NOT_STORED = "probe span not stored:"           # the store keeps no probes
PROBE_BAD_ID = f"probe span without a {PROBE_ID}"     # no valid ID
PROBE_OVER_LIMIT = "probe span over the limit of"     # more than MAX_PROBES_PER_EXPORT in the export


@dataclass
class Decoded:
    records: list[dict] = field(default_factory=list)
    probes: list[str] = field(default_factory=list)  # probe IDs (contract section 17), in the order sent
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
    if kind == "sqs.purge" and attrs.get("messaging.operation.name") == "PurgeQueue":
        return "purge"
    if kind == "sns.publish" and attrs.get("messaging.operation.name") in ("Publish", "PublishBatch"):
        return "publish"
    if kind == "sqs.settle":
        op = attrs.get("messaging.operation.name")
        if op in DELETE_OPS:
            return "delete"
        if op in VISIBILITY_OPS:
            return "change_visibility"
    return None


def _resource_str(resource: dict, key: str) -> Optional[str]:
    v = resource.get(key)
    return v if isinstance(v, str) and v != "" else None


def to_record(resource: dict, trace_id: str, span_id: str, start_ns: int, end_ns: int,
              attrs: dict) -> tuple[Optional[dict], str]:
    """Returns (record, "") or (None, reason)."""
    if isinstance(attrs.get("trailbeam.kind"), str) and attrs["trailbeam.kind"].startswith("amqp."):
        return _amqp_record(resource, trace_id, span_id, start_ns, end_ns, attrs)
    if attrs.get("trailbeam.schema_version") not in ACCEPTED_SCHEMA_VERSIONS:
        return None, (f"trailbeam.schema_version {attrs.get('trailbeam.schema_version')!r} is not one of "
                      f"{', '.join(map(str, ACCEPTED_SCHEMA_VERSIONS))}")
    kind = _event_kind(attrs)
    if kind is None:
        return None, (f"unknown trailbeam.kind {attrs.get('trailbeam.kind')!r} / messaging.operation.name "
                      f"{attrs.get('messaging.operation.name')!r}")
    bad = _span_checks(trace_id, span_id, start_ns, end_ns)
    if bad:
        return None, bad
    if kind in ("purge", "publish") and attrs.get("trailbeam.start_observed") is False:
        return None, f"{kind} span without an observed start"  # always a client call (sections 18, 21.1)
    observed = attrs.get("trailbeam.start_observed") is not False
    rec = {"trace_id": trace_id, "span_id": span_id, "kind": kind, "time_ns": int(end_ns),
           "start_ns": int(start_ns) if observed else None,
           "service_name": _resource_str(resource, "service.name"),
           "service_version": _resource_str(resource, "service.version")}
    for attr, (name, typ) in _FIELDS.items():
        v = attrs.get(attr)
        if typ is int and isinstance(v, str) and v.lstrip("-").isdigit():
            v = int(v)
        rec[name] = v if isinstance(v, typ) and not isinstance(v, bool) and v != "" else None
    missing = [f for f in _REQUIRED[kind] if rec[f] is None]
    if missing:
        return None, f"{kind} span without {', '.join(missing)}"
    if kind in ("delete", "change_visibility", "purge") and rec["result"] not in ("ok", "failed"):
        return None, f"trailbeam.result {rec['result']!r} is not ok or failed"
    if kind == "publish" and not valid_topic_arn(attrs.get("aws.sns.topic.arn")):
        return None, "publish span without a valid aws.sns.topic.arn"
    if kind == "outcome" and rec["outcome"] not in OUTCOMES:
        return None, f"unknown trailbeam.outcome {rec['outcome']!r}"
    if kind != "publish":  # an SNS message ID names no message of a queue (store.py, Publishes)
        rec["message_id"] = message_key(SQS, rec["message_id"])
    rec["attributes"] = attrs
    return rec, ""


def _span_checks(trace_id: str, span_id: str, start_ns: int, end_ns: int) -> str:
    if not trace_id or not span_id or not start_ns or not end_ns:
        return "span without trace ID, span ID, start time or end time"
    if end_ns < start_ns:
        return "span ends before it starts"
    return ""


def _amqp_int(v) -> Optional[int]:
    """An integer attribute: an OTLP int, or a decimal string (A12); None for anything else, booleans included."""
    if isinstance(v, bool):
        return None
    if isinstance(v, int):
        return v
    if isinstance(v, str) and re.fullmatch(r"-?[0-9]+", v):
        return int(v)
    return None


def _amqp_has(name: str, v) -> bool:
    if name in AMQP_BOOLS:
        return isinstance(v, bool)
    if name in AMQP_INTS:
        return _amqp_int(v) is not None
    return isinstance(v, str) and v != ""


def amqp_place(vhost: str, name: str, exchange: bool = False) -> str:
    """The ``queue_url`` of a RabbitMQ queue (or exchange) of a virtual host: ``amqp://rabbitmq/<vhost>/<queue>``,
    each part percent-encoded, exchanges under ``exchange/``. Its last path segment is the name, so queues.key
    keys it by this URL and queues.place_of_key names it. Brokers are not told apart (A3), so the host is fixed."""
    return ("amqp://rabbitmq/" + quote(vhost, safe="") + ("/exchange/" if exchange else "/")
            + quote(name, safe=""))


def _amqp_kind(kind: str, attrs: dict) -> str:
    if kind in ("amqp.nack", "amqp.reject"):
        return "change_visibility" if attrs["trailbeam.amqp.requeue"] else "reject"
    return {"amqp.publish": "send", "amqp.return": "return", "amqp.deliver": "receive", "amqp.ack": "delete",
            "amqp.cancel": "cancel", "amqp.channel_closed": "channel_closed"}[kind]


def _amqp_record(resource: dict, trace_id: str, span_id: str, start_ns: int, end_ns: int,
                 attrs: dict) -> tuple[Optional[dict], str]:
    """A RabbitMQ span (contract/span-contract-amqp.md) as a record, checked as A12 says: (record, "") or (None,
    reason). The reasons are A12's, in its order."""
    kind = attrs["trailbeam.kind"]
    version = attrs.get("trailbeam.schema_version")
    if version != AMQP_SCHEMA_VERSION or isinstance(version, bool):
        return None, f"amqp span with trailbeam.schema_version {version}; needs {AMQP_SCHEMA_VERSION}"
    system = attrs.get("messaging.system.name")
    if system != RABBITMQ:
        return None, f"amqp span with messaging.system.name {system!r}; needs {RABBITMQ!r}"
    op = attrs.get("messaging.operation.name")
    if op not in AMQP_OPERATIONS.get(kind, ()):
        return None, f"unknown trailbeam.kind {kind!r} / messaging.operation.name {op!r}"
    missing = [a for a in AMQP_REQUIRED[kind] if not _amqp_has(a, attrs.get(a))]
    if missing:
        return None, f"{kind} span without {', '.join(missing)}"
    for name, allowed in AMQP_ENUMS.items():
        if name in attrs and attrs[name] not in allowed:
            return None, f"{kind} span with {name} {attrs[name]!r}; not one of {', '.join(allowed)}"
    if "trailbeam.result" in attrs and attrs["trailbeam.result"] not in ("ok", "failed"):
        return None, f"trailbeam.result {attrs['trailbeam.result']!r} is not ok or failed"
    bad = _span_checks(trace_id, span_id, start_ns, end_ns)
    if bad:
        return None, bad

    record_kind = _amqp_kind(kind, attrs)
    observed = attrs.get("trailbeam.start_observed") is not False
    rec = {f: None for f in RECORD_FIELDS}
    rec.update({"trace_id": trace_id, "span_id": span_id, "kind": record_kind, "time_ns": int(end_ns),
                "start_ns": int(start_ns) if observed else None,
                "service_name": _resource_str(resource, "service.name"),
                "service_version": _resource_str(resource, "service.version")})
    for attr, (name, typ) in _FIELDS.items():
        if name in _AMQP_FIELDS:
            v = attrs.get(attr)
            if typ is int:
                v = _amqp_int(v)
            rec[name] = v if isinstance(v, typ) and not isinstance(v, bool) and v != "" else None
    vhost = attrs[_VHOST]
    rec["queue_url"] = amqp_place(vhost, rec["queue"], exchange=kind in ("amqp.publish", "amqp.return"))
    mid = attrs.get("messaging.message.id")
    rec["message_id"] = message_key(RABBITMQ, mid) if isinstance(mid, str) else None
    # A span about one delivery of a message: its join key (A6). A delivery without a message ID belongs to no
    # message (A6, P2), so neither it nor its settles are joined: each is kept and dropped by its own time.
    if _TAG in AMQP_REQUIRED[kind] and rec["message_id"] is not None:
        rec["receipt_hmac"] = f"{attrs[_CHANNEL]}:{_amqp_int(attrs[_TAG])}"
        rec["key_id"] = AMQP_KEY_ID
    count = _amqp_int(attrs.get("trailbeam.receive_count"))
    if record_kind == "receive" and count is not None and count >= 1 and "trailbeam.receive_count.source" in attrs:
        rec["receive_count"] = count  # A12: without its source, no count
    if record_kind == "change_visibility":
        rec["visibility_timeout"] = 0  # requeued: back in the queue at once (A2, "released to the queue")
    rec["attributes"] = attrs
    return rec, ""


def _from_spans(spans: Iterable[tuple]) -> Decoded:
    out = Decoded()
    for scope, resource, trace_id, span_id, start_ns, end_ns, attrs in spans:
        if scope != SCOPE_NAME:
            out.ignored += 1
            continue
        if attrs.get("trailbeam.kind") == PROBE_KIND:
            probe_id, reason = probe_id_of(attrs)
            if probe_id is None:
                out.rejected.append(reason)
            elif len(out.probes) >= MAX_PROBES_PER_EXPORT:
                out.rejected.append(f"{PROBE_OVER_LIMIT} {MAX_PROBES_PER_EXPORT} in one export")
            else:
                out.probes.append(probe_id)
            continue
        rec, reason = to_record(resource, trace_id, span_id, start_ns, end_ns, attrs)
        if rec is None:
            out.rejected.append(reason)
        else:
            out.records.append(rec)
    return out


def valid_probe_id(v) -> bool:
    lo, hi = PROBE_ID_LENGTH
    return isinstance(v, str) and lo <= len(v) <= hi and set(v) <= PROBE_ID_CHARS


def probe_id_of(attrs: dict) -> tuple[Optional[str], str]:
    """A probe span's ID (contract section 17), or (None, reason). Its times and trace are not read: the server
    records when it stored the probe."""
    if attrs.get("trailbeam.schema_version") not in ACCEPTED_SCHEMA_VERSIONS:
        return None, (f"trailbeam.schema_version {attrs.get('trailbeam.schema_version')!r} is not one of "
                      f"{', '.join(map(str, ACCEPTED_SCHEMA_VERSIONS))}")
    v = attrs.get(PROBE_ID)
    if not valid_probe_id(v):
        lo, hi = PROBE_ID_LENGTH
        return None, f"{PROBE_BAD_ID} of {lo} to {hi} letters, digits, - or _"
    return v, ""
