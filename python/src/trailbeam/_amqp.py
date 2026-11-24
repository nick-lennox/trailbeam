"""RabbitMQ spans (contract/span-contract-amqp.md, schema version 4), independent of the client library.

The pika hooks (``trailbeam.pika``) turn what the application calls and what the broker sends into the plain
values these methods take; an aio-pika or another AMQP 0-9-1 client's hooks can reuse them. Nothing here talks
to the broker (A4.3): every span is built from a call the application made or a frame the broker sent.

Per channel the package keeps a ``ChannelState``: a random channel ID (A3, A6), whether the channel is in
confirm mode and the publishes waiting for their confirm (A4.1), the consumers' queues, and the deliveries it
emitted and has not seen settled (A4.4, A4.6). Every record is bounded.
"""
from __future__ import annotations

import calendar
import logging
import os
import threading
import time
import uuid
from collections import OrderedDict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Mapping, Optional

from . import _capture, _order

log = logging.getLogger("trailbeam")

AMQP_SCHEMA_VERSION = 4
RABBITMQ = "rabbitmq"
DEFAULT_EXCHANGE = "amq.default"  # A5: the name the OpenTelemetry conventions use for ""

SPAN_PUBLISH = "trailbeam publish"
SPAN_RETURN = "trailbeam return"
SPAN_DELIVER = "trailbeam deliver"
SPAN_ACK = "trailbeam ack"
SPAN_NACK = "trailbeam nack"
SPAN_REJECT = "trailbeam reject"
SPAN_CANCEL = "trailbeam cancel"
SPAN_CHANNEL_CLOSED = "trailbeam channel_closed"

# span name -> (trailbeam.kind, messaging.operation.type), A4
KINDS = {
    SPAN_PUBLISH: ("amqp.publish", "send"),
    SPAN_RETURN: ("amqp.return", "send"),
    SPAN_DELIVER: ("amqp.deliver", "receive"),
    SPAN_ACK: ("amqp.ack", "settle"),
    SPAN_NACK: ("amqp.nack", "settle"),
    SPAN_REJECT: ("amqp.reject", "settle"),
    SPAN_CANCEL: ("amqp.cancel", "cancel"),
    SPAN_CHANNEL_CLOSED: ("amqp.channel_closed", "settle"),
}
SETTLE_SPANS = {"basic.ack": SPAN_ACK, "basic.nack": SPAN_NACK, "basic.reject": SPAN_REJECT}

# AMQP 0-9-1 reply codes, for trailbeam.error_code of a publish whose channel closed before its confirm (A4.1)
REPLY_CODE_NAMES = {
    311: "CONTENT_TOO_LARGE", 312: "NO_ROUTE", 313: "NO_CONSUMERS", 320: "CONNECTION_FORCED",
    402: "INVALID_PATH", 403: "ACCESS_REFUSED", 404: "NOT_FOUND", 405: "RESOURCE_LOCKED",
    406: "PRECONDITION_FAILED", 501: "FRAME_ERROR", 502: "SYNTAX_ERROR", 503: "COMMAND_INVALID",
    504: "CHANNEL_ERROR", 505: "UNEXPECTED_FRAME", 506: "RESOURCE_ERROR", 530: "NOT_ALLOWED",
    540: "NOT_IMPLEMENTED", 541: "INTERNAL_ERROR",
}

CLOSE_OPERATIONS = {"channel.close", "connection.close", "connection.lost"}
INITIATORS = ("client", "broker", "network")

MAX_DELIVERIES_PER_CHANNEL = 100_000  # only reached with no prefetch limit and nothing settled
MAX_PUBLISHES_PER_CHANNEL = 100_000  # publishes waiting for a confirm
MAX_CONSUMERS_PER_CHANNEL = 10_000
MAX_PACKAGE_IDS = 10_000

_now = time.time_ns  # replaced in tests


def exchange_name(exchange: Any) -> str:
    """A5: the exchange as Trailbeam names it; the default exchange ``""`` is ``amq.default``."""
    return exchange if isinstance(exchange, str) and exchange else DEFAULT_EXCHANGE


def _amqp_int(v: Any) -> Optional[int]:
    """An AMQP integer header value: an int that is not a bool (A7: anything else is absent)."""
    return v if isinstance(v, int) and not isinstance(v, bool) else None


def _text(v: Any) -> Optional[str]:
    if isinstance(v, (bytes, bytearray)):
        try:
            v = bytes(v).decode("utf-8")
        except UnicodeDecodeError:
            return None
    return v if isinstance(v, str) and v else None


def _timestamp_ms(v: Any) -> Optional[int]:
    """An AMQP timestamp (POSIX seconds) in milliseconds. Client libraries decode it to an int or a datetime."""
    if isinstance(v, datetime):
        if v.tzinfo is None:
            v = v.replace(tzinfo=timezone.utc)
        return calendar.timegm(v.utctimetuple()) * 1000
    n = _amqp_int(v)
    return n * 1000 if n is not None and n >= 0 else None


def receive_count(redelivered: bool, headers: Any) -> tuple[Optional[int], Optional[str]]:
    """A7: (trailbeam.receive_count, trailbeam.receive_count.source), or (None, None) when the broker gave none."""
    h = headers if isinstance(headers, Mapping) else {}
    acquired = _amqp_int(h.get("x-acquired-count"))
    if acquired is not None:
        return acquired + 1, "x-acquired-count"
    delivered = _amqp_int(h.get("x-delivery-count"))
    if delivered is not None:
        return delivered + 1, "x-delivery-count"
    if not redelivered:
        return 1, "first_delivery"
    return None, None


def dead_letter_attrs(x_death: Any, headers: Any = None) -> dict:
    """A8: the trailbeam.dead_letter.* attributes from the newest ``x-death`` entry; {} when it is not usable."""
    if not isinstance(x_death, (list, tuple)) or not x_death or not isinstance(x_death[0], Mapping):
        return {}
    first = x_death[0]
    queue, reason = _text(first.get("queue")), _text(first.get("reason"))
    if queue is None or reason is None:
        return {}
    out: dict = {"trailbeam.dead_letter.source_queue": queue, "trailbeam.dead_letter.reason": reason}
    count = _amqp_int(first.get("count"))
    if count is not None:
        out["trailbeam.dead_letter.count"] = count
    t = _timestamp_ms(first.get("time"))
    if t is not None:
        out["trailbeam.dead_letter.time_ms"] = t
    exchange = first.get("exchange")
    if isinstance(exchange, (bytes, bytearray)):
        exchange = _text(exchange) or None
    if isinstance(exchange, str):
        out["trailbeam.dead_letter.exchange"] = exchange_name(exchange)
    h = headers if isinstance(headers, Mapping) else {}
    for header, attr in (("x-first-death-queue", "trailbeam.dead_letter.first_queue"),
                         ("x-first-death-reason", "trailbeam.dead_letter.first_reason")):
        v = _text(h.get(header))
        if v is not None:
            out[attr] = v
    return out


@dataclass
class Delivery:
    """A delivery the package emitted and has not seen settled (A4.4)."""

    queue: str
    consumer_tag: Optional[str]
    message_id: Optional[str]


@dataclass
class _Call:
    """A call in flight: its start time and issued tick (A4: the interval of the call, with both ticks)."""

    start_ns: int
    issued: int
    scope: str


@dataclass
class _Pending:
    """A publish waiting for its confirm, or a client cancel waiting for its cancel-ok."""

    attrs: dict
    call: _Call


@dataclass
class ChannelState:
    """What the package knows about one channel. ``id`` is the random UUID of A3, chosen when the channel is
    first seen; channel numbers are reused, IDs are not."""

    vhost: str = "/"
    host: Optional[str] = None
    port: Optional[int] = None
    number: Optional[int] = None
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    confirm: bool = False
    seq: int = 0  # publish sequence number of the last publish in confirm mode
    publishes: "OrderedDict[int, _Pending]" = field(default_factory=OrderedDict)
    deliveries: "OrderedDict[int, Delivery]" = field(default_factory=OrderedDict)
    consumers: "OrderedDict[str, tuple[str, bool]]" = field(default_factory=OrderedDict)  # tag -> (queue, auto_ack)
    get: Optional[tuple] = None  # a basic.get in flight: (queue, auto_ack, _Call)
    cancels: "dict[str, _Pending]" = field(default_factory=dict)
    closed: bool = False

    def consumer(self, tag: Any) -> Optional[tuple[str, bool]]:
        return self.consumers.get(tag) if isinstance(tag, str) else None

    def add_consumer(self, tag: str, queue: str, auto_ack: bool) -> None:
        self.consumers.pop(tag, None)
        self.consumers[tag] = (queue, bool(auto_ack))
        while len(self.consumers) > MAX_CONSUMERS_PER_CHANNEL:
            self.consumers.popitem(last=False)

    def record(self, tag: int, delivery: Delivery) -> None:
        self.deliveries.pop(tag, None)
        self.deliveries[tag] = delivery
        while len(self.deliveries) > MAX_DELIVERIES_PER_CHANNEL:
            self.deliveries.popitem(last=False)

    def covered(self, tag: int, multiple: bool) -> list[tuple[int, Delivery]]:
        """The recorded deliveries a settle with this tag covers (A4.4): tag 0 with ``multiple`` is all of them."""
        if not multiple:
            d = self.deliveries.get(tag)
            return [(tag, d)] if d is not None else []
        return [(t, d) for t, d in self.deliveries.items() if tag == 0 or t <= tag]

    def unsettled_for(self, consumer_tag: str) -> int:
        return sum(1 for d in self.deliveries.values() if d.consumer_tag == consumer_tag)


class AmqpHooks:
    """Builds and emits the RabbitMQ spans. Every public method is safe to call from a client library's hook:
    a failure is logged at debug level and never reaches the application."""

    def __init__(self, emitter, capture: Optional[_capture.Config] = None, set_message_id: bool = False):
        self.emitter = emitter
        self.capture = capture if capture is not None else _capture.Config()
        self.set_message_id = set_message_id
        self._package_ids: "OrderedDict[str, None]" = OrderedDict()
        self._lock = threading.RLock()

    # -- helpers ------------------------------------------------------------------------------------

    @staticmethod
    def start_call() -> _Call:
        """Section 2: the tick just before the time, both just before the call."""
        issued = _order.tick()
        return _Call(_now(), issued, _order.scope)

    def new_message_id(self) -> str:
        """A6, design choice P2 option a: an ID the package sets on a publish without one (opt-in)."""
        mid = str(uuid.uuid4())
        with self._lock:
            self._package_ids[mid] = None
            while len(self._package_ids) > MAX_PACKAGE_IDS:
                self._package_ids.popitem(last=False)
        return mid

    def _id_attrs(self, message_id: Any) -> dict:
        mid = _text(message_id)
        if mid is None:
            return {}
        with self._lock:
            source = "package" if mid in self._package_ids else "application"
        return {"messaging.message.id": mid, "trailbeam.amqp.message_id.source": source}

    def _base(self, st: ChannelState, name: str, operation: str, destination: str, channel: bool) -> dict:
        kind, op_type = KINDS[name]
        a = {
            "trailbeam.schema_version": AMQP_SCHEMA_VERSION,
            "trailbeam.kind": kind,
            "trailbeam.source": "client",
            "messaging.system.name": RABBITMQ,
            "messaging.operation.type": op_type,
            "messaging.operation.name": operation,
            "messaging.destination.name": destination,
            "trailbeam.amqp.vhost": st.vhost or "/",
            "messaging.client.id": self.emitter.client_id,
            "process.pid": os.getpid(),
            "thread.name": threading.current_thread().name,
        }
        if st.host:
            a["server.address"] = st.host
        if isinstance(st.port, int):
            a["server.port"] = st.port
        if channel:  # A3: on every span but publish and return
            a["trailbeam.amqp.channel.id"] = st.id
            if isinstance(st.number, int):
                a["trailbeam.amqp.channel.number"] = st.number
        return a

    def _emit(self, name: str, attrs: dict, start_ns: int, end_ns: int, issued: Optional[int], returned: int,
              scope: str, start_observed: bool = True) -> None:
        if scope != _order.scope:  # forked while the call was in flight: the issued tick is the parent's
            issued = None
        attrs["trailbeam.order.scope"] = _order.scope
        if issued is not None and start_observed:
            attrs["trailbeam.order.issued"] = issued
        attrs["trailbeam.order.returned"] = returned
        if not start_observed:
            attrs["trailbeam.start_observed"] = False
            start_ns = end_ns
        attrs = {k: v for k, v in attrs.items() if v is not None and v != ""}
        span = self.emitter.tracer.start_span(name, attributes=attrs, start_time=min(start_ns, end_ns))
        span.end(end_time=end_ns)

    def _event(self, name: str, attrs: dict) -> None:
        """Something the broker sent, or a close: zero-length at this moment, start not observed (A4)."""
        end = _now()
        self._emit(name, attrs, end, end, None, _order.tick(), _order.scope, start_observed=False)

    def _end_call(self, name: str, attrs: dict, call: _Call, end_ns: Optional[int] = None,
                  returned: Optional[int] = None) -> None:
        if end_ns is None:
            end_ns = _now()
        if returned is None:
            returned = _order.tick()
        self._emit(name, attrs, call.start_ns, end_ns, call.issued, returned, call.scope)

    def _safely(self, fn: Callable, *args) -> Any:
        try:
            with self._lock:
                return fn(*args)
        except Exception:  # pragma: no cover - defensive; never break the application's AMQP call
            log.debug("trailbeam amqp hook failed", exc_info=True)
            return None

    # -- publish (A4.1) -----------------------------------------------------------------------------

    def publish_attrs(self, st: ChannelState, exchange: Any, routing_key: Any, mandatory: bool,
                      message_id: Any, delivery_mode: Any, headers: Any, body: Any, content_encoding: Any) -> dict:
        dest = exchange_name(exchange)
        a = self._base(st, SPAN_PUBLISH, "basic.publish", dest, channel=False)
        a["rabbitmq.destination.routing_key"] = _text(routing_key)
        a.update(self._id_attrs(message_id))
        a["trailbeam.amqp.mandatory"] = bool(mandatory)
        if _amqp_int(delivery_mode) in (1, 2):
            a["trailbeam.amqp.delivery_mode"] = delivery_mode
        a.update(_capture.amqp_content_attrs(self.capture, dest, body, headers, content_encoding))
        return a

    def publish(self, st: ChannelState, attrs: Optional[dict], send: Callable[[], Any]) -> Any:
        """Times ``send`` (the library's publish) and emits or parks its span. ``attrs`` from ``publish_attrs``;
        None when building them failed, and then the publish goes ahead with no span."""
        if attrs is None:
            return send()
        call = self.start_call()
        seq = None
        attrs["trailbeam.amqp.confirmed"] = st.confirm  # at the write: confirm mode may start after marshaling
        if st.confirm:
            with self._lock:
                seq = st.seq + 1
                st.seq = seq
                st.publishes[seq] = _Pending(attrs, call)
                while len(st.publishes) > MAX_PUBLISHES_PER_CHANNEL:
                    st.publishes.popitem(last=False)
        try:
            result = send()
        except BaseException as e:
            end = _now()
            with self._lock:
                if seq is not None and st.publishes.pop(seq, None) is not None and st.seq == seq:
                    st.seq = seq - 1  # the library raised before the frame went out: the broker never numbered it
            attrs["trailbeam.result"] = "failed"
            attrs["trailbeam.error_code"] = type(e).__name__
            self._safely(self._end_call, SPAN_PUBLISH, attrs, call, end)
            raise
        if seq is None:  # no confirms: ends when the library returns from writing the frame
            attrs["trailbeam.result"] = "ok"
            self._safely(self._end_call, SPAN_PUBLISH, attrs, call)
        return result

    def confirm_mode(self, st: ChannelState) -> None:
        st.confirm = True

    def on_confirm(self, st: ChannelState, ack: bool, tag: Any, multiple: bool) -> None:
        """The broker's basic.ack or basic.nack for publishes in confirm mode."""
        self._safely(self._on_confirm, st, ack, tag, multiple)

    def _on_confirm(self, st: ChannelState, ack: bool, tag: Any, multiple: bool) -> None:
        tag = _amqp_int(tag)
        if tag is None:
            return
        end, returned = _now(), _order.tick()
        if multiple:
            seqs = [s for s in st.publishes if tag == 0 or s <= tag]
        else:
            seqs = [tag] if tag in st.publishes else []
        for s in seqs:
            p = st.publishes.pop(s)
            p.attrs["trailbeam.result"] = "ok" if ack else "failed"
            if not ack:
                p.attrs["trailbeam.error_code"] = "nack"
            self._end_call(SPAN_PUBLISH, p.attrs, p.call, end, returned)

    # -- return (A4.2) ------------------------------------------------------------------------------

    def on_return(self, st: ChannelState, exchange: Any, routing_key: Any, reply_code: Any, reply_text: Any,
                  message_id: Any) -> None:
        self._safely(self._on_return, st, exchange, routing_key, reply_code, reply_text, message_id)

    def _on_return(self, st, exchange, routing_key, reply_code, reply_text, message_id) -> None:
        a = self._base(st, SPAN_RETURN, "basic.return", exchange_name(exchange), channel=False)
        a["rabbitmq.destination.routing_key"] = _text(routing_key)
        a.update(self._id_attrs(message_id))
        a["trailbeam.amqp.reply_code"] = _amqp_int(reply_code)
        a["trailbeam.amqp.reply_text"] = _text(reply_text)
        self._event(SPAN_RETURN, a)

    # -- deliver (A4.3) -----------------------------------------------------------------------------

    def on_deliver(self, st: ChannelState, consumer_tag: Any, delivery_tag: Any, redelivered: Any, exchange: Any,
                   routing_key: Any, message_id: Any, timestamp: Any, headers: Any, body: Any,
                   content_encoding: Any) -> None:
        """A push delivery, before the application's callback runs."""
        self._safely(self._on_deliver, st, consumer_tag, delivery_tag, redelivered, exchange, routing_key,
                     message_id, timestamp, headers, body, content_encoding)

    def _on_deliver(self, st, consumer_tag, delivery_tag, redelivered, exchange, routing_key, message_id,
                    timestamp, headers, body, content_encoding) -> None:
        consumer = st.consumer(consumer_tag)
        if consumer is None or not consumer[0]:
            return  # a consumer the package didn't see start: its queue is not known
        queue, auto_ack = consumer
        a = self._delivery_attrs(st, "basic.deliver", queue, consumer_tag, delivery_tag, redelivered, exchange,
                                 routing_key, message_id, timestamp, headers, body, content_encoding, auto_ack)
        if a is not None:
            self._event(SPAN_DELIVER, a)

    def get_started(self, st: ChannelState, queue: Any, auto_ack: bool) -> None:
        """The application called basic.get."""
        if isinstance(queue, str) and queue:
            st.get = (queue, bool(auto_ack), self.start_call())

    def get_failed(self, st: ChannelState) -> None:
        st.get = None

    def on_get_ok(self, st: ChannelState, delivery_tag: Any, redelivered: Any, exchange: Any, routing_key: Any,
                  message_id: Any, timestamp: Any, headers: Any, body: Any, content_encoding: Any) -> None:
        """basic.get-ok: the pull delivery, the interval of the call (a basic.get-empty gives no span)."""
        self._safely(self._on_get_ok, st, delivery_tag, redelivered, exchange, routing_key, message_id, timestamp,
                     headers, body, content_encoding)

    def _on_get_ok(self, st, delivery_tag, redelivered, exchange, routing_key, message_id, timestamp, headers,
                   body, content_encoding) -> None:
        pending, st.get = st.get, None
        if pending is None:
            return
        queue, auto_ack, call = pending
        end, returned = _now(), _order.tick()
        a = self._delivery_attrs(st, "basic.get", queue, None, delivery_tag, redelivered, exchange, routing_key,
                                 message_id, timestamp, headers, body, content_encoding, auto_ack)
        if a is not None:
            self._end_call(SPAN_DELIVER, a, call, end, returned)

    def _delivery_attrs(self, st, operation, queue, consumer_tag, delivery_tag, redelivered, exchange, routing_key,
                        message_id, timestamp, headers, body, content_encoding, auto_ack) -> Optional[dict]:
        tag = _amqp_int(delivery_tag)
        if tag is None:
            return None
        a = self._base(st, SPAN_DELIVER, operation, queue, channel=True)
        a["rabbitmq.message.delivery_tag"] = tag
        if operation == "basic.deliver":
            a["trailbeam.amqp.consumer_tag"] = _text(consumer_tag)
        a["trailbeam.amqp.exchange"] = exchange_name(exchange)
        a["rabbitmq.destination.routing_key"] = _text(routing_key)
        a["trailbeam.amqp.redelivered"] = bool(redelivered)
        mid = _text(message_id)
        a["messaging.message.id"] = mid
        h = headers if isinstance(headers, Mapping) else {}
        count, source = receive_count(bool(redelivered), h)
        if count is not None:
            a["trailbeam.receive_count"], a["trailbeam.receive_count.source"] = count, source
        a["trailbeam.amqp.acquired_count"] = _amqp_int(h.get("x-acquired-count"))
        a["trailbeam.amqp.delivery_count"] = _amqp_int(h.get("x-delivery-count"))
        a.update(dead_letter_attrs(h.get("x-death"), h))
        a["trailbeam.amqp.timestamp_ms"] = _timestamp_ms(timestamp)
        a.update(_capture.amqp_content_attrs(self.capture, queue, body, h, content_encoding))
        if not auto_ack:  # with auto_ack the broker settled it on sending: nothing to settle or requeue
            st.record(tag, Delivery(queue, _text(consumer_tag) if operation == "basic.deliver" else None, mid))
        return a

    # -- ack, nack, reject (A4.4) -------------------------------------------------------------------

    def settle(self, st: ChannelState, operation: str, delivery_tag: Any, multiple: bool, requeue: Optional[bool],
               send: Callable[[], Any]) -> Any:
        """Times ``send`` (the library's basic.ack, basic.nack or basic.reject) and emits one span per recorded
        delivery it covers."""
        name = SETTLE_SPANS[operation]
        tag = _amqp_int(delivery_tag)
        with self._lock:
            covered = st.covered(tag, bool(multiple)) if tag is not None else []
        if not covered:
            return send()
        call = self.start_call()
        try:
            result = send()
        except BaseException as e:
            self._safely(self._settled, st, name, operation, covered, multiple, requeue, call, type(e).__name__)
            raise
        self._safely(self._settled, st, name, operation, covered, multiple, requeue, call, None)
        return result

    def _settled(self, st, name, operation, covered, multiple, requeue, call, error) -> None:
        end, returned = _now(), _order.tick()
        for tag, d in covered:
            if error is None:
                st.deliveries.pop(tag, None)
            a = self._base(st, name, operation, d.queue, channel=True)
            a["messaging.message.id"] = d.message_id
            a["rabbitmq.message.delivery_tag"] = tag
            a["trailbeam.amqp.consumer_tag"] = d.consumer_tag
            if operation != "basic.reject":
                a["trailbeam.amqp.multiple"] = bool(multiple)
            if operation != "basic.ack":
                a["trailbeam.amqp.requeue"] = bool(requeue)
            a["trailbeam.result"] = "ok" if error is None else "failed"
            a["trailbeam.error_code"] = error
            self._end_call(name, a, call, end, returned)

    # -- consumers (A4.5) ---------------------------------------------------------------------------

    def consumer_started(self, st: ChannelState, consumer_tag: Any, queue: Any, auto_ack: bool) -> None:
        if isinstance(consumer_tag, str) and consumer_tag and isinstance(queue, str):
            with self._lock:
                st.add_consumer(consumer_tag, queue, auto_ack)

    def _cancel_attrs(self, st: ChannelState, consumer_tag: str, by: str) -> Optional[dict]:
        consumer = st.consumer(consumer_tag)
        if consumer is None or not consumer[0]:
            return None
        a = self._base(st, SPAN_CANCEL, "basic.cancel", consumer[0], channel=True)
        a["trailbeam.amqp.consumer_tag"] = consumer_tag
        a["trailbeam.amqp.cancelled_by"] = by
        a["trailbeam.amqp.unsettled_count"] = st.unsettled_for(consumer_tag)
        return a

    def cancel(self, st: ChannelState, consumer_tag: Any, waits: bool, send: Callable[[], Any]) -> Any:
        """A client basic.cancel. ``waits``: a cancel-ok will come (``cancel_ok`` ends the span); otherwise the span
        ends when ``send`` returns."""
        a = self._safely(self._cancel_attrs, st, consumer_tag, "client") if isinstance(consumer_tag, str) else None
        if a is None:
            return send()
        call = self.start_call()
        if waits:
            with self._lock:
                st.cancels[consumer_tag] = _Pending(a, call)
        try:
            result = send()
        except BaseException as e:
            with self._lock:
                st.cancels.pop(consumer_tag, None)
            a["trailbeam.result"], a["trailbeam.error_code"] = "failed", type(e).__name__
            self._safely(self._end_call, SPAN_CANCEL, a, call)
            raise
        if not waits:
            a["trailbeam.result"] = "ok"
            self._safely(self._end_call, SPAN_CANCEL, a, call)
        return result

    def cancel_ok(self, st: ChannelState, consumer_tag: Any) -> None:
        self._safely(self._cancel_ok, st, consumer_tag)

    def _cancel_ok(self, st: ChannelState, consumer_tag: Any) -> None:
        p = st.cancels.pop(consumer_tag, None) if isinstance(consumer_tag, str) else None
        if p is not None:
            p.attrs["trailbeam.result"] = "ok"
            self._end_call(SPAN_CANCEL, p.attrs, p.call)

    def on_broker_cancel(self, st: ChannelState, consumer_tag: Any) -> None:
        self._safely(self._on_broker_cancel, st, consumer_tag)

    def _on_broker_cancel(self, st: ChannelState, consumer_tag: Any) -> None:
        if not isinstance(consumer_tag, str):
            return
        a = self._cancel_attrs(st, consumer_tag, "broker")
        if a is not None:
            self._event(SPAN_CANCEL, a)

    # -- channel closed (A4.6) ----------------------------------------------------------------------

    def on_close(self, st: ChannelState, operation: str, initiator: str, reply_code: Any = None,
                 reply_text: Any = None) -> None:
        """The channel closed (``operation``: channel.close, connection.close or connection.lost). One span per
        unsettled delivery; publishes still waiting for a confirm fail, as do client cancels waiting for cancel-ok."""
        self._safely(self._on_close, st, operation, initiator, reply_code, reply_text)

    def _on_close(self, st, operation, initiator, reply_code, reply_text) -> None:
        if st.closed:
            return
        st.closed = True
        # Only a close the broker sent carries a reason worth showing; the client's own code is its choice.
        code = _amqp_int(reply_code) if initiator == "broker" else None
        text = _text(reply_text) if initiator == "broker" else None
        error = REPLY_CODE_NAMES.get(code, "channel_closed") if code is not None else "channel_closed"
        end, returned = _now(), _order.tick()
        for p in st.publishes.values():
            p.attrs["trailbeam.result"], p.attrs["trailbeam.error_code"] = "failed", error
            self._end_call(SPAN_PUBLISH, p.attrs, p.call, end, returned)
        st.publishes.clear()
        for p in st.cancels.values():
            p.attrs["trailbeam.result"], p.attrs["trailbeam.error_code"] = "failed", error
            self._end_call(SPAN_CANCEL, p.attrs, p.call, end, returned)
        st.cancels.clear()
        st.get = None
        for tag, d in st.deliveries.items():
            a = self._base(st, SPAN_CHANNEL_CLOSED, operation, d.queue, channel=True)
            a["messaging.message.id"] = d.message_id
            a["rabbitmq.message.delivery_tag"] = tag
            a["trailbeam.amqp.consumer_tag"] = d.consumer_tag
            a["trailbeam.amqp.close.initiator"] = initiator
            a["trailbeam.amqp.close.reply_code"] = code
            a["trailbeam.amqp.close.reply_text"] = text
            self._emit(SPAN_CHANNEL_CLOSED, a, end, end, None, returned, _order.scope, start_observed=False)
        st.deliveries.clear()
