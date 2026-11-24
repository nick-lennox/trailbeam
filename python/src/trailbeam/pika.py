"""Trailbeam hooks for pika, the RabbitMQ client (contract/span-contract-amqp.md, schema version 4). Usage::

    import trailbeam.pika
    trailbeam.pika.install()          # once per process, before opening connections

The hooks sit on ``pika.channel.Channel``, the channel every pika connection type uses underneath
(``BlockingConnection``, ``SelectConnection``, the asyncio, Tornado and Twisted adapters):

- ``basic_publish``, and the broker's confirms after ``confirm_delivery`` (a ``trailbeam publish`` per publish)
  and its ``basic.return`` (``trailbeam return``);
- deliveries to ``basic_consume`` consumers and ``basic_get`` replies (``trailbeam deliver``), emitted before
  the application's callback runs;
- ``basic_ack``, ``basic_nack`` and ``basic_reject`` (one span per delivery covered);
- ``basic_cancel`` and a cancel from the broker (``trailbeam cancel``);
- the channel or its connection closing with deliveries not settled (``trailbeam channel_closed``).

The hooks make no broker calls of their own and never change a message, except that ``set_message_id=True``
(or ``TRAILBEAM_AMQP_SET_MESSAGE_ID=on``) gives a publish without a ``message_id`` a random one (A6). A channel
opened before ``install`` is not followed. A consumer on the server-named queue ``""`` gets no spans: its queue
name is not known to the hooks.
"""
from __future__ import annotations

import copy
import functools
import inspect
import logging
import os
import threading
from typing import Any, Optional

import pika
import pika.channel
import pika.connection
from pika.adapters import blocking_connection
from opentelemetry.sdk.trace import SpanProcessor, TracerProvider
from opentelemetry.sdk.trace.export import SpanExporter

from . import _capture
from ._amqp import AmqpHooks, ChannelState
from ._emitter import Emitter, make_provider

log = logging.getLogger("trailbeam")

ENV_SET_MESSAGE_ID = "TRAILBEAM_AMQP_SET_MESSAGE_ID"
_STATE_ATTR = "_trailbeam_amqp"
_CONN_CLOSING_ATTR = "_trailbeam_closing"

_hooks: Optional[AmqpHooks] = None
_state: dict[str, Any] = {}
_install_lock = threading.Lock()


_st_lock = threading.Lock()


def _st(ch: pika.channel.Channel) -> ChannelState:
    st = getattr(ch, _STATE_ATTR, None)
    if st is not None:
        return st
    with _st_lock:  # pika 1.5's thread-safe Connection marshals publishes in the application's threads
        st = getattr(ch, _STATE_ATTR, None)
        if st is not None:
            return st
        params = getattr(getattr(ch, "connection", None), "params", None)
        port = getattr(params, "port", None)
        st = ChannelState(vhost=getattr(params, "virtual_host", None) or "/", host=getattr(params, "host", None),
                          port=port if isinstance(port, int) else None, number=getattr(ch, "channel_number", None))
        setattr(ch, _STATE_ATTR, st)
    return st


def _bind(orig, ch, args, kwargs) -> dict:
    """The call's arguments by name, with defaults, as the original method sees them."""
    b = _signature(orig).bind(ch, *args, **kwargs)
    b.apply_defaults()
    return b.arguments


def _bind_or_none(orig, ch, args, kwargs) -> Optional[dict]:
    """As ``_bind``; None when the arguments don't fit, so pika itself reports the mistake."""
    try:
        return _bind(orig, ch, args, kwargs)
    except TypeError:
        return None


@functools.lru_cache(maxsize=None)
def _signature(fn) -> inspect.Signature:
    return inspect.signature(fn)


def _props(properties) -> tuple:
    """(message_id, timestamp, headers, content_encoding, delivery_mode) of a BasicProperties, or Nones."""
    if properties is None:
        return None, None, None, None, None
    return (getattr(properties, "message_id", None), getattr(properties, "timestamp", None),
            getattr(properties, "headers", None), getattr(properties, "content_encoding", None),
            getattr(properties, "delivery_mode", None))


def _close_info(ch: pika.channel.Channel) -> tuple[str, str, Optional[int], Optional[str]]:
    """(operation, initiator, reply code, reply text) of a channel that is closing (A4.6)."""
    reason = getattr(ch, "_closing_reason", None)
    exc = pika.exceptions
    code, text = getattr(reason, "reply_code", None), getattr(reason, "reply_text", None)
    if isinstance(reason, exc.ChannelClosedByBroker):
        return "channel.close", "broker", code, text
    if isinstance(reason, exc.ConnectionClosedByBroker):
        return "connection.close", "broker", code, text
    if isinstance(reason, exc.ConnectionClosedByClient):
        return "connection.close", "client", None, None
    if isinstance(reason, exc.ChannelClosedByClient):
        closing = getattr(getattr(ch, "connection", None), _CONN_CLOSING_ATTR, False)
        return ("connection.close" if closing else "channel.close"), "client", None, None
    return "connection.lost", "network", None, None


# -- the patched methods --------------------------------------------------------------------------------------

def _wrap(name: str, make):
    orig = getattr(pika.channel.Channel, name)
    wrapper = functools.wraps(orig)(make(orig))
    wrapper._trailbeam_orig = orig
    return wrapper


_local = threading.local()  # depth of basic_publish calls on this thread: its inner steps are not hooked again


def _publish_attrs(h: AmqpHooks, ch, a: dict) -> Optional[dict]:
    """The publish's span attributes; with ``set_message_id``, first gives ``a["properties"]`` a message ID (a copy
    of the application's properties, which are not changed)."""
    try:
        props = a.get("properties")
        if h.set_message_id and not getattr(props, "message_id", None):
            props = copy.copy(props) if props is not None else pika.BasicProperties()
            props.message_id = h.new_message_id()
            a["properties"] = props
        mid, _, headers, encoding, mode = _props(props)
        return h.publish_attrs(_st(ch), a.get("exchange"), a.get("routing_key"), a.get("mandatory"), mid, mode,
                               headers, a.get("body"), encoding)
    except Exception:  # pragma: no cover - defensive
        log.debug("trailbeam: publish hook failed", exc_info=True)
        return None


def _basic_publish(orig):
    def basic_publish(self, *args, **kwargs):
        h = _hooks
        a = _bind_or_none(orig, self, args, kwargs) if h is not None else None
        if a is None:
            return orig(self, *args, **kwargs)
        attrs = _publish_attrs(h, self, a)
        _local.depth = getattr(_local, "depth", 0) + 1
        try:
            return h.publish(_st(self), attrs, lambda: orig(**a))
        finally:
            _local.depth -= 1

    return basic_publish


class _Frames(list):
    """A marshaled publish (pika 1.5's Channel._marshal_publish) carrying its span attributes to the write."""

    trailbeam_attrs: Optional[dict] = None


def _marshal_publish(orig):
    """pika 1.5's thread-safe Connection marshals a publish in the caller's thread and writes it later, on its
    IOLoop thread, without Channel.basic_publish: the span is the write (``_publish_marshaled``)."""
    def _marshal_publish(self, *args, **kwargs):
        h = _hooks
        a = _bind_or_none(orig, self, args, kwargs) if h is not None and not getattr(_local, "depth", 0) else None
        if a is None:
            return orig(self, *args, **kwargs)
        attrs = _publish_attrs(h, self, a)
        frames = _Frames(orig(**a))
        frames.trailbeam_attrs = attrs
        return frames

    return _marshal_publish


def _publish_marshaled(orig):
    def _publish_marshaled(self, marshaled_frames, *args, **kwargs):
        h = _hooks
        attrs = getattr(marshaled_frames, "trailbeam_attrs", None)
        if h is None or attrs is None or getattr(_local, "depth", 0):
            return orig(self, marshaled_frames, *args, **kwargs)
        marshaled_frames.trailbeam_attrs = None  # one span per publish, if the frames were written twice
        return h.publish(_st(self), attrs, lambda: orig(self, marshaled_frames, *args, **kwargs))

    return _publish_marshaled


def _confirm_delivery(orig):
    def confirm_delivery(self, *args, **kwargs):
        h = _hooks
        a = _bind_or_none(orig, self, args, kwargs) if h is not None else None
        if a is None:
            return orig(self, *args, **kwargs)
        user_cb = a.get("ack_nack_callback")
        if callable(user_cb) and not getattr(user_cb, "_trailbeam", False):
            st = _st(self)

            def ack_nack(method_frame, _user=user_cb):
                if _hooks is not None:
                    m = getattr(method_frame, "method", None)
                    _hooks.on_confirm(st, isinstance(m, pika.spec.Basic.Ack), getattr(m, "delivery_tag", None),
                                      bool(getattr(m, "multiple", False)))
                return _user(method_frame)

            ack_nack._trailbeam = True
            a["ack_nack_callback"] = ack_nack
        result = orig(**a)
        h.confirm_mode(_st(self))
        return result

    return confirm_delivery


def _basic_consume(orig):
    def basic_consume(self, *args, **kwargs):
        tag = orig(self, *args, **kwargs)
        h = _hooks
        if h is not None:
            try:
                a = _bind(orig, self, args, kwargs)
                h.consumer_started(_st(self), tag, a.get("queue"), bool(a.get("auto_ack")))
            except Exception:  # pragma: no cover - defensive
                log.debug("trailbeam: consume hook failed", exc_info=True)
        return tag

    return basic_consume


def _on_deliver(orig):
    def _on_deliver(self, method_frame, header_frame, body):
        h = _hooks
        if h is not None and self.is_open:
            m = method_frame.method
            if m.consumer_tag in getattr(self, "_consumers", {}):  # pika dispatches it (else it drops it)
                mid, ts, headers, encoding, _ = _props(getattr(header_frame, "properties", None))
                h.on_deliver(_st(self), m.consumer_tag, m.delivery_tag, m.redelivered, m.exchange, m.routing_key,
                             mid, ts, headers, body, encoding)
        return orig(self, method_frame, header_frame, body)

    return _on_deliver


def _basic_get(orig):
    def basic_get(self, *args, **kwargs):
        h = _hooks
        if h is None:
            return orig(self, *args, **kwargs)
        st = _st(self)
        try:
            a = _bind(orig, self, args, kwargs)
            if getattr(self, "_on_getok_callback", None) is None:  # pika raises on a second get in flight
                h.get_started(st, a.get("queue"), bool(a.get("auto_ack")))
        except Exception:  # pragma: no cover - defensive
            log.debug("trailbeam: get hook failed", exc_info=True)
        try:
            return orig(self, *args, **kwargs)
        except BaseException:
            if getattr(self, "_on_getok_callback", None) is None:
                h.get_failed(st)
            raise

    return basic_get


def _on_getok(orig):
    def _on_getok(self, method_frame, header_frame, body):
        h = _hooks
        if h is not None and getattr(self, "_on_getok_callback", None) is not None:
            m = method_frame.method
            mid, ts, headers, encoding, _ = _props(getattr(header_frame, "properties", None))
            h.on_get_ok(_st(self), m.delivery_tag, m.redelivered, m.exchange, m.routing_key, mid, ts, headers, body,
                        encoding)
        return orig(self, method_frame, header_frame, body)

    return _on_getok


def _on_getempty(orig):
    def _on_getempty(self, method_frame):
        if _hooks is not None:
            _hooks.get_failed(_st(self))
        return orig(self, method_frame)

    return _on_getempty


def _settle(operation: str):
    def make(orig):
        def settle(self, *args, **kwargs):
            h = _hooks
            if h is None:
                return orig(self, *args, **kwargs)
            a = _bind_or_none(orig, self, args, kwargs)
            if a is None:
                return orig(self, *args, **kwargs)
            return h.settle(_st(self), operation, a.get("delivery_tag"), bool(a.get("multiple", False)),
                            a.get("requeue"), lambda: orig(self, *args, **kwargs))

        return settle

    return make


def _basic_cancel(orig):
    def basic_cancel(self, *args, **kwargs):
        h = _hooks
        if h is None:
            return orig(self, *args, **kwargs)
        a = _bind_or_none(orig, self, args, kwargs)
        if a is None:
            return orig(self, *args, **kwargs)
        tag, user_cb = a.get("consumer_tag"), a.get("callback")
        st = _st(self)
        # pika sends nothing for a nowait cancel of a tag it no longer knows; nothing to time then
        known = tag in getattr(self, "_consumers", {}) and tag not in getattr(self, "_cancelled", set())
        if not known:
            return orig(self, *args, **kwargs)
        if callable(user_cb):
            def cancel_ok(method_frame, _user=user_cb):
                if _hooks is not None:
                    _hooks.cancel_ok(st, getattr(getattr(method_frame, "method", None), "consumer_tag", tag))
                return _user(method_frame)

            a["callback"] = cancel_ok
        return h.cancel(st, tag, callable(user_cb), lambda: orig(**a))

    return basic_cancel


def _on_cancel(orig):
    def _on_cancel(self, method_frame):
        h = _hooks
        if h is not None:
            h.on_broker_cancel(_st(self), getattr(method_frame.method, "consumer_tag", None))
        return orig(self, method_frame)

    return _on_cancel


def _on_return(orig):
    def _on_return(self, method_frame, header_frame, body):
        h = _hooks
        if h is not None:
            m = method_frame.method
            mid = _props(getattr(header_frame, "properties", None))[0]
            h.on_return(_st(self), m.exchange, m.routing_key, m.reply_code, m.reply_text, mid)
        return orig(self, method_frame, header_frame, body)

    return _on_return


def _transition_to_closed(orig):
    def _transition_to_closed(self):
        h = _hooks
        if h is not None and getattr(self, _STATE_ATTR, None) is not None:
            try:
                h.on_close(_st(self), *_close_info(self))
            except Exception:  # pragma: no cover - defensive
                log.debug("trailbeam: close hook failed", exc_info=True)
        return orig(self)

    return _transition_to_closed


def _connection_close(orig, impl_of=lambda c: c):
    """Connection.close (and BlockingConnection.close, which closes its channels first): the channels it closes
    report ``connection.close``, not ``channel.close``."""
    def close(self, *args, **kwargs):
        try:
            setattr(impl_of(self), _CONN_CLOSING_ATTR, True)
        except Exception:  # pragma: no cover - defensive
            pass
        return orig(self, *args, **kwargs)

    close._trailbeam_orig = orig
    return functools.wraps(orig)(close)


_CHANNEL_PATCHES = {
    "basic_publish": _basic_publish,
    "_marshal_publish": _marshal_publish,  # pika 1.5 and later
    "_publish_marshaled": _publish_marshaled,
    "confirm_delivery": _confirm_delivery,
    "basic_consume": _basic_consume,
    "_on_deliver": _on_deliver,
    "basic_get": _basic_get,
    "_on_getok": _on_getok,
    "_on_getempty": _on_getempty,
    "basic_ack": _settle("basic.ack"),
    "basic_nack": _settle("basic.nack"),
    "basic_reject": _settle("basic.reject"),
    "basic_cancel": _basic_cancel,
    "_on_cancel": _on_cancel,
    "_on_return": _on_return,
    "_transition_to_closed": _transition_to_closed,
}


def _patch() -> None:
    for name, make in _CHANNEL_PATCHES.items():
        current = getattr(pika.channel.Channel, name, None)
        if current is not None and getattr(current, "_trailbeam_orig", None) is None:
            setattr(pika.channel.Channel, name, _wrap(name, make))
    conn = pika.connection.Connection
    if getattr(conn.close, "_trailbeam_orig", None) is None:
        conn.close = _connection_close(conn.close)
    blocking = blocking_connection.BlockingConnection
    if getattr(blocking.close, "_trailbeam_orig", None) is None:
        blocking.close = _connection_close(blocking.close, lambda c: c._impl)


def _unpatch() -> None:
    # Channels opened while installed registered some of these as callbacks; with _hooks None they pass through.
    for name in _CHANNEL_PATCHES:
        orig = getattr(getattr(pika.channel.Channel, name, None), "_trailbeam_orig", None)
        if orig is not None:
            setattr(pika.channel.Channel, name, orig)
    for cls in (pika.connection.Connection, blocking_connection.BlockingConnection):
        orig = getattr(cls.close, "_trailbeam_orig", None)
        if orig is not None:
            cls.close = orig


# -- install / uninstall --------------------------------------------------------------------------------------

def install(
    *,
    span_exporter: Optional[SpanExporter] = None,
    span_processor: Optional[SpanProcessor] = None,
    tenant: Optional[dict] = None,
    capture: Optional[dict] = None,
    set_message_id: Optional[bool] = None,
) -> TracerProvider:
    """Install the Trailbeam hooks into pika. Returns the Trailbeam tracer provider.

    ``span_exporter`` / ``span_processor`` replace the default OTLP/HTTP export to
    $TRAILBEAM_OTLP_TRACES_ENDPOINT. ``tenant`` (``{"message_attribute": header name, "body_path": ...}``) and
    ``capture`` are as for ``trailbeam.boto.install`` (A11: the tenant comes from a message header; the body
    lists name queues, and exchanges for publishes). ``set_message_id``: give a publish without a
    ``message_id`` a random UUID, marked ``trailbeam.amqp.message_id.source = package``; off unless True or
    $TRAILBEAM_AMQP_SET_MESSAGE_ID is on. Calling ``install`` again replaces the previous installation.
    """
    global _hooks
    with _install_lock:
        _uninstall_locked()
        if set_message_id is None:
            set_message_id = (os.environ.get(ENV_SET_MESSAGE_ID) or "").strip().lower() in ("on", "true", "1", "yes")
        provider = make_provider(span_exporter, span_processor)
        _hooks = AmqpHooks(Emitter(provider, None, source="client"), _capture.resolve_config(tenant, capture),
                           set_message_id=bool(set_message_id))
        _patch()
        _state["provider"] = provider
        return provider


def _uninstall_locked() -> None:
    global _hooks
    if not _state:
        return
    _hooks = None
    _unpatch()
    _state.pop("provider").shutdown()
    _state.clear()


def uninstall() -> None:
    """Remove the hooks, flush and shut down the Trailbeam tracer provider."""
    with _install_lock:
        _uninstall_locked()
