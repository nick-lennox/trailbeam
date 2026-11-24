"""Trailbeam hooks for boto3 / botocore SQS clients (contract sections 4.1 to 4.3, and 18 for PurgeQueue).

The hooks ride on ``opentelemetry-instrumentation-botocore``: each Trailbeam span is a child of the stock
span for the same API call. Usage::

    import trailbeam.boto
    trailbeam.boto.install()          # before or after creating clients; once per process

If the application has already called ``BotocoreInstrumentor().instrument(...)``, ``install`` keeps
that instrumentation (and its tracer provider) and chains the application's own hooks after ours.
Otherwise it instruments botocore with the Trailbeam tracer provider, so the stock span is exported too.
"""
from __future__ import annotations

import functools
import logging
import sys
import threading
import time
from typing import Any, Callable, Optional

from opentelemetry.instrumentation.botocore import BotocoreInstrumentor
from opentelemetry.sdk.trace import SpanProcessor, TracerProvider
from opentelemetry.sdk.trace.export import SpanExporter

from ._contract import (
    SPAN_CHANGE_VISIBILITY,
    SPAN_DELETE,
    SPAN_PURGE,
    SPAN_RECEIVE,
    SPAN_SEND,
    int_or_none,
    put,
    queue_name_from_url,
    resolve_hasher,
)
from . import _capture, _order
from ._emitter import Emitter, make_provider
from ._handles import HandleMap

log = logging.getLogger("trailbeam")

# System attributes the receive hook asks for (section 4.2).
RECEIVE_SYSTEM_ATTRIBUTES = (
    "ApproximateReceiveCount",
    "SentTimestamp",
    "ApproximateFirstReceiveTimestamp",
    "AWSTraceHeader",
    "DeadLetterQueueSourceArn",  # schema 3.1: SQS sets it only on a receive from a dead-letter queue
    "MessageGroupId",  # 3.2: FIFO queues (and fair standard queues)
    "SequenceNumber",  # 3.2: FIFO queues only
)

_SETTLE_OPS = {
    "DeleteMessage": (SPAN_DELETE, False),
    "DeleteMessageBatch": (SPAN_DELETE, True),
    "ChangeMessageVisibility": (SPAN_CHANGE_VISIBILITY, False),
    "ChangeMessageVisibilityBatch": (SPAN_CHANGE_VISIBILITY, True),
}


def add_receive_attribute_names(params: dict, system_names_param: bool = True) -> None:
    """Ask for the system attributes Trailbeam needs, keeping everything the caller asked for.

    ``system_names_param`` is False when the installed botocore predates ``MessageSystemAttributeNames``
    (added in 2024; botocore 1.34.0 rejects it as an unknown parameter): then the names go in
    ``AttributeNames``, which SQS still accepts for them."""
    new = list(params.get("MessageSystemAttributeNames") or [])
    legacy = list(params.get("AttributeNames") or [])
    asked = set(new) | set(legacy)
    if "All" in asked:
        return
    missing = [n for n in RECEIVE_SYSTEM_ATTRIBUTES if n not in asked]
    if not missing:
        return
    # A caller using only the deprecated AttributeNames keeps using it; otherwise use the current name.
    if (legacy and not new) or not system_names_param:
        params["AttributeNames"] = legacy + missing
    else:
        params["MessageSystemAttributeNames"] = new + missing

@functools.lru_cache(maxsize=1)
def botocore_has_system_names_param() -> bool:
    """Whether this botocore's SQS model has ``ReceiveMessage.MessageSystemAttributeNames``."""
    try:
        from botocore.loaders import create_loader

        model = create_loader().load_service_model("sqs", "service-2")
        request = model["operations"]["ReceiveMessage"]["input"]["shape"]
        return "MessageSystemAttributeNames" in model["shapes"][request]["members"]
    except Exception:  # pragma: no cover - an unexpected model layout: assume a current botocore
        log.debug("trailbeam: could not read botocore's SQS model", exc_info=True)
        return True


def add_message_attribute_name(params: dict, name: Optional[str]) -> None:
    """3.2: ask for one message attribute (the configured tenant attribute, or an Extended Client size
    attribute), keeping everything the caller asked for."""
    if not name:
        return
    asked = list(params.get("MessageAttributeNames") or [])
    if name in asked or "All" in asked or ".*" in asked:
        return
    params["MessageAttributeNames"] = asked + [name]


_MAX_PENDING = 10_000
_now = time.time_ns  # wall clock for span start and end (contract section 2); replaced in tests
_TRACKED_OPS = frozenset(_SETTLE_OPS) | {"SendMessage", "SendMessageBatch", "ReceiveMessage", "PurgeQueue"}


class SqsHooks:
    """request_hook / response_hook pair for BotocoreInstrumentor. Hooks never raise."""

    def __init__(self, emitter: Emitter, handles: Optional[HandleMap] = None,
                 capture: Optional[_capture.Config] = None):
        self.emitter = emitter
        self.capture = capture if capture is not None else _capture.Config()
        self.handles = handles if handles is not None else HandleMap()
        # Requests waiting for their response, keyed by the stock span object's id:
        # (params, start_ns, order scope, issued tick).
        self._pending: dict[int, tuple[dict, int, str, int]] = {}
        self._lock = threading.Lock()

    # -- hooks -------------------------------------------------------------------------------

    def request_hook(self, span, service: str, operation: str, params: dict) -> None:
        if service != "sqs":
            return
        try:
            if operation not in _TRACKED_OPS:
                return
            if operation == "ReceiveMessage":
                add_receive_attribute_names(params, botocore_has_system_names_param())
                add_message_attribute_name(params, self.capture.tenant_attribute)
                for name in _capture.OFFLOADED_SIZE_ATTRIBUTES:
                    add_message_attribute_name(params, name)
            with self._lock:
                if len(self._pending) >= _MAX_PENDING:  # only if responses were lost; stay bounded
                    self._pending.clear()
                # Section 2: the span starts just before the request is sent. The instrumentation calls
                # this hook right before the API call (and its retries), so take the time last, with the
                # call-order tick (schema version 3) immediately before it.
                issued = _order.tick()
                self._pending[id(span)] = (params, _now(), _order.scope, issued)
        except Exception:  # pragma: no cover - defensive; never break the caller's SQS call
            log.debug("trailbeam request hook failed", exc_info=True)

    def response_hook(self, span, service: str, operation: str, result: Optional[dict]) -> None:
        if service != "sqs":
            return
        end_ns = _now()  # section 2: the span ends when the response (or the error) arrived
        if operation not in _TRACKED_OPS:
            return
        returned = _order.tick()
        with self._lock:
            pending = self._pending.pop(id(span), None)
        if pending is None:
            return
        params, start_ns, scope, issued = pending
        if scope != _order.scope:  # forked while the call was in flight: the issued tick is the parent's
            issued = None
        t = (start_ns, end_ns, (issued, returned))
        try:
            if operation == "ReceiveMessage":
                self._on_receive(span, params, result, t)
            elif operation in ("SendMessage", "SendMessageBatch"):
                self._on_send(span, operation, params, result, t)
            else:
                # The instrumentation calls this hook from a ``finally``: during an exception that
                # was not a ClientError, result is None and sys.exc_info() holds the exception.
                exc = sys.exc_info()[1]
                if operation == "PurgeQueue":
                    self._on_purge(span, params, result, exc, t)
                else:
                    self._on_settle(span, operation, params, result, exc, t)
        except Exception:  # pragma: no cover - defensive
            log.debug("trailbeam response hook failed", exc_info=True)

    # -- span builders -----------------------------------------------------------------------

    def _on_send(self, span, operation: str, params: dict, result: Optional[dict], t: tuple) -> None:
        if not result or "Error" in result:
            return
        name, url = _queue(params)
        if operation == "SendMessage":
            if result.get("MessageId"):
                attrs = {"messaging.message.id": result["MessageId"]}
                attrs.update(self._sent(name, params, result))
                self.emitter.emit(SPAN_SEND, operation, name, url, attrs,
                                  parent=span, start_ns=t[0], end_ns=t[1], order=t[2])
            return
        requests = {e.get("Id"): e for e in params.get("Entries") or [] if isinstance(e, dict)}
        for entry in result.get("Successful") or []:
            attrs: dict = {}
            put(attrs, "messaging.message.id", entry.get("MessageId"))
            put(attrs, "trailbeam.batch_entry_id", entry.get("Id"))
            request = requests.get(entry.get("Id"))
            if request is not None:  # the body comes from the request entry with the same Id
                attrs.update(self._sent(name, request, entry))
            if "messaging.message.id" in attrs:
                self.emitter.emit(SPAN_SEND, operation, name, url, attrs, parent=span, start_ns=t[0], end_ns=t[1], order=t[2])

    def _on_receive(self, span, params: dict, result: Optional[dict], t: tuple) -> None:
        if not result or "Error" in result:
            return
        name, url = _queue(params)
        for m in result.get("Messages") or []:
            rh = m.get("ReceiptHandle")
            mid = m.get("MessageId")
            if not rh or not mid:
                continue
            self.handles.put(rh, mid)
            a = m.get("Attributes") or {}
            attrs = {"messaging.message.id": mid}
            attrs.update(self.emitter.hash_attrs(rh))
            put(attrs, "trailbeam.receive_count", int_or_none(a.get("ApproximateReceiveCount")))
            put(attrs, "trailbeam.sent_timestamp_ms", int_or_none(a.get("SentTimestamp")))
            put(attrs, "trailbeam.first_receive_timestamp_ms", int_or_none(a.get("ApproximateFirstReceiveTimestamp")))
            put(attrs, "trailbeam.aws_trace_header", a.get("AWSTraceHeader"))
            put(attrs, "trailbeam.dead_letter_queue_source_arn", a.get("DeadLetterQueueSourceArn"))
            attrs.update(_capture.fifo_attrs(a.get("MessageGroupId"), a.get("SequenceNumber")))
            attrs.update(self._contents(name, m.get("Body"), m.get("MessageAttributes"), m.get("MD5OfBody")))
            self.emitter.emit(SPAN_RECEIVE, "ReceiveMessage", name, url, attrs, parent=span, start_ns=t[0], end_ns=t[1], order=t[2])

    def _contents(self, queue_name: str, body, attributes, md5) -> dict:
        try:
            return _capture.content_attrs(self.capture, queue_name, body, attributes, md5)
        except Exception:  # pragma: no cover - defensive; a capture bug must not lose the span
            log.debug("trailbeam content attributes failed", exc_info=True)
            return {"trailbeam.body.capture_status": _capture.STATUS_REDACTION_FAILED}

    def _sent(self, queue_name: str, request: dict, response: dict) -> dict:
        """3.2 attributes of one sent message: the body from the request, checked against the response's MD5."""
        attrs = _capture.fifo_attrs(request.get("MessageGroupId"), response.get("SequenceNumber"))
        attrs.update(self._contents(queue_name, request.get("MessageBody"), request.get("MessageAttributes"),
                                    response.get("MD5OfMessageBody")))
        return attrs

    def _on_purge(self, span, params: dict, result: Optional[dict], exc, t: tuple) -> None:
        """3.3 (section 18): one span per PurgeQueue call, ok or failed. SQS does not say which messages went."""
        name, url = _queue(params)
        error = _call_error(result, exc)
        attrs = {"trailbeam.result": "ok" if error is None else "failed"}
        put(attrs, "trailbeam.error_code", _plain_code(error))
        self.emitter.emit(SPAN_PURGE, "PurgeQueue", name, url, attrs, parent=span, start_ns=t[0], end_ns=t[1], order=t[2])

    def _on_settle(self, span, operation: str, params: dict, result: Optional[dict], exc, t: tuple) -> None:
        span_name, batch = _SETTLE_OPS[operation]
        name, url = _queue(params)
        call_error = _call_error(result, exc)

        if batch:
            entries = [(e.get("Id"), e.get("ReceiptHandle"), e.get("VisibilityTimeout"))
                       for e in params.get("Entries") or []]
        else:
            entries = [(None, params.get("ReceiptHandle"), params.get("VisibilityTimeout"))]

        ok_ids: set = set()
        failed: dict = {}
        if call_error is None and batch:
            ok_ids = {e.get("Id") for e in result.get("Successful") or []}
            failed = {e.get("Id"): e.get("Code") or "Failed" for e in result.get("Failed") or []}

        for entry_id, rh, vt in entries:
            if not isinstance(rh, str) or not rh:
                continue
            if call_error is not None:
                error = call_error
            elif not batch or entry_id in ok_ids:
                error = None
            else:
                error = failed.get(entry_id, "MissingFromResponse")
            attrs = self.emitter.hash_attrs(rh)
            put(attrs, "messaging.message.id", self.handles.get(rh))
            if batch:
                put(attrs, "trailbeam.batch_entry_id", entry_id)
            if span_name == SPAN_CHANGE_VISIBILITY:
                put(attrs, "trailbeam.visibility_timeout", int_or_none(vt))
            attrs["trailbeam.result"] = "ok" if error is None else "failed"
            put(attrs, "trailbeam.error_code", error)
            self.emitter.emit(span_name, operation, name, url, attrs, parent=span, start_ns=t[0], end_ns=t[1], order=t[2])
            if error is None and span_name == SPAN_DELETE:
                self.handles.discard(rh)


def _call_error(result: Optional[dict], exc) -> Optional[str]:
    """The error of a whole call, or None if SQS answered with success."""
    if result is None:
        return type(exc).__name__ if exc is not None else "NoResponse"
    if "Error" in result:  # ClientError: the whole call failed
        return (result.get("Error") or {}).get("Code") or "ClientError"
    return None


def _plain_code(code: Optional[str]) -> Optional[str]:
    """An error code without a protocol namespace (``com.amazonaws.sqs#QueueDoesNotExist``), as section 18 asks."""
    if not code:
        return None
    return code[code.rfind("#") + 1:] or None


def _queue(params: dict) -> tuple[str, str]:
    """(queue name, queue URL) from the ``QueueUrl`` request parameter, verbatim (section 6)."""
    url = params.get("QueueUrl")
    if not isinstance(url, str) or not url:
        return "", ""
    return queue_name_from_url(url), url


# -- install / uninstall -------------------------------------------------------------------------

_state: dict[str, Any] = {}
_install_lock = threading.Lock()


def _instrumentor() -> BotocoreInstrumentor:
    """The BotocoreInstrumentor singleton, without calling its constructor again when it exists: in older
    releases (0.49b0 to at least 0.60b0), ``BotocoreInstrumentor.__init__`` sets the hooks to None on every call, which would drop the
    hooks the application gave ``instrument`` (QV-335, the oldest supported 0.49b0)."""
    existing = getattr(BotocoreInstrumentor, "_instance", None)
    return existing if isinstance(existing, BotocoreInstrumentor) else BotocoreInstrumentor()


def _chain(first: Callable, second: Optional[Callable]) -> Callable:
    if second is None:
        return first

    def hook(*args):
        first(*args)
        second(*args)

    return hook


def install(
    receipt_hash_key: Optional[str] = None,
    *,
    span_exporter: Optional[SpanExporter] = None,
    span_processor: Optional[SpanProcessor] = None,
    tenant: Optional[dict] = None,
    capture: Optional[dict] = None,
) -> TracerProvider:
    """Install the Trailbeam SQS hooks into botocore. Returns the Trailbeam tracer provider.

    ``receipt_hash_key`` takes precedence over $TRAILBEAM_RECEIPT_HASH_KEY. ``span_exporter`` /
    ``span_processor`` replace the default OTLP/HTTP export (used by the tests).

    ``tenant`` (``{"message_attribute": ..., "body_path": ...}``) and ``capture`` (``{"body_queues":
    [...], "error_queues": [...], "redact_keys": [...], "redact_patterns": [...], "max_body_bytes": n}``)
    are contract section 15; each replaces its TRAILBEAM_* environment variables. Body capture is off
    unless a queue is listed in ``body_queues``.
    Calling ``install`` again replaces the previous installation.
    """
    with _install_lock:
        _uninstall_locked()
        config = _capture.resolve_config(tenant, capture)
        provider = make_provider(span_exporter, span_processor)
        hooks = SqsHooks(Emitter(provider, resolve_hasher(receipt_hash_key), source="client"), capture=config)
        inst = _instrumentor()  # a singleton
        if inst.is_instrumented_by_opentelemetry:
            prev = (inst.request_hook, inst.response_hook)
            inst.request_hook = _chain(hooks.request_hook, prev[0])
            inst.response_hook = _chain(hooks.response_hook, prev[1])
            _state.update(provider=provider, hooks=hooks, prev_hooks=prev, instrumented_by_us=False)
        else:
            inst.instrument(tracer_provider=provider, request_hook=hooks.request_hook,
                            response_hook=hooks.response_hook)
            _state.update(provider=provider, hooks=hooks, prev_hooks=None, instrumented_by_us=True)
        return provider


def _uninstall_locked() -> None:
    if not _state:
        return
    inst = _instrumentor()
    if _state["instrumented_by_us"]:
        inst.uninstrument()
    else:
        inst.request_hook, inst.response_hook = _state["prev_hooks"]
    provider: TracerProvider = _state["provider"]
    provider.shutdown()
    _state.clear()


def uninstall() -> None:
    """Remove the Trailbeam hooks, flush and shut down the Trailbeam tracer provider."""
    with _install_lock:
        _uninstall_locked()
