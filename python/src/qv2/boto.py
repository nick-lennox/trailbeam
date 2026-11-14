"""qv2 hooks for boto3 / botocore SQS clients (contract sections 4.1 to 4.3).

The hooks ride on ``opentelemetry-instrumentation-botocore``: each qv2 span is a child of the stock
span for the same API call. Usage::

    import qv2.boto
    qv2.boto.install()          # before or after creating clients; once per process

If the application has already called ``BotocoreInstrumentor().instrument(...)``, ``install`` keeps
that instrumentation (and its tracer provider) and chains the application's own hooks after ours.
Otherwise it instruments botocore with the qv2 tracer provider, so the stock span is exported too.
"""
from __future__ import annotations

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
    SPAN_RECEIVE,
    SPAN_SEND,
    int_or_none,
    put,
    queue_name_from_url,
    resolve_hasher,
)
from ._emitter import Emitter, make_provider
from ._handles import HandleMap

log = logging.getLogger("qv2")

# System attributes the receive hook asks for (section 4.2).
RECEIVE_SYSTEM_ATTRIBUTES = (
    "ApproximateReceiveCount",
    "SentTimestamp",
    "ApproximateFirstReceiveTimestamp",
    "AWSTraceHeader",
)

_SETTLE_OPS = {
    "DeleteMessage": (SPAN_DELETE, False),
    "DeleteMessageBatch": (SPAN_DELETE, True),
    "ChangeMessageVisibility": (SPAN_CHANGE_VISIBILITY, False),
    "ChangeMessageVisibilityBatch": (SPAN_CHANGE_VISIBILITY, True),
}


def add_receive_attribute_names(params: dict) -> None:
    """Ask for the system attributes qv2 needs, keeping everything the caller asked for."""
    new = list(params.get("MessageSystemAttributeNames") or [])
    legacy = list(params.get("AttributeNames") or [])
    asked = set(new) | set(legacy)
    if "All" in asked:
        return
    missing = [n for n in RECEIVE_SYSTEM_ATTRIBUTES if n not in asked]
    if not missing:
        return
    # A caller using only the deprecated AttributeNames keeps using it; otherwise use the current name.
    if legacy and not new:
        params["AttributeNames"] = legacy + missing
    else:
        params["MessageSystemAttributeNames"] = new + missing

_MAX_PENDING = 10_000
_now = time.time_ns  # wall clock for span start and end (contract section 2); replaced in tests
_TRACKED_OPS = frozenset(_SETTLE_OPS) | {"SendMessage", "SendMessageBatch", "ReceiveMessage"}


class SqsHooks:
    """request_hook / response_hook pair for BotocoreInstrumentor. Hooks never raise."""

    def __init__(self, emitter: Emitter, handles: Optional[HandleMap] = None):
        self.emitter = emitter
        self.handles = handles if handles is not None else HandleMap()
        # Requests waiting for their response, keyed by the stock span object's id: (params, start_ns).
        self._pending: dict[int, tuple[dict, int]] = {}
        self._lock = threading.Lock()

    # -- hooks -------------------------------------------------------------------------------

    def request_hook(self, span, service: str, operation: str, params: dict) -> None:
        if service != "sqs":
            return
        try:
            if operation not in _TRACKED_OPS:
                return
            if operation == "ReceiveMessage":
                add_receive_attribute_names(params)
            with self._lock:
                if len(self._pending) >= _MAX_PENDING:  # only if responses were lost; stay bounded
                    self._pending.clear()
                # Section 2: the span starts just before the request is sent. The instrumentation calls
                # this hook right before the API call (and its retries), so take the time last.
                self._pending[id(span)] = (params, _now())
        except Exception:  # pragma: no cover - defensive; never break the caller's SQS call
            log.debug("qv2 request hook failed", exc_info=True)

    def response_hook(self, span, service: str, operation: str, result: Optional[dict]) -> None:
        if service != "sqs":
            return
        end_ns = _now()  # section 2: the span ends when the response (or the error) arrived
        if operation not in _TRACKED_OPS:
            return
        with self._lock:
            pending = self._pending.pop(id(span), None)
        if pending is None:
            return
        params, start_ns = pending
        t = (start_ns, end_ns)
        try:
            if operation == "ReceiveMessage":
                self._on_receive(span, params, result, t)
            elif operation in ("SendMessage", "SendMessageBatch"):
                self._on_send(span, operation, params, result, t)
            else:
                # The instrumentation calls this hook from a ``finally``: during an exception that
                # was not a ClientError, result is None and sys.exc_info() holds the exception.
                exc = sys.exc_info()[1]
                self._on_settle(span, operation, params, result, exc, t)
        except Exception:  # pragma: no cover - defensive
            log.debug("qv2 response hook failed", exc_info=True)

    # -- span builders -----------------------------------------------------------------------

    def _on_send(self, span, operation: str, params: dict, result: Optional[dict], t: tuple) -> None:
        if not result or "Error" in result:
            return
        name, url = _queue(params)
        if operation == "SendMessage":
            if result.get("MessageId"):
                self.emitter.emit(SPAN_SEND, operation, name, url, {"messaging.message.id": result["MessageId"]},
                                  parent=span, start_ns=t[0], end_ns=t[1])
            return
        for entry in result.get("Successful") or []:
            attrs: dict = {}
            put(attrs, "messaging.message.id", entry.get("MessageId"))
            put(attrs, "qv2.batch_entry_id", entry.get("Id"))
            if "messaging.message.id" in attrs:
                self.emitter.emit(SPAN_SEND, operation, name, url, attrs, parent=span, start_ns=t[0], end_ns=t[1])

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
            put(attrs, "qv2.receive_count", int_or_none(a.get("ApproximateReceiveCount")))
            put(attrs, "qv2.sent_timestamp_ms", int_or_none(a.get("SentTimestamp")))
            put(attrs, "qv2.first_receive_timestamp_ms", int_or_none(a.get("ApproximateFirstReceiveTimestamp")))
            put(attrs, "qv2.aws_trace_header", a.get("AWSTraceHeader"))
            self.emitter.emit(SPAN_RECEIVE, "ReceiveMessage", name, url, attrs, parent=span, start_ns=t[0], end_ns=t[1])

    def _on_settle(self, span, operation: str, params: dict, result: Optional[dict], exc, t: tuple) -> None:
        span_name, batch = _SETTLE_OPS[operation]
        name, url = _queue(params)
        call_error = None
        if result is None:
            call_error = type(exc).__name__ if exc is not None else "NoResponse"
        elif "Error" in result:  # ClientError: the whole call failed
            call_error = (result.get("Error") or {}).get("Code") or "ClientError"

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
                put(attrs, "qv2.batch_entry_id", entry_id)
            if span_name == SPAN_CHANGE_VISIBILITY:
                put(attrs, "qv2.visibility_timeout", int_or_none(vt))
            attrs["qv2.result"] = "ok" if error is None else "failed"
            put(attrs, "qv2.error_code", error)
            self.emitter.emit(span_name, operation, name, url, attrs, parent=span, start_ns=t[0], end_ns=t[1])
            if error is None and span_name == SPAN_DELETE:
                self.handles.discard(rh)


def _queue(params: dict) -> tuple[str, str]:
    """(queue name, queue URL) from the ``QueueUrl`` request parameter, verbatim (section 6)."""
    url = params.get("QueueUrl")
    if not isinstance(url, str) or not url:
        return "", ""
    return queue_name_from_url(url), url


# -- install / uninstall -------------------------------------------------------------------------

_state: dict[str, Any] = {}
_install_lock = threading.Lock()


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
) -> TracerProvider:
    """Install the qv2 SQS hooks into botocore. Returns the qv2 tracer provider.

    ``receipt_hash_key`` takes precedence over $QV2_RECEIPT_HASH_KEY. ``span_exporter`` /
    ``span_processor`` replace the default OTLP/HTTP export (used by the tests).
    Calling ``install`` again replaces the previous installation.
    """
    with _install_lock:
        _uninstall_locked()
        provider = make_provider(span_exporter, span_processor)
        hooks = SqsHooks(Emitter(provider, resolve_hasher(receipt_hash_key), source="client"))
        inst = BotocoreInstrumentor()  # a singleton
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
    inst = BotocoreInstrumentor()
    if _state["instrumented_by_us"]:
        inst.uninstrument()
    else:
        inst.request_hook, inst.response_hook = _state["prev_hooks"]
    provider: TracerProvider = _state["provider"]
    provider.shutdown()
    _state.clear()


def uninstall() -> None:
    """Remove the qv2 hooks, flush and shut down the qv2 tracer provider."""
    with _install_lock:
        _uninstall_locked()
