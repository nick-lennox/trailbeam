"""The export path of Trailbeam spans: a bounded batch queue that counts every span it loses.

The SDK's ``BatchSpanProcessor`` drops spans when its queue is full and says so in a "Queue full"
warning that its logger's ``DuplicateFilter`` suppresses after the first one (QV-68: 5,056 spans
dropped, 2 log lines). ``CountingSpanProcessor`` puts a gate in front of it and wraps its exporter, so
that every span Trailbeam accepts ends up in exactly one of: exported, still queued, or dropped for one
of three reasons:

- ``queue_full``: the queue already held ``max_queue_size`` spans that were not yet exported;
- ``export_failed``: the exporter returned a failure or raised (the SDK does not retry a failed batch;
  the OTLP exporter has already retried what it can);
- ``shutdown``: still queued when shutdown gave up, or ended after shutdown.

The gate counts a span as queued from ``on_end`` until its export returns, which is never less than
what the SDK's queue holds, and the SDK's queue has the same capacity, so the SDK itself never drops on
a full queue. Only public SDK APIs are used, so this works with every SDK version the package allows.

Drops are counted process-wide (``stats()``), added to the ``trailbeam.sdk.spans.dropped`` counter of
the global OpenTelemetry meter provider, and logged on the ``trailbeam`` logger at WARNING: at the first
drop, then at most once per ``REPORT_INTERVAL_S`` with the count since the last line, and at shutdown.
"""
from __future__ import annotations

import logging
import os
import threading
import time
import weakref
from typing import Optional, Sequence

from opentelemetry.sdk.trace import ReadableSpan, Span, SpanProcessor
from opentelemetry.sdk.trace.export import BatchSpanProcessor, SpanExporter, SpanExportResult

log = logging.getLogger("trailbeam")

QUEUE_SIZE_ENV = "TRAILBEAM_MAX_QUEUE_SIZE"
# The SDK default is 2,048: two seconds at the 1,000 spans/s small-tier target. 16,384 holds about 16 s
# of that rate, at about 3 KB per span without bodies (QV-75 measurement), so about 50 MB when full.
DEFAULT_MAX_QUEUE_SIZE = 16384
DEFAULT_MAX_EXPORT_BATCH_SIZE = 512  # the SDK default, and OTEL_BSP_MAX_EXPORT_BATCH_SIZE still applies
REPORT_INTERVAL_S = 60.0
REASONS = ("queue_full", "export_failed", "shutdown")
METRIC_NAME = "trailbeam.sdk.spans.dropped"
DOCS = '"High volume" in the Trailbeam collector/README.md'


class _Stats:
    """Process-wide counts over every Trailbeam processor, and the rate-limited drop log line."""

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self._reset()

    def _reset(self) -> None:
        self.exported = 0
        self.dropped = dict.fromkeys(REASONS, 0)
        self.unreported = dict.fromkeys(REASONS, 0)
        self.last_report: Optional[float] = None
        self.timer: Optional[threading.Timer] = None
        self.counter = None

    def snapshot(self, queued: int) -> dict:
        with self.lock:
            dropped = dict(self.dropped)
            return {"exported": self.exported, "queued": queued, "dropped": sum(dropped.values()),
                    "dropped_by_reason": dropped}

    def add_exported(self, n: int) -> None:
        with self.lock:
            self.exported += n

    def add_dropped(self, reason: str, n: int, limit: int) -> None:
        if n <= 0:
            return
        report = False
        with self.lock:
            self.dropped[reason] += n
            self.unreported[reason] += n
            now = time.monotonic()
            if self.last_report is None or now - self.last_report >= REPORT_INTERVAL_S:
                report = True
            elif self.timer is None:
                self.timer = threading.Timer(REPORT_INTERVAL_S - (now - self.last_report), self.report, (limit,))
                self.timer.daemon = True
                self.timer.start()
        _metric(self, reason, n)
        if report:
            self.report(limit)

    def report(self, limit: int, final: bool = False) -> None:
        """Log the drops not logged yet, if any, as one WARNING line."""
        with self.lock:
            if self.timer is not None and self.timer is not threading.current_thread():
                self.timer.cancel()
            self.timer = None
            unreported = self.unreported
            n = sum(unreported.values())
            if n == 0:
                return
            first = self.last_report is None
            since = "" if first else f" in the last {time.monotonic() - self.last_report:.0f} s"
            self.unreported = dict.fromkeys(REASONS, 0)
            self.last_report = time.monotonic()
            total = sum(self.dropped.values())
        parts = ", ".join(f"{unreported[r]} {r.replace('_', ' ')}" for r in REASONS if unreported[r])
        log.warning(
            "trailbeam: dropped %d spans%s (%s); %d dropped since start. These messages will have gaps in "
            "their Trailbeam timelines. The export queue holds %d spans (%s). For high volume, export through "
            "an OpenTelemetry Collector with a persistent queue: %s%s",
            n, since, parts, total, limit, QUEUE_SIZE_ENV, DOCS,
            "" if final else f". The next count follows in {REPORT_INTERVAL_S:.0f} s if spans are still dropped.")


_stats = _Stats()
if hasattr(os, "register_at_fork"):  # a forked child starts its own counts (and has no timer thread)
    def _stats_after_fork() -> None:
        _stats.lock = threading.Lock()
        _stats._reset()

    os.register_at_fork(after_in_child=_stats_after_fork)
_processors: "weakref.WeakSet[CountingSpanProcessor]" = weakref.WeakSet()


def _metric(stats: _Stats, reason: str, n: int) -> None:
    try:
        if stats.counter is None:
            from opentelemetry import metrics

            from . import __version__

            # The API's proxy meter follows a meter provider that is set later.
            stats.counter = metrics.get_meter("trailbeam", __version__).create_counter(
                METRIC_NAME, unit="{span}", description="Trailbeam spans this process dropped before export")
        stats.counter.add(n, {"error.type": reason})
    except Exception:  # a metrics problem must not break the application or lose the log line
        log.debug("trailbeam: drop metric failed", exc_info=True)


def stats() -> dict:
    """Trailbeam spans of this process: ``exported``, ``queued`` (accepted, export not finished yet),
    ``dropped`` and ``dropped_by_reason`` (``queue_full``, ``export_failed``, ``shutdown``)."""
    return _stats.snapshot(sum(p.queued for p in list(_processors)))


def configured_max_queue_size() -> int:
    """$TRAILBEAM_MAX_QUEUE_SIZE if it is a positive integer, else the default."""
    raw = os.environ.get(QUEUE_SIZE_ENV, "").strip()
    if raw:
        try:
            value = int(raw)
            if value > 0:
                return value
        except ValueError:
            pass
        log.warning("trailbeam: ignoring %s=%r, which is not a positive integer", QUEUE_SIZE_ENV, raw)
    return DEFAULT_MAX_QUEUE_SIZE


def _env_batch_size() -> int:
    try:
        return int(os.environ.get("OTEL_BSP_MAX_EXPORT_BATCH_SIZE", DEFAULT_MAX_EXPORT_BATCH_SIZE))
    except ValueError:
        return DEFAULT_MAX_EXPORT_BATCH_SIZE


class _CountingExporter(SpanExporter):
    def __init__(self, inner: SpanExporter, owner: "CountingSpanProcessor"):
        self.inner = inner
        self.owner = owner

    def export(self, spans: Sequence[ReadableSpan]) -> SpanExportResult:
        result = SpanExportResult.FAILURE
        try:
            result = self.inner.export(spans)
            return result
        finally:  # an exception counts as a failure, and the SDK logs it
            self.owner._exported(len(spans), result == SpanExportResult.SUCCESS)

    def shutdown(self) -> None:
        self.inner.shutdown()

    def force_flush(self, timeout_millis: int = 30000) -> bool:
        return self.inner.force_flush(timeout_millis)


class CountingSpanProcessor(SpanProcessor):
    """A ``BatchSpanProcessor`` with a bounded queue whose every lost span is counted (module docstring)."""

    def __init__(self, exporter: SpanExporter, max_queue_size: Optional[int] = None, **batch_kwargs):
        self.limit = max_queue_size or configured_max_queue_size()
        batch_kwargs.setdefault("max_export_batch_size", min(_env_batch_size(), self.limit))
        self._lock = threading.Lock()
        self.queued = 0
        self._shutdown = False
        self.batch = BatchSpanProcessor(_CountingExporter(exporter, self), max_queue_size=self.limit,
                                        **batch_kwargs)
        _processors.add(self)
        if hasattr(os, "register_at_fork"):
            ref = weakref.ref(self)

            def after_in_child() -> None:
                p = ref()
                if p is not None:
                    p._lock = threading.Lock()
                    p.queued = 0  # the SDK empties its queue in the child, too

            os.register_at_fork(after_in_child=after_in_child)

    def on_start(self, span: Span, parent_context=None) -> None:
        self.batch.on_start(span, parent_context)

    def on_end(self, span: ReadableSpan) -> None:
        if not span.context.trace_flags.sampled:
            return  # the SDK's processor ignores these too (the Trailbeam sampler is always on)
        with self._lock:
            reason = "shutdown" if self._shutdown else "queue_full" if self.queued >= self.limit else None
            if reason is None:
                self.queued += 1
        if reason is not None:
            _stats.add_dropped(reason, 1, self.limit)
            return
        self.batch.on_end(span)

    def _exported(self, n: int, ok: bool) -> None:
        with self._lock:
            self.queued -= n
        if ok:
            _stats.add_exported(n)
        else:
            _stats.add_dropped("export_failed", n, self.limit)

    def force_flush(self, timeout_millis: int = 30000) -> bool:
        return self.batch.force_flush(timeout_millis)

    def shutdown(self) -> None:
        with self._lock:
            if self._shutdown:
                return
            self._shutdown = True
        try:
            self.batch.shutdown()
        finally:
            with self._lock:
                left, self.queued = self.queued, 0
            _stats.add_dropped("shutdown", left, self.limit)
            _stats.report(self.limit, final=True)
