"""The default export path counts and logs every Trailbeam span it drops (QV-75).

Each test forces drops and checks that the counter, the metric and the log lines agree with the number
of spans the exporter never received.
"""
import logging
import re
import threading
import time

import pytest
from opentelemetry import metrics
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader
from opentelemetry.sdk.trace.export import SpanExporter, SpanExportResult

import trailbeam
from trailbeam import _export
from trailbeam._emitter import make_provider

LINE = re.compile(r"trailbeam: dropped (\d+) spans.*?\((.*?)\); (\d+) dropped since start")

_reader = InMemoryMetricReader()
metrics.set_meter_provider(MeterProvider(metric_readers=[_reader]))


class Gate(SpanExporter):
    """Records what it is given. Blocks every export until ``release`` is set; fails while ``fail`` is set."""

    def __init__(self, blocked=False):
        self.received = 0
        self.release = threading.Event()
        if not blocked:
            self.release.set()
        self.fail = False

    def export(self, spans):
        self.release.wait()
        if self.fail:
            return SpanExportResult.FAILURE
        self.received += len(spans)
        return SpanExportResult.SUCCESS

    def shutdown(self):
        self.release.set()


_baseline = {}


@pytest.fixture(autouse=True)
def fresh(monkeypatch):
    _export._stats._reset()
    _baseline.clear()
    _baseline.update(_metric_cumulative())  # the counter is cumulative: tests compare against this
    yield
    t = _export._stats.timer
    if t is not None:
        t.cancel()


def metric_total():
    """The drop counter's increase since the test started, by error.type."""
    now = _metric_cumulative()
    return {k: v - _baseline.get(k, 0) for k, v in now.items() if v - _baseline.get(k, 0)}


def _metric_cumulative():
    data = _reader.get_metrics_data()
    out = {}
    for rm in (data.resource_metrics if data else []):
        for sm in rm.scope_metrics:
            for m in sm.metrics:
                if m.name == _export.METRIC_NAME:
                    for p in m.data.data_points:
                        out[p.attributes["error.type"]] = p.value
    return out


def drop_lines(caplog):
    return [LINE.search(r.getMessage()) for r in caplog.records
            if r.name == "trailbeam" and r.levelno == logging.WARNING and "dropped" in r.getMessage()]


def emit(provider, n):
    tracer = provider.get_tracer("trailbeam")
    for _ in range(n):
        tracer.start_span("trailbeam send").end()


def test_default_queue_is_bigger_than_the_sdk_default(monkeypatch):
    monkeypatch.delenv(_export.QUEUE_SIZE_ENV, raising=False)
    provider = make_provider(span_exporter=Gate())
    (proc,) = provider._active_span_processor._span_processors
    assert isinstance(proc, _export.CountingSpanProcessor)
    assert proc.limit == _export.DEFAULT_MAX_QUEUE_SIZE == 16384
    provider.shutdown()


def test_queue_size_from_the_environment(monkeypatch, caplog):
    monkeypatch.setenv(_export.QUEUE_SIZE_ENV, "100")
    assert _export.configured_max_queue_size() == 100
    monkeypatch.setenv(_export.QUEUE_SIZE_ENV, "lots")
    with caplog.at_level(logging.WARNING, logger="trailbeam"):
        assert _export.configured_max_queue_size() == _export.DEFAULT_MAX_QUEUE_SIZE
    assert "not a positive integer" in caplog.text
    # A queue smaller than the batch size still works (the SDK would refuse it).
    monkeypatch.setenv(_export.QUEUE_SIZE_ENV, "10")
    exporter = Gate()
    provider = make_provider(span_exporter=exporter)
    emit(provider, 5)
    provider.force_flush()
    assert exporter.received == 5
    provider.shutdown()


def test_queue_full_drops_are_counted_and_logged_exactly(monkeypatch, caplog):
    monkeypatch.setenv(_export.QUEUE_SIZE_ENV, "200")
    exporter = Gate(blocked=True)  # the server is down: nothing leaves the queue
    provider = make_provider(span_exporter=exporter)
    with caplog.at_level(logging.WARNING, logger="trailbeam"):
        emit(provider, 1000)
        assert trailbeam.export_stats()["queued"] == 200
        exporter.release.set()  # the server is back
        assert provider.force_flush(10_000)
        emit(provider, 50)  # room again: these are not dropped
        provider.shutdown()

    real = 1050 - exporter.received
    s = trailbeam.export_stats()
    assert real == 800
    assert s["dropped"] == s["dropped_by_reason"]["queue_full"] == real
    assert s["exported"] == exporter.received == 250 and s["queued"] == 0
    lines = drop_lines(caplog)
    assert lines and all(lines)
    assert sum(int(m.group(1)) for m in lines) == real  # the log lines add up to the real count
    assert int(lines[-1].group(3)) == real
    assert metric_total() == {"queue_full": real}


def test_failed_exports_are_drops(caplog):
    exporter = Gate()
    exporter.fail = True
    provider = make_provider(span_exporter=exporter)
    with caplog.at_level(logging.WARNING, logger="trailbeam"):
        emit(provider, 1300)
        provider.force_flush(10_000)
        exporter.fail = False
        emit(provider, 7)
        provider.shutdown()
    s = trailbeam.export_stats()
    assert exporter.received == 7
    assert s["dropped_by_reason"] == {"queue_full": 0, "export_failed": 1300, "shutdown": 0}
    assert sum(int(m.group(1)) for m in drop_lines(caplog)) == 1300
    assert metric_total() == {"export_failed": 1300}


def test_an_exporter_that_raises_is_a_failed_export():
    class Boom(SpanExporter):
        def export(self, spans):
            raise RuntimeError("no network")

    provider = make_provider(span_exporter=Boom())
    emit(provider, 3)
    provider.force_flush(10_000)
    provider.shutdown()
    assert trailbeam.export_stats()["dropped_by_reason"]["export_failed"] == 3


def test_spans_ended_after_shutdown_are_drops(caplog):
    exporter = Gate()
    provider = make_provider(span_exporter=exporter)
    emit(provider, 20)
    with caplog.at_level(logging.WARNING, logger="trailbeam"):
        provider.shutdown()
        emit(provider, 4)  # an SQS call that finished after uninstall: lost
    s = trailbeam.export_stats()
    assert exporter.received == 20
    assert s["dropped_by_reason"] == {"queue_full": 0, "export_failed": 0, "shutdown": 4}
    assert metric_total() == {"shutdown": 4}


def test_one_line_per_interval_with_the_count_since_the_last(monkeypatch, caplog):
    monkeypatch.setattr(_export, "REPORT_INTERVAL_S", 0.3)
    monkeypatch.setenv(_export.QUEUE_SIZE_ENV, "10")
    exporter = Gate(blocked=True)
    provider = make_provider(span_exporter=exporter)
    with caplog.at_level(logging.WARNING, logger="trailbeam"):
        emit(provider, 10)
        emit(provider, 1)  # first drop: logged at once
        assert len(drop_lines(caplog)) == 1
        emit(provider, 499)  # within the interval: one more line, from the timer
        assert len(drop_lines(caplog)) == 1
        time.sleep(0.6)
        assert len(drop_lines(caplog)) == 2
        exporter.release.set()
        provider.shutdown()
    counts = [int(m.group(1)) for m in drop_lines(caplog)]
    assert counts == [1, 499]
    assert trailbeam.export_stats()["dropped"] == 500 == 510 - exporter.received
