"""Prometheus metrics for ``GET /metrics``, in the text exposition format
(https://prometheus.io/docs/instrumenting/exposition_formats/#text-based-format).

Hand-written so the server keeps its one dependency (opentelemetry-proto): counters, histograms and gauges
read at scrape time, which is all the server needs. Label values come from fixed sets (route templates,
status codes, reasons), never from span data, so the number of series is bounded.
"""
from __future__ import annotations

import bisect
import math
import threading
from typing import Callable, Iterable, Optional, Sequence

CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8"

# Seconds. Export latency and pages: 5 ms to 60 s (QV-68 measured exports from 0.1 s to 35 minutes).
LATENCY_BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60)
# One receipt-hash join is an index lookup inside the insert transaction: tens of microseconds when it
# uses spans_hash, milliseconds to seconds when it scans (QV-68).
JOIN_BUCKETS = (0.00005, 0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.1, 1)


def _escape(v: str) -> str:
    return str(v).replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')


def _labels(names: Sequence[str], values: Sequence[str], extra: str = "") -> str:
    parts = [f'{n}="{_escape(v)}"' for n, v in zip(names, values)]
    if extra:
        parts.append(extra)
    return "{" + ",".join(parts) + "}" if parts else ""


def _num(v: float) -> str:
    if v == math.inf:
        return "+Inf"
    if isinstance(v, int) or (isinstance(v, float) and v.is_integer() and abs(v) < 1e15):
        return str(int(v))
    return repr(float(v))


class _Family:
    kind = ""

    def __init__(self, registry: "Registry", name: str, help_text: str, labelnames: Sequence[str]):
        self.name, self.help, self.labelnames = name, help_text, tuple(labelnames)
        self._lock = registry.lock

    def _key(self, labels: dict) -> tuple:
        if set(labels) != set(self.labelnames):
            raise ValueError(f"{self.name}: labels {sorted(labels)} != {sorted(self.labelnames)}")
        return tuple(str(labels[n]) for n in self.labelnames)

    def header(self) -> list[str]:
        return [f"# HELP {self.name} {self.help}", f"# TYPE {self.name} {self.kind}"]


class Counter(_Family):
    kind = "counter"

    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self._values: dict[tuple, float] = {}

    def inc(self, n: float = 1, **labels) -> None:
        k = self._key(labels)
        with self._lock:
            self._values[k] = self._values.get(k, 0) + n

    def value(self, **labels) -> float:
        with self._lock:
            return self._values.get(self._key(labels), 0)

    def render(self) -> list[str]:
        with self._lock:
            items = sorted(self._values.items())
        return self.header() + [f"{self.name}{_labels(self.labelnames, k)} {_num(v)}" for k, v in items]


class Histogram(_Family):
    kind = "histogram"

    def __init__(self, registry, name, help_text, labelnames, buckets: Sequence[float]):
        super().__init__(registry, name, help_text, labelnames)
        self.buckets = tuple(sorted(buckets))
        self._values: dict[tuple, list] = {}  # key -> [per-bucket counts (+Inf last), sum]

    def observe(self, v: float, **labels) -> None:
        k = self._key(labels)
        i = bisect.bisect_left(self.buckets, v)
        with self._lock:
            s = self._values.get(k)
            if s is None:
                s = self._values[k] = [[0] * (len(self.buckets) + 1), 0.0]
            s[0][i] += 1
            s[1] += v

    def count(self, **labels) -> int:
        with self._lock:
            s = self._values.get(self._key(labels))
            return sum(s[0]) if s else 0

    def render(self) -> list[str]:
        with self._lock:
            items = sorted((k, (list(c), t)) for k, (c, t) in self._values.items())
        out = self.header()
        for k, (counts, total) in items:
            cum = 0
            for le, n in zip(self.buckets + (math.inf,), counts):
                cum += n
                bound = 'le="' + _num(le) + '"'
                out.append(f"{self.name}_bucket{_labels(self.labelnames, k, bound)} {cum}")
            out.append(f"{self.name}_sum{_labels(self.labelnames, k)} {_num(total)}")
            out.append(f"{self.name}_count{_labels(self.labelnames, k)} {cum}")
        return out


class Gauge(_Family):
    """Read when scraped: ``fn`` returns a number, or ``[(label values, number), ...]``. A gauge whose
    ``fn`` raises is left out of that scrape (with the error counted), so one failing read never fails
    the whole scrape."""
    kind = "gauge"

    def __init__(self, registry, name, help_text, labelnames, fn: Callable[[], object]):
        super().__init__(registry, name, help_text, labelnames)
        self.fn = fn
        self._errors = registry.scrape_errors

    def render(self) -> list[str]:
        try:
            v = self.fn()
        except Exception:
            self._errors.inc(gauge=self.name)
            return []
        if v is None:
            return []
        rows = v if isinstance(v, list) else [((), v)]
        return self.header() + [f"{self.name}{_labels(self.labelnames, k)} {_num(x)}" for k, x in rows]


class Registry:
    def __init__(self):
        self.lock = threading.Lock()
        self._families: list[_Family] = []
        self.scrape_errors = Counter(self, "trailbeam_metrics_gauge_errors_total",
                                     "Gauges left out of a scrape because reading them failed.", ("gauge",))

    def _add(self, f):
        if any(x.name == f.name for x in self._families):
            raise ValueError(f"metric {f.name} registered twice")
        self._families.append(f)
        return f

    def counter(self, name: str, help_text: str, labelnames: Iterable[str] = ()) -> Counter:
        return self._add(Counter(self, name, help_text, tuple(labelnames)))

    def histogram(self, name: str, help_text: str, labelnames: Iterable[str] = (),
                  buckets: Sequence[float] = LATENCY_BUCKETS) -> Histogram:
        return self._add(Histogram(self, name, help_text, tuple(labelnames), buckets))

    def gauge(self, name: str, help_text: str, fn: Callable[[], object], labelnames: Iterable[str] = ()) -> Gauge:
        return self._add(Gauge(self, name, help_text, tuple(labelnames), fn))

    def render(self) -> str:
        lines: list[str] = []
        for f in self._families:
            lines += f.render()
        lines += self.scrape_errors.render()
        return "\n".join(lines) + "\n"


def route_label(path: str, api_routes: Optional[Iterable[str]] = None) -> str:
    """The route template for a GET path: never the message ID or any other value from the URL."""
    if path.startswith("/api/"):
        parts = [p for p in path[len("/api/"):].split("/") if p]
        if parts == ["ready"]:
            return "/api/ready"
        if not parts or (api_routes is not None and parts[0] not in api_routes):
            return "/api/other"
        if parts[0] == "messages" and len(parts) > 1:
            return "/api/messages/{id}/body" if len(parts) == 3 and parts[2] == "body" else "/api/messages/{id}"
        if parts[0] == "tenants" and parts[1:] == ["search"]:
            return "/api/tenants/search"
        return f"/api/{parts[0]}" + ("/other" if len(parts) > 1 else "")
    if path.startswith("/ui/"):
        return "/ui/{file}"
    if path in ("/", "/message", "/duplicates", "/stuck", "/tenants", "/people", "/tokens", "/metrics", "/login", "/logout"):
        return path
    return "other"
