"""Links from a timeline step to the same trace in the tracing tool a team already runs (QV-304).

``--trace-link`` (``TRAILBEAM_TRACE_LINK``) is a URL template. When it is set, every recorded step of a timeline
(``GET /api/messages/{id}``) gets ``span.trace_url``: the template with these placeholders filled in from the
step's span. The web UI then shows "Open trace" on that step.

- ``{trace_id}``: the trace ID, 32 lowercase hex digits, as OpenTelemetry writes it (Grafana Tempo, Jaeger,
  Honeycomb, Datadog's trace page).
- ``{trace_id_64}``: the trace ID's low 64 bits as a decimal number: Datadog's own form of a trace ID (what its
  logs call ``dd.trace_id``).
- ``{span_id}``: the span ID, 16 lowercase hex digits.
- ``{start_ms}``, ``{end_ms}``: when the span started and ended, Unix milliseconds.
- ``{from_ms}``, ``{to_ms}``: an hour before the span started and an hour after it ended, Unix milliseconds: a
  time range for tools whose links need one (Grafana Explore).
- ``{from_s}``, ``{to_s}``: the same range in Unix seconds (Honeycomb's ``trace_start_ts`` and ``trace_end_ts``).

Only these names, in braces, are placeholders. Any other ``{word}`` stops the server at start (a typo would
otherwise give a link that opens nothing); other braces, such as Grafana's JSON in a URL, are left as they are.
The template must be an http or https URL with a trace ID placeholder, so a link never runs script.
"""
from __future__ import annotations

import re
from typing import Optional

PLACEHOLDERS = ("trace_id", "trace_id_64", "span_id", "start_ms", "end_ms", "from_ms", "to_ms", "from_s", "to_s")
_NAME = re.compile(r"\{([a-z][a-z0-9_]*)\}")
_HEX = re.compile(r"[0-9a-f]+")
RANGE_MS = 3_600_000  # {from_ms} and {to_ms}: this much either side of the span


def check(template: str) -> str:
    """The template, stripped, or ValueError saying what is wrong with it."""
    t = template.strip()
    if not re.match(r"https?://[^/\s{}]", t, re.IGNORECASE):
        raise ValueError(f"expected an http:// or https:// URL, got {template!r}")
    if any(c.isspace() for c in t):
        raise ValueError("a URL has no spaces: encode them as %20")
    unknown = sorted({n for n in _NAME.findall(t) if n not in PLACEHOLDERS})
    if unknown:
        raise ValueError(f"unknown placeholder {', '.join('{' + n + '}' for n in unknown)}; the placeholders are "
                         + ", ".join("{" + p + "}" for p in PLACEHOLDERS))
    if "{trace_id}" not in t and "{trace_id_64}" not in t:
        raise ValueError("the template needs {trace_id} or {trace_id_64}, the trace to open")
    return t


def trace_url(template: str, span: dict, start_ns: Optional[int], end_ns: Optional[int]) -> Optional[str]:
    """The link for one span (``{"trace_id", "span_id"}``), or None when its IDs aren't hex (never from a real
    OTLP span, but the link must not carry anything else into the URL)."""
    trace_id, span_id = str(span.get("trace_id") or ""), str(span.get("span_id") or "")
    if not _HEX.fullmatch(trace_id) or not _HEX.fullmatch(span_id):
        return None
    end_ms = None if end_ns is None else end_ns // 1_000_000
    start_ms = end_ms if start_ns is None else start_ns // 1_000_000
    values = {"trace_id": trace_id, "span_id": span_id, "trace_id_64": str(int(trace_id[-16:], 16))}
    if end_ms is not None:
        values.update(start_ms=str(start_ms), end_ms=str(end_ms), from_ms=str(start_ms - RANGE_MS),
                      to_ms=str(end_ms + RANGE_MS), from_s=str((start_ms - RANGE_MS) // 1000),
                      to_s=str(-(-(end_ms + RANGE_MS) // 1000)))
    missing = False

    def fill(m: re.Match) -> str:
        nonlocal missing
        if m.group(1) not in values:
            missing = True
            return m.group(0)
        return values[m.group(1)]

    out = _NAME.sub(fill, template)
    return None if missing else out


def add_links(timeline: dict, template: Optional[str]) -> dict:
    """Set ``span.trace_url`` on every step of the timeline that has a span. No template: unchanged."""
    if not template:
        return timeline
    for e in timeline.get("events", ()):
        s = e.get("span")
        if not isinstance(s, dict):
            continue
        url = trace_url(template, s, _ns(e.get("start_unix_nano")), _ns(e.get("end_unix_nano")))
        if url:
            s["trace_url"] = url
    return timeline


def _ns(v) -> Optional[int]:
    try:
        return None if v is None else int(v)
    except (TypeError, ValueError):
        return None
