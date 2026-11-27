"""Dashboard widget measures, worked out (QV-632, the first part of QV-591: dashboards in self-hosted Trailbeam).

The hosted app's engine in Python: lib/dashboards/measures.ts, with the parts of lib/query.ts (the Event explorer's
query language), lib/app/queue-live.ts (a queue's figures) and lib/timerange.ts (presets) it uses, with the same
behaviour. Pure functions over plain inputs: no store, no clock, no AWS. The storage, the endpoints and the screens
that call them are later parts of QV-591.

The two engines must not drift: contract/fixtures/measures-parity.json holds cases (a widget or measure, its inputs,
the expected result) that python/tests/server/test_measures.py runs here and the hosted app's
test/measures-parity.test.ts runs against measures.ts. The file reaches the app inside @trailbeam/ui
(@trailbeam/ui/fixtures/measures-parity.json), as alerts-parity.json does (QV-627).

The inputs are dicts with the hosted app's JSON field names, so the fixture and, later, the API carry them unchanged:

- ``data``: what the widgets read, the app's EventData: ``events`` (the Event explorer's rows: ``ms``, ``event``,
  ``status``, ``queue``, ``messageId``, ``service``, ``producer``, ``consumer``, ``tenant``, ``receiveCount``,
  ``durationMs``, ``waitMs``, ``sizeBytes``, ``detail``, ``attrs``, ``body``), ``now`` (ms), ``queues`` (the queue names)
  and ``queueLive`` (queue name -> the queue's figures from AWS: its settings, its depth now and CloudWatch windows
  of history, and what the spans say about the messages in it).
- a time range: ``{"from": ms, "to": ms}``, the end excluded.
- a widget: ``{id, type, title, filter?, queries?: [{measure, group_by?}], columns?, rule?, follow?, time?, layout}``.

This is the own-data half of measures.ts only. The hosted app also draws sample figures for the demo workspace
(``now`` on a measure, ``shape``, the sample queues); self-hosted Trailbeam has no sample data, so they are not here
and ``data["live"]`` is taken as true. A field absent from an event and a field set to null are the same here; the
fixture leaves absent fields out.

Two kinds of measure:

- event measures (counts, rates, processing time, time in queue, fields): counted from the events in the range that
  match the widget's filter, in n equal buckets, one line per value of the group-by field or one for all of it.
- queue measures (depth, age, headroom): per queue, from ``queueLive``. CloudWatch has no tenant or producer, so only
  the filter's queue terms narrow them (queue_only_terms lists the rest, and the widget says so). A queue without
  figures has none, and a widget with none at all says why (no_depth).

Numbers are formatted as JavaScript formats them (Math.round, toFixed, toLocaleString), so fmt() gives the same text.
"""
from __future__ import annotations

import json
import math
import re
from dataclasses import dataclass
from decimal import ROUND_HALF_UP, Decimal
from typing import Callable, Optional

from .alerts import limit_of, tone_of
from .bodies import body_path, body_values, term_hit

INF = math.inf
MIN = 60_000
HOUR = 60 * MIN
DAY = 24 * HOUR


# ── JavaScript's arithmetic and formatting ──────────────────────────────────────────────────────────────────────

def _is_num(v) -> bool:
    return isinstance(v, (int, float)) and not isinstance(v, bool)


def _total(xs) -> float:
    """xs.reduce((a, b) => a + b, 0): left to right, with no compensation (Python 3.12's sum() compensates)."""
    t = 0
    for x in xs:
        t = t + x
    return t


def _div(a: float, b: float) -> float:
    """a / b as JavaScript divides: by zero gives an infinity, or NaN for 0 / 0."""
    if b == 0:
        if a == 0 or a != a:
            return math.nan
        return math.copysign(INF, a) * math.copysign(1, b)
    return a / b


def js_round(x: float) -> int:
    """Math.round: halves go up (2.5 -> 3, -2.5 -> -2)."""
    r = math.floor(x)
    return r + 1 if x - r >= 0.5 else r


def to_fixed(x: float, digits: int) -> str:
    """Number.prototype.toFixed: the exact value rounded half up, away from zero."""
    return f"{Decimal(x).quantize(Decimal(1).scaleb(-digits), rounding=ROUND_HALF_UP):f}"


def js_num(v: float) -> str:
    """String(number) for a finite number."""
    if isinstance(v, int):
        return str(v)
    if v.is_integer() and abs(v) < 1e21:
        return str(int(v))
    return re.sub(r"e([+-])0*(\d)", r"e\1\2", repr(v))


def js_str(v) -> str:
    """String(v) for what JSON can hold."""
    if v is None:
        return "null"
    if isinstance(v, bool):
        return "true" if v else "false"
    if _is_num(v):
        if math.isnan(v):
            return "NaN"
        if math.isinf(v):
            return "Infinity" if v > 0 else "-Infinity"
        return js_num(v)
    if isinstance(v, str):
        return v
    if isinstance(v, list):
        return ",".join("" if x is None else js_str(x) for x in v)
    return "[object Object]"


def js_json(v) -> str:
    """JSON.stringify(v) for what JSON can hold: numbers as JavaScript writes them."""
    if v is None or (_is_num(v) and not math.isfinite(v)):
        return "null"
    if isinstance(v, bool):
        return "true" if v else "false"
    if _is_num(v):
        return js_num(v)
    if isinstance(v, str):
        return json.dumps(v, ensure_ascii=False)
    if isinstance(v, (list, tuple)):
        return "[" + ",".join(js_json(x) for x in v) + "]"
    if isinstance(v, dict):
        return "{" + ",".join(f"{json.dumps(str(k), ensure_ascii=False)}:{js_json(x)}" for k, x in v.items()) + "}"
    return "null"


def _truthy(v) -> bool:
    """JavaScript's truthiness: null, false, 0, NaN and "" are false; an empty list or object is true."""
    if v is None or v is False:
        return False
    if _is_num(v):
        return v == v and v != 0
    if isinstance(v, str):
        return v != ""
    return True


def fmt_num(n: int) -> str:
    """n.toLocaleString("en-US") for a whole number."""
    return f"{n:,}"


def fmt_bytes(n: float) -> str:
    """topology.ts fmtBytes."""
    if n < 1024:
        return f"{js_round(n)} B"
    if n < 1024 ** 2:
        return f"{to_fixed(n / 1024, 1)} KB"
    if n < 1024 ** 3:
        return f"{to_fixed(n / 1024 ** 2, 1)} MB"
    return f"{to_fixed(n / 1024 ** 3, 2)} GB"


# ── time ranges (lib/timerange.ts) ──────────────────────────────────────────────────────────────────────────────

PRESETS = [
    {"id": "5m", "label": "Past 5 minutes", "ms": 5 * MIN},
    {"id": "15m", "label": "Past 15 minutes", "ms": 15 * MIN},
    {"id": "1h", "label": "Past 1 hour", "ms": HOUR},
    {"id": "4h", "label": "Past 4 hours", "ms": 4 * HOUR},
    {"id": "1d", "label": "Past 1 day", "ms": DAY},
    {"id": "2d", "label": "Past 2 days", "ms": 2 * DAY},
    {"id": "7d", "label": "Past 7 days", "ms": 7 * DAY},
    {"id": "30d", "label": "Past 30 days", "ms": 30 * DAY},
]


def preset(id_: Optional[str], now: int) -> dict:
    """The range a preset ID names, ending now; an unknown ID is the past hour."""
    p = next((x for x in PRESETS if x["id"] == id_), PRESETS[2])
    return {"from": now - p["ms"], "to": now, "label": p["label"], "preset": p["id"]}


# ── the Event explorer's query language (lib/query.ts) ──────────────────────────────────────────────────────────

_NUM = re.compile(r"(>=|<=|>|<)?(\d+(?:\.\d+)?)(ms|s|m|b|kb|mb)?", re.I)
_RANGE = re.compile(r"(\d+(?:\.\d+)?)(ms|s|m|b|kb|mb)?\.\.(\d+(?:\.\d+)?)(ms|s|m|b|kb|mb)?", re.I)
_UNIT = {"": 1, "ms": 1, "s": 1000, "m": 60_000, "b": 1, "kb": 1024, "mb": 1024 ** 2}
_TOKEN = re.compile(r'(?:[^\s"]+|"[^"]*"?)+')
_BODY = re.compile(r"^body(\.|\[|$)", re.I)


def _attr(name: str):
    return lambda e: (e.get("attrs") or {}).get(name)


def _key(k: str):
    return lambda e: e.get(k)


def _field(name, label, group, kind, get, facet=False, buckets=None, sample_only=False) -> dict:
    return {"name": name, "label": label, "group": group, "kind": kind, "facet": facet, "buckets": buckets,
            "sampleOnly": sample_only, "get": get}


def _a(name: str, group: str, kind: str, facet: bool = True, sample_only: bool = False) -> dict:
    return _field(name, name[name.find(".") + 1:], group, kind, _attr(name), facet=facet, sample_only=sample_only)


def _s(name: str, group: str, kind: str) -> dict:
    return _a(name, group, kind, sample_only=True)


#: every field that can be filtered on or shown as a column; on a workspace's own events attributes are the spans'
FIELD_DEFS = [
    _field("status", "Status", "Event", "text", _key("status"), facet=True),
    _field("event", "Event", "Event", "text", _key("event"), facet=True),
    _field("queue", "Queue", "Event", "text", _key("queue"), facet=True),
    _field("service", "Service", "Event", "text", _key("service"), facet=True),
    _field("producer", "Producer", "Event", "text", _key("producer"), facet=True),
    _field("consumer", "Consumer", "Event", "text", _key("consumer"), facet=True),
    _field("tenant", "Tenant", "Event", "text", _key("tenant"), facet=True),
    _field("source", "Source", "Event", "text", _key("source"), facet=True),
    _field("receive_count", "Receive count", "Event", "number", _key("receiveCount"), facet=True),
    _field("message_id", "Message ID", "Event", "text", _key("messageId")),
    _field("body_size", "Body size", "Event", "bytes", _key("sizeBytes"), facet=True, buckets=[1024, 4096, 16384, 65536]),
    _field("processing_time", "Processing time", "Event", "duration", _key("durationMs"), facet=True,
           buckets=[100, 500, 1000, 5000]),
    _field("time_in_queue", "Time in queue", "Event", "duration", _key("waitMs"), facet=True,
           buckets=[100, 1000, 10_000, 60_000]),
    _a("message.ApproximateReceiveCount", "Message attributes", "number"),
    _a("message.MessageGroupId", "Message attributes", "text"),
    _a("message.tenant", "Message attributes", "text"),
    _a("message.BodySize", "Message attributes", "bytes", False),
    _a("queue.account", "Queue attributes", "text"),
    _a("queue.region", "Queue attributes", "text"),
    _s("queue.FifoQueue", "Queue attributes", "text"),
    _s("queue.VisibilityTimeout", "Queue attributes", "duration"),
    _s("queue.MessageRetentionPeriod", "Queue attributes", "number"),
    _s("queue.DelaySeconds", "Queue attributes", "duration"),
    _s("queue.ReceiveMessageWaitTimeSeconds", "Queue attributes", "duration"),
    _s("queue.MaximumMessageSize", "Queue attributes", "bytes"),
    _s("queue.ContentBasedDeduplication", "Queue attributes", "text"),
    _s("queue.RedrivePolicy.maxReceiveCount", "Queue attributes", "number"),
    _s("queue.RedrivePolicy.deadLetterTargetArn", "Queue attributes", "text"),
    _s("mapping.BatchSize", "Event source mapping", "number"),
    _s("mapping.MaximumBatchingWindowInSeconds", "Event source mapping", "duration"),
    _s("mapping.ReportBatchItemFailures", "Event source mapping", "text"),
]
FIELDS = [f["name"] for f in FIELD_DEFS]
_DEF = {f["name"].lower(): f for f in FIELD_DEFS}
#: earlier names, still accepted in a query or a saved column list
_ALIAS = {"size": "body_size", "duration": "processing_time", "handler_duration": "processing_time"}
_BODY_DEFS: dict = {}


def is_body_field(name: str) -> bool:
    return bool(_BODY.match(name))


def _body_def(name: str) -> dict:
    field = "body" + name[4:]
    if field not in _BODY_DEFS:
        path = body_path(field)

        def get(e):
            if "body" not in e:
                return None
            body = e["body"]
            if not path:
                return js_json(body)
            vs = body_values(body, path)
            v = vs[0] if vs else None
            return ("true" if v else "false") if isinstance(v, bool) else v

        _BODY_DEFS[field] = _field(field, field if path else "Body", "Event", "text", get)
    return _BODY_DEFS[field]


def field_def(name) -> Optional[dict]:
    """query.ts def(): a field by name (any case, earlier names too), or a body field made for its path."""
    if not isinstance(name, str):
        return None
    if is_body_field(name):
        return _body_def(name)
    low = name.lower()
    return _DEF.get(_ALIAS.get(low, low))


def _dist(a: str, b: str) -> int:
    d = [[i] + [0] * len(b) for i in range(len(a) + 1)]
    for j in range(1, len(b) + 1):
        d[0][j] = j
    for i in range(1, len(a) + 1):
        for j in range(1, len(b) + 1):
            d[i][j] = min(d[i - 1][j] + 1, d[i][j - 1] + 1, d[i - 1][j - 1] + (0 if a[i - 1] == b[j - 1] else 1))
    return d[len(a)][len(b)]


def parse(q: str, live: bool = False) -> list[dict]:
    """A query as terms {raw, start, end, neg, field?, value, error?}. Terms are field:value, ANDed; terms on one field
    are alternatives; a leading minus excludes. live: the query runs on own events, which have no sampleOnly field."""
    terms = []
    for m in _TOKEN.finditer(q if isinstance(q, str) else js_str(q)):
        raw = m.group(0)
        neg = raw.startswith("-") and len(raw) > 1
        body = raw[1:] if neg else raw
        i = body.find(":")
        t = {"raw": raw, "start": m.start(), "end": m.end(), "neg": neg, "value": body.replace('"', "")}
        if i > 0 and not body.startswith('"'):
            name = body[:i]
            t["value"] = body[i + 1:].replace('"', "")
            d = field_def(name)
            if d:
                t["field"] = d["name"]
                if d["name"] == "event" and t["value"].lower() == "handler_error":
                    t["value"] = "processing_error"  # the earlier name for this event kind, still accepted
            else:
                low = name.lower()
                near = sorted(FIELDS, key=lambda f: _dist(low, f.lower()))[0]
                hint = f" Did you mean {near}?" if _dist(low, near.lower()) <= 3 else ""
                t["error"] = f"There is no field called {name}.{hint}"
            if d and d["kind"] != "text" and t["value"] and not _NUM.fullmatch(t["value"]) and not _RANGE.fullmatch(t["value"]):
                t["error"] = f"{d['name']} takes a number, for example {d['name']}:>=2."
            if d and d["sampleOnly"] and live:
                t["error"] = f"{d['name']} isn't recorded on your own events, so it would match nothing."
        terms.append(t)
    return terms


def _rel(x) -> Optional[float]:
    """x as JavaScript's < and > read it: a number, true and false as 1 and 0, a numeric string as a number; None when
    it is none of them (NaN, which compares false)."""
    if isinstance(x, bool):
        return int(x)
    if _is_num(x):
        return x
    if isinstance(x, str):
        try:
            return float(x.strip()) if x.strip() else 0.0
        except ValueError:
            return None
    return None


def num_hit(x, value: str) -> Optional[bool]:
    """A number against a query value such as >=3, 30s or 1kb..4kb; None if the value is not numeric."""
    r = _RANGE.fullmatch(value)
    if r:
        v = _rel(x)
        return v is not None and float(r[1]) * _UNIT[(r[2] or "").lower()] <= v < float(r[3]) * _UNIT[(r[4] or "").lower()]
    m = _NUM.fullmatch(value)
    if not m:
        return None
    n = float(m[2]) * _UNIT[(m[3] or "").lower()]
    if not m[1]:
        return _is_num(x) and x == n
    v = _rel(x)
    if v is None:
        return False
    return {">": v > n, ">=": v >= n, "<": v < n, "<=": v <= n}[m[1]]


def text_hit(v, value: str) -> bool:
    """Text, ignoring case; * is a wildcard."""
    s, want = ("" if v is None else js_str(v)).lower(), value.lower()
    if "*" in want:
        return re.fullmatch(".*".join(re.escape(p) for p in want.split("*")), s) is not None
    return s == want


def _hit(e: dict, t: dict) -> bool:
    if not t.get("field"):
        s = t["value"].lower()
        return not s or s in (e.get("messageId") or "") or s in (e.get("detail") or "").lower()
    if is_body_field(t["field"]):
        if "body" not in e:
            return False  # no stored body: payload capture is off for this queue
        return term_hit(e["body"], {"value": t["value"], "path": body_path(t["field"])})
    d = field_def(t["field"])
    v = d["get"](e)
    if not t["value"]:
        return True  # still being typed
    if d["kind"] != "text":
        return False if v is None else bool(num_hit(v, t["value"]))
    return text_hit(v, t["value"])


def matches(e: dict, terms: list[dict], skip: Optional[str] = None) -> bool:
    """skip: ignore positive terms on this field (a facet counts its own values so)."""
    by_field: dict[str, list] = {}
    for t in terms:
        if t.get("error"):
            continue
        if t["neg"]:
            if _hit(e, t):
                return False
            continue
        if not t.get("field"):
            if not _hit(e, t):
                return False
            continue
        if t["field"] == skip:
            continue
        by_field.setdefault(t["field"], []).append(t)
    return all(any(_hit(e, t) for t in ts) for ts in by_field.values())


# ── a queue's figures (lib/app/queue-live.ts) ───────────────────────────────────────────────────────────────────

DRAIN_POINTS = 5  # how much of the queue's recent history the drain rate is averaged over


def drain_s(visible, sent: list, deleted: list, period_s: float) -> Optional[float]:
    """Seconds until the backlog is gone at the recent net rate (deleted minus sent); inf when it is not shrinking."""
    if visible is None:
        return None
    if visible <= 0:
        return 0
    def tail(p):
        return _total(v for _, v in p[-DRAIN_POINTS:])
    n = max(1, min(DRAIN_POINTS, max(len(sent), len(deleted))))
    net = _div(tail(deleted) - tail(sent), n * period_s)
    return _div(visible, net) if net > 0 else INF


def retention_pct(oldest_s, retention_s) -> Optional[float]:
    return None if oldest_s is None or not _truthy(retention_s) else (100 * oldest_s) / retention_s


def visibility_pct(p95_ms, vt_s) -> Optional[float]:
    return None if p95_ms is None or not _truthy(vt_s) else (100 * p95_ms) / (vt_s * 1000)


def dlq_change(w: dict, since_ms: float) -> Optional[float]:
    """How much the DLQ's depth changed since since_ms: its latest point minus the last at or before then."""
    d = w["dlq"]
    if len(d) < 2:
        return None
    start = next((p for p in reversed(d) if p[0] <= since_ms), d[0])
    return d[-1][1] - start[1]


def window_for(q: dict, from_ms: float) -> Optional[dict]:
    """The finest window that reaches back to from_ms, or the one that reaches furthest."""
    ws = q.get("windows") or []
    if not ws:
        return None
    return next((w for w in ws if w["fromMs"] <= from_ms), None) or sorted(ws, key=lambda w: w["fromMs"])[0]


def bucketize(points: list, from_: float, to: float, n: int, kind: str) -> list:
    """Points in n equal buckets over [from, to): a gauge the highest in a bucket, a count the sum. An empty bucket keeps
    the gauge's last value, and a count is 0."""
    span = (to - from_) / n
    out: list = [None] * n
    before = 0
    for t, v in points:
        if t < from_:
            before = v
            continue
        if t >= to:
            continue
        i = min(n - 1, math.floor((t - from_) / span))
        out[i] = (out[i] or 0) + v if kind == "count" else max(-INF if out[i] is None else out[i], v)
    if kind == "count":
        return [0 if v is None else v for v in out]
    res, prev = [], before
    for v in out:
        if v is not None:
            prev = v
        res.append(prev)
    return res


def health(q: Optional[dict], p95_ms: Optional[float] = None) -> Optional[dict]:
    """The queue's health from its own figures, {tone, why}; None when AWS was not read."""
    if not q or q.get("status") != "ok":
        return None
    ws = q.get("windows") or []
    w = ws[0] if ws else None
    ret = retention_pct(q.get("oldestS"), q.get("retentionS"))
    if ret is not None and ret >= 90:
        return {"tone": "critical", "why": "Near retention"}
    if w:
        read_at = q.get("readAt")
        ch = dlq_change(w, (w["toMs"] if read_at is None else read_at) - 3_600_000)
        if (0 if ch is None else ch) > 0:
            return {"tone": "critical", "why": "DLQ growing"}
    vis = visibility_pct(p95_ms, q.get("visibilityTimeoutS"))
    drain = drain_s(q.get("visible"), w["sent"], w["deleted"], w["periodS"]) if w else None
    if drain == INF and (q.get("visible") or 0) > 0:
        return {"tone": "warning", "why": "Backlog growing"}
    if (q.get("oldestS") or 0) > 300:
        return {"tone": "warning", "why": "Old messages"}
    if vis is not None and vis >= 80:
        return {"tone": "warning", "why": "Near timeout"}
    if (q.get("dlqDepth") or 0) > 0:
        return {"tone": "warning", "why": "In the DLQ"}
    if ((q.get("nearDlq") or {}).get("count") or 0) > 0:
        return {"tone": "warning", "why": "Near the DLQ"}
    if (q.get("stuck") or 0) > 0:
        return {"tone": "warning", "why": "Stuck messages"}
    return {"tone": "good", "why": "Healthy"}


def health_of(q: Optional[dict], p95_ms: Optional[float] = None) -> Optional[str]:
    h = health(q, p95_ms)
    return h["tone"] if h else None


def why_none(q: Optional[dict]) -> str:
    """Why a queue has no AWS figures, in a sentence."""
    if not q:
        return "No data. No connected AWS account names this queue. Connect its account in Connections to read its depth and settings."
    return q.get("detail") or "No data from CloudWatch for this queue yet."


# ── the measures ────────────────────────────────────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class Measure:
    id: str
    label: str
    ask: str
    unit: str  # count | rate | s | ms | pct | bytes
    agg: str  # how several queues become one figure: sum | max
    col: Optional[str] = None  # a shorter name for a table column
    #: a queue measure: the figure for one queue now from its figures, None when it has none. p95: the queue's p95
    #: processing time over the range, from its events
    live: Optional[Callable] = None
    #: its history in n buckets over [from, to), or None (worked out from the spans as they are now)
    history: Optional[Callable] = None
    #: its history comes from the spans, for ranges within the past 24 hours only
    spans: bool = False
    #: an event measure: which events (when), what to take from each (take) and how to fold it (max, avg or p95)
    ev: Optional[dict] = None

    @property
    def queue(self) -> bool:
        return self.live is not None


ASKS = ["Is it backing up?", "Are consumers keeping up?", "Is it failing?", "Is it about to fail?", "From event fields"]


def _is(*kinds):
    return lambda e: e.get("event") in kinds


def _gauge(q: dict, key: str, f, t, n) -> Optional[list]:
    """A CloudWatch gauge in n buckets, from the window that reaches back to f."""
    w = window_for(q, f)
    return bucketize(w[key], f, t, n, "gauge") if w else None


def _drain_now(q: dict) -> Optional[float]:
    ws = q.get("windows") or []
    if not ws:
        return None
    w = ws[0]
    return drain_s(q.get("visible"), w["sent"], w["deleted"], w["periodS"])


def _near_now(q: dict) -> Optional[float]:
    nd = q.get("nearDlq")
    if not (_truthy(q.get("maxReceiveCount")) and nd):
        return None
    return (100 * nd["count"]) / nd["inFlight"] if _truthy(nd["inFlight"]) else 0


def _drain_history(q: dict, f, t, n) -> Optional[list]:
    """Time to drain at the end of each bucket, from CloudWatch's points before then; the last is the figure now."""
    w = window_for(q, f)
    if not w:
        return None
    span = (t - f) / n
    vis, sent, dele = (sorted(w[k], key=lambda p: p[0]) for k in ("visible", "sent", "deleted"))
    iv = i_s = i_d = 0
    out = []
    for i in range(n):
        end = f + (i + 1) * span
        while iv < len(vis) and vis[iv][0] < end:
            iv += 1
        while i_s < len(sent) and sent[i_s][0] < end:
            i_s += 1
        while i_d < len(dele) and dele[i_d][0] < end:
            i_d += 1
        d = drain_s(vis[iv - 1][1] if iv else 0, sent[max(0, i_s - 5):i_s], dele[max(0, i_d - 5):i_d], w["periodS"])
        out.append(0 if d is None else d)
    now = _drain_now(q)
    if now is not None:
        out[n - 1] = now
    return out


def _span_gauge(q: dict, key: str, f, t, n, now) -> Optional[list]:
    """A span-derived gauge in n buckets, ending at its figure now; on the day window only."""
    w = window_for(q, f)
    pts = w.get(key) if w and f >= w["fromMs"] else None
    if pts is None or now is None:
        return None
    out = bucketize(pts, f, t, n, "gauge")
    out[n - 1] = now
    return out


def _retention_history(q, f, t, n):
    if not _truthy(q.get("retentionS")):
        return None
    g = _gauge(q, "oldestS", f, t, n)
    return None if g is None else [retention_pct(v, q["retentionS"]) for v in g]


def _visibility_history(q, f, t, n, p):
    vt = q.get("visibilityTimeoutS")
    if not _truthy(vt):
        return None
    span = (t - f) / n
    out = []
    for i in range(n):
        v = visibility_pct(p(f + i * span, f + (i + 1) * span), vt)
        out.append(0 if v is None else v)
    return out


MEASURES = [
    Measure("messages_visible", "Messages visible", ASKS[0], "count", "sum", col="Visible",
            live=lambda q, p: q.get("visible"), history=lambda q, f, t, n, p: _gauge(q, "visible", f, t, n)),
    Measure("messages_in_flight", "Messages in flight", ASKS[0], "count", "sum", col="In flight",
            live=lambda q, p: q.get("inFlight"), history=lambda q, f, t, n, p: _gauge(q, "inFlight", f, t, n)),
    Measure("oldest_age", "Age of oldest message", ASKS[0], "s", "max", col="Oldest message",
            live=lambda q, p: q.get("oldestS"), history=lambda q, f, t, n, p: _gauge(q, "oldestS", f, t, n)),
    # visible messages over the recent net rate CloudWatch counts (deleted minus sent), and the same at each bucket's end
    Measure("time_to_drain", "Time to drain", ASKS[0], "s", "max",
            live=lambda q, p: _drain_now(q), history=lambda q, f, t, n, p: _drain_history(q, f, t, n)),
    Measure("sent", "Sent per minute", ASKS[1], "rate", "sum", ev={"when": _is("send")}),
    Measure("received", "Received per minute", ASKS[1], "rate", "sum", ev={"when": _is("receive")}),
    Measure("deleted", "Deleted per minute", ASKS[1], "rate", "sum", ev={"when": _is("delete")}),
    Measure("processing_time", "Processing time, p95", ASKS[1], "ms", "max",
            ev={"when": _is("delete", "processing_error"), "take": _key("durationMs"), "fold": "p95"}),
    Measure("time_in_queue", "Time in queue, p95", ASKS[1], "ms", "max",
            ev={"when": _is("receive"), "take": _key("waitMs"), "fold": "p95"}),
    Measure("dlq_depth", "DLQ depth", ASKS[2], "count", "sum",
            live=lambda q, p: q.get("dlqDepth") if _truthy(q.get("dlq")) else None,
            history=lambda q, f, t, n, p: _gauge(q, "dlq", f, t, n) if _truthy(q.get("dlq")) else None),
    # instrumented messages with no final settle, first seen more than 5 minutes ago (the Stuck page)
    Measure("stuck_in_flight", "Stuck in flight", ASKS[2], "count", "sum", spans=True,
            live=lambda q, p: q.get("stuck"), history=lambda q, f, t, n, p: _span_gauge(q, "stuck", f, t, n, q.get("stuck"))),
    Measure("processing_errors", "Processing errors", ASKS[2], "count", "sum", ev={"when": _is("processing_error")}),
    Measure("dead_lettered", "Messages dead-lettered", ASKS[2], "count", "sum", ev={"when": _is("dead_letter")}),
    Measure("retention_headroom", "Retention headroom", ASKS[3], "pct", "max",
            live=lambda q, p: retention_pct(q.get("oldestS"), q.get("retentionS")),
            history=lambda q, f, t, n, p: _retention_history(q, f, t, n)),
    # p95 processing time from the queue's events, over its visibility timeout from GetQueueAttributes
    Measure("visibility_headroom", "Visibility timeout headroom", ASKS[3], "pct", "max",
            live=lambda q, p: visibility_pct(p(), q.get("visibilityTimeoutS")), history=_visibility_history),
    # instrumented messages in flight on their last allowed receive, as a share of those in flight
    Measure("near_dlq", "One receive from the DLQ", ASKS[3], "pct", "max", spans=True,
            live=lambda q, p: _near_now(q), history=lambda q, f, t, n, p: _span_gauge(q, "nearDlq", f, t, n, _near_now(q))),
    Measure("events", "Events matching the filter", ASKS[4], "count", "sum", ev={}),
    Measure("body_size", "Body size, average", ASKS[4], "bytes", "max", ev={"take": _key("sizeBytes"), "fold": "avg"}),
    Measure("receive_count", "Receive count, highest", ASKS[4], "count", "max", ev={"take": _key("receiveCount"), "fold": "max"}),
]
_BY_ID = {m.id: m for m in MEASURES}


def measure(id_) -> Optional[Measure]:
    return _BY_ID.get(id_) if isinstance(id_, str) else None


#: the queue table's columns when a widget names none
TABLE_COLUMNS = ["messages_visible", "messages_in_flight", "oldest_age", "dlq_depth", "time_to_drain"]
#: the measures whose history the server works out from the spans
SPAN_MEASURES = {"stuck_in_flight", "near_dlq"}
#: the fields a widget may group by or count by, and the numeric ones: the Event explorer's own list
GROUP_FIELDS = [{"id": f["name"], "label": f["label"]} for f in FIELD_DEFS if f["facet"] and f["kind"] == "text" and f["group"] == "Event"]
NUMBER_FIELDS = [{"id": f["name"], "label": f["label"]} for f in FIELD_DEFS if f["kind"] != "text" and f["group"] == "Event"]


def _live(data: dict, queue: str) -> Optional[dict]:
    q = (data.get("queueLive") or {}).get(queue)
    return q if q and q.get("status") == "ok" else None


def p95_of(xs: list) -> float:
    """The 95th percentile, by nearest rank; 0 for none."""
    if not xs:
        return 0
    s = sorted(xs)
    return s[min(len(s) - 1, math.ceil(0.95 * len(s)) - 1)]


def p95_processing(data: dict, queue: str, from_: float, to: float) -> Optional[float]:
    """p95 processing time of one queue's events in [from, to), or None without any."""
    xs = [e["durationMs"] for e in data.get("events") or []
          if e.get("queue") == queue and from_ <= e["ms"] < to and e.get("event") in ("delete", "processing_error")
          and e.get("durationMs") is not None]
    return p95_of(xs) if xs else None


def queue_value(m: Measure, queue: str, data: dict, time: Optional[dict] = None) -> Optional[float]:
    """A queue measure's figure for one queue now, from its figures; None when it has none (or m is an event measure)."""
    if not m.queue:
        return None
    q = _live(data, queue)
    if not q:
        return None
    t = time or preset("1h", data["now"])
    return m.live(q, lambda: p95_processing(data, queue, t["from"], t["to"]))


def depth_missing(m: Measure, data: dict) -> bool:
    """A queue measure with no queue that has a figure for it."""
    return m.queue and not any(queue_value(m, q, data) is not None for q in data.get("queues") or [])


def no_history(m: Measure, data: dict, time: Optional[dict] = None) -> bool:
    """A queue measure with no history over the range: worked out from the spans as they are now, or from the spans'
    history (24 hours) when no queue has it for this range. Without a range, the past hour."""
    if not m.queue:
        return False
    if not m.history:
        return True
    if not m.spans:
        return False
    t = time or preset("1h", data["now"])
    return not any(q.get("status") == "ok" and m.history(q, t["from"], t["to"], 2, lambda a, b: None) is not None
                   for q in (data.get("queueLive") or {}).values())


def span_queues(dash: dict, names: list) -> list:
    """The queues a dashboard draws span-history measures for: every queue a widget using one can show, whatever its
    variables are set to."""
    out: dict = {}
    any_ = [{**v, "value": "*"} for v in dash.get("variables") or []]
    for w in dash.get("widgets") or []:
        if w.get("type") == "queue_table":
            ids = w.get("columns") or TABLE_COLUMNS
        else:
            ids = [q.get("measure") for q in w.get("queries") or []]
        if not any(i in SPAN_MEASURES for i in ids):
            continue
        for q in queues_for(parse(effective_filter(w, any_)), names):
            out[q] = True
    return list(out)


def no_depth(data: dict) -> str:
    """Why a queue measure has no figure, in a sentence."""
    qs = list((data.get("queueLive") or {}).values())
    failed = next((q for q in qs if q.get("status") == "failed"), None)
    if failed:
        return f"No data. {why_none(failed)}"
    if any(q.get("status") == "ok" for q in qs):
        return ("No data for this measure from the queues that match. It needs a setting the queue does not have "
                "(a redrive policy, for example) or a figure CloudWatch has not reported yet.")
    return f"No data. {why_none(qs[0] if qs else None)}"


def queue_only_terms(filter_: str) -> list:
    """The filter terms a queue measure can't apply: everything but queue."""
    return [t["raw"].strip() for t in parse(filter_) if t.get("field") != "queue" and not t.get("error") and t["raw"].strip()]


def _dur(sec: float) -> str:
    """A length of time: 4s, 2m, 5m 12s, 3h 41m."""
    if sec < 60:
        return f"{to_fixed(sec, 1) if sec < 10 and math.fmod(sec, 1) else js_round(sec)}s"
    s = js_round(sec)
    m, h = (s // 60) % 60, s // 3600
    if h:
        return f"{h}h {m}m" if m else f"{h}h"
    return f"{m}m {s % 60}s" if s % 60 else f"{m}m"


def fmt(v: float, unit: str) -> str:
    """A figure as a widget shows it, in its unit."""
    if not math.isfinite(v):
        return "Growing"
    if unit == "pct":
        return f"{to_fixed(v, 1) if 0 < v < 10 else js_round(v)}%"
    if unit == "s":
        return _dur(v)
    if unit == "ms":
        return f"{js_round(v)}ms" if v < 1000 else _dur(v / 1000)
    if unit == "bytes":
        return fmt_bytes(v)
    if v >= 10_000:
        return re.sub(r"\.0$", "", to_fixed(v / 1000, 0 if v >= 100_000 else 1)) + "k"
    return fmt_num(js_round(v))


fmt_limit = fmt  # a rule's limit, as someone types it, in the measure's own unit


# ── filters and variables ───────────────────────────────────────────────────────────────────────────────────────

_VAR = re.compile(r"(\S*)\$([A-Za-z_][A-Za-z0-9_.]*)")
_NEEDS_QUOTES = re.compile(r"[\s()]")


def _quoted(v: str) -> str:
    return f'"{v}"' if _NEEDS_QUOTES.search(v) else v


def effective_filter(w: dict, vars_: list) -> str:
    """The filter a widget really runs: $name becomes the variable's value, and a widget that follows the dashboard gets
    every other variable added."""
    f = w.get("filter") or ""
    used = set()

    def sub(m):
        v = next((x for x in vars_ if x["name"] == m[2]), None)
        if not v:
            return m[0]
        used.add(m[2])
        return "" if v["value"] == "*" else f"{m[1]}{_quoted(v['value'])}"

    f = _VAR.sub(sub, f)
    if w.get("follow") is not False:
        for v in vars_:
            if v["name"] not in used and v["value"] != "*":
                f += f" {v['field']}:{_quoted(v['value'])}"
    return re.sub(r"\s+", " ", f).strip()


def _wild(pattern: str, name: str) -> bool:
    return re.fullmatch(".*".join(re.escape(p) for p in pattern.lower().split("*")), name.lower()) is not None


def queues_for(terms: list, names: list) -> list:
    """The queues a filter leaves. Only its queue terms count for a queue measure."""
    pos = [t for t in terms if not t["neg"] and t.get("field") == "queue" and t["value"] and not t.get("error")]
    neg = [t for t in terms if t["neg"] and t.get("field") == "queue" and t["value"] and not t.get("error")]
    return [n for n in names if (not pos or any(_wild(t["value"], n) for t in pos)) and not any(_wild(t["value"], n) for t in neg)]


# ── series ──────────────────────────────────────────────────────────────────────────────────────────────────────

def _fold(m: Measure, vals: list) -> float:
    return _total(vals) if m.agg == "sum" else max([0, *vals])


def _finite_or(v: float, other: float) -> float:
    return v if math.isfinite(v) else other


def series_for(m: Measure, filter_: str, group_by: Optional[str], time: dict, n: int, data: dict) -> list:
    """One measure as lines {name, points, now}: one per group value, or one for all of it. points: n buckets over
    time; now: the figure over the whole range (a queue measure's figure now). Largest first."""
    terms = parse(filter_)
    if depth_missing(m, data):
        return []
    if m.queue:
        per = []
        for q in queues_for(terms, data.get("queues") or []):
            now = queue_value(m, q, data, time)
            if now is None:
                continue
            # CloudWatch's history; a figure with none is drawn flat at its value now (no_history says so)
            h = m.history(_live(data, q), time["from"], time["to"], n,
                          lambda a, b, q=q: p95_processing(data, q, a, b)) if m.history else None
            per.append({"name": q, "now": now, "points": h if h is not None else [_finite_or(now, 0)] * n})
        if group_by == "queue":
            return sorted(per, key=lambda s: -_finite_or(s["now"], 1e12))
        return [{"name": m.label, "now": _fold(m, [p["now"] for p in per]),
                 "points": [_fold(m, [p["points"][i] for p in per]) for i in range(n)]}]
    span = (time["to"] - time["from"]) / n
    ev = m.ev or {}
    when, take = ev.get("when"), ev.get("take")
    rows = [e for e in data.get("events") or []
            if time["from"] <= e["ms"] < time["to"] and matches(e, terms) and (not when or when(e))]
    d = field_def(group_by) if _truthy(group_by) else None

    def key(e):
        if not d:
            return m.label
        v = d["get"](e)
        return "none" if v is None else js_str(v)

    by: dict[str, list] = {}
    for e in rows:
        by.setdefault(key(e), []).append(e)
    if not by:
        by[m.label] = []

    def val(es: list, minutes: float) -> float:
        if not take:
            return len(es) / max(minutes, 1e-9) if m.unit == "rate" else len(es)
        xs = [x for x in (take(e) for e in es) if x is not None]
        if not xs:
            return 0
        if ev.get("fold") == "p95":
            return p95_of(xs)
        if ev.get("fold") == "max":
            return max(xs)
        return _total(xs) / len(xs)

    out = []
    for name, es in by.items():
        bucketed: list[list] = [[] for _ in range(n)]
        for e in es:
            bucketed[min(n - 1, math.floor((e["ms"] - time["from"]) / span))].append(e)
        out.append({"name": name, "now": val(es, (time["to"] - time["from"]) / 60_000),
                    "points": [val(b, span / 60_000) for b in bucketed]})
    return sorted(out, key=lambda s: -s["now"])


def event_buckets(filter_: str, time: dict, n: int, data: dict) -> list:
    """Events per bucket, ok and failed ({ok, err}), as the Event explorer's histogram counts them."""
    terms = parse(filter_)
    span = (time["to"] - time["from"]) / n
    b = [{"ok": 0, "err": 0} for _ in range(n)]
    for e in data.get("events") or []:
        if time["from"] <= e["ms"] < time["to"] and matches(e, terms):
            b[min(n - 1, math.floor((e["ms"] - time["from"]) / span))]["err" if e.get("status") == "error" else "ok"] += 1
    return b


def events_for(filter_: str, time: dict, data: dict) -> list:
    terms = parse(filter_)
    return [e for e in data.get("events") or [] if time["from"] <= e["ms"] < time["to"] and matches(e, terms)]


# ── alerting ────────────────────────────────────────────────────────────────────────────────────────────────────

TONE_WORD = {"good": "OK", "warning": "Warning", "critical": "Critical"}


def queue_tone(queue: str, data: dict) -> Optional[str]:
    """A queue's health, as its figures give it (the sidebar's dots show the same thing)."""
    t = preset("1h", data["now"])
    return health_of((data.get("queueLive") or {}).get(queue), p95_processing(data, queue, t["from"], t["to"]))


def alert_of(w: dict, vars_: list, data: dict) -> Optional[dict]:
    """What a widget with a rule says right now: {widget, tone, value, limit, queue}, its figure and status and the
    queue behind it (the worst; "" for an event measure). None without a rule, a limit or a figure."""
    qs = w.get("queries") or []
    m = measure(_get(qs[0], "measure") if qs else None)
    rule = w.get("rule")
    if not m or not _truthy(rule) or w.get("type") not in ("number", "timeseries") or depth_missing(m, data):
        return None
    filter_ = effective_filter(w, vars_)
    time = preset(w.get("time") or "1h", data["now"])
    # a queue rule whose queues have no figure has none, even if a queue it does not match has one (QV-357)
    if m.queue and not any(queue_value(m, q, data, time) is not None for q in queues_for(parse(filter_), data.get("queues") or [])):
        return None
    s = series_for(m, filter_, None, time, 12, data)
    if not s:
        return None
    now = s[0]["now"]
    tone = tone_of(now, rule)
    if not tone:
        return None
    worst = ""
    if m.queue:
        by_queue = series_for(m, filter_, "queue", time, 2, data)
        worst = by_queue[0]["name"] if by_queue else ""
    lim = limit_of(rule, tone)
    return {"widget": w, "tone": tone, "value": fmt(now, m.unit),
            "limit": f"{'below' if lim['below'] else 'above'} {fmt_limit(lim['limit'], m.unit)}", "queue": worst}


# ── checking a definition ───────────────────────────────────────────────────────────────────────────────────────

WIDGET_TYPES = ["timeseries", "heatmap", "distribution", "number", "gauge", "toplist", "queue_table", "status_grid",
                "event_list", "queue_flow", "note"]
#: the most a stored dashboard definition may take, as JSON
MAX_DEF_BYTES = 256 * 1024


def _tpl(v) -> str:
    """${v} for a value read from a definition: an absent one is "undefined"."""
    return "undefined" if v is None else js_str(v)


def _get(o, k):
    return o.get(k) if isinstance(o, dict) else None


def problem(w) -> Optional[str]:
    """A widget definition's mistakes, in plain words, or None."""
    if not isinstance(w, (dict, list)):
        return "A widget is a JSON object."
    type_ = _get(w, "type")
    if not _truthy(type_) or type_ not in WIDGET_TYPES:
        return f'"type" is "{"" if type_ is None else js_str(type_)}", which is not a widget type.'
    if not isinstance(_get(w, "title"), str):
        return '"title" must be text.'
    for q in _get(w, "queries") or []:
        if not measure(_get(q, "measure")):
            return f'queries has an unknown measure "{_tpl(_get(q, "measure"))}".'
        g = _get(q, "group_by")
        if _truthy(g) and not field_def(g):
            return f'group_by has an unknown field "{_tpl(g)}".'
    f = _get(w, "filter")
    bad = next((t for t in parse("" if f is None else f) if t.get("error") and "$" not in t["raw"]), None)
    if bad:
        return f"filter: {bad['error']}"
    cols = _get(w, "columns") or []
    if type_ == "event_list":
        for c in cols:
            if c not in ("date", "event", "detail", "body") and not field_def(c):
                return f'columns has an unknown field "{_tpl(c)}".'
    if type_ == "queue_table":
        for c in cols:
            if not measure(c):
                return f'columns has an unknown measure "{_tpl(c)}".'
    fld = _get(w, "field")
    if _truthy(fld) and not field_def(fld):
        return f'field is "{_tpl(fld)}", which is not a field.'
    lay = _get(w, "layout")
    if not isinstance(lay, dict) or any(not _is_num(lay.get(k)) for k in ("x", "y", "w", "h")):
        return '"layout" needs x, y, w and h as numbers.'
    return None


def check_def(raw) -> dict:
    """A whole dashboard definition, checked before it is stored: {"def": ...} or {"error": "..."}. Its size, then each
    variable, section and widget. Absent variables and sections are empty lists."""
    if not isinstance(raw, dict):
        return {"error": "A dashboard is a JSON object."}
    if not isinstance(raw.get("widgets"), list):
        return {"error": "That JSON has no list of widgets."}
    variables = raw.get("variables")
    variables = [] if variables is None else variables
    sections = raw.get("sections")
    sections = [] if sections is None else sections
    if not isinstance(variables, list):
        return {"error": '"variables" must be a list.'}
    if not isinstance(sections, list):
        return {"error": '"sections" must be a list.'}
    d = {"variables": variables, "sections": sections, "widgets": raw["widgets"]}
    size = len(js_json(d).encode("utf-8"))
    if size > MAX_DEF_BYTES:
        return {"error": f"That dashboard is {math.ceil(size / 1024)} KB. A dashboard can be at most {MAX_DEF_BYTES // 1024} KB."}
    for i, v in enumerate(variables):
        if not isinstance(v, dict) or not all(isinstance(v.get(k), str) for k in ("name", "field", "value")):
            return {"error": f'Variable {i + 1} needs "name", "field" and "value" as text.'}
    for i, s in enumerate(sections):
        if not isinstance(s, dict) or not all(isinstance(s.get(k), str) for k in ("id", "title")):
            return {"error": f'Section {i + 1} needs "id" and "title" as text.'}
    for i, w in enumerate(d["widgets"]):
        p = problem(w)
        if p:
            return {"error": f"Widget {i + 1}: {p}"}
        if not isinstance(w.get("id"), str):
            return {"error": f'Widget {i + 1}: "id" must be text.'}
    return {"def": d}
