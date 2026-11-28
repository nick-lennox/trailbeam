"""What AWS says about a queue, and the figures worked out from it (QV-589), as pure functions.

Open-source Trailbeam reads AWS only when it is turned on (aws_reads.py makes the reads with boto3). What those reads
return is plain data in the shape of @trailbeam/ui/model's aws.ts, and this module is that file's port: the settings
from GetQueueAttributes' attributes map (``settings_from``), the CloudWatch period for a window (``period_for``), the
figures now (``now_figures``), a queue's figures for the Overview, the dashboards and the alert check (``live_of``, the
``QueueLive`` shape measures.py reads as ``queueLive``), and the queue page's AWS part (``queue_aws_view``).

Dicts carry the TypeScript names (``visibilityTimeoutS``, ``dlqName``, ...), so they go to the browser as they are.
Times are milliseconds since the epoch; a CloudWatch point is ``[ms, value]``.

contract/fixtures/aws-figures-parity.json holds the cases both engines must agree on (make-aws-figures-parity.py
writes it from this module; python/tests/server/test_aws_figures.py and @trailbeam/ui's test/aws.test.ts run it).
"""
from __future__ import annotations

import json
import math
import re
from typing import Any, Optional

#: the Stuck page's limit: a message received and not settled for longer is stuck
STUCK_AFTER_MS = 300_000
#: the window the span-derived history goes on: a day at one minute
SPAN_WINDOW = "day"

#: The SQS metrics read, and the statistic for each. Depth and age are gauges, so the highest value in a period;
#: sends and deletes are counts, so their sum.
METRICS = (
    {"key": "visible", "name": "ApproximateNumberOfMessagesVisible", "stat": "Maximum"},
    {"key": "inFlight", "name": "ApproximateNumberOfMessagesNotVisible", "stat": "Maximum"},
    {"key": "oldestS", "name": "ApproximateAgeOfOldestMessage", "stat": "Maximum"},
    {"key": "sent", "name": "NumberOfMessagesSent", "stat": "Sum"},
    {"key": "deleted", "name": "NumberOfMessagesDeleted", "stat": "Sum"},
)
METRIC_NAMES = tuple(m["key"] for m in METRICS)

_ARN = re.compile(r"^arn:[^:]+:sqs:([^:]*):([^:]*):(.+)$")
_DAY_MS = 86_400_000


def _num(v: Any) -> Optional[float]:
    """JavaScript's Number() on an attribute value, null for no value or one that is not a finite number."""
    if v is None or v == "":
        return None
    if isinstance(v, bool):
        return int(v)
    if isinstance(v, (int, float)):
        return v if math.isfinite(v) else None
    s = str(v).strip()
    if s == "":
        return 0  # Number("  ") is 0
    if "_" in s:
        return None  # Python's float() reads 1_000, JavaScript's Number() does not
    try:
        n = float(s)
    except ValueError:
        return None
    if not math.isfinite(n):
        return None
    return int(n) if n.is_integer() else n


def parse_queue_arn(arn: Optional[str]) -> Optional[dict]:
    """A queue's name, account and region from its ARN, ``arn:aws:sqs:<region>:<account>:<name>``."""
    m = _ARN.match(arn or "")
    return {"name": m.group(3), "account": m.group(2) or None, "region": m.group(1) or None} if m else None


def settings_from(a: dict) -> dict:
    """The parts of GetQueueAttributes' Attributes map that Trailbeam uses."""
    max_receive, dlq_arn = None, None
    if a.get("RedrivePolicy"):
        try:
            p = json.loads(a["RedrivePolicy"])
        except (ValueError, TypeError):
            p = None  # a policy that is not JSON: treated as none
        if isinstance(p, dict):
            max_receive = _num(p.get("maxReceiveCount"))  # a number or a string, depending on how it was set
            dlq_arn = p["deadLetterTargetArn"] if isinstance(p.get("deadLetterTargetArn"), str) else None
    dlq = parse_queue_arn(dlq_arn) if dlq_arn else None
    return {
        "arn": a.get("QueueArn"),
        "fifo": a.get("FifoQueue") == "true",
        "visibilityTimeoutS": _num(a.get("VisibilityTimeout")),
        "retentionS": _num(a.get("MessageRetentionPeriod")),
        "delayS": _num(a.get("DelaySeconds")),
        "maxReceiveCount": max_receive,
        "dlqArn": dlq_arn,
        "dlqName": dlq["name"] if dlq else None,
        "visible": _num(a.get("ApproximateNumberOfMessages")),
        "inFlight": _num(a.get("ApproximateNumberOfMessagesNotVisible")),
        "delayed": _num(a.get("ApproximateNumberOfMessagesDelayed")),
    }


def empty_series() -> dict:
    return {k: [] for k in METRIC_NAMES}


def period_for(from_ms: float, to_ms: float, now_ms: float, max_points: int = 1440) -> int:
    """CloudWatch keeps 1-minute points for 15 days, 5-minute points for 63 days and 1-hour points for 455 days, and a
    period must be a multiple of 60 s. The shortest period that both exists that far back and keeps a request under
    max_points per metric."""
    age = now_ms - from_ms
    floor = 3600 if age > 63 * _DAY_MS else 300 if age > 15 * _DAY_MS else 60
    need = math.ceil((to_ms - from_ms) / 1000 / max_points / 60) * 60
    return max(floor, need)


def empty_aws(status: str, detail: Optional[str], account: Optional[dict] = None) -> dict:
    """A queue's reads when there are none: status says why (no_account, not_configured, failed)."""
    return {"status": status, "detail": detail, "accountId": (account or {}).get("accountId"),
            "region": (account or {}).get("region"), "readAt": None, "settings": None, "series": {}, "triggers": None,
            "errors": []}


def name_matches(pattern: str, name: str) -> bool:
    """A queue name a list of names names: the exact name, or a pattern with * in it."""
    # a * matches what a queue name can hold, never a / or a ? (QV-740)
    return re.fullmatch("[A-Za-z0-9_.-]*".join(re.escape(s) for s in pattern.split("*")), name) is not None


def _last(p: Optional[list]) -> Optional[list]:
    return p[-1] if p else None


def _or(a: Any, b: Any) -> Any:
    """JavaScript's a ?? b."""
    return b if a is None else a


def now_figures(a: Optional[dict], window_id: str) -> dict:
    """The queue's figures now. Visible and in flight are SQS's own counts from GetQueueAttributes, which are current;
    CloudWatch's latest minute is used when they are missing. The age of the oldest message and the dead-letter queue's
    depth exist only in CloudWatch, so they are as of its latest point: oldestAt and dlqAt are when that point's period
    starts. CloudWatch's newest point is for the minute that ended about 30 s before the read, so these trail the true
    figures by a minute or two."""
    a = a or {}
    s = (a.get("series") or {}).get(window_id)
    q = (s or {}).get("queue") or {}
    st = a.get("settings") or {}
    v, f, o, d = _last(q.get("visible")), _last(q.get("inFlight")), _last(q.get("oldestS")), _last((s or {}).get("dlq"))
    dlq_now = a.get("dlqNow")
    has_dlq = bool(st.get("dlqName"))
    dlq = (s or {}).get("dlq") or []
    return {
        "visible": _or(st.get("visible"), v[1] if v else None),
        "inFlight": _or(st.get("inFlight"), f[1] if f else None),
        "delayed": st.get("delayed"),
        "oldestS": o[1] if o else None,
        "oldestAt": o[0] if o else None,
        "dlqDepth": (dlq_now["visible"] if dlq_now else d[1] if d else None) if has_dlq else None,
        # when the depth is from: the DLQ's own read, or the start of CloudWatch's latest period
        "dlqAt": (dlq_now["at"] if dlq_now else d[0] if d else None) if has_dlq else None,
        "dlqFromSqs": bool(has_dlq and dlq_now),
        # how the dead-letter queue's depth changed over the window: last point minus first
        "dlqChange": dlq[-1][1] - dlq[0][1] if has_dlq and s and len(dlq) > 1 else None,
    }


def live_of(aws: dict, view: Optional[dict], windows: list, now: float, spans: Optional[dict] = None) -> dict:
    """A queue's figures (QueueLive) from its AWS reads (``aws``), "In ‹queue› now" (``view``, queue_now's
    InQueueView) and the span history over the day window (``spans``: {"stuck": [...], "nearDlq": [...] or None})."""
    s = aws.get("settings")
    first = windows[0]["id"] if windows else None
    f = now_figures(aws, first or "")
    series = (aws.get("series") or {}).get(first or "") if aws.get("status") == "ok" else None
    period_ms = _or((series or {}).get("periodS"), 60) * 1000
    mrc = (s or {}).get("maxReceiveCount")
    held = (view or {}).get("held") or []
    near = (sum(1 for m in held if m["state"] == "in_flight" and m["receiveCount"] >= mrc)
            if view and mrc else None)
    out_windows = []
    for w in windows:
        x = (aws.get("series") or {}).get(w["id"])
        if not x:
            continue
        item = {"periodS": x["periodS"], "fromMs": x["fromMs"], "toMs": x["toMs"], **x["queue"], "dlq": x["dlq"]}
        if spans and w["id"] == SPAN_WINDOW:
            item["stuck"] = spans["stuck"]
            if spans.get("nearDlq"):
                item["nearDlq"] = spans["nearDlq"]
        out_windows.append(item)
    out_windows.sort(key=lambda w: w["periodS"])  # stable, as Array.prototype.sort is
    errors = aws.get("errors") or []
    detail = aws.get("detail")
    return {
        "status": aws["status"], "detail": _or(detail, " ".join(errors) if errors else None),
        "readAt": aws.get("readAt"),
        "visibilityTimeoutS": (s or {}).get("visibilityTimeoutS"), "retentionS": (s or {}).get("retentionS"),
        "delayS": (s or {}).get("delayS"), "maxReceiveCount": mrc,
        "dlq": (s or {}).get("dlqName"), "fifo": s["fifo"] if s else None,
        "visible": f["visible"], "inFlight": f["inFlight"], "oldestS": f["oldestS"], "dlqDepth": f["dlqDepth"],
        "oldestAsOf": None if f["oldestAt"] is None else f["oldestAt"] + period_ms,
        "dlqAsOf": None if f["dlqAt"] is None else f["dlqAt"] if f["dlqFromSqs"] else f["dlqAt"] + period_ms,
        "oldestUnpublished": series is not None and not series["queue"]["oldestS"],
        "dlqUnpublished": series is not None and bool((s or {}).get("dlqName")) and not series["dlq"] and not f["dlqFromSqs"],
        "windows": out_windows,
        "stuck": sum(1 for m in held if m["sentMs"] < now - STUCK_AFTER_MS) if view else None,
        "nearDlq": None if near is None or not view else {"count": near, "inFlight": view["instrumented"]["inFlight"]},
    }


def page_trigger(t: dict) -> dict:
    """A trigger as the queue page draws it."""
    return {k: t.get(k) for k in ("functionName", "state", "batchSize", "batchingWindowS", "reportBatchItemFailures",
                                  "maxConcurrency", "filters")}


def queue_aws_view(a: dict, window_id: str) -> dict:
    """The queue page's AWS part (QueuePageData.aws) from a queue's reads over the page's window."""
    w = (a.get("series") or {}).get(window_id)
    f = now_figures(a, window_id)
    st = a.get("settings")
    return {
        "status": a["status"], "detail": a.get("detail"), "readAt": a.get("readAt"),
        "settings": {k: st.get(k) for k in ("fifo", "visibilityTimeoutS", "retentionS", "delayS", "maxReceiveCount",
                                            "dlqName", "visible", "inFlight")} if st else None,
        "series": {"visible": w["queue"]["visible"], "inFlight": w["queue"]["inFlight"],
                   "oldestS": w["queue"]["oldestS"], "sent": w["queue"]["sent"], "dlq": w["dlq"]} if w else None,
        "now": {k: f[k] for k in ("visible", "inFlight", "oldestS", "oldestAt", "dlqDepth", "dlqAt", "dlqChange")}
        if a["status"] == "ok" else None,
        "errors": a.get("errors") or [],
    }
