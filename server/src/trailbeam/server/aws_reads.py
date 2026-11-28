"""The AWS reads behind the queue page's CloudWatch band, the Overview's queue figures, the dashboards' queue measures
and "In ‹queue› now" (QV-589), when AWS is turned on. Each is one of the calls the read-only policy allows, and
nothing else:

    sqs:GetQueueAttributes           a queue's settings, its redrive policy and SQS's own counts
    cloudwatch:GetMetricData         depth, in flight, age of the oldest message, sends, deletes and the
                                     dead-letter queue's depth, with history
    lambda:ListEventSourceMappings   the Lambda triggers on a queue and their batch settings

and sts:GetCallerIdentity, which needs no permission and can't be denied, for the credentials' own account when the
settings name none (``caller_account``): a queue outside that account is never read (QV-740).

ReceiveMessage is never called: it would change the visibility of the messages it returned.

This is the hosted app's lib/app/aws-reads.ts with boto3 instead of the AWS SDK for JavaScript. The functions take a
boto3 client (or anything with the same methods, so tests pass moto's or a stub), and return plain data in the shape
of @trailbeam/ui/model's aws.ts; aws_figures.py works out the figures from it.

boto3 is an optional install (``pip install "trailbeam[aws]"``) and is imported only by ``clients``, so a server with
AWS off never imports it.
"""
from __future__ import annotations

import datetime as dt
import os
from typing import Any, Iterable, Optional

from . import aws_figures as af

#: GetMetricData takes at most 500 queries in a call
MAX_QUERIES = 500

INSTALL_HINT = 'AWS reads need boto3: pip install "trailbeam[aws]"'


class AwsUnavailable(RuntimeError):
    """boto3 is not installed."""


def error_name(e: BaseException) -> str:
    """What AWS answered, as the hosted app's errName gives it: the error code of a ClientError (AccessDenied,
    AWS.SimpleQueueService.NonExistentQueue, ...), or the exception's class name (NoCredentialsError, ...)."""
    resp = getattr(e, "response", None)
    if isinstance(resp, dict):
        code = (resp.get("Error") or {}).get("Code")
        if code:
            return str(code)
    return type(e).__name__


def sqs_endpoint() -> Optional[str]:
    """An SQS emulator for local runs, from the AWS SDKs' own AWS_ENDPOINT_URL_SQS or AWS_ENDPOINT_URL. It is passed
    to the client explicitly, as the hosted app does: a QueueUrl names its host, which is real AWS for the
    https://sqs.<region>.amazonaws.com/... URLs Trailbeam builds."""
    e = (os.environ.get("AWS_ENDPOINT_URL_SQS") or "").strip() or (os.environ.get("AWS_ENDPOINT_URL") or "").strip()
    return e or None


def clients(region: Optional[str] = None, profile: Optional[str] = None) -> dict:
    """SQS, CloudWatch and Lambda clients with boto3's usual credentials (the environment, a profile, the instance or
    task role), in the region given or boto3's own default."""
    try:
        import boto3
    except ImportError as e:  # pragma: no cover - the [aws] extra is installed in the tests
        raise AwsUnavailable(INSTALL_HINT) from e
    session = boto3.session.Session(profile_name=profile or None, region_name=region or None)
    sqs_kw = {"endpoint_url": sqs_endpoint()} if sqs_endpoint() else {}
    return {"sqs": session.client("sqs", **sqs_kw), "cloudwatch": session.client("cloudwatch"),
            "lambda": session.client("lambda"), "sts": session.client("sts"), "region": session.region_name}


def caller_account(sts: Any) -> str:
    """The AWS account of the credentials (sts:GetCallerIdentity)."""
    return str(sts.get_caller_identity()["Account"])


# ── GetQueueAttributes ───────────────────────────────────────────────────────────────────────────────────────────

def read_queue_settings(sqs: Any, queue_url: str) -> dict:
    out = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["All"])
    return af.settings_from((out or {}).get("Attributes") or {})


# ── CloudWatch GetMetricData ─────────────────────────────────────────────────────────────────────────────────────

def _ms(t: Any) -> Optional[float]:
    if isinstance(t, dt.datetime):
        if t.tzinfo is None:
            t = t.replace(tzinfo=dt.timezone.utc)
        return int(round(t.timestamp() * 1000))
    if isinstance(t, (int, float)):
        return t
    return None


def _finite(v: Any) -> bool:
    return isinstance(v, (int, float)) and not isinstance(v, bool) and v == v and v not in (float("inf"), float("-inf"))


def _tidy(p: list) -> list:
    """Pages can interleave: each series in time order, one point per time (the last read wins)."""
    seen: dict = {}
    for t, v in p:
        seen[t] = v
    return [[t, seen[t]] for t in sorted(seen)]


def read_queue_metrics(cw: Any, queues: Iterable[str], dlqs: Iterable[str], from_ms: float, to_ms: float,
                       period_s: int, metrics: Optional[Iterable[str]] = None) -> dict:
    """Series for each queue (by name) and each dead-letter queue, in one or more GetMetricData calls (a call takes at
    most 500 queries, and a long answer comes back in pages). Every queue is in the client's account and region:
    CloudWatch's SQS metrics have only the QueueName dimension. ``metrics``: the ones to read for the queues, all
    when None; each metric is billed, so a caller that needs only some asks for those alone. A dead-letter queue has
    only its depth read. Returns {"queues": {name: series}, "dlqs": {name: points}}."""
    queues, dlqs = list(queues), list(dlqs)
    ms = [m for m in af.METRICS if metrics is None or m["key"] in set(metrics)]
    dlq_ms = [m for m in af.METRICS if m["key"] == "visible"]
    wanted: list = []

    def add(queue: str, dlq: bool, these: list) -> None:
        for m in these:
            qid = f"m{len(wanted)}"
            wanted.append({"id": qid, "queue": queue, "dlq": dlq, "key": m["key"], "q": {
                "Id": qid, "ReturnData": True, "MetricStat": {
                    "Metric": {"Namespace": "AWS/SQS", "MetricName": m["name"],
                               "Dimensions": [{"Name": "QueueName", "Value": queue}]},
                    "Period": int(period_s), "Stat": m["stat"]}}})

    for q in dict.fromkeys(queues):
        add(q, False, ms)
    for q in dict.fromkeys(dlqs):
        add(q, True, dlq_ms)
    by_id = {w["id"]: w for w in wanted}
    out_q = {q: af.empty_series() for q in queues}
    out_d: dict = {q: [] for q in dlqs}
    start = dt.datetime.fromtimestamp(from_ms / 1000, tz=dt.timezone.utc)
    end = dt.datetime.fromtimestamp(to_ms / 1000, tz=dt.timezone.utc)
    for i in range(0, len(wanted), MAX_QUERIES):
        chunk = wanted[i:i + MAX_QUERIES]
        token = None
        while True:
            kw = {"NextToken": token} if token else {}
            out = cw.get_metric_data(MetricDataQueries=[w["q"] for w in chunk], StartTime=start, EndTime=end,
                                     ScanBy="TimestampAscending", **kw) or {}
            for r in out.get("MetricDataResults") or []:
                w = by_id.get(r.get("Id"))
                if not w:
                    continue
                pts = [[t, v] for t, v in zip(map(_ms, r.get("Timestamps") or []), r.get("Values") or [])
                       if _finite(t) and _finite(v)]
                into = out_d.setdefault(w["queue"], []) if w["dlq"] else \
                    out_q.setdefault(w["queue"], af.empty_series())[w["key"]]
                into.extend(pts)
            token = out.get("NextToken")
            if not token:
                break
    for s in out_q.values():
        for k in s:
            s[k] = _tidy(s[k])
    for k in out_d:
        out_d[k] = _tidy(out_d[k])
    return {"queues": out_q, "dlqs": out_d}


# ── Lambda ListEventSourceMappings ───────────────────────────────────────────────────────────────────────────────

def read_triggers(lam: Any, queue_arn: str) -> list:
    """The Lambda triggers on a queue (event source mappings), in the shape of aws.ts's AwsTrigger."""
    out: list = []
    marker = None
    while True:
        kw = {"Marker": marker} if marker else {}
        r = lam.list_event_source_mappings(EventSourceArn=queue_arn, **kw) or {}
        for m in r.get("EventSourceMappings") or []:
            fn = m.get("FunctionArn")
            filters = ((m.get("FilterCriteria") or {}).get("Filters")) or []
            out.append({
                "uuid": str(m.get("UUID") or ""),
                "functionArn": fn,
                "functionName": (fn.split(":")[6] if len(fn.split(":")) > 6 else fn) if fn else "",
                "state": m.get("State"),
                "batchSize": af._num(m.get("BatchSize")),
                "batchingWindowS": af._num(m.get("MaximumBatchingWindowInSeconds")),
                "reportBatchItemFailures": "ReportBatchItemFailures" in (m.get("FunctionResponseTypes") or []),
                "maxConcurrency": af._num((m.get("ScalingConfig") or {}).get("MaximumConcurrency")),
                "filters": len(filters),
                "filterPatterns": [f["Pattern"] for f in filters if isinstance(f, dict) and isinstance(f.get("Pattern"), str)],
            })
        marker = r.get("NextMarker")
        if not marker:
            break
    return out
