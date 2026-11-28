"""Writes aws-figures-parity.json: the cases on AWS reads' figures both engines must agree on (QV-589).

Self-hosted Trailbeam (server/src/trailbeam/server/aws_figures.py, python/tests/server/test_aws_figures.py) and the
shared TypeScript (@trailbeam/ui/model aws.ts, run by ui/test/aws.test.ts and by the hosted app's tests, reading
@trailbeam/ui/fixtures/aws-figures-parity.json) run every case and must give the expected result: the settings from
GetQueueAttributes' attributes (``settingsFrom``), a queue ARN's parts, the CloudWatch period for a window
(``periodFor``), which queue names a pattern names, the figures now (``nowFigures``), a queue's figures
(``liveOf``) and the queue page's AWS part (``queueAwsView``). The expected results are the Python engine's. Run from
the repository root after changing a case or the figures:

    PYTHONPATH=python/src:server/src python contract/fixtures/make-aws-figures-parity.py
"""
import json
from pathlib import Path

from trailbeam.server import aws_figures as af

OUT = Path(__file__).resolve().parent / "aws-figures-parity.json"
MIN = 60_000
H = 3_600_000
DAY = 86_400_000
T0 = 1_764_000_000_000  # 2025-11-24T16:00:00Z, ms
DLQ_ARN = "arn:aws:sqs:eu-west-1:123456789012:orders-dlq"
POLICY = json.dumps({"deadLetterTargetArn": DLQ_ARN, "maxReceiveCount": 5})

ATTRS = {
    "QueueArn": "arn:aws:sqs:eu-west-1:123456789012:orders", "VisibilityTimeout": "30",
    "MessageRetentionPeriod": "345600", "DelaySeconds": "0", "RedrivePolicy": POLICY,
    "ApproximateNumberOfMessages": "12", "ApproximateNumberOfMessagesNotVisible": "3",
    "ApproximateNumberOfMessagesDelayed": "0",
}

SETTINGS = [
    ("every attribute Trailbeam uses", ATTRS),
    ("a FIFO queue", {**ATTRS, "QueueArn": "arn:aws:sqs:eu-west-1:123456789012:orders.fifo", "FifoQueue": "true"}),
    ("FifoQueue false", {**ATTRS, "FifoQueue": "false"}),
    ("no redrive policy", {k: v for k, v in ATTRS.items() if k != "RedrivePolicy"}),
    ("maxReceiveCount as a string", {**ATTRS, "RedrivePolicy": json.dumps({"deadLetterTargetArn": DLQ_ARN, "maxReceiveCount": "3"})}),
    ("a redrive policy that is not JSON", {**ATTRS, "RedrivePolicy": "{not json"}),
    ("a redrive policy of null", {**ATTRS, "RedrivePolicy": "null"}),
    ("a redrive policy with no target", {**ATTRS, "RedrivePolicy": json.dumps({"maxReceiveCount": 2})}),
    ("a target that is not a queue ARN", {**ATTRS, "RedrivePolicy": json.dumps({"deadLetterTargetArn": "orders-dlq", "maxReceiveCount": 2})}),
    ("a target that is not a string", {**ATTRS, "RedrivePolicy": json.dumps({"deadLetterTargetArn": 7, "maxReceiveCount": 2})}),
    ("empty values", {**ATTRS, "VisibilityTimeout": "", "ApproximateNumberOfMessages": ""}),
    ("values that are not numbers", {**ATTRS, "VisibilityTimeout": "thirty", "DelaySeconds": "NaN", "MessageRetentionPeriod": "Infinity"}),
    ("a fraction and spaces", {**ATTRS, "VisibilityTimeout": " 42 ", "DelaySeconds": "1.5"}),
    ("nothing", {}),
]

ARNS = [
    DLQ_ARN, "arn:aws-cn:sqs:cn-north-1:123456789012:a.fifo", "arn:aws:sqs:::bare", "arn:aws:sqs:eu-west-1:1:with:colon",
    "arn:aws:sns:eu-west-1:1:topic", "orders", "", None,
]

PERIODS = [
    ("the past hour", T0 - H, T0, T0),
    ("the past day", T0 - DAY, T0, T0),
    ("the past 30 days", T0 - 30 * DAY, T0, T0),
    ("a day 20 days ago", T0 - 20 * DAY, T0 - 19 * DAY, T0),
    ("a day 70 days ago", T0 - 70 * DAY, T0 - 69 * DAY, T0),
    ("an hour 15 days ago, at the edge", T0 - 15 * DAY, T0 - 15 * DAY + H, T0),
    ("a week", T0 - 7 * DAY, T0, T0),
    ("a minute", T0 - MIN, T0, T0),
    ("a day with at most 100 points", T0 - DAY, T0, T0, 100),
]

NAMES = [
    ("orders", "orders"), ("orders", "orders-dlq"), ("orders*", "orders-dlq"), ("*", "anything"), ("*-dlq", "orders-dlq"),
    ("*-dlq", "orders"), ("orders.fifo", "orders.fifo"), ("orders.fifo", "ordersXfifo"), ("a*b*c", "a-b-c"),
    ("a*b*c", "a-c"), ("(x)+", "(x)+"), ("(x)+", "xx"), ("", ""), ("", "orders"),
]


def pts(start, values, step=MIN):
    return [[start + i * step, v] for i, v in enumerate(values)]


def series(start, step=MIN, **kw):
    q = af.empty_series()
    q.update({k: v for k, v in kw.items() if k != "dlq"})
    return {"queue": q, "dlq": kw.get("dlq", [])}


def window(wid, from_ms, to_ms, period_s, **kw):
    s = series(from_ms, **kw)
    return wid, {"periodS": period_s, "fromMs": from_ms, "toMs": to_ms, **s}


def aws_ok(settings=ATTRS, windows=(), errors=(), triggers=None, dlq_now=None, detail=None):
    a = {**af.empty_aws("ok", detail, {"accountId": "123456789012", "region": "eu-west-1"}), "readAt": T0,
         "settings": af.settings_from(settings) if settings is not None else None, "series": dict(windows),
         "errors": list(errors), "triggers": triggers}
    if dlq_now is not None:
        a["dlqNow"] = dlq_now
    return a


R = T0 - H
RECENT = window("recent", R, T0, 60, visible=pts(R, [4, 6, 9]), inFlight=pts(R, [1, 2, 2]),
                oldestS=pts(R, [30, 90, 150]), sent=pts(R, [10, 12, 9]), deleted=pts(R, [8, 11, 12]),
                dlq=pts(R, [0, 1, 3]))
DAYW = window("day", T0 - DAY, T0, 60, visible=pts(T0 - DAY, [5, 7]), dlq=pts(T0 - DAY, [2]))
MONTH = window("month", T0 - 30 * DAY, T0, 3600, visible=pts(T0 - 30 * DAY, [1, 2, 3], H))
EMPTY = window("recent", R, T0, 60)
QUIET_DLQ = window("recent", R, T0, 60, visible=pts(R, [0]), oldestS=pts(R, [0]))

NO_COUNTS = {k: v for k, v in ATTRS.items() if not k.startswith("Approximate")}
NO_DLQ = {k: v for k, v in ATTRS.items() if k != "RedrivePolicy"}

AWS = {
    "read, with SQS's counts": aws_ok(windows=[RECENT]),
    "read, without SQS's counts": aws_ok(settings=NO_COUNTS, windows=[RECENT]),
    "read, with the DLQ's own count": aws_ok(windows=[RECENT], dlq_now={"visible": 7, "at": T0 - 5000}),
    "read, no dead-letter queue": aws_ok(settings=NO_DLQ, windows=[RECENT]),
    "read, CloudWatch returned nothing": aws_ok(windows=[EMPTY]),
    "read, CloudWatch has no DLQ point": aws_ok(windows=[QUIET_DLQ]),
    "read, no CloudWatch window": aws_ok(),
    "read, CloudWatch failed": aws_ok(errors=["cloudwatch:GetMetricData failed with AccessDenied."]),
    "read, two failures": aws_ok(errors=["cloudwatch:GetMetricData failed with AccessDenied.",
                                         "lambda:ListEventSourceMappings failed with AccessDenied."]),
    "read, day and month": aws_ok(windows=[MONTH, DAYW]),
    "no account names it": af.empty_aws("no_account", "No queue named in AWS settings matches this one."),
    "AWS is off": af.empty_aws("not_configured", "AWS is off on this server."),
    "the read failed": af.empty_aws("failed", "sqs:GetQueueAttributes failed on orders with AccessDenied.",
                                    {"accountId": "123456789012", "region": "eu-west-1"}),
}


def held(state, receives, sent_ms):
    return {"id": f"m{receives}{state}{sent_ms}", "state": state, "receiveCount": receives, "sentMs": sent_ms}


VIEW = {"held": [held("in_flight", 5, T0 - 10 * MIN), held("in_flight", 2, T0 - MIN), held("visible", 5, T0 - 2 * MIN),
                 held("in_flight", 6, T0 - 30_000)],
        "instrumented": {"inFlight": 3, "visible": 1}}
EMPTY_VIEW = {"held": [], "instrumented": {"inFlight": 0, "visible": 0}}
SPANS = {"stuck": pts(T0 - DAY, [0, 1]), "nearDlq": pts(T0 - DAY, [0, 50])}
SPANS_NO_NEAR = {"stuck": pts(T0 - DAY, [2]), "nearDlq": None}

W_RECENT = [{"id": "recent", "fromMs": R, "toMs": T0, "periodS": 60}]
W_DASH = [{"id": "day", "fromMs": T0 - DAY, "toMs": T0, "periodS": 60},
          {"id": "month", "fromMs": T0 - 30 * DAY, "toMs": T0, "periodS": 3600}]
W_MONTH_FIRST = list(reversed(W_DASH))


def main():
    out = {
        "settingsFrom": [{"name": n, "attributes": a, "expected": af.settings_from(a)} for n, a in SETTINGS],
        "parseQueueArn": [{"arn": a, "expected": af.parse_queue_arn(a)} for a in ARNS],
        "periodFor": [{"name": c[0], "fromMs": c[1], "toMs": c[2], "nowMs": c[3], "maxPoints": c[4] if len(c) > 4 else None,
                       "expected": af.period_for(*c[1:])} for c in PERIODS],
        "nameMatches": [{"pattern": p, "name": n, "expected": af.name_matches(p, n)} for p, n in NAMES],
        "aws": AWS,
        "nowFigures": [{"aws": k, "windowId": w, "expected": af.now_figures(AWS[k] if k else None, w)}
                       for k in [*AWS, None] for w in ("recent", "day")],
        "liveOf": [],
        "queueAwsView": [{"aws": k, "windowId": "recent", "expected": af.queue_aws_view(AWS[k], "recent")} for k in AWS],
    }
    live = [
        ("read, with In ‹queue› now", "read, with SQS's counts", "VIEW", W_RECENT, None),
        ("read, no view", "read, with SQS's counts", None, W_RECENT, None),
        ("read, an empty view", "read, with SQS's counts", "EMPTY_VIEW", W_RECENT, None),
        ("no maxReceiveCount", "read, no dead-letter queue", "VIEW", W_RECENT, None),
        ("the DLQ's own count", "read, with the DLQ's own count", "VIEW", W_RECENT, None),
        ("not published", "read, CloudWatch returned nothing", "VIEW", W_RECENT, None),
        ("no DLQ point", "read, CloudWatch has no DLQ point", None, W_RECENT, None),
        ("no window read", "read, no CloudWatch window", None, W_RECENT, None),
        ("failures as the detail", "read, two failures", None, W_RECENT, None),
        ("dashboards, with the span history", "read, day and month", "VIEW", W_DASH, "SPANS"),
        ("dashboards, no near-DLQ history", "read, day and month", "VIEW", W_DASH, "SPANS_NO_NEAR"),
        ("windows asked for coarsest first", "read, day and month", None, W_MONTH_FIRST, "SPANS"),
        ("no windows asked for", "read, with SQS's counts", None, [], None),
        ("no account", "no account names it", "VIEW", W_RECENT, None),
        ("off", "AWS is off", None, W_DASH, None),
        ("failed", "the read failed", "VIEW", W_RECENT, None),
    ]
    named = {"VIEW": VIEW, "EMPTY_VIEW": EMPTY_VIEW, "SPANS": SPANS, "SPANS_NO_NEAR": SPANS_NO_NEAR}
    out["views"] = {"VIEW": VIEW, "EMPTY_VIEW": EMPTY_VIEW}
    out["spans"] = {"SPANS": SPANS, "SPANS_NO_NEAR": SPANS_NO_NEAR}
    for name, aws, view, windows, spans in live:
        out["liveOf"].append({"name": name, "aws": aws, "view": view, "windows": windows, "now": T0, "spans": spans,
                              "expected": af.live_of(AWS[aws], named.get(view), windows, T0, named.get(spans))})
    OUT.write_text(json.dumps(out, indent=1, ensure_ascii=False) + "\n")


if __name__ == "__main__":
    main()
