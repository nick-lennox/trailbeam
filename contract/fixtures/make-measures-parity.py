"""Writes measures-parity.json: the dashboard widget measure cases both engines must agree on (QV-632).

Self-hosted Trailbeam (server/src/trailbeam/server/measures.py, python/tests/server/test_measures.py) and the hosted
app (lib/dashboards/measures.ts, its test/measures-parity.test.ts, reading @trailbeam/ui/fixtures/measures-parity.json)
run every case and must give the expected result. The expected results are the Python engine's; the hosted app's test
is what holds measures.ts to them. Run from the repository root after changing a case or the engine:

    PYTHONPATH=python/src:server/src python contract/fixtures/make-measures-parity.py

and check the hosted app's test still passes on the new file before merging.

A dataset is the app's EventData on a workspace's own data (live: true, each event live: true), so the app's test passes
it to measures.ts as it is. In expected results, and in fmt's inputs, the strings "Infinity" and "NaN" stand for those
numbers, which JSON can't hold.
"""
import json
import math
from pathlib import Path

from trailbeam.server import measures as M

OUT = Path(__file__).resolve().parent / "measures-parity.json"
MIN = 60_000
H = 60 * MIN
DAY = 24 * H
NOW = 1_764_000_000_000  # 2025-11-24T16:00:00Z, ms


def plain(v):
    """Infinity and NaN as strings, so JSON holds them."""
    if isinstance(v, float) and not math.isfinite(v):
        return "NaN" if v != v else ("Infinity" if v > 0 else "-Infinity")
    if isinstance(v, dict):
        return {k: plain(x) for k, x in v.items()}
    if isinstance(v, (list, tuple)):
        return [plain(x) for x in v]
    return v


def unplain(v):
    return {"Infinity": math.inf, "-Infinity": -math.inf, "NaN": math.nan}.get(v, v) if isinstance(v, str) else v


# ── datasets ────────────────────────────────────────────────────────────────────────────────────────────────────

QUEUES = {  # queue: (producer, consumer)
    "orders": ("api", "orders-worker"),
    "billing-webhooks.fifo": ("billing", "billing-handler"),
    "email-dispatch": ("api", "mailer"),
    "invoices": ("billing", "invoicer"),
}
TENANTS = ["acme", "globex", None]


def events():
    """40 messages over the past hour (and 3 before it), sent, received (once to three times) and deleted, failed or
    dead-lettered, with tenants, sizes, attributes and some bodies; plus events at the range's two ends."""
    out = []
    names = list(QUEUES)

    def ev(key, ms, kind, q, mid, svc, **kw):
        e = {"key": key, "ms": ms, "event": kind, "status": "error" if kind in ("processing_error", "dead_letter") else "ok",
             "queue": q, "messageId": mid, "service": svc, "producer": QUEUES[q][0], "consumer": QUEUES[q][1],
             "detail": kw.pop("detail", f"{kind} {mid}"), "source": "sqs", "hasPage": False, "live": True}
        e.update({k: v for k, v in kw.items() if v is not None})
        out.append(e)

    for i in range(43):
        q = names[i % 4]
        mid = f"m-{i:02d}"
        tenant = TENANTS[i % 3]
        at = NOW - 58 * MIN + i * 80_000 - (3 * H if i >= 40 else 0)
        size = 200 + (i * 997) % 9000
        attrs = {"queue.region": "us-east-1" if i % 5 else "eu-west-1", "message.tenant": tenant}
        body = {"customer": f"cus_{tenant or 'none'}", "amount": 100 * i, "items": [{"sku": f"A-{i % 3}"}], "rush": i % 4 == 0}
        ev(f"{mid}/send", at, "send", q, mid, QUEUES[q][0], tenant=tenant, sizeBytes=size,
           attrs={k: v for k, v in attrs.items() if v is not None}, **({"body": body} if i % 2 == 0 else {}))
        receives = 1 + (i % 7 == 3) + (i % 11 == 5) * 2
        t = at
        for r in range(1, receives + 1):
            wait = 50 + (i * 131 + r * 17) % 4000
            t += wait
            ev(f"{mid}/receive/{r}", t, "receive", q, mid, QUEUES[q][1], tenant=tenant, receiveCount=r, waitMs=wait,
               sizeBytes=size, attrs={**{k: v for k, v in attrs.items() if v is not None}, "message.ApproximateReceiveCount": r})
            work = 5 + (i * 53 + r * 29) % 1500
            t += work
            if r < receives:
                ev(f"{mid}/error/{r}", t, "processing_error", q, mid, QUEUES[q][1], tenant=tenant, receiveCount=r,
                   durationMs=work, detail=f"TimeoutError in {QUEUES[q][1]}")
            elif i % 13 == 6:
                ev(f"{mid}/dlq", t, "dead_letter", q, mid, "sqs", tenant=tenant, receiveCount=r)
            else:
                ev(f"{mid}/delete", t, "delete", q, mid, QUEUES[q][1], tenant=tenant, receiveCount=r, durationMs=work)
    # the range's ends: from is in, to is out
    ev("edge/from", NOW - H, "send", "orders", "edge-from", "api", sizeBytes=10)
    ev("edge/to", NOW, "send", "orders", "edge-to", "api", sizeBytes=10)
    return sorted(out, key=lambda e: (-e["ms"], e["key"]))


def pts(f, to, step, fn):
    return [[t, fn(k)] for k, t in enumerate(range(f, to, step))]


def window(period_s, frm, step, *, visible, sent, deleted, oldest, dlq, stuck=None, near=None, in_flight=None):
    w = {"periodS": period_s, "fromMs": frm, "toMs": NOW,
         "visible": pts(frm, NOW, step, visible), "inFlight": pts(frm, NOW, step, in_flight or (lambda k: 4 + k % 5)),
         "oldestS": pts(frm, NOW, step, oldest), "sent": pts(frm, NOW, step, sent), "deleted": pts(frm, NOW, step, deleted),
         "dlq": pts(frm, NOW, step, dlq)}
    if stuck:
        w["stuck"] = pts(frm, NOW, step, stuck)
    if near:
        w["nearDlq"] = pts(frm, NOW, step, near)
    return w


def queue_live(**kw):
    q = {"status": "ok", "detail": None, "readAt": NOW, "visibilityTimeoutS": 30, "retentionS": 4 * 86_400, "delayS": 0,
         "maxReceiveCount": 3, "dlq": None, "fifo": False, "visible": 0, "inFlight": 0, "oldestS": 0, "dlqDepth": None,
         "oldestAsOf": NOW - MIN, "dlqAsOf": None, "oldestUnpublished": False, "dlqUnpublished": False, "windows": [],
         "stuck": 0, "nearDlq": {"count": 0, "inFlight": 0}}
    q.update(kw)
    return q


def own():
    orders_hour = window(60, NOW - 3 * H, 5 * MIN, visible=lambda k: 40 + (k * 7) % 90, sent=lambda k: 60 + k % 4,
                         deleted=lambda k: 70 + k % 6, oldest=lambda k: 30 * (k % 15), dlq=lambda k: k // 12)
    # the points out of order: the engines sort them by time where they need to
    orders_hour["sent"].reverse()
    orders_day = window(300, NOW - DAY, 30 * MIN, visible=lambda k: 30 + k * 2, sent=lambda k: 300 + k % 3,
                        deleted=lambda k: 310, oldest=lambda k: 60 * (k % 9), dlq=lambda k: min(2, k // 20),
                        stuck=lambda k: k % 3, near=lambda k: 12.5 * (k % 2))
    orders_week = window(3600, NOW - 15 * DAY, 6 * H, visible=lambda k: 10 + k, sent=lambda k: 3600, deleted=lambda k: 3700,
                         oldest=lambda k: 120, dlq=lambda k: 1)
    billing_day = window(300, NOW - DAY, 20 * MIN, visible=lambda k: 5 * k, sent=lambda k: 400, deleted=lambda k: 250,
                         oldest=lambda k: 10 * k, dlq=lambda k: 0, stuck=lambda k: 1 + k % 2, near=lambda k: 0)
    return {
        "live": True, "now": NOW, "capped": False, "queues": ["orders", "billing-webhooks.fifo", "email-dispatch", "invoices", "legacy"],
        "events": events(),
        "queueLive": {
            "orders": queue_live(dlq="orders-dlq", visible=120, inFlight=8, oldestS=420, dlqDepth=2, dlqAsOf=NOW - MIN,
                                 stuck=1, nearDlq={"count": 1, "inFlight": 8}, windows=[orders_hour, orders_day, orders_week]),
            # receives less than is sent: never drains; no redrive policy, a short visibility timeout, no retention read
            "billing-webhooks.fifo": queue_live(fifo=True, visibilityTimeoutS=5, retentionS=None, maxReceiveCount=None,
                                                visible=50, inFlight=3, oldestS=7200, stuck=2, nearDlq=None,
                                                windows=[billing_day]),
            # empty and with no CloudWatch history yet
            "email-dispatch": queue_live(visible=0, inFlight=0, oldestS=None, oldestUnpublished=True, stuck=None, nearDlq=None),
            "legacy": queue_live(status="failed", detail="AccessDenied: the role can't call sqs:GetQueueAttributes on legacy.",
                                 readAt=None, visible=None, inFlight=None, oldestS=None, windows=[]),
        },
    }


def datasets():
    d = own()
    ql = d["queueLive"]
    return {
        "own": d,
        # no queue in a connected account: queue measures have nothing; events (the newest 30 here) still count
        "none": {**d, "events": d["events"][:30], "queueLive": {}},
        # these two are about the queue figures only, so they carry no events
        "failed": {**d, "events": [], "queues": ["legacy"], "queueLive": {"legacy": ql["legacy"]}},
        # a failed read AWS gave no detail for
        "failedQuiet": {**d, "events": [], "queues": ["legacy"], "queueLive": {"legacy": {**ql["legacy"], "detail": None}}},
        "okOnly": {**d, "events": [], "queues": ["email-dispatch"], "queueLive": {"email-dispatch": ql["email-dispatch"]}},
        "empty": {"live": True, "now": NOW, "capped": False, "queues": [], "events": [], "queueLive": {}},
    }


# ── cases ───────────────────────────────────────────────────────────────────────────────────────────────────────

def rng(preset=None, frm=None, to=None):
    return M.preset(preset, NOW) if preset else {"from": frm, "to": to, "label": "custom"}


H1 = rng("1h")
SERIES = [  # name, measure, filter, groupBy, range, n, dataset
    *[(f"{m.id}: all of it", m.id, "", None, H1, 12, "own") for m in M.MEASURES],
    *[(f"{m.id}: by queue", m.id, "", "queue", H1, 6, "own") for m in M.MEASURES],
    *[(f"{m.id}: no AWS figures", m.id, "", None, H1, 4, "none") for m in M.MEASURES],
    *[(f"{m.id}: past 7 days", m.id, "", None, rng("7d"), 7, "own") for m in M.MEASURES],
    *[(f"{m.id}: past 15 minutes, orders and billing", m.id, "queue:orders,queue:billing* -queue:email*", None, rng("15m"), 3, "own")
      for m in M.MEASURES],
    ("sent by service", "sent", "", "service", H1, 6, "own"),
    ("events by tenant: none for no tenant", "events", "", "tenant", H1, 4, "own"),
    ("events by status", "events", "", "status", H1, 4, "own"),
    ("events by receive count", "events", "event:receive", "receive_count", H1, 4, "own"),
    ("events by a body path", "events", "event:send", "body.customer", H1, 2, "own"),
    ("events by the whole body", "events", "event:send queue:orders", "body", H1, 1, "own"),
    ("events by an attribute", "events", "", "queue.region", H1, 2, "own"),
    ("an unknown group-by field: one line", "events", "", "nope", H1, 2, "own"),
    ("an alias group-by field", "events", "event:delete", "duration", H1, 2, "own"),
    ("one bucket", "received", "", None, H1, 1, "own"),
    ("sixty buckets", "sent", "queue:orders", None, H1, 60, "own"),
    ("a custom range not on bucket edges", "deleted", "", None, rng(frm=NOW - 37 * MIN - 123, to=NOW - 5 * MIN + 7), 7, "own"),
    ("a range with nothing in it", "processing_time", "", None, rng(frm=NOW - 10 * DAY, to=NOW - 9 * DAY), 4, "own"),
    ("filter: tenant", "events", "tenant:acme", None, H1, 3, "own"),
    ("filter: two tenants are alternatives", "events", "tenant:acme tenant:globex", None, H1, 3, "own"),
    ("filter: not a tenant", "events", "-tenant:acme", None, H1, 3, "own"),
    ("filter: a wildcard", "events", "consumer:*worker", None, H1, 3, "own"),
    ("filter: any case", "events", "QUEUE:Orders", None, H1, 3, "own"),
    ("filter: receive count at least 2", "events", "receive_count:>=2", None, H1, 3, "own"),
    ("filter: an exact number", "events", "receive_count:2", None, H1, 3, "own"),
    ("filter: a duration range", "processing_time", "processing_time:100ms..1s", None, H1, 3, "own"),
    ("filter: a duration in seconds", "time_in_queue", "time_in_queue:>2s", None, H1, 3, "own"),
    ("filter: bytes", "body_size", "body_size:<1kb", None, H1, 3, "own"),
    ("filter: an alias field", "events", "size:>=8kb", None, H1, 3, "own"),
    ("filter: a body path", "events", "body.customer:cus_acme", None, H1, 3, "own"),
    ("filter: a body number", "events", "body.amount:>=2000", None, H1, 3, "own"),
    ("filter: a body array", "events", "body.items.sku:A-1", None, H1, 3, "own"),
    ("filter: a body boolean", "events", "body.rush:true", None, H1, 3, "own"),
    ("filter: body text anywhere", "events", "body:globex", None, H1, 3, "own"),
    ("filter: not in the body", "events", "-body.customer:cus_acme", None, H1, 3, "own"),
    ("filter: an attribute", "events", "queue.region:eu-*", None, H1, 3, "own"),
    ("filter: a numeric attribute", "events", "message.ApproximateReceiveCount:>1", None, H1, 3, "own"),
    ("filter: bare text matches the detail", "events", "timeouterror", None, H1, 3, "own"),
    ("filter: bare text matches a message ID", "events", "m-1", None, H1, 3, "own"),
    ("filter: quoted text", "events", '"TimeoutError in billing"', None, H1, 3, "own"),
    ("filter: an unknown field is left out", "events", "queu:orders", None, H1, 3, "own"),
    ("filter: a bad number is left out", "events", "receive_count:many queue:orders", None, H1, 3, "own"),
    ("filter: handler_error is processing_error", "events", "event:handler_error", None, H1, 3, "own"),
    ("filter: a term still being typed", "events", "tenant:", None, H1, 3, "own"),
    ("filter: a sample-only field matches nothing here", "events", "queue.FifoQueue:true", None, H1, 3, "own"),
    ("filter: matches nothing", "sent", "queue:nowhere", "queue", H1, 3, "own"),
    ("queue measure: a filter on a tenant does not narrow it", "messages_visible", "tenant:acme", None, H1, 4, "own"),
    ("queue measure: one queue", "oldest_age", "queue:orders", "queue", H1, 4, "own"),
    ("queue measure: past 1 day takes the day window", "stuck_in_flight", "", "queue", rng("1d"), 4, "own"),
    ("queue measure: past 4 hours goes back past the hour window", "messages_visible", "", "queue", rng("4h"), 4, "own"),
    ("queue measure: past 30 days, longer than every window", "dlq_depth", "", "queue", rng("30d"), 5, "own"),
    ("queue measure: a queue whose figures failed", "messages_visible", "queue:legacy", None, H1, 2, "own"),
    ("queue measure: only a queue with no history", "messages_in_flight", "", None, H1, 3, "okOnly"),
    ("queue measure: only a failed queue", "messages_visible", "", None, H1, 3, "failed"),
    ("events: no events at all", "sent", "", "queue", H1, 3, "empty"),
    ("queue measure: no queues at all", "messages_visible", "", None, H1, 3, "empty"),
]

NAMES = ["orders", "Orders-DLQ", "billing-webhooks.fifo", "email-dispatch", "a.b+c"]
QUEUES_FOR = [
    "", "queue:orders", "queue:ORDERS", "queue:orders*", "-queue:orders*", "queue:*.fifo", "queue:a.b+c", "queue:a.b*",
    "queue:orders queue:email-dispatch", "queue:*-* -queue:*dlq", "tenant:acme", "queue:", "-queue:", "queu:orders",
    'queue:"orders"', "queue:*",
]
EFFECTIVE = [  # name, widget, variables
    ("no filter, no variables", {}, []),
    ("a variable in the filter", {"filter": "queue:$queue"}, [{"name": "queue", "field": "queue", "value": "orders"}]),
    ("a variable set to any", {"filter": "queue:$queue event:send"}, [{"name": "queue", "field": "queue", "value": "*"}]),
    ("a value with a space is quoted", {"filter": "tenant:$t"}, [{"name": "t", "field": "tenant", "value": "acme corp"}]),
    ("a value with a bracket is quoted", {"filter": "tenant:$t"}, [{"name": "t", "field": "tenant", "value": "a(b)"}]),
    ("an unknown variable stays", {"filter": "queue:$nope  event:send"}, [{"name": "queue", "field": "queue", "value": "orders"}]),
    ("following adds the others", {"filter": "event:send"},
     [{"name": "queue", "field": "queue", "value": "orders"}, {"name": "t", "field": "tenant", "value": "*"},
      {"name": "svc", "field": "service", "value": "billing api"}]),
    ("pinned: no others", {"filter": "event:send", "follow": False}, [{"name": "queue", "field": "queue", "value": "orders"}]),
    ("pinned still fills its own", {"filter": "queue:$queue", "follow": False}, [{"name": "queue", "field": "queue", "value": "orders"}]),
    ("a used variable is not added again", {"filter": "-queue:$queue"}, [{"name": "queue", "field": "queue", "value": "orders"}]),
    ("a variable name with dots", {"filter": "body.customer:$c.id"}, [{"name": "c.id", "field": "body.customer", "value": "cus_1"}]),
    ("a bare variable", {"filter": "$q"}, [{"name": "q", "field": "queue", "value": "orders"}]),
    ("spaces are tidied", {"filter": "  event:send   queue:$q  "}, [{"name": "q", "field": "queue", "value": "*"}]),
    ("the same variable twice", {"filter": "queue:$q -queue:$q-dlq"}, [{"name": "q", "field": "queue", "value": "orders"}]),
]
FMT = [
    *[(v, "count") for v in [0, 1, 0.4, 0.5, 1.5, 2.5, 999.5, 1234, 9999.4, 9999.5, 10_000, 12_345, 99_950, 100_000, 1_234_567, -3]],
    *[(v, "rate") for v in [0, 0.16666666666666666, 11 / 60, 59.5, 25_000]],
    *[(v, "s") for v in [0, 0.05, 0.25, 4, 5.55, 9.96, 10, 59.4, 59.6, 60, 90, 312, 3600, 3660, 13_260, 86_400, 2.5, 1e-7]],
    *[(v, "ms") for v in [0, 0.5, 12.4, 999.4, 999.6, 1000, 1500, 65_000, 3_725_000]],
    *[(v, "pct") for v in [0, 0.04, 0.05, 0.25, 0.35, 5, 9.95, 9.96, 10, 12.5, 99.5, 250]],
    *[(v, "bytes") for v in [0, 512.5, 1023, 1024, 1536, 1_048_576, 3.75 * 1024 ** 2, 1024 ** 3, 2.555 * 1024 ** 3]],
    *[("Infinity", u) for u in ["count", "s", "ms", "pct", "bytes"]],
]
P95 = [[], [5], list(range(1, 101)), [(i + 1) * 100 for i in range(10)], [3, 1, 2], [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21],
       [2.5, 0.5, 1.5, 7.25]]
PARSE = ["", "queue:orders", "-queue:orders", "-", "queu:orders", "xyzzy:1", "receive_count:>=2", "receive_count:lots",
         "processing_time:1s..5s", "event:HANDLER_ERROR", 'tenant:"acme corp" text', '"unclosed quote', "body.a.b:1",
         "Body[0].x:y", "queue.FifoQueue:true", "size:<1kb", "a:b:c", ":x", "  two   spaces  "]


def widget(**kw):
    w = {"id": kw.pop("id", "w1"), "type": "number", "title": "A widget", "layout": {"x": 0, "y": 0, "w": 3, "h": 2}}
    w.update(kw)
    return w


PROBLEM = [
    ("fine", widget()),
    ("not an object", "a widget"),
    ("null", None),
    ("a list", []),
    ("no type", {"title": "x"}),
    ("an unknown type", widget(type="pie")),
    ("a title that is not text", widget(title=5)),
    ("an unknown measure", widget(queries=[{"measure": "nope"}])),
    ("a query with no measure", widget(queries=[{}])),
    ("an unknown group_by", widget(queries=[{"measure": "sent", "group_by": "colour"}])),
    ("a body group_by is a field", widget(queries=[{"measure": "sent", "group_by": "body.customer"}])),
    ("a bad filter", widget(filter="queu:orders")),
    ("a bad filter with a variable is let through", widget(filter="$x:orders")),
    ("a bad number in the filter", widget(filter="receive_count:many")),
    ("event_list columns", widget(type="event_list", columns=["date", "event", "detail", "body", "queue", "body.a"])),
    ("an unknown event_list column", widget(type="event_list", columns=["date", "colour"])),
    ("queue_table columns", widget(type="queue_table", columns=["messages_visible", "near_dlq"])),
    ("an unknown queue_table column", widget(type="queue_table", columns=["messages_visible", "sent_count"])),
    ("an unknown field", widget(type="distribution", field="colour")),
    ("a known field", widget(type="distribution", field="tenant")),
    ("no layout", {"type": "note", "title": "Hi"}),
    ("a layout with text", widget(layout={"x": 0, "y": "1", "w": 3, "h": 2})),
    ("a layout missing h", widget(layout={"x": 0, "y": 0, "w": 3})),
]
CHECK_DEF = [
    ("fine", {"variables": [{"name": "q", "field": "queue", "value": "*"}], "sections": [{"id": "s1", "title": "Queues"}],
              "widgets": [widget(), widget(id="w2", type="note", text="hello")]}),
    ("variables and sections may be left out", {"widgets": []}),
    ("variables null", {"widgets": [], "variables": None}),
    ("not an object", ["widgets"]),
    ("no widgets", {"variables": []}),
    ("widgets not a list", {"widgets": {"a": 1}}),
    ("variables not a list", {"widgets": [], "variables": {}}),
    ("sections not a list", {"widgets": [], "sections": "s"}),
    ("a variable with no value", {"widgets": [], "variables": [{"name": "q", "field": "queue"}]}),
    ("a variable that is not an object", {"widgets": [], "variables": ["q"]}),
    ("a section with no title", {"widgets": [], "sections": [{"id": "s1"}]}),
    ("a widget with a problem", {"widgets": [widget(), widget(type="pie")]}),
    ("a widget with no id", {"widgets": [{k: v for k, v in widget().items() if k != "id"}]}),
    ("a widget with a number id", {"widgets": [widget(id=7)]}),
    ("other keys are dropped", {"widgets": [], "name": "Orders", "extra": True}),
]
SPAN_DEFS = [
    ("no span measure", {"variables": [], "widgets": [widget(queries=[{"measure": "messages_visible"}])]}),
    ("a span measure, all queues", {"variables": [], "widgets": [widget(queries=[{"measure": "stuck_in_flight"}])]}),
    ("variables are set to any", {"variables": [{"name": "q", "field": "queue", "value": "orders"}],
                                  "widgets": [widget(filter="queue:$q", queries=[{"measure": "near_dlq"}]),
                                              widget(id="w2", filter="queue:*.fifo", queries=[{"measure": "stuck_in_flight"}])]}),
    ("a queue table with default columns has none", {"variables": [], "widgets": [widget(type="queue_table")]}),
    ("a queue table with a span column", {"variables": [], "widgets": [widget(type="queue_table", filter="-queue:orders",
                                                                               columns=["messages_visible", "stuck_in_flight"])]}),
    ("pinned to one queue", {"variables": [{"name": "q", "field": "queue", "value": "billing*"}],
                             "widgets": [widget(filter="queue:email-dispatch", follow=False, queries=[{"measure": "stuck_in_flight"}])]}),
]
ALERT_VARS = [{"name": "queue", "field": "queue", "value": "*"}]
ALERTS = [  # name, widget, variables, dataset
    ("no rule", widget(queries=[{"measure": "messages_visible"}]), [], "own"),
    ("a rule with no limit", widget(queries=[{"measure": "messages_visible"}], rule={}), [], "own"),
    ("a rule on a widget type that has none", widget(type="toplist", queries=[{"measure": "messages_visible"}], rule={"crit": 1}), [], "own"),
    ("no measure", widget(rule={"crit": 1}), [], "own"),
    ("an unknown measure", widget(queries=[{"measure": "nope"}], rule={"crit": 1}), [], "own"),
    ("depth, critical, the worst queue named", widget(queries=[{"measure": "messages_visible"}], rule={"warn": 50, "crit": 150}), ALERT_VARS, "own"),
    ("depth, warning", widget(queries=[{"measure": "messages_visible"}], rule={"warn": 50, "crit": 500}), [], "own"),
    ("depth, good", widget(queries=[{"measure": "messages_visible"}], rule={"warn": 500}), [], "own"),
    ("time to drain growing", widget(type="timeseries", queries=[{"measure": "time_to_drain"}], rule={"crit": 3600}), [], "own"),
    ("time to drain on one queue", widget(queries=[{"measure": "time_to_drain"}], filter="queue:orders", rule={"warn": 1}), [], "own"),
    ("a variable narrows it", widget(queries=[{"measure": "oldest_age"}], filter="queue:$queue", rule={"crit": 600}),
     [{"name": "queue", "field": "queue", "value": "orders"}], "own"),
    ("the queues it matches have no figure (QV-357)", widget(queries=[{"measure": "dlq_depth"}], filter="queue:billing*", rule={"crit": 0}), [], "own"),
    ("a queue with no AWS figures", widget(queries=[{"measure": "messages_visible"}], filter="queue:invoices", rule={"crit": 0}), [], "own"),
    ("no queue has figures", widget(queries=[{"measure": "messages_visible"}], rule={"crit": 0}), [], "none"),
    ("retention headroom in percent", widget(queries=[{"measure": "retention_headroom"}], rule={"warn": 0.1, "crit": 50}), [], "own"),
    ("visibility headroom", widget(queries=[{"measure": "visibility_headroom"}], rule={"warn": 50, "crit": 90}), [], "own"),
    ("below: sends dropped", widget(type="timeseries", queries=[{"measure": "sent"}], rule={"warn": 1, "crit": 0.1, "below": True}), [], "own"),
    ("below, good", widget(queries=[{"measure": "sent"}], rule={"warn": 0.01, "below": True}), [], "own"),
    ("an event measure names no queue", widget(queries=[{"measure": "processing_errors"}], rule={"crit": 2}), [], "own"),
    ("processing time in ms", widget(queries=[{"measure": "processing_time"}], rule={"warn": 500, "crit": 1200}), [], "own"),
    ("its own time range", widget(queries=[{"measure": "events"}], time="15m", rule={"warn": 10}), [], "own"),
    ("an unknown time range is the past hour", widget(queries=[{"measure": "events"}], time="3w", rule={"warn": 10}), [], "own"),
    ("crit only, under", widget(queries=[{"measure": "dead_lettered"}], rule={"crit": 100}), [], "own"),
    ("body size in bytes", widget(queries=[{"measure": "body_size"}], rule={"warn": 2048}), [], "own"),
    ("no events", widget(queries=[{"measure": "events"}], rule={"warn": 0}), [], "empty"),
]


def main():
    ds = datasets()
    own_ = ds["own"]
    time_1h, time_7d = M.preset("1h", NOW), M.preset("7d", NOW)
    f = {
        "about": "Dashboard widget measure cases both engines must agree on (QV-632). Written by make-measures-parity.py; see that file.",
        "version": 1,
        "datasets": ds,
        "measures": [{"id": m.id, "label": m.label, "col": m.col, "ask": m.ask, "unit": m.unit, "agg": m.agg, "queue": m.queue,
                      "spans": m.spans, "fold": (m.ev or {}).get("fold")} for m in M.MEASURES],
        "groupFields": M.GROUP_FIELDS,
        "numberFields": M.NUMBER_FIELDS,
        "series": [{"name": n, "measure": m, "filter": fl, "groupBy": g, "time": t, "n": k, "data": d,
                    "expected": plain(M.series_for(M.measure(m), fl, g, t, k, ds[d]))} for n, m, fl, g, t, k, d in SERIES],
        "queueValue": [{"measure": m.id, "queue": q, "time": t, "data": "own",
                        "expected": plain(M.queue_value(m, q, own_, t))}
                       for m in M.MEASURES if m.queue for q in own_["queues"] for t in (time_1h, time_7d)],
        "depthMissing": [{"measure": m.id, "data": d, "expected": M.depth_missing(m, ds[d])} for m in M.MEASURES for d in ds],
        "noHistory": [{"measure": m.id, "data": d, "time": t, "expected": M.no_history(m, ds[d], t)}
                      for m in M.MEASURES for d in ("own", "okOnly", "none") for t in (time_1h, M.preset("1d", NOW), time_7d)],
        "noDepth": [{"data": d, "expected": M.no_depth(ds[d])} for d in ds],
        "queueTone": [{"queue": q, "data": "own", "expected": M.queue_tone(q, own_)} for q in own_["queues"]],
        "spanQueues": [{"name": n, "def": d, "names": NAMES, "expected": M.span_queues(d, NAMES)} for n, d in SPAN_DEFS],
        "queuesFor": [{"filter": fl, "names": NAMES, "expected": M.queues_for(M.parse(fl), NAMES)} for fl in QUEUES_FOR],
        "queueOnlyTerms": [{"filter": fl, "expected": M.queue_only_terms(fl)}
                           for fl in ["", "queue:orders", "queue:orders tenant:acme -service:api", "queu:x text", "  "]],
        "effectiveFilter": [{"name": n, "widget": w, "variables": v, "expected": M.effective_filter(w, v)} for n, w, v in EFFECTIVE],
        "parse": [{"query": q, "expected": M.parse(q)} for q in PARSE],
        "fmt": [{"value": v, "unit": u, "expected": M.fmt(unplain(v), u)} for v, u in FMT],
        "p95Of": [{"values": xs, "expected": M.p95_of(xs)} for xs in P95],
        "eventBuckets": [{"filter": fl, "time": t, "n": k, "data": "own", "expected": M.event_buckets(fl, t, k, own_)}
                         for fl, t, k in [("", time_1h, 6), ("status:error", time_1h, 4), ("queue:orders", rng("15m"), 5),
                                          ("", rng(frm=NOW - 61 * MIN, to=NOW + MIN), 1)]],
        "eventsFor": [{"filter": fl, "time": time_1h, "data": "own", "expected": [e["key"] for e in M.events_for(fl, time_1h, own_)]}
                      for fl in ["event:dead_letter", "receive_count:3", "body.amount:>3000 -tenant:globex"]],
        "alertOf": [{"name": n, "widget": w, "variables": v, "data": d, "expected": M.alert_of(w, v, ds[d])} for n, w, v, d in ALERTS],
        "problem": [{"name": n, "widget": w, "expected": M.problem(w)} for n, w in PROBLEM],
        "checkDef": [{"name": n, "def": d, "expected": M.check_def(d)} for n, d in CHECK_DEF],
    }
    OUT.write_text(json.dumps(f, indent=1) + "\n")


if __name__ == "__main__":
    main()
