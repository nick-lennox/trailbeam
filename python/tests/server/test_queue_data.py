"""AWS figures in the queue APIs (queue_data.py, QV-589 part 3): /api/queues/{queue}/now and /series carry what AWS
says about the queue, "In ‹queue› now" goes by the queue's own settings and SQS's counts when they were read, and the
dashboards' queueLive has the queue's figures. With reads off nothing changes and no AWS client is made."""
from __future__ import annotations

import json

import pytest
from spans import SEC, T0, Consumer
from test_aws_reader import Stubs as _Stubs, never
from trailbeam.server import aws_api, aws_reader, aws_settings, dashboards_api, queue_data
from trailbeam.server.access import Who
from trailbeam.server.server import ApiError, App

MS = T0 // 1_000_000
ACCOUNT = "000000000000"  # the spans' queue URL: http://localhost:9324/000000000000/orders
DLQ_ARN = f"arn:aws:sqs:eu-west-1:{ACCOUNT}:orders-dlq"
ORDERS = {"VisibilityTimeout": "45", "MessageRetentionPeriod": "86400", "ApproximateNumberOfMessages": "7",
          "ApproximateNumberOfMessagesNotVisible": "4", "ApproximateNumberOfMessagesDelayed": "0",
          "RedrivePolicy": json.dumps({"deadLetterTargetArn": DLQ_ARN, "maxReceiveCount": 5})}


def Stubs(**kw):
    # the credentials are in the emulator's account, so CloudWatch answers for its queues (QV-741)
    return _Stubs(account=ACCOUNT, **kw)


def server_aws(stubs=None, queues=("orders",), enabled=True):
    a = aws_api.ServerAws(aws_settings.MemorySettings(), lambda who: True, now_ms=lambda: MS,
                          clients_for=stubs or never, installed=lambda: True, source=lambda r: ("env", r))
    if queues:
        # an emulator's account, as its queue URLs name it (QV-740: the credentials' own would be another)
        a.settings.save({"enabled": enabled, "queues": list(queues), "region": "eu-west-1", "accountId": ACCOUNT},
                        "test", MS)
    return a


def app_of(db, at_ns, aws):
    return App(db, now=lambda: at_ns, max_receive_counts={"orders": 3}, aws=aws, ui_dir=None)


def sent_and_received(ingest):
    api, worker = Consumer("api", service="orders-api"), Consumer("w")
    ingest(api.send(T0, "m1"), api.send(T0, "m2"), service="orders-api")
    ingest(worker.receive(T0 + 2 * SEC, "m2", "h2"))


# ── reads off ────────────────────────────────────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("aws", [None, "off", "unnamed"])
def test_with_reads_off_now_and_series_are_from_spans_and_no_client_is_made(db, ingest, aws):
    sent_and_received(ingest)
    a = None if aws is None else server_aws(queues=() if aws == "unnamed" else ("orders",), enabled=aws != "off")
    app = app_of(db, T0 + 12 * SEC, a)
    d = app.api("queues/orders/now", {"minutes": "60"})
    assert d["aws"]["status"] == "not_configured" and d["aws"]["detail"] == aws_reader.OFF
    assert d["live"]["status"] == "not_configured" and d["live"]["windows"] == []
    assert d["totals"] is None and d["uninstrumented"] is None
    assert d["config"]["visibilityKnown"] is False and d["config"]["maxReceiveCount"] == 3
    two = next(m for m in d["held"] if m["id"] == "m2")
    assert two["timeoutS"] == 30  # SQS's default
    s = app.api("queues/orders/series", {})
    assert s["aws"]["status"] == "not_configured" and s["max_receive_count"] == 3


def test_a_queue_the_settings_do_not_name_keeps_the_servers_settings(db, ingest):
    sent_and_received(ingest)
    stubs = Stubs(attrs={"orders": ORDERS})
    d = app_of(db, T0 + 12 * SEC, server_aws(stubs, queues=("billing-*",))).api("queues/orders/now", {})
    assert d["aws"]["status"] == "no_account" and d["aws"]["detail"] == aws_reader.NOT_NAMED
    assert d["totals"] is None and d["config"]["maxReceiveCount"] == 3
    assert stubs.regions == []  # nothing read


# ── only queues the store has seen (QV-740) ──────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("asked", ["999999999999%2Fap-south-1%2Fbilling-secrets", "billing-secrets",
                                   "https%3A%2F%2Fsqs.ap-south-1.amazonaws.com%2Fbilling-secrets"])
def test_a_queue_the_store_has_not_seen_is_not_read_whatever_the_settings_name(db, ingest, asked):
    """The report's step 2: any member can put any key in the page's URL. With every name allowed and no account
    set, a key without spans is still not read with the server's credentials."""
    sent_and_received(ingest)
    stubs = Stubs(attrs={"orders": ORDERS})
    app = app_of(db, T0 + 12 * SEC, server_aws(stubs, queues=("*",)))
    for endpoint in ("now", "series"):
        a = app.api(f"queues/{asked}/{endpoint}", {"minutes": "60"})["aws"]
        assert a["status"] == "no_account" and a["detail"] == aws_reader.NOT_SEEN and a["settings"] is None
    assert stubs.regions == [] and stubs.aws_calls() == 0 and stubs.sts.calls == 0
    # a measure naming it is not read either
    got = queue_data.live_by_name(app.aws.reader, db, ["billing-secrets", "orders"], MS + 12_000, 900, {})
    assert got["billing-secrets"]["status"] == "no_account" and got["orders"]["status"] == "ok"
    assert not any("billing-secrets" in u for u in stubs.sqs.calls)


# ── reads on ─────────────────────────────────────────────────────────────────────────────────────────────────────

def test_now_goes_by_the_queues_own_settings_and_sqs_counts(db, ingest):
    sent_and_received(ingest)
    stubs = Stubs(attrs={"orders": ORDERS})
    d = app_of(db, T0 + 12 * SEC, server_aws(stubs)).api("queues/orders/now", {})
    assert d["aws"]["status"] == "ok" and d["aws"]["settings"]["dlqName"] == "orders-dlq"
    assert d["aws"]["triggers"][0]["functionName"] == "ship"
    c = d["config"]
    assert (c["visibilityTimeoutS"], c["retentionS"], c["maxReceiveCount"], c["dlq"]) == (45, 86400, 5, "orders-dlq")
    assert c["visibilityKnown"] is True and c["arn"].endswith(":orders")
    two = next(m for m in d["held"] if m["id"] == "m2")
    assert two["timeoutS"] == 45 and two["untilMs"] == MS + 47_000  # the queue's timeout, from the receive at 2 s
    assert d["instrumented"] == {"visible": 1, "inFlight": 1}
    assert d["totals"] == {"visible": 7, "inFlight": 4, "delayed": 0}
    assert d["uninstrumented"] == {"visible": 6, "inFlight": 3, "over": False}
    live = d["live"]
    assert live["status"] == "ok" and live["visible"] == 7 and live["inFlight"] == 4 and live["maxReceiveCount"] == 5
    assert live["windows"] == [] and stubs.cw.calls == []  # no minutes: CloudWatch is not read
    assert live["nearDlq"] == {"count": 0, "inFlight": 1} and live["stuck"] == 0


def test_now_with_minutes_reads_cloudwatch_over_the_overviews_range(db, ingest):
    sent_and_received(ingest)
    stubs = Stubs(attrs={"orders": ORDERS})
    d = app_of(db, T0 + 12 * SEC, server_aws(stubs)).api("queues/orders/now", {"minutes": "15"})
    w = d["aws"]["series"][queue_data.OVERVIEW_WINDOW]
    assert w["periodS"] == 60 and w["toMs"] - w["fromMs"] >= 60 * 60_000  # at least an hour, as the Overview reads
    assert [v for _, v in w["queue"]["visible"]] == [3.0] and len(stubs.cw.calls) == 1
    assert d["live"]["windows"][0]["periodS"] == 60 and d["live"]["oldestS"] == 3.0


def test_the_queues_retention_period_lets_older_messages_go(db, ingest):
    sent_and_received(ingest)
    short = {**ORDERS, "MessageRetentionPeriod": "60"}
    d = app_of(db, T0 + 90 * SEC, server_aws(Stubs(attrs={"orders": short}))).api("queues/orders/now", {})
    assert d["held"] == [] and d["uninstrumented"]["visible"] == 7


def test_more_instrumented_than_sqs_holds_says_so(db, ingest):
    sent_and_received(ingest)
    few = {**ORDERS, "ApproximateNumberOfMessages": "0", "ApproximateNumberOfMessagesNotVisible": "0"}
    d = app_of(db, T0 + 12 * SEC, server_aws(Stubs(attrs={"orders": few}))).api("queues/orders/now", {})
    assert d["uninstrumented"] == {"visible": 0, "inFlight": 0, "over": True}


def test_a_queue_without_a_redrive_policy_has_no_max_receive_count_whatever_the_server_says(db, ingest):
    sent_and_received(ingest)
    plain = {k: v for k, v in ORDERS.items() if k != "RedrivePolicy"}
    app = app_of(db, T0 + 12 * SEC, server_aws(Stubs(attrs={"orders": plain})))
    assert app.api("queues/orders/now", {})["config"]["maxReceiveCount"] is None
    assert app.api("queues/orders/series", {})["max_receive_count"] is None


def test_series_carries_cloudwatch_over_the_pages_range_at_its_step(db, ingest):
    sent_and_received(ingest)
    stubs = Stubs(attrs={"orders": ORDERS})
    s = app_of(db, T0 + 12 * SEC, server_aws(stubs)).api(
        "queues/orders/series", {"from_ms": str(MS - 3_600_000), "to_ms": str(MS + 60_000), "step_s": "300"})
    a = s["aws"]
    assert a["status"] == "ok" and s["max_receive_count"] == 5
    w = a["series"][queue_data.PAGE_WINDOW]
    assert w["periodS"] == 300 and w["fromMs"] % 300_000 == 0
    assert a["triggers"] and stubs.lam.calls


# ── the Overview's one request, and what a person can make the server read (QV-743) ─────────────────────────────

def test_the_overview_asks_for_its_queues_at_once_and_viewers_share_the_read(db, ingest):
    sent_and_received(ingest)
    stubs = Stubs(attrs={"orders": ORDERS})
    app = app_of(db, T0 + 12 * SEC, server_aws(stubs))
    one = app.api("queues/orders/now", {"minutes": "60"})
    stubs.cw.calls.clear()
    app.aws.reader.cache.clear()
    d = app.api("queues/now", {"keys": "orders,never-seen", "minutes": "60"}, Who("session", "member", 7))
    assert d["minutes"] == 60 and d["errors"] == {} and set(d["queues"]) == {"orders", "never-seen"}
    o = d["queues"]["orders"]
    # the same answer as /now's, but CloudWatch only for what the Overview's health uses, and the DLQ's depth
    assert o["queue"] == one["queue"] and o["totals"] == one["totals"] and o["held"] == one["held"]
    assert o["live"]["visible"] == 7 and o["live"]["oldestS"] == 3.0 and o["live"]["dlq"] == "orders-dlq"
    assert len(stubs.cw.calls) == 1 and stubs.cw.calls[0][2] == len(queue_data.OVERVIEW_METRICS) + 1
    assert d["queues"]["never-seen"]["aws"]["detail"] == aws_reader.NOT_SEEN
    # another viewer within the minute: the kept read
    app.api("queues/now", {"keys": "never-seen,orders", "minutes": "60"}, Who("session", "member", 8))
    assert len(stubs.cw.calls) == 1


def test_the_batch_has_a_limit_and_no_keys_is_no_queues(db, ingest):
    sent_and_received(ingest)
    app = app_of(db, T0 + 12 * SEC, server_aws(Stubs(attrs={"orders": ORDERS})))
    assert app.api("queues/now", {"keys": " , "}) == {"minutes": None, "queues": {}, "errors": {}}
    many = ",".join(f"q{i}" for i in range(queue_data.NOW_BATCH_MAX + 1))
    with pytest.raises(ApiError) as e:
        app.api("queues/now", {"keys": many})
    assert e.value.status == 400 and str(queue_data.NOW_BATCH_MAX) in e.value.message
    d = app.api("queues/now", {"keys": "orders"})
    assert d["minutes"] is None and d["queues"]["orders"]["live"]["windows"] == []


def test_a_member_varying_the_series_range_is_stopped_at_the_budget(db, ingest):
    """The report's second finding: every range is a new billed read, so a member could spend without limit."""
    sent_and_received(ingest)
    stubs = Stubs(attrs={"orders": ORDERS})
    app = app_of(db, T0 + 12 * SEC, server_aws(stubs))
    app.aws.reader.budget = aws_reader.Budget(limit=20)  # 6 metrics a read: three reads
    member = Who("session", "member", 7)

    def series(i, who=member):
        return app.api("queues/orders/series", {"from_ms": str(MS - 3_600_000 - i * 60_000), "to_ms": str(MS)},
                       who)["aws"]

    assert [series(i)["errors"] for i in range(3)] == [[], [], []]
    over = series(3)
    assert over["status"] == "ok" and over["settings"] is not None and over["series"] == {}
    assert len(over["errors"]) == 1 and "new CloudWatch metrics" in over["errors"][0]
    assert len(stubs.cw.calls) == 3
    assert series(3, Who("session", "member", 8))["errors"] == []  # someone else's budget
    assert series(4, Who("shared_token"))["errors"] == []  # the shared token: one budget for all who use it
    assert len(stubs.cw.calls) == 5


def test_a_failed_read_is_said_and_now_falls_back_to_spans(db, ingest):
    from botocore.exceptions import NoCredentialsError
    sent_and_received(ingest)
    d = app_of(db, T0 + 12 * SEC, server_aws(Stubs(fail={"orders": NoCredentialsError}))).api("queues/orders/now", {})
    assert d["aws"]["status"] == "failed" and d["aws"]["detail"] == aws_reader.NO_CREDENTIALS
    assert d["live"]["detail"] == aws_reader.NO_CREDENTIALS
    assert d["totals"] is None and d["config"]["maxReceiveCount"] == 3


# ── the dashboards' queueLive ────────────────────────────────────────────────────────────────────────────────────

def test_dashboards_queue_live_is_the_queues_figures_with_reads_on(db, ingest):
    sent_and_received(ingest)
    stubs = Stubs(attrs={"orders": ORDERS})
    app = app_of(db, T0 + 12 * SEC, server_aws(stubs))
    api = dashboards_api.for_app(app, env={"TRAILBEAM_DASHBOARDS": "1"})
    d = api.data(MS - 3_600_000, MS + 12_000, None)
    q = d["queueLive"]["orders"]
    assert q["status"] == "ok" and q["visible"] == 7 and q["stuck"] == 0
    assert sorted(w["periodS"] for w in q["windows"]) == [60, 3600]  # a day and 30 days
    assert stubs.lam.calls == []  # no triggers for the dashboards
    assert api.no_depth_text() is None


def test_dashboards_queue_live_with_reads_off_reads_nothing(db, ingest):
    sent_and_received(ingest)
    app = app_of(db, T0 + 12 * SEC, server_aws(queues=()))
    api = dashboards_api.for_app(app, env={"TRAILBEAM_DASHBOARDS": "1"})
    q = api.data(MS - 3_600_000, MS + 12_000, None)["queueLive"]["orders"]
    assert q == {"status": "not_configured", "detail": dashboards_api.NOT_QUEUE_LIVE}
    assert api.no_depth_text() == dashboards_api.NOT_QUEUE_LIVE


def test_live_by_name_works_out_in_queue_only_for_queues_aws_read(db, ingest):
    sent_and_received(ingest)
    a = server_aws(Stubs(attrs={"orders": ORDERS}), queues=("orders",))
    got = queue_data.live_by_name(a.reader, db, ["orders", "billing"], MS + 12_000, 900, {})
    assert got["orders"]["status"] == "ok" and got["orders"]["stuck"] == 0
    assert got["billing"]["status"] == "no_account" and got["billing"]["stuck"] is None
    off = queue_data.live_by_name(a.reader, db, ["orders"], MS + 12_000, 900, {}, in_queue=False)
    assert off["orders"]["stuck"] is None
