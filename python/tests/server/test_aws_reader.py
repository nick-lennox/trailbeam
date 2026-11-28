"""The AWS reader (aws_reader.py, QV-589): which queues are read and why the others are not, the reads against moto's
SQS and CloudWatch, and the short-lived cache against counting stubs (billed GetMetricData is read at most once a
minute, a failed read is not kept, two pages that miss together make one read). No AWS account is used."""
import datetime as dt
import json
import threading

import moto
import pytest
from botocore.exceptions import ClientError, NoCredentialsError

from trailbeam.server import aws_reader, aws_reads
from trailbeam.server.aws_reader import AwsReader, Cache, Status

REGION = "eu-west-1"
ACCOUNT = "123456789012"
T0 = 1_764_000_000_000
MIN = 60_000
ON = {"enabled": True, "queues": ["orders", "billing-*"], "region": None, "accountId": None}


def ref(name, key=None, account=ACCOUNT, region=REGION, url=True):
    return {"key": key or name, "name": name, "account": account, "region": region,
            "url": f"https://sqs.{region}.amazonaws.com/{account}/{name}" if url else None}


def window(id_="1h", to=T0, span=60 * MIN, period=60):
    return {"id": id_, "fromMs": to - span, "toMs": to, "periodS": period}


def never(region):
    raise AssertionError("no AWS client may be made")


# ── stubs that count ────────────────────────────────────────────────────────────────────────────────────────────

def denied(op):
    return ClientError({"Error": {"Code": "AccessDenied", "Message": "no"}}, op)


class Sqs:
    def __init__(self, attrs=None, fail=None):
        self.calls, self.attrs, self.fail = [], attrs or {}, fail or {}

    def get_queue_attributes(self, QueueUrl, AttributeNames):
        self.calls.append(QueueUrl)
        name = QueueUrl.rsplit("/", 1)[1]
        if name in self.fail:
            e = self.fail[name]
            raise e() if isinstance(e, type) else e
        return {"Attributes": {"QueueArn": f"arn:aws:sqs:{REGION}:{ACCOUNT}:{name}", "VisibilityTimeout": "30",
                               "ApproximateNumberOfMessages": "7", **self.attrs.get(name, {})}}


class Cw:
    def __init__(self, fail=None):
        self.calls, self.fail = [], fail

    def get_metric_data(self, MetricDataQueries, StartTime, EndTime, ScanBy, **kw):
        self.calls.append((StartTime, EndTime, len(MetricDataQueries)))
        if self.fail:
            raise self.fail
        return {"MetricDataResults": [{"Id": q["Id"], "Timestamps": [EndTime], "Values": [3.0]}
                                      for q in MetricDataQueries]}


class Lam:
    def __init__(self, fail=None):
        self.calls, self.fail = [], fail

    def list_event_source_mappings(self, EventSourceArn, **kw):
        self.calls.append(EventSourceArn)
        if self.fail:
            raise self.fail
        return {"EventSourceMappings": [{"UUID": "u1", "FunctionArn": f"arn:aws:lambda:{REGION}:{ACCOUNT}:function:"
                                         "ship", "State": "Enabled", "BatchSize": 10}]}


class Stubs:
    def __init__(self, **kw):
        self.sqs, self.cw, self.lam = Sqs(kw.get("attrs"), kw.get("fail")), Cw(kw.get("cw_fail")), \
            Lam(kw.get("lam_fail"))
        self.regions = []

    def __call__(self, region):
        self.regions.append(region)
        return {"sqs": self.sqs, "cloudwatch": self.cw, "lambda": self.lam, "region": region or "us-east-1"}


def reader(settings=ON, stubs=None, installed=True):
    return AwsReader(lambda: settings, clients_for=stubs or never, installed=lambda: installed)


# ── which queues are read ───────────────────────────────────────────────────────────────────────────────────────

def test_with_reads_off_nothing_is_read_and_no_client_is_made():
    for s in ({"enabled": False, "queues": ["orders"]}, {"enabled": True, "queues": []}):
        out = reader(s).read([ref("orders")], [window()], T0)
        assert out["orders"]["status"] == "not_configured" and out["orders"]["detail"] == aws_reader.OFF
        assert out["orders"]["settings"] is None and out["orders"]["series"] == {}


def test_a_queue_the_settings_do_not_name_or_in_another_account_is_not_read():
    s = {**ON, "accountId": "999999999999"}
    out = reader(s).read([ref("payments"), ref("orders"), ref("billing-eu", account=None, url=False)], [], T0)
    assert out["payments"]["status"] == "no_account" and out["payments"]["detail"] == aws_reader.NOT_NAMED
    assert out["orders"]["status"] == "no_account"
    assert out["orders"]["detail"] == ("This queue is in AWS account 123456789012, and the AWS settings name account "
                                       "999999999999, so it is not read.")
    assert out["orders"]["accountId"] == ACCOUNT and out["orders"]["region"] == REGION


def test_reads_on_without_boto3_say_how_to_install_it():
    out = reader(installed=False).read([ref("orders"), ref("payments")], [], T0)
    assert out["orders"]["status"] == "not_configured" and 'pip install "trailbeam[aws]"' in out["orders"]["detail"]
    assert out["payments"]["detail"] == aws_reader.NOT_NAMED


def test_the_settings_unreadable_is_a_failed_read_not_an_error():
    def down():
        raise ConnectionError("down")

    out = AwsReader(down, clients_for=never).read([ref("orders")], [], T0)
    assert out["orders"]["status"] == "failed" and "ConnectionError" in out["orders"]["detail"]


def test_each_queue_is_read_in_its_own_region_and_the_settings_region_fills_in():
    st = Stubs()
    s = {**ON, "region": "us-east-2", "accountId": ACCOUNT}
    out = reader(s, st).read([ref("orders"), ref("billing-us", region="us-west-2"),
                              ref("billing-x", account=None, region=None, url=False)], [], T0)
    assert sorted(st.regions) == ["eu-west-1", "us-east-2", "us-west-2"]
    assert {k: v["region"] for k, v in out.items()} == {"orders": REGION, "billing-us": "us-west-2",
                                                         "billing-x": "us-east-2"}
    # without a URL in its spans, the URL is built from the region and the settings' account
    assert f"https://sqs.us-east-2.amazonaws.com/{ACCOUNT}/billing-x" in st.sqs.calls


def test_a_queue_without_a_url_or_an_account_says_what_to_set():
    out = reader(stubs=Stubs()).read([ref("orders", account=None, url=False)], [], T0)
    assert out["orders"]["status"] == "failed" and out["orders"]["detail"] == aws_reader.NO_URL


def test_no_region_anywhere_says_where_to_set_one():
    def no_region(region):
        from botocore.exceptions import NoRegionError
        raise NoRegionError()

    out = reader(stubs=no_region).read([ref("orders", region=None)], [], T0)
    assert out["orders"]["status"] == "failed" and out["orders"]["detail"] == aws_reader.NO_REGION


def test_failures_say_what_aws_answered_and_a_partial_failure_keeps_the_rest():
    st = Stubs(fail={"orders": NoCredentialsError, "billing-a": denied("GetQueueAttributes")},
               lam_fail=denied("ListEventSourceMappings"))
    out = reader(stubs=st).read([ref("orders"), ref("billing-a"), ref("billing-b")], [window()], T0)
    assert out["orders"]["status"] == "failed" and out["orders"]["detail"] == aws_reader.NO_CREDENTIALS
    assert out["billing-a"]["detail"] == "sqs:GetQueueAttributes failed on billing-a with AccessDenied."
    b = out["billing-b"]
    assert b["status"] == "ok" and b["settings"]["visible"] == 7 and b["triggers"] is None
    assert b["errors"] == ["lambda:ListEventSourceMappings failed with AccessDenied."]
    assert b["series"]["1h"]["queue"]["visible"] == [[T0, 3.0]]


# ── against moto ─────────────────────────────────────────────────────────────────────────────────────────────────

@pytest.fixture
def aws(monkeypatch):
    for k in ("AWS_ENDPOINT_URL", "AWS_ENDPOINT_URL_SQS", "AWS_PROFILE"):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", REGION)
    with moto.mock_aws():
        yield aws_reads.clients(REGION)


def test_reads_a_queue_its_dead_letter_queue_and_its_series(aws):
    sqs, cw = aws["sqs"], aws["cloudwatch"]
    dlq = sqs.create_queue(QueueName="orders-dlq")["QueueUrl"]
    dlq_arn = sqs.get_queue_attributes(QueueUrl=dlq, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
    url = sqs.create_queue(QueueName="orders", Attributes={
        "VisibilityTimeout": "45", "RedrivePolicy": json.dumps({"deadLetterTargetArn": dlq_arn,
                                                                "maxReceiveCount": "4"})})["QueueUrl"]
    sqs.send_message(QueueUrl=url, MessageBody="x")
    sqs.send_message(QueueUrl=dlq, MessageBody="y")
    now = int(dt.datetime.now(dt.timezone.utc).timestamp() * 1000)
    at = dt.datetime.fromtimestamp((now - 5 * MIN) / 1000, tz=dt.timezone.utc)
    for name, metric, v in (("orders", "ApproximateNumberOfMessagesVisible", 12.0),
                            ("orders-dlq", "ApproximateNumberOfMessagesVisible", 2.0)):
        cw.put_metric_data(Namespace="AWS/SQS", MetricData=[{
            "MetricName": metric, "Dimensions": [{"Name": "QueueName", "Value": name}], "Timestamp": at,
            "Value": v}])
    account = url.split("/")[3]
    r = AwsReader(lambda: {**ON, "queues": ["orders"]}, installed=lambda: True)
    q = {"key": "orders", "name": "orders", "account": account, "region": REGION, "url": url}
    a = r.read([q], [window(to=now)], now)["orders"]
    assert a["status"] == "ok" and a["errors"] == [], a
    assert a["settings"]["visibilityTimeoutS"] == 45 and a["settings"]["maxReceiveCount"] == 4
    assert a["settings"]["dlqName"] == "orders-dlq" and a["settings"]["visible"] == 1
    assert a["triggers"] == [] and a["readAt"] == now
    s = a["series"]["1h"]
    assert [v for _, v in s["queue"]["visible"]] == [12.0] and [v for _, v in s["dlq"]] == [2.0]
    # the dead-letter queue's depth from SQS itself
    b = AwsReader(lambda: {**ON, "queues": ["orders"]}, installed=lambda: True).read(
        [q], [], now, dlq_depth="sqs")["orders"]
    assert b["dlqNow"] == {"visible": 1, "at": now}


# ── the cache ────────────────────────────────────────────────────────────────────────────────────────────────────

def test_reads_are_kept_for_their_lifetimes():
    st = Stubs()
    r = reader(stubs=st)
    qs, w = [ref("orders"), ref("billing-a")], [window(to=T0 + 1000)]
    r.read(qs, w, T0)
    assert (len(st.sqs.calls), len(st.cw.calls), len(st.lam.calls), len(st.regions)) == (2, 1, 2, 1)
    # a page refreshed within 15 s: nothing read again
    r.read(qs, w, T0 + 14_000)
    assert (len(st.sqs.calls), len(st.cw.calls), len(st.lam.calls)) == (2, 1, 2)
    # the counts after 15 s; the billed metrics only after a minute, and the window aligned to its period, so
    # the same request is made across the minute
    r.read(qs, [window(to=T0 + 15_000)], T0 + 15_000)
    assert (len(st.sqs.calls), len(st.cw.calls)) == (4, 1)
    r.read(qs, [window(to=T0 + MIN)], T0 + MIN)  # the same request, but kept only a minute
    assert len(st.cw.calls) == 2 and len(st.lam.calls) == 2
    # triggers after 5 minutes, clients after 10
    r.read(qs, [], T0 + 5 * MIN)
    assert len(st.lam.calls) == 4 and len(st.regions) == 1
    r.read(qs, [], T0 + 10 * MIN)
    assert len(st.regions) == 2
    # the settings changed: every read again
    r.clear()
    r.read(qs, [], T0 + 10 * MIN)
    assert len(st.regions) == 3


def test_only_the_metrics_asked_for_are_read():
    st = Stubs()
    r = reader(stubs=st)
    r.read([ref("orders")], [window()], T0, metrics=["visible", "oldestS"], triggers=False)
    assert st.cw.calls[-1][2] == 2 and st.lam.calls == []
    r.read([ref("orders")], [window()], T0, metrics=[], triggers=False)
    assert len(st.cw.calls) == 1  # nothing to read: no call
    r.read([ref("orders")], [window()], T0, metrics=["visible"], attrs_ttl_ms=600_000)
    r.read([ref("orders")], [window()], T0 + 20_000, metrics=["visible"], attrs_ttl_ms=600_000)
    assert len(st.sqs.calls) == 2  # the long-kept settings read is apart from the short one


def test_a_failed_read_is_not_kept():
    st = Stubs(cw_fail=denied("GetMetricData"))
    r = reader(stubs=st)
    a = r.read([ref("orders")], [window()], T0)["orders"]
    assert a["status"] == "ok" and a["errors"] == ["cloudwatch:GetMetricData failed with AccessDenied."]
    st.cw.fail = None
    a = r.read([ref("orders")], [window()], T0 + 1)["orders"]
    assert a["errors"] == [] and len(st.cw.calls) == 2


def test_two_pages_that_miss_together_make_one_read():
    c = Cache()
    started, go = threading.Event(), threading.Event()
    calls, got = [], []

    def slow():
        calls.append(1)
        started.set()
        go.wait(5)
        return "v"

    t = threading.Thread(target=lambda: got.append(c.get("k", 1000, 0, slow)))
    t.start()
    started.wait(5)
    u = threading.Thread(target=lambda: got.append(c.get("k", 1000, 1, lambda: calls.append(2) or "w")))
    u.start()
    go.set()
    t.join(5), u.join(5)
    assert calls == [1] and got == ["v", "v"]


def test_waiters_on_a_failed_read_get_its_error_and_the_cache_stays_small():
    c = Cache(max_entries=3)
    with pytest.raises(ValueError):
        c.get("k", 1000, 0, lambda: (_ for _ in ()).throw(ValueError("x")))
    assert len(c) == 0 and c.get("k", 1000, 0, lambda: 1) == 1
    for i in range(5):
        c.get(f"e{i}", 10, i * 100, lambda: i)
    assert len(c) <= 3


# ── the status ───────────────────────────────────────────────────────────────────────────────────────────────────

def test_the_status_looks_for_credentials_only_with_reads_on():
    def no_lookup(region):
        raise AssertionError("credentials looked up with reads off")

    off = Status(lambda: {"enabled": False, "queues": ["orders"], "region": None}, installed=lambda: True,
                 source=no_lookup).get(T0)
    assert off == {"on": False, "queues": 1, "boto3": True, "credentials": None, "region": None, "problem": None}
    assert Status(lambda: ON, installed=lambda: False, source=no_lookup).get(T0)["problem"] == aws_reader.NO_BOTO3

    looked = []

    def source(region):
        looked.append(region)
        return (None if len(looked) == 1 else "env"), region or "eu-west-1"

    s = Status(lambda: {**ON, "region": "us-east-1"}, installed=lambda: True, source=source)
    assert s.get(T0)["problem"] == aws_reader.NO_CREDENTIALS
    assert s.get(T0 + 1000)["problem"] == aws_reader.NO_CREDENTIALS and looked == ["us-east-1"]  # kept a minute
    s.clear()
    got = s.get(T0 + 2000)
    assert got == {"on": True, "queues": 2, "boto3": True, "credentials": "env", "region": "us-east-1",
                   "problem": None}


def test_credentials_source_reads_boto3s_chain(monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "ap-south-1")
    assert aws_reader.credentials_source() == ("env", "ap-south-1")
    assert aws_reader.credentials_source("eu-west-1") == ("env", "eu-west-1")
