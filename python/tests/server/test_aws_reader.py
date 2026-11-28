"""The AWS reader (aws_reader.py, QV-589): which queues are read and why the others are not, the reads against moto's
SQS and CloudWatch, and the short-lived cache against counting stubs (billed GetMetricData is read at most once a
minute, a failed read is not kept, two pages that miss together make one read). No AWS account is used.

The QV-740 tests check that only the settings decide where the server's credentials read: a queue's account, region,
URL and name from spans (or a page's URL) can't widen them."""
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
ON = {"enabled": True, "queues": ["orders", "billing-*"], "region": REGION, "regions": [], "accountId": None}


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


class Sts:
    def __init__(self, account=ACCOUNT, fail=None):
        self.calls, self.account, self.fail = 0, account, fail

    def get_caller_identity(self):
        self.calls += 1
        if self.fail:
            raise self.fail
        return {"Account": self.account, "Arn": f"arn:aws:iam::{self.account}:role/trailbeam", "UserId": "x"}


class Stubs:
    def __init__(self, **kw):
        self.sqs, self.cw, self.lam = Sqs(kw.get("attrs"), kw.get("fail")), Cw(kw.get("cw_fail")), \
            Lam(kw.get("lam_fail"))
        self.sts = Sts(kw.get("account", ACCOUNT), kw.get("sts_fail"))
        self.home = kw.get("home", "us-east-1")
        self.regions = []

    def __call__(self, region):
        self.regions.append(region)
        return {"sqs": self.sqs, "cloudwatch": self.cw, "lambda": self.lam, "sts": self.sts,
                "region": region or self.home}

    def aws_calls(self):
        return len(self.sqs.calls) + len(self.cw.calls) + len(self.lam.calls)


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
    s = {**ON, "region": "us-east-2", "regions": [REGION, "us-west-2"], "accountId": ACCOUNT}
    out = reader(s, st).read([ref("orders"), ref("billing-us", region="us-west-2"),
                              ref("billing-x", account=None, region=None, url=False)], [], T0)
    assert sorted(st.regions) == ["eu-west-1", "us-east-2", "us-west-2"]
    assert {k: v["region"] for k, v in out.items()} == {"orders": REGION, "billing-us": "us-west-2",
                                                         "billing-x": "us-east-2"}
    # without a URL in its spans, the URL is built from the region and the settings' account
    assert f"https://sqs.us-east-2.amazonaws.com/{ACCOUNT}/billing-x" in st.sqs.calls


def test_without_an_account_in_the_settings_it_is_the_credentials_own():
    st = Stubs()
    r = reader(stubs=st)
    out = r.read([ref("orders", account=None, url=False), ref("billing-a")], [], T0)
    assert out["orders"]["status"] == "ok" and out["orders"]["accountId"] == ACCOUNT
    assert st.sqs.calls == [f"https://sqs.{REGION}.amazonaws.com/{ACCOUNT}/orders",
                            f"https://sqs.{REGION}.amazonaws.com/{ACCOUNT}/billing-a"]
    r.read([ref("orders")], [], T0 + 20_000)
    assert st.sts.calls == 1  # kept as the clients are
    # named in the settings: SQS is read there, and the credentials' account is asked only to know whose CloudWatch
    # it is (QV-741)
    st2 = Stubs()
    out = reader({**ON, "accountId": ACCOUNT}, st2).read([ref("orders")], [window()], T0)
    assert st2.sts.calls == 1 and len(st2.sqs.calls) == 1 and len(st2.cw.calls) == 1
    assert out["orders"]["errors"] == [] and out["orders"]["series"]["1h"]["queue"]["visible"] == [[T0, 3.0]]


def test_the_credentials_account_not_found_says_what_to_set():
    st = Stubs(sts_fail=denied("GetCallerIdentity"))
    out = reader(stubs=st).read([ref("orders")], [window()], T0)
    assert out["orders"]["status"] == "failed" and "sts:GetCallerIdentity failed with AccessDenied" in \
        out["orders"]["detail"] and "Set the AWS account ID" in out["orders"]["detail"]
    assert st.aws_calls() == 0
    st = Stubs(sts_fail=NoCredentialsError())
    assert reader(stubs=st).read([ref("orders")], [], T0)["orders"]["detail"] == aws_reader.NO_CREDENTIALS


def test_no_region_anywhere_says_where_to_set_one():
    def no_region(region):
        from botocore.exceptions import NoRegionError
        raise NoRegionError()

    out = reader({**ON, "region": None}, stubs=no_region).read([ref("orders", region=None)], [], T0)
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


# ── who picks what is read (QV-740) ────────────────────────────────────────────────────────────────────────────

ANY = {**ON, "queues": ["*"]}


def test_a_key_naming_another_account_is_not_read_with_the_servers_credentials():
    """The report's steps: queues ["*"], no account set, and a member asks for
    999999999999/ap-south-1/billing-secrets."""
    st = Stubs()
    target = ref("billing-secrets", key="999999999999/ap-south-1/billing-secrets", account="999999999999",
                 region="ap-south-1", url=False)
    a = reader(ANY, st).read([target], [window()], T0)[target["key"]]
    assert a["status"] == "no_account" and "ap-south-1" in a["detail"] and a["settings"] is None
    # the region allowed: still another account than the credentials'
    a = reader({**ANY, "regions": ["ap-south-1"]}, st).read([target], [window()], T0)[target["key"]]
    assert a["status"] == "no_account" and a["detail"] == aws_reader.other_account("999999999999", ACCOUNT)
    assert st.aws_calls() == 0 and a["triggers"] is None


def test_a_url_key_without_an_account_is_read_only_in_the_settings_account():
    """The report's step 4: accountId set, and a URL key with no account in it. Before, the URL went to SQS as typed."""
    st = Stubs()
    for url in ("https://sqs.eu-west-1.amazonaws.com/orders", "https://evil.example/999999999999/orders",
                "http://169.254.169.254/latest/orders"):
        q = {"key": url, "name": "orders", "account": None, "region": REGION, "url": url}
        a = reader({**ON, "accountId": "111111111111"}, st).read([q], [], T0)[url]
        assert a["status"] == "ok" and a["accountId"] == "111111111111"
    assert set(st.sqs.calls) == {f"https://sqs.{REGION}.amazonaws.com/111111111111/orders"}


def test_only_the_home_region_and_the_regions_listed_are_read():
    st = Stubs(home="eu-central-1")
    s = {**ANY, "region": None, "regions": ["us-west-2"]}
    out = reader(s, st).read([ref("a", region=None), ref("b", region="us-west-2"), ref("c", region="eu-central-1"),
                              ref("d", region="ap-south-1")], [], T0)
    assert {k: v["status"] for k, v in out.items()} == {"a": "ok", "b": "ok", "c": "ok", "d": "no_account"}
    assert out["a"]["region"] == "eu-central-1"
    assert out["d"]["detail"] == aws_reader.other_region("ap-south-1", ["eu-central-1", "us-west-2"])
    assert not any("ap-south-1" in u for u in st.sqs.calls) and "ap-south-1" not in st.regions


def test_a_name_sqs_would_not_take_is_never_read():
    st = Stubs()
    for name in ("orders/../999999999999/x", "orders?Action=DeleteQueue", "a b", "a" * 81):
        a = reader(ANY, st).read([ref(name, key="k", url=False)], [], T0)["k"]
        assert a["status"] == "no_account" and a["detail"] == aws_reader.NOT_A_NAME
    assert st.regions == [] and st.aws_calls() == 0
    # and a pattern's * matches only what a name can hold
    from trailbeam.server.aws_figures import name_matches
    assert name_matches("orders-*", "orders-eu.fifo") and not name_matches("*", "a/b") and not name_matches("*", "a?b")


def test_a_dead_letter_queue_in_another_account_is_not_read():
    dlq = {"RedrivePolicy": json.dumps({"deadLetterTargetArn": "arn:aws:sqs:eu-west-1:999999999999:loot",
                                        "maxReceiveCount": 3})}
    st = Stubs(attrs={"orders": dlq})
    a = reader(stubs=st).read([ref("orders")], [], T0, dlq_depth="sqs")["orders"]
    assert a["status"] == "ok" and a["dlqNow"] is None
    assert st.sqs.calls == [f"https://sqs.{REGION}.amazonaws.com/{ACCOUNT}/orders"]


# ── what a member is told (QV-788) ─────────────────────────────────────────────────────────────────────────────

def _says_nothing_of(a, *words):
    text = json.dumps(a)
    return not any(w in text for w in words)


def test_a_member_is_not_told_the_allowed_regions_or_the_credentials_account():
    """The report's steps: queues ["*"], home eu-west-1 and us-west-2, no account set, the credentials in
    111111111111, and spans for a queue in zz-abc-1 and one in 999999999999."""
    st = Stubs(account="111111111111")
    s = {**ANY, "regions": ["us-west-2"]}
    qs = [ref("regional", key="111111111111/zz-abc-1/regional", account="111111111111", region="zz-abc-1", url=False),
          ref("billing-secrets", key="999999999999/eu-west-1/billing-secrets", account="999999999999", url=False)]
    admin = reader(s, st).read(qs, [], T0)
    assert admin[qs[0]["key"]]["detail"] == aws_reader.other_region("zz-abc-1", [REGION, "us-west-2"])
    assert admin[qs[1]["key"]]["detail"] == aws_reader.other_account("999999999999", "111111111111")
    member = reader(s, st).read(qs, [], T0, admin=False)
    for q in qs:
        a = member[q["key"]]
        assert a["status"] == "no_account" and a["detail"] == aws_reader.MEMBER_NOT_READ
        # only the account and region its own spans name
        assert (a["accountId"], a["region"]) == (q["account"], q["region"])
    assert _says_nothing_of(member[qs[0]["key"]], REGION, "us-west-2")
    assert _says_nothing_of(member[qs[1]["key"]], "111111111111")


def test_a_member_is_not_told_the_settings_account():
    a = reader({**ON, "accountId": "222222222222"}, Stubs()).read([ref("orders", account="999999999999")], [], T0,
                                                                  admin=False)["orders"]
    assert a["detail"] == aws_reader.MEMBER_NOT_READ and _says_nothing_of(a, "222222222222")


def test_a_member_is_not_told_why_cloudwatch_is_not_read():
    st = Stubs(account="111111111111")
    a = reader({**ON, "accountId": "222222222222"}, st).read([ref("orders", account="222222222222")], [window()], T0,
                                                             admin=False)["orders"]
    assert a["status"] == "ok" and a["errors"] == [aws_reader.MEMBER_NO_METRICS] and _says_nothing_of(a, "111111111111")
    a = reader({**ON, "accountId": ACCOUNT}, Stubs(sts_fail=denied("GetCallerIdentity"))).read(
        [ref("orders")], [window()], T0, admin=False)["orders"]
    assert a["errors"] == [aws_reader.MEMBER_NO_METRICS]


def test_a_member_is_not_told_what_the_server_lacks():
    """No boto3, no credentials, no region: /api/health tells a member only that there is a problem (QV-744)."""
    a = reader(stubs=Stubs(), installed=False).read([ref("orders")], [], T0, admin=False)["orders"]
    assert a["status"] == "not_configured" and a["detail"] == aws_reader.MEMBER_NOT_READ
    a = reader(ANY, Stubs(sts_fail=NoCredentialsError())).read([ref("orders", account=None)], [], T0,
                                                               admin=False)["orders"]
    assert a["status"] == "failed" and a["detail"] == aws_reader.MEMBER_NOT_READ and a["accountId"] is None

    def no_region(region):
        from botocore.exceptions import NoRegionError
        raise NoRegionError()
    a = reader({**ON, "region": None}, no_region).read([ref("orders", region=None)], [], T0, admin=False)["orders"]
    assert a["detail"] == aws_reader.MEMBER_NOT_READ
    # what a member is still told: the settings don't name the queue (an admin adds it), and reads are off
    assert reader(stubs=Stubs()).read([ref("other")], [], T0, admin=False)["other"]["detail"] == aws_reader.NOT_NAMED
    assert reader({**ON, "enabled": False}).read([ref("orders")], [], T0, admin=False)["orders"]["detail"] == \
        aws_reader.OFF


# ── CloudWatch is the credentials' account's (QV-741) ─────────────────────────────────────────────────────────────

def test_a_queue_in_another_account_than_the_credentials_gets_no_cloudwatch_figures():
    """The report's steps: the credentials are in 111111111111, a queue policy lets them read 222222222222's orders,
    and the settings name 222222222222. CloudWatch would answer with 111111111111's orders."""
    st = Stubs(account="111111111111")
    out = reader({**ON, "accountId": "222222222222"}, st).read([ref("orders", account="222222222222")],
                                                               [window(), window("1d", period=300)], T0)
    a = out["orders"]
    assert a["status"] == "ok" and a["accountId"] == "222222222222"
    assert a["settings"]["visible"] == 7 and a["triggers"] is not None  # SQS's own counts, and its Lambda triggers
    assert st.sqs.calls == [f"https://sqs.{REGION}.amazonaws.com/222222222222/orders"]
    assert a["series"] == {} and st.cw.calls == []
    assert a["errors"] == [aws_reader.other_account_metrics("222222222222", "111111111111")]
    assert "account 222222222222" in a["errors"][0] and "account 111111111111" in a["errors"][0]


def test_cloudwatch_is_not_read_when_the_credentials_account_cannot_be_found():
    st = Stubs(sts_fail=denied("GetCallerIdentity"))
    r = reader({**ON, "accountId": ACCOUNT}, st)
    a = r.read([ref("orders")], [window()], T0)["orders"]
    assert a["status"] == "ok" and a["settings"]["visible"] == 7 and a["series"] == {} and st.cw.calls == []
    assert a["errors"] == [aws_reader.metrics_unconfirmed(denied("GetCallerIdentity"))]
    assert "sts:GetCallerIdentity failed with AccessDenied" in a["errors"][0]
    # the failure is kept a minute, so a server that can't reach STS doesn't wait on it at every page
    r.read([ref("orders")], [window()], T0 + 20_000)
    assert st.sts.calls == 1
    r.read([ref("orders")], [window()], T0 + 2 * MIN)
    assert st.sts.calls == 2


def test_dead_letter_queue_depths_from_sqs_are_kept_by_arn():
    dlq = {"RedrivePolicy": json.dumps({"deadLetterTargetArn": f"arn:aws:sqs:{REGION}:{ACCOUNT}:orders-dlq",
                                        "maxReceiveCount": 3})}
    st = Stubs(attrs={"orders": dlq, "orders-dlq": {"ApproximateNumberOfMessages": "4000"}})
    out = reader({**ANY, "accountId": ACCOUNT}, st).read([ref("orders")], [window()], T0, dlq_depth="sqs")
    assert out["orders"]["dlqNow"] == {"visible": 4000, "at": T0}
    # the depth came from SQS, so CloudWatch is not asked for it
    from trailbeam.server.aws_figures import METRICS
    assert st.cw.calls[0][2] == len(METRICS)


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


def test_every_queue_of_a_region_is_one_getmetricdata_per_window():
    """QV-743: the queues asked together are read in one call per window, whatever their number."""
    st = Stubs()
    names = ["orders", *(f"billing-{i}" for i in range(40))]
    got = reader(stubs=st).read([ref(n) for n in names], [window()], T0, metrics=["oldestS", "sent", "deleted"],
                                triggers=False)
    assert all(got[n]["series"]["1h"]["queue"]["oldestS"] == [[T0, 3.0]] for n in names)
    assert len(st.cw.calls) == 1 and st.cw.calls[0][2] == 3 * len(names)


def test_a_callers_new_cloudwatch_reads_are_capped_a_minute():
    """QV-743: each read the cache doesn't have counts its metrics against the caller; past the budget nothing is
    read for them, and the page says why. Kept reads, another caller and the server's own reads are not held."""
    st = Stubs()
    r = AwsReader(lambda: ON, clients_for=st, installed=lambda: True, budget=aws_reader.Budget(limit=12))
    q = [ref("orders")]

    def ranged(i, at=T0, caller="user:7"):  # a page whose range moves by a minute each time: a new read each time
        return r.read(q, [window(to=T0 - i * MIN)], at, triggers=False, caller=caller)["orders"]

    assert ranged(0)["errors"] == [] and ranged(1)["errors"] == []  # 5 metrics each: 10 of 12
    a = ranged(2)
    assert a["status"] == "ok" and a["settings"] is not None and a["series"] == {}
    assert a["errors"] == [aws_reader.over_budget(aws_reader.OverBudget(15, 12))]
    assert "15 new CloudWatch metrics" in a["errors"][0] and "12" in a["errors"][0]
    assert len(st.cw.calls) == 2
    assert ranged(0)["errors"] == []  # kept: no new read, nothing counted
    assert ranged(2, caller="user:8")["errors"] == []  # another person has their own budget
    assert ranged(3, caller=None)["errors"] == []  # the server's own reads (the alert check) have none
    assert ranged(4, at=T0 + MIN)["errors"] == []  # a minute later the budget is whole again
    assert len(st.cw.calls) == 5


def test_the_budget_counts_nothing_it_refuses():
    b = aws_reader.Budget(limit=10)
    b.take("a", 6, T0)
    with pytest.raises(aws_reader.OverBudget):
        b.take("a", 5, T0)
    b.take("a", 4, T0)  # the refused 5 were not counted
    with pytest.raises(aws_reader.OverBudget):
        b.take("a", 1, T0 + 1)
    b.take(None, 10 ** 9, T0)


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



def test_a_waiter_gives_up_on_a_read_that_hangs():
    # QV-742: one slow read must not hold every other viewer's request with it
    c = Cache(wait_s=0.2)
    started, go = threading.Event(), threading.Event()
    got = []

    def slow():
        started.set()
        go.wait(5)
        return "v"

    t = threading.Thread(target=lambda: got.append(c.get("k", 1000, 0, slow)))
    t.start()
    started.wait(5)
    with pytest.raises(aws_reader.WaitTimeout) as e:
        c.get("k", 1000, 1, lambda: "w")
    assert e.value.wait_s == 0.2
    go.set()
    t.join(5)
    assert got == ["v"] and c.get("k", 1000, 2, lambda: "w") == "v"


def test_a_page_waiting_on_a_hung_read_says_aws_did_not_answer():
    st = Stubs()
    started, go = threading.Event(), threading.Event()
    plain = st.sqs.get_queue_attributes

    def hang(QueueUrl, AttributeNames):
        started.set()
        go.wait(5)
        return plain(QueueUrl, AttributeNames)

    st.sqs.get_queue_attributes = hang
    r = AwsReader(lambda: ON, clients_for=st, installed=lambda: True, cache=Cache(wait_s=0.2))
    first = []
    t = threading.Thread(target=lambda: first.append(r.read([ref("orders")], [], T0)))
    t.start()
    started.wait(5)
    a = r.read([ref("orders")], [], T0 + 1)["orders"]
    assert a["status"] == "failed"
    assert a["detail"] == "sqs:GetQueueAttributes on orders did not answer within 0.2 s, so nothing is shown from it " \
                          "this time."
    go.set()
    t.join(5)
    assert first[0]["orders"]["status"] == "ok"


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
