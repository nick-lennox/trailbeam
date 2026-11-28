"""Redrive (QV-590): the gate, the move-task calls and their sentences (redrive.py), the log on each store
(redrive_log.py), and the API's refresh of open moves (redrive_api.py). No AWS: a fake SQS client, and, when
TRAILBEAM_TEST_REDRIVE_SQS_ENDPOINT names an ElasticMQ (1.5 or later; moto has no move tasks), a real move on it."""
from __future__ import annotations

import json
import os
import sys
import types
import uuid
from pathlib import Path

import pytest
from botocore.exceptions import ClientError

from trailbeam.server import redrive, redrive_api, redrive_log
from trailbeam.server.aws_settings import AwsSettings, MemorySettings
from trailbeam.server.db import Database

ROOT = Path(__file__).resolve().parents[2]
PG = os.environ.get("TRAILBEAM_TEST_POSTGRES_URL")
needs_pg = pytest.mark.skipif(not PG, reason="TRAILBEAM_TEST_POSTGRES_URL is not set")
EMQ = os.environ.get("TRAILBEAM_TEST_REDRIVE_SQS_ENDPOINT")

ON = {"enabled": True, "queues": ["orders"], "redrive": ["*-dlq"], "region": "eu-west-1", "accountId": None}
DLQ = {"name": "orders-dlq", "account": "123456789012", "region": "eu-west-1"}
ARN = "arn:aws:sqs:eu-west-1:123456789012:orders-dlq"
URL = "https://sqs.eu-west-1.amazonaws.com/123456789012/orders-dlq"


def denied(message: str, code: str = "AccessDenied") -> ClientError:
    return ClientError({"Error": {"Code": code, "Message": message}}, "StartMessageMoveTask")


class Sqs:
    """The calls redrive.py makes, answering from ``tasks`` and ``depth``; ``fail``: an error a call raises."""

    def __init__(self, depth=3):
        self.tasks: list = []
        self.depth = depth
        self.fail: dict = {}
        self.calls: list = []

    def _maybe(self, name):
        self.calls.append(name)
        if name in self.fail:
            raise self.fail[name]

    def get_queue_attributes(self, QueueUrl, AttributeNames):
        self._maybe("get_queue_attributes")
        return {"Attributes": {"ApproximateNumberOfMessages": str(self.depth),
                               "ApproximateNumberOfMessagesNotVisible": "1",
                               "ApproximateNumberOfMessagesDelayed": "0"}}

    def list_message_move_tasks(self, SourceArn, MaxResults):
        self._maybe("list_message_move_tasks")
        return {"Results": [dict(t) for t in self.tasks]}

    def start_message_move_task(self, SourceArn, **kw):
        self._maybe("start_message_move_task")
        self.tasks.insert(0, {"TaskHandle": "h-1", "Status": "RUNNING", "SourceArn": SourceArn,
                              "ApproximateNumberOfMessagesMoved": 0, "ApproximateNumberOfMessagesToMove": self.depth,
                              "StartedTimestamp": 1_000_000})
        return {"TaskHandle": "h-1"}

    def cancel_message_move_task(self, TaskHandle):
        self._maybe("cancel_message_move_task")
        return {"ApproximateNumberOfMessagesMoved": 1}


# ── the gate ─────────────────────────────────────────────────────────────────────────────────────────────────────

def test_the_gate_opens_only_with_aws_on_the_queue_named_and_an_admin():
    def g(settings=ON, dlq=DLQ, installed=True, can_manage=True):
        return redrive.gate(settings, dlq, installed=installed, can_manage=can_manage)

    assert g() == {"ok": True, "account": "123456789012", "region": "eu-west-1"}
    assert g(settings={**ON, "enabled": False})["code"] == "not_configured"
    assert g(settings={**ON, "queues": []})["code"] == "not_configured"  # reads on needs a queue named
    assert g(installed=False) == {"ok": False, "code": "not_configured", "reason": redrive.NO_BOTO3}
    # off by default: reads on, and no dead-letter queue named to redrive
    assert g(settings={**ON, "redrive": []}) == {"ok": False, "code": "dlq_not_named",
                                                 "reason": redrive.not_named("orders-dlq")}
    assert g(settings={**ON, "redrive": ["billing-dlq"]})["code"] == "dlq_not_named"
    assert g(settings={**ON, "accountId": "999999999999"})["code"] == "no_account"
    # no account from the spans: the settings' one, or none known
    assert g(dlq={"name": "orders-dlq", "account": None, "region": None},
             settings={**ON, "accountId": "999999999999"}) == {"ok": True, "account": "999999999999",
                                                               "region": "eu-west-1"}
    assert g(dlq={"name": "orders-dlq", "account": None, "region": None})["code"] == "no_account"
    assert g(can_manage=False) == {"ok": False, "code": "not_manager", "reason": redrive.NOT_MANAGER}
    # every reason is one sentence for the button that stays off
    assert "Settings, under AWS" in g(settings={**ON, "enabled": False})["reason"]


# ── the move-task calls ──────────────────────────────────────────────────────────────────────────────────────────

def test_preview_reads_the_depth_and_a_move_already_running():
    sqs = Sqs()
    assert redrive.preview(sqs, ARN, URL) == {"ok": True, "preview": {"visible": 3, "inFlight": 1, "running": None}}
    redrive.start(sqs, ARN)
    p = redrive.preview(sqs, ARN, URL)["preview"]
    assert p["running"]["handle"] == "h-1" and p["running"]["toMove"] == 3


def test_start_does_not_double_a_running_move():
    sqs = Sqs()
    assert redrive.start(sqs, ARN) == {"ok": True, "handle": "h-1"}
    again = redrive.start(sqs, ARN)
    assert again == {"ok": False, "refused": True,
                     "reason": "A redrive of orders-dlq is already running: 0 of 3 moved."}
    assert sqs.calls.count("start_message_move_task") == 1


def test_a_denial_names_the_action_aws_named_and_where_to_allow_it():
    sqs = Sqs()
    sqs.fail["start_message_move_task"] = denied(
        "User: arn:aws:sts::123456789012:assumed-role/t/x is not authorized to perform: sqs:receivemessage on "
        "resource: arn:aws:sqs:eu-west-1:123456789012:orders-dlq because no identity-based policy allows it.")
    r = redrive.start(sqs, ARN)
    assert r == {"ok": False, "refused": False, "reason": (
        "AWS denied sqs:ReceiveMessage on orders-dlq, which sqs:StartMessageMoveTask needs to move the messages. "
        "Allow sqs:ReceiveMessage on arn:aws:sqs:eu-west-1:123456789012:orders-dlq for the server's AWS credentials.")}
    sqs.fail["start_message_move_task"] = denied("Access to the resource is denied.")
    assert redrive.start(sqs, ARN)["reason"].startswith("AWS denied sqs:StartMessageMoveTask on orders-dlq.")


@pytest.mark.parametrize("err, text", [
    (ClientError({"Error": {"Code": "AWS.SimpleQueueService.NonExistentQueue", "Message": "x"}}, "x"),
     "SQS has no queue orders-dlq in this account and region."),
    (ClientError({"Error": {"Code": "UnsupportedOperation", "Message": "Source queue must be configured as a "
                                                                       "Dead Letter Queue."}}, "x"),
     "SQS refused to move messages from orders-dlq: Source queue must be configured as a Dead Letter Queue. Only a "
     "queue that is some queue's dead-letter queue can be redriven, and only one move can run on it at a time."),
    (ClientError({"Error": {"Code": "ThrottlingException", "Message": "Rate exceeded"}}, "x"),
     "sqs:ListMessageMoveTasks failed on orders-dlq with ThrottlingException: Rate exceeded."),
])
def test_what_sqs_refused_is_a_sentence(err, text):
    sqs = Sqs()
    sqs.fail["list_message_move_tasks"] = err
    assert redrive.preview(sqs, ARN, URL) == {"ok": False, "reason": text}


def test_no_credentials_says_how_to_give_them():
    from botocore.exceptions import NoCredentialsError

    sqs = Sqs()
    sqs.fail["list_message_move_tasks"] = NoCredentialsError()
    assert "No AWS credentials were found" in redrive.start(sqs, ARN)["reason"]


def test_cancel_and_its_failures():
    sqs = Sqs()
    assert redrive.cancel(sqs, ARN, "h-1") == {"ok": True}
    sqs.fail["cancel_message_move_task"] = ClientError({"Error": {"Code": "ResourceNotFoundException"}}, "x")
    assert redrive.cancel(sqs, ARN, "h-1")["reason"] == "SQS has no running move with that handle. It may have " \
                                                        "finished."
    sqs.fail["cancel_message_move_task"] = denied("not authorized to perform: sqs:CancelMessageMoveTask")
    assert redrive.cancel(sqs, ARN, "h-1")["reason"].startswith("AWS denied sqs:CancelMessageMoveTask on orders-dlq.")


def test_a_finished_move_is_found_by_its_start_time():
    sqs = Sqs()
    sqs.tasks = [{"Status": "COMPLETED", "StartedTimestamp": 1_030_000, "ApproximateNumberOfMessagesMoved": 3,
                  "ApproximateNumberOfMessagesToMove": 3},
                 {"Status": "COMPLETED", "StartedTimestamp": 900_000}]
    t = redrive.move_task(sqs, ARN, None, 1_000_000)
    assert t["startedAt"] == 1_030_000 and t["moved"] == 3
    assert redrive.move_task(sqs, ARN, None, 5_000_000) is None


def test_next_check_waits_a_minute_after_sqs_says_done_then_reads_the_depth():
    row = {"status": "RUNNING", "statusAt": 0, "leftAt": None}
    assert redrive.next_check(row, 10) == "task"
    done = {**row, "status": "COMPLETED", "statusAt": 1000}
    assert redrive.next_check(done, 1000 + redrive.SETTLE_MS - 1) == "wait"
    assert redrive.next_check(done, 1000 + redrive.SETTLE_MS) == "depth"
    assert redrive.next_check({**done, "leftAt": 70_000}, 80_000) == "done"
    assert redrive.next_check(done, 1000 + redrive.CHECK_WITHIN_MS + 1) == "done"


# ── the log ──────────────────────────────────────────────────────────────────────────────────────────────────────

def pg_store():
    sys.path.insert(0, str(ROOT / "tests" / "conformance"))
    from pg_helpers import postgres_store
    return postgres_store()


@pytest.fixture(params=["memory", "sqlite", "sqlite-memory", pytest.param("postgres", marks=needs_pg)])
def log(request, tmp_path):
    backing = None
    if request.param == "memory":
        t = redrive_log.MemoryLog()
    else:
        backing = {"sqlite": lambda: Database(str(tmp_path / "trailbeam.sqlite")),
                   "sqlite-memory": lambda: Database(":memory:"), "postgres": pg_store}[request.param]()
        t = redrive_log.LogTables(backing)
    yield t
    if backing is not None:
        backing.close()


def entry(**kw):
    return {"dlqName": "orders-dlq", "dlqArn": ARN, "accountId": "123456789012", "region": "eu-west-1",
            "messageIds": ["m-1"], "requestedBy": "Ada", "requestedByEmail": "ada@example.com",
            "requestedAt": 1_000, "outcome": "started", "taskHandle": "h-1", "status": "RUNNING", "statusAt": 1_000,
            **kw}


def test_the_log_keeps_each_request_newest_first_and_follows_its_move(log):
    a = log.add(entry())
    b = log.add(entry(requestedAt=2_000, outcome="refused", taskHandle=None, status=None, statusAt=None,
                      detail="A redrive of orders-dlq is already running: 0 of 3 moved."))
    got = log.list()
    assert [e["id"] for e in got] == [b, a]
    assert got[0]["outcome"] == "refused" and got[0]["detail"].startswith("A redrive")
    assert got[1] == {"id": a, "dlqName": "orders-dlq", "dlqArn": ARN, "accountId": "123456789012",
                      "region": "eu-west-1", "messageIds": ["m-1"], "requestedBy": "Ada",
                      "requestedByEmail": "ada@example.com", "requestedAt": 1_000, "outcome": "started",
                      "detail": None, "taskHandle": "h-1", "status": "RUNNING", "moved": None, "toMove": None,
                      "failure": None, "statusAt": 1_000, "left": None, "leftAt": None}
    log.set_status(a, {"status": "COMPLETED", "moved": 3, "toMove": 3, "statusAt": 5_000, "id": "not changed"})
    log.set_status(a, {"left": 0, "leftAt": 65_000})
    e = log.get(a)
    assert (e["status"], e["moved"], e["toMove"], e["statusAt"], e["left"], e["leftAt"]) == \
        ("COMPLETED", 3, 3, 5_000, 0, 65_000)
    assert log.get("0" * 32) is None
    assert len(log.list(1)) == 1


def test_a_new_store_has_the_log_and_a_store_without_one_keeps_it_in_memory():
    db = Database(":memory:")
    try:
        assert isinstance(redrive_log.tables_for(db), redrive_log.LogTables)
    finally:
        db.close()
    assert isinstance(redrive_log.tables_for(object()), redrive_log.MemoryLog)


# ── the API's refresh ────────────────────────────────────────────────────────────────────────────────────────────

class Clock:
    def __init__(self, t=1_000_000):
        self.t = t

    def __call__(self):
        return self.t


def api(sqs, admin=True, settings=ON):
    clock = Clock()
    s = AwsSettings(MemorySettings())
    s.save(settings, "test", 0)
    reader = types.SimpleNamespace(cache=types.SimpleNamespace(get=lambda key, ttl, now, make: make()),
                                   clients_for=lambda region: {"sqs": sqs, "region": region or "eu-west-1"})
    aws = types.SimpleNamespace(settings=s, reader=reader)
    r = redrive_api.ServerRedrive(aws, redrive_log.MemoryLog(), lambda who: admin, clock, installed=lambda: True)
    return r, clock


WHO = types.SimpleNamespace(name="Ada", email="ada@example.com", role="admin")


def test_a_started_move_is_followed_until_the_queue_is_seen_empty_a_minute_after():
    sqs = Sqs()
    r, clock = api(sqs)
    status, out = r.call("POST", "/api/redrive/start", {"dlq": DLQ, "messageIds": ["m-1"]}, WHO)
    assert status == 200
    e = out["entry"]
    assert (e["outcome"], e["status"], e["taskHandle"], e["requestedBy"], e["requestedByEmail"], e["dlqArn"]) == \
        ("started", "RUNNING", "h-1", "Ada", "ada@example.com", ARN)

    # SQS finishes; it says so, and gives no handle any more
    sqs.tasks[0].update(Status="COMPLETED", ApproximateNumberOfMessagesMoved=3, TaskHandle=None)
    clock.t += 10_000
    v = r.view(WHO)
    assert v["on"] and v["canRedrive"]
    assert (v["log"][0]["status"], v["log"][0]["moved"], v["log"][0]["statusAt"]) == ("COMPLETED", 3, clock.t)
    done_at = clock.t
    # within the minute: no depth read
    clock.t += 30_000
    sqs.calls.clear()
    r.refresh(force=True)
    assert "get_queue_attributes" not in sqs.calls and r.log.list()[0]["leftAt"] is None
    # a minute on: the depth is read once, and the move is done
    sqs.depth = 0
    clock.t = done_at + redrive.SETTLE_MS
    r.refresh(force=True)
    got = r.log.list()[0]
    assert (got["left"], got["leftAt"]) == (1, clock.t)  # 0 visible + 1 in flight, from the fake
    sqs.calls.clear()
    r.refresh(force=True)
    assert sqs.calls == []  # nothing open


def test_the_refresh_reads_sqs_at_most_every_few_seconds():
    sqs = Sqs()
    r, clock = api(sqs)
    r.call("POST", "/api/redrive/start", {"dlq": DLQ}, WHO)
    sqs.calls.clear()
    r.view(WHO)
    r.view(WHO)
    assert sqs.calls.count("list_message_move_tasks") == 1
    clock.t += redrive_api.REFRESH_MS
    r.view(WHO)
    assert sqs.calls.count("list_message_move_tasks") == 2


def test_every_write_goes_through_the_gate_again():
    from trailbeam.server.alerts_api import ApiProblem

    sqs = Sqs()
    r, _ = api(sqs, admin=False)
    with pytest.raises(ApiProblem) as e:
        r.call("POST", "/api/redrive/start", {"dlq": DLQ}, WHO)
    assert e.value.status == 403 and sqs.calls == []
    r, _ = api(sqs, settings={**ON, "redrive": []})
    with pytest.raises(ApiProblem) as e:
        r.call("POST", "/api/redrive/start", {"dlq": DLQ}, WHO)
    assert e.value.status == 409 and e.value.message == redrive.not_named("orders-dlq") and sqs.calls == []
    assert r.view(WHO)["on"] is False and "names the dead-letter queues" in r.view(WHO)["reason"]


def test_a_refused_or_failed_start_is_logged_with_why():
    from trailbeam.server.alerts_api import ApiProblem

    sqs = Sqs()
    r, clock = api(sqs)
    r.call("POST", "/api/redrive/start", {"dlq": DLQ}, WHO)
    clock.t += 1
    with pytest.raises(ApiProblem):
        r.call("POST", "/api/redrive/start", {"dlq": DLQ}, WHO)
    clock.t += 1
    sqs.tasks.clear()
    sqs.fail["start_message_move_task"] = denied("Access to the resource is denied.")
    with pytest.raises(ApiProblem):
        r.call("POST", "/api/redrive/start", {"dlq": DLQ}, WHO)
    assert [e["outcome"] for e in r.log.list()] == ["failed", "refused", "started"]
    assert r.log.list()[1]["detail"] == "A redrive of orders-dlq is already running: 0 of 3 moved."


def test_the_gate_finds_a_messages_account_and_region_from_its_spans():
    r, _ = api(Sqs(), settings={**ON, "region": None})
    r.place_of = lambda mid, queue: {"account": "123456789012", "region": "eu-west-1"} if mid == "m-1" else None
    gates = r.call("POST", "/api/redrive/gates", {"dlqs": [{"name": "orders-dlq", "messageId": "m-1"},
                                                          {"name": "orders-dlq", "messageId": "m-2"}]}, WHO)[1]
    assert gates["gates"][0] == {"ok": True, "dlq": DLQ}
    assert gates["gates"][1]["code"] == "no_account"
    assert gates["gates"][1]["dlq"] == {"name": "orders-dlq", "account": None, "region": None}


def test_cancel_records_who_asked_and_keeps_moving_until_sqs_says_so():
    sqs = Sqs()
    r, clock = api(sqs)
    id_ = r.call("POST", "/api/redrive/start", {"dlq": DLQ}, WHO)[1]["entry"]["id"]
    clock.t += 1_000
    e = r.call("POST", "/api/redrive/cancel", {"id": id_}, WHO)[1]["entry"]
    assert (e["status"], e["failure"], e["statusAt"]) == ("CANCELLING", "Cancel requested by Ada.", clock.t)
    assert "cancel_message_move_task" in sqs.calls


# ── a real move, on ElasticMQ ────────────────────────────────────────────────────────────────────────────────────

@pytest.mark.skipif(not EMQ, reason="TRAILBEAM_TEST_REDRIVE_SQS_ENDPOINT is not set (ElasticMQ 1.5 or later)")
def test_a_redrive_on_elasticmq_moves_the_dead_letter_queue_back():
    import time

    import boto3

    sqs = boto3.session.Session().client("sqs", endpoint_url=EMQ, region_name="elasticmq")
    n = uuid.uuid4().hex[:8]
    dlq_url = sqs.create_queue(QueueName=f"orders-{n}-dlq")["QueueUrl"]
    dlq_arn = sqs.get_queue_attributes(QueueUrl=dlq_url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
    src = sqs.create_queue(QueueName=f"orders-{n}", Attributes={
        "RedrivePolicy": json.dumps({"deadLetterTargetArn": dlq_arn, "maxReceiveCount": "1"})})["QueueUrl"]
    try:
        for i in range(3):
            sqs.send_message(QueueUrl=src, MessageBody=f"m{i}")
        for _ in range(6):  # received once and let go: the second receive moves each to the dead-letter queue
            for m in sqs.receive_message(QueueUrl=src, MaxNumberOfMessages=10).get("Messages", []):
                sqs.change_message_visibility(QueueUrl=src, ReceiptHandle=m["ReceiptHandle"], VisibilityTimeout=0)
        assert redrive.queue_depth(sqs, dlq_url) == 3

        p = redrive.preview(sqs, dlq_arn, dlq_url)
        assert p["ok"] and p["preview"]["visible"] == 3 and p["preview"]["running"] is None
        started = redrive.start(sqs, dlq_arn)
        assert started["ok"], started
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline and redrive.queue_depth(sqs, dlq_url):
            time.sleep(0.2)
        assert redrive.queue_depth(sqs, dlq_url) == 0
        assert redrive.queue_depth(sqs, src) == 3
        t = redrive.move_task(sqs, dlq_arn, started["handle"], int(time.time() * 1000))
        assert t is not None and t["status"] in ("RUNNING", "COMPLETED")
    finally:
        sqs.delete_queue(QueueUrl=src)
        sqs.delete_queue(QueueUrl=dlq_url)


# ── through server.App ───────────────────────────────────────────────────────────────────────────────────────────

def test_the_app_finds_a_dead_lettered_messages_account_from_its_spans(db, ingest):
    from spans import T0, Consumer
    from trailbeam.server import server

    ingest(Consumer(queue="orders-dlq").send(T0, "m-dl"))
    app = server.App(db, ui_dir=None)
    app.aws.settings.save({"enabled": True, "queues": ["orders"], "redrive": ["orders-dlq"], "region": "eu-west-1"},
                          "test", 0)
    app.redrive.installed = lambda: True
    status, out = app.redrive.call("POST", "/api/redrive/gates",
                                   {"dlqs": [{"name": "orders-dlq", "messageId": "m-dl"}]}, WHO)
    # the spans' URL (http://localhost:9324/000000000000/orders-dlq) names the account; the region is the settings'
    assert status == 200 and out["gates"][0]["ok"] is True
    assert out["gates"][0]["dlq"]["account"] == "000000000000"
    assert app.redrive.call("GET", "/api/redrive", None, WHO)[1] == {"on": True, "reason": None, "canRedrive": True,
                                                                     "log": []}


@pytest.mark.skipif(not EMQ, reason="TRAILBEAM_TEST_REDRIVE_SQS_ENDPOINT is not set (ElasticMQ 1.5 or later)")
def test_a_redrive_through_the_app_on_elasticmq(db, monkeypatch):
    """The server's own wiring: boto3's clients (aws_reads.clients, with AWS_ENDPOINT_URL_SQS), the gate, the log.
    The ElasticMQ must be configured as aws.region us-east-1 and aws.accountId 111111111111, so the ARN the server
    builds is the queue's."""
    import time

    import boto3
    from trailbeam.server import server

    monkeypatch.setenv("AWS_ENDPOINT_URL_SQS", EMQ)
    sqs = boto3.session.Session().client("sqs", endpoint_url=EMQ, region_name="us-east-1")
    n = uuid.uuid4().hex[:8]
    dlq_url = sqs.create_queue(QueueName=f"app-{n}-dlq")["QueueUrl"]
    dlq_arn = sqs.get_queue_attributes(QueueUrl=dlq_url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
    if dlq_arn != f"arn:aws:sqs:us-east-1:111111111111:app-{n}-dlq":
        pytest.skip(f"the ElasticMQ is not us-east-1 and 111111111111 ({dlq_arn})")
    src = sqs.create_queue(QueueName=f"app-{n}", Attributes={
        "RedrivePolicy": json.dumps({"deadLetterTargetArn": dlq_arn, "maxReceiveCount": "1"})})["QueueUrl"]
    try:
        for i in range(2):
            sqs.send_message(QueueUrl=src, MessageBody=f"m{i}")
        for _ in range(6):
            for m in sqs.receive_message(QueueUrl=src, MaxNumberOfMessages=10).get("Messages", []):
                sqs.change_message_visibility(QueueUrl=src, ReceiptHandle=m["ReceiptHandle"], VisibilityTimeout=0)
        app = server.App(db, ui_dir=None)
        app.aws.settings.save({"enabled": True, "queues": [f"app-{n}"], "redrive": ["app-*-dlq"],
                               "region": "us-east-1", "accountId": "111111111111"}, "test", 0)
        dlq = {"name": f"app-{n}-dlq", "account": None, "region": None}
        assert app.redrive.call("POST", "/api/redrive/preview", {"dlq": dlq}, WHO)[1]["preview"]["visible"] == 2
        e = app.redrive.call("POST", "/api/redrive/start", {"dlq": dlq, "messageIds": ["m0"]}, WHO)[1]["entry"]
        assert (e["outcome"], e["dlqArn"], e["accountId"], e["region"]) == ("started", dlq_arn, "111111111111",
                                                                          "us-east-1")
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline and redrive.queue_depth(sqs, dlq_url):
            time.sleep(0.2)
        assert redrive.queue_depth(sqs, src) == 2
        app.redrive.refresh(force=True)
        got = app.redrive.log.get(e["id"])
        assert got["status"] in ("RUNNING", "COMPLETED") and got["outcome"] == "started"
    finally:
        sqs.delete_queue(QueueUrl=src)
        sqs.delete_queue(QueueUrl=dlq_url)
