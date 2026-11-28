"""The AWS reads (aws_reads.py, QV-589) against moto's in-process SQS, CloudWatch and Lambda, and against stubs for
what moto doesn't do (pages, more than 500 queries, a denied call). No AWS account is used."""
import datetime as dt
import io
import json
import zipfile

import boto3
import moto
import pytest
from botocore.exceptions import ClientError, NoCredentialsError

from trailbeam.server import aws_reads

REGION = "eu-west-1"
T0 = 1_764_000_000_000
MIN = 60_000


@pytest.fixture
def aws(monkeypatch):
    for k in ("AWS_ENDPOINT_URL", "AWS_ENDPOINT_URL_SQS", "AWS_PROFILE"):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", REGION)
    with moto.mock_aws():
        yield aws_reads.clients(REGION)


def make_queues(sqs):
    dlq = sqs.create_queue(QueueName="orders-dlq")["QueueUrl"]
    dlq_arn = sqs.get_queue_attributes(QueueUrl=dlq, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
    url = sqs.create_queue(QueueName="orders", Attributes={
        "VisibilityTimeout": "45", "MessageRetentionPeriod": "86400", "DelaySeconds": "2",
        "RedrivePolicy": json.dumps({"deadLetterTargetArn": dlq_arn, "maxReceiveCount": "4"})})["QueueUrl"]
    return url, dlq_arn


def test_clients_use_the_region_and_the_sqs_emulator(monkeypatch, aws):
    assert aws["region"] == REGION
    monkeypatch.setenv("AWS_ENDPOINT_URL_SQS", "http://localhost:4566")
    c = aws_reads.clients(REGION)
    assert c["sqs"].meta.endpoint_url == "http://localhost:4566"
    assert c["cloudwatch"].meta.endpoint_url != "http://localhost:4566"
    monkeypatch.delenv("AWS_ENDPOINT_URL_SQS")
    monkeypatch.setenv("AWS_ENDPOINT_URL", " http://emulator:9324 ")
    assert aws_reads.sqs_endpoint() == "http://emulator:9324"


def test_queue_settings(aws):
    url, dlq_arn = make_queues(aws["sqs"])
    aws["sqs"].send_message(QueueUrl=url, MessageBody="a")
    aws["sqs"].send_message(QueueUrl=url, MessageBody="b")
    s = aws_reads.read_queue_settings(aws["sqs"], url)
    assert s["arn"].endswith(":orders") and s["fifo"] is False
    assert (s["visibilityTimeoutS"], s["retentionS"], s["delayS"]) == (45, 86400, 2)
    assert (s["maxReceiveCount"], s["dlqArn"], s["dlqName"]) == (4, dlq_arn, "orders-dlq")
    assert s["inFlight"] == 0 and s["delayed"] is not None


def test_a_fifo_queue_and_one_with_no_redrive_policy(aws):
    url = aws["sqs"].create_queue(QueueName="pay.fifo", Attributes={"FifoQueue": "true"})["QueueUrl"]
    s = aws_reads.read_queue_settings(aws["sqs"], url)
    assert s["fifo"] is True and s["maxReceiveCount"] is None and s["dlqName"] is None


def test_a_queue_that_is_not_there(aws):
    with pytest.raises(ClientError) as e:
        aws_reads.read_queue_settings(aws["sqs"], f"https://sqs.{REGION}.amazonaws.com/123456789012/nope")
    assert "NonExistentQueue" in aws_reads.error_name(e.value)


def put(cw, queue, metric, points):
    cw.put_metric_data(Namespace="AWS/SQS", MetricData=[{
        "MetricName": metric, "Dimensions": [{"Name": "QueueName", "Value": queue}],
        "Timestamp": dt.datetime.fromtimestamp(t / 1000, tz=dt.timezone.utc), "Value": v} for t, v in points])


def test_metrics_for_queues_and_dead_letter_queues(aws):
    now = (int(dt.datetime.now(dt.timezone.utc).timestamp() * 1000) // MIN) * MIN
    start = now - 10 * MIN
    cw = aws["cloudwatch"]
    put(cw, "orders", "ApproximateNumberOfMessagesVisible", [(start + MIN, 3), (start + 2 * MIN, 5)])
    put(cw, "orders", "ApproximateAgeOfOldestMessage", [(start + 2 * MIN, 120)])
    put(cw, "orders", "NumberOfMessagesSent", [(start + MIN, 4), (start + MIN + 5000, 6)])
    put(cw, "orders-dlq", "ApproximateNumberOfMessagesVisible", [(start + MIN, 1)])
    put(cw, "orders-dlq", "NumberOfMessagesSent", [(start + MIN, 9)])  # not read: a DLQ has only its depth read
    r = aws_reads.read_queue_metrics(cw, ["orders"], ["orders-dlq"], start, now, 60)
    q = r["queues"]["orders"]
    assert q["visible"] == [[start + MIN, 3], [start + 2 * MIN, 5]]
    assert q["oldestS"] == [[start + 2 * MIN, 120]]
    assert q["sent"] == [[start + MIN, 10]]  # Sum over the minute
    assert q["inFlight"] == [] and q["deleted"] == []
    assert r["dlqs"] == {"orders-dlq": [[start + MIN, 1]]}


def test_only_the_metrics_asked_for(aws):
    calls = []

    class CW:
        def get_metric_data(self, **kw):
            calls.append(kw)
            return {"MetricDataResults": []}

    r = aws_reads.read_queue_metrics(CW(), ["a", "a", "b"], ["d"], T0 - 3_600_000, T0, 60, metrics=["oldestS"])
    names = [(q["MetricStat"]["Metric"]["Dimensions"][0]["Value"], q["MetricStat"]["Metric"]["MetricName"])
             for q in calls[0]["MetricDataQueries"]]
    assert names == [("a", "ApproximateAgeOfOldestMessage"), ("b", "ApproximateAgeOfOldestMessage"),
                     ("d", "ApproximateNumberOfMessagesVisible")]
    assert calls[0]["ScanBy"] == "TimestampAscending" and calls[0]["StartTime"].timestamp() * 1000 == T0 - 3_600_000
    assert set(r["queues"]) == {"a", "b"} and r["queues"]["a"]["visible"] == []


def test_pages_and_chunks_of_500(aws):
    """A long answer comes back in pages that can interleave; more than 500 queries take more than one call."""
    calls = []

    class CW:
        def get_metric_data(self, MetricDataQueries, NextToken=None, **kw):
            calls.append((len(MetricDataQueries), NextToken))
            first = MetricDataQueries[0]["Id"]
            t = dt.datetime.fromtimestamp(T0 / 1000, tz=dt.timezone.utc)
            later = t + dt.timedelta(minutes=1)
            if NextToken is None:
                return {"NextToken": "p2", "MetricDataResults": [
                    {"Id": first, "Timestamps": [later], "Values": [2.0]}, {"Id": "nope", "Timestamps": [t], "Values": [1]}]}
            return {"MetricDataResults": [{"Id": first, "Timestamps": [t, later, t], "Values": [1.0, 3.0, float("nan")]}]}

    queues = [f"q{i}" for i in range(101)]  # 505 queries
    r = aws_reads.read_queue_metrics(CW(), queues, [], T0, T0 + 120_000, 60)
    assert calls == [(500, None), (500, "p2"), (5, None), (5, "p2")]
    assert r["queues"]["q0"]["visible"] == [[T0, 1.0], [T0 + MIN, 3.0]]  # in time order, one point per time
    assert r["queues"]["q100"]["visible"] == [[T0, 1.0], [T0 + MIN, 3.0]]


def lambda_function(aws, name):
    iam = boto3.client("iam", region_name=REGION)
    role = iam.create_role(RoleName=f"{name}-role", AssumeRolePolicyDocument="{}")["Role"]["Arn"]
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("handler.py", "def handler(e, c):\n    return None\n")
    return aws["lambda"].create_function(FunctionName=name, Runtime="python3.12", Role=role, Handler="handler.handler",
                                         Code={"ZipFile": buf.getvalue()})["FunctionArn"]


def test_triggers(aws):
    url, _ = make_queues(aws["sqs"])
    arn = aws_reads.read_queue_settings(aws["sqs"], url)["arn"]
    lambda_function(aws, "billing")
    aws["lambda"].create_event_source_mapping(EventSourceArn=arn, FunctionName="billing", BatchSize=7,
                                              MaximumBatchingWindowInSeconds=5,
                                              FunctionResponseTypes=["ReportBatchItemFailures"],
                                              FilterCriteria={"Filters": [{"Pattern": '{"body":{"kind":["a"]}}'}]})
    [t] = aws_reads.read_triggers(aws["lambda"], arn)
    assert t["functionName"] == "billing" and t["functionArn"].endswith(":function:billing")
    assert (t["batchSize"], t["batchingWindowS"], t["reportBatchItemFailures"]) == (7, 5, True)
    assert (t["filters"], t["filterPatterns"]) == (1, ['{"body":{"kind":["a"]}}'])
    assert t["uuid"] and t["state"]
    assert aws_reads.read_triggers(aws["lambda"], arn.replace("orders", "other")) == []


def test_trigger_pages_and_odd_fields():
    calls = []

    class L:
        def list_event_source_mappings(self, EventSourceArn, Marker=None):
            calls.append(Marker)
            if Marker is None:
                return {"NextMarker": "m2", "EventSourceMappings": [
                    {"UUID": "u1", "FunctionArn": "arn:aws:lambda:eu-west-1:1:function:a:live", "State": "Enabled",
                     "BatchSize": 10, "ScalingConfig": {"MaximumConcurrency": 5}}]}
            return {"EventSourceMappings": [{"FunctionArn": None, "FilterCriteria": {"Filters": [{}, {"Pattern": "x"}]}}]}

    a, b = aws_reads.read_triggers(L(), "arn:aws:sqs:eu-west-1:1:q")
    assert calls == [None, "m2"]
    assert (a["functionName"], a["maxConcurrency"], a["reportBatchItemFailures"], a["filters"]) == ("a", 5, False, 0)
    assert (b["uuid"], b["functionName"], b["functionArn"], b["batchSize"]) == ("", "", None, None)
    assert (b["filters"], b["filterPatterns"]) == (2, ["x"])


def test_error_names():
    denied = ClientError({"Error": {"Code": "AccessDenied", "Message": "no"}}, "GetQueueAttributes")
    assert aws_reads.error_name(denied) == "AccessDenied"
    assert aws_reads.error_name(NoCredentialsError()) == "NoCredentialsError"
    assert aws_reads.error_name(ValueError("x")) == "ValueError"


def test_boto3_is_imported_only_for_clients():
    """A server with AWS off never imports boto3: importing the reads doesn't."""
    import subprocess
    import sys
    code = "import sys; import trailbeam.server.aws_reads, trailbeam.server.aws_figures; print('boto3' in sys.modules)"
    assert subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=True).stdout.strip() == "False"
