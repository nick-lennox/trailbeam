"""Revision 3.7 (contract section 27): S3 uploads. One trailbeam s3_upload per PutObject, CompleteMultipartUpload or
CopyObject call, ok or failed. Shared vectors in contract/span-contract-vectors.json ("s3"); Node.js checks the same.
The end-to-end test uploads to a bucket whose notification goes to an SQS queue, in moto."""
import copy
import json
from pathlib import Path
from urllib.parse import unquote_plus

import boto3
import pytest
from botocore.exceptions import EndpointConnectionError
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

import trailbeam.boto
from trailbeam import _capture, _s3
from sdk_constants import TEST_KEY

VECTORS = json.loads(
    (Path(__file__).resolve().parents[2] / "contract" / "span-contract-vectors.json").read_text("utf-8"))["s3"]
UP = VECTORS["uploads"]


@pytest.fixture
def install():
    exporter = InMemorySpanExporter()

    def go(body_queues=()):
        trailbeam.boto.install(TEST_KEY, span_processor=SimpleSpanProcessor(exporter),
                               capture={"body_queues": list(body_queues)})
        return trailbeam.boto._state["hooks"]

    def spans():
        return [s for s in exporter.get_finished_spans() if s.instrumentation_scope.name == "trailbeam"]
    yield go, spans
    trailbeam.boto.uninstall()


def check(got, case):
    assert len(got) == len(case["spans"]), [dict(s.attributes) for s in got]
    for s, want in zip(got, case["spans"]):
        assert s.name == "trailbeam s3_upload"
        a = dict(s.attributes)
        assert {k: a.get(k) for k in want} == want
        extra = set(a) - set(want) - set(UP["present"]) - {"thread.name"}
        assert not extra, extra
        assert set(UP["present"]) <= set(a)
        assert not set(UP["absent"]) & set(a)
        assert not any("secret" in str(v) for v in a.values())
        assert a["trailbeam.order.issued"] < a["trailbeam.order.returned"]


@pytest.mark.parametrize("case", UP["cases"], ids=lambda c: c["name"])
def test_upload_vectors(install, case):
    go, spans = install
    h = go(case["body_queues"])
    span = object()
    params = copy.deepcopy(case["request"])
    h.request_hook(span, "s3", case["operation"], params)
    assert params == case["request"]  # the request is never changed
    result = ({"Error": {"Code": case["error"], "Message": "x"}} if "error" in case
              else copy.deepcopy(case["response"]))
    h.response_hook(span, "s3", case["operation"], result)
    assert params == case["request"]
    check(spans(), case)
    assert h._pending == {}


def test_a_call_that_raised_is_failed_with_the_exception_type(install):
    go, spans = install
    h = go()
    span = object()
    h.request_hook(span, "s3", "PutObject", {"Bucket": "photos", "Key": "a", "Body": b"x"})
    try:
        raise EndpointConnectionError(endpoint_url="http://nowhere")
    except EndpointConnectionError:
        h.response_hook(span, "s3", "PutObject", None)  # botocore calls the hook from a finally
    [s] = spans()
    assert s.attributes["trailbeam.result"] == "failed"
    assert s.attributes["trailbeam.error_code"] == "EndpointConnectionError"


def test_other_s3_operations_have_no_span(install):
    go, spans = install
    h = go(["*"])
    for op in ("GetObject", "DeleteObject", "UploadPart", "CreateMultipartUpload", "PutBucketNotificationConfiguration"):
        span = object()
        h.request_hook(span, "s3", op, {"Bucket": "photos", "Key": "a"})
        h.response_hook(span, "s3", op, {"ETag": '"x"'})
    assert spans() == [] and h._pending == {}


def test_a_key_longer_than_s3_allows_is_not_recorded():
    on = _capture.Config(body_queues=["*"])
    assert "trailbeam.s3.object_key" not in _s3.upload_attrs(on, "PutObject", {"Bucket": "b", "Key": "é" * 513}, {}, None)
    assert _s3.upload_attrs(on, "PutObject", {"Bucket": "b", "Key": "é" * 512}, {}, None)["trailbeam.s3.object_key"]


def test_uploads_end_to_end_in_moto(monkeypatch):
    """Real boto3 clients: each upload's span names the bucket, ETag and key the notification's receive names."""
    moto = pytest.importorskip("moto")
    for k, v in (("AWS_ACCESS_KEY_ID", "x"), ("AWS_SECRET_ACCESS_KEY", "x"), ("AWS_DEFAULT_REGION", "us-east-1")):
        monkeypatch.setenv(k, v)
    exporter = InMemorySpanExporter()
    trailbeam.boto.install(TEST_KEY, span_processor=SimpleSpanProcessor(exporter), capture={"body_queues": ["*"]})
    try:
        with moto.mock_aws():
            sqs, s3 = boto3.client("sqs"), boto3.client("s3")
            url = sqs.create_queue(QueueName="uploads")["QueueUrl"]
            arn = sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
            s3.create_bucket(Bucket="photos")
            s3.put_bucket_notification_configuration(Bucket="photos", NotificationConfiguration={
                "QueueConfigurations": [{"QueueArn": arn, "Events": ["s3:ObjectCreated:*"]}]})
            s3.put_object(Bucket="photos", Key="a dir/red flower+(1).jpg", Body=b"secret", Metadata={"m": "secret"})
            s3.copy_object(Bucket="photos", Key="copy.jpg", CopySource={"Bucket": "photos", "Key": "a dir/red flower+(1).jpg"})
            with pytest.raises(Exception):
                s3.put_object(Bucket="no-such-bucket", Key="a", Body=b"x")
            receives = []
            while True:
                got = sqs.receive_message(QueueUrl=url, MaxNumberOfMessages=10).get("Messages", [])
                if not got:
                    break
                receives += got
    finally:
        trailbeam.boto.uninstall()
    ups = [dict(s.attributes) for s in exporter.get_finished_spans() if s.name == "trailbeam s3_upload"]
    assert [(u["messaging.operation.name"], u["trailbeam.result"]) for u in ups] == [
        ("PutObject", "ok"), ("CopyObject", "ok"), ("PutObject", "failed")]
    assert ups[2]["trailbeam.error_code"] == "NoSuchBucket"
    recs = [dict(s.attributes) for s in exporter.get_finished_spans()
            if s.name == "trailbeam receive" and s.attributes.get("trailbeam.s3.event_name", "").startswith("ObjectCreated")]
    by_name = {r["trailbeam.s3.event_name"]: r for r in recs}
    for up, name in ((ups[0], "ObjectCreated:Put"), (ups[1], "ObjectCreated:Copy")):
        r = by_name[name]
        assert r["trailbeam.s3.bucket"] == up["messaging.destination.name"] == "photos"
        # the receive's key is URL-encoded as S3 writes it; the upload's is the key itself (section 27.3)
        assert unquote_plus(r["trailbeam.s3.object_key"]) == up["trailbeam.s3.object_key"]
    assert not any("secret" in str(v) for u in ups for v in u.values())
