"""The real Trailbeam Python package, on a local SQS emulator (moto), exporting OTLP/HTTP to a running store.

Needs the `trailbeam` package from ../python and moto: pip install -e '../python[test]'. Skipped locally without
them, and a failure in CI (tests/needs.py).
No AWS: credentials are dummies and every call goes to the in-process emulator.
"""
import os
import time

import pytest
from needs import need

need("trailbeam.boto")
need("moto.server")

for _k in [k for k in os.environ if k.startswith("AWS_")]:
    del os.environ[_k]
os.environ.update(AWS_ACCESS_KEY_ID="test", AWS_SECRET_ACCESS_KEY="test", AWS_DEFAULT_REGION="us-east-1",
                  AWS_CONFIG_FILE=os.devnull, AWS_SHARED_CREDENTIALS_FILE=os.devnull,
                  AWS_EC2_METADATA_DISABLED="true")

KEY = "end-to-end-key-0123456789"


@pytest.fixture
def sqs():
    import boto3
    from moto.server import ThreadedMotoServer

    srv = ThreadedMotoServer(ip_address="127.0.0.1", port=0, verbose=False)
    srv.start()
    host, port = srv.get_host_and_port()
    client = boto3.session.Session().client("sqs", endpoint_url=f"http://{host}:{port}", region_name="us-east-1")
    yield client
    srv.stop()


def install(server_url):
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
    from opentelemetry.sdk.trace.export import SimpleSpanProcessor

    import trailbeam.boto
    return trailbeam.boto.install(KEY, span_processor=SimpleSpanProcessor(OTLPSpanExporter(endpoint=server_url + "/v1/traces")))


def test_batch_delete_from_another_process_joins_by_hash(server, store, sqs):
    import trailbeam.boto

    try:
        install(server)
        url = sqs.create_queue(QueueName="trailbeam-e2e")["QueueUrl"]
        sent = sqs.send_message_batch(QueueUrl=url, Entries=[{"Id": str(i), "MessageBody": f"b{i}"} for i in range(3)])
        mids = sorted(e["MessageId"] for e in sent["Successful"])
        got = sqs.receive_message(QueueUrl=url, MaxNumberOfMessages=10, WaitTimeSeconds=1)["Messages"]
        assert sorted(m["MessageId"] for m in got) == mids
        # A new installation has an empty handle-to-ID map, like a second process: the delete spans
        # carry only the receipt-handle hash.
        install(server)
        res = sqs.delete_message_batch(QueueUrl=url, Entries=[
            {"Id": f"e{i}", "ReceiptHandle": m["ReceiptHandle"]} for i, m in enumerate(got)])
        assert len(res["Successful"]) == 3
    finally:
        trailbeam.boto.uninstall()

    deadline = time.time() + 5
    while time.time() < deadline and len(store.query("SELECT row FROM spans")) < 9:
        time.sleep(0.05)
    dels = store.query("SELECT * FROM spans WHERE kind = 'delete'")
    assert len(dels) == 3
    assert all(d["message_id"] is None and d["mid_source"] == "receipt_hash" and d["result"] == "ok" for d in dels)
    assert sorted(d["mid"] for d in dels) == mids
    for m in mids:
        assert [r["kind"] for r in store.message_rows(m)] == ["send", "receive", "delete"]
