"""Record the spans the smoke test sends: real qv2 boto3 hooks on moto's local SQS emulator (no AWS).

    pip install -e '../../python[test]' -e '..'
    python record_spans.py            # rewrites recorded-spans.pb and recorded-spans.json

Writes the spans as one OTLP/HTTP protobuf request body (``recorded-spans.pb``, what a qv2 package
exports) and what the smoke test should find (``recorded-spans.json``). Scenario on queue ``qv2-smoke``:

- ``redelivered``: received with a 1 s visibility timeout and not deleted, received again after it
  expired, then deleted with the second handle. Its timeline has an inferred visibility-timeout expiry.
- ``released``: received, made visible again with ``ChangeMessageVisibility`` 0, received and deleted.
- ``never-received``: sent only, so it is stuck (with ``older_than_s=0``).
"""
from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path

for _k in [k for k in os.environ if k.startswith("AWS_")]:
    del os.environ[_k]
os.environ.update(AWS_ACCESS_KEY_ID="test", AWS_SECRET_ACCESS_KEY="test", AWS_DEFAULT_REGION="us-east-1",
                  AWS_CONFIG_FILE=os.devnull, AWS_SHARED_CREDENTIALS_FILE=os.devnull,
                  AWS_EC2_METADATA_DISABLED="true", OTEL_SERVICE_NAME="qv2-smoke-worker")

import boto3  # noqa: E402
from moto.server import ThreadedMotoServer  # noqa: E402
from opentelemetry.exporter.otlp.proto.common.trace_encoder import encode_spans  # noqa: E402
from opentelemetry.sdk.trace.export import SimpleSpanProcessor  # noqa: E402
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter  # noqa: E402

import qv2.boto  # noqa: E402

HERE = Path(__file__).resolve().parent
KEY = "smoke-test-key-0123456789"  # not a secret: only hashes emulator receipt handles


def only(resp: dict) -> dict:
    msgs = resp.get("Messages", [])
    assert len(msgs) == 1, msgs
    return msgs[0]


def main() -> None:
    srv = ThreadedMotoServer(ip_address="127.0.0.1", port=0, verbose=False)
    srv.start()
    host, port = srv.get_host_and_port()
    exporter = InMemorySpanExporter()
    qv2.boto.install(KEY, span_processor=SimpleSpanProcessor(exporter))
    try:
        sqs = boto3.session.Session().client("sqs", endpoint_url=f"http://{host}:{port}", region_name="us-east-1")
        url = sqs.create_queue(QueueName="qv2-smoke")["QueueUrl"]

        def send(body):
            return sqs.send_message(QueueUrl=url, MessageBody=body)["MessageId"]

        def receive(vis):
            return only(sqs.receive_message(QueueUrl=url, MaxNumberOfMessages=1, VisibilityTimeout=vis,
                                            WaitTimeSeconds=2, AttributeNames=["All"]))

        redelivered = send("redelivered")
        first = receive(1)
        assert first["MessageId"] == redelivered
        time.sleep(1.5)
        second = receive(30)
        assert second["MessageId"] == redelivered and second["Attributes"]["ApproximateReceiveCount"] == "2"
        sqs.delete_message(QueueUrl=url, ReceiptHandle=second["ReceiptHandle"])

        released = send("released")
        m = receive(30)
        sqs.change_message_visibility(QueueUrl=url, ReceiptHandle=m["ReceiptHandle"], VisibilityTimeout=0)
        m2 = receive(30)
        assert m2["MessageId"] == released
        sqs.delete_message(QueueUrl=url, ReceiptHandle=m2["ReceiptHandle"])

        never = send("never-received")
    finally:
        qv2.boto.uninstall()
        srv.stop()

    spans = exporter.get_finished_spans()
    (HERE / "recorded-spans.pb").write_bytes(encode_spans(spans).SerializeToString())
    expected = {
        "queue": "qv2-smoke",
        "redelivered": {"message_id": redelivered,
                        "events": ["sent", "received", "visibility_timeout_expired", "received", "deleted"]},
        "released": {"message_id": released,
                     "events": ["sent", "received", "visibility_changed", "visibility_timeout_expired",
                                "received", "deleted"]},
        "never_received": {"message_id": never, "events": ["sent"]},
        "qv2_span_count": sum(s.instrumentation_scope.name == "qv2" for s in spans),
    "span_count": len(spans),
    }
    (HERE / "recorded-spans.json").write_text(json.dumps(expected, indent=2) + "\n")
    print(f"recorded {len(spans)} spans", file=sys.stderr)


if __name__ == "__main__":
    main()
