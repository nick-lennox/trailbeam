"""Shared fixtures. No AWS: every AWS_* variable is removed and the SQS client talks to a local emulator.

By default the emulator is moto's server, started in-process on 127.0.0.1. Set TRAILBEAM_TEST_SQS_ENDPOINT
to use another local emulator instead, e.g. ElasticMQ (http://localhost:9324) or LocalStack
(http://localhost:4566).
"""
import os

for _k in [k for k in os.environ if k.startswith("AWS_")]:
    del os.environ[_k]
os.environ.update(
    AWS_ACCESS_KEY_ID="test",
    AWS_SECRET_ACCESS_KEY="test",
    AWS_DEFAULT_REGION="us-east-1",
    AWS_CONFIG_FILE=os.devnull,
    AWS_SHARED_CREDENTIALS_FILE=os.devnull,
    AWS_EC2_METADATA_DISABLED="true",
)
for _k in ("TRAILBEAM_RECEIPT_HASH_KEY", "OTEL_RESOURCE_ATTRIBUTES", "OTEL_SERVICE_NAME"):
    os.environ.pop(_k, None)

import uuid  # noqa: E402

import pytest  # noqa: E402

from sdk_constants import TEST_KEY  # noqa: E402,F401


@pytest.fixture(scope="session")
def sqs_endpoint():
    url = os.environ.get("TRAILBEAM_TEST_SQS_ENDPOINT")
    if url:
        yield url
        return
    from moto.server import ThreadedMotoServer

    server = ThreadedMotoServer(ip_address="127.0.0.1", port=0, verbose=False)
    server.start()
    host, port = server.get_host_and_port()
    yield f"http://{host}:{port}"
    server.stop()


@pytest.fixture
def make_client(sqs_endpoint):
    import boto3
    from botocore.config import Config

    def make(endpoint=None, **cfg):
        return boto3.session.Session().client(
            "sqs", endpoint_url=endpoint or sqs_endpoint, region_name="us-east-1",
            config=Config(**cfg) if cfg else None,
        )

    return make


@pytest.fixture
def queue_url(make_client):
    c = make_client()
    url = c.create_queue(QueueName=f"trailbeam-test-{uuid.uuid4().hex[:12]}")["QueueUrl"]
    yield url
    try:
        c.delete_queue(QueueUrl=url)
    except Exception:
        pass
