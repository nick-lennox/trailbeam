"""QV-516: Python's ``$`` also matches just before a final newline, so a value such as ``"abc\\n"`` passed the ID, ARN
and account checks that Node.js and the server reject. The vectors in contract/span-contract-vectors.json cover the
SNS, EventBridge, S3 notification and PutEvents fields; these cover the checks no vector reaches."""
import pytest

from trailbeam import _capture, _s3


def test_offloaded_size_rejects_a_trailing_newline():
    assert _capture.offloaded_size({"ExtendedPayloadSize": {"StringValue": "1024"}}) == 1024
    assert _capture.offloaded_size({"ExtendedPayloadSize": {"StringValue": "1024\n"}}) is None


def test_redact_key_rejects_a_trailing_newline():
    _capture.Config(redact_keys=("password",))
    with pytest.raises(ValueError):
        _capture.Config(redact_keys=("password\n",))


def test_upload_bucket_rejects_a_trailing_newline():
    config = _capture.Config()
    assert _s3.upload_attrs(config, "PutObject", {"Bucket": "orders", "Key": "k"}, {}, None) is not None
    assert _s3.upload_attrs(config, "PutObject", {"Bucket": "orders\n", "Key": "k"}, {}, None) is None
