"""QV-515 (contract sections 21.3 and 23): a receive parses a body at most once for its SNS, EventBridge and S3
readings, and not at all when the body is over the size or the object-and-array limit. Node.js checks the same
(node/test/envelope-limits.test.js)."""
import json
import time

import pytest

from trailbeam import _capture

MAX = _capture.MAX_ENVELOPE_BYTES
CONTAINERS = _capture.MAX_ENVELOPE_CONTAINERS
CONFIG = _capture.resolve_config({}, {}, {})
EVENT = {"version": "0", "id": "6a7e8feb-b491-4cf7-a9f1-bf3703467718", "detail-type": "Order placed",
         "source": "com.example.orders", "account": "111122223333", "time": "2026-11-24T10:00:00Z",
         "region": "us-east-2"}
EXPECTED = {"trailbeam.eventbridge.event_id": EVENT["id"], "trailbeam.eventbridge.source": "com.example.orders",
            "trailbeam.eventbridge.detail_type": "Order placed", "trailbeam.eventbridge.account": "111122223333",
            "trailbeam.eventbridge.region": "us-east-2", "trailbeam.eventbridge.time_ms": 1795514400000}


def event(detail, **extra) -> str:
    return json.dumps({**EVENT, **extra, "detail": detail}, ensure_ascii=False)


def padded(size: int, pad: str = "x") -> str:
    """An EventBridge event of exactly ``size`` UTF-8 bytes (``pad`` is one character)."""
    base = len(event({"pad": ""}).encode("utf-8"))
    n = (size - base) // len(pad.encode("utf-8"))
    body = event({"pad": pad * n})
    body = body.replace('"pad"', '"pad' + "y" * (size - len(body.encode("utf-8"))) + '"', 1)
    assert len(body.encode("utf-8")) == size
    return body


@pytest.fixture
def loads(monkeypatch):
    calls = []
    real = json.loads

    def spy(s, *a, **k):
        calls.append(len(s))
        return real(s, *a, **k)

    monkeypatch.setattr(json, "loads", spy)
    return calls


def receive(body):
    return _capture.receive_body_attrs(CONFIG, "orders", body, None)


def test_the_crafted_body_of_the_issue_is_not_parsed(loads):
    """About 1 MiB, both markers, arrays nested 500 deep: over the size limit, so never parsed."""
    unit = "[" * 500 + "]" * 500
    body = '{"Type":"Notification","detail-type":1,"x":[' + ",".join([unit] * (1048576 // 1001)) + "]}"
    assert len(body) > MAX
    assert receive(body) == {}
    assert _capture.sns_envelope(body) is None
    assert _capture.origin_receive_attrs(CONFIG, "orders", body) == {}
    assert loads == []


def test_too_many_objects_and_arrays_is_not_parsed(loads):
    unit = "[" * 500 + "]" * 500
    body = '{"Type":"Notification","detail-type":1,"x":[' + ",".join([unit] * 30) + "]}"
    assert len(body) < MAX
    assert receive(body) == {}
    assert loads == []


def test_a_body_with_both_markers_is_parsed_once(loads):
    """Not an SNS envelope (no TopicArn), but an EventBridge event: one parse serves both readings."""
    body = event({"note": "x"}, Type="Notification")
    assert receive(body) == EXPECTED
    assert len(loads) == 1


def test_an_sns_envelope_is_parsed_once(loads):
    body = json.dumps({"Type": "Notification", "MessageId": "22b80b92-fdea-4c2c-8f9d-bdfb0c7bf324",
                       "TopicArn": "arn:aws:sns:us-east-2:111122223333:orders", "detail-type": "x",
                       "Timestamp": "2026-11-24T10:00:00.000Z", "Message": "{}"})
    out = receive(body)
    assert out["trailbeam.sns.message_id"] == "22b80b92-fdea-4c2c-8f9d-bdfb0c7bf324"
    assert not any(k.startswith("trailbeam.eventbridge.") for k in out)
    assert len(loads) == 1


@pytest.mark.parametrize("pad", ["x", "é", "😀"])
def test_the_size_limit_is_in_utf8_bytes(pad):
    assert receive(padded(MAX, pad)) == EXPECTED
    assert receive(padded(MAX + 1, pad)) == {}
    assert _capture.origin_receive_attrs(CONFIG, "orders", padded(MAX + 1, pad)) == {}


def test_the_object_and_array_limit():
    # The event object and its detail array are two; the rest are empty arrays inside detail.
    assert receive(event([[]] * (CONTAINERS - 2))) == EXPECTED
    assert receive(event([[]] * (CONTAINERS - 1))) == {}
    assert receive(event([{}] * (CONTAINERS - 1))) == {}


def test_brackets_inside_strings_do_not_count():
    assert receive(event({"text": '[{\\"' * (CONTAINERS + 1)})) == EXPECTED


def test_an_unterminated_string_is_scanned_in_linear_time():
    body = '{"detail-type":"' + '\\"' * (MAX // 2 - 20)
    start = time.perf_counter()
    assert receive(body) == {}
    assert time.perf_counter() - start < 1.0
