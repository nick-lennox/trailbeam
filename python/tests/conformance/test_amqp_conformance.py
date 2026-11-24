"""RabbitMQ spans (contract/span-contract-amqp.md, schema version 4), through the Store interface only: every backend
stores the amqp.* spans of the contract's fixture, keys their messages by (system, message ID) so that an SQS message
and a RabbitMQ message with the same ID text stay two messages, joins each settle to its delivery by channel and
delivery tag, counts a fanout copy per queue, and drops spans of no message by their own time (QV-306)."""
import hashlib
import json
import sys
from pathlib import Path

import pytest

from trailbeam.server import queries, systems
from trailbeam.server.otlp import decode
from trailbeam.server.store import Cutoffs

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "server"))
from spans import SEC, Consumer, to_proto  # noqa: E402

FIXTURE = json.loads((Path(__file__).resolve().parents[3] / "contract" / "fixtures" / "amqp-4.0-spans.json")
                     .read_text("utf-8"))
FOREVER = 2 ** 63 - 1
LT = 900


def _value(v):
    if isinstance(v, bool):
        return {"boolValue": v}
    if isinstance(v, int):
        return {"intValue": str(v)}
    return {"stringValue": str(v)}


def otlp_json(spans) -> bytes:
    """The fixture's spans as one OTLP/JSON export, a resource per span; trace and span IDs made from the span's
    place in the fixture, so a span has the same IDs in every export."""
    out = []
    for sp in spans:
        i = next((n for n, x in enumerate(FIXTURE["spans"]) if x is sp or x == sp), id(sp))
        h = hashlib.sha256(f"{sp['scenario']}/{i}/{sp['attributes'].get('trailbeam.amqp.vhost')}".encode()).hexdigest()
        out.append({"resource": {"attributes": [{"key": k, "value": _value(v)} for k, v in sp["resource"].items()]},
                    "scopeSpans": [{"scope": {"name": "trailbeam"}, "spans": [{
                        "traceId": h[:32], "spanId": h[32:48], "name": sp["name"],
                        "startTimeUnixNano": str(sp["start_time_unix_nano"]),
                        "endTimeUnixNano": str(sp["end_time_unix_nano"]),
                        "attributes": [{"key": k, "value": _value(v)} for k, v in sp["attributes"].items()]}]}]})
    return json.dumps({"resourceSpans": out}).encode()


def load(s, scenarios=None):
    spans = [sp for sp in FIXTURE["spans"] if scenarios is None or sp["scenario"] in scenarios]
    d = decode(otlp_json(spans), "application/json")
    assert d.rejected == []
    assert s.insert(d.records) == (len(spans), 0)
    return d.records


def key(mid):
    return systems.message_key(systems.RABBITMQ, mid)


def summary(s, mid):
    (one,) = s.summaries([key(mid)])
    return one


def test_every_fixture_span_is_stored_with_the_kind_of_its_step(new_store):
    s = new_store()
    load(s)
    counts = s.counts()
    by_kind = {}
    for sp in FIXTURE["spans"]:
        a = sp["attributes"]
        k = {"amqp.publish": "send", "amqp.return": "return", "amqp.deliver": "receive", "amqp.ack": "delete",
             "amqp.cancel": "cancel", "amqp.channel_closed": "channel_closed"}.get(a["trailbeam.kind"])
        if k is None:
            k = "change_visibility" if a["trailbeam.amqp.requeue"] else "reject"
        by_kind[k] = by_kind.get(k, 0) + 1
    assert counts["spans"] == by_kind
    # every settle of a delivery with a message ID found it; those without one join nothing (A6)
    no_id = [sp for sp in FIXTURE["spans"] if "messaging.message.id" not in sp["attributes"]
             and sp["attributes"]["trailbeam.kind"] in ("amqp.ack", "amqp.nack", "amqp.reject", "amqp.channel_closed")]
    assert no_id and counts["unjoined_settles"] == len(no_id)
    mids = {sp["attributes"].get("messaging.message.id") for sp in FIXTURE["spans"]} - {None}
    assert counts["messages"] == len(mids)


def test_settles_join_their_delivery_by_channel_and_tag_whichever_comes_first(new_store):
    s = new_store()
    spans = [sp for sp in FIXTURE["spans"] if sp["scenario"] == "requeue-once"]
    settles = [sp for sp in spans if sp["attributes"]["trailbeam.kind"] != "amqp.deliver"]
    rest = [sp for sp in spans if sp not in settles]
    for part in (settles, rest):  # the settles first: the deliveries join them when they come
        d = decode(otlp_json(part), "application/json")
        s.insert(d.records)
    rows = s.message_rows(key("m-requeue"))
    receives = {r["row"]: r for r in rows if r["kind"] == "receive"}
    joined = [r for r in rows if r["kind"] in ("delete", "change_visibility")]
    assert len(receives) == 2 and len(joined) == 2
    for r in joined:
        assert r["mid_source"] == "receipt_hash"
        assert r["receipt_hmac"] == receives[r["receive_row"]]["receipt_hmac"]
    assert summary(s, "m-requeue")["final_delete"]


def test_an_sqs_message_and_a_rabbitmq_message_with_one_id_are_two_messages(new_store):
    """A6: the key is (system, message ID). The SQS message keeps its ID as its key, as before 4.0."""
    s = new_store()
    load(s, {"happy"})
    c = Consumer()
    sqs = decode(to_proto([c.send(FIXTURE["spans"][0]["end_time_unix_nano"] + SEC, "m-happy")]).SerializeToString(),
                 "application/x-protobuf")
    assert sqs.rejected == [] and s.insert(sqs.records) == (1, 0)
    sqs_rows, amqp_rows = s.message_rows("m-happy"), s.message_rows(key("m-happy"))
    assert [r["kind"] for r in sqs_rows] == ["send"] and sqs_rows[0]["queue"] == "orders"
    assert sorted(r["kind"] for r in amqp_rows) == ["delete", "receive", "send"]
    assert {r["mid"] for r in amqp_rows} == {"~rabbitmq~m-happy"}
    assert {x["mid"] for x in s.summaries(["m-happy", key("m-happy")])} == {"m-happy", "~rabbitmq~m-happy"}
    assert s.counts()["messages"] == 2
    assert s.queue_messages("orders") >= {"m-happy", key("m-happy")}  # one queue name, two messages


def test_a_fanout_copy_on_each_queue_is_not_a_duplicate_but_a_retry_is(new_store):
    s = new_store()
    load(s, {"fanout", "publisher-retry"})
    fan = summary(s, "m-fan")
    assert fan["deliveries"] == 2 and not fan["dup_received"] and not fan["dup_succeeded"]
    assert fan["final_delete"] and not fan["stuck_open"]
    retry = summary(s, "m-retry")
    assert retry["dup_received"] and retry["dup_succeeded"]
    assert s.duplicate_messages(10) == [key("m-retry")]


def test_a_copy_left_unacked_keeps_the_message_open(new_store):
    s = new_store()
    load(s, {"stuck", "happy"})
    assert summary(s, "m-stuck")["stuck_open"] and not summary(s, "m-happy")["stuck_open"]
    end = max(sp["end_time_unix_nano"] for sp in FIXTURE["spans"])
    assert [c.message_id for c in s.stuck_messages(end + SEC, 10)] == [key("m-stuck")]


def test_spans_without_a_message_id_belong_to_no_message_and_expire_by_their_own_time(new_store):
    s = new_store()
    records = load(s, {"no-message-id", "consumer-timeout"})
    loose = [r for r in records if r["message_id"] is None]
    assert loose and all(r["receipt_hmac"] is None for r in loose)  # joins nothing
    assert any(r["kind"] == "cancel" for r in loose)
    assert s.counts()["messages"] == 1  # m-timeout; the no-message-id spans are counted, not a message
    resume = None
    while True:
        step = s.purge_step(Cutoffs(FOREVER), resume, 500)
        resume = step.resume
        if resume is None:
            break
    assert s.counts()["spans"] == {} and s.counts()["messages"] == 0


def test_the_timeline_of_every_fixture_message_builds(new_store):
    """What the timeline says about RabbitMQ is QV-307's; here: every message's page is built from stored rows."""
    s = new_store()
    load(s)
    now = max(sp["end_time_unix_nano"] for sp in FIXTURE["spans"]) + SEC
    for mid in sorted({sp["attributes"].get("messaging.message.id") for sp in FIXTURE["spans"]} - {None}):
        t = queries.message(s, key(mid), now, LT)
        assert t is not None and t["events"], mid


@pytest.mark.parametrize("vhost", ["/", "staging"])
def test_queues_of_two_virtual_hosts_are_two_queue_keys(vhost):
    spans = [dict(sp, attributes={**sp["attributes"], "trailbeam.amqp.vhost": vhost})
             for sp in FIXTURE["spans"] if sp["scenario"] == "happy"]
    d = decode(otlp_json(spans), "application/json")
    deliver = next(r for r in d.records if r["kind"] == "receive")
    assert deliver["queue"] == "orders"
    assert deliver["queue_url"] == ("amqp://rabbitmq/%2F/orders" if vhost == "/" else "amqp://rabbitmq/staging/orders")
    publish = next(r for r in d.records if r["kind"] == "send")
    assert publish["queue"] == "amq.default" and "/exchange/" in publish["queue_url"]


def test_a_message_with_only_a_return_has_no_timeline_yet(new_store):
    """Its return is stored and counted; the timeline shows returns from QV-307, so until then the message page
    finds nothing to show rather than failing."""
    s = new_store()
    d = decode(otlp_json([sp for sp in FIXTURE["spans"] if sp["attributes"]["trailbeam.kind"] == "amqp.return"]),
               "application/json")
    assert s.insert(d.records) == (1, 0) and s.counts()["spans"] == {"return": 1}
    assert queries.message(s, key("m-noroute"), 2 ** 62, LT) is None
