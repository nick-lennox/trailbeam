"""GET /api/timeline-events (QV-580): the Event explorer's events made from timelines, inferred ones included.

timeline_events.events_of_timeline is a port of @trailbeam/ui's eventsOfTimeline. Parity is checked both ways: here,
the port makes the events the TypeScript made of both backends' timelines (ui/test/fixtures/viewmodel-parity.json,
``expected``); in ui/test/timeline-events.test.ts, the TypeScript makes the events this endpoint answered of the
inputs it recorded (ui/test/fixtures/timeline-events-parity.json). This file also checks that recording is still what
the server answers. After a deliberate change, run ``python ui/test/make-timeline-events-fixture.py`` and ``npm test``
in ui/.
"""
import importlib.util
import json
import os
import sys
from pathlib import Path

import pytest

from spans import SEC, T0, Consumer
from trailbeam.server import server, timeline_events as te
from trailbeam.server.server import ApiError

ROOT = Path(__file__).resolve().parents[3]
VIEWMODEL = ROOT / "ui" / "test" / "fixtures" / "viewmodel-parity.json"
RECORDED = ROOT / "ui" / "test" / "fixtures" / "timeline-events-parity.json"
MS = 1_000_000


def _maker():
    spec = importlib.util.spec_from_file_location("make_timeline_events_fixture",
                                                  ROOT / "ui" / "test" / "make-timeline-events-fixture.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.mark.parametrize("backend", ["python", "hosted"])
def test_the_port_makes_the_events_eventsOfTimeline_made(backend):
    f = json.loads(VIEWMODEL.read_text())
    a = f["answers"][backend]
    places = {x["name"]: {"account": x["account"], "region": x["region"]} for x in a["flows"]}
    got = [e for t in a["timelines"] for e in te.events_of_timeline(t, places=places)]
    got.sort(key=lambda e: (-e["ms"], e["key"]))
    assert got == f["expected"]["events"]


def test_the_recorded_answer_is_the_servers():
    assert json.loads(json.dumps(_maker().record())) == json.loads(RECORDED.read_text())


@pytest.mark.skipif(not os.environ.get("TRAILBEAM_TEST_POSTGRES_URL"), reason="TRAILBEAM_TEST_POSTGRES_URL is not set")
def test_a_postgres_store_answers_the_same():
    sys.path.insert(0, str(ROOT / "python" / "tests" / "conformance"))
    from pg_helpers import postgres_store
    s = postgres_store(store_bodies=frozenset({"*"}))
    try:
        assert json.loads(json.dumps(_maker().record(s))) == json.loads(RECORDED.read_text())
    finally:
        s.close()


def test_the_recording_covers_what_the_typescript_must_agree_on():
    events = json.loads(RECORDED.read_text())["answer"]["events"]
    kinds = {(e["event"], e["source"]) for e in events}
    assert {("delete", "inferred"), ("processing_error", "inferred"), ("visibility_expired", "inferred"),
            ("dead_letter", "package"), ("processing_error", "package")} <= kinds
    assert any("body" in e for e in events) and any("sizeBytes" in e for e in events)


def test_a_cloudtrail_send_a_body_on_its_span_and_a_queue_place_from_its_url():
    """The TypeScript's own case (ui/test/viewmodel.test.ts), through the port."""
    t = {"message_id": "m1", "current_queue": "q", "events": [
        {"id": "e1", "type": "received", "queue": "q", "delivery": 1, "receive_count": 1, "end_unix_nano": "2000000000",
         "consumer": {"service_name": "w"}, "span": {"trace_id": "t", "span_id": "s"}}]}
    send, recv = te.events_of_timeline(
        t, urls={"q": "https://sqs.eu-west-1.amazonaws.com/123456789012/q"},
        body={"value": {"a": 1}, "traceId": "t", "spanId": "s"},
        trail_send={"queue": "q", "timeMs": 1000, "call": "SendMessage", "producer": "sns.amazonaws.com", "byService": True})
    assert send["source"] == "cloudtrail" and "body" not in send
    assert send["detail"] == "Recorded by CloudTrail (whole seconds). Sent by the AWS service sns.amazonaws.com"
    assert recv["producer"] == "sns.amazonaws.com" and recv["body"] == {"a": 1}
    assert "waitMs" not in recv  # a CloudTrail send is not an observed send, and SQS gave no SentTimestamp
    assert recv["attrs"]["queue.region"] == "eu-west-1" and recv["attrs"]["queue.account"] == "123456789012"
    assert te.parse_queue_url("http://localhost:9324/000000000000/orders") == {
        "name": "orders", "account": "000000000000", "region": None}
    assert te.parse_queue_url("https://queue.amazonaws.com/123456789012/a%20b")["region"] == "us-east-1"
    assert te.parse_queue_url("orders") is None
    assert te.handler_failed("KeyError", "orderId", "failed_exception") == "The handler threw: KeyError: orderId"
    assert te.handler_failed(None, None, None) == "The handler failed"
    m = lambda kind, tenant, size: {"kind": kind, "tenant": tenant, "tenantSource": None, "groupId": None,  # noqa: E731
                                    "sequenceNumber": None, "bodySize": size}
    assert te.meta_summary([m("receive", "b", 3), m("send", "a", None)])["tenant"] == "a"
    assert te.meta_summary([m("receive", "b", 3), m("send", "a", None)])["bodySize"] == 3


def _app(db, now_ns, lambda_timeout_s=60):
    return server.App(db, ui_dir=None, now=lambda: now_ns, lambda_timeout_s=lambda_timeout_s)


def _window(app, from_ns, to_ns, **q):
    return app.api("timeline-events", {"from_ms": str(from_ns // MS), "to_ms": str(to_ns // MS), **q})


def test_an_invocation_with_no_outcome_is_listed_when_only_its_timeout_is_in_the_window(db, ingest):
    """The inferred error is at the receive plus the Lambda timeout: its message has no span in the window."""
    fn = Consumer("env-1", source="lambda", service="orders-fn")
    ingest(fn.send(T0, "m1"), fn.receive(T0 + SEC, "m1", "rh-1", lambda_inv="inv-1"), service="orders-fn")
    a = _window(_app(db, T0 + 3600 * SEC), T0 + 30 * SEC, T0 + 120 * SEC)
    assert [(e["event"], e["source"], e["ms"]) for e in a["events"]] == [
        ("processing_error", "inferred", (T0 + 61 * SEC) // MS)]
    assert a["events"][0]["consumer"] == "orders-fn" and a["events"][0]["receiveCount"] == 1
    # and the whole story, newest first, when the window holds it
    whole = _window(_app(db, T0 + 3600 * SEC), T0, T0 + 120 * SEC)
    assert [e["event"] for e in whole["events"]] == ["processing_error", "receive", "send"]


def test_a_successful_handler_lists_the_inferred_delete_with_its_processing_time(db, ingest):
    fn = Consumer("env-1", source="lambda", service="orders-fn")
    ingest(Consumer("producer", service="checkout").send(T0, "m1"),
           fn.receive(T0 + SEC, "m1", "rh-1", lambda_inv="inv-1"),
           fn.outcome(T0 + 3 * SEC, "m1", "success", "inv-1"), service="orders-fn")
    a = _window(_app(db, T0 + 3600 * SEC), T0, T0 + 10 * SEC)
    d = a["events"][0]
    assert (d["event"], d["source"], d["call"], d["durationMs"]) == ("delete", "inferred", "Delete (inferred)", 2000)
    assert [e["event"] for e in a["events"]] == ["delete", "receive", "send"]


def test_the_most_recently_active_messages_up_to_the_limit(db, ingest):
    c = Consumer()
    ingest(*(c.send(T0 + i * SEC, f"m{i}") for i in range(3)))
    app = _app(db, T0 + 3600 * SEC)
    a = _window(app, T0, T0 + 10 * SEC, limit="2")
    assert (a["messages"], a["capped"], [e["messageId"] for e in a["events"]]) == (2, True, ["m2", "m1"])
    a = _window(app, T0, T0 + 10 * SEC)
    assert (a["messages"], a["capped"], a["queues"]) == (3, False, ["orders"])
    assert _window(app, T0 + 20 * SEC, T0 + 30 * SEC)["events"] == []


def test_window_messages_walks_past_a_page_of_spans(db, ingest, monkeypatch):
    monkeypatch.setattr(te, "_PAGE", 2)
    c = Consumer()
    ingest(*(sp for i in range(3) for sp in (c.send(T0 + i * SEC, f"m{i}"), c.receive(T0 + i * SEC + 1, f"m{i}", f"rh{i}"))))
    assert te.window_messages(db, T0, T0 + 10 * SEC, 10) == (["m2", "m1", "m0"], False)
    assert te.window_messages(db, T0, T0 + 10 * SEC, 3) == (["m2", "m1", "m0"], False)
    assert te.window_messages(db, T0, T0 + 10 * SEC, 2) == (["m2", "m1"], True)


def test_a_queue_asked_for_is_read_on_its_own_so_a_busier_one_cannot_crowd_it_out(db, ingest, monkeypatch):
    _quiet_and_busy(db, ingest, monkeypatch)


def test_a_queue_of_the_same_name_in_another_account_is_another_queue(db, ingest):
    _same_name_elsewhere(db, ingest)


@pytest.mark.skipif(not os.environ.get("TRAILBEAM_TEST_POSTGRES_URL"), reason="TRAILBEAM_TEST_POSTGRES_URL is not set")
def test_a_postgres_store_reads_one_queue_the_same(monkeypatch):
    sys.path.insert(0, str(ROOT / "python" / "tests" / "conformance"))
    from pg_helpers import postgres_store

    from trailbeam.server.otlp import decode
    from spans import to_proto
    for scenario in (_quiet_and_busy, _same_name_elsewhere):
        s = postgres_store()
        try:
            def ingest(*spans, service="orders-worker"):
                s.insert(decode(to_proto(list(spans), service=service).SerializeToString(),
                                "application/x-protobuf").records)
            scenario(s, ingest, monkeypatch)
        finally:
            s.close()


def _quiet_and_busy(db, ingest, monkeypatch):
    """QV-726: 3 messages on quiet, then 600 sends on busy. Unfiltered, the 500 most recently active are all busy's;
    asked for quiet (by key or by name), its 3 are read, with their events on it only."""
    monkeypatch.setattr(te, "_PAGE", 50)
    c = Consumer()
    ingest(*(sp for i in range(3) for sp in (c.send(T0 + i * SEC, f"q{i}", queue="quiet"),
                                             c.receive(T0 + i * SEC + 1, f"q{i}", f"rh{i}", queue="quiet"))))
    ingest(*(c.send(T0 + 60 * SEC + i * MS, f"b{i}", queue="busy") for i in range(600)))
    app = _app(db, T0 + 3600 * SEC)
    every = _window(app, T0, T0 + 3600 * SEC, limit="500")
    assert (every["messages"], every["capped"], every["queues"]) == (500, True, ["busy"])
    for asked in ("quiet", "000000000000//quiet"):
        a = _window(app, T0, T0 + 3600 * SEC, limit="500", queue=asked)
        assert (a["messages"], a["capped"], a["queues"]) == (3, False, ["quiet"])
        assert [e["event"] for e in a["events"] if e["messageId"] == "q0"] == ["receive", "send"]
    a = _window(app, T0, T0 + 3600 * SEC, limit="500", queue="busy")
    assert (a["messages"], a["capped"]) == (500, True)
    a = _window(app, T0, T0 + 3600 * SEC, limit="500", queue="quiet,busy")
    assert (a["messages"], a["capped"]) == (500, True) and {e["queue"] for e in a["events"]} == {"busy"}
    assert _window(app, T0, T0 + 3600 * SEC, queue="nowhere")["events"] == []


def _same_name_elsewhere(db, ingest, monkeypatch=None):
    elsewhere = "https://sqs.eu-west-1.amazonaws.com/123456789012/orders"
    mine, theirs = Consumer("a"), Consumer("b")
    ingest(mine.send(T0, "x1"))
    spans = [theirs.send(T0 + SEC, "y1"), theirs.receive(T0 + 2 * SEC, "y1", "hy")]
    for sp in spans:
        sp.attrs["aws.sqs.queue.url"] = elsewhere
    ingest(*spans)
    app = _app(db, T0 + 3600 * SEC)

    def mids(**q):
        return sorted({e["messageId"] for e in _window(app, T0, T0 + 10 * SEC, **q)["events"]})
    assert mids() == ["x1", "y1"]
    assert mids(queue="000000000000//orders") == ["x1"]
    assert mids(queue="123456789012/eu-west-1/orders") == ["y1"]
    assert mids(queue="000000000000//orders,123456789012/eu-west-1/orders") == ["x1", "y1"]
    with pytest.raises(ApiError) as e:  # the name alone is two queues
        _window(app, T0, T0 + 10 * SEC, queue="orders")
    assert e.value.status == 400 and "more than one queue is called orders" in str(e.value)


def test_a_move_to_the_dead_letter_queue_is_found_by_asking_for_the_dead_letter_queue_too(db, ingest):
    """The move is reported on the receive from the dead-letter queue: when that is the message's only span in the
    window, asking for the source queue alone doesn't find it; asking for both does, and the move is on the source."""
    c = Consumer()
    ingest(c.send(T0, "m1"), c.receive(T0 + SEC, "m1", "rh1"),
           c.receive(T0 + 600 * SEC, "m1", "rh2", queue="orders-dlq", dlq_source="orders"))
    app = _app(db, T0 + 3600 * SEC)
    alone = _window(app, T0 + 300 * SEC, T0 + 900 * SEC, queue="orders")
    both = _window(app, T0 + 300 * SEC, T0 + 900 * SEC, queue="orders,orders-dlq")
    assert alone["messages"] == 0
    assert both["messages"] == 1 and [e["queue"] for e in both["events"] if e["event"] == "dead_letter"] == ["orders"]


def test_bodies_only_for_the_queues_the_server_stores_them_for():
    """With TRAILBEAM_STORE_BODIES unset the same events come without bodies; and bodies stop at the page's budget."""
    f = json.loads(RECORDED.read_text())
    now_ns = f["nowMs"] * MS
    every = te.page(_maker_db(frozenset({"*"})), f["fromMs"], f["toMs"], now_ns, 900)
    none = te.page(_maker_db(frozenset()), f["fromMs"], f["toMs"], now_ns, 900)
    assert any("body" in e for e in every["events"]) and not any("body" in e for e in none["events"])
    assert [e["key"] for e in none["events"]] == [e["key"] for e in every["events"]]
    few = te.page(_maker_db(frozenset({"*"})), f["fromMs"], f["toMs"], now_ns, 900, max_body_chars=1)
    assert few["bodies_capped"] and not every["bodies_capped"] and not any("body" in e for e in few["events"])


def _maker_db(stores):
    import base64

    from trailbeam.server.db import Database
    from trailbeam.server.otlp import decode
    db = Database(":memory:", store_bodies=stores)
    for e in json.loads((ROOT / "contract" / "fixtures" / "contract-3.2-spans.json").read_text())["exports"]:
        db.insert(decode(base64.b64decode(e["base64"]), e["contentType"]).records)
    return db


def test_bad_requests(db):
    app = _app(db, T0)
    with pytest.raises(ApiError) as e:
        app.api("timeline-events/x", {})
    assert e.value.status == 404
    with pytest.raises(ApiError) as e:
        app.api("timeline-events", {"from_ms": "2", "to_ms": "1"})
    assert e.value.status == 400
    with pytest.raises(ApiError):
        app.api("timeline-events", {"limit": "2001"})
    with pytest.raises(ApiError) as e:
        app.api("timeline-events", {"queue": ",".join(f"q{i}" for i in range(te.MAX_QUEUES + 1))})
    assert e.value.status == 400
