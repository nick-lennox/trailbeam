"""The Body tab and body.* search (contract/body-view.md) on the recorded contract 3.2 spans.

The expected outputs are contract/fixtures/body-views.json, which the hosted app checks its own
implementation against too. If a rule changes, rerun contract/fixtures/make-body-views.py and update the
app in the same change.
"""
import base64
import importlib.util
import json
import threading
import urllib.error
import urllib.parse
import urllib.request
from html import unescape
from pathlib import Path

import pytest

from trailbeam.server import bodies
from trailbeam.server.db import Database
from trailbeam.server.otlp import decode
from trailbeam.server.server import make_server

FIXTURES = Path(__file__).resolve().parents[3] / "contract" / "fixtures"
EXPECTED = json.loads((FIXTURES / "body-views.json").read_text())
SPANS = json.loads((FIXTURES / "contract-3.2-spans.json").read_text())
CASES = {c["name"]: c for c in EXPECTED["cases"]}


def load(store_bodies, services=None) -> Database:
    db = Database(store_bodies=frozenset(store_bodies))
    for e in SPANS["exports"]:
        if services is None or e["service"] in services:
            db.insert(decode(base64.b64decode(e["base64"]), e["contentType"]).records)
    return db


@pytest.mark.parametrize("name", list(CASES))
def test_views_and_searches_match_the_shared_fixture(name):
    case = CASES[name]
    db = load(case["store_bodies"], case["services"])
    mids = [r["mid"] for r in db.query("SELECT DISTINCT mid FROM spans WHERE mid IS NOT NULL ORDER BY mid")]
    assert {m: bodies.body_view(m, db.message_rows(m), db.store_bodies) for m in mids} == case["views"]
    for s in case["searches"]:
        assert sorted(bodies.search(db, bodies.parse_query(s["query"]), db.store_bodies)) == s["message_ids"], s["query"]
    db.close()


@pytest.mark.parametrize("t", EXPECTED["terms"], ids=lambda t: t["query"])
def test_term_cases(t):
    assert bodies.matches(bodies.parsed_body(t["body"], t["format"]), bodies.parse_query(t["query"])) is t["matches"]


def test_the_fixture_is_current():
    """body-views.json is what make-body-views.py writes now."""
    spec = importlib.util.spec_from_file_location("make_body_views", FIXTURES / "make-body-views.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    cases = []
    for case in mod.CASES:
        db = load(case["store_bodies"], case["services"])
        mids = [r["mid"] for r in db.query("SELECT DISTINCT mid FROM spans WHERE mid IS NOT NULL ORDER BY mid")]
        cases.append({**case, "views": {m: bodies.body_view(m, db.message_rows(m), db.store_bodies) for m in mids},
                      "searches": [{"query": q, "message_ids": sorted(bodies.search(db, bodies.parse_query(q), db.store_bodies))}
                                   for q in mod.SEARCHES]})
        db.close()
    assert cases == EXPECTED["cases"]
    assert [(t["body"], t["format"], t["query"], t["matches"]) for t in EXPECTED["terms"]] == [tuple(x) for x in mod.TERMS]


def test_the_rules_the_app_also_tests():
    """The same facts trailbeam-app's test/contract-3.2.test.ts asserts, read from the shared fixture."""
    orders = CASES["orders"]["views"]
    v = orders["m-orders-1"]
    assert (v["kind"], v["body"]["from_kind"], v["body"]["service"], v["body"]["format"], v["body"]["redacted_count"],
            v["size"], v["tenant"]) == ("body", "send", "checkout-api", "json", 3, 135, "acme")
    assert json.loads(v["body"]["body"]) == {"customer": {"id": "acme", "email": "[REDACTED]"},
                                             "order": {"sku": "A-100", "qty": 2}, "card": "[REDACTED]",
                                             "password": "[REDACTED]"}
    assert orders["m-orphan"]["body"]["from_kind"] == "receive"  # its producer runs no package
    assert (orders["m-pay-1"]["reason"], orders["m-pay-1"]["size"]) == ("off", 40)
    assert orders["m-legacy"]["reason"] == "package_too_old"
    assert orders["m-bill-1"]["reason"] == "not_opted_in"  # billing captured it; the server is not keeping billing
    assert CASES["default"]["views"]["m-orders-1"]["reason"] == "not_opted_in"
    pay = CASES["consumer only"]["views"]["m-pay-1"]
    assert (pay["reason"], pay["receive_status"]) == ("producer_not_instrumented", "off")


def test_a_later_send_replaces_a_receives_copy_and_nothing_replaces_the_send():
    db = Database(store_bodies=frozenset({"orders"}))
    by = {e["service"]: e for e in SPANS["exports"]}
    for s in ("orders-worker", "checkout-api", "orders-worker"):  # the receive arrives first
        e = by[s]
        db.insert(decode(base64.b64decode(e["base64"]), e["contentType"]).records)
    got = {m: bodies.body_view(m, db.message_rows(m), db.store_bodies)["body"]["from_kind"]
           for m in ("m-orders-1", "m-orders-2", "m-orphan")}
    assert got == {"m-orders-1": "send", "m-orders-2": "send", "m-orphan": "receive"}


def test_a_queue_taken_off_the_list_is_no_longer_shown_or_searched(tmp_path):
    path = str(tmp_path / "t.sqlite")
    db = Database(path, store_bodies=frozenset({"orders"}))
    for e in SPANS["exports"]:
        db.insert(decode(base64.b64decode(e["base64"]), e["contentType"]).records)
    db.close()
    db = Database(path, store_bodies=frozenset({"billing"}))  # restarted with another list
    v = bodies.body_view("m-orders-1", db.message_rows("m-orders-1"), db.store_bodies)
    assert (v["kind"], v["reason"]) == ("none", "not_opted_in")
    assert bodies.search(db, bodies.parse_query("body.order.sku:A-100"), db.store_bodies) == []
    # the file still holds it: nothing is deleted
    assert db.query("""SELECT COUNT(*) AS n FROM spans WHERE mid = 'm-orders-1'
                       AND json_type(attributes, '$."trailbeam.body"') = 'text'""")[0]["n"] == 2
    db.close()


@pytest.mark.parametrize("q", ["queue:orders", "acme", "body", "-", ""])
def test_only_body_terms(q):
    with pytest.raises(bodies.QueryError):
        bodies.parse_query(q)


# ---- over HTTP --------------------------------------------------------------------------------


@pytest.fixture
def served():
    db = load(["orders"])
    httpd = make_server(db, "127.0.0.1", 0, ui_dir=None)
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{httpd.server_address[1]}"
    httpd.shutdown()
    httpd.server_close()
    db.close()


def get(url, path):
    try:
        with urllib.request.urlopen(url + path, timeout=5) as r:
            return r.status, r.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()


def test_api_body(served):
    for mid, want in CASES["orders"]["views"].items():
        status, text = get(served, f"/api/messages/{urllib.parse.quote(mid, safe='')}/body")
        assert (status, json.loads(text)) == (200, want)
    assert get(served, "/api/messages/nope/body")[0] == 404
    assert get(served, "/api/messages/m-orders-1/other")[0] == 404


def test_api_search(served):
    status, text = get(served, "/api/messages?" + urllib.parse.urlencode({"body": "body.order.qty:>=2"}))
    d = json.loads(text)
    assert status == 200 and d["body_search"] == {"stores_bodies": True}
    assert sorted(m["message_id"] for m in d["messages"]) == ["m-orders-1", "m-orphan"]
    assert [m["message_id"] for m in json.loads(get(served, "/api/messages?" + urllib.parse.urlencode(
        {"body": "body.order.qty:>=2", "limit": "1"}))[1])["messages"]] == [d["messages"][0]["message_id"]]
    assert json.loads(get(served, "/api/messages?" + urllib.parse.urlencode(
        {"body": "body.order.qty:>=2", "queue": "billing"}))[1])["messages"] == []
    status, text = get(served, "/api/messages?" + urllib.parse.urlencode({"body": "tenant:acme"}))
    assert status == 400 and "body" in json.loads(text)["error"]


def test_messages_page_searches_bodies_and_says_who_can_read_them(served):
    status, html = get(served, "/?" + urllib.parse.urlencode({"body": "body.customer.id:globex"}))
    assert status == 200
    text = unescape(html)
    assert "Messages whose body matches" in text and "m-orders-2" in text and "m-orders-1" not in text
    assert "anyone who can reach this server can read them" in text
    status, html = get(served, "/?" + urllib.parse.urlencode({"body": "queue:orders"}))
    assert status == 400


def test_messages_page_says_when_the_server_keeps_no_bodies():
    db = load([])
    httpd = make_server(db, "127.0.0.1", 0, ui_dir=None)
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    try:
        _, html = get(f"http://127.0.0.1:{httpd.server_address[1]}", "/?body=body:acme")
        assert "This server keeps no bodies" in unescape(html)
    finally:
        httpd.shutdown()
        httpd.server_close()
        db.close()


POINTER = ('["software.amazon.payloadoffloading.PayloadS3Pointer",{"s3BucketName":"payloads-123456789012",'
           '"s3Key":"61fc25e3-8ae5-4569-9189-43c27e5ba06f"}]')


def _row(row, kind, attributes, span_id):
    return {"row": row, "kind": kind, "attributes": json.dumps(attributes), "queue": "orders", "service_name": "svc",
            "trace_id": "t1", "span_id": span_id, "time_ns": row, "body_dropped": None}


def test_a_kept_s3_pointer_has_its_capture_status_and_offloaded_size():
    pointer = {"trailbeam.body": POINTER, "trailbeam.body.format": "json", "trailbeam.body.redacted_count": 0,
               "trailbeam.body.capture_status": "offloaded_to_s3", "messaging.message.body.size": 145}
    rows = [_row(1, "send", {**pointer, "trailbeam.body.offloaded_size": 300000}, "s1"),
            _row(2, "receive", {**pointer, "trailbeam.body.offloaded_size": 7}, "s2")]
    v = bodies.body_view("m1", rows, frozenset(["orders"]))
    assert (v["kind"], v["body"]["capture_status"], v["offloaded_size"], v["size"]) == (
        "body", "offloaded_to_s3", 300000, 145)
    # The kept span has no size: the first span with one, sends first.
    rows = [_row(1, "send", pointer, "s1"), _row(2, "receive", {**pointer, "trailbeam.body.offloaded_size": 7}, "s2")]
    assert bodies.body_view("m1", rows, frozenset(["orders"]))["offloaded_size"] == 7
    # A span from a store with no status.
    old = {k: v for k, v in pointer.items() if k != "trailbeam.body.capture_status"}
    v = bodies.body_view("m1", [_row(1, "send", old, "s1")], frozenset(["orders"]))
    assert (v["body"]["capture_status"], v["offloaded_size"]) == (None, None)
