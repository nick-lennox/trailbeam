"""The JSON API over HTTP (https://docs.trailbeam.app/reference/api)."""
import json
import urllib.error
import urllib.parse
import urllib.request

import pytest

from spans import SEC, T0, Consumer, to_proto


def get(url, path):
    try:
        with urllib.request.urlopen(url + path, timeout=5) as r:
            assert r.headers.get("Content-Type") == "application/json"
            return r.status, json.loads(r.read())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read())


def post(url, spans):
    req = urllib.request.Request(url + "/v1/traces", data=to_proto(spans).SerializeToString(), method="POST",
                                 headers={"Content-Type": "application/x-protobuf"})
    with urllib.request.urlopen(req, timeout=5) as r:
        assert r.status == 200


@pytest.fixture
def loaded(server):
    a, b = Consumer("consumer-a"), Consumer("consumer-b")
    mid = "5fea7756-0ea4-451a-a703-a558b933e274"
    post(server, [Consumer("producer").send(T0, mid), a.receive(T0 + SEC, mid, "rh-1"),
                  b.receive(T0 + 40 * SEC, mid, "rh-2", count=2), b.delete(T0 + 41 * SEC, "rh-2"),
                  a.send(T0, "id/with spaces?&")])
    return server, mid


def test_timeline(loaded):
    url, mid = loaded
    status, t = get(url, f"/api/messages/{mid}")
    assert status == 200 and t["message_id"] == mid and t["state"] == "deleted"
    assert [e["type"] for e in t["events"]] == ["sent", "received", "visibility_timeout_expired", "received", "deleted"]
    assert [e["inferred"] for e in t["events"]] == [False, False, True, False, False]
    assert isinstance(t["events"][0]["time_unix_nano"], str)  # 64-bit, kept exact for JavaScript


def test_message_id_is_url_decoded(loaded):
    url, _ = loaded
    status, t = get(url, "/api/messages/" + urllib.parse.quote("id/with spaces?&", safe=""))
    assert status == 200 and t["message_id"] == "id/with spaces?&"


def test_unknown_message(loaded):
    url, _ = loaded
    assert get(url, "/api/messages/nope")[0] == 404


def test_lists(loaded):
    url, mid = loaded
    status, d = get(url, "/api/duplicates")
    assert status == 200 and [m["message_id"] for m in d["messages"]] == [mid]
    assert d["messages"][0]["reasons"] == ["received_more_than_once"]
    assert get(url, "/api/duplicates?reason=succeeded_more_than_once")[1] == {"messages": [], "next_cursor": None}
    status, s = get(url, "/api/stuck?older_than_s=60")
    assert status == 200 and s["older_than_s"] == 60
    assert [m["message_id"] for m in s["messages"]] == ["id/with spaces?&"]
    assert get(url, "/api/stuck")[1]["older_than_s"] == 300
    recent = get(url, "/api/messages?limit=1")[1]["messages"]
    assert [m["message_id"] for m in recent] == [mid]
    assert get(url, "/api/messages?queue=payments")[1] == {"messages": [], "next_cursor": None}


def test_service_version_on_steps_and_list_rows_and_as_a_filter(server):
    """QV-362: the resource's service.version is kept on each span, shown on the timeline step's consumer and
    on the list rows' consumers, and filters the three lists. A span without it has none."""
    old, new = Consumer("consumer-a", version="1.4.2"), Consumer("consumer-b", version="1.4.3")
    post(server, [Consumer("producer").send(T0, "m-1"), old.receive(T0 + SEC, "m-1", "rh-1"),
                  new.receive(T0 + 40 * SEC, "m-1", "rh-2", count=2),
                  Consumer("producer").send(T0, "m-2"), new.receive(T0 + SEC, "m-2", "rh-3")])
    _, t = get(server, "/api/messages/m-1")
    assert [(e["type"], e["consumer"].get("service_version")) for e in t["events"] if not e["inferred"]] == \
        [("sent", None), ("received", "1.4.2"), ("received", "1.4.3")]
    assert "service_version" not in t["events"][0]["consumer"]  # left out when not set, as the Caller's fields
    _, d = get(server, "/api/duplicates")
    assert d["messages"][0]["consumers"] == [
        {"service_name": "orders-worker", "client_id": "consumer-a", "service_version": "1.4.2"},
        {"service_name": "orders-worker", "client_id": "consumer-b", "service_version": "1.4.3"}]
    for path, want in (("/api/messages?version=1.4.3", ["m-1", "m-2"]), ("/api/messages?version=1.4.2", ["m-1"]),
                       ("/api/messages?version=9", []), ("/api/duplicates?version=1.4.2", ["m-1"]),
                       ("/api/duplicates?version=9", []), ("/api/stuck?older_than_s=1&version=1.4.3", ["m-1", "m-2"]),
                       ("/api/stuck?older_than_s=1&version=1.4.2", ["m-1"]),
                       ("/api/messages?queue=orders&version=1.4.2", ["m-1"])):
        status, body = get(server, path)
        assert status == 200 and [m["message_id"] for m in body["messages"]] == want, path
    assert get(server, "/api/messages?version=1.4.2&body=body:x")[0] == 400


@pytest.mark.parametrize("path", [
    "/api/stuck?older_than_s=soon", "/api/stuck?older_than_s=-1", "/api/duplicates?limit=0",
    "/api/duplicates?limit=5000", "/api/duplicates?reason=x", "/api/messages/a?lambda_timeout_s=0",
])
def test_bad_parameters(server, path):
    status, body = get(server, path)
    assert status == 400 and body["error"]


@pytest.mark.parametrize("bad", ["x", "-1", "0"])
def test_stuck_ignores_to_ms(loaded, bad):
    """QV-96: the upper bound is now minus older_than_s, so to_ms, even a bad one, changes nothing."""
    url, _ = loaded
    status, d = get(url, f"/api/stuck?older_than_s=60&to_ms={bad}")
    assert status == 200 and [m["message_id"] for m in d["messages"]] == ["id/with spaces?&"]
    assert get(url, "/api/stuck?from_ms=x")[0] == 400  # from_ms is still read


@pytest.mark.parametrize("path", ["/api/messages/a/b", "/api/stuck/x", "/api/duplicates/x", "/api/health/x",
                                  "/api/stats/x", "/api/"])
def test_unknown_endpoints(server, path):
    assert get(server, path)[0] == 404


def test_health_shows_configuration(server):
    assert get(server, "/api/health")[1]["config"] == {"auth": "off", "stuck_after_s": 300, "lambda_timeout_s": 900,
                                                         "max_receive_counts": {}, "store_bodies": [], "retention": None,
                                                         "trace_link": None}


def test_ready_when_the_store_answers(server):
    assert get(server, "/api/ready") == (200, {"status": "ready", "version": get(server, "/api/health")[1]["version"]})
