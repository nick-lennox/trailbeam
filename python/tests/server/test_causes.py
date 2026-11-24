"""Unfinished messages grouped by cause (causes.py, QV-367): parity with the hosted app on the shared fixture, the
fingerprint, and GET /api/causes over stored spans."""
import json
import urllib.error
import urllib.request
from pathlib import Path

import pytest

from spans import SEC, T0, Consumer, to_proto
from trailbeam.server import causes

FIXTURE = Path(__file__).resolve().parents[3] / "ui" / "test" / "fixtures" / "causes-parity.json"


def _classified(f: dict) -> list:
    return [causes.cause_of(c["timeline"], f["nowMs"], visibility_s=c["visibilityS"], queue_url=c["queueUrl"],
                            tenant=c["tenant"], group_id=c["group"]) for c in f["cases"]]


def test_parity_with_the_app():
    """The groups the app's lib/app/causes.ts makes of these timelines (its tests read the same file)."""
    f = json.loads(FIXTURE.read_text())
    xs = _classified(f)
    for c, x, want in zip(f["cases"], xs, f["expected"]["classified"]):
        assert causes.public(x) == want, c["name"]
    assign: dict = {}
    groups = causes.group(xs, f["firstSeenMs"], assign)
    assert [causes.public(g) for g in groups] == f["expected"]["groups"]
    assert assign == f["expected"]["assign"]
    r = f["expected"]["range"]
    assert [causes.public(g) for g in causes.in_range(xs, f["firstSeenMs"], r["fromMs"], r["toMs"])] == r["groups"]
    t0 = min(f["firstSeenMs"].values())
    assert {g["id"]: causes.trend_of(g["times"], t0, f["nowMs"]) for g in groups} == f["expected"]["trends"]


def test_the_fixture_has_every_kind():
    kinds = {g["kind"] for g in json.loads(FIXTURE.read_text())["expected"]["groups"]}
    assert kinds == set(causes.BASIS)


def test_fingerprint():
    assert causes.fingerprint("order 9f3c2a71-5d2e-4c1e-b7a0-38e1d2f4a6c9 not found") == "order <uuid> not found"
    assert causes.fingerprint("Stripe timed out after 10000 ms") == "Stripe timed out after <n> ms"
    assert causes.fingerprint("Stripe timed out after 2500ms") == "Stripe timed out after <n>"
    assert causes.fingerprint("trace deadbeef12 failed") == "trace <hex> failed"
    assert causes.fingerprint("at 2026-11-23T05:57:23.148Z from 10.0.0.12") == "at <time> from <ip>"
    assert causes.fingerprint("no user ana@example.com") == "no user <email>"
    assert causes.fingerprint("KeyError: 'orderId'") != causes.fingerprint("KeyError: 'sku'")
    assert causes.fingerprint("a   b\n c") == "a b c"
    assert len(causes.fingerprint("x" * 500)) == 200
    assert causes.fingerprint(None) == ""


def test_queue_key():
    assert causes.queue_key("orders", "https://sqs.eu-west-1.amazonaws.com/222222222222/orders") == \
        "222222222222/eu-west-1/orders"
    assert causes.queue_key("orders", "http://localhost:9324/000000000000/orders") == "000000000000//orders"
    assert causes.queue_key("orders", None) == "orders"


# ---- GET /api/causes ---------------------------------------------------------------------------------------

def get(url, path):
    try:
        with urllib.request.urlopen(url + path, timeout=5) as r:
            return r.status, json.loads(r.read())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read())


def post(url, spans, service="orders-worker"):
    req = urllib.request.Request(url + "/v1/traces", data=to_proto(spans, service=service).SerializeToString(),
                                 method="POST", headers={"Content-Type": "application/x-protobuf"})
    with urllib.request.urlopen(req, timeout=5) as r:
        assert r.status == 200


def failed(c: Consumer, t, mid, inv, text):
    s = c.outcome(t, mid, "failed_exception", inv, error_type="TimeoutError")
    s.attrs["trailbeam.error_message"] = text
    return s


@pytest.fixture
def loaded(server):
    p, w = Consumer("producer"), Consumer("worker", source="lambda")
    spans = []
    # two messages failing with the same exception (error texts differing by a number); one moved to the DLQ
    for i, mid in enumerate(["m-1", "m-2"]):
        t = T0 + i * 100 * SEC
        spans += [p.send(t, mid), w.receive(t + SEC, mid, f"rh-{mid}", lambda_inv=f"inv-{mid}"),
                  failed(w, t + 2 * SEC, mid, f"inv-{mid}", f"Stripe timed out after {10000 + i} ms")]
    spans.append(Consumer("dlq-reader").receive(T0 + 300 * SEC, "m-2", "rh-dlq", queue="orders-dlq",
                                                 dlq_source="orders"))
    # received, never deleted
    spans += [p.send(T0, "m-3"), Consumer("poller").receive(T0 + SEC, "m-3", "rh-3")]
    # finished: not listed
    spans += [p.send(T0, "m-4"), Consumer("poller").receive(T0 + SEC, "m-4", "rh-4"),
              Consumer("poller").delete(T0 + 2 * SEC, "rh-4")]
    post(server, spans)
    return server


def test_causes_groups_the_stuck_messages(loaded):
    status, d = get(loaded, "/api/causes")
    assert status == 200
    assert d["total"] == 3 and d["capped"] is False and d["in_dead_letter_queue"] == 1 and d["older_than_s"] == 300
    by = {g["kind"]: g for g in d["groups"]}
    assert set(by) == {"exc", "not_deleted"}
    exc = by["exc"]
    assert d["groups"][0] is not None and d["groups"][0]["id"] == exc["id"]  # dead-lettered: worst first
    assert exc["count"] == 2 and exc["counts"] == {"dead_lettered": 1, "stuck": 1, "filtered": 0}
    assert exc["label"] == "orders-worker raised TimeoutError: Stripe timed out after 10000 ms"
    assert exc["queueKey"] == "000000000000//orders" and exc["firstMs"] == T0 // 10**6 + 2000
    assert exc["sampleMid"] == "m-2" and sorted(exc["examples"]) == ["m-1", "m-2"]
    nd = by["not_deleted"]
    assert nd["inferred"] is True and causes.ASSUMED_VISIBILITY in nd["why"]
    assert {m["message_id"]: m["cause"] for m in d["messages"]} == {"m-1": exc["id"], "m-2": exc["id"], "m-3": nd["id"]}
    assert d["dead_letter_queues"] == [{"key": "000000000000//orders-dlq", "name": "orders-dlq", "account": "000000000000",
                                        "region": None, "messageIds": ["m-2"], "sources": ["orders"],
                                        "newest": T0 // 10**6 + 300_000}]


def test_causes_narrowed(loaded):
    _, d = get(loaded, "/api/causes")
    exc = next(g for g in d["groups"] if g["kind"] == "exc")
    _, one = get(loaded, f"/api/causes?cause={exc['id']}&limit=1")
    assert one["cause"] == exc["id"] and one["listed"] == 2 and len(one["messages"]) == 1
    assert len(one["groups"]) == 2  # the groups aren't narrowed, only the list
    _, unknown = get(loaded, "/api/causes?cause=000000000000")
    assert unknown["listed"] == 3  # a cause with no messages now: every message
    # a time range keeps the causes seen in it: only m-2's exception, at T0 + 102 s
    _, ranged = get(loaded, f"/api/causes?from_ms={T0 // 10**6 + 50_000}&to_ms={T0 // 10**6 + 200_000}")
    assert [(g["kind"], g["count"]) for g in ranged["groups"]] == [("exc", 1)]
    assert [m["message_id"] for m in ranged["messages"]] == ["m-2"] and ranged["total"] == 1
    _, q = get(loaded, "/api/causes?queue=nope")
    assert q["groups"] == [] and q["messages"] == []
    _, young = get(loaded, "/api/causes?older_than_s=1000000000")
    assert young["total"] == 0


@pytest.mark.parametrize("path", ["/api/causes?older_than_s=x", "/api/causes?from_ms=2&to_ms=1",
                                  "/api/causes?limit=0", "/api/causes/x"])
def test_causes_bad_requests(server, path):
    assert get(server, path)[0] in (400, 404)


def test_causes_capped(db, ingest, monkeypatch):
    p, c = Consumer("producer"), Consumer("poller")
    ingest(*[s for i in range(5) for s in (p.send(T0 + i * SEC, f"c-{i}"), c.receive(T0 + i * SEC + 1, f"c-{i}", f"r{i}"))])
    read = causes.read_unfinished(db, T0 + 3600 * SEC, 900, None, 300, limit=3)
    assert read["capped"] is True and len(read["classified"]) == 3
    assert [i["message_id"] for i in read["items"]] == ["c-0", "c-1", "c-2"]  # oldest first
