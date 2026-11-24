"""GET /api/events (events.py, QV-394): the recorded steps in a window, paged, and their counts per bucket.
The same answers from Postgres are checked by the conformance suite (tests/conformance/cases.py, ``events``)."""
import json
import urllib.error
import urllib.request

import pytest

from spans import SEC, T0, Consumer, to_proto
from trailbeam.server import events

MS = 1_000_000
T0_MS = T0 // MS


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


@pytest.fixture
def loaded(server):
    w, p = Consumer("orders-worker"), Consumer("checkout-api")
    post(server, [p.send(T0, "m-1"), w.receive(T0 + SEC, "m-1", "rh-1"), w.delete(T0 + 2 * SEC, "rh-1"),
                  p.send(T0 + 61 * SEC, "m-2", queue="billing"),
                  w.receive(T0 + 62 * SEC, "m-2", "rh-2", queue="billing"),
                  w.change_visibility(T0 + 63 * SEC, "rh-2", 0, queue="billing"),
                  w.outcome(T0 + 64 * SEC, "m-2", "failed_exception", "inv-1", queue="billing", error_type="Boom"),
                  # a delete whose receive never arrives and that carries no message ID: no message
                  w.delete(T0 + 65 * SEC, "rh-never"),
                  # a PurgeQueue call belongs to no message and is not a step
                  w.purge(T0 + 66 * SEC)])
    return server


def window(**kw):
    q = {"from_ms": T0_MS, "to_ms": T0_MS + 120_000, **kw}
    return "/api/events?" + "&".join(f"{k}={v}" for k, v in q.items())


def test_every_recorded_step_newest_first_and_no_purge(loaded):
    status, body = get(loaded, window())
    assert status == 200
    kinds = [(e["kind"], e["message_id"], e["queue"]) for e in body["events"]]
    assert kinds == [("delete", None, "orders"), ("outcome", "m-2", "billing"),
                     ("change_visibility", "m-2", "billing"), ("receive", "m-2", "billing"),
                     ("send", "m-2", "billing"), ("delete", "m-1", "orders"), ("receive", "m-1", "orders"),
                     ("send", "m-1", "orders")]
    out = body["events"][1]
    assert (out["outcome"], out["error_type"], out["time_unix_nano"]) == ("failed_exception", "Boom", str(T0 + 64 * SEC))
    assert body["events"][2]["visibility_timeout"] == 0 and body["events"][3]["receive_count"] == 1
    assert body["next_cursor"] is None
    h = body["histogram"]
    assert h["bucket_ms"] == 60_000 and h["total"] == 8
    assert h["totals"] == {"send": 2, "receive": 2, "delete": 2, "change_visibility": 1, "outcome": 1}
    # buckets start on minute edges since the epoch, and cover the whole window
    assert [b["start_ms"] for b in h["buckets"]] == [(T0_MS // 60_000 + i) * 60_000 for i in range(len(h["buckets"]))]
    assert sum(b["send"] for b in h["buckets"]) == 2


def test_pages_cover_every_step_once_and_only_the_first_has_the_histogram(loaded):
    seen, cursor, first = [], None, True
    while True:
        status, body = get(loaded, window(limit=3, **({"cursor": cursor} if cursor else {})))
        assert status == 200 and len(body["events"]) <= 3
        assert (body["histogram"] is not None) == first
        first = False
        seen += [(e["trace_id"], e["span_id"]) for e in body["events"]]
        if not (cursor := body["next_cursor"]):
            break
    _, everything = get(loaded, window())
    assert seen == [(e["trace_id"], e["span_id"]) for e in everything["events"]]


@pytest.mark.parametrize("q, want", [
    ({"queue": "billing"}, 4),
    ({"service": "checkout-api"}, 0),  # every span here is from the export's service, orders-worker
    ({"service": "orders-worker"}, 8),
    ({"kind": "delete,change_visibility"}, 3),
    ({"kind": "outcome"}, 1),
    ({"message_id": "m-1"}, 3),
    ({"message_id": "m-2", "kind": "receive"}, 1),
    ({"message_id": "m-2", "queue": "orders"}, 0),
    ({"message_id": "no-such"}, 0),
])
def test_filters_apply_to_the_rows_and_the_counts(loaded, q, want):
    status, body = get(loaded, window(**q))
    assert status == 200
    assert len(body["events"]) == want == body["histogram"]["total"]


def test_a_window_counts_only_its_own_steps(loaded):
    # [T0 + 1 s, T0 + 62 s]: the first receive to the second receive, both included
    status, body = get(loaded, f"/api/events?from_ms={T0_MS + 1000}&to_ms={T0_MS + 62_000}")
    assert status == 200
    assert [e["kind"] for e in body["events"]] == ["receive", "send", "delete", "receive"]
    assert body["histogram"]["total"] == 4


def test_message_id_pages_too(loaded):
    _, a = get(loaded, window(message_id="m-2", limit=2))
    _, b = get(loaded, window(message_id="m-2", limit=2, cursor=a["next_cursor"]))
    assert [e["kind"] for e in a["events"] + b["events"]] == ["outcome", "change_visibility", "receive", "send"]
    assert b["next_cursor"] is None and b["histogram"] is None and a["histogram"]["total"] == 4


@pytest.mark.parametrize("q, word", [({"kind": "purge"}, "kind"), ({"kind": "send,bogus"}, "kind"),
                                     ({"cursor": "not-a-cursor"}, "cursor"), ({"limit": 0}, "limit")])
def test_bad_parameters_are_a_400(loaded, q, word):
    status, body = get(loaded, window(**q))
    assert status == 400 and word in body["error"]


def test_bucket_widths():
    day = 86_400_000
    assert events.bucket_ms(0, 3_600_000 - 1) == 60_000  # an hour: 60 one-minute buckets
    assert events.bucket_ms(0, 7 * day) == 3_600_000  # a week: 169 hourly buckets
    assert events.bucket_ms(0, 30 * day) == 21_600_000  # 30 days: 6 hours
    assert events.bucket_ms(0, 400 * day) == 3 * day  # longer than 200 days: whole days
    for f, t in [(0, 10 ** 15), (123, 7 * day + 5), (59_999, 60_000)]:
        w = events.bucket_ms(f, t)
        assert t // w - f // w + 1 <= events.MAX_BUCKETS


def _direct(db, from_ns, to_ns, bucket_ns, **kw):
    """step_counts from the spans alone, as the store would count them with no rollup."""
    where, p = db._step_where(kw.get("queue"), kw.get("service"), kw.get("kinds"))
    return sorted(tuple(r) for r in db.query(
        f"SELECT time_ns / ? AS b, kind, COUNT(*) FROM spans WHERE time_ns >= ? AND time_ns < ?{where} GROUP BY 1, 2",
        (bucket_ns, from_ns, to_ns, *p)))


def test_the_hourly_rollup_counts_what_the_spans_count(db, ingest):
    from trailbeam.server.store import Cutoffs
    hour = 3600 * SEC
    w, p = Consumer("w"), Consumer("p")
    for i in range(30):  # a message every 50 minutes, over a day: whole hours and partial ones
        t = T0 + i * 50 * 60 * SEC
        ingest(p.send(t, f"m{i}", queue="a" if i % 3 else "b"), service="checkout-api")
        ingest(w.receive(t + SEC, f"m{i}", f"h{i}", queue="a" if i % 3 else "b"),
               w.delete(t + 2 * SEC, f"h{i}", queue="a" if i % 3 else "b"))
    ingest(p.send(T0, "m0", queue="b"), service="checkout-api")  # an exporter retry: stored once, counted once
    ingest(w.delete(T0 + 5 * hour, "h-never"), w.purge(T0 + 6 * hour))
    windows = [(T0 - 3 * 86_400 * SEC + 17, T0 + 4 * 86_400 * SEC), (T0 + 1, T0 + 20 * hour - 1), (T0, T0 + 24 * hour)]
    filters = [{}, {"queue": "a"}, {"service": "checkout-api"}, {"kinds": ("delete",)}, {"queue": "b", "kinds": ("send",)}]

    def check():
        for a, b in windows:
            width = events.bucket_ms(a // MS, b // MS - 1) * MS
            for kw in filters:
                got = sorted(db.step_counts(a, b, width, **kw))
                assert got == _direct(db, a, b, width, **kw), (a, b, kw)
    check()
    assert events.hour_split(*windows[0], events.bucket_ms(windows[0][0] // MS, windows[0][1] // MS) * MS)
    # retention deletes whole messages and unjoined settles: the rollup loses them too
    cut, resume, deleted = Cutoffs(default=T0 + 12 * hour), None, 0
    while True:
        r = db.purge_step(cut, resume, limit=1000)
        deleted += r.deleted
        if not (resume := r.resume):
            break
    assert deleted > 0
    check()
    assert db.query("SELECT COUNT(*) FROM step_rollup WHERE n <= 0")[0][0] == 0
