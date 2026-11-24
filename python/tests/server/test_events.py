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


def test_the_hour_plan_adds_up_to_the_window():
    """events.hour_split (QV-468): its rollup hours plus and minus its span ranges count every step of the window
    once, wherever the window starts and ends and whatever now is: before, inside or after it."""
    import random
    rnd = random.Random(468)
    hour = events.HOUR_NS
    times = sorted(rnd.randrange(0, 80 * hour) for _ in range(3000))

    def spans(a, b):
        return sum(1 for t in times if a <= t < b)

    for _ in range(400):
        a = rnd.randrange(0, 70 * hour)
        b = a + rnd.choice([1, hour // 3, hour, 3 * hour + 7, rnd.randrange(1, 60 * hour)])
        width = rnd.choice([hour, 3 * hour, 24 * hour])
        now = rnd.choice([a - hour, (a + b) // 2, b, b + 2 * hour, 10 ** 30])
        ranges, hours = events.hour_split(a, b, width, now)
        got = sum(sign * spans(x, y) for x, y, sign in ranges)
        if hours:
            got += spans(hours[0] * hour, hours[1] * hour)
        assert got == spans(a, b), (a, b, width, now, ranges, hours)
        assert all(x // width == (y - 1) // width for x, y, _ in ranges if x < y)  # a range is in one bucket
    # a window ending now takes its current hour from the rollup, less what is newer than its end
    ranges, hours = events.hour_split(7 * hour + 5, 75 * hour + 10, hour, now_ns=75 * hour + 10)
    assert hours == (7, 76) and ranges == [(7 * hour, 7 * hour + 5, -1), (75 * hour + 10, 76 * hour, -1)]
    assert events.hour_split(0, 10 * hour, 60 * 10 ** 9) is None  # buckets of minutes: all from the spans


class _Recorder:
    """A store whose counts and rows are given, recording the step_rows calls events.page makes."""

    def __init__(self, counted, rows):
        self.counted, self.rows, self.calls = counted, rows, []

    def step_counts(self, *a, **kw):
        return self.counted

    def step_rows(self, from_ns, to_ns, limit, **kw):
        self.calls.append((from_ns, to_ns, kw.get("kinds")))
        return [r for r in self.rows if from_ns <= r["time_ns"] < to_ns][:limit]


def _row(t, kind="send"):
    return {"time_ns": t, "start_ns": None, "kind": kind, "mid": None, "queue": "q", "service_name": None,
            "service_version": None, "result": None, "error_code": None, "outcome": None, "error_type": None,
            "receive_count": None, "visibility_timeout": None, "trace_id": "a" * 32, "span_id": f"{t:016x}"}


def test_the_first_page_reads_rows_only_where_the_counts_found_them():
    hour_ms = 3_600_000
    frm, to = 0, 7 * 24 * hour_ms - 1  # hourly buckets
    width = events.bucket_ms(frm, to) * MS
    # sends in buckets 10 (3) and 150 (2), a delete in 160: a page of 2 (and whether there is more) is in 150 and 160
    counted = [(10, "send", 3), (150, "send", 2), (160, "delete", 1)]
    rows = [_row(10 * width + k) for k in range(3)] + [_row(150 * width + k) for k in range(2)] + [_row(160 * width)]
    rows.sort(key=lambda r: -r["time_ns"])
    db = _Recorder(counted, rows)
    page = events.page(db, frm, to, 2, kinds=("send", "delete", "outcome"))
    assert db.calls == [(150 * width, 161 * width, ("send", "delete"))]  # no outcome: none counted
    assert len(page["events"]) == 2 and page["next_cursor"]
    db = _Recorder(counted, rows)
    assert len(events.page(db, frm, to, 3)["events"]) == 3 and db.calls[0][:2] == (10 * width, 161 * width)
    db = _Recorder([], [])
    assert events.page(db, frm, to, 3)["events"] == [] and db.calls == []  # nothing counted: no rows read
    # the rows aren't there (retention took them after the counts): the whole window is read instead
    db = _Recorder(counted, [_row(10 * width + 5)])
    page = events.page(db, frm, to, 3)
    assert [c[:2] for c in db.calls] == [(10 * width, 161 * width), (0, to * MS + MS)]
    assert len(page["events"]) == 1
