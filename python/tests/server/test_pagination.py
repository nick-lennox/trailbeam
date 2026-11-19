"""List pages: cursor pagination, time ranges and the bounded tenant search (QV-73), through the API."""
import sys
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "bench"))

import loadgen  # noqa: E402

from trailbeam.server.db import SqliteStore  # noqa: E402
from trailbeam.server.server import ApiError, App  # noqa: E402

PROFILE = loadgen.Profile(queues=4, tenants=5, body_size=0, body_queues=0, redelivery=0.3, visibility_change=0.1,
                          dlq=0.1, stuck=0.2, lambda_consumer=0.2, late_join=0.2, batch_spans=50)


@pytest.fixture(scope="module")
def app():
    s = SqliteStore()
    t = [time.time_ns() - 3600 * 10 ** 9]

    def clock():  # a message every 50 ms, so last-seen ties and ranges are both exercised
        t[0] += 50_000_000
        return t[0]
    for batch in loadgen.Stream(PROFILE, seed=4, clock=clock).batches_of_messages(600):
        s.insert(loadgen.records(batch))
    yield App(s, now=lambda: t[0] + 2 * 86_400 * 10 ** 9, stuck_after_s=0, ui_dir=None)  # all of it 2 days ago
    s.close()


def _all_pages(app, endpoint, q, limit):
    ids, cursor, pages = [], None, 0
    while True:
        d = app.api(endpoint, {**q, "limit": str(limit), **({"cursor": cursor} if cursor else {})})
        assert len(d["messages"]) <= limit
        ids += [m["message_id"] for m in d["messages"]]
        pages += 1
        cursor = d["next_cursor"]
        if cursor is None:
            return ids, pages
        assert len(d["messages"]) == limit


@pytest.mark.parametrize("endpoint, q", [
    ("messages", {}), ("messages", {"queue": "bench-q001"}), ("messages", {"tenant": "tenant-000"}),
    ("messages", {"tenant": "tenant-001", "queue": "bench-q002"}), ("duplicates", {}),
    ("duplicates", {"reason": "received_more_than_once", "queue": "bench-q000"}), ("stuck", {}),
    ("stuck", {"queue": "bench-q003"})])
def test_pages_put_together_are_the_whole_list(app, endpoint, q):
    whole = [m["message_id"] for m in app.api(endpoint, {**q, "limit": "1000"})["messages"]]
    assert len(whole) > 10
    for limit in (1, 7, len(whole), len(whole) + 1):
        ids, pages = _all_pages(app, endpoint, q, limit)
        assert ids == whole
        assert pages == max(1, -(-len(whole) // limit))


def test_a_range_keeps_messages_last_seen_in_it(app):
    everything = app.api("messages", {"limit": "1000"})["messages"]
    ms = sorted(_ms(m["last_seen"]) for m in everything)
    lo, hi = ms[len(ms) // 4], ms[3 * len(ms) // 4]
    got = app.api("messages", {"limit": "1000", "from_ms": str(lo), "to_ms": str(hi)})["messages"]
    assert [m["message_id"] for m in got] == [m["message_id"] for m in everything if lo <= _ms(m["last_seen"]) <= hi]
    dup = app.api("duplicates", {"limit": "1000", "to_ms": str(hi)})["messages"]
    assert dup and all(_ms(m["last_seen"]) <= hi for m in dup)
    old = app.api("stuck", {"limit": "1000"})["messages"]
    late = app.api("stuck", {"limit": "1000", "from_ms": str(lo)})["messages"]
    assert 0 < len(late) < len(old) and all(_ms(m["first_seen"]) >= lo for m in late)


def test_a_bad_cursor_or_range_is_a_400(app):
    for q in ({"cursor": "!!"}, {"cursor": "bm90LWEtY3Vyc29y"}, {"from_ms": "5", "to_ms": "4"}):
        with pytest.raises(ApiError) as e:
            app.api("messages", q)
        assert e.value.status == 400


def test_tenant_search_is_bounded_by_the_range_and_a_queue(app):
    unbounded = app.api("tenants/search", {"q": "tenant", "limit": "100", "from_ms": "0"})["tenants"]
    assert sorted(unbounded) == [f"tenant-00{i}" for i in range(5)]
    assert app.api("tenants/search", {"q": "TENANT-00", "limit": "100"})["tenants"] == []  # default: the last day
    assert app.api("tenants/search", {"q": "tenant", "limit": "100", "since_s": str(3 * 86_400)})["tenants"] == unbounded
    on_q = app.api("tenants/search", {"q": "tenant", "limit": "100", "from_ms": "0", "queue": "bench-q001"})["tenants"]
    assert set(on_q) <= set(unbounded) and on_q
    for t in on_q:
        assert app.api("messages", {"tenant": t, "queue": "bench-q001", "limit": "1"})["messages"]
    assert app.api("tenants/search", {"q": "tenant", "from_ms": "0", "queue": "no-such-queue"})["tenants"] == []


def test_the_web_pages_link_to_the_next_page(app):
    from trailbeam.server import web
    status, _, body = web.handle(app.api, ApiError, "/", {"limit": "5"})
    assert status == 200 and b"Next page" in body and b"cursor=" in body
    status, _, body = web.handle(app.api, ApiError, "/", {"limit": "1000"})
    assert b"Next page" not in body


def _ms(iso):
    from datetime import datetime, timedelta, timezone
    dt = datetime.strptime(iso, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc)
    return (dt - datetime(1970, 1, 1, tzinfo=timezone.utc)) // timedelta(milliseconds=1)
