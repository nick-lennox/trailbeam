"""The benchmark load generator (bench/loadgen.py): its spans are valid Trailbeam spans with the stories it
claims, and it reports what it sent."""
import sys
import threading
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "bench"))

import loadgen  # noqa: E402

from trailbeam.server import queries  # noqa: E402
from trailbeam.server.db import Database  # noqa: E402
from trailbeam.server.otlp import decode  # noqa: E402
from trailbeam.server.server import make_server  # noqa: E402

EVERYTHING = loadgen.Profile(queues=7, tenants=5, body_size=1024, redelivery=0.2, visibility_change=0.2, dlq=0.1,
                             stuck=0.1, lambda_consumer=0.3, late_join=0.3, batch_spans=100)


def ingest(profile, messages, store_bodies=("*",), seed=1):
    db = Database(":memory:", store_bodies=store_bodies)
    for batch in loadgen.Stream(profile, seed=seed).batches_of_messages(messages):
        d = decode(loadgen.encode(batch), "application/x-protobuf")
        assert d.rejected == [] and d.ignored == 0
        db.insert(d.records)
    return db


def test_every_span_is_accepted_and_every_settle_joins():
    db = ingest(EVERYTHING, 600)
    c = db.counts()
    assert c["messages"] == 600
    assert c["unjoined_settles"] == 0
    assert set(c["spans"]) == {"send", "receive", "delete", "change_visibility", "outcome"}


def test_late_joins_arrive_before_their_receive():
    p = loadgen.Profile(late_join=1.0, lambda_consumer=0, batch_spans=50, stuck=0)
    seen, early = set(), 0
    for batch in loadgen.Stream(p, seed=2).batches_of_messages(100):
        for s in batch:
            h = s.attrs.get("trailbeam.receipt_handle.hmac")
            if s.kind == "receive":
                seen.add(h)
            elif s.kind in ("delete", "change_visibility"):
                early += h not in seen
                assert "messaging.message.id" not in s.attrs  # another process: only the hash join can work
    assert early > 90
    assert ingest(p, 100).counts()["unjoined_settles"] == 0


def test_the_stories_show_up_in_the_timelines():
    db = ingest(EVERYTHING, 600)
    now = time.time_ns()
    states = [queries.message(db, r["mid"], now, 900) for r in db.query("SELECT DISTINCT mid FROM spans")]
    assert any(t["in_dead_letter_queue"] for t in states)
    assert any(t["deliveries"] > 1 for t in states)
    assert queries.duplicates(db, now, 900, 10)
    assert queries.stuck(db, now, 900, 0, 10)
    tenants = queries.tenants(db, now, 900, 0, now // 1_000_000)
    assert {r["tenant"] for r in tenants["rows"]} <= {f"tenant-{i:03d}" for i in range(5)}
    assert queries.body_search(db, now, 900, 10, None, [{"field": "body.order.status", "path": ["order", "status"],
                                                         "value": "failed", "neg": False}]) is not None


@pytest.mark.parametrize("size", [0, 1024, 64 * 1024])
def test_body_size(size):
    p = loadgen.Profile(body_size=size)
    spans = loadgen.MessageFactory(p, seed=3).message(10 ** 18)
    send = spans[0]
    if size:
        assert len(send.attrs["trailbeam.body"]) == size
        assert send.attrs["messaging.message.body.size"] == size
    else:
        assert "trailbeam.body" not in send.attrs and send.attrs["trailbeam.body.capture_status"] == "off"


def test_records_match_the_server_decoder():
    batch = next(loadgen.Stream(EVERYTHING, seed=4).batches_of_messages(50))
    via_http = decode(loadgen.encode(batch), "application/x-protobuf").records
    direct = loadgen.records(batch)
    strip = lambda r: {k: v for k, v in r.items() if k != "service_name"}  # noqa: E731
    key = lambda r: r["span_id"]  # noqa: E731  (encode groups spans by service)
    assert sorted(map(strip, direct), key=key) == sorted(map(strip, via_http), key=key)


@pytest.fixture
def server():
    db = Database(":memory:")
    httpd = make_server(db, "127.0.0.1", 0, ui_dir=None)
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{httpd.server_address[1]}/v1/traces", db
    httpd.shutdown()


def test_run_reports_what_the_server_stored(server):
    url, db = server
    res = loadgen.run(url, loadgen.Profile(batch_spans=64), rate=100, duration=2, concurrency=2)
    assert res["errors"] == 0 and res["exports"] > 0
    stored = sum(db.counts()["spans"].values())
    assert stored == res["exports"] * 64
    assert res["export_p99_ms"] is not None


def test_repeated_runs_send_new_spans(server):
    # The server skips a span it already stored: a second run with the same IDs would measure nothing.
    url, db = server
    profile = loadgen.Profile(batch_spans=64)
    first = loadgen.run(url, profile, rate=50, duration=1, concurrency=2)
    second = loadgen.run(url, profile, rate=50, duration=1, concurrency=2)
    assert first["seed"] != second["seed"]
    assert sum(db.counts()["spans"].values()) == (first["exports"] + second["exports"]) * 64
    # The same seed gives the same trace and span IDs, but each span's end time is the time it is generated.
    # A duplicate is the same span again, end time included (an exporter retry: schema 10, QV-78), so these
    # are stored: a rerun with a seed does not measure duplicates.
    again = loadgen.run(url, profile, rate=50, duration=1, concurrency=2, seed=second["seed"])
    assert again["seed"] == second["seed"]
    assert sum(db.counts()["spans"].values()) == (first["exports"] + second["exports"] + again["exports"]) * 64


def test_run_achieved_rate_matches_the_offered_rate(server):
    # 100 spans/s offered in 1-second exports: the achieved rate is over the whole window, not to the last export
    url, db = server
    profile = loadgen.Profile()
    rate = 100 / profile.mean_spans_per_message()
    res = loadgen.run(url, profile, rate=rate, duration=3, concurrency=2, schedule_delay=1)
    assert res["errors"] == 0
    assert abs(sum(db.counts()["spans"].values()) - res["achieved_spans_per_s"] * res["elapsed_s"]) < 2
    assert 0.85 * res["target_spans_per_s"] <= res["achieved_spans_per_s"] <= 1.05 * res["target_spans_per_s"]


def test_run_sdk_counts_drops(server):
    url, db = server
    res = loadgen.run_sdk(url, loadgen.Profile(), rate=50, duration=1, settle_s=10)
    assert res["emitted_spans"] > 0
    assert res["dropped_spans"] == 0 and res["received_spans"] == res["emitted_spans"]
    assert res["exported_spans"] == res["emitted_spans"] and res["never_exported_spans"] == 0


def test_run_sdk_counts_failed_exports_without_server_counts(server):
    url, db = server
    # a 404 is not retried: every export fails, the processor drops those spans, the server stores none
    res = loadgen.run_sdk(url.replace("/v1/traces", "/v1/nowhere"), loadgen.Profile(), rate=50, duration=1,
                          settle_s=10)
    assert res["emitted_spans"] > 0 and res["exported_spans"] == 0
    assert res["failed_export_spans"] == res["dropped_spans"] == res["emitted_spans"]
    assert res["received_spans"] == 0


@pytest.mark.parametrize("processor", ["trailbeam", "stock"])
def test_run_sdk_processors_without_drops(server, processor):
    url, db = server
    res = loadgen.run_sdk(url, loadgen.Profile(), rate=50, duration=1, settle_s=10, processor=processor)
    assert res["processor"] == processor and res["dropped_spans"] == 0
    assert res["received_spans"] == res["emitted_spans"] > 0
    if processor == "trailbeam":
        assert res["max_queue_size"] == 16384
        assert res["trailbeam_dropped"] == res["logged_dropped"] == res["drop_log_lines"] == 0


def test_run_sdk_trailbeam_counter_and_log_match_real_drops(server):
    url, db = server
    # a 404 is not retried: every export fails. The counter and its log lines must say exactly how many.
    res = loadgen.run_sdk(url.replace("/v1/traces", "/v1/nowhere"), loadgen.Profile(), rate=50, duration=1,
                          settle_s=10)
    assert res["dropped_spans"] == res["emitted_spans"] > 0
    assert res["trailbeam_dropped"] == res["logged_dropped"] == res["dropped_spans"]
    assert res["trailbeam_dropped_by_reason"]["export_failed"] == res["dropped_spans"]
    assert res["drop_log_lines"] >= 1
