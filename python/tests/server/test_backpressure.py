"""Ingest backpressure (backpressure.py): 429 when too many exports are in progress, 503 when the writer is
behind, both with Retry-After and before anything is written; 200 only after the spans are committed."""
import json
import threading
import time
import urllib.error
import urllib.request

import pytest
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceResponse

from spans import SEC, T0, Consumer, to_json, to_proto
from trailbeam.server.backpressure import RETRY_AFTER_MAX_S, IngestGate, Rejected, status_body
from trailbeam.server.db import WriterBusy
from trailbeam.server.server import App, make_server


def body(n=1, start=0):
    c = Consumer()
    return to_proto([c.receive(T0 + i * SEC, f"m{start + i}", f"rh-{start + i}") for i in range(n)]).SerializeToString()


def post(url, data, ctype="application/x-protobuf"):
    req = urllib.request.Request(url + "/v1/traces", data=data, method="POST", headers={"Content-Type": ctype})
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return r.status, dict(r.headers), r.read()
    except urllib.error.HTTPError as e:
        return e.code, dict(e.headers), e.read()


def stored(db):
    return db.query("SELECT COUNT(*) AS n FROM spans")[0]["n"]


@pytest.fixture
def gated(db):
    """A server with a small gate, and the App behind it."""
    gate = IngestGate(max_pending=2, max_wait_s=0.2)
    httpd = make_server(db, "127.0.0.1", 0, ui_dir=None, gate=gate)
    app = httpd.app
    t = threading.Thread(target=httpd.serve_forever, daemon=True)
    t.start()
    yield f"http://127.0.0.1:{httpd.server_address[1]}", app
    httpd.shutdown()
    httpd.server_close()


def test_accepted_when_idle(gated, db):
    url, app = gated
    status, headers, _ = post(url, body(3))
    assert status == 200 and "Retry-After" not in headers
    assert stored(db) == 3
    assert app.gate.pending == 0


def test_writer_behind_is_503_with_retry_after_and_nothing_stored(gated, db):
    url, app = gated
    with db.lock:  # another export holding the writer for longer than max_wait_s
        status, headers, out = post(url, body(2))
    assert status == 503
    assert 1 <= int(headers["Retry-After"]) <= RETRY_AFTER_MAX_S
    assert headers["Content-Type"] == "application/x-protobuf"
    assert b"TRAILBEAM_INGEST_MAX_WAIT_S" in out and out[0] == 0x08 and out[1] == 14  # google.rpc.Status UNAVAILABLE
    assert stored(db) == 0
    assert app.m_rejected.value(reason="writer_behind") == 1
    # the exporter's retry once the writer is free: stored, exactly once
    status, _, _ = post(url, body(2))
    assert status == 200 and stored(db) == 2


def test_queue_full_is_429_before_decoding(gated, db):
    url, app = gated
    entered, release = threading.Barrier(3), threading.Event()
    results = []

    def hold():
        with app.gate.admit():
            entered.wait()
            release.wait(5)

    holders = [threading.Thread(target=hold) for _ in range(2)]
    for h in holders:
        h.start()
    entered.wait()
    try:
        results.append(post(url, b"not protobuf at all"))  # never decoded: 429, not 400
    finally:
        release.set()
        for h in holders:
            h.join()
    status, headers, out = results[0]
    assert status == 429
    assert 1 <= int(headers["Retry-After"]) <= RETRY_AFTER_MAX_S
    assert out[:2] == bytes([0x08, 8])  # RESOURCE_EXHAUSTED
    assert app.m_rejected.value(reason="queue_full") == 1
    assert app.gate.pending == 0
    assert post(url, body(1))[0] == 200


def test_json_request_gets_a_json_status(gated, db):
    url, _ = gated
    c = Consumer()
    data = json.dumps(to_json([c.receive(T0, "m1", "rh-1")])).encode()
    with db.lock:
        status, headers, out = post(url, data, "application/json")
    assert status == 503 and headers["Content-Type"] == "application/json"
    assert json.loads(out)["code"] == 14
    assert post(url, data, "application/json")[0] == 200


def test_never_200_without_commit_under_contention(db):
    """Many exporters at once against a small gate and a slow writer: every span in a 200 is stored, no
    span in a 429/503 is, and retrying the rejected ones stores each span exactly once."""
    gate = IngestGate(max_pending=3, max_wait_s=0.05)
    httpd = make_server(db, "127.0.0.1", 0, ui_dir=None, gate=gate)
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    url = f"http://127.0.0.1:{httpd.server_address[1]}"
    real_insert = db.insert

    def slow_insert(records, **kw):  # a writer that takes 30 ms per export
        records = list(records)
        return real_insert(_Slow(records), **kw)

    db.insert = slow_insert
    codes, lock = [], threading.Lock()
    accepted_spans = []

    def exporter(k):
        for j in range(6):
            data = body(4, start=(k * 100 + j) * 4)
            while True:
                status, headers, _ = post(url, data)
                with lock:
                    codes.append(status)
                if status == 200:
                    with lock:
                        accepted_spans.append(4)
                    break
                assert status in (429, 503) and int(headers["Retry-After"]) >= 1
                time.sleep(0.01)  # a test does not wait the full Retry-After

    try:
        threads = [threading.Thread(target=exporter, args=(k,)) for k in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(60)
    finally:
        httpd.shutdown()
        httpd.server_close()
    assert any(c in (429, 503) for c in codes), codes
    assert stored(db) == sum(accepted_spans) == 8 * 6 * 4
    assert db.query("SELECT COUNT(*) AS n FROM (SELECT trace_id, span_id FROM spans GROUP BY 1, 2)")[0]["n"] == 192


class _Slow(list):
    def __iter__(self):
        time.sleep(0.03)
        return super().__iter__()


def test_insert_wait_bound(db):
    c = Consumer()
    from trailbeam.server.otlp import decode
    recs = decode(to_proto([c.receive(T0, "m1", "rh-1")]).SerializeToString(), "application/x-protobuf").records
    done = threading.Event()

    def holder():
        with db.lock:
            done.wait(5)

    t = threading.Thread(target=holder)
    t.start()
    time.sleep(0.05)
    try:
        t0 = time.monotonic()
        with pytest.raises(WriterBusy):
            db.insert(recs, wait_s=0.1)
        assert time.monotonic() - t0 < 2
    finally:
        done.set()
        t.join()
    timings = {}
    assert db.insert(recs, wait_s=1, timings=timings) == (1, 0)
    assert set(timings) == {"wait_s", "write_s"}


def test_retry_after_follows_the_backlog():
    g = IngestGate(max_pending=4, max_wait_s=1)
    assert g.retry_after() == 1
    g.wrote(0.9)
    with g.admit(), g.admit(), g.admit():
        assert g.retry_after() == 3  # 0.9 s x 3 waiting, rounded up
        with g.admit():
            with pytest.raises(Rejected) as e:
                with g.admit():
                    pass
    assert e.value.status == 429 and e.value.retry_after_s == RETRY_AFTER_MAX_S
    assert g.pending == 0


def test_status_body_is_a_google_rpc_status():
    from opentelemetry.proto.collector.trace.v1 import trace_service_pb2  # noqa: F401 (protobuf available)
    try:
        from google.rpc import status_pb2
    except ImportError:
        pytest.skip("googleapis-common-protos not installed")
    ctype, out = status_body(14, "behind", json_mode=False)
    s = status_pb2.Status.FromString(out)
    assert ctype == "application/x-protobuf" and (s.code, s.message) == (14, "behind")


def test_app_ingest_still_answers_partial_success(db):
    app = App(db, ui_dir=None)
    status, ctype, out, headers = app.ingest(body(1), "application/x-protobuf")
    assert (status, headers) == (200, {})
    assert ExportTraceServiceResponse.FromString(out).partial_success.rejected_spans == 0
