"""OTLP/HTTP ingest: protobuf and JSON, gzip, and what is ignored or rejected."""
import gzip
import json
import urllib.error
import urllib.request

import pytest
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceResponse

from spans import SEC, T0, Consumer, to_json, to_proto


def post(url, body: bytes, ctype="application/x-protobuf", encoding=None):
    headers = {"Content-Type": ctype}
    if encoding:
        headers["Content-Encoding"] = encoding
    req = urllib.request.Request(url + "/v1/traces", data=body, method="POST", headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=5) as r:
            return r.status, r.headers.get("Content-Type"), r.read()
    except urllib.error.HTTPError as e:
        return e.code, e.headers.get("Content-Type"), e.read()


def get(url, path):
    try:
        with urllib.request.urlopen(url + path, timeout=5) as r:
            return r.status, json.loads(r.read())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read())


def kinds(db):
    return [(r["kind"], r["mid"]) for r in db.query("SELECT kind, mid FROM spans ORDER BY row")]


def test_protobuf(server, db):
    c = Consumer()
    status, ctype, body = post(server, to_proto([c.receive(T0, "m1", "rh-1"), c.delete(T0 + SEC, "rh-1")]).SerializeToString())
    assert (status, ctype) == (200, "application/x-protobuf")
    assert not ExportTraceServiceResponse.FromString(body).HasField("partial_success")
    assert kinds(db) == [("receive", "m1"), ("delete", "m1")]


def test_gzip_protobuf_as_the_collector_sends_it(server, db):
    body = gzip.compress(to_proto([Consumer().send(T0, "m1")]).SerializeToString())
    assert post(server, body, encoding="gzip")[0] == 200
    assert kinds(db) == [("send", "m1")]


def test_json(server, db):
    c = Consumer()
    body = json.dumps(to_json([c.receive(T0, "m1", "rh-1"), c.change_visibility(T0 + SEC, "rh-1", 0)])).encode()
    status, ctype, out = post(server, body, ctype="application/json")
    assert (status, ctype, json.loads(out)) == (200, "application/json", {})
    assert kinds(db) == [("receive", "m1"), ("change_visibility", "m1")]
    row = db.query("SELECT * FROM spans WHERE kind='receive'")[0]
    assert (row["receive_count"], row["pid"], row["service_name"], row["time_ns"]) == (1, 4242, "orders-worker", T0)


def test_spans_of_other_scopes_are_ignored(server, db):
    status, _, body = post(server, to_proto([Consumer().send(T0, "m1")], scope="opentelemetry.instrumentation.botocore").SerializeToString())
    assert status == 200 and not ExportTraceServiceResponse.FromString(body).HasField("partial_success")
    assert kinds(db) == []
    assert get(server, "/api/health")[1]["ingest"]["ignored"] == 1


@pytest.mark.parametrize("mutate,reason", [
    (lambda a: a.update({"trailbeam.schema_version": 1}), "trailbeam.schema_version 1 is not one of 2, 3"),
    (lambda a: a.pop("trailbeam.receipt_handle.hmac"), "receive span without receipt_hmac"),
    (lambda a: a.update({"trailbeam.kind": "sqs.peek"}), "unknown trailbeam.kind"),
    (lambda a: a.update({"messaging.message.id": ""}), "receive span without message_id"),
])
def test_invalid_trailbeam_spans_are_rejected_and_reported(server, db, mutate, reason):
    c = Consumer()
    bad = c.receive(T0, "m1", "rh-1")
    mutate(bad.attrs)
    status, _, body = post(server, to_proto([bad, c.send(T0, "m2")]).SerializeToString())
    ps = ExportTraceServiceResponse.FromString(body).partial_success
    assert status == 200 and ps.rejected_spans == 1 and reason in ps.error_message
    assert kinds(db) == [("send", "m2")]


def test_rejections_in_json_mode(server, db):
    bad = Consumer().receive(T0, "m1", "rh-1")
    bad.attrs["trailbeam.schema_version"] = 1
    out = json.loads(post(server, json.dumps(to_json([bad])).encode(), ctype="application/json")[2])
    assert out["partialSuccess"]["rejectedSpans"] == "1"


def test_settle_operation_decides_delete_or_change_visibility(server, db):
    """Classification uses messaging.operation.name, never the span name (contract section 1)."""
    d = Consumer().delete(T0, "rh-1")
    d.name = "trailbeam change_visibility"  # misleading name: must be ignored
    post(server, to_proto([d]).SerializeToString())
    assert [r["kind"] for r in db.query("SELECT kind FROM spans")] == ["delete"]


@pytest.mark.parametrize("body,ctype,encoding,status", [
    (b"\xff\xff not protobuf", "application/x-protobuf", None, 400),
    (b"{not json", "application/json", None, 400),
    (b"[]", "application/json", None, 400),
    (b"x", "text/plain", None, 400),
    (b"not gzip", "application/x-protobuf", "gzip", 400),
    (b"", "application/x-protobuf", "br", 415),
])
def test_bad_requests(server, body, ctype, encoding, status):
    assert post(server, body, ctype, encoding)[0] == status


def test_unknown_paths(server):
    req = urllib.request.Request(server + "/v1/metrics", data=b"", method="POST")
    with pytest.raises(urllib.error.HTTPError) as e:
        urllib.request.urlopen(req, timeout=5)
    assert e.value.code == 404
    assert get(server, "/api/nope") == (404, {"error": "no such endpoint: /api/nope"})


def test_health(server, db):
    c = Consumer()
    post(server, to_proto([c.receive(T0, "m1", "rh-1"), c.delete(T0, "rh-2")]).SerializeToString())
    status, h = get(server, "/api/health")
    assert status == 200 and h["status"] == "ok"
    assert h["ingest"]["inserted"] == 2
    assert h["data"] == {"last_row": 2}
    status, st = get(server, "/api/stats")
    assert status == 200
    assert st["data"] == {"spans": {"receive": 1, "delete": 1}, "messages": 1, "unjoined_settles": 1}


def test_health_answers_while_an_export_is_being_written(server, db):
    """QV-68: health counted the table behind the writer lock and stopped answering at 10M spans."""
    if db.path == ":memory:":
        pytest.skip(":memory: reads through the writer connection")
    with db.lock:  # an export holds the writer
        db.conn.execute("BEGIN IMMEDIATE")
        try:
            status, h = get(server, "/api/health")
            status2, st = get(server, "/api/stats")
        finally:
            db.conn.execute("ROLLBACK")
    assert status == 200 and h["data"] == {"last_row": 0}
    assert status2 == 200 and st["data"]["spans"] == {}


# -- contract v2 times (section 2) ----------------------------------------------------------------

@pytest.mark.parametrize("encode", ["proto", "json"])
def test_start_and_end_are_stored(server, db, encode):
    c = Consumer()
    spans = [c.send(T0 + 40, "m1", start=T0), c.receive(T0 + 2 * SEC, "m1", "rh-1", lambda_inv="inv-1", start=T0 + SEC)]
    if encode == "proto":
        assert post(server, to_proto(spans).SerializeToString())[0] == 200
    else:
        assert post(server, json.dumps(to_json(spans)).encode(), ctype="application/json")[0] == 200
    rows = {r["kind"]: r for r in db.query("SELECT kind, start_ns, time_ns FROM spans")}
    assert (rows["send"]["start_ns"], rows["send"]["time_ns"]) == (T0, T0 + 40)
    # trailbeam.start_observed = false: the span's start time is not a start, so it is not stored as one.
    assert (rows["receive"]["start_ns"], rows["receive"]["time_ns"]) == (None, T0 + 2 * SEC)


def test_a_span_that_ends_before_it_starts_is_rejected(server, db):
    bad = Consumer().send(T0, "m1", start=T0 + 1)
    status, _, body = post(server, to_proto([bad]).SerializeToString())
    assert status == 200
    assert kinds(db) == []
    assert get(server, "/api/health")[1]["ingest"]["rejected"] == 1

