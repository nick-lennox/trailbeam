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


def kinds(store):
    return [(r["kind"], r["mid"]) for r in store.query("SELECT kind, mid FROM spans ORDER BY row")]


def test_protobuf(server, store):
    c = Consumer()
    status, ctype, body = post(server, to_proto([c.receive(T0, "m1", "rh-1"), c.delete(T0 + SEC, "rh-1")]).SerializeToString())
    assert (status, ctype) == (200, "application/x-protobuf")
    assert not ExportTraceServiceResponse.FromString(body).HasField("partial_success")
    assert kinds(store) == [("receive", "m1"), ("delete", "m1")]


def test_gzip_protobuf_as_the_collector_sends_it(server, store):
    body = gzip.compress(to_proto([Consumer().send(T0, "m1")]).SerializeToString())
    assert post(server, body, encoding="gzip")[0] == 200
    assert kinds(store) == [("send", "m1")]


def test_json(server, store):
    c = Consumer()
    body = json.dumps(to_json([c.receive(T0, "m1", "rh-1"), c.change_visibility(T0 + SEC, "rh-1", 0)])).encode()
    status, ctype, out = post(server, body, ctype="application/json")
    assert (status, ctype, json.loads(out)) == (200, "application/json", {})
    assert kinds(store) == [("receive", "m1"), ("change_visibility", "m1")]
    row = store.query("SELECT * FROM spans WHERE kind='receive'")[0]
    assert (row["receive_count"], row["pid"], row["service_name"], row["time_ns"]) == (1, 4242, "orders-worker", T0)


def test_spans_of_other_scopes_are_ignored(server, store):
    status, _, body = post(server, to_proto([Consumer().send(T0, "m1")], scope="opentelemetry.instrumentation.botocore").SerializeToString())
    assert status == 200 and not ExportTraceServiceResponse.FromString(body).HasField("partial_success")
    assert kinds(store) == []
    assert get(server, "/api/health")[1]["ingest"]["ignored"] == 1


@pytest.mark.parametrize("mutate,reason", [
    (lambda a: a.update({"qv2.schema_version": 2}), "qv2.schema_version 2 is not 1"),
    (lambda a: a.pop("qv2.receipt_handle.hmac"), "receive span without receipt_hmac"),
    (lambda a: a.update({"qv2.kind": "sqs.peek"}), "unknown qv2.kind"),
    (lambda a: a.update({"messaging.message.id": ""}), "receive span without message_id"),
])
def test_invalid_qv2_spans_are_rejected_and_reported(server, store, mutate, reason):
    c = Consumer()
    bad = c.receive(T0, "m1", "rh-1")
    mutate(bad.attrs)
    status, _, body = post(server, to_proto([bad, c.send(T0, "m2")]).SerializeToString())
    ps = ExportTraceServiceResponse.FromString(body).partial_success
    assert status == 200 and ps.rejected_spans == 1 and reason in ps.error_message
    assert kinds(store) == [("send", "m2")]


def test_rejections_in_json_mode(server, store):
    bad = Consumer().receive(T0, "m1", "rh-1")
    bad.attrs["qv2.schema_version"] = 2
    out = json.loads(post(server, json.dumps(to_json([bad])).encode(), ctype="application/json")[2])
    assert out["partialSuccess"]["rejectedSpans"] == "1"


def test_settle_operation_decides_delete_or_change_visibility(server, store):
    """Classification uses messaging.operation.name, never the span name (contract section 1)."""
    d = Consumer().delete(T0, "rh-1")
    d.name = "qv2 change_visibility"  # misleading name: must be ignored
    post(server, to_proto([d]).SerializeToString())
    assert [r["kind"] for r in store.query("SELECT kind FROM spans")] == ["delete"]


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


def test_health(server, store):
    c = Consumer()
    post(server, to_proto([c.receive(T0, "m1", "rh-1"), c.delete(T0, "rh-2")]).SerializeToString())
    status, h = get(server, "/api/health")
    assert status == 200 and h["status"] == "ok"
    assert h["ingest"]["inserted"] == 2
    assert h["store"] == {"spans": {"receive": 1, "delete": 1}, "messages": 1, "unjoined_settles": 1}
