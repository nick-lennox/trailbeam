"""GET /metrics: Prometheus text format, the ingest, join, page and database series, bounded labels."""
import re
import urllib.request

from spans import SEC, T0, Consumer, to_proto
from trailbeam.server import metrics


def scrape(url):
    with urllib.request.urlopen(url + "/metrics", timeout=5) as r:
        assert r.status == 200
        assert r.headers["Content-Type"] == metrics.CONTENT_TYPE
        return r.read().decode()


def samples(text):
    """{'name{labels}': value} for every sample line, checking each line's syntax."""
    out = {}
    line_re = re.compile(r'^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{(?:[a-zA-Z_][a-zA-Z0-9_]*="(?:[^"\\]|\\.)*",?)*\})? (\S+)$')
    for line in text.splitlines():
        if not line or line.startswith("# HELP ") or line.startswith("# TYPE "):
            continue
        m = line_re.match(line)
        assert m, f"not a sample line: {line!r}"
        out[m.group(1) + (m.group(2) or "")] = float(m.group(3))
    return out


def post(url, data):
    req = urllib.request.Request(url + "/v1/traces", data=data, method="POST",
                                 headers={"Content-Type": "application/x-protobuf"})
    with urllib.request.urlopen(req, timeout=5) as r:
        return r.status


def test_metrics_after_ingest_and_pages(server, db):
    c = Consumer()
    assert post(server, to_proto([c.receive(T0, "m1", "rh-1"), c.delete(T0 + SEC, "rh-1")]).SerializeToString()) == 200
    assert post(server, to_proto([c.delete(T0 + 2 * SEC, "rh-2"), c.receive(T0 + SEC, "m2", "rh-2")])
                .SerializeToString()) == 200
    for path in ("/api/messages", "/api/messages/m1", "/api/messages/m1/body", "/", "/api/health"):
        try:
            urllib.request.urlopen(server + path, timeout=5).read()
        except urllib.error.HTTPError:
            pass  # the body view of a server that keeps no bodies may 404; it is still timed
    s = samples(scrape(server))
    assert s['trailbeam_ingest_requests_total{code="200"}'] == 2
    assert s['trailbeam_ingest_spans_total{result="inserted"}'] == 4
    assert s['trailbeam_ingest_request_duration_seconds_count{outcome="accepted"}'] == 2
    assert s['trailbeam_ingest_write_duration_seconds_count'] == 2
    assert s['trailbeam_ingest_writer_wait_seconds_count'] == 2
    assert s['trailbeam_join_duration_seconds_count{join="settle"}'] == 2
    assert s['trailbeam_join_duration_seconds_count{join="receive"}'] == 2
    assert s['trailbeam_ingest_queue_depth'] == 0
    assert s['trailbeam_ingest_queue_capacity'] == 16
    assert s['trailbeam_db_last_row'] == 4
    # page latency by route template: never the message ID
    assert s['trailbeam_http_request_duration_seconds_count{route="/api/messages/{id}"}'] == 1
    assert s['trailbeam_http_request_duration_seconds_count{route="/api/messages"}'] == 1
    assert s['trailbeam_http_request_duration_seconds_count{route="/"}'] == 1
    assert s['trailbeam_http_requests_total{route="/api/health",code="200"}'] == 1
    assert not any("m1" in k for k in s)
    if db.path != ":memory:":
        assert s['trailbeam_db_file_bytes{file="main"}'] > 0
    else:
        assert not any(k.startswith("trailbeam_db_file_bytes") for k in s)
    assert any(k.startswith("trailbeam_build_info{version=") for k in s)


def test_rejected_bodies_are_counted(server):
    req = urllib.request.Request(server + "/v1/traces", data=b"x", method="POST",
                                 headers={"Content-Type": "application/x-protobuf", "Content-Encoding": "br"})
    try:
        urllib.request.urlopen(req, timeout=5)
    except urllib.error.HTTPError as e:
        assert e.code == 415
    s = samples(scrape(server))
    assert s['trailbeam_ingest_requests_total{code="415"}'] == 1


def test_histogram_is_cumulative():
    r = metrics.Registry()
    h = r.histogram("x_seconds", "x", buckets=(0.1, 1))
    for v in (0.05, 0.1, 0.5, 2):
        h.observe(v)
    s = samples(r.render())
    assert s == {'x_seconds_bucket{le="0.1"}': 2, 'x_seconds_bucket{le="1"}': 3, 'x_seconds_bucket{le="+Inf"}': 4,
                 "x_seconds_sum": 2.65, "x_seconds_count": 4}


def test_failing_gauge_does_not_fail_the_scrape():
    r = metrics.Registry()
    r.gauge("ok_value", "fine", lambda: 1)
    r.gauge("bad_value", "fails", lambda: 1 / 0)
    s = samples(r.render())
    assert s["ok_value"] == 1 and "bad_value" not in s
    assert s['trailbeam_metrics_gauge_errors_total{gauge="bad_value"}'] == 1


def test_label_values_are_escaped():
    r = metrics.Registry()
    r.counter("c_total", "c", ("v",)).inc(v='a"b\\c\nd')
    assert 'c_total{v="a\\"b\\\\c\\nd"} 1' in r.render()


def test_route_labels():
    api = {"messages", "health", "tenants"}
    assert metrics.route_label("/api/messages/abc-123", api) == "/api/messages/{id}"
    assert metrics.route_label("/api/messages/abc/body", api) == "/api/messages/{id}/body"
    assert metrics.route_label("/api/tenants/search", api) == "/api/tenants/search"
    assert metrics.route_label("/api/nope/1", api) == "/api/other"
    assert metrics.route_label("/ui/assets/app-1234.js", api) == "/ui/{file}"
    assert metrics.route_label("/favicon.ico", api) == "other"
    assert metrics.route_label("/message", api) == "/message"
