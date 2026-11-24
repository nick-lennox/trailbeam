"""GET /metrics: Prometheus text format, the ingest, join, page and database series, bounded labels."""
import re
import time
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
    # A GET is counted after its answer is sent (do_GET's finally), on that request's own server thread. The next
    # request, on a new thread, can be answered and counted first, so /api/health being counted says nothing about
    # "/" (QV-389 waited for /api/health only; "/" was still missing, QV-502). Wait for every count asserted below.
    want = {'trailbeam_ingest_requests_total{code="200"}': 2,
            'trailbeam_ingest_spans_total{result="inserted"}': 4,
            'trailbeam_ingest_request_duration_seconds_count{outcome="accepted"}': 2,
            'trailbeam_ingest_write_duration_seconds_count': 2,
            'trailbeam_ingest_writer_wait_seconds_count': 2,
            'trailbeam_join_duration_seconds_count{join="settle"}': 2,
            'trailbeam_join_duration_seconds_count{join="receive"}': 2,
            'trailbeam_ingest_queue_depth': 0,
            'trailbeam_ingest_queue_capacity': 16,
            'trailbeam_db_last_row': 4,
            # page latency by route template: never the message ID
            'trailbeam_http_request_duration_seconds_count{route="/api/messages/{id}"}': 1,
            'trailbeam_http_request_duration_seconds_count{route="/api/messages"}': 1,
            'trailbeam_http_request_duration_seconds_count{route="/"}': 1,
            'trailbeam_http_requests_total{route="/api/health",code="200"}': 1}
    deadline = time.monotonic() + 10
    while True:
        s = samples(scrape(server))
        if all(s.get(k) == v for k, v in want.items()) or time.monotonic() > deadline:
            break
        time.sleep(0.05)
    assert {k: s.get(k) for k in want} == want
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


def _gauges(app):
    return samples(app.metrics_text().decode())


def test_stuck_and_dead_letter_gauges(db, ingest):
    """What a monitor alerts on (QV-304): stuck messages as the Stuck page counts them, the oldest one's age,
    and messages moved to a dead-letter queue in the last 15 minutes."""
    from trailbeam.server import server
    now = [T0 + 600 * SEC]
    app = server.App(db, ui_dir=None, stuck_after_s=300, now=lambda: now[0])
    s = _gauges(app)
    assert s["trailbeam_stuck_messages"] == 0
    assert s["trailbeam_stuck_oldest_age_seconds"] == 0
    assert s["trailbeam_dead_lettered_messages_recent"] == 0
    assert s["trailbeam_stuck_after_seconds"] == 300

    c = Consumer()
    ingest(c.send(T0, "old", queue="orders"), c.receive(T0 + SEC, "old", "rh-1", queue="orders"),  # stuck: 600 s
           c.send(T0 + 400 * SEC, "new", queue="orders"),  # 200 s: not yet
           c.send(T0, "done", queue="orders"), c.receive(T0 + SEC, "done", "rh-2", queue="orders"),
           c.delete(T0 + 2 * SEC, "rh-2", queue="orders"),
           c.receive(T0 + 590 * SEC, "dead", "rh-3", count=4, queue="orders-dlq", dlq_source="orders"))
    app._queue_gauges_at = None  # past the cache
    s = _gauges(app)
    assert s["trailbeam_stuck_messages"] == 1
    assert s["trailbeam_stuck_oldest_age_seconds"] == 600
    assert s["trailbeam_dead_lettered_messages_recent"] == 1

    now[0] += 16 * 60 * SEC  # the move is out of the 15 minutes; "dead" and "new" are now stuck too
    s = _gauges(app)
    assert s["trailbeam_dead_lettered_messages_recent"] == 1, "cached for a few seconds"
    app._queue_gauges_at = None
    s = _gauges(app)
    assert s["trailbeam_dead_lettered_messages_recent"] == 0
    assert s["trailbeam_stuck_messages"] == 3
    assert s["trailbeam_stuck_oldest_age_seconds"] == 600 + 16 * 60


def test_stuck_count_is_capped(db, ingest, monkeypatch):
    from trailbeam.server import server
    monkeypatch.setattr(server, "STUCK_COUNT_CAP", 3)
    c = Consumer()
    ingest(*[c.send(T0 + i * SEC, f"m{i}") for i in range(5)])
    app = server.App(db, ui_dir=None, stuck_after_s=60, now=lambda: T0 + 3600 * SEC)
    assert _gauges(app)["trailbeam_stuck_messages"] == 3


def test_a_purged_message_is_not_counted_stuck(db, ingest):
    """QV-245: a message a recorded PurgeQueue removed is off the Stuck page, so off the gauge; one in the purge's
    window (possibly purged) is still counted."""
    from trailbeam.server import server
    c = Consumer()
    ingest(c.send(T0, "gone"), c.send(T0 + 9 * SEC + 1, "during"), c.purge(T0 + 10 * SEC, start=T0 + 9 * SEC))
    app = server.App(db, ui_dir=None, stuck_after_s=60, now=lambda: T0 + 3600 * SEC)
    assert _gauges(app)["trailbeam_stuck_messages"] == 1


def test_a_store_that_fails_leaves_the_queue_gauges_out(db, monkeypatch):
    from trailbeam.server import server
    app = server.App(db, ui_dir=None)

    def boom(*a, **kw):
        raise RuntimeError("store down")
    monkeypatch.setattr(db, "stuck_messages", boom)
    s = _gauges(app)
    assert "trailbeam_stuck_messages" not in s and "trailbeam_dead_lettered_messages_recent" not in s
    assert s['trailbeam_metrics_gauge_errors_total{gauge="trailbeam_stuck_messages"}'] == 1
    assert "trailbeam_ingest_queue_depth" in s, "the rest of the scrape is served"
    monkeypatch.undo()
    assert _gauges(app)["trailbeam_stuck_messages"] == 0, "a failed read is not cached"


def test_the_monitoring_files_name_metrics_the_server_has(db):
    """deploy/monitoring (QV-304): every metric the Datadog check, the monitors, the alert rules and the dashboard
    name is one the server serves, and every label is one with a fixed set of values (no message, queue or
    tenant). deploy/monitoring/check.sh runs them against the real Agent and Prometheus."""
    import json
    from pathlib import Path
    from trailbeam.server import server

    app = server.App(db, ui_dir=None)
    families = {f.name: f for f in app.registry._families + [app.registry.scrape_errors]}
    bounded = {"route", "code", "reason", "result", "outcome", "join", "file", "gauge", "version"}
    for f in families.values():
        assert set(f.labelnames) <= bounded, f"{f.name}: labels {f.labelnames}"

    def known(name):
        base = re.sub(r"_(bucket|sum|count)$", "", name) if name not in families else name
        return name in families or base in families or name.removesuffix("_total") + "_total" in families

    mon = Path(__file__).resolve().parents[3] / "deploy" / "monitoring"
    conf = (mon / "datadog" / "openmetrics.yaml").read_text()
    listed = re.findall(r"^      - ([a-z0-9_]+)\s*$", conf.split("\n    metrics:\n", 1)[1], re.M)
    assert listed and all(known("trailbeam_" + m) for m in listed), [m for m in listed if not known("trailbeam_" + m)]
    for f in (mon / "datadog" / "monitors").glob("*.json"):
        for m in re.findall(r"trailbeam\.([a-z0-9_]+)", json.loads(f.read_text())["query"]):
            assert m in listed and known("trailbeam_" + m), f"{f.name}: {m}"
    exprs = re.findall(r"^\s+expr: (.+)$", (mon / "prometheus" / "trailbeam-alerts.yml").read_text(), re.M)
    exprs += [t["expr"] for p in json.loads((mon / "grafana" / "trailbeam-dashboard.json").read_text())["panels"]
              for t in p["targets"]]
    for e in exprs:
        for m in re.findall(r"\btrailbeam_[a-z0-9_]+", e):
            assert known(m), f"{e}: {m}"
