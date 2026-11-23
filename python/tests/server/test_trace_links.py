"""--trace-link (QV-304): each recorded step's span.trace_url, its checks at start, and "Open trace" on the page."""
import json
import urllib.request

import pytest

from spans import SEC, T0, Consumer, to_proto
from trailbeam.server import server, trace_links

TRACE = "4bf92f3577b34da6a3ce929d0e0e4736"


def test_placeholders_are_filled_from_the_span():
    s = {"trace_id": TRACE, "span_id": "00f067aa0ba902b7"}
    t = ("https://example.test/t/{trace_id}?dd={trace_id_64}&span={span_id}&s={start_ms}&e={end_ms}"
         "&from={from_ms}&to={to_ms}&fs={from_s}&ts={to_s}")
    url = trace_links.trace_url(t, s, 1_700_000_000_000_000_000, 1_700_000_000_250_000_000)
    assert url == (f"https://example.test/t/{TRACE}?dd={int(TRACE[16:], 16)}&span=00f067aa0ba902b7"
                   "&s=1700000000000&e=1700000000250&from=1699996400000&to=1700003600250&fs=1699996400&ts=1700003601")
    # Datadog's 64-bit form: the low 64 bits, in decimal
    assert trace_links.trace_url("https://x.test/{trace_id_64}", s, None, None) == "https://x.test/11803532876627986230"


def test_other_braces_are_left_alone():
    # Grafana Explore takes JSON in the URL; only the known names are placeholders
    t = 'https://grafana.test/explore?left={"queries":[{"query":"{trace_id}"}]}'
    assert trace_links.check(t) == t
    assert trace_links.trace_url(t, {"trace_id": TRACE, "span_id": "ab"}, None, 1) == t.replace("{trace_id}", TRACE)


def test_a_time_placeholder_without_a_time_gives_no_link():
    assert trace_links.trace_url("https://x.test/{trace_id}?t={end_ms}", {"trace_id": TRACE, "span_id": "ab"},
                                 None, None) is None


def test_ids_that_are_not_hex_give_no_link():
    assert trace_links.trace_url("https://x.test/{trace_id}", {"trace_id": "a/../b", "span_id": "ab"}, 1, 2) is None


@pytest.mark.parametrize("bad, why", [
    ("javascript:alert(1)//{trace_id}", "http:// or https://"),
    ("ftp://x.test/{trace_id}", "http:// or https://"),
    ("https://x.test/{traceid}", "unknown placeholder {traceid}"),
    ("https://x.test/trace", "needs {trace_id} or {trace_id_64}"),
    ("https://x.test/a b/{trace_id}", "no spaces"),
    ("https://{trace_id}.x.test/", "http:// or https://"),
])
def test_bad_templates_stop_the_server(bad, why):
    with pytest.raises(ValueError, match=why.replace("{", r"\{").replace("}", r"\}")):
        trace_links.check(bad)


def test_the_flag_and_the_variable(monkeypatch, capsys):
    from trailbeam.server.__main__ import build_parser
    a = build_parser().parse_args(["--trace-link", " https://app.datadoghq.com/apm/trace/{trace_id} "])
    assert a.trace_link == "https://app.datadoghq.com/apm/trace/{trace_id}"
    assert build_parser().parse_args([]).trace_link is None
    monkeypatch.setenv("TRAILBEAM_TRACE_LINK", "http://jaeger:16686/trace/{trace_id}")
    assert build_parser().parse_args([]).trace_link == "http://jaeger:16686/trace/{trace_id}"
    monkeypatch.setenv("TRAILBEAM_TRACE_LINK", "http://jaeger:16686/trace/{trace}")
    with pytest.raises(SystemExit, match="TRAILBEAM_TRACE_LINK: unknown placeholder"):
        build_parser()


def _serve(db, **kw):
    import threading
    httpd = server.make_server(db, "127.0.0.1", 0, ui_dir=None, **kw)
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    return httpd, f"http://127.0.0.1:{httpd.server_address[1]}"


def _post(base, spans):
    req = urllib.request.Request(base + "/v1/traces", data=to_proto(spans).SerializeToString(), method="POST",
                                 headers={"Content-Type": "application/x-protobuf"})
    assert urllib.request.urlopen(req, timeout=5).status == 200


def _get(base, path):
    with urllib.request.urlopen(base + path, timeout=5) as r:
        return r.read().decode()


def test_every_recorded_step_links_to_its_trace(db):
    httpd, base = _serve(db, trace_link="https://app.datadoghq.com/apm/trace/{trace_id}?spanID={span_id}")
    try:
        c = Consumer()
        _post(base, [c.send(T0, "m1", trace_id=bytes.fromhex(TRACE)), c.receive(T0 + SEC, "m1", "rh-1", trace_id=bytes.fromhex(TRACE)),
                     c.delete(T0 + 2 * SEC, "rh-1", trace_id=bytes.fromhex(TRACE))])
        t = json.loads(_get(base, "/api/messages/m1"))
        spans = [e["span"] for e in t["events"] if "span" in e]
        assert len(spans) == 3
        for s in spans:
            assert s["trace_url"] == f"https://app.datadoghq.com/apm/trace/{s['trace_id']}?spanID={s['span_id']}"
        html = _get(base, "/message?id=m1")
        assert html.count(">Open trace</a>") == 3
        assert 'href="https://app.datadoghq.com/apm/trace/' in html and 'rel="noopener noreferrer"' in html
        cfg = json.loads(_get(base, "/api/health"))["config"]
        assert cfg["trace_link"] == "https://app.datadoghq.com/apm/trace/{trace_id}?spanID={span_id}"
    finally:
        httpd.shutdown()
        httpd.server_close()


def test_no_template_no_links(db):
    httpd, base = _serve(db)
    try:
        c = Consumer()
        _post(base, [c.receive(T0, "m1", "rh-1")])
        t = json.loads(_get(base, "/api/messages/m1"))
        assert all("trace_url" not in e["span"] for e in t["events"] if "span" in e)
        assert "Open trace" not in _get(base, "/message?id=m1")
        assert json.loads(_get(base, "/api/health"))["config"]["trace_link"] is None
    finally:
        httpd.shutdown()
        httpd.server_close()
