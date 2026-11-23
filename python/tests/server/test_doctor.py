"""``trailbeam doctor`` (QV-148): each check passes against a working server, and each one fails, with its cause
and fix, when what it checks is broken. The test span never shows as a message."""
import http.server
import io
import json
import socket
import threading
import urllib.request

import pytest

from spans import T0, Consumer, to_json, to_proto
from test_openapi import serve
from trailbeam.server import auth as auth_mod, doctor, otlp, server
from trailbeam.server.store import StoreUnavailable, WriterBusy

TOKEN = "d" * 64
NOW = T0 + 60 * 10 ** 9  # a minute after the spans of these tests


def target(base, token=None, ingest=None, **kw):
    return doctor.Target(url=base, ingest_url=ingest or base + doctor.INGEST_PATH, token=token,
                         token_source="a test" if token else "none", timeout_s=2, **kw)


def by_name(results):
    return {r.name: r for r in results}


def statuses(results):
    return [r.status for r in results]


def send_real_span(base, token=None, scope="trailbeam"):
    body = to_proto([Consumer().receive(T0, "m-1", "rh-1")], scope=scope).SerializeToString()
    h = {"Content-Type": "application/x-protobuf", **({"Authorization": f"Bearer {token}"} if token else {})}
    with urllib.request.urlopen(urllib.request.Request(base + "/v1/traces", data=body, headers=h), timeout=5) as r:
        assert r.status == 200


def api(base, path, token=None):
    h = {"Authorization": f"Bearer {token}"} if token else {}
    with urllib.request.urlopen(urllib.request.Request(base + path, headers=h), timeout=5) as r:
        return json.loads(r.read())


@pytest.fixture(autouse=True)
def _no_otlp_headers(monkeypatch):
    """The exporter headers of the machine running the tests are not the ones a test means."""
    for k in doctor.OTLP_HEADERS_ENV:
        monkeypatch.delenv(k, raising=False)


@pytest.fixture
def app(db):
    return server.App(db, ui_dir=None)


@pytest.fixture
def with_token(db):
    return server.App(db, ui_dir=None, auth=auth_mod.Auth(TOKEN))


def free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


# ---- every check passes ---------------------------------------------------------------------------------------

def test_a_working_server_with_a_token_passes_every_check(with_token):
    with serve(with_token) as base:
        send_real_span(base, TOKEN)
        results = doctor.run(target(base, TOKEN), now_ns=NOW)
        out = io.StringIO()
        code = doctor.report(target(base, TOKEN), results, out)
    assert statuses(results) == ["ok"] * 5, out.getvalue()
    assert code == 0 and "All checks passed." in out.getvalue()
    r = by_name(results)
    assert "accept the token" in r["Token accepted"].detail
    assert "read back" in r["Test span stored and read back"].detail
    assert "60 s ago" in r["Last real span"].detail


def test_a_server_without_a_token_passes(app):
    with serve(app) as base:
        send_real_span(base)
        results = doctor.run(target(base), now_ns=NOW)
    assert statuses(results) == ["ok"] * 5
    assert "no access token" in by_name(results)["Token accepted"].detail


def test_the_test_span_never_shows_as_a_message_and_is_read_back(app, db):
    with serve(app) as base:
        results = doctor.run(target(base), now_ns=NOW)
        assert by_name(results)["Test span stored and read back"].status == "ok"
        assert api(base, "/api/messages")["messages"] == []
        assert api(base, "/api/stats")["data"]["messages"] == 0
        assert api(base, "/api/stats")["data"]["spans"] == {}
        assert api(base, "/api/health")["data"] == {"last_row": 0, "last_span_ns": None}
    assert db.last_row() == 0
    [(probe_id, _)] = db.conn.execute("SELECT id, received_ns FROM probes").fetchall()
    assert probe_id.startswith("doctor-")


def test_probes_older_than_a_day_are_deleted_when_one_is_stored(db):
    db.record_probes(["old-probe-1"], 1)
    db.record_probes(["new-probe-1"], db.PROBE_KEEP_NS + 2)
    assert db.probe("old-probe-1") is None and db.probe("new-probe-1") == db.PROBE_KEEP_NS + 2


# ---- 1. the server --------------------------------------------------------------------------------------------

def test_nothing_listening_fails_the_server_and_ingest_and_skips_the_rest():
    base = f"http://127.0.0.1:{free_port()}"
    results = doctor.run(target(base), now_ns=NOW)
    assert statuses(results) == ["FAIL", "FAIL", "skip", "skip", "skip"]
    r = by_name(results)["Server reachable"]
    assert "nothing is listening" in r.detail and "trailbeam start" in r.fix
    out = io.StringIO()
    assert doctor.report(target(base), results, out) == 1
    assert "Likely cause:" in out.getvalue() and "Fix:" in out.getvalue() and "2 of 5 checks failed" in out.getvalue()


def test_a_host_that_does_not_resolve():
    r = doctor.run(target("http://no-such-host.invalid:4318"), now_ns=NOW)[0]
    assert r.status == "FAIL" and "does not resolve" in r.detail


class _NotTrailbeam(http.server.BaseHTTPRequestHandler):
    def do_GET(self):  # noqa: N802
        body = b"<html>hello</html>"
        self.send_response(200)
        self.send_header("Content-Type", "text/html")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    do_POST = do_GET  # noqa: N815

    def log_message(self, *a):
        pass


def test_another_service_on_the_port_is_not_taken_for_trailbeam():
    httpd = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _NotTrailbeam)
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    try:
        results = doctor.run(target(f"http://127.0.0.1:{httpd.server_address[1]}"), now_ns=NOW)
    finally:
        httpd.shutdown()
        httpd.server_close()
    r = by_name(results)["Server reachable"]
    assert r.status == "FAIL" and "not as Trailbeam does" in r.detail
    assert by_name(results)["Token accepted"].status == "skip"


# ---- 2. the ingest port ---------------------------------------------------------------------------------------

def test_a_closed_ingest_port_fails_and_names_the_variable_services_read(app):
    with serve(app) as base:
        ingest = f"http://127.0.0.1:{free_port()}/v1/traces"
        results = doctor.run(target(base, ingest=ingest), now_ns=NOW)
    r = by_name(results)["Ingest port accepts connections"]
    assert r.status == "FAIL" and "nothing is listening" in r.detail
    assert f"TRAILBEAM_OTLP_TRACES_ENDPOINT={ingest}" in r.fix
    assert statuses(results)[2:] == ["skip"] * 3


def test_a_wrong_ingest_path_fails(app):
    with serve(app) as base:
        results = doctor.run(target(base, ingest=base + "/v1/wrong"), now_ns=NOW)
    r = by_name(results)["Ingest port accepts connections"]
    assert r.status == "FAIL" and "answers 404" in r.detail and "/v1/traces" in r.fix


# ---- 3. the token ---------------------------------------------------------------------------------------------

def test_no_token_given_to_a_server_that_has_one(with_token):
    with serve(with_token) as base:
        results = doctor.run(target(base), now_ns=NOW)
    r = by_name(results)["Token accepted"]
    assert r.status == "FAIL" and "missing" in r.detail and "--token-file" in r.fix
    assert statuses(results)[3:] == ["skip", "skip"]


def test_the_wrong_token(with_token):
    with serve(with_token) as base:
        results = doctor.run(target(base, "w" * 64), now_ns=NOW)
    r = by_name(results)["Token accepted"]
    assert r.status == "FAIL" and "not accepted" in r.detail and "another one" in r.cause


def test_a_token_file_that_cannot_be_read(with_token, tmp_path, monkeypatch):
    for k in ("TRAILBEAM_TOKEN", "TRAILBEAM_URL", "TRAILBEAM_PORT", "TRAILBEAM_OTLP_TRACES_ENDPOINT"):
        monkeypatch.delenv(k, raising=False)
    (tmp_path / "empty").write_text("\n")
    with serve(with_token) as base:
        for path, said in ((tmp_path / "missing", "cannot read"), (tmp_path / "empty", "is empty")):
            monkeypatch.setenv("TRAILBEAM_TOKEN_FILE", str(path))
            t = doctor.target_from_args(doctor_args("--url", base))
            assert t.token_error and said in t.token_error
            r = by_name(doctor.run(t, now_ns=NOW))["Token accepted"]
            assert r.status == "FAIL" and said in r.cause


def test_ingest_behind_another_server_with_its_own_token(app, db, tmp_path):
    """--url has no token, but the ingest address is a server that has one."""
    from trailbeam.server.db import Database
    other_db = Database(str(tmp_path / "other.sqlite"))
    try:
        with serve(app) as base, serve(server.App(other_db, ui_dir=None, auth=auth_mod.Auth(TOKEN))) as other:
            results = doctor.run(target(base, ingest=other + "/v1/traces"), now_ns=NOW)
    finally:
        other_db.close()
    r = by_name(results)["Token accepted"]
    assert r.status == "FAIL" and "another server" in r.cause


# ---- 4. the test span -----------------------------------------------------------------------------------------

def test_ingest_and_the_api_are_two_different_servers(app, tmp_path, monkeypatch):
    from trailbeam.server.db import Database
    monkeypatch.setattr(doctor, "READ_BACK_S", 0.3)
    other_db = Database(str(tmp_path / "other.sqlite"))
    try:
        with serve(app) as base, serve(server.App(other_db, ui_dir=None)) as other:
            results = doctor.run(target(base, ingest=other + "/v1/traces"), now_ns=NOW)
    finally:
        other_db.close()
    r = by_name(results)["Test span stored and read back"]
    assert r.status == "FAIL" and "does not have it" in r.detail and "two different Trailbeams" in r.cause


def test_a_server_older_than_doctor_rejects_the_test_span(app, monkeypatch):
    monkeypatch.setattr(otlp, "PROBE_KIND", "not-known-to-an-old-server")  # 0.3 reads it as an unknown kind
    with serve(app) as base:
        results = doctor.run(target(base), now_ns=NOW)
    r = by_name(results)["Test span stored and read back"]
    assert r.status == "FAIL" and "does not know test spans" in r.detail and "upgrade" in r.fix
    last = by_name(results)["Last real span"]  # the server counted the test span as rejected: not the user's span
    assert last.status == "FAIL" and "break the span contract" not in last.cause


def test_a_rejected_test_span_is_not_counted_twice_among_real_rejected_ones(app, monkeypatch):
    monkeypatch.setattr(otlp, "PROBE_KIND", "not-known-to-an-old-server")
    bad = to_json([Consumer().send(T0, "m-bad")])
    bad["resourceSpans"][0]["scopeSpans"][0]["spans"][0]["attributes"][0]["value"] = {"intValue": "1"}
    with serve(app) as base:
        urllib.request.urlopen(urllib.request.Request(base + "/v1/traces", data=json.dumps(bad).encode(),
                                                      headers={"Content-Type": "application/json"}), timeout=5)
        r = by_name(doctor.run(target(base), now_ns=NOW))["Last real span"]
    assert "1 Trailbeam span(s) arrived" in r.cause and "break the span contract" in r.cause


@pytest.mark.parametrize("error, said", [(StoreUnavailable("down"), "503"), (WriterBusy(), "503")])
def test_an_overloaded_or_disconnected_store(app, db, monkeypatch, error, said):
    def fail(*a, **kw):
        raise error
    monkeypatch.setattr(db, "record_probes", fail)
    with serve(app) as base:
        results = doctor.run(target(base), now_ns=NOW)
    r = by_name(results)["Test span stored and read back"]
    assert r.status == "FAIL" and said in r.detail and "/api/ready" in r.fix


def test_a_store_that_does_not_keep_probes(app, db, monkeypatch):
    def no(*a, **kw):
        raise NotImplementedError("this store does not keep probes")
    monkeypatch.setattr(db, "record_probes", no)
    with serve(app) as base:
        results = doctor.run(target(base), now_ns=NOW)
    r = by_name(results)["Test span stored and read back"]
    assert r.status == "FAIL" and "its store does not keep them" in r.detail and "store" in r.fix
    # doctor's own rejected test span is not taken for a span of the user's that broke the contract
    assert "break the span contract" not in by_name(results)["Last real span"].cause


def test_a_test_span_id_the_server_does_not_accept(app, monkeypatch):
    monkeypatch.setattr(otlp, "valid_probe_id", lambda v: False)
    with serve(app) as base:
        r = by_name(doctor.run(target(base), now_ns=NOW))["Test span stored and read back"]
    assert r.status == "FAIL" and "did not accept the test span's ID" in r.detail and "same Trailbeam version" in r.fix


def test_the_reasons_doctor_tells_apart_are_the_servers(app, db):
    """doctor copies the fixed starts of the reasons (contract section 17) so it runs without the server extra."""
    assert doctor.PROBE_NOT_STORED == otlp.PROBE_NOT_STORED and doctor.PROBE_BAD_ID == otlp.PROBE_BAD_ID
    assert doctor.PROBE_UNKNOWN == f"unknown trailbeam.kind {otlp.PROBE_KIND!r}"


def test_an_export_carries_at_most_8_probes_in_one_write(app, db, monkeypatch):
    calls = []
    real = db.record_probes
    monkeypatch.setattr(db, "record_probes", lambda ids, *a, **kw: (calls.append(list(ids)), real(ids, *a, **kw)))
    export = json.loads(doctor.probe_export("doctor-0000000000000000"))
    one = export["resourceSpans"][0]["scopeSpans"][0]["spans"][0]
    spans = []
    for i in range(otlp.MAX_PROBES_PER_EXPORT + 2):
        sp = json.loads(json.dumps(one))
        sp["attributes"] = [a if a["key"] != "trailbeam.probe.id" else
                            {"key": a["key"], "value": {"stringValue": f"doctor-{i:016d}"}} for a in sp["attributes"]]
        spans.append(sp)
    export["resourceSpans"][0]["scopeSpans"][0]["spans"] = spans
    with serve(app) as base:
        with urllib.request.urlopen(urllib.request.Request(base + "/v1/traces", data=json.dumps(export).encode(),
                                    headers={"Content-Type": "application/json"}), timeout=5) as r:
            answer = json.loads(r.read())
        health = api(base, "/api/health")
    assert calls == [[f"doctor-{i:016d}" for i in range(otlp.MAX_PROBES_PER_EXPORT)]]  # one transaction
    assert answer["partialSuccess"]["rejectedSpans"] == "2"
    assert f"{otlp.PROBE_OVER_LIMIT} {otlp.MAX_PROBES_PER_EXPORT} in one export" in answer["partialSuccess"]["errorMessage"]
    assert health["ingest"]["rejected"] == 2
    assert db.probe(f"doctor-{otlp.MAX_PROBES_PER_EXPORT:016d}") is None



def test_the_probe_write_counts_toward_backpressure(app, db, monkeypatch):
    import time
    real = db.record_probes
    monkeypatch.setattr(db, "record_probes", lambda *a, **kw: (time.sleep(0.2), real(*a, **kw)))
    seen = []
    monkeypatch.setattr(app.gate, "wrote", seen.append)
    with serve(app) as base:
        urllib.request.urlopen(urllib.request.Request(base + "/v1/traces",
                                                      data=doctor.probe_export("doctor-0000000000000001"),
                                                      headers={"Content-Type": "application/json"}), timeout=5)
    assert len(seen) == 1 and seen[0] >= 0.2  # the spans' write and the probes' write, reported once

# ---- 5. the last real span ------------------------------------------------------------------------------------

def test_no_span_yet_says_the_services_are_not_sending(app):
    with serve(app) as base:
        results = doctor.run(target(base), now_ns=NOW)
    r = by_name(results)["Last real span"]
    assert r.status == "FAIL" and r.detail == "no span stored yet"
    assert "the test span went through" in r.cause and "trailbeam.boto.install()" in r.fix
    assert statuses(results) == ["ok", "ok", "ok", "ok", "FAIL"]


def test_no_span_yet_but_other_traces_arrive(app):
    with serve(app) as base:
        send_real_span(base, scope="not-trailbeam")
        r = by_name(doctor.run(target(base), now_ns=NOW))["Last real span"]
    assert r.status == "FAIL" and "1 span(s) arrived that are not Trailbeam's" in r.cause


def test_no_span_yet_but_spans_were_rejected(app):
    bad = to_json([Consumer().send(T0, "m-bad")])
    bad["resourceSpans"][0]["scopeSpans"][0]["spans"][0]["attributes"][0]["value"] = {"intValue": "1"}
    with serve(app) as base:
        urllib.request.urlopen(urllib.request.Request(base + "/v1/traces", data=json.dumps(bad).encode(),
                                                      headers={"Content-Type": "application/json"}), timeout=5)
        r = by_name(doctor.run(target(base), now_ns=NOW))["Last real span"]
    assert r.status == "FAIL" and "break the span contract" in r.cause


def test_an_old_last_span_is_a_warning_not_a_failure(app):
    with serve(app) as base:
        send_real_span(base)
        results = doctor.run(target(base), now_ns=T0 + 3 * 3600 * 10 ** 9)
    r = by_name(results)["Last real span"]
    assert r.status == "warn" and "3.0 h ago" in r.detail
    assert doctor.report(target(base), results, io.StringIO()) == 0


def test_a_span_from_the_future_warns_about_clocks(app):
    with serve(app) as base:
        send_real_span(base)
        r = by_name(doctor.run(target(base), now_ns=T0 - 3600 * 10 ** 9))["Last real span"]
    assert r.status == "warn" and "in the future" in r.detail and "NTP" in r.fix


def test_an_unreachable_store_fails_the_last_check():
    health = {"status": "ok", "version": "x", "data": {"last_row": None, "last_span_ns": None, "store": "unreachable"}}
    r = doctor._last_span(health, NOW, probe_ok=False, t=target("http://x"))
    assert r.status == "FAIL" and "cannot reach its database" in r.detail


# ---- the command ----------------------------------------------------------------------------------------------

def doctor_args(*argv):
    return doctor.parser().parse_args(list(argv))


def test_the_command_reads_the_token_trailbeam_start_made_and_exits_1_on_a_failure(with_token, tmp_path,
                                                                                    monkeypatch, capsys):
    for k in ("TRAILBEAM_TOKEN", "TRAILBEAM_TOKEN_FILE", "TRAILBEAM_URL", "TRAILBEAM_OTLP_TRACES_ENDPOINT"):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setenv("TRAILBEAM_DATA_DIR", str(tmp_path))
    (tmp_path / "token").write_text(TOKEN + "\n")
    with serve(with_token) as base:
        assert doctor.main(["--url", base]) == 1  # no real span yet
        out = capsys.readouterr().out
        assert f"token: {tmp_path / 'token'}" in out and "FAIL  Last real span" in out
        send_real_span(base, TOKEN)
        assert doctor.main(["--url", base]) == 0
        assert "All checks passed." in capsys.readouterr().out


def test_with_a_services_environment_the_token_comes_from_otel_exporter_otlp_headers(with_token, tmp_path,
                                                                                         monkeypatch, capsys):
    """QV-343: the variables trailbeam start --data-dir prints for services are enough for doctor, and the token
    is the one in the header the services send, not ~/.trailbeam/token."""
    for k in ("TRAILBEAM_TOKEN", "TRAILBEAM_TOKEN_FILE", "TRAILBEAM_URL", "TRAILBEAM_PORT"):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setenv("TRAILBEAM_DATA_DIR", str(tmp_path))  # holds no token: not where the service's one is
    with serve(with_token) as base:
        monkeypatch.setenv("TRAILBEAM_OTLP_TRACES_ENDPOINT", base + "/v1/traces")
        monkeypatch.setenv("OTEL_EXPORTER_OTLP_HEADERS", f"Authorization=Bearer%20{TOKEN}")
        send_real_span(base, TOKEN)
        assert doctor.main([]) == 0
        out = capsys.readouterr().out
        assert "token: OTEL_EXPORTER_OTLP_HEADERS" in out and "All checks passed." in out
        # a wrong token in the header is reported as the services' token, not as a missing one
        monkeypatch.setenv("OTEL_EXPORTER_OTLP_HEADERS", "Authorization=Bearer%20" + "w" * 64)
        assert doctor.main([]) == 1
        out = capsys.readouterr().out
        assert "not accepted" in out and "token from OTEL_EXPORTER_OTLP_HEADERS" in out


@pytest.mark.parametrize("value,token,error", [
    (f"Authorization=Bearer%20{TOKEN}", TOKEN, None),
    (f"x-tenant=a,authorization=bearer%20{TOKEN} ", TOKEN, None),
    (f"Authorization=Bearer {TOKEN}", TOKEN, None),
    ("x-tenant=a", None, "has no Authorization header"),
    ("Authorization=Basic%20abc", None, "not 'Bearer <token>'"),
    ("Authorization=Bearer%20", None, "not 'Bearer <token>'"),
])
def test_the_token_in_otlp_headers(value, token, error):
    got, err = doctor._token_from_headers(value, "OTEL_EXPORTER_OTLP_HEADERS")
    assert got == token and (err is None if error is None else error in err)


def test_otlp_headers_order(monkeypatch, tmp_path):
    for k in ("TRAILBEAM_TOKEN", "TRAILBEAM_TOKEN_FILE", "TRAILBEAM_URL", "TRAILBEAM_OTLP_TRACES_ENDPOINT"):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setenv("TRAILBEAM_DATA_DIR", str(tmp_path))
    (tmp_path / "token").write_text("from-file\n")
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_HEADERS", "Authorization=Bearer%20general")
    t = doctor.target_from_args(doctor_args("--url", "http://x"), discover=False)
    assert (t.token, t.token_source) == ("general", "OTEL_EXPORTER_OTLP_HEADERS")  # over the default file
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_TRACES_HEADERS", "Authorization=Bearer%20traces")
    t = doctor.target_from_args(doctor_args("--url", "http://x"), discover=False)
    assert (t.token, t.token_source) == ("traces", "OTEL_EXPORTER_OTLP_TRACES_HEADERS")  # as exporters choose
    monkeypatch.setenv("TRAILBEAM_TOKEN", "given")
    assert doctor.target_from_args(doctor_args("--url", "http://x"), discover=False).token == "given"
    monkeypatch.delenv("TRAILBEAM_TOKEN")
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_TRACES_HEADERS", "x-tenant=a")
    t = doctor.target_from_args(doctor_args("--url", "http://x"), discover=False)
    assert t.token is None and "no Authorization header" in t.token_error
    assert "OTEL_EXPORTER_OTLP_TRACES_HEADERS=Authorization=Bearer%20<token>" in doctor._token_cause(t, "the API")[1]


def test_trailbeam_doctor_is_a_subcommand(monkeypatch):
    from trailbeam.server import __main__ as cli
    got = []
    monkeypatch.setattr(doctor, "main", lambda argv: got.append(argv) or 3)
    with pytest.raises(SystemExit) as e:
        cli.main(["doctor", "--url", "http://x"])
    assert e.value.code == 3 and got == [["--url", "http://x"]]


def test_defaults_follow_the_environment(monkeypatch):
    for k in ("TRAILBEAM_URL", "TRAILBEAM_OTLP_TRACES_ENDPOINT", "TRAILBEAM_TOKEN_FILE"):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setenv("TRAILBEAM_PORT", "9999")
    monkeypatch.setenv("TRAILBEAM_TOKEN", "from-env")
    t = doctor.target_from_args(doctor_args())
    assert (t.url, t.ingest_url, t.token, t.token_source) == (
        "http://127.0.0.1:9999", "http://127.0.0.1:9999/v1/traces", "from-env", "TRAILBEAM_TOKEN")
    monkeypatch.setenv("TRAILBEAM_OTLP_TRACES_ENDPOINT", "collector:4318/v1/traces")
    assert doctor.target_from_args(doctor_args("--url", "trailbeam:8080/")).ingest_url == "http://collector:4318/v1/traces"


# ---- which server, when --url is not given (QV-220) -----------------------------------------------------------

def _no_address_env(monkeypatch, tmp_path):
    for k in ("TRAILBEAM_URL", "TRAILBEAM_PORT", "TRAILBEAM_OTLP_TRACES_ENDPOINT", "TRAILBEAM_TOKEN",
              "TRAILBEAM_TOKEN_FILE"):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setenv("TRAILBEAM_DATA_DIR", str(tmp_path))


def test_the_server_comes_from_the_ingest_address_when_no_url_is_given(monkeypatch, tmp_path):
    _no_address_env(monkeypatch, tmp_path)
    t = doctor.target_from_args(doctor_args())
    assert (t.url, t.url_source) == ("http://127.0.0.1:4318", "")  # nothing known: the default
    monkeypatch.setenv("TRAILBEAM_OTLP_TRACES_ENDPOINT", "http://127.0.0.1:24318/v1/traces")
    t = doctor.target_from_args(doctor_args())
    assert (t.url, t.ingest_url, t.url_source) == (
        "http://127.0.0.1:24318", "http://127.0.0.1:24318/v1/traces", "TRAILBEAM_OTLP_TRACES_ENDPOINT")
    assert doctor.target_from_args(doctor_args("--ingest-url", "tb:9/v1/traces")).url == "http://tb:9"
    # Given addresses still win.
    assert doctor.target_from_args(doctor_args("--url", "http://x:1")).url == "http://x:1"
    monkeypatch.setenv("TRAILBEAM_PORT", "9999")
    assert doctor.target_from_args(doctor_args()).url == "http://127.0.0.1:9999"


def test_through_a_collector_the_server_is_the_one_trailbeam_start_wrote_down(app, monkeypatch, tmp_path):
    _no_address_env(monkeypatch, tmp_path)
    httpd = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _NotTrailbeam)  # stands in for a Collector
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    try:
        with serve(app) as base:
            (tmp_path / "address").write_text(base + "\n")
            monkeypatch.setenv("TRAILBEAM_OTLP_TRACES_ENDPOINT", f"http://127.0.0.1:{httpd.server_address[1]}/v1/traces")
            t = doctor.target_from_args(doctor_args())
            assert t.url == base and t.url_source.startswith(str(tmp_path / "address"))
            assert t.ingest_url.endswith(f":{httpd.server_address[1]}/v1/traces")
    finally:
        httpd.shutdown()
        httpd.server_close()


def test_a_server_found_without_url_that_fails_says_where_the_address_came_from(monkeypatch, tmp_path):
    _no_address_env(monkeypatch, tmp_path)
    monkeypatch.setenv("TRAILBEAM_OTLP_TRACES_ENDPOINT", f"http://127.0.0.1:{free_port()}/v1/traces")
    t = doctor.target_from_args(doctor_args())
    r = doctor.run(t, now_ns=NOW)[0]
    assert r.status == "FAIL" and "from TRAILBEAM_OTLP_TRACES_ENDPOINT" in r.fix and "--url" in r.fix
    out = io.StringIO()
    doctor.report(t, [r], out)
    assert f"server {t.url} (from TRAILBEAM_OTLP_TRACES_ENDPOINT)" in out.getvalue()


def test_a_token_not_accepted_names_the_address_checked(with_token):
    with serve(with_token) as base:
        r = by_name(doctor.run(target(base, "w" * 64), now_ns=NOW))["Token accepted"]
    assert r.status == "FAIL" and f"at {base}" in r.cause and base in r.fix
