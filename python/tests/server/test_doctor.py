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
    db.record_probe("old-probe-1", 1)
    db.record_probe("new-probe-1", db.PROBE_KEEP_NS + 2)
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


@pytest.mark.parametrize("error, said", [(StoreUnavailable("down"), "503"), (WriterBusy(), "503")])
def test_an_overloaded_or_disconnected_store(app, db, monkeypatch, error, said):
    def fail(*a, **kw):
        raise error
    monkeypatch.setattr(db, "record_probe", fail)
    with serve(app) as base:
        results = doctor.run(target(base), now_ns=NOW)
    r = by_name(results)["Test span stored and read back"]
    assert r.status == "FAIL" and said in r.detail and "/api/ready" in r.fix


def test_a_store_that_does_not_keep_probes(app, db, monkeypatch):
    def no(*a, **kw):
        raise NotImplementedError("this store does not keep probes")
    monkeypatch.setattr(db, "record_probe", no)
    with serve(app) as base:
        r = by_name(doctor.run(target(base), now_ns=NOW))["Test span stored and read back"]
    assert r.status == "FAIL" and "does not keep probes" in r.detail


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
