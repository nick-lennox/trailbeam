"""The shared access token (auth.py): off by default; when set, ingest, the API and the pages need it."""
import json
import os
import subprocess
import sys
import threading
import urllib.error
import urllib.parse
import urllib.request

import pytest

from trailbeam.server import auth
from trailbeam.server.db import Database
from trailbeam.server.server import make_server

from spans import T0, Consumer, to_proto

TOKEN = "s3cret-token-0123456789"


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *a, **k):
        return None


OPENER = urllib.request.build_opener(NoRedirect)


def call(url, path, *, method="GET", data=None, headers=None):
    req = urllib.request.Request(url + path, data=data, method=method, headers=headers or {})
    try:
        with OPENER.open(req, timeout=10) as r:
            return r.status, dict(r.headers), r.read()
    except urllib.error.HTTPError as e:
        return e.code, dict(e.headers), e.read()


def otlp():
    c = Consumer()
    return to_proto([c.send(T0, "m-auth-1"), c.receive(T0 + 1_000, "m-auth-1", "rh-a")]).SerializeToString()


@pytest.fixture
def serve():
    started = []

    def start(token=None):
        db = Database()
        httpd = make_server(db, "127.0.0.1", 0, ui_dir=None, auth=auth.Auth(token) if token else None)
        threading.Thread(target=httpd.serve_forever, daemon=True).start()
        started.append((httpd, db))
        return f"http://127.0.0.1:{httpd.server_address[1]}", db

    yield start
    for httpd, db in started:
        httpd.shutdown()
        httpd.server_close()
        db.close()


PROTO = {"Content-Type": "application/x-protobuf"}
BEARER = {"Authorization": f"Bearer {TOKEN}"}


def test_off_by_default_everything_is_open(serve):
    url, db = serve()
    assert call(url, "/v1/traces", method="POST", data=otlp(), headers=PROTO)[0] == 200
    assert call(url, "/api/messages")[0] == 200
    assert call(url, "/")[0] == 200
    assert json.loads(call(url, "/api/health")[2])["config"]["auth"] == "off"
    assert call(url, "/login")[0] == 404 and call(url, "/login", method="POST", data=b"")[0] == 404


def test_ingest_needs_the_bearer_token(serve):
    url, db = serve(TOKEN)
    for h in ({}, {"Authorization": "Bearer wrong-token-0123456789"}, {"Authorization": TOKEN},
              {"Authorization": "Basic " + TOKEN}):
        status, headers, _ = call(url, "/v1/traces", method="POST", data=otlp(), headers={**PROTO, **h})
        assert status == 401 and headers["WWW-Authenticate"].startswith("Bearer")
    assert db.message_rows("m-auth-1") == []
    assert call(url, "/v1/traces", method="POST", data=otlp(), headers={**PROTO, **BEARER})[0] == 200
    assert len(db.message_rows("m-auth-1")) == 2
    assert call(url, "/v1/traces", method="POST", data=otlp(),
                headers={**PROTO, "Authorization": f"bearer  {TOKEN}"})[0] == 200  # scheme is case-insensitive


def test_api_needs_the_token_and_health_says_only_that_it_is_up(serve):
    url, _ = serve(TOKEN)
    status, _, body = call(url, "/api/messages")
    assert status == 401 and "Bearer" in json.loads(body)["error"]
    assert call(url, "/api/messages/m-auth-1")[0] == 401
    assert call(url, "/api/messages", headers=BEARER)[0] == 200
    assert json.loads(call(url, "/api/health")[2]) == {"status": "ok", "version": json.loads(
        call(url, "/api/health", headers=BEARER)[2])["version"]}
    assert json.loads(call(url, "/api/health", headers=BEARER)[2])["config"]["auth"] == "token"
    status, _, body = call(url, "/api/ready")  # readiness probes carry no token either
    assert status == 200 and json.loads(body)["status"] == "ready"


def test_a_browser_signs_in_once_and_gets_a_session_cookie(serve):
    url, _ = serve(TOKEN)
    status, _, html = call(url, "/stuck?older_than_s=5")
    assert status == 401 and b'action="/login"' in html and b'value="/stuck?older_than_s=5"' in html
    form = urllib.parse.urlencode({"token": "wrong-token-0123456789", "next": "/stuck"}).encode()
    status, headers, html = call(url, "/login", method="POST", data=form)
    assert status == 401 and b"That is not the token" in html and "Set-Cookie" not in headers
    form = urllib.parse.urlencode({"token": TOKEN, "next": "/stuck?older_than_s=5"}).encode()
    status, headers, _ = call(url, "/login", method="POST", data=form)
    assert status == 303 and headers["Location"] == "/stuck?older_than_s=5"
    cookie = headers["Set-Cookie"]
    assert "HttpOnly" in cookie and "SameSite=Strict" in cookie and "Secure" not in cookie
    assert TOKEN not in cookie  # an HMAC of the token, not the token
    session = {"Cookie": cookie.split(";")[0]}
    assert call(url, "/stuck", headers=session)[0] == 200
    assert call(url, "/api/messages", headers=session)[0] == 200
    # the cookie is not an ingest credential
    assert call(url, "/v1/traces", method="POST", data=otlp(), headers={**PROTO, **session})[0] == 401
    status, headers, _ = call(url, "/logout", headers=session)
    assert status == 303 and "Max-Age=0" in headers["Set-Cookie"]
    assert call(url, "/", headers={"Cookie": "trailbeam_session=forged"})[0] == 401


def test_secure_cookie_behind_https_and_no_open_redirect(serve):
    url, _ = serve(TOKEN)
    form = urllib.parse.urlencode({"token": TOKEN, "next": "//evil.example/x"}).encode()
    status, headers, _ = call(url, "/login", method="POST", data=form, headers={"X-Forwarded-Proto": "https"})
    assert status == 303 and headers["Location"] == "/" and "Secure" in headers["Set-Cookie"]
    for bad in ("https://evil.example", "/\\evil.example", "javascript:alert(1)", ""):
        assert auth.safe_next(bad) == "/"
    assert auth.safe_next("/message?id=a%20b") == "/message?id=a%20b"


@pytest.mark.parametrize("bad", ["", "short", "has space in the token value", "tøken-with-non-ascii-chars"])
def test_unusable_tokens_are_refused(bad):
    with pytest.raises(auth.TokenError):
        auth.Auth(bad)


def _env(tmp_path, **extra):
    env = {k: v for k, v in os.environ.items() if not k.startswith("TRAILBEAM_")}
    return {**env, "TRAILBEAM_DB": str(tmp_path / "q.sqlite"), "TRAILBEAM_PORT": "0", **extra}


def test_the_server_refuses_to_start_with_a_bad_token(tmp_path):
    p = subprocess.run([sys.executable, "-m", "trailbeam.server"], env=_env(tmp_path, TRAILBEAM_TOKEN="short"),
                       capture_output=True, text=True, timeout=30)
    assert p.returncode == 2 and "at least 16 characters" in p.stderr


def test_the_server_reads_the_token_from_a_file(tmp_path):
    f = tmp_path / "token"
    f.write_text(TOKEN + "\n")
    p = subprocess.Popen([sys.executable, "-m", "trailbeam.server", "--token-file", str(f)], env=_env(tmp_path),
                         stderr=subprocess.PIPE, text=True)
    try:
        port = None
        while port is None:
            line = p.stderr.readline()
            assert line, "the server exited"
            if "listening on http://127.0.0.1:" in line:
                port = int(line.split("http://127.0.0.1:")[1].split()[0])
        url = f"http://127.0.0.1:{port}"
        assert call(url, "/api/messages")[0] == 401
        assert call(url, "/api/messages", headers=BEARER)[0] == 200
    finally:
        p.terminate()
        p.wait(timeout=10)
