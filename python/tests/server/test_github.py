"""Sign in with GitHub (github.py), end to end through the HTTP server, against a fake GitHub in this process (its OAuth
endpoints and the REST API under /api/v3, as GitHub Enterprise Server serves them), on SQLite and, when
TRAILBEAM_TEST_POSTGRES_URL is set, Postgres. Needs no extra: GitHub sign-in is the standard library only."""
import html
import http.client
import json
import os
import secrets
import sys
import threading
import time
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

from trailbeam.server import __main__ as main_mod, github, oidc
from trailbeam.server.db import Database
from trailbeam.server.server import make_server

ROOT = Path(__file__).resolve().parents[2]
PG = os.environ.get("TRAILBEAM_TEST_POSTGRES_URL")
NOW_MS = lambda: time.time_ns() // 1_000_000  # noqa: E731


class FakeGitHub:
    """GitHub's /login/oauth/access_token (which checks the client secret, the redirect URI and the PKCE verifier) and
    the API's /user, /user/emails, /user/memberships/orgs and /user/teams. ``approve`` stands for the person
    authorising the app at GitHub. Tests set ``users`` (by login) to change what the API says."""

    def __init__(self, client_id="Iv1.trailbeam", client_secret="gh-s3cret"):
        self.client_id, self.client_secret = client_id, client_secret
        self.codes: dict = {}
        self.tokens: dict = {}
        self.token_requests: list = []
        self.api_requests: list = []
        self.token_error = None  # e.g. "bad_verification_code": the token endpoint answers 200 with it
        self.page_size = 100
        self.next_host = None    # a Link rel="next" to another host
        self.users = {"alice": self.user(1001, "alice", "Alice", "alice@example.com")}
        self.lock = threading.Lock()
        gh = self

        class H(BaseHTTPRequestHandler):
            def log_message(self, *a):
                pass

            def _json(self, status, obj, link=None):
                b = json.dumps(obj).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(b)))
                if link:
                    self.send_header("Link", link)
                self.end_headers()
                self.wfile.write(b)

            def do_POST(self):
                n = int(self.headers.get("Content-Length") or 0)
                form = dict(urllib.parse.parse_qsl(self.rfile.read(n).decode()))
                if self.path != "/login/oauth/access_token":
                    return self._json(404, {})
                gh.token_requests.append({"form": form, "accept": self.headers.get("Accept")})
                self._json(200, gh.token(form))

            def do_GET(self):
                u = urllib.parse.urlsplit(self.path)
                tok = (self.headers.get("Authorization") or "").removeprefix("Bearer ")
                gh.api_requests.append((u.path, self.headers.get("User-Agent"), self.headers.get("Accept")))
                login = gh.tokens.get(tok)
                if login is None:
                    return self._json(401, {"message": "Bad credentials"})
                p = gh.users[login]
                q = dict(urllib.parse.parse_qsl(u.query))
                if u.path == "/api/v3/user":
                    return self._json(200, p["user"])
                rows = {"/api/v3/user/emails": p["emails"], "/api/v3/user/memberships/orgs": p["orgs"],
                        "/api/v3/user/teams": p["teams"]}.get(u.path)
                if rows is None:
                    return self._json(404, {"message": "Not Found"})
                page = int(q.get("page", "1"))
                chunk = rows[(page - 1) * gh.page_size:page * gh.page_size]
                link = None
                if page * gh.page_size < len(rows):
                    host = gh.next_host or f"127.0.0.1:{gh.port}"
                    nq = urllib.parse.urlencode({**q, "page": page + 1})
                    link = f'<http://{host}{u.path}?{nq}>; rel="next"'
                self._json(200, chunk, link)

        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), H)
        self.port = self.httpd.server_address[1]
        self.url = f"http://127.0.0.1:{self.port}"
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)
        self.thread.start()

    @staticmethod
    def user(uid, login, name, email, verified=True, orgs=(), teams=()):
        return {"user": {"id": uid, "login": login, "name": name, "email": None},
                "emails": [{"email": f"{login}@users.noreply.github.com", "primary": False, "verified": True},
                           {"email": email, "primary": True, "verified": verified}],
                "orgs": [{"state": "active", "organization": {"login": o}} for o in orgs],
                "teams": [{"slug": t.split("/")[1], "organization": {"login": t.split("/")[0]}} for t in teams]}

    def close(self):
        self.httpd.shutdown()
        self.httpd.server_close()

    def approve(self, authorize_url: str, login="alice") -> str:
        u = urllib.parse.urlsplit(authorize_url)
        assert u.path == "/login/oauth/authorize"
        q = dict(urllib.parse.parse_qsl(u.query))
        assert q["client_id"] == self.client_id
        code = secrets.token_urlsafe(16)
        with self.lock:
            self.codes[code] = {**q, "login": login}
        return q["redirect_uri"] + "?" + urllib.parse.urlencode({"code": code, "state": q["state"]})

    def token(self, form):
        if self.token_error:
            return {"error": self.token_error, "error_description": "no"}
        if (form.get("client_id"), form.get("client_secret")) != (self.client_id, self.client_secret):
            return {"error": "incorrect_client_credentials"}
        with self.lock:
            c = self.codes.pop(form.get("code"), None)
        if c is None or c["redirect_uri"] != form.get("redirect_uri"):
            return {"error": "bad_verification_code"}
        if oidc.pkce_challenge(form.get("code_verifier", "")) != c["code_challenge"]:
            return {"error": "bad_verification_code", "error_description": "PKCE"}
        tok = "gho_" + secrets.token_urlsafe(20)
        self.tokens[tok] = c["login"]
        return {"access_token": tok, "token_type": "bearer", "scope": c["scope"]}


# ---- the harness ----------------------------------------------------------------------------------------------


def _open(kind, tmp_path):
    if kind == "sqlite":
        return Database(str(tmp_path / "trailbeam.sqlite"))
    sys.path.insert(0, str(ROOT / "tests" / "conformance"))
    from pg_helpers import postgres_store
    return postgres_store()


@pytest.fixture(params=["sqlite", pytest.param("postgres", marks=pytest.mark.skipif(
    not PG, reason="TRAILBEAM_TEST_POSTGRES_URL is not set"))])
def store(request, tmp_path):
    s = _open(request.param, tmp_path)
    yield s
    s.close()


@pytest.fixture
def gh():
    g = FakeGitHub()
    yield g
    g.close()


class Response:
    def __init__(self, r):
        self.status, self.headers, self.body = r.status, r.headers, r.read()

    @property
    def text(self):
        return html.unescape(self.body.decode())  # the pages escape apostrophes

    def json(self):
        return json.loads(self.body)


class Browser:
    def __init__(self, port):
        self.port = port
        self.cookies: dict = {}

    def get(self, path, headers=None):
        c = http.client.HTTPConnection("127.0.0.1", self.port, timeout=10)
        h = dict(headers or {})
        if self.cookies:
            h["Cookie"] = "; ".join(f"{k}={v}" for k, v in self.cookies.items())
        c.request("GET", path, headers=h)
        r = Response(c.getresponse())
        c.close()
        for sc in r.headers.get_all("Set-Cookie") or []:
            name, _, value = sc.split(";")[0].partition("=")
            if "Max-Age=0" in sc:
                self.cookies.pop(name, None)
            else:
                self.cookies[name] = value
        return r


def _path(url):
    u = urllib.parse.urlsplit(url)
    return u.path + ("?" + u.query if u.query else "")


class Trailbeam:
    def __init__(self, db, gh, **settings):
        self.httpd = make_server(db, "127.0.0.1", 0)
        self.port = self.httpd.server_address[1]
        self.app = self.httpd.app
        self.gh = gh
        self.configure(**settings)
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)
        self.thread.start()

    def configure(self, **settings):
        kw = dict(client_id=self.gh.client_id, client_secret=self.gh.client_secret, github_url=self.gh.url,
                  public_url=f"http://127.0.0.1:{self.port}")
        if not {"allowed_orgs", "member_teams", "default_role"} & set(settings):
            kw["default_role"] = "member"  # any GitHub account, as most tests sign in with no org
        kw.update(settings)
        s = github.make_settings(**kw)
        self.app.github = github.GitHub(s, self.app.accounts, self.app.sessions)
        return s

    def browser(self):
        return Browser(self.port)

    def close(self):
        self.httpd.shutdown()
        self.httpd.server_close()

    def sign_in(self, browser, login="alice", next_path="/") -> Response:
        r = browser.get("/login/github?" + urllib.parse.urlencode({"next": next_path}))
        assert r.status == 302, r.text
        return browser.get(_path(self.gh.approve(r.headers["Location"], login)))


@pytest.fixture
def tb(store, gh):
    t = Trailbeam(store, gh)
    yield t
    t.close()


def _admin(tb, email="root@example.com"):
    uid = tb.app.accounts.create_first_admin(email, "Root", "scrypt$fake", NOW_MS())
    tb.app.access.state.changed()
    return uid


def _me(browser):
    r = browser.get("/api/me")
    return r.status, (r.json() if r.status == 200 else None)


# ---- the flow -------------------------------------------------------------------------------------------------


def test_the_sign_in_page_offers_github(tb):
    _admin(tb)
    r = tb.browser().get("/login?next=/stuck")
    assert r.status == 200
    assert 'href="/login/github?next=%2Fstuck"' in r.text and "Sign in with GitHub" in r.text
    assert 'name="password"' in r.text and "/login/oidc" not in r.text


def test_github_and_single_sign_on_side_by_side(tb):
    _admin(tb)
    s = oidc.make_settings(issuer="https://id.example.com", client_id="tb", public_url=f"http://127.0.0.1:{tb.port}",
                           label="Okta")
    tb.app.oidc = oidc.Oidc(s, tb.app.accounts, tb.app.sessions, provider=object())
    text = tb.browser().get("/login").text
    assert text.index("Sign in with Okta") < text.index("Sign in with GitHub")
    assert 'href="/login/oidc?next=%2F"' in text and 'href="/login/github?next=%2F"' in text


def test_a_member_signs_in_and_lands_where_they_were_going(tb, gh):
    _admin(tb)
    b = tb.browser()
    r = b.get("/login/github?next=/tenants")
    assert r.status == 302
    loc = urllib.parse.urlsplit(r.headers["Location"])
    assert f"{loc.scheme}://{loc.netloc}{loc.path}" == gh.url + "/login/oauth/authorize"
    q = dict(urllib.parse.parse_qsl(loc.query))
    assert q["redirect_uri"] == f"http://127.0.0.1:{tb.port}/login/github/callback"
    assert q["scope"] == "read:user user:email"  # no read:org without orgs or teams
    assert q["code_challenge_method"] == "S256" and len(q["code_challenge"]) == 43 and q["state"]
    assert q["allow_signup"] == "false"
    assert r.headers["Referrer-Policy"] == "no-referrer" and r.headers["Cache-Control"] == "no-store"
    cookie = [c for c in r.headers.get_all("Set-Cookie") if c.startswith("trailbeam_github=")][0]
    assert "HttpOnly" in cookie and "SameSite=Lax" in cookie and "Path=/login/github" in cookie
    r = b.get(_path(gh.approve(r.headers["Location"])))
    assert r.status == 200 and 'http-equiv="refresh" content="0;url=/tenants"' in r.text
    assert "trailbeam_github" not in b.cookies  # cleared
    status, me = _me(b)
    assert status == 200 and me["via"] == "session"
    assert me["user"]["email"] == "alice@example.com" and me["user"]["role"] == "member"
    assert me["user"]["name"] == "Alice"
    user = tb.app.accounts.user_by_email("alice@example.com")
    assert user["password_hash"] is None
    assert [(i["issuer"], i["subject"]) for i in tb.app.accounts.oidc_identities(user["id"])] == [(gh.url, "1001")]
    form = gh.token_requests[-1]["form"]
    assert form["client_secret"] == gh.client_secret and form["code_verifier"]
    assert gh.token_requests[-1]["accept"] == "application/json"
    paths = [p for p, _, _ in gh.api_requests]
    assert paths == ["/api/v3/user", "/api/v3/user/emails"]  # no org or team calls when none are set
    assert all(ua.startswith("trailbeam/") and accept == "application/vnd.github+json"
               for _, ua, accept in gh.api_requests)
    # the people list marks them as signing in through a provider (removing them doesn't keep them out)
    assert tb.app.accounts.oidc_identities()


def test_me_lists_the_github_account_as_a_way_in(tb, gh):
    _admin(tb)
    b = tb.browser()
    tb.sign_in(b)
    sign_in = _me(b)[1]["sign_in"]
    assert sign_in["password"] is False
    [way] = sign_in["identities"]
    assert way["provider"] == "github" and way["label"] == "GitHub" and way["on"] is True
    assert way["email"] == "alice@example.com" and way["last_sign_in_ms"] > 0
    # GitHub sign-in turned off: the account is still theirs, but no longer a way in
    tb.app.github = None
    [way] = _me(b)[1]["sign_in"]["identities"]
    assert way["on"] is False and way["label"] is None


def test_the_account_is_the_github_id_not_the_login_or_email(tb, gh):
    _admin(tb)
    tb.sign_in(tb.browser())
    gh.users["alice"] = gh.user(1001, "alice-renamed", "Alice B", "alice.new@example.com")
    assert tb.sign_in(tb.browser()).status == 200
    users = [u for u in tb.app.accounts.users() if u["role"] != "admin"]
    assert len(users) == 1
    gh.users["bob"] = gh.user(1002, "bob", "", "bob@example.com")  # no name: the login
    b = tb.browser()
    assert tb.sign_in(b, "bob").status == 200
    assert _me(b)[1]["user"]["name"] == "bob"


def test_next_is_never_another_site(tb):
    _admin(tb)
    r = tb.sign_in(tb.browser(), next_path="//evil.example.com/x")
    assert r.status == 200 and "evil.example.com" not in r.text


def test_signing_out_ends_the_session(tb):
    _admin(tb)
    b = tb.browser()
    tb.sign_in(b)
    assert _me(b)[0] == 200
    b.get("/logout")
    assert _me(b)[0] == 401


def test_a_session_lasts_twelve_hours_or_what_the_setting_says(tb):
    _admin(tb)
    b = tb.browser()
    before = NOW_MS()
    tb.sign_in(b)
    rows = tb.app.accounts._read("SELECT expires_at_ms FROM sessions WHERE user_id = ?",
                                 (tb.app.accounts.user_by_email("alice@example.com")["id"],))
    assert abs(rows[0]["expires_at_ms"] - before - 12 * 3_600_000) < 60_000
    assert tb.configure(session_hours="2").session_ms == 2 * 3_600_000


# ---- the state, the browser and the code ------------------------------------------------------------------------


def test_a_state_works_once(tb, gh):
    _admin(tb)
    b = tb.browser()
    r = b.get("/login/github")
    cb = _path(gh.approve(r.headers["Location"]))
    keep = dict(b.cookies)
    assert b.get(cb).status == 200
    b2 = tb.browser()
    b2.cookies = keep
    r = b2.get(cb)
    assert r.status == 400 and "has been used or took too long" in r.text


def test_a_callback_from_another_browser_is_refused(tb, gh):
    """Login CSRF: an attacker starts a sign-in with their own account and sends the victim the callback URL."""
    _admin(tb)
    r = tb.browser().get("/login/github")
    cb = _path(gh.approve(r.headers["Location"]))
    victim = tb.browser()
    r = victim.get(cb)
    assert r.status == 400 and "started in another browser" in r.text
    assert "trailbeam_sid" not in victim.cookies
    assert gh.token_requests == []  # refused before the code is used


def test_an_openid_connect_sign_in_s_state_is_not_a_github_one(tb, gh):
    _admin(tb)
    b = tb.browser()
    b.cookies["trailbeam_github"] = "b" * 43
    now = NOW_MS()
    tb.app.accounts.create_oidc_login(github._sha256("oidc-state"), github._sha256("b" * 43), "an-oidc-nonce", "v",
                                      "/", now, now + 60_000)
    r = b.get("/login/github/callback?code=x&state=oidc-state")
    assert r.status == 400 and "has been used or took too long" in r.text


def test_a_sign_in_runs_out_after_ten_minutes(tb, gh):
    _admin(tb)
    b = tb.browser()
    r = b.get("/login/github")
    cb = _path(gh.approve(r.headers["Location"]))
    real = tb.app.github.now_ms
    tb.app.github.now_ms = lambda: real() + oidc.LOGIN_TTL_MS + 1
    r = b.get(cb)
    assert r.status == 400 and "took too long" in r.text


def test_an_unknown_state_and_a_missing_code(tb):
    _admin(tb)
    b = tb.browser()
    assert b.get("/login/github/callback?code=x&state=made-up").status == 400
    assert b.get("/login/github/callback?state=made-up").status == 400
    assert b.get("/login/github/callback").status == 400


def test_the_person_cancels_at_github(tb):
    _admin(tb)
    r = tb.browser().get("/login/github/callback?error=access_denied&state=s&error_description=<b>no</b>")
    assert r.status == 403 and "cancelled the sign-in at GitHub" in r.text and b"<b>" not in r.body


@pytest.mark.parametrize("error", ["bad_verification_code", "incorrect_client_credentials"])
def test_github_refuses_the_code(tb, gh, error):
    _admin(tb)
    gh.token_error = error
    r = tb.sign_in(tb.browser())
    assert r.status == 400 and "GitHub didn't accept this sign-in" in r.text
    assert tb.app.accounts.user_by_email("alice@example.com") is None


def test_a_wrong_client_secret(tb, gh):
    _admin(tb)
    tb.configure(client_secret="not-it")
    r = tb.sign_in(tb.browser())
    assert r.status == 400 and "GitHub didn't accept" in r.text


def test_github_is_down(store, gh):
    t = Trailbeam(store, gh)
    try:
        _admin(t)
        b = t.browser()
        r = b.get("/login/github")
        cb = _path(gh.approve(r.headers["Location"]))
        gh.close()
        r = b.get(cb)
        assert r.status == 502 and "GitHub can't be reached" in r.text
    finally:
        t.close()


def test_an_api_error_is_a_refusal(tb, gh):
    _admin(tb)
    gh.tokens.clear()  # the API answers 401 for every token
    orig = gh.token

    def token(form):
        d = orig(form)
        gh.tokens.clear()
        return d
    gh.token = token
    r = tb.sign_in(tb.browser())
    assert r.status == 502 and "GitHub refused the sign-in" in r.text


# ---- the email ----------------------------------------------------------------------------------------------------


def test_an_unverified_primary_email_is_refused(tb, gh):
    _admin(tb)
    gh.users["alice"] = gh.user(1001, "alice", "Alice", "alice@example.com", verified=False)
    r = tb.sign_in(tb.browser())
    assert r.status == 403 and "isn't verified at GitHub" in r.text
    assert tb.app.accounts.user_by_email("alice@example.com") is None


def test_no_primary_email_is_refused(tb, gh):
    _admin(tb)
    gh.users["alice"]["emails"] = [{"email": "x@example.com", "primary": False, "verified": True}]
    r = tb.sign_in(tb.browser())
    assert r.status == 403 and "didn't send your email" in r.text


def test_an_email_with_a_local_account_is_not_taken_over(tb, gh):
    _admin(tb, "alice@example.com")
    r = tb.sign_in(tb.browser())
    assert r.status == 403 and "already has an account" in r.text


def test_linking_by_email_when_turned_on(tb, gh):
    uid = _admin(tb, "alice@example.com")
    tb.configure(link_email=True)
    b = tb.browser()
    assert tb.sign_in(b).status == 200
    assert _me(b)[1]["user"]["id"] == uid and _me(b)[1]["user"]["role"] == "admin"


def test_a_disabled_person_is_refused(tb):
    _admin(tb)
    tb.sign_in(tb.browser())
    uid = tb.app.accounts.user_by_email("alice@example.com")["id"]
    tb.app.accounts.set_disabled(uid, True, NOW_MS())
    r = tb.sign_in(tb.browser())
    assert r.status == 403 and "disabled" in r.text


# ---- organisations and teams ------------------------------------------------------------------------------------


def test_allowed_orgs(tb, gh):
    _admin(tb)
    tb.configure(allowed_orgs="Acme, other")
    b = tb.browser()
    r = b.get("/login/github")
    assert dict(urllib.parse.parse_qsl(urllib.parse.urlsplit(r.headers["Location"]).query))["scope"] == \
        "read:user user:email read:org"
    r = b.get(_path(gh.approve(r.headers["Location"])))
    assert r.status == 403 and "isn't in an organisation this server lets in" in r.text
    gh.users["alice"] = gh.user(1001, "alice", "Alice", "alice@example.com", orgs=["acme"])
    assert tb.sign_in(tb.browser()).status == 200
    gh.users["alice"]["orgs"][0]["state"] = "pending"  # invited, not yet a member
    assert tb.sign_in(tb.browser()).status == 403


def test_teams_make_admins_and_members_and_follow_github(tb, gh):
    _admin(tb)
    tb.configure(admin_teams="acme/ops", member_teams="Acme/Engineering")
    gh.users["alice"] = gh.user(1001, "alice", "Alice", "alice@example.com", teams=["acme/engineering"])
    b = tb.browser()
    assert tb.sign_in(b).status == 200
    assert _me(b)[1]["user"]["role"] == "member"
    gh.users["alice"]["teams"].append({"slug": "ops", "organization": {"login": "ACME"}})
    b = tb.browser()
    assert tb.sign_in(b).status == 200
    assert _me(b)[1]["user"]["role"] == "admin"
    gh.users["alice"]["teams"] = []
    r = tb.sign_in(tb.browser())  # member teams set: the default role is none
    assert r.status == 403 and "teams give you no access" in r.text


def test_the_last_admin_stays_one_when_their_team_goes(store, gh):
    t = Trailbeam(store, gh, admin_teams="acme/ops")
    try:
        gh.users["alice"] = gh.user(1001, "alice", "Alice", "alice@example.com", teams=["acme/ops"])
        assert t.sign_in(t.browser()).status == 200
        gh.users["alice"]["teams"] = []
        b = t.browser()
        assert t.sign_in(b).status == 200
        assert _me(b)[1]["user"]["role"] == "admin"
    finally:
        t.close()


def test_teams_over_several_pages(tb, gh):
    _admin(tb)
    gh.page_size = 2
    tb.configure(admin_teams="acme/ops")
    gh.users["alice"] = gh.user(1001, "alice", "Alice", "alice@example.com",
                                teams=["acme/a", "acme/b", "acme/c", "acme/ops"])
    b = tb.browser()
    assert tb.sign_in(b).status == 200
    assert _me(b)[1]["user"]["role"] == "admin"
    assert [p for p, _, _ in gh.api_requests].count("/api/v3/user/teams") == 2


def test_a_next_page_on_another_host_is_not_followed(tb, gh):
    """The access token goes to GitHub's API only."""
    _admin(tb)
    gh.page_size, gh.next_host = 2, "evil.example.com:1"
    tb.configure(admin_teams="acme/ops", default_role="member")
    gh.users["alice"] = gh.user(1001, "alice", "Alice", "alice@example.com", teams=["acme/a", "acme/b", "acme/ops"])
    b = tb.browser()
    assert tb.sign_in(b).status == 200
    assert _me(b)[1]["user"]["role"] == "member"  # the third team was on the page not followed


def test_with_no_admin_only_an_admin_team_gets_in_and_becomes_the_first_admin(store, gh):
    t = Trailbeam(store, gh, admin_teams="acme/ops")
    try:
        r = t.browser().get("/login")
        assert r.status == 200 and "Sign in with GitHub" in r.text and 'name="password"' not in r.text
        r = t.sign_in(t.browser())
        assert r.status == 403 and "no admin yet" in r.text
        assert t.app.accounts.users() == []
        gh.users["root"] = gh.user(7, "root", "Root", "root@example.com", teams=["acme/ops"])
        b = t.browser()
        assert t.sign_in(b, "root").status == 200
        status, me = _me(b)
        assert status == 200 and me["auth"] == "accounts" and me["user"]["role"] == "admin"
    finally:
        t.close()


def test_github_sign_in_off_is_not_found(store):
    httpd = make_server(store, "127.0.0.1", 0)
    th = threading.Thread(target=httpd.serve_forever, daemon=True)
    th.start()
    try:
        b = Browser(httpd.server_address[1])
        assert b.get("/login/github").status == 404
        assert b.get("/login/github/callback?code=a&state=b").status == 404
        assert "github" not in b.get("/login").text.lower()
    finally:
        httpd.shutdown()
        httpd.server_close()


def test_https_cookies(tb, gh):
    _admin(tb)
    from trailbeam.server.proxy import TrustedProxies, parse_networks

    tb.app.proxies = TrustedProxies(parse_networks("127.0.0.1"))
    b = tb.browser()
    r = b.get("/login/github", {"X-Forwarded-Proto": "https"})
    cookie = [c for c in r.headers.get_all("Set-Cookie") if "trailbeam_github" in c][0]
    assert cookie.startswith("__Secure-trailbeam_github=") and "; Secure" in cookie
    cb = _path(gh.approve(r.headers["Location"]))
    plain = tb.browser()  # a plain-named cookie, as a sibling subdomain could plant, doesn't count over HTTPS
    plain.cookies["trailbeam_github"] = b.cookies["__Secure-trailbeam_github"]
    assert plain.get(cb, {"X-Forwarded-Proto": "https"}).status == 400  # and that used up the state
    r = b.get("/login/github", {"X-Forwarded-Proto": "https"})
    r = b.get(_path(gh.approve(r.headers["Location"])), {"X-Forwarded-Proto": "https"})
    assert r.status == 200 and any(c.startswith("__Host-trailbeam_sid=") for c in r.headers.get_all("Set-Cookie"))


def test_starting_sign_ins_is_limited_per_address(tb):
    _admin(tb)
    b = tb.browser()
    codes = [b.get("/login/github").status for _ in range(oidc.STARTS_PER_ADDRESS + 1)]
    assert codes[-1] == 429 and codes[:-1] == [302] * oidc.STARTS_PER_ADDRESS


# ---- settings -----------------------------------------------------------------------------------------------------


def test_settings():
    assert github.make_settings(client_id=None) is None
    s = github.make_settings(client_id=" Iv1.x ", client_secret="s", public_url="https://tb.example.com/",
                             default_role="member")
    assert (s.client_id, s.public_url, s.github_url, s.api_url) == (
        "Iv1.x", "https://tb.example.com", "https://github.com", "https://api.github.com")
    assert s.redirect_uri == "https://tb.example.com/login/github/callback"
    assert s.default_role == "member" and not s.maps_teams and s.scopes == ("read:user", "user:email")
    e = github.make_settings(client_id="x", client_secret="s", public_url="https://tb.example.com",
                             github_url="https://github.example.com/", member_teams="acme/eng")
    assert e.github_url == "https://github.example.com" and e.api_url == "https://github.example.com/api/v3"
    assert e.default_role is None and e.member_teams == ("acme/eng",) and "read:org" in e.scopes
    o = github.make_settings(client_id="x", client_secret="s", public_url="https://tb.example.com", allowed_orgs="Acme")
    assert o.allowed_orgs == ("acme",) and o.default_role == "member"
    bad = [dict(client_id=None, allowed_orgs="acme"),
           dict(client_id="x", public_url="https://tb.example.com"),
           dict(client_id="x", client_secret="s"),
           dict(client_id="x", client_secret="s", public_url="http://tb.example.com"),
           dict(client_id="x", client_secret="s", public_url="https://tb.example.com/app"),
           dict(client_id="x", client_secret="s", public_url="https://tb.example.com", github_url="http://gh.example"),
           dict(client_id="x", client_secret="s", public_url="https://tb.example.com", admin_teams="ops"),
           dict(client_id="x", client_secret="s", public_url="https://tb.example.com", admin_teams="a/b/c"),
           dict(client_id="x", client_secret="s", public_url="https://tb.example.com", default_role="admin"),
           dict(client_id="x", client_secret="s", public_url="https://tb.example.com", session_hours="0")]
    for kw in bad[1:]:
        kw.setdefault("default_role", "member")
    for kw in bad:
        with pytest.raises(oidc.SettingsError):
            github.make_settings(**kw)
    with pytest.raises(oidc.SettingsError, match="--github-session-hours"):
        github.make_settings(client_id="x", client_secret="s", public_url="https://tb.example.com", session_hours="x",
                             default_role="member")


@pytest.mark.parametrize("extra", [{}, {"admin_teams": "acme/ops"}])
def test_open_to_every_github_account_only_when_asked(extra):
    """Anyone can make a GitHub account: without allowed orgs or member teams, the default role must be said."""
    base = dict(client_id="x", client_secret="s", public_url="https://tb.example.com", **extra)
    with pytest.raises(oidc.SettingsError, match="any GitHub account"):
        github.make_settings(**base)
    assert github.make_settings(**base, default_role="member").default_role == "member"
    assert github.make_settings(**base, default_role="none").default_role is None


def _clean_env(monkeypatch):
    for k in list(os.environ):
        if k.startswith(("TRAILBEAM_GITHUB", "TRAILBEAM_OIDC")) or k in ("TRAILBEAM_PUBLIC_URL", "TRAILBEAM_ACCOUNTS"):
            monkeypatch.delenv(k)


def test_flags_and_environment(tmp_path, monkeypatch):
    _clean_env(monkeypatch)
    secret = tmp_path / "secret"
    secret.write_text("abc\n")
    monkeypatch.setenv("TRAILBEAM_GITHUB_CLIENT_ID", "Iv1.tb")
    monkeypatch.setenv("TRAILBEAM_PUBLIC_URL", "https://tb.example.com")
    monkeypatch.setenv("TRAILBEAM_GITHUB_ADMIN_TEAMS", "acme/ops, acme/sre")
    monkeypatch.setenv("TRAILBEAM_GITHUB_LINK_EMAIL", "true")
    s = main_mod.github_settings(main_mod.build_parser().parse_args(
        ["--github-client-secret-file", str(secret), "--github-allowed-orgs", "acme"]))
    assert (s.client_id, s.client_secret, s.admin_teams, s.allowed_orgs, s.link_email) == (
        "Iv1.tb", "abc", ("acme/ops", "acme/sre"), ("acme",), True)
    monkeypatch.setenv("TRAILBEAM_GITHUB_DEFAULT_ROLE", "member")
    monkeypatch.setenv("TRAILBEAM_GITHUB_CLIENT_SECRET", "from-env")
    assert main_mod.github_settings(main_mod.build_parser().parse_args([])).client_secret == "from-env"
    monkeypatch.setenv("TRAILBEAM_GITHUB_SESSION_HOURS", "2")
    assert main_mod.github_settings(main_mod.build_parser().parse_args([])).session_hours == 2
    _clean_env(monkeypatch)
    assert main_mod.github_settings(main_mod.build_parser().parse_args([])) is None


def test_the_server_refuses_to_start_with_half_the_settings(tmp_path, monkeypatch, caplog):
    _clean_env(monkeypatch)
    a = main_mod.build_parser().parse_args(["--db", str(tmp_path / "x.sqlite"), "--accounts",
                                            "--github-client-id", "Iv1.tb", "--public-url", "https://tb.example.com"])
    with pytest.raises(SystemExit) as e:
        main_mod.serve(a)
    assert e.value.code == 2 and "client secret" in caplog.text


def test_github_sign_in_needs_users_and_roles_switched_on(tmp_path, monkeypatch, caplog):
    _clean_env(monkeypatch)
    monkeypatch.setenv("TRAILBEAM_GITHUB_CLIENT_SECRET", "s")
    a = main_mod.build_parser().parse_args(["--db", str(tmp_path / "x.sqlite"), "--github-client-id", "Iv1.tb",
                                            "--public-url", "https://tb.example.com", "--github-allowed-orgs", "a"])
    with pytest.raises(SystemExit) as e:
        main_mod.serve(a)
    assert e.value.code == 2
    assert "--accounts" in caplog.text and "GitHub sign-in" in caplog.text
    assert not (tmp_path / "x.sqlite").exists()
