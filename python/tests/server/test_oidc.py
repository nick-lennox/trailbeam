"""Single sign-on with OpenID Connect (oidc.py), end to end through the HTTP server, against an identity provider in
this process (oidc_fake.py), on SQLite and, when TRAILBEAM_TEST_POSTGRES_URL is set, Postgres. Most tests try to
break it: a state used twice or from another browser, ID tokens with the wrong issuer, audience, nonce, signature or
algorithm, groups that leave, a groups overage, domains, and an email that already has an account.

tests/server/test_oidc_keycloak.py runs the same flow against a real Keycloak in CI."""
import http.client
import json
import os
import sys
import threading
import time
import urllib.parse
from pathlib import Path

import pytest

pytest.importorskip("jwt")

from oidc_fake import FakeIdP  # noqa: E402

from trailbeam.server import __main__ as main_mod, oidc  # noqa: E402
from trailbeam.server.db import Database  # noqa: E402
from trailbeam.server.server import make_server  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
PG = os.environ.get("TRAILBEAM_TEST_POSTGRES_URL")
NOW_MS = lambda: time.time_ns() // 1_000_000  # noqa: E731


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
def idp():
    p = FakeIdP()
    yield p
    p.close()


class Response:
    def __init__(self, r):
        self.status = r.status
        self.headers = r.headers
        self.body = r.read()

    @property
    def text(self):
        return self.body.decode()

    def json(self):
        return json.loads(self.body)


class Browser:
    """Cookies by name, as a browser keeps them for one site (paths and SameSite aside)."""

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
            first = sc.split(";")[0]
            name, _, value = first.partition("=")
            if "Max-Age=0" in sc:
                self.cookies.pop(name, None)
            else:
                self.cookies[name] = value
        return r


class Trailbeam:
    def __init__(self, db, idp, **settings):
        self.httpd = make_server(db, "127.0.0.1", 0)
        self.port = self.httpd.server_address[1]
        self.app = self.httpd.app
        self.idp = idp
        self.configure(**settings)
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)
        self.thread.start()

    def configure(self, **settings):
        kw = dict(issuer=self.idp.issuer, client_id=self.idp.client_id, client_secret=self.idp.client_secret,
                  public_url=f"http://127.0.0.1:{self.port}")
        kw.update(settings)
        s = oidc.make_settings(**kw)
        self.app.oidc = oidc.Oidc(s, self.app.accounts, self.app.sessions)
        self.app.sso_label = s.label
        return s

    def browser(self):
        return Browser(self.port)

    def close(self):
        self.httpd.shutdown()
        self.httpd.server_close()

    def sign_in(self, browser, sub="alice-sub", next_path="/", **claims) -> Response:
        r = browser.get("/login/oidc?" + urllib.parse.urlencode({"next": next_path}))
        assert r.status == 302, r.text
        cb = self.idp.approve(r.headers["Location"], sub, **claims)
        return browser.get(_path(cb))


def _path(url):
    u = urllib.parse.urlsplit(url)
    return u.path + ("?" + u.query if u.query else "")


@pytest.fixture
def tb(store, idp):
    t = Trailbeam(store, idp)
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


def test_sign_in_page_offers_single_sign_on(tb):
    _admin(tb)
    tb.configure(label="Keycloak")
    r = tb.browser().get("/login?next=/stuck")
    assert r.status == 200
    assert 'href="/login/oidc?next=%2Fstuck"' in r.text and "Sign in with Keycloak" in r.text
    assert 'name="password"' in r.text  # the password form stays


def test_a_member_signs_in_and_lands_where_they_were_going(tb, idp):
    _admin(tb)
    b = tb.browser()
    r = b.get("/login/oidc?next=/tenants")
    assert r.status == 302
    loc = urllib.parse.urlsplit(r.headers["Location"])
    q = dict(urllib.parse.parse_qsl(loc.query))
    assert q["redirect_uri"] == f"http://127.0.0.1:{tb.port}/login/oidc/callback"
    assert q["code_challenge_method"] == "S256" and len(q["code_challenge"]) == 43
    assert q["scope"] == "openid email profile" and q["state"] and q["nonce"]
    assert r.headers["Referrer-Policy"] == "no-referrer"
    cookie = [c for c in r.headers.get_all("Set-Cookie") if c.startswith("trailbeam_oidc=")][0]
    assert "HttpOnly" in cookie and "SameSite=Lax" in cookie and "Path=/login/oidc" in cookie
    r = b.get(_path(idp.approve(r.headers["Location"])))
    # a page that moves on by itself, not a redirect: SameSite=Strict cookies are held back after one
    assert r.status == 200 and 'http-equiv="refresh" content="0;url=/tenants"' in r.text
    assert r.headers["Cache-Control"] == "no-store" and r.headers["Referrer-Policy"] == "no-referrer"
    sid = [c for c in r.headers.get_all("Set-Cookie") if c.startswith("trailbeam_sid=")][0]
    assert "SameSite=Strict" in sid and "HttpOnly" in sid
    assert "trailbeam_oidc" not in b.cookies  # cleared
    status, me = _me(b)
    assert status == 200 and me["via"] == "session"
    assert me["user"]["email"] == "alice@example.com" and me["user"]["role"] == "member"
    assert me["user"]["name"] == "Alice"
    user = tb.app.accounts.user_by_email("alice@example.com")
    assert user["password_hash"] is None  # only single sign-on lets them in
    ids = tb.app.accounts.oidc_identities(user["id"])
    assert [(i["issuer"], i["subject"]) for i in ids] == [(idp.issuer, "alice-sub")]
    # the token endpoint got the verifier and the client secret (client_secret_basic)
    req = idp.token_requests[-1]
    assert req["authorization"].startswith("Basic ") and "client_secret" not in req["form"]


def test_the_second_sign_in_is_the_same_person(tb):
    _admin(tb)
    tb.sign_in(tb.browser())
    r = tb.sign_in(tb.browser(), email="alice.new@example.com")  # the email changed at the provider
    assert r.status == 200
    users = [u for u in tb.app.accounts.users() if u["role"] != "admin"]
    assert len(users) == 1  # the account is iss + sub, not the email
    assert tb.app.accounts.oidc_identities(users[0]["id"])[0]["email"] == "alice.new@example.com"


def test_next_is_never_another_site(tb):
    _admin(tb)
    r = tb.sign_in(tb.browser(), next_path="//evil.example/x")
    assert r.status == 200 and "url=/\"" in r.text


def test_a_public_client_sends_its_id_and_no_secret(store):
    p = FakeIdP(client_secret=None)
    t = Trailbeam(store, p)
    try:
        _admin(t)
        assert t.sign_in(t.browser()).status == 200
        req = p.token_requests[-1]
        assert req["authorization"] is None and req["form"]["client_id"] == "trailbeam"
        assert "client_secret" not in req["form"]
    finally:
        t.close()
        p.close()


def test_client_secret_post_when_the_provider_takes_only_that(tb, idp):
    _admin(tb)
    idp.auth_methods = ["client_secret_post"]
    assert tb.sign_in(tb.browser()).status == 200
    req = idp.token_requests[-1]
    assert req["authorization"] is None and req["form"]["client_secret"] == idp.client_secret


def test_userinfo_fills_in_an_email_the_id_token_lacks(tb, idp):
    _admin(tb)
    idp.claims = {"email": None, "email_verified": None}
    idp.userinfo_claims = {"email": "from-userinfo@example.com", "email_verified": True}
    assert tb.sign_in(tb.browser()).status == 200
    assert tb.app.accounts.user_by_email("from-userinfo@example.com") is not None


def test_no_email_anywhere_is_refused(tb, idp):
    _admin(tb)
    idp.claims = {"email": None}
    idp.with_userinfo = False
    r = tb.sign_in(tb.browser())
    assert r.status == 403 and "didn&#x27;t send an email" in r.text


def test_signing_out_ends_the_session(tb):
    _admin(tb)
    b = tb.browser()
    tb.sign_in(b)
    assert _me(b)[0] == 200
    b.get("/logout")
    assert _me(b)[0] == 401


# ---- the state, the browser and the code ------------------------------------------------------------------------


def test_a_state_works_once(tb, idp):
    _admin(tb)
    b = tb.browser()
    r = b.get("/login/oidc")
    cb = _path(idp.approve(r.headers["Location"]))
    keep = dict(b.cookies)
    assert b.get(cb).status == 200
    b2 = tb.browser()
    b2.cookies = keep
    r = b2.get(cb)
    assert r.status == 400 and "has been used or took too long" in r.text


def test_a_callback_from_another_browser_is_refused(tb, idp):
    """Login CSRF: an attacker starts a sign-in with their own account and sends the victim the callback URL."""
    _admin(tb)
    attacker = tb.browser()
    r = attacker.get("/login/oidc")
    cb = _path(idp.approve(r.headers["Location"], sub="mallory"))
    victim = tb.browser()
    r = victim.get(cb)
    assert r.status == 400 and "started in another browser" in r.text
    assert "trailbeam_sid" not in victim.cookies
    victim.cookies["trailbeam_oidc"] = "x" * 43  # a cookie of its own, not the one of this sign-in
    assert victim.get(cb).status == 400


def test_a_sign_in_runs_out_after_ten_minutes(tb, idp):
    _admin(tb)
    b = tb.browser()
    r = b.get("/login/oidc")
    cb = _path(idp.approve(r.headers["Location"]))
    real = tb.app.oidc.now_ms
    tb.app.oidc.now_ms = lambda: real() + oidc.LOGIN_TTL_MS + 1
    r = b.get(cb)
    assert r.status == 400 and "took too long" in r.text


def test_an_unknown_state_and_a_missing_code(tb):
    _admin(tb)
    b = tb.browser()
    assert b.get("/login/oidc/callback?code=x&state=made-up").status == 400
    assert b.get("/login/oidc/callback?state=made-up").status == 400
    assert b.get("/login/oidc/callback").status == 400


def test_the_provider_says_no(tb):
    _admin(tb)
    r = tb.browser().get("/login/oidc/callback?error=access_denied&state=s&error_description=<b>no</b>")
    assert r.status == 403 and "cancelled the sign-in" in r.text and "<b>" not in r.text


def test_a_code_the_provider_refuses(tb, idp):
    _admin(tb)
    b = tb.browser()
    r = b.get("/login/oidc")
    q = dict(urllib.parse.parse_qsl(urllib.parse.urlsplit(r.headers["Location"]).query))
    r = b.get("/login/oidc/callback?" + urllib.parse.urlencode({"code": "never-issued", "state": q["state"]}))
    assert r.status == 502 and "refused the sign-in" in r.text


def test_a_wrong_client_secret(store, idp):
    t = Trailbeam(store, idp, client_secret="not-the-secret")
    try:
        _admin(t)
        assert t.sign_in(t.browser()).status == 502
    finally:
        t.close()


# ---- the ID token's checks ------------------------------------------------------------------------------------


@pytest.mark.parametrize("claims", [
    {"iss": "http://127.0.0.1:1/realms/other"},
    {"aud": "someone-else"},
    {"aud": ["trailbeam", "someone-else"]},  # several audiences and no azp
    {"aud": ["trailbeam", "someone-else"], "azp": "someone-else"},
    {"nonce": "not-this-sign-ins"},
    {"nonce": None},
    {"exp": int(time.time()) - 3600},
    {"iat": None},
    {"sub": None},
])
def test_id_tokens_that_dont_check_out(tb, idp, claims):
    _admin(tb)
    idp.claims = claims
    b = tb.browser()
    r = tb.sign_in(b)
    assert r.status in (400, 502), r.text
    assert "trailbeam_sid" not in b.cookies


def test_several_audiences_with_azp_are_fine(tb, idp):
    _admin(tb)
    idp.claims = {"aud": ["trailbeam", "api"], "azp": "trailbeam"}
    assert tb.sign_in(tb.browser()).status == 200


@pytest.mark.parametrize("alg", ["HS256", "none"])
def test_tokens_signed_with_the_secret_or_not_at_all(tb, idp, alg):
    _admin(tb)
    idp.alg = alg
    assert tb.sign_in(tb.browser()).status in (400, 502)


def test_a_token_signed_by_another_key(tb, idp):
    """Signed under the provider's key ID with a key that isn't the provider's: the JWKS's key refuses it."""
    import jwt
    from cryptography.hazmat.primitives.asymmetric import rsa

    _admin(tb)
    other = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    idp.sign = lambda claims: jwt.encode(claims, other, algorithm="RS256", headers={"kid": idp.kid})
    b = tb.browser()
    assert tb.sign_in(b).status == 400
    assert "trailbeam_sid" not in b.cookies


def test_discovery_must_name_the_same_issuer(tb, idp):
    _admin(tb)
    idp.discovery_issuer = idp.issuer + "/"
    r = tb.browser().get("/login/oidc")
    assert r.status == 502 and "set up wrongly" in r.text


def test_a_provider_that_is_down(store):
    t = Trailbeam(store, FakeIdP())
    try:
        t.idp.close()
        _admin(t)
        r = t.browser().get("/login/oidc")
        assert r.status == 502 and "can&#x27;t be reached" in r.text
    finally:
        t.close()


# ---- groups and roles ---------------------------------------------------------------------------------------


def test_groups_make_admins_and_members_and_follow_the_provider(tb):
    _admin(tb)
    tb.configure(admin_groups="trailbeam-admins", member_groups="engineering")
    b = tb.browser()
    tb.sign_in(b, groups=["engineering", "trailbeam-admins"])
    assert _me(b)[1]["user"]["role"] == "admin"
    b2 = tb.browser()
    tb.sign_in(b2, groups=["engineering"])  # left the admin group
    assert _me(b2)[1]["user"]["role"] == "member"
    assert _me(b)[0] == 401  # the role change ended the admin's old session
    r = tb.sign_in(tb.browser(), groups=["sales"])  # in neither group: member groups are set, so no default role
    assert r.status == 403 and "give you no access" in r.text


def test_a_default_role_of_none_refuses_people_in_no_group(tb):
    _admin(tb)
    tb.configure(admin_groups="admins", default_role="none")
    assert tb.sign_in(tb.browser(), groups=["x"]).status == 403
    assert tb.sign_in(tb.browser(), sub="b", email="b@example.com", groups="admins").status == 200  # one string


def test_without_group_settings_the_role_is_kept_here(tb):
    _admin(tb)
    b = tb.browser()
    tb.sign_in(b, groups=["whatever"])
    uid = tb.app.accounts.user_by_email("alice@example.com")["id"]
    tb.app.accounts.set_role(uid, "admin", NOW_MS())  # an admin promotes them here
    b2 = tb.browser()
    tb.sign_in(b2)
    assert _me(b2)[1]["user"]["role"] == "admin"


def test_the_last_admin_stays_one_when_their_group_goes(store, idp):
    t = Trailbeam(store, idp, admin_groups="admins")
    try:
        b = t.browser()
        t.sign_in(b, groups=["admins"])  # no admin yet: an admin group lets the first one in
        assert _me(b)[1]["user"]["role"] == "admin"
        b2 = t.browser()
        assert t.sign_in(b2, groups=[]).status == 200
        assert _me(b2)[1]["user"]["role"] == "admin"  # the server always keeps an admin
    finally:
        t.close()


def test_a_custom_groups_claim(tb):
    _admin(tb)
    tb.configure(groups_claim="roles", admin_groups="Trailbeam.Admin")
    b = tb.browser()
    tb.sign_in(b, roles=["Trailbeam.Admin"])
    assert _me(b)[1]["user"]["role"] == "admin"


def test_an_entra_groups_overage_is_refused_with_a_reason(tb, idp):
    _admin(tb)
    tb.configure(admin_groups="admins")
    idp.claims = {"_claim_names": {"groups": "src1"},
                  "_claim_sources": {"src1": {"endpoint": "https://graph.microsoft.com/v1.0/users/x/getMemberObjects"}}}
    r = tb.sign_in(tb.browser())
    assert r.status == 403 and "too many of them" in r.text


# ---- who may sign in ------------------------------------------------------------------------------------------


def test_allowed_domains_need_a_verified_email_in_them(tb, idp):
    _admin(tb)
    tb.configure(allowed_domains="example.com, Example.org")
    assert tb.sign_in(tb.browser()).status == 200
    assert tb.sign_in(tb.browser(), sub="o", email="o@example.org").status == 200
    assert tb.sign_in(tb.browser(), sub="e", email="e@evil.example").status == 403
    assert tb.sign_in(tb.browser(), sub="u", email="u@example.com", email_verified=False).status == 403
    assert tb.sign_in(tb.browser(), sub="h", email="h@example.com", hd="other.example").status == 403


def test_google_needs_the_workspace_domain_claim():
    s = oidc.make_settings(issuer="https://accounts.google.com", client_id="c", public_url="https://tb.example.com",
                           allowed_domains="example.com")
    rules = oidc.Rules(s)
    base = {"iss": "https://accounts.google.com", "sub": "1", "email": "a@example.com", "email_verified": True}
    assert rules.person({**base, "hd": "example.com"}).email == "a@example.com"
    with pytest.raises(oidc.OidcError) as e:
        rules.person(base)  # a personal Google account with a verified example.com address
    assert "Google Workspace" in e.value.message and "no hd claim" in e.value.detail


def test_an_email_with_a_local_account_is_not_taken_over(tb):
    _admin(tb, "alice@example.com")
    b = tb.browser()
    r = tb.sign_in(b)  # the provider says alice@example.com, but iss+sub is new
    assert r.status == 403 and "already has an account" in r.text
    assert "trailbeam_sid" not in b.cookies


def test_linking_by_email_when_turned_on_and_verified(tb):
    uid = _admin(tb, "alice@example.com")
    tb.configure(link_email=True)
    assert tb.sign_in(tb.browser(), email_verified=False).status == 403
    b = tb.browser()
    assert tb.sign_in(b).status == 200
    me = _me(b)[1]
    assert me["user"]["id"] == uid and me["user"]["role"] == "admin"


def test_a_disabled_person_is_refused(tb):
    _admin(tb)
    tb.sign_in(tb.browser())
    uid = tb.app.accounts.user_by_email("alice@example.com")["id"]
    tb.app.accounts.set_disabled(uid, True, NOW_MS())
    r = tb.sign_in(tb.browser())
    assert r.status == 403 and "disabled" in r.text


def test_with_no_admin_only_an_admin_group_gets_in_and_becomes_the_first_admin(store, idp):
    t = Trailbeam(store, idp, admin_groups="admins")
    try:
        r = t.browser().get("/login")
        assert r.status == 200 and "Sign in with" in r.text and 'name="password"' not in r.text
        r = t.sign_in(t.browser(), groups=["staff"])
        assert r.status == 403 and "no admin yet" in r.text
        assert t.app.accounts.users() == []
        b = t.browser()
        assert t.sign_in(b, sub="root", email="root@example.com", groups=["admins"]).status == 200
        status, me = _me(b)
        assert status == 200 and me["auth"] == "accounts" and me["user"]["role"] == "admin"
        assert _me(t.browser())[0] == 401  # the server switched to accounts
    finally:
        t.close()


def test_single_sign_on_off_is_not_found(store):
    httpd = make_server(store, "127.0.0.1", 0)
    th = threading.Thread(target=httpd.serve_forever, daemon=True)
    th.start()
    try:
        b = Browser(httpd.server_address[1])
        assert b.get("/login/oidc").status == 404
        assert b.get("/login/oidc/callback?code=a&state=b").status == 404
    finally:
        httpd.shutdown()
        httpd.server_close()


def test_https_cookies(tb, idp):
    _admin(tb)
    from trailbeam.server.proxy import TrustedProxies, parse_networks

    tb.app.proxies = TrustedProxies(parse_networks("127.0.0.1"))
    b = tb.browser()
    h = {"X-Forwarded-Proto": "https"}
    r = b.get("/login/oidc", h)
    c = [x for x in r.headers.get_all("Set-Cookie") if "oidc" in x][0]
    assert c.startswith("__Secure-trailbeam_oidc=") and "; Secure" in c
    r = b.get(_path(idp.approve(r.headers["Location"])), h)
    assert r.status == 200
    assert any(x.startswith("__Host-trailbeam_sid=") and "; Secure" in x for x in r.headers.get_all("Set-Cookie"))


# ---- settings -------------------------------------------------------------------------------------------------


def test_settings():
    s = oidc.make_settings(issuer="https://id.example.com/realms/main", client_id="tb",
                           public_url="https://tb.example.com/", member_groups="eng")
    assert s.redirect_uri == "https://tb.example.com/login/oidc/callback"
    assert s.default_role is None  # member groups given: people in none are refused
    assert oidc.make_settings(issuer="https://i.example.com", client_id="c", public_url="https://t.example.com",
                              scopes="email profile").scopes == ("openid", "email", "profile")
    assert oidc.make_settings(issuer=None, client_id=None, public_url=None) is None
    for kw, msg in [
        (dict(issuer="https://i.example.com", client_id="c", public_url=None), "--public-url"),
        (dict(issuer="https://i.example.com", client_id=None, public_url="https://t.example.com"), "client-id"),
        (dict(issuer="http://id.example.com", client_id="c", public_url="https://t.example.com"), "https"),
        (dict(issuer="https://i.example.com", client_id="c", public_url="https://t.example.com/tb"), "without a path"),
        (dict(issuer="https://i.example.com", client_id="c", public_url="https://t.example.com",
              default_role="admin"), "member or none"),
        (dict(issuer=None, client_id="c", public_url=None), "without --oidc-issuer"),
    ]:
        with pytest.raises(oidc.SettingsError, match=msg):
            oidc.make_settings(**kw)


def test_the_server_refuses_to_start_with_half_the_settings(tmp_path, monkeypatch):
    for k in list(os.environ):
        if k.startswith("TRAILBEAM_OIDC") or k == "TRAILBEAM_PUBLIC_URL":
            monkeypatch.delenv(k)
    a = main_mod.build_parser().parse_args(["--db", str(tmp_path / "x.sqlite"), "--oidc-issuer",
                                            "https://id.example.com", "--oidc-client-id", "tb"])
    with pytest.raises(SystemExit) as e:
        main_mod.serve(a)
    assert e.value.code == 2


def test_flags_and_environment(tmp_path, monkeypatch):
    secret = tmp_path / "secret"
    secret.write_text("abc\n")
    monkeypatch.setenv("TRAILBEAM_OIDC_ISSUER", "https://id.example.com/realms/main")
    monkeypatch.setenv("TRAILBEAM_OIDC_CLIENT_ID", "tb")
    monkeypatch.setenv("TRAILBEAM_PUBLIC_URL", "https://tb.example.com")
    monkeypatch.setenv("TRAILBEAM_OIDC_ADMIN_GROUPS", "a, b")
    monkeypatch.setenv("TRAILBEAM_OIDC_LINK_EMAIL", "true")
    a = main_mod.build_parser().parse_args(["--oidc-client-secret-file", str(secret), "--oidc-label", "Okta"])
    s = main_mod.oidc_settings(a)
    assert (s.client_secret, s.admin_groups, s.link_email, s.label) == ("abc", ("a", "b"), True, "Okta")
    monkeypatch.setenv("TRAILBEAM_OIDC_CLIENT_SECRET", "from-env")
    assert main_mod.oidc_settings(main_mod.build_parser().parse_args([])).client_secret == "from-env"


def test_without_the_extra_it_says_which_to_install(monkeypatch):
    monkeypatch.setitem(sys.modules, "jwt", None)  # import jwt raises ImportError
    with pytest.raises(oidc.SettingsError, match=r'trailbeam\[server,sso\]'):
        oidc.require_pyjwt()


def test_starting_sign_ins_is_limited_per_address(tb):
    _admin(tb)
    b = tb.browser()
    codes = [b.get("/login/oidc").status for _ in range(oidc.STARTS_PER_ADDRESS + 1)]
    assert codes[:-1] == [302] * oidc.STARTS_PER_ADDRESS
    assert codes[-1] == 429
