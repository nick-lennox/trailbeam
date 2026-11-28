"""Who may do what (access.py, tokens.py): ingest and personal tokens, the switch from TRAILBEAM_TOKEN to accounts
when the first admin exists, and the admin/member check on every route, with attempts to get round each.

Runs on SQLite (file and ``:memory:``), and on Postgres when TRAILBEAM_TEST_POSTGRES_URL names a database."""
import json
import os
import sys
import threading
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

import pytest

from trailbeam.server import access, auth, first_admin, tokens as tokens_mod
from trailbeam.server.access import ADMIN, INGEST, MEMBER, METRICS, OPEN, Access, AccessUnavailable, State
from trailbeam.server.accounts import Accounts
from trailbeam.server.db import Database
from trailbeam.server.passwords import Hasher
from trailbeam.server.proxy import TrustedProxies, parse_networks
from trailbeam.server.server import make_server
from trailbeam.server.tokens import Tokens, new_token, token_hash, well_formed

from spans import T0, Consumer, to_proto

ROOT = Path(__file__).resolve().parents[2]
PG = os.environ.get("TRAILBEAM_TEST_POSTGRES_URL")
SHARED = "s3cret-token-0123456789"
PW = "correct horse battery"
NOW = 1_800_000_000_000
QUICK = Hasher(log2_n=10, r=8, p=1)  # quick to test; test_passwords.py checks the real parameters


class Clock:
    def __init__(self):
        self.ms = NOW
        self.mono = 1000.0

    def now_ms(self):
        return self.ms

    def monotonic(self):
        return self.mono

    def advance(self, s: float):
        self.ms += int(s * 1000)
        self.mono += s


def _open(kind, tmp_path, memory=False, name="trailbeam.sqlite"):
    if kind == "sqlite":
        return Database(":memory:" if memory else str(tmp_path / name))
    sys.path.insert(0, str(ROOT / "tests" / "conformance"))
    from pg_helpers import postgres_store
    return postgres_store()


@pytest.fixture(params=["sqlite", "sqlite-memory", pytest.param("postgres", marks=pytest.mark.skipif(
    not PG, reason="TRAILBEAM_TEST_POSTGRES_URL is not set"))])
def store(request, tmp_path):
    s = _open(request.param.split("-")[0], tmp_path, memory=request.param.endswith("memory"))
    yield s
    s.close()


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *a, **k):
        return None


OPENER = urllib.request.build_opener(NoRedirect)
PROTO = {"Content-Type": "application/x-protobuf"}


def call(url, path, *, method="GET", data=None, headers=None):
    req = urllib.request.Request(url + path, data=data, method=method, headers=headers or {})
    try:
        with OPENER.open(req, timeout=10) as r:
            return r.status, dict(r.headers), r.read()
    except urllib.error.HTTPError as e:
        return e.code, dict(e.headers), e.read()


def otlp(mid="m-access-1"):
    c = Consumer()
    return to_proto([c.send(T0, mid), c.receive(T0 + 1_000, mid, "rh-a")]).SerializeToString()


def bearer(t):
    return {"Authorization": f"Bearer {t}"}


def key(t):
    return {"x-trailbeam-key": t}


@pytest.fixture
def serve(store):
    """``serve(token=None, **kw)``: the server on ``store``; returns (base URL, App). The "admin exists" cache is
    off (ttl 0), so each test sees its own changes at once; test_the_switch_* check the cache itself."""
    started = []

    def start(token=None, ttl_s=0.0, **kw):
        httpd = make_server(store, "127.0.0.1", 0, ui_dir=None, auth=auth.Auth(token) if token else None, **kw)
        app = httpd.app
        app.access.state.ttl_s = ttl_s
        if app.signin is not None:
            app.signin.hasher = QUICK
        if app.tokens is not None:
            app.tokens.touch_inline = True
        threading.Thread(target=httpd.serve_forever, daemon=True).start()
        started.append(httpd)
        return f"http://127.0.0.1:{httpd.server_address[1]}", app

    yield start
    for httpd in started:
        httpd.shutdown()
        httpd.server_close()


def add_user(app, email, role, password=PW, name=None):
    return app.accounts.create_user(email, name or email.split("@")[0].title(), role, QUICK.hash(password), NOW)


def sign_in(url, email, password=PW):
    """The session cookie (name=value), or None, and the response status."""
    form = urllib.parse.urlencode({"email": email, "password": password, "next": "/tenants"}).encode()
    status, headers, _ = call(url, "/login", method="POST", data=form,
                              headers={"Content-Type": "application/x-www-form-urlencoded"})
    c = headers.get("Set-Cookie", "")
    return (c.split(";")[0] if status == 303 and c.startswith("trailbeam_sid=") else None), status, headers


# ---- tokens ---------------------------------------------------------------------------------------------------


def test_tokens_have_their_prefix_40_random_characters_and_only_their_hash_is_stored(store):
    a = Accounts(store)
    t = Tokens(a)
    tid, tok = t.create_ingest("orders-api prod")
    assert tok.startswith("tbi_") and len(tok) == 44 and well_formed(tok, "tbi_")
    assert len({new_token("tbi_") for _ in range(1000)}) == 1000
    row = a.ingest_tokens()[0]
    assert row["id"] == tid and row["prefix"] == tok[:8] and row["name"] == "orders-api prod"
    assert a.ingest_token(token_hash(tok))["id"] == tid
    assert all(tok not in str(v) for r in a._read("SELECT * FROM ingest_tokens") for v in r.values())
    uid = a.create_user("ana@example.com", "Ana", "member", None, NOW)
    pid, ptok = t.create_personal(uid, "ci")
    assert ptok.startswith("tbp_") and len(ptok) == 44
    assert a.personal_tokens(uid)[0]["prefix"] == ptok[:8]
    assert all(ptok not in str(v) for r in a._read("SELECT * FROM personal_tokens") for v in r.values())


@pytest.mark.parametrize("bad", [None, "", "tbi_", "tbi_" + "a" * 39, "tbi_" + "a" * 41, "tbi_" + "a" * 39 + "-",
                                 "tbp_" + "a" * 40, "TBI_" + "a" * 40, "tbi_" + "é" * 40, " tbi_" + "a" * 39])
def test_malformed_tokens_are_refused_without_a_store_lookup(bad):
    class Never:
        def ingest_token(self, h):
            raise AssertionError("looked up")

        personal_token = ingest_token

    t = Tokens(Never())
    assert t.ingest(bad) is None
    assert t.personal(bad.replace("tbi_", "tbp_", 1) if bad and bad.startswith("tbi_") else "tbi_" + "a" * 40) is None


def test_a_revoked_ingest_token_stops_here_at_once_and_on_other_replicas_when_their_cache_expires(store):
    clock = Clock()
    a = Accounts(store)
    here = Tokens(a, now_ms=clock.now_ms, clock=clock.monotonic)
    there = Tokens(a, now_ms=clock.now_ms, clock=clock.monotonic)
    tid, tok = here.create_ingest("billing")
    assert here.ingest(tok) == tid and there.ingest(tok) == tid
    assert here.revoke_ingest(tid) and not here.revoke_ingest(tid)
    assert here.ingest(tok) is None
    assert there.ingest(tok) == tid  # cached
    clock.advance(tokens_mod.INGEST_CACHE_TTL_S)
    assert there.ingest(tok) is None
    assert a.ingest_tokens()[0]["revoked_at_ms"] == NOW  # the row stays, for the list


def test_unknown_tokens_are_cached_too(store):
    clock = Clock()
    a = Accounts(store)
    calls = []
    orig = a.ingest_token
    a.ingest_token = lambda h: calls.append(h) or orig(h)
    t = Tokens(a, now_ms=clock.now_ms, clock=clock.monotonic)
    unknown = new_token("tbi_")
    for _ in range(5):
        assert t.ingest(unknown) is None
    assert len(calls) == 1
    clock.advance(tokens_mod.INGEST_CACHE_TTL_S)
    t.ingest(unknown)
    assert len(calls) == 2


def test_last_used_is_written_at_most_once_a_minute_per_token(store):
    clock = Clock()
    a = Accounts(store)
    writes = []
    orig = a.touch_ingest_token
    a.touch_ingest_token = lambda tid, now: writes.append(now) or orig(tid, now)
    t = Tokens(a, now_ms=clock.now_ms, clock=clock.monotonic, touch_inline=True)
    tid, tok = t.create_ingest("orders")
    for _ in range(50):
        t.ingest(tok)
        clock.advance(1)
    assert writes == [NOW]
    assert a.ingest_tokens()[0]["last_used_ms"] == NOW
    clock.advance(11)
    t.ingest(tok)
    assert len(writes) == 2


def test_personal_tokens_follow_their_owner(store):
    clock = Clock()
    a = Accounts(store)
    t = Tokens(a, now_ms=clock.now_ms, clock=clock.monotonic)
    uid = a.create_user("ana@example.com", "Ana", "member", None, NOW)
    pid, tok = t.create_personal(uid, "ci")
    assert t.personal(tok).role == "member"
    a.set_role(uid, "admin", NOW)
    t.forget_user(uid)
    assert t.personal(tok).role == "admin"
    a.create_user("root@example.com", "Root", "admin", None, NOW)  # the last admin can't be disabled
    a.set_disabled(uid, True, NOW)
    clock.advance(tokens_mod.PERSONAL_CACHE_TTL_S)
    assert t.personal(tok) is None
    a.set_disabled(uid, False, NOW)
    clock.advance(tokens_mod.PERSONAL_CACHE_TTL_S)
    assert t.personal(tok).user_id == uid
    _, other = t.create_personal(uid, "other")
    assert not t.revoke_personal(pid, user_id=uid + 1)  # someone else's
    assert t.revoke_personal(pid, user_id=uid)
    assert t.personal(tok) is None and t.personal(other) is not None
    a.remove_user(uid)
    clock.advance(tokens_mod.PERSONAL_CACHE_TTL_S)
    assert t.personal(other) is None and a.personal_tokens() == []


def test_an_expired_personal_token_stops_working(store):
    clock = Clock()
    a = Accounts(store)
    t = Tokens(a, now_ms=clock.now_ms, clock=clock.monotonic)
    uid = a.create_user("ana@example.com", "Ana", "member", None, NOW)
    _, tok = t.create_personal(uid, "ci", expires_at_ms=NOW + 60_000)
    assert t.personal(tok) is not None
    clock.advance(60)
    assert t.personal(tok) is None


# ---- the checks, without HTTP ----------------------------------------------------------------------------------


class H(dict):
    """Headers, looked up ignoring case as http.server's are."""

    def get(self, k, default=None):
        for kk, v in self.items():
            if kk.lower() == k.lower():
                return v
        return default


def test_x_trailbeam_key_is_read_before_authorization():
    assert access.credential(H({"x-trailbeam-key": " a ", "Authorization": "Bearer b"})) == "a"
    assert access.credential(H({"Authorization": "Bearer b"})) == "b"
    assert access.credential(H({"Authorization": "bearer  b "})) == "b"
    assert access.credential(H({"Authorization": "Basic b"})) is None
    assert access.credential(H({"Authorization": "Bearer "})) is None
    assert access.credential(H({})) is None


def test_every_level_once_an_admin_exists(store):
    a = Accounts(store)
    t = Tokens(a)
    from trailbeam.server.sessions import Sessions
    s = Sessions(a)
    acc = Access(auth.Auth(SHARED), a, s, t, State(a, ttl_s=0))
    admin = a.create_user("root@example.com", "Root", "admin", None, NOW)
    member = a.create_user("ana@example.com", "Ana", "member", None, NOW)
    _, atok = t.create_personal(admin, "a")
    _, mtok = t.create_personal(member, "m")
    _, itok = t.create_ingest("svc")
    sid, _ = s.start(member)
    cases = {
        "nothing": H(), "shared": H(bearer(SHARED)), "shared key": H(key(SHARED)), "ingest": H(key(itok)),
        "member token": H(bearer(mtok)), "admin token": H(bearer(atok)),
        "member session": H({"Cookie": f"trailbeam_sid={sid}"}),
        "old cookie": H({"Cookie": auth.Auth(SHARED).set_cookie(False).split(";")[0]}),
    }
    want = {  # level -> who is let in
        OPEN: set(cases),
        INGEST: {"shared", "shared key", "ingest"},
        METRICS: {"shared", "shared key", "member token", "admin token", "member session"},
        MEMBER: {"member token", "admin token", "member session"},
        ADMIN: {"admin token"},
    }
    for level, ok in want.items():
        got = {name for name, h in cases.items() if acc.check(h, level).ok}
        assert got == ok, level
    assert acc.check(H(bearer(mtok)), ADMIN).status == 403
    assert acc.check(H(bearer(SHARED)), MEMBER).message == access.SHARED_UI_GONE
    assert acc.check(H(), MEMBER).status == 401
    # over HTTPS only the __Host- cookie is a session (sessions.py)
    assert not acc.check(cases["member session"], MEMBER, "https").ok
    assert acc.check(H({"Cookie": f"__Host-trailbeam_sid={sid}"}), MEMBER, "https").ok
    assert acc.check(H(bearer(mtok)), MEMBER, "https").ok
    assert acc.mode() == "accounts"


def test_every_level_before_an_admin_exists(store):
    a = Accounts(store)
    t = Tokens(a)
    member = a.create_user("ana@example.com", "Ana", "member", None, NOW)  # a member alone doesn't switch
    _, mtok = t.create_personal(member, "m")
    for shared in (None, auth.Auth(SHARED)):
        acc = Access(shared, a, None, t, State(a, ttl_s=0))
        assert acc.mode() == ("token" if shared else "off")
        for level in (METRICS, MEMBER):
            assert acc.check(H(), level).ok is (shared is None)
            assert acc.check(H(bearer(SHARED)), level).ok
            assert acc.check(H(key(SHARED)), level).ok
            assert acc.check(H(bearer(mtok)), level).ok is (shared is None)
        assert acc.check(H(bearer(SHARED)), ADMIN).message == access.NO_ADMIN_YET
        assert acc.check(H(), INGEST).ok is (shared is None)
        # with the token set, a caller without it is told only that, not why ADMIN would be refused (QV-790)
        for h in (H(), H(bearer("nope")), H(bearer(mtok))):
            d = acc.check(h, ADMIN)
            assert not d.ok and (d.status, d.message) == ((401, access.NEEDS_SHARED) if shared
                                                           else (403, access.NO_ADMIN_YET))


def test_ingest_closes_when_the_first_ingest_token_exists_and_a_revoked_one_keeps_it_closed(store):
    a = Accounts(store)
    t = Tokens(a)
    acc = Access(None, a, None, t, State(a, ttl_s=0))
    assert acc.check(H(), INGEST).ok and not acc.ingest_closed()
    tid, tok = t.create_ingest("svc")
    assert not acc.check(H(), INGEST).ok
    assert acc.check(H(key(tok)), INGEST).ok and acc.check(H(bearer(tok)), INGEST).ok
    t.revoke_ingest(tid)
    assert not acc.check(H(key(tok)), INGEST).ok
    assert not acc.check(H(), INGEST).ok  # revoking the last token does not open ingest again


def test_the_switch_is_cached_for_at_most_5_seconds():
    calls = []

    class A:
        admin = False

        def auth_state(self):
            calls.append(1)
            return self.admin, False

    clock = Clock()
    a = A()
    st = State(a, clock=clock.monotonic)
    assert st.ttl_s <= 5
    assert not st.admin_exists()
    a.admin = True
    clock.advance(4.9)
    assert not st.admin_exists() and len(calls) == 1
    clock.advance(0.1)
    assert st.admin_exists() and len(calls) == 2


def test_a_store_that_cant_answer_keeps_the_last_answer_and_never_lets_anyone_in_without_one():
    class Down:
        fail = True

        def auth_state(self):
            if self.fail:
                raise OSError("connection refused")
            return True, True

    clock = Clock()
    d = Down()
    st = State(d, ttl_s=0, clock=clock.monotonic)
    acc = Access(None, d, None, None, st)
    with pytest.raises(AccessUnavailable):
        acc.check(H(), MEMBER)
    with pytest.raises(AccessUnavailable):
        acc.check(H(), INGEST)
    d.fail = False
    assert acc.mode() == "accounts"
    d.fail = True
    assert acc.mode() == "accounts"  # the last answer
    assert not acc.check(H(), MEMBER).ok


# ---- over HTTP ------------------------------------------------------------------------------------------------


def test_without_users_or_tokens_nothing_changes(serve):
    url, app = serve()
    assert call(url, "/v1/traces", method="POST", data=otlp(), headers=PROTO)[0] == 200
    assert call(url, "/api/messages")[0] == 200 and call(url, "/")[0] == 200 and call(url, "/metrics")[0] == 200
    h = json.loads(call(url, "/api/health")[2])
    assert h["config"]["auth"] == "off" and h["config"]["ingest_auth"] == "open"
    assert json.loads(call(url, "/api/me")[2]) == {"auth": "off", "via": "none", "user": None, "sign_out": False,
                                                        "features": {"dashboards": False, "alerts": False,
                                                                     "accounts": True}}
    assert call(url, "/login")[0] == 404


def test_me_says_which_off_by_default_parts_are_on(serve, monkeypatch):
    # the UI shows Dashboards and Alerts from this, instead of asking their routes for a 404 on every page (QV-766)
    monkeypatch.setenv("TRAILBEAM_DASHBOARDS", "1")
    monkeypatch.setenv("TRAILBEAM_ALERT_CHECK", "on")
    url, _ = serve()
    assert json.loads(call(url, "/api/me")[2])["features"] == {"dashboards": True, "alerts": True, "accounts": True}


def test_with_accounts_off_the_token_server_says_how_to_turn_them_on(serve):
    # the README team install: TRAILBEAM_TOKEN, users and roles off. /people must not advise a users create that
    # refuses without --accounts and makes an admin the server ignores (QV-767)
    url, _ = serve(SHARED, accounts=None)
    me = json.loads(call(url, "/api/me", headers=bearer(SHARED))[2])
    assert (me["auth"], me["via"], me["user"], me["features"]["accounts"]) == ("token", "shared_token", None, False)
    status, _, body = call(url, "/api/users", headers=bearer(SHARED))
    assert status == 403 and json.loads(body)["error"] == access.ACCOUNTS_OFF
    assert "TRAILBEAM_ACCOUNTS=1" in access.ACCOUNTS_OFF and "users create --accounts" in access.ACCOUNTS_OFF


@pytest.mark.parametrize("accounts", ["auto", None])
def test_with_the_token_and_no_admin_admin_routes_say_nothing_of_the_setup_without_the_token(serve, accounts):
    # 0.4.0 answered 401 on every path but /api/ready and /api/health without the token. The no-admin sentences
    # (accounts on or off, the CLI to run) are for a caller who has it (QV-790)
    url, _ = serve(SHARED, accounts=accounts)
    told = access.NO_ADMIN_YET if accounts else access.ACCOUNTS_OFF
    for h in ({}, bearer("nope"), key("nope")):
        for path in ("/api/users", "/api/tokens/ingest"):
            status, headers, body = call(url, path, headers=h)
            assert status == 401 and json.loads(body)["error"] == access.NEEDS_SHARED, (path, h)
            assert headers["WWW-Authenticate"] == 'Bearer realm="trailbeam"'
        status, _, body = call(url, "/people", headers=h)
        assert status == 401 and told.encode() not in body and b"TRAILBEAM_ACCOUNTS" not in body
        assert b"Welcome back." in body  # the token login page
    status, _, body = call(url, "/api/users", headers=bearer(SHARED))
    assert status == 403 and json.loads(body)["error"] == told
    status, _, body = call(url, "/people", headers=bearer(SHARED))
    assert status == 403 and b"users create --accounts" in body


def test_ingest_tokens_over_http_in_both_headers(serve):
    url, app = serve()
    tid, tok = app.tokens.create_ingest("orders-api prod")
    app.access.state.changed()
    status, headers, body = call(url, "/v1/traces", method="POST", data=otlp(), headers=PROTO)
    assert status == 401 and headers["WWW-Authenticate"] == 'Bearer realm="trailbeam"'
    assert b"x-trailbeam-key" in body and b"Authorization: Bearer" in body
    assert call(url, "/v1/traces", method="POST", data=otlp(), headers={**PROTO, **key(tok)})[0] == 200
    assert call(url, "/v1/traces", method="POST", data=otlp("m-2"), headers={**PROTO, **bearer(tok)})[0] == 200
    # a wrong x-trailbeam-key is not rescued by a right Authorization
    assert call(url, "/v1/traces", method="POST", data=otlp(),
                headers={**PROTO, **key(new_token("tbi_")), **bearer(tok)})[0] == 401
    assert json.loads(call(url, "/api/health")[2])["config"]["ingest_auth"] == "token"
    # an ingest token reads nothing: but no admin and no TRAILBEAM_TOKEN, so the API is open anyway
    app.tokens.revoke_ingest(tid)
    assert call(url, "/v1/traces", method="POST", data=otlp(), headers={**PROTO, **key(tok)})[0] == 401
    assert app.accounts.ingest_tokens()[0]["last_used_ms"] is not None


def test_the_shared_token_works_in_x_trailbeam_key_too(serve):
    url, app = serve(SHARED)
    assert call(url, "/v1/traces", method="POST", data=otlp(), headers={**PROTO, **key(SHARED)})[0] == 200
    assert call(url, "/api/messages", headers=key(SHARED))[0] == 200
    _, tok = app.tokens.create_ingest("svc")
    assert call(url, "/v1/traces", method="POST", data=otlp("m-3"), headers={**PROTO, **key(tok)})[0] == 200
    assert call(url, "/api/messages", headers=key(tok))[0] == 401  # an ingest token never reads
    assert json.loads(call(url, "/api/me", headers=bearer(SHARED))[2])["via"] == "shared_token"


def test_once_an_admin_exists_the_shared_token_opens_only_ingest_and_metrics(serve):
    url, app = serve(SHARED)
    old_cookie = auth.Auth(SHARED).set_cookie(False).split(";")[0]
    assert call(url, "/api/messages", headers={"Cookie": old_cookie})[0] == 200
    add_user(app, "root@example.com", "admin")
    assert call(url, "/v1/traces", method="POST", data=otlp(), headers={**PROTO, **bearer(SHARED)})[0] == 200
    assert call(url, "/metrics", headers=bearer(SHARED))[0] == 200
    status, headers, body = call(url, "/api/messages", headers=bearer(SHARED))
    assert status == 401 and "TRAILBEAM_TOKEN works only for ingest and /metrics" in json.loads(body)["error"]
    assert call(url, "/api/messages", headers={"Cookie": old_cookie})[0] == 401
    status, _, page = call(url, "/tenants", headers={"Cookie": old_cookie})
    assert status == 401 and b'name="email"' in page and b'name="token"' not in page
    # the login form takes no token any more
    form = urllib.parse.urlencode({"token": SHARED}).encode()
    status, headers, _ = call(url, "/login", method="POST", data=form)
    assert status == 401 and "Set-Cookie" not in headers
    h = json.loads(call(url, "/api/health", headers=bearer(SHARED))[2])
    assert set(h) == {"status", "version"}
    status, _, body = call(url, "/metrics")
    assert status == 401 and b"<html" not in body.lower()


def test_people_sign_in_and_out_and_personal_tokens_carry_their_role(serve):
    url, app = serve()
    add_user(app, "root@example.com", "admin")
    ana = add_user(app, "ana@example.com", "member")
    assert call(url, "/api/messages")[0] == 401 and call(url, "/")[0] == 401
    assert call(url, "/v1/traces", method="POST", data=otlp(), headers=PROTO)[0] == 200  # no token: still open
    cookie, status, headers = sign_in(url, "ANA@example.com")
    assert cookie and headers["Location"] == "/tenants"
    assert "HttpOnly" in headers["Set-Cookie"] and "SameSite=Strict" in headers["Set-Cookie"]
    assert call(url, "/api/messages", headers={"Cookie": cookie})[0] == 200
    assert call(url, "/tenants", headers={"Cookie": cookie})[0] == 200
    me = json.loads(call(url, "/api/me", headers={"Cookie": cookie})[2])
    assert me == {"auth": "accounts", "via": "session",
                  "user": {"id": ana, "email": "ana@example.com", "name": "Ana", "role": "member"}, "sign_out": True,
                  "features": {"dashboards": False, "alerts": False, "accounts": True},
                  "sign_in": {"password": True, "identities": []}}
    full = json.loads(call(url, "/api/health", headers={"Cookie": cookie})[2])
    assert full["config"]["auth"] == "accounts"
    # a session never sends spans
    _, itok = app.tokens.create_ingest("svc")
    assert call(url, "/v1/traces", method="POST", data=otlp(), headers={**PROTO, "Cookie": cookie})[0] == 401
    # sign out ends it in the store
    status, headers, _ = call(url, "/logout", headers={"Cookie": cookie})
    assert status == 303 and headers["Set-Cookie"].startswith("trailbeam_sid=;")
    assert call(url, "/api/messages", headers={"Cookie": cookie})[0] == 401
    # personal token
    _, ptok = app.tokens.create_personal(ana, "ci")
    assert call(url, "/api/messages", headers=bearer(ptok))[0] == 200
    assert call(url, "/metrics", headers=bearer(ptok))[0] == 200
    assert json.loads(call(url, "/api/me", headers=bearer(ptok))[2])["via"] == "personal_token"
    assert call(url, "/v1/traces", method="POST", data=otlp(), headers={**PROTO, **bearer(ptok)})[0] == 401
    assert call(url, "/v1/traces", method="POST", data=otlp(), headers={**PROTO, **key(ptok)})[0] == 401
    assert call(url, "/api/messages", headers=bearer(itok))[0] == 401  # an ingest token never reads


@pytest.mark.parametrize("email,password", [("ana@example.com", "wrong password!"), ("nobody@example.com", PW)])
def test_a_wrong_sign_in_says_the_same_and_sets_no_cookie(serve, email, password):
    url, app = serve()
    add_user(app, "ana@example.com", "admin")
    cookie, status, headers = sign_in(url, email, password)
    assert cookie is None and status == 401 and "Set-Cookie" not in headers


def test_a_locked_account_answers_429_whatever_the_password(serve):
    url, app = serve()
    uid = add_user(app, "ana@example.com", "admin")
    app.signin.lock_after = 3
    for _ in range(3):
        assert sign_in(url, "ana@example.com", "wrong password!")[1] == 401
    cookie, status, headers = sign_in(url, "ana@example.com")  # the right one
    assert cookie is None and status == 429 and int(headers["Retry-After"]) > 0
    assert app.accounts.user(uid)["locked_until_ms"] is not None


def test_sign_in_over_https_through_a_trusted_proxy_sets_the_host_cookie(serve):
    url, app = serve(trusted_proxies=TrustedProxies(parse_networks("127.0.0.1/32")))
    add_user(app, "ana@example.com", "admin")
    form = urllib.parse.urlencode({"email": "ana@example.com", "password": PW}).encode()
    status, headers, _ = call(url, "/login", method="POST", data=form, headers={"X-Forwarded-Proto": "https"})
    assert status == 303 and headers["Set-Cookie"].startswith("__Host-trailbeam_sid=")
    assert "Secure" in headers["Set-Cookie"]


def test_over_https_a_plain_session_cookie_is_not_a_session(serve):
    url, app = serve(trusted_proxies=TrustedProxies(parse_networks("127.0.0.1/32")))
    add_user(app, "root@example.com", "admin")
    ana = add_user(app, "ana@example.com", "member")
    sid, _ = app.sessions.start(ana)
    https = {"X-Forwarded-Proto": "https"}
    # the same session ID: planted under the plain name it signs no one in over HTTPS
    assert call(url, "/api/me", headers={**https, "Cookie": f"trailbeam_sid={sid}"})[0] == 401
    assert call(url, "/api/messages", headers={**https, "Cookie": f"trailbeam_sid={sid}"})[0] == 401
    assert call(url, "/api/messages", headers={**https, "Cookie": f"__Host-trailbeam_sid={sid}"})[0] == 200
    assert call(url, "/api/messages", headers={"Cookie": f"trailbeam_sid={sid}"})[0] == 200  # plain HTTP
    # a junk cookie someone planted next to it (a space in its value) hides no session (QV-473)
    for c in (f"__Host-trailbeam_sid={sid}; junk=a b", f"junk=x y; __Host-trailbeam_sid={sid}; other=1"):
        assert call(url, "/api/messages", headers={**https, "Cookie": c})[0] == 200
    assert call(url, "/api/messages", headers={**https, "Cookie": f"junk=a b; trailbeam_sid={sid}"})[0] == 401
    assert call(url, "/api/messages", headers={"Cookie": f"trailbeam_sid={sid}; junk=a b"})[0] == 200
    # a name that only starts with something str.strip() would take is not the __Host- name: anyone can
    # plant it, because the browser applies the __Host- rules only to a name starting with __Host- (QV-483)
    for ch in ("\x0b", "\x1c", "\x1f", "\x85", "\xa0"):
        for c in (f"{ch}__Host-trailbeam_sid={sid}", f"junk=1; {ch}__Host-trailbeam_sid={sid}"):
            assert call(url, "/api/me", headers={**https, "Cookie": c})[0] == 401, repr(c)
    # /logout over HTTPS doesn't end the session a plain cookie names...
    status, headers, _ = call(url, "/logout", headers={**https, "Cookie": f"trailbeam_sid={sid}"})
    assert status == 303 and headers["Set-Cookie"].startswith("__Host-trailbeam_sid=;")
    assert call(url, "/api/messages", headers={**https, "Cookie": f"__Host-trailbeam_sid={sid}"})[0] == 200
    # ...but does end the __Host- one
    call(url, "/logout", headers={**https, "Cookie": f"__Host-trailbeam_sid={sid}"})
    assert call(url, "/api/messages", headers={**https, "Cookie": f"__Host-trailbeam_sid={sid}"})[0] == 401


def test_sign_in_from_another_site_is_refused(serve):
    url, app = serve()
    add_user(app, "ana@example.com", "admin")
    form = urllib.parse.urlencode({"email": "ana@example.com", "password": PW}).encode()
    assert call(url, "/login", method="POST", data=form, headers={"Sec-Fetch-Site": "cross-site"})[0] == 403


def test_disabling_or_removing_someone_stops_their_session_and_tokens(serve):
    url, app = serve()
    add_user(app, "root@example.com", "admin")
    ana = add_user(app, "ana@example.com", "member")
    cookie, _, _ = sign_in(url, "ana@example.com")
    _, ptok = app.tokens.create_personal(ana, "ci")
    app.accounts.set_disabled(ana, True, NOW)
    app.sessions.forget_user(ana)
    app.tokens.forget_user(ana)
    assert call(url, "/api/messages", headers={"Cookie": cookie})[0] == 401
    assert call(url, "/api/messages", headers=bearer(ptok))[0] == 401
    bob = add_user(app, "bob@example.com", "member")
    _, btok = app.tokens.create_personal(bob, "ci")
    assert call(url, "/api/messages", headers=bearer(btok))[0] == 200
    app.accounts.remove_user(bob)
    app.tokens.forget_user(bob)
    assert call(url, "/api/messages", headers=bearer(btok))[0] == 401


# Every GET the server answers, API and pages, with what it answers once an admin exists and the request carries
# nothing. A route added to the server without a level here fails test_every_route_has_a_level.
PAGES = ["/", "/message?id=m-1", "/duplicates", "/stuck", "/tenants", "/people", "/tokens", "/account", "/ui/app.js", "/no-such-page",
         "/metrics"]


def _api_paths():
    from openapi_contract import Contract
    out = []
    for op in Contract().ops:
        if op.method == "GET":
            out.append(op.template.replace("{message_id}", "m-1").replace("{probe_id}", "p-1"))
    return out


def test_every_route_needs_a_person_once_an_admin_exists(serve):
    url, app = serve(SHARED)
    add_user(app, "root@example.com", "admin")
    member = add_user(app, "ana@example.com", "member")
    _, mtok = app.tokens.create_personal(member, "m")
    _, itok = app.tokens.create_ingest("svc")
    open_ = {"/api/ready", "/api/health"}
    for path in _api_paths() + PAGES:
        status, _, body = call(url, path)
        if path.split("?")[0] in open_:
            assert status in (200, 503), path
            continue
        assert status == 401, path
        for creds in (bearer(SHARED), key(SHARED), key(itok), bearer(itok)):
            status = call(url, path, headers=creds)[0]
            assert status == (200 if path == "/metrics" and SHARED in str(creds) else 401), (path, creds)
        assert call(url, path, headers=bearer(mtok))[0] != 401, path  # a member: in (then 200 or 404)


def test_every_route_has_a_level():
    """The levels that aren't MEMBER are listed, and only these routes have them."""
    assert access.ROUTES == {
        ("GET", "/api/ready"): OPEN, ("GET", "/api/health"): OPEN, ("GET", "/login"): OPEN,
        ("POST", "/login"): OPEN, ("GET", "/logout"): OPEN, ("GET", "/setup"): OPEN, ("POST", "/setup"): OPEN,
        ("GET", "/login/oidc"): OPEN, ("GET", "/login/oidc/callback"): OPEN,
        ("POST", "/login/passkey/options"): OPEN, ("POST", "/login/passkey"): OPEN,
        ("GET", "/login/github"): OPEN, ("GET", "/login/github/callback"): OPEN,
        ("POST", "/v1/traces"): INGEST, ("GET", "/metrics"): METRICS,
        ("GET", "/set-password"): OPEN, ("POST", "/set-password"): OPEN, ("GET", "/people"): ADMIN, ("GET", "/tokens"): MEMBER,
        ("GET", "/account"): MEMBER}
    for p in _api_paths() + PAGES:
        p = p.split("?")[0]
        assert access.level_of("GET", p) == ROUTES_EXPECTED.get(p, MEMBER), p
    # people and tokens (people_api.py): admins, but a person's own tokens; by the whole path
    assert {(m, t): lvl for m, t, lvl, _ in access.API_ROUTES} == {
        ("GET", "/api/users"): ADMIN, ("POST", "/api/users"): ADMIN, ("PATCH", "/api/users/{id}"): ADMIN,
        ("DELETE", "/api/users/{id}"): ADMIN, ("POST", "/api/users/{id}/link"): ADMIN,
        ("GET", "/api/users/{id}/passkeys"): ADMIN, ("DELETE", "/api/users/{id}/passkeys"): ADMIN,
        ("GET", "/api/tokens/ingest"): ADMIN, ("POST", "/api/tokens/ingest"): ADMIN,
        ("DELETE", "/api/tokens/ingest/{id}"): ADMIN, ("GET", "/api/me/tokens"): MEMBER,
        ("POST", "/api/me/tokens"): MEMBER, ("DELETE", "/api/me/tokens/{id}"): MEMBER,
        ("GET", "/api/me/passkeys"): MEMBER, ("POST", "/api/me/passkeys/options"): MEMBER,
        ("POST", "/api/me/passkeys"): MEMBER, ("DELETE", "/api/me/passkeys/{id}"): MEMBER}
    assert access.level_of("PATCH", "/api/users/12") == ADMIN
    assert access.level_of("DELETE", "/api/users/12/x") == MEMBER  # no route: a 404, not an admin check
    assert access.api_route("DELETE", "/api/users/0") is None and access.api_route("GET", "/api/users/1") is None
    assert access.api_route("POST", "/api/users/7/link") == (ADMIN, "user_link", 7)


ROUTES_EXPECTED = {"/api/ready": OPEN, "/api/health": OPEN, "/metrics": METRICS, "/api/users": ADMIN,
                   "/api/tokens/ingest": ADMIN, "/people": ADMIN}


def test_the_switch_reaches_another_replica_within_5_seconds(store, tmp_path):
    if getattr(store, "path", None) == ":memory:":
        pytest.skip("replicas share a file or a database")
    clock = Clock()
    a = make_server(store, "127.0.0.1", 0, ui_dir=None, auth=auth.Auth(SHARED)).app
    b = make_server(store, "127.0.0.1", 0, ui_dir=None, auth=auth.Auth(SHARED)).app
    for app in (a, b):
        app.access.state.clock = clock.monotonic
        app.access.state.changed()  # read at start-up on the real clock
    hb = H(bearer(SHARED))
    assert a.access.check(hb, MEMBER).ok and b.access.check(hb, MEMBER).ok
    a.accounts.create_user("root@example.com", "Root", "admin", None, NOW)
    a.access.state.changed()
    assert not a.access.check(hb, MEMBER).ok
    assert b.access.check(hb, MEMBER).ok  # its answer is less than 5 s old
    clock.advance(access.STATE_TTL_S)
    assert not b.access.check(hb, MEMBER).ok
    assert b.access.check(hb, INGEST).ok and b.access.check(hb, METRICS).ok


@pytest.mark.parametrize("shared", [None, SHARED])
def test_while_the_store_cant_say_who_it_is_nobody_gets_in(serve, monkeypatch, shared):
    url, app = serve(shared)
    monkeypatch.setattr(app.accounts, "auth_state", lambda: (_ for _ in ()).throw(OSError("down")))
    app.access.state._value = None  # as when the store was already down at start-up
    creds = bearer(shared) if shared else {}
    status, headers, _ = call(url, "/v1/traces", method="POST", data=otlp(), headers={**PROTO, **creds})
    if shared:  # TRAILBEAM_TOKEN sends spans whether or not an admin exists: nothing to ask the store
        assert status == 200
    else:
        assert status == 503 and headers["Retry-After"] == "1"
    assert call(url, "/api/messages", headers=creds)[0] == 500
    assert call(url, "/tenants", headers=creds)[0] == 500
    assert json.loads(call(url, "/api/health", headers=creds)[2]) == {"status": "ok", "version": app_version()}
    assert call(url, "/api/ready")[0] == 200


def app_version():
    from trailbeam.server import __version__
    return __version__


def test_start_up_says_what_the_shared_token_and_ingest_open_now(serve):
    import logging

    from trailbeam.server.__main__ import access_log

    url, app = serve(SHARED)
    url, open_app = serve()  # the same store, without TRAILBEAM_TOKEN
    assert [lvl for lvl, m in access_log(open_app, False, "127.0.0.1") if "ingest is open" in m] == [logging.WARNING]
    assert any("anyone who can reach this port can send spans and read every timeline" in m
               for _, m in access_log(open_app, False, "0.0.0.0"))
    assert access_log(app, True, "127.0.0.1") == [
        (logging.INFO, "access token set: ingest, the API and the web screen need it"),
        (logging.INFO, first_admin.NO_ADMIN)]
    add_user(app, "root@example.com", "admin")
    assert not any(m == first_admin.NO_ADMIN for _, m in access_log(app, True, "127.0.0.1"))
    warnings = [m for lvl, m in access_log(app, True, "127.0.0.1") if lvl == logging.WARNING]
    assert any("works only for ingest and /metrics" in m for m in warnings)
