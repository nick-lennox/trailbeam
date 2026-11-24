"""The people-and-tokens API behind the People and Tokens pages (people_api.py), and the /set-password page its
invite and reset links lead to. With attempts to break them: members and the shared token on admin routes,
another person's token, forms and pages from other sites, used, replaced and old links, a disabled person's link,
and the last admin.

The Postgres cases run when TRAILBEAM_TEST_POSTGRES_URL names a database, as in test_postgres.py."""
import json
import os
import sys
import threading
import time
import urllib.parse
from pathlib import Path

import pytest

from trailbeam.server import access as access_mod, auth, people_api
from trailbeam.server.accounts import Accounts, BadSetupCode
from trailbeam.server.db import Database
from trailbeam.server.passwords import Hasher
from trailbeam.server.proxy import TrustedProxies, parse_networks
from trailbeam.server.server import ApiError, make_server
from trailbeam.server.tokens import Tokens

from test_openapi import contract, request

ROOT = Path(__file__).resolve().parents[2]
PG = os.environ.get("TRAILBEAM_TEST_POSTGRES_URL")
PW = "correct horse battery"
QUICK = Hasher(log2_n=10, r=8, p=1)
TOKEN = "shared-token-0123456789abcdef"
J = {"Content-Type": "application/json"}
FORM = {"Content-Type": "application/x-www-form-urlencoded"}


@pytest.fixture(params=["sqlite", "sqlite-memory", pytest.param("postgres", marks=pytest.mark.skipif(
    not PG, reason="TRAILBEAM_TEST_POSTGRES_URL is not set"))])
def store(request, tmp_path):
    kind = request.param.split("-")[0]
    if kind == "sqlite":
        s = Database(":memory:" if request.param.endswith("memory") else str(tmp_path / "trailbeam.sqlite"))
    else:
        sys.path.insert(0, str(ROOT / "tests" / "conformance"))
        from pg_helpers import postgres_store
        s = postgres_store()
    yield s
    s.close()


class Server:
    """A server with an admin (Ada) and a member (Ben), each with a personal token."""

    def __init__(self, store, token, **opts):
        self.httpd = make_server(store, "127.0.0.1", 0, ui_dir=None, auth=auth.Auth(token) if token else None,
                                 **opts)
        self.app = self.httpd.app
        self.app.access.state.ttl_s = 0.0
        self.app.signin.hasher = QUICK
        threading.Thread(target=self.httpd.serve_forever, daemon=True).start()
        self.url = f"http://127.0.0.1:{self.httpd.server_address[1]}"
        acc = self.acc = self.app.accounts
        self.ada = acc.create_first_admin("ada@example.com", "Ada", QUICK.hash(PW), _now())
        self.ben = acc.create_user("ben@example.com", "Ben", "member", QUICK.hash(PW), _now())
        t = Tokens(acc)
        self.admin = {"Authorization": "Bearer " + t.create_personal(self.ada, "ada")[1]}
        self.member = {"Authorization": "Bearer " + t.create_personal(self.ben, "ben")[1]}

    def __call__(self, method, path, obj=None, headers=None, raw=None):
        data = raw if raw is not None else (json.dumps(obj).encode() if obj is not None else None)
        h = {**(J if data is not None else {}), **(headers or {})}
        status, hdrs, body = request(self.url, method, path, data, h)
        try:
            return status, hdrs, json.loads(body) if body else None
        except ValueError:
            return status, hdrs, body

    def close(self):
        self.httpd.shutdown()
        self.httpd.server_close()


def _now():
    return time.time_ns() // 1_000_000


@pytest.fixture
def srv(store):
    s = Server(store, TOKEN)
    yield s
    s.close()


@pytest.fixture
def open_srv(store):
    """No TRAILBEAM_TOKEN: ingest is open until the first ingest token."""
    s = Server(store, None)
    yield s
    s.close()


def _code(link_url):
    return link_url.split("#code=", 1)[1]


def set_password(srv, code, pw=PW, pw2=None, headers=None):
    form = {"code": code, "password": pw, "password2": pw if pw2 is None else pw2}
    return request(srv.url, "POST", "/set-password", urllib.parse.urlencode(form).encode(),
                   {**FORM, **(headers or {})})


# ---- every route answers --------------------------------------------------------------------------------

def test_every_example_request_answers_on_a_server_with_an_admin(srv):
    """Each People and Tokens operation's x-example-request, on a server with that person and token."""
    ing = srv("POST", "/api/tokens/ingest", {"name": "orders"}, srv.admin)[2]["token"]["id"]
    mine = srv("POST", "/api/me/tokens", {"name": "x"}, srv.admin)[2]["token"]["id"]
    asked = 0
    for op in sorted(contract().ops, key=lambda o: o.method == "DELETE"):  # remove Ben last
        if not {"People", "Tokens"} & set(op.op.get("tags", [])):
            continue
        path = op.op.get("x-example-request", op.template)
        path = path.replace("/api/tokens/ingest/1", f"/api/tokens/ingest/{ing}").replace(
            "/api/me/tokens/1", f"/api/me/tokens/{mine}").replace("/api/users/2", f"/api/users/{srv.ben}")
        body = {"email": "cy@example.com"} if op.key == "POST /api/users" else (
            {"name": "n"} if op.method == "POST" and "tokens" in path else ({"role": "member"} if op.method == "PATCH"
                                                                          else {}))
        status, _, out = srv(op.method, path, None if op.method in ("GET", "DELETE") else body, srv.admin)
        assert status in (200, 201), (op.key, status, out)
        asked += 1
    assert asked == 11


# ---- who may ------------------------------------------------------------------------------------------------

def test_members_and_the_shared_token_get_no_admin_route(srv):
    for method, path in [("GET", "/api/users"), ("POST", "/api/users"), ("PATCH", f"/api/users/{srv.ben}"),
                         ("DELETE", f"/api/users/{srv.ada}"), ("POST", f"/api/users/{srv.ada}/link"),
                         ("GET", "/api/tokens/ingest"), ("POST", "/api/tokens/ingest"),
                         ("DELETE", "/api/tokens/ingest/1")]:
        body = None if method in ("GET", "DELETE") else {"email": "eve@example.com", "role": "admin", "name": "e"}
        assert srv(method, path, body, srv.member)[0] == 403, (method, path)
        assert srv(method, path, body, {"Authorization": f"Bearer {TOKEN}"})[0] == 401, (method, path)
        assert srv(method, path, body)[0] == 401
    assert [u["email"] for u in srv.acc.users()] == ["ada@example.com", "ben@example.com"]
    assert srv.acc.user(srv.ben)["role"] == "member"


ODD_ADMIN_PATHS = ["/api/users/", "/api//users", "/api/users//", "/api/tokens/ingest/",
                   "/api//tokens/ingest", "/api/tokens//ingest", "/api/tokens/ingest//", "/api/users/%2F"]


def test_an_admin_list_is_not_reached_by_another_spelling_of_its_path(srv):
    """QV-451: the level was checked on the exact path and the handler matched a normalised one, so a member
    read /api/users/ and /api/tokens/ingest/. Now one route decision serves both: any other spelling is no
    route, for anyone."""
    for path in ODD_ADMIN_PATHS:
        st, _, out = srv("GET", path, headers=srv.member)
        assert st in (403, 404), (path, st)
        assert "ben@example.com" not in str(out), path
        assert srv("GET", path, headers=srv.admin)[0] == 404, path
    # Python's http.server makes a leading // one / before the path reaches us: that is /api/users itself
    assert srv("GET", "//api/users", headers=srv.member)[0] == 403
    assert srv("GET", "/api/users", headers=srv.admin)[0] == 200
    assert srv("GET", "/api/tokens/ingest", headers=srv.admin)[0] == 200
    assert srv("GET", "/api/me/tokens", headers=srv.member)[0] == 200
    assert srv("GET", "/api/me/tokens/", headers=srv.member)[0] == 404


def test_the_api_dispatches_on_the_route_it_is_given_and_never_normalises(srv):
    app = srv.app
    for path in ("users/", "/users", "users//", "tokens/ingest/", "/tokens/ingest"):
        with pytest.raises(ApiError) as e:
            app.api(path, {}, access_mod.Who("personal_token", "admin", srv.ada))
        assert e.value.status == 404, path


def test_before_an_admin_exists_no_admin_route_answers(store):
    httpd = make_server(store, "127.0.0.1", 0, ui_dir=None, auth=auth.Auth(TOKEN))
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    url = f"http://127.0.0.1:{httpd.server_address[1]}"
    try:
        bearer = {"Authorization": f"Bearer {TOKEN}"}
        assert request(url, "GET", "/api/users", None, bearer)[0] == 403
        assert request(url, "POST", "/api/users", b'{"email": "e@example.com", "role": "admin"}',
                       {**J, **bearer})[0] == 403
        # personal tokens belong to a person: the shared token is none
        status, _, body = request(url, "GET", "/api/me/tokens", None, bearer)
        assert status == 403 and b"belong to a person" in body
        assert Accounts(store).users() == []
    finally:
        httpd.shutdown()
        httpd.server_close()


def test_the_people_page_is_for_admins(srv, tmp_path):
    """/people: the shell for an admin (the HTML fallback without the build), "Not allowed" for a member, the
    sign-in page for nobody; the page's data (/api/users) is ADMIN too."""
    status, _, body = srv("GET", "/people", headers=srv.admin)
    assert status == 200 and b"<h1>People</h1>" in body and b"trailbeam users" in body  # not built: says what to use
    (tmp_path / "index.html").write_text('<!doctype html><div id="root"></div>')
    srv.app.ui_dir = tmp_path
    assert srv("GET", "/people", headers=srv.admin)[:3:2] == (200, b'<!doctype html><div id="root"></div>')
    status, _, body = srv("GET", "/people", headers=srv.member)
    assert status == 403 and b"Not allowed" in body and b"root" not in body
    status, _, body = srv("GET", "/people")
    assert b"root" not in body and b"Sign in" in body, body[:300]
    status, _, body = srv("GET", "/people", headers={"Authorization": f"Bearer {TOKEN}"})
    assert b"root" not in body, "the shared token reads no admin page"


def test_the_tokens_page_is_for_anyone_signed_in(srv, tmp_path):
    """/tokens: the shell for an admin and for a member (it shows ingest tokens only to an admin, whose data,
    /api/tokens/ingest, stays ADMIN), the HTML fallback without the build, the sign-in page for nobody."""
    status, _, body = srv("GET", "/tokens", headers=srv.member)
    assert status == 200 and b"<h1>Tokens</h1>" in body and b"trailbeam tokens" in body  # not built: says what to use
    (tmp_path / "index.html").write_text('<!doctype html><div id="root"></div>')
    srv.app.ui_dir = tmp_path
    for who in (srv.admin, srv.member):
        assert srv("GET", "/tokens", headers=who)[:3:2] == (200, b'<!doctype html><div id="root"></div>')
    assert srv("GET", "/api/tokens/ingest", headers=srv.member)[0] == 403
    assert srv("GET", "/api/me/tokens", headers=srv.member)[0] == 200
    status, _, body = srv("GET", "/tokens")
    assert b"root" not in body and b"Sign in" in body, body[:300]
    status, _, body = srv("GET", "/tokens", headers={"Authorization": f"Bearer {TOKEN}"})
    assert b"root" not in body, "the shared token reads no page"


def test_writes_from_another_site_s_page_are_refused_before_anything_changes(srv):
    for site in ("cross-site", "same-site"):
        st, _, out = srv("PATCH", f"/api/users/{srv.ben}", {"role": "admin"}, {**srv.admin, "Sec-Fetch-Site": site})
        assert st == 403 and out["reason"] == "csrf"
        assert srv("DELETE", f"/api/users/{srv.ben}", None, {**srv.admin, "Sec-Fetch-Site": site})[0] == 403
        assert srv("POST", "/api/users", {"email": "e@example.com"}, {**srv.admin, "Sec-Fetch-Site": site})[0] == 403
    assert srv("POST", "/api/users", {"email": "e@example.com"}, {**srv.admin, "Origin": "https://evil.example"}
               )[0] == 403
    assert srv.acc.user(srv.ben)["role"] == "member" and len(srv.acc.users()) == 2
    # the server's own page passes
    assert srv("PATCH", f"/api/users/{srv.ben}", {"role": "admin"}, {**srv.admin, "Sec-Fetch-Site": "same-origin"}
               )[0] == 200


def test_over_https_a_plain_session_cookie_changes_no_one(store):
    """Writes check the scheme like reads do: over HTTPS only the __Host- cookie is a session (QV-408, QV-453)."""
    s = Server(store, TOKEN, trusted_proxies=TrustedProxies(parse_networks("127.0.0.1/32")))
    try:
        sid, _ = s.app.sessions.start(s.ada)
        https = {"X-Forwarded-Proto": "https", "Sec-Fetch-Site": "same-origin"}
        plain, host = {"Cookie": f"trailbeam_sid={sid}"}, {"Cookie": f"__Host-trailbeam_sid={sid}"}
        assert s("PATCH", f"/api/users/{s.ben}", {"role": "admin"}, {**https, **plain})[0] == 401
        assert s("POST", "/api/users", {"email": "e@example.com"}, {**https, **plain})[0] == 401
        assert s("POST", "/api/tokens/ingest", {"name": "x"}, {**https, **plain})[0] == 401
        assert s.acc.user(s.ben)["role"] == "member" and len(s.acc.users()) == 2
        assert s("PATCH", f"/api/users/{s.ben}", {"role": "admin"}, {**https, **host})[0] == 200
        assert s("PATCH", f"/api/users/{s.ben}", {"role": "member"}, {"Sec-Fetch-Site": "same-origin", **plain}
                 )[0] == 200  # plain HTTP
    finally:
        s.close()


def test_a_form_can_t_post_to_the_api(srv):
    """A plain HTML form (on any site) can only send form, multipart or text bodies: all refused."""
    for ctype in ("application/x-www-form-urlencoded", "multipart/form-data; boundary=x", "text/plain"):
        st, _, out = srv("POST", "/api/users", raw=b'{"email": "e@example.com"}',
                         headers={**srv.admin, "Content-Type": ctype})
        assert st == 415, ctype
    assert len(srv.acc.users()) == 2


# ---- people --------------------------------------------------------------------------------------------------

def test_adding_a_person_gives_an_invite_link_that_works_once(srv):
    st, headers, out = srv("POST", "/api/users", {"email": "cy@example.com", "name": "Cy"}, srv.admin)
    assert st == 201 and headers["Cache-Control"] == "no-store"
    assert out["user"]["role"] == "member" and out["user"]["status"] == "invited"
    link = out["link"]
    assert link["kind"] == "invite" and link["url"].startswith(srv.url + "/set-password#code=")
    assert link["expires_at_ms"] - _now() > 6 * 86_400_000
    # only the hash is stored
    code = _code(link["url"])
    assert code not in json.dumps(srv.acc._read("SELECT * FROM setup_codes"))
    # the page, and the code used
    st, headers, page = request(srv.url, "GET", "/set-password")
    assert st == 200 and headers["Referrer-Policy"] == "no-referrer" and b"location.hash" in page
    st, headers, _ = set_password(srv, code)
    assert st == 303 and headers["Location"] == "/" and headers["Set-Cookie"].startswith("trailbeam_sid=")
    cookie = {"Cookie": headers["Set-Cookie"].split(";")[0]}
    st, _, me = srv("GET", "/api/me", headers=cookie)
    assert me["user"]["email"] == "cy@example.com" and me["user"]["role"] == "member"
    users = {u["email"]: u for u in srv("GET", "/api/users", headers=srv.admin)[2]["users"]}
    assert users["cy@example.com"]["status"] == "active" and users["ada@example.com"]["you"] is True
    # twice: refused, and the password stays
    st, _, page = set_password(srv, code, pw="another password here")
    assert st == 403 and b"has been used" in page
    assert srv.app.signin.hasher.verify(PW, srv.acc.user_by_email("cy@example.com")["password_hash"])


def test_a_new_link_replaces_the_old_one(srv):
    out = srv("POST", "/api/users", {"email": "cy@example.com"}, srv.admin)[2]
    first, uid = _code(out["link"]["url"]), out["user"]["id"]
    second = _code(srv("POST", f"/api/users/{uid}/link", {}, srv.admin)[2]["link"]["url"])
    assert set_password(srv, first)[0] == 403
    assert set_password(srv, second)[0] == 303


def test_a_reset_link_sets_a_new_password_and_ends_the_other_sessions(srv):
    sid, _ = srv.app.sessions.start(srv.ben)
    ben = {"Cookie": f"trailbeam_sid={sid}"}
    assert srv("GET", "/api/me", headers=ben)[0] == 200
    st, _, out = srv("POST", f"/api/users/{srv.ben}/link", {}, srv.admin)
    assert st == 201 and out["link"]["kind"] == "reset"
    assert 23 * 3_600_000 < out["link"]["expires_at_ms"] - _now() <= 86_400_000
    # making the link changes nothing yet
    assert srv("GET", "/api/me", headers=ben)[0] == 200
    assert set_password(srv, _code(out["link"]["url"]), pw="a brand new password")[0] == 303
    assert srv("GET", "/api/me", headers=ben)[0] == 401
    assert srv.app.signin.hasher.verify("a brand new password", srv.acc.user(srv.ben)["password_hash"])


def test_set_password_refuses_bad_codes_mismatches_and_short_passwords(srv):
    code = _code(srv("POST", f"/api/users/{srv.ben}/link", {}, srv.admin)[2]["link"]["url"])
    assert set_password(srv, "not-a-code-at-all-but-long-enough")[0] == 403
    assert set_password(srv, code, pw2="something else entirely")[0] == 400
    assert set_password(srv, code, pw="short")[0] == 400
    assert set_password(srv, code)[0] == 303  # still unused after those


def test_a_disabled_person_s_link_stops_working(srv):
    code = _code(srv("POST", f"/api/users/{srv.ben}/link", {}, srv.admin)[2]["link"]["url"])
    assert srv("PATCH", f"/api/users/{srv.ben}", {"disabled": True}, srv.admin)[2]["user"]["status"] == "disabled"
    assert set_password(srv, code)[0] == 403
    st, _, out = srv("POST", f"/api/users/{srv.ben}/link", {}, srv.admin)
    assert st == 409 and out["reason"] == "disabled"
    # disabled: their personal token stops working on this replica at once
    assert srv("GET", "/api/me", headers=srv.member)[0] == 401
    assert srv("PATCH", f"/api/users/{srv.ben}", {"disabled": False}, srv.admin)[0] == 200
    assert srv("GET", "/api/me", headers=srv.member)[0] == 200


def test_an_expired_link_is_refused(srv):
    out = srv("POST", "/api/users", {"email": "cy@example.com"}, srv.admin)[2]
    code = _code(out["link"]["url"])
    with pytest.raises(BadSetupCode):
        srv.acc.use_user_code(people_api.first_admin.code_hash(code), "x", _now() + people_api.INVITE_MS + 1)
    assert srv.acc.user_code(people_api.first_admin.code_hash(code), _now() + people_api.INVITE_MS + 1) is None


def test_a_role_change_reaches_the_person_s_token_at_once_on_this_replica(srv):
    assert srv("GET", "/api/users", headers=srv.member)[0] == 403
    st, _, out = srv("PATCH", f"/api/users/{srv.ben}", {"role": "admin"}, srv.admin)
    assert st == 200 and out["user"]["role"] == "admin"
    assert srv("GET", "/api/users", headers=srv.member)[0] == 200


def test_the_last_admin_can_t_be_demoted_disabled_or_removed(srv):
    for change in ({"role": "member"}, {"disabled": True}):
        st, _, out = srv("PATCH", f"/api/users/{srv.ada}", change, srv.admin)
        assert st == 409 and out["reason"] == "last_admin", change
    st, _, out = srv("DELETE", f"/api/users/{srv.ada}", None, srv.admin)
    assert st == 409 and out["reason"] == "last_admin"
    assert srv.acc.user(srv.ada)["role"] == "admin" and srv.acc.user(srv.ada)["disabled_at_ms"] is None
    # with a second admin, the first can step down
    assert srv("PATCH", f"/api/users/{srv.ben}", {"role": "admin"}, srv.admin)[0] == 200
    assert srv("PATCH", f"/api/users/{srv.ada}", {"role": "member"}, srv.member)[0] == 200
    st, _, out = srv("DELETE", f"/api/users/{srv.ben}", None, srv.member)  # Ben is now the last one
    assert st == 409


def test_no_viewer_role(srv):
    st, _, out = srv("POST", "/api/users", {"email": "v@example.com", "role": "viewer"}, srv.admin)
    assert st == 400 and "admin, member" in out["error"]
    assert srv("PATCH", f"/api/users/{srv.ben}", {"role": "viewer"}, srv.admin)[0] == 400


def test_bad_bodies(srv):
    assert srv("POST", "/api/users", {}, srv.admin)[0] == 400
    assert srv("POST", "/api/users", {"email": 3}, srv.admin)[0] == 400
    assert srv("POST", "/api/users", raw=b"{not json", headers=srv.admin)[0] == 400
    assert srv("POST", "/api/users", raw=b"[]", headers=srv.admin)[0] == 400
    assert srv("PATCH", f"/api/users/{srv.ben}", {}, srv.admin)[0] == 400
    assert srv("PATCH", f"/api/users/{srv.ben}", {"disabled": "yes"}, srv.admin)[0] == 400
    st, _, out = srv("POST", "/api/users", {"email": "BEN@example.com"}, srv.admin)
    assert st == 409 and out["reason"] == "email_taken"
    st, _, _ = request(srv.url, "POST", "/api/users", None, {**srv.admin, **J, "Content-Length": "20000"})
    assert st == 413
    assert srv("PATCH", "/api/users/abc", {"role": "admin"}, srv.admin)[0] == 404
    assert srv("PATCH", "/api/users/999", {"role": "admin"}, srv.admin)[0] == 404


def test_removing_a_person_takes_their_tokens_and_links(srv):
    code = _code(srv("POST", f"/api/users/{srv.ben}/link", {}, srv.admin)[2]["link"]["url"])
    st, _, out = srv("DELETE", f"/api/users/{srv.ben}", None, srv.admin)
    assert st == 200 and out["removed"] == {"id": srv.ben, "email": "ben@example.com"}
    assert srv("GET", "/api/me", headers=srv.member)[0] == 401
    assert set_password(srv, code)[0] == 403
    assert srv("DELETE", f"/api/users/{srv.ben}", None, srv.admin)[0] == 404


# ---- tokens --------------------------------------------------------------------------------------------------

def test_ingest_tokens_are_shown_once_and_listed_without_the_secret(srv):
    st, headers, out = srv("POST", "/api/tokens/ingest", {"name": "orders-worker"}, srv.admin)
    assert st == 201 and headers["Cache-Control"] == "no-store"
    secret = out["secret"]
    assert secret.startswith("tbi_") and out["token"]["prefix"] == secret[:8]
    assert out["token"]["created_by"] == "ada@example.com"
    st, _, listed = srv("GET", "/api/tokens/ingest", headers=srv.admin)
    assert secret not in json.dumps(listed)
    assert listed["shared_token"] == {"set": True, "label": "legacy shared token (from TRAILBEAM_TOKEN)",
                                      "works_for": ["ingest", "metrics"]}
    assert TOKEN not in json.dumps(listed)
    # it sends spans, and reads nothing
    assert request(srv.url, "GET", "/api/messages", None, {"Authorization": f"Bearer {secret}"})[0] == 401
    tid = out["token"]["id"]
    assert srv("DELETE", f"/api/tokens/ingest/{tid}", None, srv.admin)[0] == 200
    assert srv("DELETE", f"/api/tokens/ingest/{tid}", None, srv.admin)[0] == 404  # already revoked
    assert [t["status"] for t in srv("GET", "/api/tokens/ingest", headers=srv.admin)[2]["tokens"]] == ["revoked"]
    assert srv.app.tokens.ingest(secret) is None


def test_the_first_ingest_token_of_an_open_server_needs_confirming(open_srv):
    s = open_srv
    st, _, out = s("GET", "/api/tokens/ingest", headers=s.admin)
    assert out["ingest"] == "open" and out["shared_token"]["set"] is False
    st, _, out = s("POST", "/api/tokens/ingest", {"name": "first"}, s.admin)
    assert st == 409 and out["reason"] == "closes_open_ingest"
    assert s.acc.ingest_tokens() == []
    assert s("POST", "/api/tokens/ingest", {"name": "first", "close_open_ingest": True}, s.admin)[0] == 201
    assert s("GET", "/api/tokens/ingest", headers=s.admin)[2]["ingest"] == "token"
    assert s("POST", "/api/tokens/ingest", {"name": "second"}, s.admin)[0] == 201  # closed already


def test_personal_tokens_are_your_own(srv):
    st, _, out = srv("POST", "/api/me/tokens", {"name": "laptop", "expires_in_days": 30}, srv.member)
    assert st == 201 and out["secret"].startswith("tbp_")
    assert 29 * 86_400_000 < out["token"]["expires_at_ms"] - _now() <= 30 * 86_400_000
    tid = out["token"]["id"]
    new = {"Authorization": "Bearer " + out["secret"]}
    assert srv("GET", "/api/me", headers=new)[2]["user"]["email"] == "ben@example.com"
    mine = srv("GET", "/api/me/tokens", headers=srv.member)[2]["tokens"]
    assert [t["name"] for t in mine] == ["ben", "laptop"] and out["secret"] not in json.dumps(mine)
    assert [t["name"] for t in srv("GET", "/api/me/tokens", headers=srv.admin)[2]["tokens"]] == ["ada"]
    # an admin can't revoke it through /api/me/tokens: it isn't theirs
    assert srv("DELETE", f"/api/me/tokens/{tid}", None, srv.admin)[0] == 404
    assert srv("GET", "/api/me", headers=new)[0] == 200
    assert srv("DELETE", f"/api/me/tokens/{tid}", None, srv.member)[0] == 200
    assert srv("GET", "/api/me", headers=new)[0] == 401
    for days in (0, 3651, 1.5, True, "30"):
        assert srv("POST", "/api/me/tokens", {"name": "x", "expires_in_days": days}, srv.member)[0] == 400, days
    assert srv("POST", "/api/me/tokens", {"name": "forever", "expires_in_days": None}, srv.member)[0] == 201
