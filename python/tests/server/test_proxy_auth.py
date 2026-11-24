"""Sign-in through a proxy (proxy_auth.py, QV-227): the email header of oauth2-proxy, Pomerium, Cloudflare Access
and similar, believed only from the proxy's addresses or with its shared secret; first-time people become members,
and a local account can't be taken over unless an admin links it. With attempts to get round each rule.

Runs on SQLite (file and ``:memory:``), and on Postgres when TRAILBEAM_TEST_POSTGRES_URL names a database."""
import http.client
import json
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

from trailbeam.server import __main__ as main_mod, cli, passwords, proxy_auth
from trailbeam.server.access import ADMIN, MEMBER, Access
from trailbeam.server.accounts import Accounts
from trailbeam.server.proxy import TrustedProxies, parse_networks
from trailbeam.server.proxy_auth import SECRET_HEADER, ProxyAuthError, ProxySignIn, configure
from trailbeam.server.tokens import PERSONAL_PREFIX

from test_access import NOW, PW, QUICK, add_user, call, sign_in, store  # noqa: F401 - store is a fixture

EMAIL = "X-Forwarded-Email"
SECRET = "0123456789abcdef0123456789abcdef"
LOCAL = TrustedProxies(parse_networks("127.0.0.1"))  # the tests' requests come from 127.0.0.1
ELSEWHERE = TrustedProxies(parse_networks("10.0.0.0/8"))


@pytest.fixture
def serve(store):
    """``serve(proxies=LOCAL, admin=True, **configure_kw)``: the server with proxy sign-in on; (base URL, App).
    Caches are off, so each test sees its own changes at once."""
    from trailbeam.server.server import make_server
    started = []

    def start(proxies=LOCAL, admin=True, **kw):
        cfg = configure(kw.pop("header", EMAIL), proxies=proxies, **kw)
        httpd = make_server(store, "127.0.0.1", 0, ui_dir=None, trusted_proxies=proxies, proxy_auth=cfg)
        app = httpd.app
        app.access.state.ttl_s = 0.0
        app.signin.hasher = QUICK
        if app.proxy_signin is not None:
            app.proxy_signin.cache_ttl_s = 0.0
        if admin:
            add_user(app, "root@example.com", "admin")
        threading.Thread(target=httpd.serve_forever, daemon=True).start()
        started.append(httpd)
        return f"http://127.0.0.1:{httpd.server_address[1]}", app

    yield start
    for httpd in started:
        httpd.shutdown()
        httpd.server_close()


def me(url, **headers):
    status, _, body = call(url, "/api/me", headers=headers)
    return status, json.loads(body)


def raw_get(url, path, headers):
    """GET with headers as (name, value) pairs, so one can be sent twice."""
    host, port = url.split("//")[1].split(":")
    c = http.client.HTTPConnection(host, int(port), timeout=10)
    c.putrequest("GET", path)
    for k, v in headers:
        c.putheader(k, v)
    c.endheaders()
    r = c.getresponse()
    out = r.status, r.read()
    c.close()
    return out


# ---- the settings ---------------------------------------------------------------------------------------------

def test_off_by_default_and_its_other_settings_need_it():
    assert configure(None) is None and configure("  ") is None
    for kw in ({"name_header": "X-Forwarded-User"}, {"secret": SECRET}, {"sign_out_url": "/oauth2/sign_out"}):
        with pytest.raises(ProxyAuthError, match="needs --proxy-auth-header"):
            configure(None, **kw)


def test_the_header_is_believed_only_with_addresses_or_a_secret():
    with pytest.raises(ProxyAuthError, match="--trusted-proxy.*or a shared secret"):
        configure(EMAIL)
    assert configure(EMAIL, proxies=LOCAL).by_address
    c = configure(EMAIL, secret=SECRET)
    assert not c.by_address and c.secret == SECRET
    with pytest.raises(ProxyAuthError, match="at least 16"):
        configure(EMAIL, secret="short")


@pytest.mark.parametrize("header", ["Authorization", "cookie", "X-Trailbeam-Key", SECRET_HEADER, "Host",
                                    "X-Forwarded-For", "bad header", "X-Email:"])
def test_a_header_that_means_something_else_cant_carry_the_email(header):
    with pytest.raises(ProxyAuthError):
        configure(header, proxies=LOCAL)


def test_the_name_header_and_the_sign_out_url_are_checked():
    with pytest.raises(ProxyAuthError, match="another header"):
        configure(EMAIL, name_header="x-forwarded-email", proxies=LOCAL)
    for bad in ("//evil.example/out", "javascript:alert(1)", "oauth2/sign_out", "/out\r\nSet-Cookie: x=1"):
        with pytest.raises(ProxyAuthError):
            configure(EMAIL, sign_out_url=bad, proxies=LOCAL)
    assert configure(EMAIL, sign_out_url="https://auth.example.com/oauth2/sign_out", proxies=LOCAL).sign_out_url


def test_the_command_reads_the_settings_from_flags_files_and_environment(tmp_path, monkeypatch):
    for n in ("TRAILBEAM_PROXY_AUTH_HEADER", "TRAILBEAM_PROXY_AUTH_SECRET", "TRAILBEAM_PROXY_AUTH_SECRET_FILE",
              "TRAILBEAM_TRUSTED_PROXY", "TRAILBEAM_PROXY_AUTH_NAME_HEADER", "TRAILBEAM_PROXY_AUTH_SIGN_OUT_URL"):
        monkeypatch.delenv(n, raising=False)
    assert main_mod.proxy_auth_config(main_mod.build_parser().parse_args([])) is None
    f = tmp_path / "secret"
    f.write_text(SECRET + "\n")
    c = main_mod.proxy_auth_config(main_mod.build_parser().parse_args(
        ["--proxy-auth-header", EMAIL, "--proxy-auth-secret-file", str(f)]))
    assert c.secret == SECRET and not c.by_address
    monkeypatch.setenv("TRAILBEAM_PROXY_AUTH_HEADER", "X-Auth-Request-Email")
    monkeypatch.setenv("TRAILBEAM_TRUSTED_PROXY", "10.1.0.0/16")
    monkeypatch.setenv("TRAILBEAM_PROXY_AUTH_SIGN_OUT_URL", "/oauth2/sign_out")
    c = main_mod.proxy_auth_config(main_mod.build_parser().parse_args([]))
    assert c.header == "X-Auth-Request-Email" and c.by_address and c.secret is None
    monkeypatch.delenv("TRAILBEAM_TRUSTED_PROXY")
    with pytest.raises(ProxyAuthError, match="needs the proxy's addresses"):
        main_mod.proxy_auth_config(main_mod.build_parser().parse_args([]))
    with pytest.raises(ProxyAuthError, match="secret-file"):
        main_mod.proxy_auth_config(main_mod.build_parser().parse_args(
            ["--proxy-auth-secret-file", str(tmp_path / "missing")]))


# ---- who the proxy signs in -----------------------------------------------------------------------------------

def test_a_first_time_email_becomes_a_member_with_no_password(serve):
    url, app = serve(name_header="X-Forwarded-Preferred-Username")
    status, m = me(url, **{EMAIL: "bo@example.com", "X-Forwarded-Preferred-Username": "Bo Bell"})
    assert status == 200 and m["via"] == "proxy" and m["auth"] == "accounts" and m["sign_out"] is False
    assert m["user"]["email"] == "bo@example.com" and m["user"]["name"] == "Bo Bell" and m["user"]["role"] == "member"
    u = app.accounts.user_by_email("bo@example.com")
    assert u["password_hash"] is None and u["proxy_linked_at_ms"] is not None
    # the same person again: no second account; the header's case doesn't matter
    status, m2 = me(url, **{EMAIL.lower(): "BO@example.com"})
    assert status == 200 and m2["user"]["id"] == m["user"]["id"]
    assert len(app.accounts.users()) == 2
    # Google IAP's form of the header: the same person
    assert me(url, **{EMAIL: "accounts.google.com:bo@example.com"})[1]["user"]["id"] == m["user"]["id"]
    # and they read pages and the API
    assert call(url, "/api/messages", headers={EMAIL: "bo@example.com"})[0] == 200
    assert call(url, "/tenants", headers={EMAIL: "bo@example.com"})[0] == 200
    # a proxy-made person has no password, so the sign-in form never lets anyone in as them
    assert sign_in(url, "bo@example.com", "")[0] is None


def test_a_first_time_email_is_never_an_admin(serve):
    url, app = serve()
    # nothing the proxy (or a client through it) sends makes an admin
    status, m = me(url, **{EMAIL: "eve@example.com", "X-Forwarded-Groups": "admin", "X-Role": "admin"})
    assert m["user"]["role"] == "member"
    who = app.access.check({EMAIL: "eve@example.com"}, ADMIN, peer="127.0.0.1")
    assert not who.ok and who.status == 403
    # an admin can make them one; then the proxy signs them in as an admin
    app.accounts.set_role(m["user"]["id"], "admin", NOW)
    assert app.access.check({EMAIL: "eve@example.com"}, ADMIN, peer="127.0.0.1").ok


def test_the_header_from_anyone_but_the_proxy_is_ignored(serve):
    url, app = serve(proxies=ELSEWHERE)  # the proxy is at 10.x; the test's requests come from 127.0.0.1
    status, m = me(url, **{EMAIL: "root@example.com"})
    assert status == 401
    assert app.accounts.user_by_email("mallory@example.com") is None
    me(url, **{EMAIL: "mallory@example.com"})
    assert app.accounts.user_by_email("mallory@example.com") is None  # nobody created either
    # a session still works alongside it, and the ignored header doesn't change who the session is
    cookie = sign_in(url, "root@example.com")[0]
    status, m = me(url, Cookie=cookie, **{EMAIL: "other@example.com"})
    assert status == 200 and m["via"] == "session" and m["user"]["email"] == "root@example.com"


def test_with_a_shared_secret_only_requests_carrying_it_are_believed(serve):
    url, app = serve(proxies=TrustedProxies(), secret=SECRET)
    assert me(url, **{EMAIL: "bo@example.com"})[0] == 401
    assert me(url, **{EMAIL: "bo@example.com", SECRET_HEADER: SECRET[:-1] + "0"})[0] == 401
    assert me(url, **{EMAIL: "bo@example.com", SECRET_HEADER: SECRET + "x"})[0] == 401
    assert app.accounts.user_by_email("bo@example.com") is None
    status, m = me(url, **{EMAIL: "bo@example.com", SECRET_HEADER: SECRET})
    assert status == 200 and m["via"] == "proxy"
    # the secret sent twice (a client's copy and the proxy's) is not believed
    assert raw_get(url, "/api/me", [(EMAIL, "bo@example.com"), (SECRET_HEADER, SECRET),
                                    (SECRET_HEADER, SECRET)])[0] == 401


def test_with_addresses_and_a_secret_both_must_pass(serve):
    url, _ = serve(proxies=LOCAL, secret=SECRET)
    assert me(url, **{EMAIL: "bo@example.com"})[0] == 401
    assert me(url, **{EMAIL: "bo@example.com", SECRET_HEADER: SECRET})[0] == 200
    url2, _ = serve(proxies=ELSEWHERE, secret=SECRET, admin=False)
    assert me(url2, **{EMAIL: "bo@example.com", SECRET_HEADER: SECRET})[0] == 401


def test_a_local_account_is_not_taken_over_until_an_admin_links_it(serve):
    url, app = serve()
    # an admin's account and a member's: the proxy names their emails (any case), and is refused
    for email in ("root@example.com", "ROOT@Example.COM"):
        status, body = call(url, "/api/me", headers={EMAIL: email})[::2]
        assert status == 403 and b"link-proxy" in body
    assert call(url, "/tenants", headers={EMAIL: "root@example.com"})[0] == 403
    # a refused proxy sign-in doesn't fall back to a session sent with it
    cookie = sign_in(url, "root@example.com")[0]
    assert me(url, Cookie=cookie, **{EMAIL: "root@example.com"})[0] == 403
    assert me(url, Cookie=cookie)[0] == 200  # the session itself is fine
    # linked by an admin: the proxy signs them in, as their role; their password still works
    root = app.accounts.user_by_email("root@example.com")
    assert app.accounts.set_proxy_linked(root["id"], True, NOW)
    status, m = me(url, **{EMAIL: "root@example.com"})
    assert status == 200 and m["user"]["role"] == "admin" and m["via"] == "proxy"
    assert me(url, Cookie=cookie)[0] == 200 and sign_in(url, "root@example.com")[0] is not None
    # unlinked again: refused again
    app.accounts.set_proxy_linked(root["id"], False, NOW)
    assert me(url, **{EMAIL: "root@example.com"})[0] == 403


def test_an_admin_signed_in_through_the_proxy_manages_people_and_tokens(serve):
    """The people and token routes (people_api.py) take the proxy's header on writes too, not only on reads."""
    url, app = serve()
    m = me(url, **{EMAIL: "eve@example.com"})[1]
    app.accounts.set_role(m["user"]["id"], "admin", NOW)
    j = {EMAIL: "eve@example.com", "Content-Type": "application/json"}
    assert call(url, "/api/users", headers={EMAIL: "eve@example.com"})[0] == 200
    status, _, body = call(url, "/api/users", method="POST", headers=j,
                           data=json.dumps({"email": "fay@example.com", "name": "Fay", "role": "member"}).encode())
    assert status == 201, body
    fay = json.loads(body)["user"]["id"]
    assert call(url, f"/api/users/{fay}", method="PATCH", headers=j, data=b'{"role": "admin"}')[0] == 200
    assert call(url, "/api/me/tokens", method="POST", headers=j, data=b'{"name": "laptop"}')[0] == 201
    # a member through the proxy is still refused the admin routes
    me(url, **{EMAIL: "gus@example.com"})
    g = {EMAIL: "gus@example.com", "Content-Type": "application/json"}
    assert call(url, "/api/users", method="POST", headers=g,
                data=b'{"email": "h@example.com", "name": "H", "role": "member"}')[0] == 403
    # and the header from anyone but the proxy is ignored on writes as well (no session: 401)
    url2, _ = serve(proxies=ELSEWHERE, admin=False)
    assert call(url2, "/api/me/tokens", method="POST", headers=j, data=b'{"name": "x"}')[0] == 401


def test_a_disabled_person_is_refused(serve):
    url, app = serve()
    status, m = me(url, **{EMAIL: "bo@example.com"})
    app.accounts.set_disabled(m["user"]["id"], True, NOW)
    status, body = call(url, "/api/me", headers={EMAIL: "bo@example.com"})[::2]
    assert status == 403 and b"disabled" in body


def test_two_emails_or_one_that_isnt_an_email_are_refused(serve):
    url, app = serve()
    assert raw_get(url, "/api/me", [(EMAIL, "bo@example.com"), (EMAIL, "root@example.com")])[0] == 403
    for bad in ("not-an-email", "bo@example.com, root@example.com", "a" * 300 + "@example.com"):
        assert me(url, **{EMAIL: bad})[0] == 403
    assert [u["email"] for u in app.accounts.users()] == ["root@example.com"]


def test_nothing_changes_before_an_admin_exists(serve):
    url, app = serve(admin=False)
    status, m = me(url, **{EMAIL: "bo@example.com"})
    assert status == 200 and m["via"] == "none" and m["user"] is None
    assert app.accounts.users() == []


def test_a_personal_token_is_who_it_says_and_ingest_never_takes_the_header(serve):
    url, app = serve()
    bo = add_user(app, "bo@example.com", "member")
    _, tok = app.tokens.create_personal(bo, "cli")
    assert tok.startswith(PERSONAL_PREFIX)
    status, m = me(url, Authorization=f"Bearer {tok}", **{EMAIL: "root@example.com"})
    assert status == 200 and m["via"] == "personal_token" and m["user"]["email"] == "bo@example.com"
    # an ingest token exists, so ingest is closed: the proxy's header doesn't open it
    app.tokens.create_ingest("svc")
    app.access.state.changed()
    status = call(url, "/v1/traces", method="POST", data=b"", headers={
        "Content-Type": "application/x-protobuf", EMAIL: "root@example.com"})[0]
    assert status == 401


def test_sign_out_goes_to_the_proxys_sign_out_and_the_sign_in_page_sends_them_on(serve):
    url, _ = serve(sign_out_url="/oauth2/sign_out")
    h = {EMAIL: "bo@example.com"}
    assert me(url, **h)[1]["sign_out"] is True
    status, headers, _ = call(url, "/logout", headers=h)
    assert status == 303 and headers["Location"] == "/oauth2/sign_out"
    status, headers, _ = call(url, "/login?next=/tenants", headers=h)
    assert status == 303 and headers["Location"] == "/tenants"
    status, headers, _ = call(url, "/login?next=https://evil.example/", headers=h)
    assert status == 303 and headers["Location"] == "/"
    # without the proxy's header, the sign-in form as usual
    assert call(url, "/login")[0] == 200


def test_the_store_down_is_an_error_not_a_sign_in(store):
    class Down(Accounts):
        def user_by_email(self, email):
            raise RuntimeError("store down")
    acc = Down(store)
    acc.create_user("root@example.com", "Root", "admin", QUICK.hash(PW), NOW)
    p = ProxySignIn(configure(EMAIL, proxies=LOCAL), acc, LOCAL)
    a = Access(accounts=acc, proxy=p)
    from trailbeam.server.access import AccessUnavailable
    with pytest.raises(AccessUnavailable):
        a.check({EMAIL: "bo@example.com"}, MEMBER, peer="127.0.0.1")


def test_many_first_requests_at_once_make_one_person(store):
    acc = Accounts(store)
    acc.create_user("root@example.com", "Root", "admin", QUICK.hash(PW), NOW)
    p = ProxySignIn(configure(EMAIL, proxies=LOCAL), acc, LOCAL)
    a = Access(accounts=acc, proxy=p)
    with ThreadPoolExecutor(8) as ex:
        out = list(ex.map(lambda _: a.check({EMAIL: "new@example.com"}, MEMBER, peer="127.0.0.1"), range(16)))
    assert all(d.ok for d in out) and len({d.who.user_id for d in out}) == 1
    assert len(acc.users()) == 2


def test_a_change_to_a_person_is_seen_once_the_cache_expires(store):
    acc = Accounts(store)
    acc.create_user("root@example.com", "Root", "admin", QUICK.hash(PW), NOW)
    t = [0.0]
    p = ProxySignIn(configure(EMAIL, proxies=LOCAL), acc, LOCAL, clock=lambda: t[0])
    a = Access(accounts=acc, proxy=p)
    d = a.check({EMAIL: "bo@example.com"}, MEMBER, peer="127.0.0.1")
    acc.set_disabled(d.who.user_id, True, NOW)
    assert a.check({EMAIL: "bo@example.com"}, MEMBER, peer="127.0.0.1").ok  # cached
    t[0] += proxy_auth.CACHE_TTL_S
    d = a.check({EMAIL: "bo@example.com"}, MEMBER, peer="127.0.0.1")
    assert not d.ok and d.status == 403


def test_without_a_peer_the_header_is_not_looked_at(store):
    acc = Accounts(store)
    acc.create_user("root@example.com", "Root", "admin", QUICK.hash(PW), NOW)
    a = Access(accounts=acc, proxy=ProxySignIn(configure(EMAIL, proxies=LOCAL), acc, LOCAL))
    assert a.check({EMAIL: "bo@example.com"}, MEMBER).status == 401


# ---- the command line ------------------------------------------------------------------------------------------

@pytest.fixture
def sqlite_target(tmp_path, monkeypatch):
    from trailbeam.server.db import Database
    real = passwords.Hasher
    monkeypatch.setattr(passwords, "Hasher", lambda *a, **k: real(log2_n=10, r=8, p=1))
    monkeypatch.setattr(cli, "_interactive", lambda: False)
    path = str(tmp_path / "trailbeam.sqlite")
    Database(path).close()
    return path


def _people(path):
    from trailbeam.server.db import Database
    s = Database(path)
    try:
        return {u["email"]: u for u in Accounts(s).users()}
    finally:
        s.close()


def test_link_and_unlink_and_create_for_the_proxy(sqlite_target, tmp_path, capsys, monkeypatch):
    monkeypatch.setenv("TRAILBEAM_ACCOUNTS", "1")  # the users commands refuse to run without it (QV-349)
    db = ["--db", sqlite_target]
    pw = tmp_path / "pw"
    pw.write_text(PW + "\n")
    assert cli.main(["users", "create", *db, "--email", "ada@example.com", "--role", "admin",
                     "--password-file", str(pw)]) == 0
    assert _people(sqlite_target)["ada@example.com"]["proxy_linked_at_ms"] is None
    assert cli.main(["users", "link-proxy", *db, "ADA@example.com"]) == 0
    assert "can now sign in through the proxy" in capsys.readouterr().err
    assert _people(sqlite_target)["ada@example.com"]["proxy_linked_at_ms"] is not None
    assert cli.main(["users", "link-proxy", *db, "ada@example.com"]) == 0
    assert "already" in capsys.readouterr().err
    # a person only for the proxy: no password asked for
    assert cli.main(["users", "create", *db, "--email", "bo@example.com", "--proxy"]) == 0
    assert "no password" in capsys.readouterr().err
    bo = _people(sqlite_target)["bo@example.com"]
    assert bo["password_hash"] is None and bo["proxy_linked_at_ms"] is not None and bo["role"] == "member"
    assert cli.main(["users", "list", *db, "--json"]) == 0
    listed = {u["email"]: u for u in json.loads(capsys.readouterr().out)}
    assert listed["ada@example.com"]["proxy"] and listed["bo@example.com"]["proxy"]
    assert cli.main(["users", "unlink-proxy", *db, "bo@example.com"]) == 0
    assert "can't sign in at all now" in capsys.readouterr().err
    assert _people(sqlite_target)["bo@example.com"]["proxy_linked_at_ms"] is None
    assert cli.main(["users", "unlink-proxy", *db, "nobody@example.com"]) == 2


def test_the_server_refuses_proxy_sign_in_with_users_and_roles_off(tmp_path):
    """Proxy sign-in creates and signs in people, so with users and roles off (QV-349) the server won't start."""
    import os
    import subprocess
    import sys
    env = {k: v for k, v in os.environ.items() if not k.startswith(("TRAILBEAM_", "OTEL_"))}
    env.update(TRAILBEAM_PROXY_AUTH_HEADER="X-Forwarded-Email", TRAILBEAM_TRUSTED_PROXY="127.0.0.1")
    r = subprocess.run([sys.executable, "-m", "trailbeam.server", "--db", str(tmp_path / "t.sqlite"), "--port", "0"],
                       env=env, capture_output=True, text=True, timeout=60)
    assert r.returncode == 2
    assert "--proxy-auth-header (TRAILBEAM_PROXY_AUTH_HEADER) is set, but users and roles are off" in r.stderr
