"""The first admin (first_admin.py): the one-time setup link of trailbeam start and its /setup form,
TRAILBEAM_ADMIN_EMAIL with TRAILBEAM_ADMIN_PASSWORD_FILE, and the rule that the server always keeps an admin.
With attempts to break them: wrong, used, old and replaced codes, two first admins at once, forms from other
sites, and the link reaching a log.

The Postgres cases run when TRAILBEAM_TEST_POSTGRES_URL names a database, as in test_postgres.py."""
import concurrent.futures
import os
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

import pytest

from trailbeam.server import auth, first_admin
from trailbeam.server.accounts import AccountError, Accounts, AdminExists, BadSetupCode, EmailTaken, LastAdmin
from trailbeam.server.db import Database
from trailbeam.server.passwords import Hasher
from trailbeam.server.server import make_server

ROOT = Path(__file__).resolve().parents[2]
PG = os.environ.get("TRAILBEAM_TEST_POSTGRES_URL")
NOW = 1_800_000_000_000
PW = "correct horse battery"
QUICK = Hasher(log2_n=10, r=8, p=1)
FORM = {"Content-Type": "application/x-www-form-urlencoded"}


def _open(kind, tmp_path, memory=False):
    if kind == "sqlite":
        return Database(":memory:" if memory else str(tmp_path / "trailbeam.sqlite"))
    sys.path.insert(0, str(ROOT / "tests" / "conformance"))
    from pg_helpers import postgres_store
    return postgres_store()


@pytest.fixture(params=["sqlite", "sqlite-memory", pytest.param("postgres", marks=pytest.mark.skipif(
    not PG, reason="TRAILBEAM_TEST_POSTGRES_URL is not set"))])
def store(request, tmp_path):
    s = _open(request.param.split("-")[0], tmp_path, memory=request.param.endswith("memory"))
    yield s
    s.close()


@pytest.fixture(params=["sqlite", pytest.param("postgres", marks=pytest.mark.skipif(
    not PG, reason="TRAILBEAM_TEST_POSTGRES_URL is not set"))])
def shared_store(request, tmp_path):
    """A store several connections reach at once (not :memory:)."""
    s = _open(request.param, tmp_path)
    yield s
    s.close()


@pytest.fixture
def acc(store):
    return Accounts(store)


# ---- the store: codes and the first admin -------------------------------------------------------------------

def test_a_setup_code_works_once(acc):
    code = first_admin.issue_code(acc, NOW)
    h = first_admin.code_hash(code)
    assert acc.setup_code_ok(h, NOW)
    uid = acc.create_first_admin("ada@example.com", "Ada", QUICK.hash(PW), NOW + 1, setup_code_sha256=h)
    assert acc.user(uid)["role"] == "admin" and acc.auth_state() == (True, False)
    assert not acc.setup_code_ok(h, NOW + 2)
    with pytest.raises(AdminExists):  # an admin exists: that answer comes first, whatever the code
        acc.create_first_admin("eve@example.com", "Eve", QUICK.hash(PW), NOW + 2, setup_code_sha256=h)


def test_only_the_store_s_hash_of_the_code_is_kept(acc, store):
    code = first_admin.issue_code(acc, NOW)
    rows = acc._read("SELECT code_sha256, kind, expires_at_ms, used_at_ms FROM setup_codes")
    assert rows == [{"code_sha256": first_admin.code_hash(code), "kind": "setup",
                     "expires_at_ms": NOW + first_admin.LIFETIME_MS, "used_at_ms": None}]
    assert len(code) >= 43 and first_admin.well_formed(code)  # 32 random bytes


@pytest.mark.parametrize("case", ["wrong", "expired", "replaced", "invite_kind"])
def test_a_code_that_is_not_the_live_one_creates_nobody(acc, case):
    code = first_admin.issue_code(acc, NOW)
    when = NOW + 1
    if case == "wrong":
        code = first_admin.new_code()
    elif case == "expired":
        when = NOW + first_admin.LIFETIME_MS
    elif case == "replaced":  # a second trailbeam start: only its link works
        first_admin.issue_code(acc, NOW)
    elif case == "invite_kind":  # a phase-2 code of another kind can't make the first admin
        with acc._write() as tx:
            tx.run("UPDATE setup_codes SET kind = 'invite'")
    h = first_admin.code_hash(code)
    assert not acc.setup_code_ok(h, when)
    with pytest.raises(BadSetupCode):
        acc.create_first_admin("ada@example.com", "Ada", QUICK.hash(PW), when, setup_code_sha256=h)
    assert acc.users() == [] and acc.auth_state()[0] is False


def test_a_refused_first_admin_leaves_the_code_unused(acc):
    acc.create_user("ada@example.com", "Ada", "member", None, NOW)
    code = first_admin.issue_code(acc, NOW)
    h = first_admin.code_hash(code)
    with pytest.raises(EmailTaken):
        acc.create_first_admin("ADA@example.com", "Ada", QUICK.hash(PW), NOW, setup_code_sha256=h)
    assert acc.setup_code_ok(h, NOW)  # rolled back with the insert: try again with another email
    acc.create_first_admin("root@example.com", "Root", QUICK.hash(PW), NOW, setup_code_sha256=h)


def test_two_first_admins_at_once_make_one(shared_store):
    """Two replicas reading TRAILBEAM_ADMIN_EMAIL at once, or a setup form sent twice: one admin."""
    code = first_admin.issue_code(Accounts(shared_store), NOW)
    h = first_admin.code_hash(code)
    go = threading.Barrier(8)

    def one(i):
        a = Accounts(shared_store)
        go.wait()
        try:
            return a.create_first_admin(f"admin{i}@example.com", f"Admin {i}", "x", NOW,
                                        setup_code_sha256=h if i % 2 else None)
        except (AdminExists, BadSetupCode):
            return None

    with concurrent.futures.ThreadPoolExecutor(8) as pool:
        made = [u for u in pool.map(one, range(8)) if u is not None]
    assert len(made) == 1
    assert [u["role"] for u in Accounts(shared_store).users()] == ["admin"]


# ---- the server keeps an admin ------------------------------------------------------------------------------

@pytest.mark.parametrize("change", ["member", "disable", "remove"])
def test_the_last_admin_stays(acc, change):
    root = acc.create_user("root@example.com", "Root", "admin", None, NOW)
    other = acc.create_user("ops@example.com", "Ops", "admin", None, NOW)

    def do(uid):
        if change == "member":
            return acc.set_role(uid, "member", NOW)
        if change == "disable":
            return acc.set_disabled(uid, True, NOW)
        return acc.remove_user(uid)

    assert do(other)  # another admin is left
    with pytest.raises(LastAdmin):
        do(root)
    assert acc.user(root)["role"] == "admin" and acc.user(root)["disabled_at_ms"] is None


def test_a_disabled_admin_does_not_count_as_the_one_kept(acc):
    root = acc.create_user("root@example.com", "Root", "admin", None, NOW)
    gone = acc.create_user("gone@example.com", "Gone", "admin", None, NOW)
    acc.set_disabled(gone, True, NOW)
    with pytest.raises(LastAdmin):
        acc.set_role(root, "member", NOW)
    assert acc.remove_user(gone)  # a disabled admin can go: nobody who signs in loses anything
    member = acc.create_user("m@example.com", "M", "member", None, NOW)
    assert acc.remove_user(member) and acc.set_role(root, "admin", NOW)


def test_two_admins_demoting_each_other_at_once_leave_one(shared_store):
    a = Accounts(shared_store)
    ids = [a.create_user(f"a{i}@example.com", f"A{i}", "admin", None, NOW) for i in range(2)]
    go = threading.Barrier(2)

    def demote(uid):
        acc = Accounts(shared_store)
        go.wait()
        try:
            return acc.set_role(uid, "member", NOW)
        except LastAdmin:
            return False

    with concurrent.futures.ThreadPoolExecutor(2) as pool:
        done = list(pool.map(demote, ids))
    assert sorted(done) == [False, True]
    assert [u["role"] for u in a.users()].count("admin") == 1


# ---- TRAILBEAM_ADMIN_EMAIL ----------------------------------------------------------------------------------

def _env(tmp_path, password=PW + "\n", **extra):
    f = tmp_path / "admin-password"
    f.write_text(password, encoding="utf-8")
    return {first_admin.EMAIL_ENV: "root@example.com", first_admin.PASSWORD_FILE_ENV: str(f), **extra}


def test_admin_from_the_environment(acc, tmp_path):
    uid = first_admin.from_env(acc, QUICK, NOW, _env(tmp_path, **{first_admin.NAME_ENV: "Ops team"}))
    u = acc.user(uid)
    assert (u["email"], u["name"], u["role"]) == ("root@example.com", "Ops team", "admin")
    assert QUICK.verify(PW, u["password_hash"])  # the file's line break is not part of it


def test_admin_from_the_environment_only_while_there_is_none(acc, tmp_path):
    acc.create_user("ada@example.com", "Ada", "admin", None, NOW)
    env = _env(tmp_path)
    os.unlink(env[first_admin.PASSWORD_FILE_ENV])  # not even read
    assert first_admin.from_env(acc, QUICK, NOW, env) is None
    assert [u["email"] for u in acc.users()] == ["ada@example.com"]


def test_admin_from_the_environment_needs_both_and_a_good_password(acc, tmp_path):
    assert first_admin.from_env(acc, QUICK, NOW, {}) is None
    env = _env(tmp_path)
    for bad in ({first_admin.EMAIL_ENV: "root@example.com"}, {first_admin.PASSWORD_FILE_ENV: "/x"}):
        with pytest.raises(first_admin.FirstAdminError, match="go together"):
            first_admin.from_env(acc, QUICK, NOW, bad)
    with pytest.raises(first_admin.FirstAdminError, match="at least 10"):
        first_admin.from_env(acc, QUICK, NOW, _env(tmp_path, password="short\n"))
    with pytest.raises(first_admin.FirstAdminError, match="can't read"):
        first_admin.from_env(acc, QUICK, NOW, {**env, first_admin.PASSWORD_FILE_ENV: str(tmp_path / "nope")})
    with pytest.raises(first_admin.FirstAdminError, match="email"):
        first_admin.from_env(acc, QUICK, NOW, {**_env(tmp_path), first_admin.EMAIL_ENV: "not an email"})
    assert acc.users() == []


def test_replicas_starting_with_the_same_admin_make_one(shared_store, tmp_path):
    env = _env(tmp_path)
    go = threading.Barrier(4)

    def start(_):
        go.wait()
        return first_admin.from_env(Accounts(shared_store), QUICK, NOW, env)

    with concurrent.futures.ThreadPoolExecutor(4) as pool:
        made = [u for u in pool.map(start, range(4)) if u is not None]
    assert len(made) == 1 and len(Accounts(shared_store).users()) == 1


def test_the_server_reads_the_admin_from_its_environment(tmp_path):
    db = tmp_path / "t.sqlite"
    env = {k: v for k, v in os.environ.items() if not k.startswith(("TRAILBEAM_", "OTEL_"))}
    env["TRAILBEAM_ACCOUNTS"] = "1"
    env.update(_env(tmp_path), TRAILBEAM_TOKEN="shared-token-0123456789abcdef")
    p = subprocess.Popen([sys.executable, "-m", "trailbeam.server", "--db", str(db), "--port", "0"], env=env,
                         stderr=subprocess.PIPE, text=True)
    try:
        log = []
        for line in p.stderr:
            log.append(line)
            if " retention " in line:  # after "listening on" and what it says about access
                break
        text = "".join(log)
        assert "created the first admin, root@example.com, from TRAILBEAM_ADMIN_EMAIL" in text
        assert "works only for ingest and /metrics" in text  # the shared token's warning, now accounts are on
    finally:
        p.terminate()
        p.wait(10)
    s = Database(str(db))
    try:
        assert [(u["email"], u["role"]) for u in Accounts(s).users()] == [("root@example.com", "admin")]
    finally:
        s.close()
    # a bad password file stops the server, before it serves anything
    env[first_admin.EMAIL_ENV] = "other@example.com"
    db2 = tmp_path / "t2.sqlite"
    Path(env[first_admin.PASSWORD_FILE_ENV]).write_text("short")
    r = subprocess.run([sys.executable, "-m", "trailbeam.server", "--db", str(db2), "--port", "0"], env=env,
                       capture_output=True, text=True, timeout=60)
    assert r.returncode == 2 and "TRAILBEAM_ADMIN_PASSWORD_FILE: the password must be at least 10" in r.stderr


# ---- /setup over HTTP ----------------------------------------------------------------------------------------

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


@pytest.fixture
def serve(store):
    started = []

    def start(token=None):
        httpd = make_server(store, "127.0.0.1", 0, ui_dir=None, auth=auth.Auth(token) if token else None)
        httpd.app.access.state.ttl_s = 0.0
        httpd.app.signin.hasher = QUICK
        threading.Thread(target=httpd.serve_forever, daemon=True).start()
        started.append(httpd)
        return f"http://127.0.0.1:{httpd.server_address[1]}", httpd.app

    yield start
    for httpd in started:
        httpd.shutdown()
        httpd.server_close()


def post_setup(url, headers=None, **fields):
    form = {"email": "ada@example.com", "name": "Ada", "password": PW, "password2": PW, **fields}
    return call(url, "/setup", method="POST", data=urllib.parse.urlencode(form).encode(),
                headers={**FORM, **(headers or {})})


TOKEN = "shared-token-0123456789abcdef"


def test_the_setup_page_is_open_and_keeps_the_code_out_of_logs_and_referrers(serve):
    url, app = serve(TOKEN)  # trailbeam start always has a token: /setup is open anyway
    status, headers, body = call(url, "/setup")
    assert status == 200
    assert headers["Referrer-Policy"] == "no-referrer" and headers["Cache-Control"] == "no-store"
    page = body.decode()
    assert 'action="/setup"' in page and 'name="code"' in page and "location.hash" in page
    assert "history.replaceState" in page  # the code leaves the address bar


def test_the_setup_link_creates_the_admin_and_signs_them_in(serve):
    url, app = serve(TOKEN)
    code = first_admin.issue_code(app.accounts, int(time.time() * 1000))
    status, headers, _ = post_setup(url, code=code)
    assert status == 303 and headers["Location"] == "/"
    cookie = headers["Set-Cookie"]
    assert cookie.startswith("trailbeam_sid=") and "HttpOnly" in cookie and "SameSite=Strict" in cookie
    status, _, body = call(url, "/api/me", headers={"Cookie": cookie.split(";")[0]})
    assert status == 200 and b'"role": "admin"' in body and b'"auth": "accounts"' in body
    # used: the link now leads to the sign-in page, and the form makes nobody
    assert call(url, "/setup")[1]["Location"] == "/login"
    status, headers, _ = post_setup(url, code=code, email="eve@example.com")
    assert status == 303 and headers["Location"] == "/login"
    assert [u["email"] for u in app.accounts.users()] == ["ada@example.com"]
    # the shared token: ingest and /metrics only, from now on
    assert call(url, "/api/messages", headers={"Authorization": f"Bearer {TOKEN}"})[0] == 401
    assert call(url, "/metrics", headers={"Authorization": f"Bearer {TOKEN}"})[0] == 200


@pytest.mark.parametrize("case", ["wrong", "none", "malformed"])
def test_a_wrong_code_is_refused_before_any_password_is_hashed(serve, case, monkeypatch):
    url, app = serve()
    first_admin.issue_code(app.accounts, int(time.time() * 1000))
    hashed = []
    monkeypatch.setattr(QUICK, "hash", lambda pw: hashed.append(pw) or "x")
    code = {"wrong": first_admin.new_code(), "none": "", "malformed": "a b<script>"}[case]
    status, _, body = post_setup(url, code=code)
    assert status == 403 and b"has been used, is out of date" in body
    assert hashed == [] and app.accounts.users() == []


def test_too_many_wrong_codes_from_one_address_slow_it_down(serve):
    url, app = serve()
    app.signin.address_limit.max_failures = 3
    for _ in range(3):
        assert post_setup(url, code=first_admin.new_code())[0] == 403
    code = first_admin.issue_code(app.accounts, int(time.time() * 1000))
    status, headers, _ = post_setup(url, code=code)  # even the right one, until the window moves on
    assert status == 429 and int(headers["Retry-After"]) > 0 and app.accounts.users() == []


@pytest.mark.parametrize("fields, says", [
    ({"password2": "something else entirely"}, b"not the same"),
    ({"password": "short", "password2": "short"}, b"at least 10 characters"),
    ({"email": "not an email"}, b"look like name@example.com"),
    ({"name": "   "}, b"printable characters"),
])
def test_a_form_that_can_t_be_used_keeps_the_code(serve, fields, says):
    url, app = serve()
    code = first_admin.issue_code(app.accounts, int(time.time() * 1000))
    status, _, body = post_setup(url, code=code, **fields)
    assert status == 400 and says in body
    assert code.encode() in body  # still in the form, to try again
    assert app.accounts.setup_code_ok(first_admin.code_hash(code), int(time.time() * 1000))


def test_a_setup_form_from_another_site_is_refused(serve):
    url, app = serve()
    code = first_admin.issue_code(app.accounts, int(time.time() * 1000))
    status, _, _ = post_setup(url, {"Origin": "https://evil.example", "Sec-Fetch-Site": "cross-site"}, code=code)
    assert status == 403 and app.accounts.users() == []


def test_setup_after_an_admin_exists_goes_to_sign_in(serve):
    url, app = serve()
    app.accounts.create_user("root@example.com", "Root", "admin", None, NOW)
    status, headers, _ = call(url, "/setup")
    assert status == 303 and headers["Location"] == "/login"


# ---- trailbeam start ----------------------------------------------------------------------------------------

def _start(tmp_path, terminal: bool):
    env = {k: v for k, v in os.environ.items() if not k.startswith(("TRAILBEAM_", "OTEL_"))}
    env["TRAILBEAM_ACCOUNTS"] = "1"
    if terminal:
        env["TRAILBEAM_START_TERMINAL"] = "1"
    p = subprocess.Popen([sys.executable, "-m", "trailbeam.server", "start", "--data-dir", str(tmp_path / "data"),
                          "--port", "0"], env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    out = []
    for line in p.stdout:
        out.append(line)
        if "Stop with Ctrl-C" in line:
            break
    p.terminate()
    _, err = p.communicate(timeout=20)
    return "".join(out), err


def test_trailbeam_start_prints_a_new_setup_link_in_its_terminal_only(tmp_path):
    out, err = _start(tmp_path, terminal=True)
    assert "Create your admin account (the link works once, for an hour" in out
    link = next(w for w in out.split() if "/setup#code=" in w)
    code = link.split("#code=")[1]
    assert first_admin.well_formed(code) and code not in err and "/setup" not in err  # never in the log
    assert "the web screen asks for it once, until an admin account exists" in out
    out2, _ = _start(tmp_path, terminal=True)
    code2 = next(w for w in out2.split() if "/setup#code=" in w).split("#code=")[1]
    assert code2 != code
    s = Database(str(tmp_path / "data" / "trailbeam.sqlite"))
    try:
        a = Accounts(s)
        assert not a.setup_code_ok(first_admin.code_hash(code), int(time.time() * 1000))  # replaced
        assert a.setup_code_ok(first_admin.code_hash(code2), int(time.time() * 1000))
        a.create_user("root@example.com", "Root", "admin", None, NOW)
    finally:
        s.close()
    out3, _ = _start(tmp_path, terminal=True)
    assert "/setup#code=" not in out3 and "Sign in to the web screen with your Trailbeam account" in out3
    assert "Access token (for your services and /metrics; the web screen needs an account)" in out3


def test_trailbeam_start_into_a_log_prints_no_link(tmp_path):
    out, err = _start(tmp_path, terminal=False)
    assert "/setup#code=" not in out + err
    assert "No admin account yet. To add one: trailbeam users create --email <email> --role admin" in out
    s = Database(str(tmp_path / "data" / "trailbeam.sqlite"))
    try:
        assert Accounts(s)._read("SELECT COUNT(*) AS n FROM setup_codes")[0]["n"] == 0  # no code made at all
    finally:
        s.close()


def test_first_admin_errors_are_account_errors():
    assert issubclass(LastAdmin, AccountError) and issubclass(AdminExists, AccountError)
    assert issubclass(BadSetupCode, AccountError)
