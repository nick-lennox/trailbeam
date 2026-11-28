"""Passkeys (passkeys.py, QV-665): adding one on /api/me/passkeys, signing in with it on /login/passkey, and every
answer that must be refused, against a software authenticator (webauthn_fake.py) whose signatures the server's
webauthn library checks for real. SQLite here, and Postgres with TRAILBEAM_TEST_POSTGRES_URL."""
from __future__ import annotations

import json
import logging
import time
import urllib.parse

import pytest

pytest.importorskip("webauthn")

from test_openapi import request  # noqa: E402
from test_people_api import PW, QUICK, Server, _code, set_password, store  # noqa: E402,F401  (store: the fixture)
from webauthn_fake import Authenticator, b64  # noqa: E402

from trailbeam.server import passkeys as passkeys_mod, web  # noqa: E402

PUBLIC = "https://trailbeam.example.com"
J = {"Content-Type": "application/json"}


def browser(s, email):
    """A session cookie for this person, signed in with their password as in a browser: only a session adds a
    passkey (QV-695)."""
    form = urllib.parse.urlencode({"email": email, "password": PW, "next": "/"}).encode()
    st, hdrs, _ = request(s.url, "POST", "/login", form, {"Content-Type": "application/x-www-form-urlencoded"})
    assert st == 303, st
    return _cookie(hdrs)


@pytest.fixture
def srv(store):  # noqa: F811
    """Ada (admin) and Ben (member) signed in in a browser (``admin``, ``member``), and their personal API tokens
    (``admin_token``, ``member_token``)."""
    s = Server(store, None, public_url=PUBLIC)
    s.admin_token, s.member_token = s.admin, s.member
    s.admin, s.member = browser(s, "ada@example.com"), browser(s, "ben@example.com")
    yield s
    s.close()


def _post(srv, path, obj, headers=None):
    status, hdrs, body = request(srv.url, "POST", path, json.dumps(obj).encode(), {**J, **(headers or {})})
    return status, hdrs, json.loads(body) if body else None


def add(srv, who, auth: Authenticator, name="Chrome on Mac", **kw):
    st, _, out = srv("POST", "/api/me/passkeys/options", {}, who)
    assert st == 200, out
    return srv("POST", "/api/me/passkeys", {"name": name, "credential": auth.create(out["options"], **kw)}, who)


def sign_in(srv, auth: Authenticator, next_path="/stuck", headers=None, **kw):
    st, _, out = _post(srv, passkeys_mod.OPTIONS_PATH, {}, headers)
    assert st == 200, out
    answer = auth.get(out["options"], **kw)
    return _post(srv, passkeys_mod.SIGN_IN_PATH, {"next": next_path, "credential": answer}, headers), answer


def _cookie(hdrs):
    return {"Cookie": hdrs["Set-Cookie"].split(";")[0]}


# ---- the whole way round -------------------------------------------------------------------------------------

def test_add_a_passkey_then_sign_in_with_it(srv):
    a = Authenticator(PUBLIC, synced=True)
    st, hdrs, out = add(srv, srv.admin, a)
    assert st == 201, out
    assert hdrs["Cache-Control"] == "no-store"
    pk = out["passkey"]
    assert pk["name"] == "Chrome on Mac" and pk["synced"] is True and pk["last_used_ms"] is None
    st, _, listed = srv("GET", "/api/me/passkeys", None, srv.admin)
    assert st == 200 and listed == {"passkeys": [pk], "on": True, "can_add": True}

    (st, hdrs, out), _ = sign_in(srv, a)
    assert st == 200, out
    assert out == {"next": "/stuck"} and hdrs["Cache-Control"] == "no-store"
    assert hdrs["Set-Cookie"].startswith("trailbeam_sid=") and "HttpOnly" in hdrs["Set-Cookie"]
    st, _, me = srv("GET", "/api/me", None, _cookie(hdrs))
    assert st == 200 and me["via"] == "session" and me["user"]["email"] == "ada@example.com"
    used = srv("GET", "/api/me/passkeys", None, srv.admin)[2]["passkeys"][0]
    assert used["last_used_ms"] is not None
    assert srv.acc.user(srv.ada)["last_sign_in_ms"] == used["last_used_ms"]


def test_the_options_ask_for_a_discoverable_passkey_that_checks_the_person(srv):
    st, _, out = srv("POST", "/api/me/passkeys/options", {}, srv.admin)
    o = out["options"]
    assert o["rp"] == {"id": "trailbeam.example.com", "name": "Trailbeam"}
    assert o["user"]["name"] == "ada@example.com" and o["user"]["displayName"] == "Ada"
    assert o["user"]["id"] == b64(str(srv.ada).encode())  # the person's ID, nothing that names them
    assert o["authenticatorSelection"]["residentKey"] == "required"
    assert o["authenticatorSelection"]["userVerification"] == "required"
    assert o["excludeCredentials"] == []
    a = Authenticator(PUBLIC)
    assert add(srv, srv.admin, a)[0] == 201
    again = srv("POST", "/api/me/passkeys/options", {}, srv.admin)[2]["options"]
    assert [c["id"] for c in again["excludeCredentials"]] == [b64(a.cred_id)]  # no second one on that device
    st, _, out = _post(srv, passkeys_mod.OPTIONS_PATH, {})
    assert out["options"]["rpId"] == "trailbeam.example.com" and out["options"]["allowCredentials"] == []
    assert out["options"]["userVerification"] == "required"


def test_an_unsafe_next_goes_home(srv):
    a = Authenticator(PUBLIC)
    add(srv, srv.admin, a)
    (st, _, out), _ = sign_in(srv, a, next_path="https://evil.example.com/")
    assert st == 200 and out == {"next": "/"}


def test_a_passkey_without_a_counter_signs_in_every_time(srv):
    a = Authenticator(PUBLIC, counter=False)
    add(srv, srv.admin, a)
    for _ in range(3):
        assert sign_in(srv, a)[0][0] == 200


# ---- refused ---------------------------------------------------------------------------------------------------

def test_an_answer_works_once(srv):
    a = Authenticator(PUBLIC)
    add(srv, srv.admin, a)
    (st, _, _), answer = sign_in(srv, a)
    assert st == 200
    st, _, out = _post(srv, passkeys_mod.SIGN_IN_PATH, {"credential": answer})
    assert st == 400 and "Try again" in out["error"]


def test_another_origin_or_relying_party_is_refused(srv):
    a = Authenticator(PUBLIC)
    add(srv, srv.admin, a)
    (st, _, out), _ = sign_in(srv, a, origin="https://trailbeam.example.com.evil.test")
    assert st == 401 and out == {"error": passkeys_mod.REFUSED}
    (st, _, out), _ = sign_in(srv, a, rp_id="evil.test")
    assert st == 401
    b = Authenticator("https://other.example.com")
    st, _, out = add(srv, srv.member, b)
    assert st == 400 and "wasn't saved" in out["error"]


def test_no_user_verification_is_refused(srv):
    a = Authenticator(PUBLIC)
    st, _, out = add(srv, srv.admin, a, uv=False)
    assert st == 400
    assert add(srv, srv.admin, a)[0] == 201
    (st, _, _), _ = sign_in(srv, a, uv=False)
    assert st == 401


def test_a_counter_that_goes_back_is_refused(srv):
    a = Authenticator(PUBLIC)
    add(srv, srv.admin, a)
    assert sign_in(srv, a)[0][0] == 200  # 1
    assert sign_in(srv, a)[0][0] == 200  # 2
    (st, _, _), _ = sign_in(srv, a, count=1)  # a copy of the passkey, behind
    assert st == 401


def test_a_counter_saved_meanwhile_by_another_sign_in_is_not_overwritten(srv):
    """QV-696: the counter is checked again when it is saved, so a lower one can't overwrite a higher one."""
    a = Authenticator(PUBLIC)
    add(srv, srv.admin, a)
    pid = int(srv.acc.passkeys(srv.ada)[0]["id"])
    assert srv.acc.passkey_used(pid, 11, 1) is True
    assert srv.acc.passkey_used(pid, 10, 2) is False  # the slower of two sign-ins at once
    assert srv.acc.passkey_used(pid, 11, 3) is False  # a replayed 11
    assert srv.acc.passkey_used(pid, 0, 4) is False  # a counter can't go back to none
    p = srv.acc.passkeys(srv.ada)[0]
    assert (p["sign_count"], p["last_used_ms"]) == (11, 1)
    assert srv.acc.passkey_used(pid + 1000, 12, 5) is False  # removed meanwhile


def test_a_sign_in_that_loses_the_race_gets_no_session(srv, monkeypatch):
    """Two sign-ins at once read the same saved counter (9), both pass the library's check, and the one with the
    lower counter (10) saves second: it is refused, after the 11, before a session starts."""
    a = Authenticator(PUBLIC)
    add(srv, srv.admin, a)
    pid = int(srv.acc.passkeys(srv.ada)[0]["id"])
    srv.acc.passkey_used(pid, 9, 1)
    real = srv.acc.passkey_by_credential

    def read_then_the_other_saves(cid):
        found = real(cid)  # it read 9
        srv.acc.passkey_used(pid, 11, 2)  # the other sign-in, with 11, saves first
        return found

    monkeypatch.setattr(srv.acc, "passkey_by_credential", read_then_the_other_saves)
    (st, hdrs, out), _ = sign_in(srv, a, count=10)
    assert st == 401 and out == {"error": passkeys_mod.REFUSED}
    assert "Set-Cookie" not in hdrs
    assert srv.acc.passkeys(srv.ada)[0]["sign_count"] == 11


def test_an_answer_from_a_frame_inside_another_site_is_refused(srv):
    """clientDataJSON's crossOrigin: true means the page asking was framed by another site's."""
    a = Authenticator(PUBLIC)
    st, _, out = add(srv, srv.admin, a, cross_origin=True)
    assert st == 400 and "wasn't saved" in out["error"]
    assert add(srv, srv.admin, a)[0] == 201
    (st, hdrs, out), _ = sign_in(srv, a, cross_origin=True)
    assert st == 401 and out == {"error": passkeys_mod.REFUSED} and "Set-Cookie" not in hdrs
    assert sign_in(srv, a)[0][0] == 200


def test_an_unknown_passkey_a_disabled_person_and_a_wrong_owner_are_refused(srv):
    stranger = Authenticator(PUBLIC)
    stranger.user_handle = str(srv.ada).encode()
    (st, _, out), _ = sign_in(srv, stranger)
    assert st == 401 and out == {"error": passkeys_mod.REFUSED}
    b = Authenticator(PUBLIC)
    assert add(srv, srv.member, b)[0] == 201
    b.user_handle = str(srv.ada).encode()  # Ben's passkey, claiming to be Ada's
    assert sign_in(srv, b)[0][0] == 401
    b.user_handle = str(srv.ben).encode()
    srv.acc.set_disabled(srv.ben, True, int(time.time() * 1000))
    assert sign_in(srv, b)[0][0] == 401


def test_a_challenge_is_for_one_purpose_and_one_person(srv):
    a = Authenticator(PUBLIC)
    st, _, out = srv("POST", "/api/me/passkeys/options", {}, srv.admin)
    made = a.create(out["options"])
    st, _, out = srv("POST", "/api/me/passkeys", {"name": "x", "credential": made}, srv.member)  # Ada's challenge
    assert st == 400
    # a sign-in challenge doesn't add a passkey
    st, _, out = _post(srv, passkeys_mod.OPTIONS_PATH, {})
    opts = srv("POST", "/api/me/passkeys/options", {}, srv.member)[2]["options"]
    made = a.create({**opts, "challenge": out["options"]["challenge"]})
    assert srv("POST", "/api/me/passkeys", {"name": "x", "credential": made}, srv.member)[0] == 400


def test_a_challenge_runs_out(srv, monkeypatch):
    a = Authenticator(PUBLIC)
    add(srv, srv.admin, a)
    st, _, out = _post(srv, passkeys_mod.OPTIONS_PATH, {})
    later = time.time_ns() // 1_000_000 + passkeys_mod.CHALLENGE_TTL_MS + 1
    monkeypatch.setattr(srv.app.passkeys, "now_ms", lambda: later)
    st, _, out = _post(srv, passkeys_mod.SIGN_IN_PATH, {"credential": a.get(out["options"])})
    assert st == 400


def test_the_same_passkey_twice_and_too_many(srv, monkeypatch):
    a = Authenticator(PUBLIC)
    assert add(srv, srv.admin, a)[0] == 201
    st, _, out = add(srv, srv.admin, a)
    assert st == 409 and out["reason"] == "passkey_taken"
    st, _, out = add(srv, srv.member, a)  # the credential is someone else's already
    assert st == 409
    monkeypatch.setattr(passkeys_mod, "MAX_PER_PERSON", 2)
    assert add(srv, srv.admin, Authenticator(PUBLIC))[0] == 201
    st, _, out = add(srv, srv.admin, Authenticator(PUBLIC))
    assert st == 409 and "most" in out["error"]


def test_bad_bodies(srv):
    st, _, out = srv("POST", "/api/me/passkeys", {"name": "x"}, srv.admin)
    assert st == 400 and "credential" in out["error"]
    a = Authenticator(PUBLIC)
    st, _, opts = srv("POST", "/api/me/passkeys/options", {}, srv.admin)
    st, _, out = srv("POST", "/api/me/passkeys", {"name": " ", "credential": a.create(opts["options"])}, srv.admin)
    assert st == 400 and "name" in out["error"]
    st, _, out = srv("POST", "/api/me/passkeys", {"name": "x" * 101, "credential": {}}, srv.admin)
    assert st == 400
    st, _, out = srv("POST", "/api/me/passkeys", {"name": "x", "credential": {"response": {}}}, srv.admin)
    assert st == 400
    assert _post(srv, passkeys_mod.SIGN_IN_PATH, {"credential": "x"})[0] == 400
    assert _post(srv, passkeys_mod.SIGN_IN_PATH, {"credential": {"id": "x"}})[0] == 400
    st, _, _ = request(srv.url, "POST", passkeys_mod.SIGN_IN_PATH, b"{}", {"Content-Type": "text/plain"})
    assert st == 415
    st, _, _ = request(srv.url, "POST", passkeys_mod.SIGN_IN_PATH, None,
                       {**J, "Content-Length": str(passkeys_mod.MAX_ANSWER + 1)})
    assert st in (413, None)


def test_an_over_long_credential_id_or_public_key_is_refused(srv):
    """WebAuthn Level 3, 7.1: a credential ID over 1023 bytes aborts the ceremony (QV-694). Saved, it could never
    sign in, and on Postgres it is over the unique index's row size (a 500)."""
    longest = Authenticator(PUBLIC, id_bytes=passkeys_mod.MAX_CREDENTIAL_ID)
    assert add(srv, srv.admin, longest)[0] == 201
    assert sign_in(srv, longest)[0][0] == 200
    for auth in (Authenticator(PUBLIC, id_bytes=passkeys_mod.MAX_CREDENTIAL_ID + 1),
                 Authenticator(PUBLIC, key_padding=passkeys_mod.MAX_PUBLIC_KEY)):
        st, _, out = add(srv, srv.member, auth)
        assert st == 400 and "didn't check out" in out["error"], (st, out)
    st, _, out = srv("GET", "/api/me/passkeys", None, srv.member)
    assert st == 200 and out["passkeys"] == []


def test_a_raw_id_that_is_not_the_attested_one_is_refused(srv):
    a = Authenticator(PUBLIC)
    st, _, opts = srv("POST", "/api/me/passkeys/options", {}, srv.admin)
    cred = a.create(opts["options"])
    cred["id"] = cred["rawId"] = "AAAA"
    st, _, out = srv("POST", "/api/me/passkeys", {"name": "x", "credential": cred}, srv.admin)
    assert st == 400, out


def test_another_sites_page_cant_sign_in(srv):
    st, _, body = request(srv.url, "POST", passkeys_mod.OPTIONS_PATH, b"{}", {**J, "Sec-Fetch-Site": "cross-site"})
    assert st == 403 and b"forbidden" in body


# ---- the address limits ------------------------------------------------------------------------------------

def test_failed_sign_ins_count_against_the_address(srv):
    srv.app.signin.address_limit.max_failures = 3
    stranger = Authenticator(PUBLIC)
    for _ in range(3):
        assert sign_in(srv, stranger)[0][0] == 401
    st, _, out = _post(srv, passkeys_mod.OPTIONS_PATH, {})
    st, hdrs, out = _post(srv, passkeys_mod.SIGN_IN_PATH, {"credential": stranger.get(out["options"])})
    assert st == 429 and int(hdrs["Retry-After"]) >= 1 and "Too many" in out["error"]


def test_starting_sign_ins_is_limited_per_address(srv):
    srv.app.passkey_starts.max_failures = 2
    assert _post(srv, passkeys_mod.OPTIONS_PATH, {})[0] == 200
    assert _post(srv, passkeys_mod.OPTIONS_PATH, {})[0] == 200
    st, hdrs, out = _post(srv, passkeys_mod.OPTIONS_PATH, {})
    assert st == 429 and "Retry-After" in hdrs


# ---- removing, and the list ------------------------------------------------------------------------------

def test_remove_a_passkey(srv):
    a = Authenticator(PUBLIC)
    pid = add(srv, srv.admin, a)[2]["passkey"]["id"]
    assert srv("DELETE", f"/api/me/passkeys/{pid}", None, srv.member)[0] == 404  # not Ben's
    st, hdrs, out = srv("DELETE", f"/api/me/passkeys/{pid}", None, srv.admin)
    assert st == 200 and out == {"removed": {"id": pid}}
    assert srv("DELETE", f"/api/me/passkeys/{pid}", None, srv.admin)[0] == 404
    assert srv("GET", "/api/me/passkeys", None, srv.admin)[2]["passkeys"] == []
    assert sign_in(srv, a)[0][0] == 401


def test_each_person_sees_only_their_own(srv):
    add(srv, srv.admin, Authenticator(PUBLIC), name="Ada's phone")
    add(srv, srv.member, Authenticator(PUBLIC), name="Ben's key")
    assert [p["name"] for p in srv("GET", "/api/me/passkeys", None, srv.member)[2]["passkeys"]] == ["Ben's key"]


def test_removing_the_person_removes_their_passkeys(srv):
    a = Authenticator(PUBLIC)
    add(srv, srv.member, a)
    assert srv("DELETE", f"/api/users/{srv.ben}", None, srv.admin)[0] == 200
    assert srv.acc.passkey_by_credential(b64(a.cred_id)) is None
    assert sign_in(srv, a)[0][0] == 401


# ---- a credential that outlives a compromise (QV-695) -------------------------------------------------------

def test_a_personal_token_cant_add_a_passkey(srv):
    """A short-lived CI token, or one copied for a while, must not leave behind a passkey that outlives it."""
    a = Authenticator(PUBLIC)
    st, _, out = srv("POST", "/api/me/passkeys/options", {}, srv.admin_token)
    assert st == 403 and out["reason"] == "needs_a_session" and "browser" in out["error"]
    # nor with options a session got: the answer is plain JSON a script could send with the token
    opts = srv("POST", "/api/me/passkeys/options", {}, srv.admin)[2]["options"]
    st, _, out = srv("POST", "/api/me/passkeys", {"name": "x", "credential": a.create(opts)}, srv.admin_token)
    assert st == 403 and out["reason"] == "needs_a_session"
    assert srv.acc.passkeys(srv.ada) == []
    # the token still lists and removes them
    listed = srv("GET", "/api/me/passkeys", None, srv.admin_token)[2]
    assert listed == {"passkeys": [], "on": True, "can_add": False}
    pid = add(srv, srv.admin, a)[2]["passkey"]["id"]
    assert srv("DELETE", f"/api/me/passkeys/{pid}", None, srv.admin_token)[0] == 200


def test_a_sign_in_through_the_proxy_cant_add_a_passkey(srv):
    """A passkey added through the proxy would sign the person in after the proxy link is removed."""
    from trailbeam.server.access import Who
    from trailbeam.server.people_api import ApiProblem

    proxy = Who("proxy", "admin", srv.ada, "ada@example.com", "Ada")
    for handler in ("passkey_options", "add_my_passkey"):
        with pytest.raises(ApiProblem) as e:
            srv.app.people.call(handler, None, {"name": "x", "credential": {}}, proxy, PUBLIC, 0)
        assert e.value.status == 403 and e.value.reason == "needs_a_session"
    assert srv.app.people.call("list_my_passkeys", None, None, proxy, PUBLIC, 0)[1]["can_add"] is False


def test_an_admin_token_cant_get_its_own_reset_link_and_add_a_passkey(srv):
    """QV-712: the token makes a reset link for its own person, sets the password, and the session that answers
    adds a passkey. Without the link's url it can't."""
    st, _, out = srv("POST", f"/api/users/{srv.ada}/link", {}, srv.admin_token)
    assert st == 403 and out["reason"] == "needs_a_session" and "People page" in out["error"]
    assert srv("POST", f"/api/users/{srv.ada}/link", {"send_email": False}, srv.admin_token)[0] == 403
    assert srv("GET", "/api/me", None, srv.admin)[0] == 200  # nothing was made: Ada's session and link stand
    assert srv.acc.passkeys(srv.ada) == []


def test_an_admin_token_cant_take_a_new_admins_invite_and_add_a_passkey(srv):
    """QV-712: the token adds an admin, takes the invite from the answer, sets the password and adds a passkey to
    an account nobody is told about. The person is still added; the answer has no url."""
    st, _, out = srv("POST", "/api/users", {"email": "eve@example.com", "name": "Eve", "role": "admin"},
                     srv.admin_token)
    assert st == 201 and out["user"]["status"] == "invited" and out["email"] is None
    assert out["link"]["kind"] == "invite" and "url" not in out["link"] and out["link"]["expires_at_ms"]
    # a browser still gets the link, for the same person
    st, _, again = srv("POST", f"/api/users/{out['user']['id']}/link", {}, srv.admin)
    assert st == 201 and "#code=" in again["link"]["url"]


def test_through_the_proxy_a_link_is_not_shown_either(srv):
    from trailbeam.server.access import Who
    from trailbeam.server.people_api import ApiProblem

    proxy = Who("proxy", "admin", srv.ada, "ada@example.com", "Ada")
    with pytest.raises(ApiProblem) as e:
        srv.app.people.call("user_link", srv.ben, {}, proxy, PUBLIC, int(time.time() * 1000))
    assert e.value.status == 403 and e.value.reason == "needs_a_session"
    st, out = srv.app.people.call("create_user", None, {"email": "fay@example.com"}, proxy, PUBLIC,
                                  int(time.time() * 1000))
    assert st == 201 and "url" not in out["link"]


def test_adding_a_passkey_is_logged_as_a_warning_with_the_address(srv, caplog):
    with caplog.at_level(logging.WARNING, logger="trailbeam.server"):
        pid = add(srv, srv.member, Authenticator(PUBLIC), name="Ben's key")[2]["passkey"]["id"]
    assert any(r.levelno == logging.WARNING and f"user {srv.ben} added passkey {pid} (\"Ben's key\") from 127.0.0.1"
               in r.getMessage() for r in caplog.records), [r.getMessage() for r in caplog.records]


def test_a_reset_link_removes_the_passkeys(srv):
    """A reset is how an admin takes an account back: a passkey someone else added must stop signing in."""
    a, other = Authenticator(PUBLIC), Authenticator(PUBLIC)
    add(srv, srv.member, a)
    add(srv, srv.admin, other)
    link = srv("POST", f"/api/users/{srv.ben}/link", {}, srv.admin)[2]["link"]
    st, _, body = request(srv.url, "GET", "/set-password")
    assert b"your passkeys" in body
    st, hdrs, _ = set_password(srv, _code(link["url"]), pw="a new password 123")
    assert st == 303
    assert srv.acc.passkeys(srv.ben) == []
    assert sign_in(srv, a)[0][0] == 401
    assert srv("GET", "/api/me", None, srv.member)[0] == 401  # and the session ended, as before
    assert sign_in(srv, other)[0][0] == 200  # Ada's stay


def test_the_command_line_reset_removes_the_passkeys(srv):
    add(srv, srv.member, Authenticator(PUBLIC))
    assert srv.acc.set_password(srv.ben, QUICK.hash("another one 123"), 1) is True
    assert len(srv.acc.passkeys(srv.ben)) == 1  # a plain password change keeps them
    assert srv.acc.set_password(srv.ben, QUICK.hash("another one 123"), 1, remove_passkeys=True) is True
    assert srv.acc.passkeys(srv.ben) == []


def test_an_admin_sees_and_removes_someone_elses_passkeys(srv):
    a = Authenticator(PUBLIC)
    pk = add(srv, srv.member, a, name="Not Ben's")[2]["passkey"]
    add(srv, srv.member, Authenticator(PUBLIC))
    users = {u["id"]: u for u in srv("GET", "/api/users", None, srv.admin_token)[2]["users"]}
    assert users[srv.ben]["passkeys"] == 2 and users[srv.ada]["passkeys"] == 0
    st, _, out = srv("GET", f"/api/users/{srv.ben}/passkeys", None, srv.admin_token)
    assert st == 200 and out["on"] is True and out["passkeys"][0] == pk and len(out["passkeys"]) == 2
    # a member can't, not even their own this way
    assert srv("GET", f"/api/users/{srv.ben}/passkeys", None, srv.member)[0] == 403
    assert srv("DELETE", f"/api/users/{srv.ben}/passkeys", None, srv.member)[0] == 403
    assert srv("GET", "/api/users/999/passkeys", None, srv.admin_token)[0] == 404
    assert srv("DELETE", "/api/users/999/passkeys", None, srv.admin_token)[0] == 404
    st, _, out = srv("DELETE", f"/api/users/{srv.ben}/passkeys", None, srv.admin)
    assert st == 200 and out == {"removed": {"id": srv.ben, "passkeys": 2}}
    assert sign_in(srv, a)[0][0] == 401
    assert srv("GET", f"/api/users/{srv.ben}", None, srv.admin_token)[0] in (404, 405)  # no such route: unchanged
    assert srv("GET", "/api/me", None, srv.member)[0] == 200  # sessions stay: a reset link ends those
    assert srv("DELETE", f"/api/users/{srv.ben}/passkeys", None, srv.admin)[2]["removed"]["passkeys"] == 0


def test_the_shared_token_has_no_passkeys(store):  # noqa: F811
    s = Server(store, "shared-token-0123456789abcdef", public_url=PUBLIC)
    try:
        st, _, out = s("GET", "/api/me/passkeys", None, {"Authorization": "Bearer shared-token-0123456789abcdef"})
        assert st == 401
    finally:
        s.close()


# ---- on and off ----------------------------------------------------------------------------------------------

def test_off_without_the_public_url(store):  # noqa: F811
    s = Server(store, None)
    try:
        assert s.app.passkeys is None
        assert s("GET", "/api/me/passkeys", None, s.admin)[2] == {"passkeys": [], "on": False, "can_add": False}
        st, _, out = s("POST", "/api/me/passkeys/options", {}, s.admin)
        assert st == 409 and out["reason"] == "passkeys_off" and "TRAILBEAM_PUBLIC_URL" in out["error"]
        assert s("POST", "/api/me/passkeys", {"name": "x", "credential": {}}, s.admin)[0] == 409
        assert _post(s, passkeys_mod.OPTIONS_PATH, {})[0] == 404
        assert _post(s, passkeys_mod.SIGN_IN_PATH, {})[0] == 404
        st, _, body = request(s.url, "GET", "/login")
        assert b"Sign in with a passkey" not in body
        assert s("GET", "/api/health", None, s.admin)[2]["config"]["passkeys"] is False
    finally:
        s.close()


def test_off_without_the_library(store, monkeypatch):  # noqa: F811
    monkeypatch.setattr(passkeys_mod, "available", lambda: False)
    s = Server(store, None, public_url=PUBLIC)
    try:
        assert s.app.passkeys is None
    finally:
        s.close()


def test_the_sign_in_page_has_the_passkey_button(srv):
    st, _, body = request(srv.url, "GET", "/login?next=/stuck")
    assert st == 200
    assert b'id="passkey" data-next="/stuck"' in body and b"Sign in with a passkey" in body
    assert b"/login/passkey/options" in body
    assert b"Or sign in with your Trailbeam email and password." in body
    assert srv("GET", "/api/health", None, srv.admin)[2]["config"]["passkeys"] is True
    # a wrong password keeps the button
    st, _, body = request(srv.url, "POST", "/login", b"email=ada%40example.com&password=nope",
                          {"Content-Type": "application/x-www-form-urlencoded"})
    assert st == 401 and b"Sign in with a passkey" in body


def test_not_before_the_first_admin(store):  # noqa: F811
    from test_people_api import make_server
    import threading

    httpd = make_server(store, "127.0.0.1", 0, ui_dir=None, public_url=PUBLIC)
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    url = f"http://127.0.0.1:{httpd.server_address[1]}"
    try:
        assert httpd.app.passkeys is not None
        st, _, _ = request(url, "POST", passkeys_mod.OPTIONS_PATH, b"{}", J)
        assert st == 404
    finally:
        httpd.shutdown()
        httpd.server_close()


def test_settings_from_the_public_url():
    s = passkeys_mod.settings("https://Trailbeam.Example.com")
    assert s == passkeys_mod.Settings("trailbeam.example.com", "https://trailbeam.example.com")
    assert passkeys_mod.settings("https://tb.example.com:8443/base").origin == "https://tb.example.com:8443"
    assert passkeys_mod.settings("https://tb.example.com:443").origin == "https://tb.example.com"
    assert passkeys_mod.settings("http://localhost:4318").origin == "http://localhost:4318"
    assert not passkeys_mod.insecure(passkeys_mod.settings("http://localhost:4318"))
    assert passkeys_mod.insecure(passkeys_mod.settings("http://tb.internal"))
    assert not passkeys_mod.insecure(passkeys_mod.settings("https://tb.internal"))


def test_the_page_without_the_build():
    st, _, body = web.handle(lambda *a: {}, Exception, "/account", {})
    assert st == 200 and b"/api/me/passkeys" in body
