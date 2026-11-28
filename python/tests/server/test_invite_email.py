"""Invites and password resets by email (QV-587, mail.py): the operator's mail server from TRAILBEAM_SMTP_*, the
People API emailing the one-time link when one is set up, and the link itself staying for servers without one or
when sending fails. The answers are checked against contract/openapi.yaml by the shared request helper."""
import pytest

from trailbeam.server import mail
from trailbeam.server.alert_delivery import DeliveryError, SmtpConfig

from test_alert_delivery import Smtp
from test_people_api import PW, TOKEN, Server, _code, set_password, store  # noqa: F401 - store is a fixture

ADA = "ada@example.com"


class Outbox:
    """A Mailer's send: keeps what would be sent, or fails like a mail server that refuses."""

    def __init__(self, fail=None):
        self.sent, self.fail = [], fail

    def __call__(self, to, m):
        if self.fail:
            raise DeliveryError(self.fail)
        self.sent.append((to, m))


SMTP = SmtpConfig(host="127.0.0.1", sender="Trailbeam <trailbeam@example.com>", tls="none", port=2525)


@pytest.fixture
def outbox():
    return Outbox()


@pytest.fixture
def mailing(store, outbox):
    s = Server(store, TOKEN, mailer=mail.Mailer(SMTP, send=outbox), public_url="https://trailbeam.example.com/")
    yield s
    s.close()


@pytest.fixture
def plain(store):
    s = Server(store, TOKEN)
    yield s
    s.close()


# ---- the settings ---------------------------------------------------------------------------------------------

def test_no_host_is_no_mail_server_and_a_half_setting_is_an_error():
    assert mail.smtp_from_env({}) is None
    with pytest.raises(mail.SmtpSettingsError, match="TRAILBEAM_SMTP_FROM is set, but not TRAILBEAM_SMTP_HOST"):
        mail.smtp_from_env({"TRAILBEAM_SMTP_FROM": "a@example.com"})
    with pytest.raises(mail.SmtpSettingsError, match="TRAILBEAM_SMTP_FROM is required"):
        mail.smtp_from_env({"TRAILBEAM_SMTP_HOST": "smtp.example.com"})


def test_the_settings_and_their_defaults(tmp_path):
    c = mail.smtp_from_env({"TRAILBEAM_SMTP_HOST": "smtp.example.com", "TRAILBEAM_SMTP_FROM": "Trailbeam <t@example.com>"})
    assert (c.host, c.port, c.tls, c.user, c.password) == ("smtp.example.com", 587, "starttls", None, None)
    assert mail.smtp_from_env({"TRAILBEAM_SMTP_HOST": "h", "TRAILBEAM_SMTP_FROM": "t@example.com",
                               "TRAILBEAM_SMTP_TLS": "SSL"}).port == 465
    secret = tmp_path / "smtp"
    secret.write_text("s3cret\n")
    c = mail.smtp_from_env({"TRAILBEAM_SMTP_HOST": "h", "TRAILBEAM_SMTP_FROM": "t@example.com", "TRAILBEAM_SMTP_PORT": "2525",
                            "TRAILBEAM_SMTP_USER": "tb", "TRAILBEAM_SMTP_PASSWORD_FILE": str(secret)})
    assert (c.port, c.user, c.password) == (2525, "tb", "s3cret")
    assert "s3cret" not in repr(c)


@pytest.mark.parametrize("env,problem", [
    ({"TRAILBEAM_SMTP_TLS": "tls"}, 'TRAILBEAM_SMTP_TLS is "starttls", "ssl" or "none"'),
    ({"TRAILBEAM_SMTP_PORT": "0"}, "TRAILBEAM_SMTP_PORT is a port number"),
    ({"TRAILBEAM_SMTP_PORT": "smtp"}, "TRAILBEAM_SMTP_PORT is a port number"),
    ({"TRAILBEAM_SMTP_USER": "tb"}, "TRAILBEAM_SMTP_USER is set, but no password"),
    ({"TRAILBEAM_SMTP_PASSWORD": "pw"}, "a mail server password is set, but not TRAILBEAM_SMTP_USER"),
    ({"TRAILBEAM_SMTP_USER": "tb", "TRAILBEAM_SMTP_PASSWORD": "pw", "TRAILBEAM_SMTP_TLS": "none"},
     "signing in to the mail server needs tls"),
    ({"TRAILBEAM_SMTP_FROM": "not an address"}, "sender is not an email address"),
    ({"TRAILBEAM_SMTP_PASSWORD_FILE": "/nonexistent/smtp", "TRAILBEAM_SMTP_USER": "tb"}, "cannot read it"),
])
def test_a_setting_that_does_not_make_sense_stops_the_server(env, problem):
    with pytest.raises(mail.SmtpSettingsError, match=problem.replace("(", r"\(")):
        mail.smtp_from_env({"TRAILBEAM_SMTP_HOST": "h", "TRAILBEAM_SMTP_FROM": "t@example.com", **env})


def test_the_public_url_for_links():
    assert mail.check_public_url("https://trailbeam.example.com/") == "https://trailbeam.example.com"
    assert mail.check_public_url("http://trailbeam.internal:4318") == "http://trailbeam.internal:4318"
    for bad in ("trailbeam.example.com", "https://t.example.com/?x=1", "ftp://t.example.com"):
        with pytest.raises(mail.SmtpSettingsError):
            mail.check_public_url(bad)


# ---- the API ----------------------------------------------------------------------------------------------------

def test_without_a_mail_server_the_link_is_shown_as_before(plain):
    assert plain("GET", "/api/users", headers=plain.admin)[2]["sends_email"] is False
    st, _, out = plain("POST", "/api/users", {"email": "cy@example.com"}, plain.browser)
    assert st == 201 and out["email"] is None and "#code=" in out["link"]["url"]
    # a personal API token adds the person, but sees no link, and can't make one it would see (QV-712)
    st, _, out = plain("POST", "/api/users", {"email": "eve@example.com"}, plain.admin)
    assert st == 201 and out["email"] is None and "url" not in out["link"]
    st, _, out = plain("POST", f"/api/users/{out['user']['id']}/link", {}, plain.admin)
    assert (st, out["reason"]) == (403, "needs_a_session")
    st, _, out = plain("POST", "/api/users", {"email": "dee@example.com", "send_email": True}, plain.browser)
    assert (st, out["reason"]) == (409, "no_mail_server")
    assert "dee@example.com" not in [u["email"] for u in plain.acc.users()]


def test_an_invite_is_emailed_and_only_the_person_gets_the_link(mailing, outbox):
    assert mailing("GET", "/api/users", headers=mailing.admin)[2]["sends_email"] is True
    st, _, out = mailing("POST", "/api/users", {"email": "cy@example.com", "name": "Cy"}, mailing.browser)
    assert st == 201
    assert out["email"] == {"sent": True, "to": "cy@example.com", "error": None}
    assert "url" not in out["link"] and out["link"]["kind"] == "invite"
    [(to, m)] = outbox.sent
    assert to == "cy@example.com" and m["subject"] == "Join Trailbeam"
    url = m["text"].split("Accept the invitation: ", 1)[1].split("\n", 1)[0]
    assert url.startswith("https://trailbeam.example.com/set-password#code=")  # the public URL, not the Host
    assert "Ada invited you to Trailbeam at https://trailbeam.example.com" in m["text"]
    assert f'href="{url}"' in m["html"] and "7 days" in m["text"]
    st, _, _ = set_password(mailing, _code(url))
    assert st == 303
    assert mailing.acc.user_by_email("cy@example.com")["password_hash"]


def test_a_reset_link_is_emailed_too(mailing, outbox):
    st, _, out = mailing("POST", f"/api/users/{mailing.ben}/link", {}, mailing.browser)
    assert st == 201 and out["link"]["kind"] == "reset" and out["email"]["sent"] is True and "url" not in out["link"]
    [(to, m)] = outbox.sent
    assert to == "ben@example.com" and m["subject"] == "Reset your Trailbeam password" and "24 hours" in m["text"]


def test_send_email_false_shows_the_link_instead(mailing, outbox):
    st, _, out = mailing("POST", "/api/users", {"email": "cy@example.com", "send_email": False}, mailing.browser)
    assert st == 201 and out["email"] is None and "#code=" in out["link"]["url"] and not outbox.sent
    st, _, out = mailing("POST", f"/api/users/{mailing.ben}/link", {"send_email": False}, mailing.browser)
    assert st == 201 and out["email"] is None and out["link"]["url"]
    st, _, out = mailing("POST", f"/api/users/{mailing.ben}/link", {"send_email": False}, mailing.admin)
    assert (st, out["reason"]) == (403, "needs_a_session") and not outbox.sent  # QV-712: a token sees no link
    assert mailing("POST", "/api/users", {"email": "dee@example.com", "send_email": "yes"}, mailing.admin)[0] == 400


def test_an_admin_token_cant_have_an_invite_emailed_to_an_address_it_chose(mailing, outbox):
    """QV-755: with a mail server, the token adds eve@attacker.example as an admin and the invite goes to her inbox;
    she sets the password, adds a passkey, and keeps an admin account after the token is revoked. Here nothing is
    emailed, so nobody can open the invite."""
    eve = {"email": "eve@attacker.example", "name": "Eve", "role": "admin"}
    st, _, out = mailing("POST", "/api/users", eve, mailing.admin)
    assert st == 201 and out["email"] is None and "url" not in out["link"] and not outbox.sent
    uid = out["user"]["id"]
    for body in ({}, {"send_email": True}):  # re-inviting her doesn't email it either
        st, _, out = mailing("POST", f"/api/users/{uid}/link", body, mailing.admin)
        assert (st, out["reason"]) == (403, "needs_a_session") and "People page" in out["error"]
    st, _, out = mailing("POST", "/api/users", {**eve, "email": "eve2@attacker.example", "send_email": True},
                         mailing.admin)
    assert (st, out["reason"]) == (403, "needs_a_session")
    assert "eve2@attacker.example" not in [u["email"] for u in mailing.acc.users()]  # asked to email: nobody added
    assert not outbox.sent and mailing.acc.user_by_email("eve@attacker.example")["password_hash"] is None
    # nor a reset to someone who has a password: a link from a token is a session it could take
    assert mailing("POST", f"/api/users/{mailing.ben}/link", {"send_email": True}, mailing.admin)[0] == 403
    assert not outbox.sent
    # an admin signed in in a browser still emails Eve her invite
    st, _, out = mailing("POST", f"/api/users/{uid}/link", {}, mailing.browser)
    assert st == 201 and out["email"]["sent"] is True and [to for to, _ in outbox.sent] == ["eve@attacker.example"]


def test_through_the_proxy_nothing_is_emailed_either(mailing, outbox):
    from trailbeam.server.access import Who
    from trailbeam.server.people_api import ApiProblem

    proxy = Who("proxy", "admin", mailing.ada, "ada@example.com", "Ada")
    people = mailing.app.people
    st, out = people.call("create_user", None, {"email": "fay@example.com"}, proxy, "https://t.example.com", 0)
    assert st == 201 and out["email"] is None and "url" not in out["link"] and not outbox.sent
    for handler, rid, body in (("user_link", out["user"]["id"], {}), ("user_link", mailing.ben, {"send_email": True}),
                               ("create_user", None, {"email": "gus@example.com", "send_email": True})):
        with pytest.raises(ApiProblem) as e:
            people.call(handler, rid, body, proxy, "https://t.example.com", 0)
        assert (e.value.status, e.value.reason) == (403, "needs_a_session")
    assert not outbox.sent


def test_when_sending_fails_the_person_is_added_and_the_link_is_shown(store):
    s = Server(store, TOKEN, mailer=mail.Mailer(SMTP, send=Outbox(fail="The mail server refused cy@example.com.")))
    try:
        st, _, out = s("POST", "/api/users", {"email": "cy@example.com"}, s.browser)
        assert st == 201
        assert out["email"] == {"sent": False, "to": "cy@example.com", "error": "The mail server refused cy@example.com."}
        assert "#code=" in out["link"]["url"]
        assert set_password(s, _code(out["link"]["url"]))[0] == 303
        # a personal API token gets neither the link nor an email (QV-712, QV-755)
        st, _, out = s("POST", "/api/users", {"email": "dee@example.com"}, s.admin)
        assert st == 201 and out["email"] is None and "url" not in out["link"]
        assert s("POST", f"/api/users/{s.ben}/link", {}, s.admin)[0] == 403
    finally:
        s.close()


def test_the_invite_goes_through_a_real_smtp_conversation(store):
    box = Smtp()
    try:
        smtp = SmtpConfig(host="127.0.0.1", port=box.port, sender="Trailbeam <trailbeam@example.com>", tls="none")
        s = Server(store, TOKEN, mailer=mail.Mailer(smtp))
        try:
            st, _, out = s("POST", "/api/users", {"email": "cy@example.com"}, s.browser)
            assert st == 201 and out["email"]["sent"] is True
        finally:
            s.close()
        [got] = box.got
        msg = got["msg"]
        assert got["to"] == ["cy@example.com"] and msg["Subject"] == "Join Trailbeam"
        url = msg.get_body(("plain",)).get_content().split("Accept the invitation: ", 1)[1].split("\n", 1)[0]
        assert url.startswith(s.url + "/set-password#code=")  # no public URL: the address the request came to
        assert "Accept the invitation" in msg.get_body(("html",)).get_content()
    finally:
        box.close()


def test_the_email_escapes_what_people_typed():
    m = mail.invite_email("https://t.example.com/set-password#code=a&b", "https://t.example.com", "<b>Eve</b>", None, 7)
    assert "<b>Eve</b>" not in m["html"] and "&lt;b&gt;Eve&lt;/b&gt;" in m["html"]
    assert 'href="https://t.example.com/set-password#code=a&amp;b"' in m["html"]
    assert mail.invite_email("u", "s", None, ADA, 7)["text"].startswith("Join Trailbeam\n\nada@example.com invited you")
    for m in (mail.invite_email("u", "s", "Ada", ADA, 7), mail.reset_email("u", "s", 24)):
        assert "—" not in m["text"] and "→" not in m["text"]  # no em dashes or arrows (the copy rules)


def test_a_bad_mail_setting_stops_the_server_at_start(tmp_path):
    import os
    import subprocess
    import sys

    env = {**os.environ, "TRAILBEAM_ACCOUNTS": "1", "TRAILBEAM_SMTP_HOST": "smtp.example.com"}
    env.pop("TRAILBEAM_SMTP_FROM", None)
    r = subprocess.run([sys.executable, "-m", "trailbeam.server", "--db", str(tmp_path / "x.sqlite"), "--port", "0"],
                       env=env, capture_output=True, text=True, timeout=30)
    assert r.returncode == 2 and "Traceback" not in r.stderr and "TRAILBEAM_SMTP_FROM is required" in r.stderr
