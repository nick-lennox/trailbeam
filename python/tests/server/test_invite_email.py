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
    st, _, out = plain("POST", "/api/users", {"email": "cy@example.com"}, plain.admin)
    assert st == 201 and out["email"] is None and "#code=" in out["link"]["url"]
    st, _, out = plain("POST", "/api/users", {"email": "dee@example.com", "send_email": True}, plain.admin)
    assert (st, out["reason"]) == (409, "no_mail_server")
    assert "dee@example.com" not in [u["email"] for u in plain.acc.users()]


def test_an_invite_is_emailed_and_only_the_person_gets_the_link(mailing, outbox):
    assert mailing("GET", "/api/users", headers=mailing.admin)[2]["sends_email"] is True
    st, _, out = mailing("POST", "/api/users", {"email": "cy@example.com", "name": "Cy"}, mailing.admin)
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
    st, _, out = mailing("POST", f"/api/users/{mailing.ben}/link", {}, mailing.admin)
    assert st == 201 and out["link"]["kind"] == "reset" and out["email"]["sent"] is True and "url" not in out["link"]
    [(to, m)] = outbox.sent
    assert to == "ben@example.com" and m["subject"] == "Reset your Trailbeam password" and "24 hours" in m["text"]


def test_send_email_false_shows_the_link_instead(mailing, outbox):
    st, _, out = mailing("POST", "/api/users", {"email": "cy@example.com", "send_email": False}, mailing.admin)
    assert st == 201 and out["email"] is None and "#code=" in out["link"]["url"] and not outbox.sent
    st, _, out = mailing("POST", f"/api/users/{mailing.ben}/link", {"send_email": False}, mailing.admin)
    assert st == 201 and out["email"] is None and out["link"]["url"]
    assert mailing("POST", "/api/users", {"email": "dee@example.com", "send_email": "yes"}, mailing.admin)[0] == 400


def test_when_sending_fails_the_person_is_added_and_the_link_is_shown(store):
    s = Server(store, TOKEN, mailer=mail.Mailer(SMTP, send=Outbox(fail="The mail server refused cy@example.com.")))
    try:
        st, _, out = s("POST", "/api/users", {"email": "cy@example.com"}, s.admin)
        assert st == 201
        assert out["email"] == {"sent": False, "to": "cy@example.com", "error": "The mail server refused cy@example.com."}
        assert "#code=" in out["link"]["url"]
        assert set_password(s, _code(out["link"]["url"]))[0] == 303
    finally:
        s.close()


def test_the_invite_goes_through_a_real_smtp_conversation(store):
    box = Smtp()
    try:
        smtp = SmtpConfig(host="127.0.0.1", port=box.port, sender="Trailbeam <trailbeam@example.com>", tls="none")
        s = Server(store, TOKEN, mailer=mail.Mailer(smtp))
        try:
            st, _, out = s("POST", "/api/users", {"email": "cy@example.com"}, s.admin)
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
