"""Alert delivery (alert_messages.py and alert_delivery.py, QV-636): parity with the hosted app's lib/alerts/deliver.ts and
channels.ts on the shared cases in contract/fixtures/alerts-delivery.json (the app's test/alerts-delivery.test.ts runs
the same file), then the real sending: a local HTTP server stands in for webhooks and Slack, a fake SMTP server for the
mail server. Nothing here touches the network."""
import copy
import email
import email.policy
import importlib.util
import json
import logging
import shutil
import socket
import socketserver
import ssl
import subprocess
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

from trailbeam.server import alert_delivery as d
from trailbeam.server import alert_messages as m

FIXTURES = Path(__file__).resolve().parents[3] / "contract" / "fixtures"
F = json.loads((FIXTURES / "alerts-delivery.json").read_text())
POSTS = [c for c in F["send"] if "posts" in c["expected"]]
MAILS = [c for c in F["send"] if "emails" in c["expected"]]


def hosted_base(c):
    return f"{c['origin'].rstrip('/')}/app/{c['workspace']['slug']}"


def test_the_fixture_is_what_its_script_writes(tmp_path, monkeypatch):
    """alerts-delivery.json is make-alerts-delivery.py's output from this code: a change to either shows here."""
    spec = importlib.util.spec_from_file_location("make_alerts_delivery", FIXTURES / "make-alerts-delivery.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    monkeypatch.setattr(mod, "OUT", tmp_path / "alerts-delivery.json")
    mod.main()
    assert json.loads(mod.OUT.read_text()) == F, "run contract/fixtures/make-alerts-delivery.py and check the app's test"


def test_the_shared_cases_are_there():
    kinds = {c["channel"]["kind"] for c in F["send"]}
    assert kinds == {"webhook", "slack", "email"} and len(F["send"]) >= 60
    assert len(F["wants"]) >= 8 and len(F["checkTarget"]) >= 40 and len(F["shownTarget"]) >= 6


# ── the shared cases ─────────────────────────────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("c", POSTS, ids=lambda c: c["name"])
def test_post_messages(c):
    """The body and signature, as the hosted app makes them (with its links)."""
    changes = copy.deepcopy(c["changes"])
    (want,) = c["expected"]["posts"]
    base, at, ch = hosted_base(c), c["at"], c["channel"]
    body = m.webhook_body(c["workspace"], changes, base, at) if ch["kind"] == "webhook" else m.slack_body(c["workspace"], changes, base, at)
    assert body == want["body"]
    if ch["kind"] == "webhook":
        assert m.sign(ch["secret"] or "", str(at), body) == want["headers"]["x-trailbeam-signature"]
    assert changes == c["changes"], "making a message changes nothing"


@pytest.mark.parametrize("c", MAILS, ids=lambda c: c["name"])
def test_email_messages(c):
    mail = m.email_of(c["channel"], c["workspace"], c["changes"], hosted_base(c), c["at"])
    assert [{"to": to, **mail} for to in m.recipients(c["channel"])] == c["expected"]["emails"]


@pytest.mark.parametrize("c", F["wants"], ids=lambda c: c["name"])
def test_wants(c):
    assert m.wants(c["channel"], c["change"]) is c["expected"]


@pytest.mark.parametrize("c", F["checkTarget"], ids=lambda c: c["name"])
def test_check_target(c):
    assert d.check_target(c["kind"], c["raw"], c["allowLocal"]) == c["expected"]


@pytest.mark.parametrize(("raw", "host"), [
    ("https://[::ffff:127.0.0.1]/", "[::ffff:7f00:1]"),
    ("https://[::FFFF:10.0.0.5]/", "[::ffff:a00:5]"),
    ("https://[::ffff:8.8.8.8]/", "[::ffff:808:808]"),
])
def test_ipv4_mapped_hosts_parse_on_every_python(raw, host):
    # Python 3.12.4+ writes .exploded of an IPv4-mapped address with a dotted tail; the parser must not depend on it.
    assert d.parse_url(raw).host == host


@pytest.mark.parametrize("c", F["shownTarget"], ids=lambda c: c["name"])
def test_shown_target(c):
    assert d.shown_target(c["channel"]) == c["expected"]


# ── what the cases don't cover ───────────────────────────────────────────────────────────────────────────────────

def test_self_hosted_links_have_no_workspace_in_the_path():
    x = F["send"][0]["changes"][0]
    assert m.href_of("https://tb.example.com/", x) == "https://tb.example.com/dashboards/d1"
    rule = next(c for c in F["send"] if c["name"] == "webhook: a quiet cause rule fires")["changes"][0]
    assert m.href_of("https://tb.example.com", rule) == "https://tb.example.com/stuck?cause=c_9f2"
    assert m.settings_url("https://tb.example.com/") == "https://tb.example.com/settings#alerts"


def test_a_label_is_cut_by_utf16_units_and_never_inside_a_character():
    """JavaScript counts a label's length in UTF-16 units; where its cut would split an emoji (and the hosted app send
    half of it), the whole emoji is left out."""
    assert m._cut("🚀" * 70) == "🚀" * 59 + "…"
    assert m._cut("🚀" * 60) == "🚀" * 60  # 120 units: at the limit, not over
    assert m._cut("🚀" * 61) == "🚀" * 59 + "…"  # 61 code points would fit if they were counted
    assert m._cut("a" * 120) == "a" * 120
    assert m._cut("é" * 121) == "é" * 119 + "…"


def test_math_round_halves_go_up():
    assert m._before(0, int(2.5 * 60 * 1000)) == "3 min ago"  # Python's round(2.5) is 2
    assert m._before(0, int(2.5 * 3600 * 1000)) == "3 h ago"


def test_smtp_config_refuses_a_password_in_clear_and_hides_it():
    with pytest.raises(ValueError):
        d.SmtpConfig(host="mail", sender="a@x.io", tls="none", user="u", password="p")
    with pytest.raises(ValueError):
        d.SmtpConfig(host="mail", sender="a@x.io", tls="tls")
    with pytest.raises(ValueError):
        d.SmtpConfig(host="mail", sender="nobody")
    cfg = d.SmtpConfig(host="mail", sender="Trailbeam <alerts@x.io>", user="u", password="hunter2")
    assert "hunter2" not in repr(cfg) and "hunter2" not in repr(d.Delivery(base="x", smtp=cfg))


# ── a local HTTP server ──────────────────────────────────────────────────────────────────────────────────────────

class Hook:
    """Records each request; answers with ``status`` after ``delay`` s, or drips its answer a byte at a time."""

    def __init__(self, ssl_context=None):
        self.requests, self.status, self.delay, self.drip, self.location = [], 204, 0.0, False, None
        hook = self

        class H(BaseHTTPRequestHandler):
            def log_message(self, *a):
                pass

            def do_POST(self):
                body = self.rfile.read(int(self.headers.get("content-length", 0)))
                hook.requests.append({"path": self.path, "headers": {k.lower(): v for k, v in self.headers.items()}, "body": body})
                time.sleep(hook.delay)
                if hook.drip:
                    for b in b"HTTP/1.1 200 OK\r\n":
                        try:
                            self.wfile.write(bytes([b]))
                            self.wfile.flush()
                        except OSError:
                            return
                        time.sleep(0.1)
                    return
                self.send_response(hook.status)
                if hook.location:
                    self.send_header("location", hook.location)
                self.send_header("content-length", "0")
                self.end_headers()

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), H)
        self.server.daemon_threads = True
        if ssl_context:
            self.server.socket = ssl_context.wrap_socket(self.server.socket, server_side=True)
        self.port = self.server.server_address[1]
        threading.Thread(target=self.server.serve_forever, args=(0.02,), daemon=True).start()

    def url(self, path="/hook", scheme="http"):
        return f"{scheme}://127.0.0.1:{self.port}{path}"

    def close(self):
        self.server.shutdown()
        self.server.server_close()


@pytest.fixture
def hook():
    h = Hook()
    yield h
    h.close()


WS = F["send"][0]["workspace"]
AT = F["send"][0]["at"]


def local(**kw):
    return d.Delivery(base="https://trailbeam.app/app/acme", allow_local=True, now=lambda: AT, **kw)


@pytest.mark.parametrize("c", POSTS, ids=lambda c: c["name"])
def test_posts_arrive_as_the_cases_say(hook, c):
    """Sent for real, to a local server standing in for the webhook or Slack: the same body and headers."""
    (want,) = c["expected"]["posts"]
    path = "/" + want["url"].split("/", 3)[3]
    ch = {**c["channel"], "target": hook.url(path)}
    d.send_to(ch, c["workspace"], c["changes"], d.Delivery(base=hosted_base(c), allow_local=True, now=lambda: c["at"]))
    (got,) = hook.requests
    assert got["path"] == path
    assert got["body"] == want["body"].encode("utf-8")
    assert {k: got["headers"][k] for k in want["headers"]} == want["headers"]


def webhook(target, **kw):
    return {"id": "ch_hook", "kind": "webhook", "name": "Hook", "target": target, "secret": "whsec_topsecret", "minTone": "warning", **kw}


FIRED = m.test_change(AT)


def fails(ch, dv, match):
    with pytest.raises(d.DeliveryError) as e:
        d.send_to(ch, WS, [FIRED], dv)
    assert str(e.value) == match
    return e.value


def test_an_error_status_fails_and_is_not_retried(hook):
    hook.status = 500
    fails(webhook(hook.url()), local(), "The endpoint answered 500 Internal Server Error.")
    assert len(hook.requests) == 1


def test_a_redirect_is_not_followed(hook):
    other = Hook()
    try:
        hook.status, hook.location = 302, other.url("/elsewhere")
        fails(webhook(hook.url()), local(), "The endpoint answered 302 Found.")
        assert len(hook.requests) == 1 and other.requests == []
    finally:
        other.close()


def test_a_slow_endpoint_times_out(hook):
    hook.delay = 2
    t = time.monotonic()
    fails(webhook(hook.url()), local(timeout_s=0.3), "The endpoint did not answer in time.")
    assert time.monotonic() - t < 1.5


def test_the_deadline_is_for_the_whole_answer_not_each_read(hook):
    """An endpoint that sends a byte every 0.1 s never trips a 0.3 s socket timeout; the deadline still ends it."""
    hook.drip = True
    t = time.monotonic()
    fails(webhook(hook.url()), local(timeout_s=0.5), "The endpoint did not answer in time.")
    assert time.monotonic() - t < 1.5


def test_nothing_listening():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    fails(webhook(f"http://127.0.0.1:{port}/"), local(), "The endpoint could not be reached: Connection refused.")


def test_a_target_is_checked_again_before_sending(hook):
    """Saved before the rules tightened, or edited in the database: still refused."""
    fails(webhook(hook.url()), d.Delivery(base="x"), "The URL must start with https://.")
    fails(webhook("file:///etc/passwd"), local(), "The URL must start with https://.")
    fails({**webhook(hook.url()), "kind": "pager"}, local(), "Unknown channel kind.")
    assert hook.requests == []


def fake_dns(monkeypatch, answers, delay=0.0):
    """getaddrinfo for hooks.example.com gives ``answers`` (IPs) after ``delay`` s; other hosts resolve as usual."""
    real, asked = socket.getaddrinfo, []

    def look(host, port, *a, **k):
        if host != "hooks.example.com":
            return real(host, port, *a, **k)
        asked.append(host)
        time.sleep(delay)
        return [(socket.AF_INET6 if ":" in ip else socket.AF_INET, socket.SOCK_STREAM, 6, "",
                 (ip, port, 0, 0) if ":" in ip else (ip, port)) for ip in answers]

    monkeypatch.setattr(socket, "getaddrinfo", look)
    return asked


def listener():
    """A listening socket that tells whether anyone connected to it."""
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    s.listen(8)
    s.setblocking(False)
    return s


def connected(s) -> bool:
    try:
        s.accept()[0].close()
        return True
    except BlockingIOError:
        return False


def test_a_public_name_resolving_to_a_private_address_is_refused_before_connecting(monkeypatch):
    """QV-672: the addresses are checked before connecting, so an open port and a closed one give the same answer, and
    nothing is connected to: a test send can't be used to scan the server's network."""
    fake_dns(monkeypatch, ["127.0.0.1"])
    open_ = listener()
    closed = socket.socket()
    closed.bind(("127.0.0.1", 0))
    closed_port = closed.getsockname()[1]
    closed.close()
    try:
        answers = []
        for port in (open_.getsockname()[1], closed_port):
            answers.append(str(fails(webhook(f"https://hooks.example.com:{port}/x"), d.Delivery(base="x"),
                                     "The webhook's host is a private or local address.")))
        assert answers[0] == answers[1]
        time.sleep(0.05)
        assert not connected(open_)
    finally:
        open_.close()


NOT_PUBLIC = ["64:ff9b::a9fe:a9fe", "64:ff9b::a00:1", "64:ff9b:1::5db8:d822", "2002:a9fe:a9fe::1", "::7f00:1",
              "fec0::1", "2001:0:4136:e378:8000:63bf:3fff:fdd2", "2001:db8::1", "ff02::1", "198.18.0.1", "192.0.0.170",
              "203.0.113.5", "240.0.0.1", "255.255.255.255", "224.0.0.1"]


@pytest.mark.parametrize("addr", NOT_PUBLIC)
def test_a_name_resolving_to_a_non_global_address_is_refused(monkeypatch, addr):
    """QV-673: not only the usual private ranges. NAT64 and 6to4 addresses are judged by the IPv4 address they carry,
    which a gateway would reach for us."""
    fake_dns(monkeypatch, [addr])
    fails(webhook("https://hooks.example.com/x"), d.Delivery(base="x"), "The webhook's host is a private or local address.")


@pytest.mark.parametrize("addr", NOT_PUBLIC)
def test_a_non_global_address_is_refused_as_a_target(addr):
    host = f"[{addr}]" if ":" in addr else addr
    assert d.check_target("webhook", f"https://{host}/")["ok"] is False


@pytest.mark.parametrize("addr", ["93.184.216.34", "2606:4700:4700::1111", "64:ff9b::5db8:d822"])
def test_public_addresses_stay_allowed(addr):
    """A DNS64 resolver answers 64:ff9b::<IPv4> for a name with only an IPv4 address: a public one must still work.
    (No public 6to4 case: Python before 3.12.4 calls all of 2002::/16 non-global, so it is refused there.)"""
    assert not d.private_host(addr)


@pytest.mark.parametrize("answers", [["93.184.216.34", "10.0.0.5"], ["2606:2800:220:1::1", "::1"],
                                     ["93.184.216.34", "::ffff:169.254.169.254"]])
def test_a_name_with_any_private_address_is_refused(monkeypatch, answers):
    """Not only the first address: a name with a public and a private one could otherwise be steered inside."""
    fake_dns(monkeypatch, answers)
    fails(webhook("https://hooks.example.com/x"), d.Delivery(base="x"), "The webhook's host is a private or local address.")


def test_the_send_goes_to_the_checked_address_with_the_name_kept(hook, monkeypatch):
    """One lookup, and the connection is to the address it gave (so DNS can't answer differently between the check and
    the connection), while the Host header still names the URL's host."""
    asked = fake_dns(monkeypatch, ["127.0.0.1"])
    d._post(f"http://hooks.example.com:{hook.port}/x", "{}", {}, local())
    (got,) = hook.requests
    assert got["headers"]["host"] == f"hooks.example.com:{hook.port}"
    assert asked == ["hooks.example.com"]


def test_a_slow_resolver_cannot_push_a_send_past_the_deadline(monkeypatch):
    fake_dns(monkeypatch, ["93.184.216.34"], delay=2)
    t = time.monotonic()
    fails(webhook("https://hooks.example.com/x"), d.Delivery(base="x", timeout_s=0.3), "The endpoint did not answer in time.")
    assert time.monotonic() - t < 1.0


def test_a_name_that_does_not_resolve():
    fails(webhook("https://nothing.invalid/x"), d.Delivery(base="x", timeout_s=5),
          "The endpoint could not be reached: its host name was not found.")


# ── TLS ──────────────────────────────────────────────────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def cert(tmp_path_factory):
    if not shutil.which("openssl"):
        pytest.skip("needs openssl to make a test certificate")
    p = tmp_path_factory.mktemp("cert")
    subprocess.run(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1", "-subj", "/CN=127.0.0.1",
                    "-addext", "subjectAltName=IP:127.0.0.1", "-keyout", str(p / "key.pem"), "-out", str(p / "cert.pem")],
                   check=True, capture_output=True)
    server = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    server.load_cert_chain(p / "cert.pem", p / "key.pem")
    client = ssl.create_default_context(cafile=str(p / "cert.pem"))
    return server, client


def test_https_checks_the_certificate(cert):
    server, client = cert
    h = Hook(ssl_context=server)
    try:
        ch = webhook(h.url(scheme="https"))
        # the check before sending wants a public https host; this one is local, so post directly
        d._post(ch["target"], "{}", {}, local(ssl_context=client))
        assert len(h.requests) == 1
        with pytest.raises(d.DeliveryError, match="TLS failed: CERTIFICATE_VERIFY_FAILED"):
            d._post(ch["target"], "{}", {}, local())
    finally:
        h.close()


# ── a fake SMTP server ───────────────────────────────────────────────────────────────────────────────────────────

class Smtp:
    """Enough of SMTP for smtplib: EHLO, STARTTLS, AUTH PLAIN and LOGIN, MAIL, RCPT, DATA, RSET, QUIT."""

    def __init__(self, user="alerts", password="hunter2", tls=None, implicit=False, refuse=(), stall=False):
        self.got, self.logins = [], []
        box = self

        class H(socketserver.StreamRequestHandler):
            def setup(self):
                if implicit:
                    self.request = tls.wrap_socket(self.request, server_side=True)
                super().setup()

            def say(self, line):
                self.wfile.write(line.encode() + b"\r\n")
                self.wfile.flush()

            def handle(self):
                if stall:
                    time.sleep(3)
                    return
                self.say("220 fake ESMTP")
                rcpt, frm = [], None
                while True:
                    raw = self.rfile.readline()
                    if not raw:
                        return
                    cmd = raw.decode().rstrip("\r\n")
                    verb = cmd.split(" ", 1)[0].upper()
                    if verb == "EHLO":
                        lines = ["250-fake", "250-AUTH PLAIN LOGIN"] + (["250-STARTTLS"] if tls and not implicit else []) + ["250 8BITMIME"]
                        for ln in lines:
                            self.say(ln)
                    elif verb == "STARTTLS":
                        self.say("220 go ahead")
                        self.request = tls.wrap_socket(self.request, server_side=True)
                        self.rfile, self.wfile = self.request.makefile("rb"), self.request.makefile("wb")
                    elif verb == "AUTH":
                        import base64
                        parts = cmd.split(" ")
                        if parts[1].upper() == "PLAIN":
                            _, u, p = base64.b64decode(parts[2]).decode().split("\0")
                        else:
                            self.say("334 VXNlcm5hbWU6")
                            u = base64.b64decode(self.rfile.readline().strip()).decode()
                            self.say("334 UGFzc3dvcmQ6")
                            p = base64.b64decode(self.rfile.readline().strip()).decode()
                        box.logins.append(u)
                        self.say("235 ok" if (u, p) == (user, password) else "535 5.7.8 bad credentials")
                    elif verb == "MAIL":
                        frm, rcpt = cmd.split(":", 1)[1].strip("<> "), []
                        self.say("250 ok")
                    elif verb == "RCPT":
                        to = cmd.split(":", 1)[1].strip("<> ")
                        if to in refuse:
                            self.say("550 5.1.1 no such user")
                        else:
                            rcpt.append(to)
                            self.say("250 ok")
                    elif verb == "DATA":
                        self.say("354 go")
                        data = b""
                        while True:
                            ln = self.rfile.readline()
                            if ln in (b".\r\n", b""):
                                break
                            data += ln[1:] if ln.startswith(b"..") else ln
                        box.got.append({"from": frm, "to": rcpt, "msg": email.message_from_bytes(data.replace(b"\r\n", b"\n"), policy=email.policy.default)})
                        self.say("250 queued")
                    elif verb == "QUIT":
                        self.say("221 bye")
                        return
                    else:
                        self.say("250 ok")

        self.server = socketserver.ThreadingTCPServer(("127.0.0.1", 0), H)
        self.server.daemon_threads = True
        self.port = self.server.server_address[1]
        threading.Thread(target=self.server.serve_forever, args=(0.02,), daemon=True).start()

    def close(self):
        self.server.shutdown()
        self.server.server_close()


def mail_channel(target="oncall@acme.io, lead@acme.io"):
    return {"id": "ch_mail", "kind": "email", "name": "On call", "target": target, "secret": None, "minTone": "warning"}


def smtp_delivery(box, tls="none", user=None, password=None, **kw):
    return local(smtp=d.SmtpConfig(host="127.0.0.1", port=box.port, sender="Trailbeam <alerts@acme.io>", tls=tls,
                                   user=user, password=password), **kw)


@pytest.mark.parametrize("c", MAILS, ids=lambda c: c["name"])
def test_emails_arrive_as_the_cases_say(c):
    box = Smtp()
    try:
        smtp = d.SmtpConfig(host="127.0.0.1", port=box.port, sender="alerts@acme.io", tls="none")
        d.send_to(c["channel"], c["workspace"], c["changes"], d.Delivery(base=hosted_base(c), now=lambda: c["at"], smtp=smtp))
        want = c["expected"]["emails"]
        assert [g["to"] for g in box.got] == [[w["to"]] for w in want]
        for g, w in zip(box.got, want):
            msg = g["msg"]
            assert g["from"] == "alerts@acme.io" and msg["To"] == w["to"] and msg["Auto-Submitted"] == "auto-generated"
            assert msg["Subject"].strip() == w["subject"]  # a long one is folded onto the next line
            assert msg.get_body(("plain",)).get_content().rstrip("\n") == w["text"]
            assert msg.get_body(("html",)).get_content().rstrip("\n") == w["html"]
    finally:
        box.close()


@pytest.mark.parametrize("mode", ["starttls", "ssl"])
def test_email_over_tls_signs_in(cert, mode):
    server, client = cert
    box = Smtp(tls=server, implicit=mode == "ssl")
    try:
        d.send_to(mail_channel("a@acme.io"), WS, [FIRED],
                  smtp_delivery(box, tls=mode, user="alerts", password="hunter2", ssl_context=client))
        assert box.logins == ["alerts"] and [g["to"] for g in box.got] == [["a@acme.io"]]
    finally:
        box.close()


def test_email_failures_say_what_went_wrong(cert):
    server, client = cert
    box = Smtp(tls=server, refuse=("lead@acme.io",))
    try:
        fails(mail_channel(), smtp_delivery(box, tls="starttls", user="alerts", password="wrong", ssl_context=client),
              "The mail server refused the user name or password.")
        fails(mail_channel(), smtp_delivery(box, tls="starttls", user="alerts", password="hunter2", ssl_context=client),
              "The mail server refused lead@acme.io.")
        assert [g["to"] for g in box.got] == [["oncall@acme.io"]], "the first address got its email"
        fails(mail_channel(), smtp_delivery(box, tls="starttls"), "The mail server's TLS failed: CERTIFICATE_VERIFY_FAILED.")
    finally:
        box.close()
    plain = Smtp()
    try:
        fails(mail_channel(), smtp_delivery(plain, tls="starttls"), "The mail server does not offer STARTTLS or the sign-in it needs.")
    finally:
        plain.close()
    stalled = Smtp(stall=True)
    try:
        fails(mail_channel(), smtp_delivery(stalled, timeout_s=0.3), "The mail server did not answer in time.")
    finally:
        stalled.close()
    fails(mail_channel(), local(), "No mail server is set up for alert emails.")


# ── a check's changes to every channel ───────────────────────────────────────────────────────────────────────────

def test_deliver_sends_each_channel_what_it_wants_and_one_failure_stops_nothing(hook, caplog):
    """And no secret reaches a log or an outcome: the webhook secret, the Slack URL past its team, a webhook's path and
    query, the SMTP password."""
    caplog.set_level(logging.DEBUG)
    bad = Hook()
    bad.status = 503
    box = Smtp(user="alerts", password="pw-never-logged")
    try:
        slack_path = "/services/T000/B000/SLACKSECRET"
        chans = [
            webhook(bad.url("/hook?token=QUERYSECRET"), id="bad"),
            {"id": "slack", "kind": "slack", "name": "#ops", "target": hook.url(slack_path), "secret": None, "minTone": "warning"},
            {**mail_channel("a@acme.io"), "id": "mail", "minTone": "critical"},
            {**mail_channel("b@acme.io"), "id": "mail-down"},
        ]
        smtp = d.SmtpConfig(host="127.0.0.1", port=box.port, sender="alerts@acme.io", tls="none")
        dv = local(smtp=smtp)
        out = d.deliver(chans, WS, [FIRED], dv)
        assert [(o["channel"], o["ok"], o["detail"]) for o in out] == [
            ("bad", False, "The endpoint answered 503 Service Unavailable."),
            ("slack", True, None),
            ("mail-down", True, None),  # "mail" wants only critical changes, and this is a warning
        ]
        assert all(o["at"] == AT for o in out)
        assert len(bad.requests) == 1 and len(hook.requests) == 1 and [g["to"] for g in box.got] == [["b@acme.io"]]

        # a mail server that refuses the sign-in, with the password in play
        dv.smtp = d.SmtpConfig(host="127.0.0.1", port=box.port, sender="alerts@acme.io", tls="ssl", user="alerts",
                               password="pw-never-logged")
        (o,) = d.deliver([chans[3]], WS, [FIRED], dv)
        assert not o["ok"]
        text = caplog.text + json.dumps(out) + json.dumps(o)
        for secret in ("whsec_topsecret", "QUERYSECRET", "SLACKSECRET", "pw-never-logged"):
            assert secret not in text
        assert "alert channel bad (http://127.0.0.1:" in caplog.text and "/…)" in caplog.text
    finally:
        bad.close()
        box.close()


def test_a_bug_in_one_channel_is_reported_by_type_only(hook, monkeypatch):
    def boom(*a):
        raise RuntimeError("secret inside")
    monkeypatch.setattr(m, "slack_body", boom)
    chans = [{"id": "s", "kind": "slack", "name": "s", "target": hook.url("/services/x"), "secret": None, "minTone": "warning"},
             webhook(hook.url())]
    out = d.deliver(chans, WS, [FIRED], local())
    assert [(o["ok"], o["detail"]) for o in out] == [(False, "Sending failed: RuntimeError."), (True, None)]
