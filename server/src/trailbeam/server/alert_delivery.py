"""Sending alerts to channels (QV-636, the second part of QV-592: alerts in self-hosted Trailbeam).

The hosted app's lib/alerts/channels.ts (checkTarget, shownTarget) and lib/alerts/deliver.ts (sendTo, deliverChanges)
in Python, standard library only. A channel is a dict {id, kind, name, target, secret, minTone}:

- "webhook": a JSON POST, signed with the channel's secret:
      x-trailbeam-timestamp: <ms since the epoch>
      x-trailbeam-signature: sha256=<hex HMAC-SHA256 of "<timestamp>.<body>">
- "slack": a Slack incoming webhook, with the changes as text.
- "email": one email to each of the channel's addresses, over the SMTP server the operator configures (the hosted app
  sends through Resend; the message is the same).

The texts and payloads are alert_messages.py's. One message per channel per check, holding every change that channel
wants. A channel that fails (an error status, a timeout, a refused email) is reported and does not stop the others.
Nothing is retried, as in the hosted app: the next change is sent as usual.

Safety:
- a target is checked again before each send (check_target), not only when the channel is saved: http or https only,
  no credentials before the host, a webhook on a public host, a Slack URL on hooks.slack.com. ``allow_local`` lets a
  webhook or Slack URL be plain http on a private or local address (an internal endpoint, or the tests).
- the address a webhook's host resolves to is checked too, after connecting and before anything is sent, so a public
  name that points at a private address is refused unless ``allow_local``.
- redirects are never followed: a 3xx answer is a failure, so a webhook can't be bounced to another host.
- every send has a deadline (``timeout_s``, 10 s as in the hosted app): the whole HTTP exchange, and each SMTP step.
- secrets stay out of logs and of the outcome's detail: the webhook secret, the Slack URL and a webhook's path and
  query (where the form says to put credentials), and the SMTP password. Logs name a channel by its ID and
  shown_target().
"""
from __future__ import annotations

import http.client
import ipaddress
import logging
import re
import smtplib
import socket
import ssl
import threading
import time
from dataclasses import dataclass, field
from email.message import EmailMessage
from email.utils import formatdate, make_msgid
from typing import Callable, Optional
from urllib.parse import unquote, urlsplit

from . import alert_messages as m

log = logging.getLogger("trailbeam.server.alerts")

CHANNEL_KINDS = ("webhook", "slack", "email")
MAX_EMAILS = 10
TIMEOUT_S = 10.0
USER_AGENT = "Trailbeam-Alerts/1"
_EMAIL = re.compile(r"^[^\s@,;]+@[^\s@,;]+\.[^\s@,;]+$")


class DeliveryError(Exception):
    """A send failed; the message is the plain reason Settings shows, with no secret in it."""


# ── URLs, as the hosted app's URL class reads them ───────────────────────────────────────────────────────────────

@dataclass
class _Url:
    scheme: str
    userinfo: str
    host: str  # serialized: lower case, IPv4 in dotted decimal, IPv6 in brackets
    port: Optional[int]
    path: str
    query: str  # with its "?", or ""
    fragment: str  # with its "#", or ""

    @property
    def origin(self) -> str:
        return f"{self.scheme}://{self.host}{'' if self.port is None else f':{self.port}'}"

    def __str__(self) -> str:
        user = f"{self.userinfo}@" if self.userinfo else ""
        return f"{self.scheme}://{user}{self.host}{'' if self.port is None else f':{self.port}'}{self.path}{self.query}{self.fragment}"


_FORBIDDEN_HOST = set(" #%/:<>?@[\\]^|\t\n\r\x00")
_DEFAULT_PORT = {"http": 80, "https": 443}


def _ipv4_part(p: str) -> Optional[int]:
    if p == "":
        return None
    if p[:2].lower() == "0x":
        digits = p[2:]
        if digits == "":
            return 0
        return int(digits, 16) if re.fullmatch(r"[0-9a-fA-F]+", digits) else None
    if len(p) > 1 and p[0] == "0":
        return int(p[1:], 8) if re.fullmatch(r"[0-7]+", p[1:]) else None
    return int(p) if p.isdigit() else None


def _ends_in_number(host: str) -> bool:
    parts = host.split(".")
    if parts[-1] == "" and len(parts) > 1:
        parts = parts[:-1]
    last = parts[-1]
    return bool(re.fullmatch(r"[0-9]+", last)) or bool(re.fullmatch(r"0[xX][0-9a-fA-F]*", last))


def _ipv4(host: str) -> Optional[str]:
    """The WHATWG IPv4 parser: "127.1", "0x7f.0.0.1" and "2130706433" are all 127.0.0.1. None when invalid."""
    parts = host.split(".")
    if parts[-1] == "" and len(parts) > 1:
        parts = parts[:-1]
    if len(parts) > 4:
        return None
    nums = [_ipv4_part(p) for p in parts]
    if any(n is None for n in nums) or any(n > 255 for n in nums[:-1]) or nums[-1] >= 256 ** (5 - len(nums)):
        return None
    v = nums[-1]
    for i, n in enumerate(nums[:-1]):
        v += n * 256 ** (3 - i)
    return str(ipaddress.IPv4Address(v))


def _ipv6_text(a: ipaddress.IPv6Address) -> str:
    """An IPv6 address as the URL class writes it: RFC 5952, never with an IPv4 tail."""
    h = [format(int(x, 16), "x") for x in a.exploded.split(":")]
    best, at = 1, -1
    i = 0
    while i < 8:
        if h[i] == "0":
            j = i
            while j < 8 and h[j] == "0":
                j += 1
            if j - i > best:
                best, at = j - i, i
            i = j
        else:
            i += 1
    if at < 0:
        return ":".join(h)
    return ":".join(h[:at]) + "::" + ":".join(h[at + best:])


def _pct(s: str, keep: str) -> str:
    """Percent-encodes what the URL class does: controls, non-ASCII and the characters not in ``keep``."""
    out = []
    for ch in s:
        o = ord(ch)
        if o < 0x21 or o > 0x7E or ch in keep:
            out.append("".join(f"%{b:02X}" for b in ch.encode("utf-8", "surrogatepass")))
        else:
            out.append(ch)
    return "".join(out)


def _dots(path: str) -> str:
    """Removes "." and ".." segments, as the URL class does."""
    segs = path.split("/")[1:]
    out: list = []
    for i, s in enumerate(segs):
        last = i == len(segs) - 1
        low = s.lower()
        if low in ("..", ".%2e", "%2e.", "%2e%2e"):
            if out:
                out.pop()
            if last:
                out.append("")
        elif low in (".", "%2e"):
            if last:
                out.append("")
        else:
            out.append(s)
    return "/" + "/".join(out)


def parse_url(text: str) -> Optional[_Url]:
    """An http or https URL the way the hosted app's ``new URL()`` reads it, or None where it would throw. Other
    schemes come back with an empty host; check_target refuses them."""
    text = re.sub(r"[\t\n\r]", "", text.strip("\x00\x01\x02\x03\x04\x05\x06\x07\x08\t\n\x0b\x0c\r\x0e\x0f\x10\x11\x12"
                                               "\x13\x14\x15\x16\x17\x18\x19\x1a\x1b\x1c\x1d\x1e\x1f "))
    sm = re.match(r"^([A-Za-z][A-Za-z0-9+.\-]*):(.*)$", text, re.S)
    if not sm:
        return None
    scheme, rest = sm.group(1).lower(), sm.group(2)
    if scheme not in _DEFAULT_PORT:
        return _Url(scheme, "", "", None, rest, "", "")
    rest = rest.replace("\\", "/").lstrip("/")
    end = len(rest)
    for ch in "/?#":
        k = rest.find(ch)
        if k >= 0:
            end = min(end, k)
    authority, tail = rest[:end], rest[end:]
    userinfo, _, hostport = authority.rpartition("@")
    if hostport.startswith("["):
        close = hostport.find("]")
        if close < 0:
            return None
        raw_host, after = hostport[: close + 1], hostport[close + 1:]
        if after and not after.startswith(":"):
            return None
        port_text = after[1:] if after else ""
    else:
        raw_host, _, port_text = hostport.partition(":")
    if port_text and not port_text.isdigit():
        return None
    port = int(port_text) if port_text else None
    if port is not None and port > 65535:
        return None
    if port == _DEFAULT_PORT[scheme]:
        port = None
    if raw_host.startswith("["):
        inner = raw_host[1:-1]
        if "%" in inner:
            return None
        try:
            host = f"[{_ipv6_text(ipaddress.IPv6Address(inner))}]"
        except ValueError:
            return None
    else:
        host = unquote(raw_host)
        if any(ord(c) > 0x7F for c in host):
            try:
                host = host.encode("idna").decode("ascii")
            except UnicodeError:
                return None
        host = host.lower()
        if not host or any(c in _FORBIDDEN_HOST for c in host):
            return None
        if _ends_in_number(host):
            v4 = _ipv4(host)
            if v4 is None:
                return None
            host = v4
    frag_at = tail.find("#")
    fragment = tail[frag_at:] if frag_at >= 0 else ""
    tail = tail[:frag_at] if frag_at >= 0 else tail
    q_at = tail.find("?")
    query = tail[q_at:] if q_at >= 0 else ""
    path = tail[:q_at] if q_at >= 0 else tail
    path = _dots(_pct(path or "/", ' "<>`{}'))
    query = "" if query == "" else "?" + _pct(query[1:], " \"#<>'")
    fragment = "" if fragment == "" else "#" + _pct(fragment[1:], ' "<>`')
    return _Url(scheme, userinfo, host, port, path, query, fragment)


def _private_ip(a) -> bool:
    if isinstance(a, ipaddress.IPv4Address):
        x, y = a.packed[0], a.packed[1]
        return x in (0, 10, 127) or (x == 169 and y == 254) or (x == 172 and 16 <= y <= 31) or (x == 192 and y == 168) \
            or (x == 100 and 64 <= y <= 127)
    if a.ipv4_mapped is not None:
        return True
    first = a.packed[0]
    return a in (ipaddress.IPv6Address("::1"), ipaddress.IPv6Address("::")) or first in (0xFC, 0xFD) \
        or (first == 0xFE and 0x80 <= a.packed[1] <= 0xBF)


def private_host(host: str) -> bool:
    """A host a webhook may not point at in production: this machine, private networks and cloud metadata. Unlike the
    hosted app, a name's trailing dot ("localhost.") doesn't get it past the check."""
    h = host.strip("[]").lower()
    name = h[:-1] if h.endswith(".") else h
    if name == "localhost" or name.endswith((".localhost", ".local", ".internal")):
        return True
    try:
        return _private_ip(ipaddress.ip_address(h))
    except ValueError:
        return False


def check_target(kind: str, raw: str, allow_local: bool = False) -> dict:
    """The target as stored, {"ok": True, "target"}, or what is wrong with it, {"ok": False, "error"}. In production a
    webhook must be https on a public host, and a Slack URL one of Slack's incoming webhooks; ``allow_local`` lets
    either be plain http on this machine or a private network."""
    text = raw.strip()
    if kind == "email":
        lst = list(dict.fromkeys(e.lower() for e in re.split(r"[\s,;]+", text) if e))
        if not lst:
            return {"ok": False, "error": "Enter at least one email address."}
        if len(lst) > MAX_EMAILS:
            return {"ok": False, "error": f"At most {MAX_EMAILS} addresses on one channel."}
        bad = next((e for e in lst if not _EMAIL.match(e)), None)
        return {"ok": False, "error": f'"{bad}" is not an email address.'} if bad else {"ok": True, "target": ", ".join(lst)}
    u = parse_url(text)
    if u is None:
        return {"ok": False, "error": "Enter the full URL, starting with https://."}
    if u.userinfo not in ("", ":"):
        return {"ok": False, "error": "Put credentials in the URL's path or query, not before the host."}
    local = allow_local and u.scheme == "http" and private_host(u.host)
    if u.scheme != "https" and not local:
        return {"ok": False, "error": "The URL must start with https://."}
    if kind == "slack":
        if not local and (u.host != "hooks.slack.com" or not u.path.startswith("/services/")):
            return {"ok": False, "error": "Use a Slack incoming webhook URL: https://hooks.slack.com/services/…"}
    elif not local and private_host(u.host):
        return {"ok": False, "error": "The webhook must be on a public host, not a private or local address."}
    return {"ok": True, "target": str(u)}


def shown_target(c: dict) -> str:
    """A Slack URL is a secret by itself, and a webhook's path or query is where the form says to put credentials, so
    lists and logs show a Slack URL only up to its team and a webhook only up to its host."""
    if c["kind"] == "slack":
        return re.sub(r"(/services/[^/]+/).*$", r"\1…", c["target"], count=1)
    if c["kind"] != "webhook":
        return c["target"]
    u = parse_url(c["target"])
    if u is None or not u.host:
        return "…"
    return u.origin if u.path == "/" and not u.query and not u.fragment else f"{u.origin}/…"


# ── how a channel is sent to ─────────────────────────────────────────────────────────────────────────────────────

@dataclass
class SmtpConfig:
    """The operator's mail server. ``tls``: "starttls" (upgrade a plain connection, port 587), "ssl" (TLS from the
    start, port 465) or "none" (a relay on a trusted network; no sign-in then, so a password never goes in clear).
    Certificates are checked against the system's trust store."""
    host: str
    sender: str
    port: int = 587
    tls: str = "starttls"
    user: Optional[str] = None
    password: Optional[str] = field(default=None, repr=False)

    def __post_init__(self):
        if self.tls not in ("starttls", "ssl", "none"):
            raise ValueError('tls is "starttls", "ssl" or "none"')
        if self.user and self.tls == "none":
            raise ValueError("signing in to the mail server needs tls (starttls or ssl)")
        if not _EMAIL.match(self.sender.rpartition("<")[2].rstrip(">").strip()):
            raise ValueError("sender is not an email address")


@dataclass
class Delivery:
    """What sending needs besides the channel. ``base``: where links point, the app's own URL (see alert_messages)."""
    base: str
    smtp: Optional[SmtpConfig] = None
    timeout_s: float = TIMEOUT_S
    allow_local: bool = False
    now: Callable[[], int] = field(default=lambda: int(time.time() * 1000))
    #: the TLS settings for https and SMTP; the default checks certificates against the system's trust store
    ssl_context: Optional[ssl.SSLContext] = field(default=None, repr=False)

    def context(self) -> ssl.SSLContext:
        return self.ssl_context or ssl.create_default_context()


class _Guarded:
    """http.client connections that refuse a private peer (unless allowed) and can be cut off at the deadline."""
    allow_local = False
    aborted = False

    def _tcp(self):
        """Connects, and checks the address connected to before anything (not even TLS) is sent."""
        http.client.HTTPConnection.connect(self)  # type: ignore[arg-type]
        peer = ipaddress.ip_address(self.sock.getpeername()[0].split("%")[0])  # type: ignore[attr-defined]
        if not self.allow_local and _private_ip(peer):
            raise DeliveryError("The webhook's host is a private or local address.")

    def abort(self):
        self.aborted = True
        sock = getattr(self, "sock", None)
        if sock is not None:
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass


class _Http(_Guarded, http.client.HTTPConnection):
    def connect(self):
        self._tcp()


class _Https(_Guarded, http.client.HTTPSConnection):
    def connect(self):
        self._tcp()
        self.sock = self._context.wrap_socket(self.sock, server_hostname=self.host)  # type: ignore[attr-defined]


def _post(url: str, body: str, headers: dict, d: Delivery) -> None:
    u = urlsplit(url)
    if u.scheme == "https":
        conn: _Guarded = _Https(u.hostname, u.port, timeout=d.timeout_s, context=d.context())
    else:
        conn = _Http(u.hostname, u.port, timeout=d.timeout_s)
    conn.allow_local = d.allow_local
    timer = threading.Timer(d.timeout_s, conn.abort)
    timer.daemon = True
    timer.start()
    try:
        conn.request("POST", (u.path or "/") + (f"?{u.query}" if u.query else ""), body=body.encode("utf-8"),
                     headers={"content-type": "application/json", "user-agent": USER_AGENT, **headers})
        r = conn.getresponse()
        status, reason = r.status, r.reason
    except DeliveryError:
        raise
    except (socket.timeout, TimeoutError):
        raise DeliveryError("The endpoint did not answer in time.") from None
    except ssl.SSLError as e:
        raise DeliveryError(f"The endpoint's TLS failed: {e.reason or type(e).__name__}.") from None
    except (http.client.HTTPException, OSError) as e:
        if conn.aborted:
            raise DeliveryError("The endpoint did not answer in time.") from None
        if isinstance(e, http.client.HTTPException):
            raise DeliveryError("The endpoint's answer was not HTTP.") from None
        raise DeliveryError(f"The endpoint could not be reached: {e.strerror or type(e).__name__}.") from None
    finally:
        timer.cancel()
        conn.close()
    if status < 200 or status >= 300:
        raise DeliveryError(f"The endpoint answered {status}{f' {reason}' if reason else ''}.")


def _email(d: Delivery, to: list, mail: dict) -> None:
    cfg = d.smtp
    if cfg is None:
        raise DeliveryError("No mail server is set up for alert emails.")
    subject = re.sub(r"[\r\n]+", " ", mail["subject"])
    try:
        if cfg.tls == "ssl":
            s: smtplib.SMTP = smtplib.SMTP_SSL(cfg.host, cfg.port, timeout=d.timeout_s, context=d.context())
        else:
            s = smtplib.SMTP(cfg.host, cfg.port, timeout=d.timeout_s)
        try:
            if cfg.tls == "starttls":
                s.starttls(context=d.context())
            if cfg.user:
                s.login(cfg.user, cfg.password or "")
            domain = cfg.sender.rpartition("@")[2].rstrip(">").strip() or None
            for addr in to:
                msg = EmailMessage()
                msg["From"] = cfg.sender
                msg["To"] = addr
                msg["Subject"] = subject
                msg["Date"] = formatdate(usegmt=True)
                msg["Message-ID"] = make_msgid(domain=domain)
                msg["Auto-Submitted"] = "auto-generated"
                msg.set_content(mail["text"])
                msg.add_alternative(mail["html"], subtype="html")
                try:
                    s.send_message(msg, to_addrs=[addr])
                except smtplib.SMTPRecipientsRefused:
                    raise DeliveryError(f"The mail server refused {addr}.") from None
        finally:
            try:
                s.quit()
            except (smtplib.SMTPException, OSError):
                s.close()
    except DeliveryError:
        raise
    except smtplib.SMTPAuthenticationError:
        raise DeliveryError("The mail server refused the user name or password.") from None
    except smtplib.SMTPNotSupportedError:
        raise DeliveryError("The mail server does not offer STARTTLS or the sign-in it needs.") from None
    except smtplib.SMTPResponseException as e:
        raise DeliveryError(f"The mail server answered {e.smtp_code}.") from None
    except smtplib.SMTPServerDisconnected as e:
        # smtplib turns a read that timed out into "Connection unexpectedly closed: timed out"
        if "timed out" in str(e):
            raise DeliveryError("The mail server did not answer in time.") from None
        raise DeliveryError("The mail server closed the connection.") from None
    except (socket.timeout, TimeoutError):
        raise DeliveryError("The mail server did not answer in time.") from None
    except ssl.SSLError as e:
        raise DeliveryError(f"The mail server's TLS failed: {e.reason or type(e).__name__}.") from None
    except smtplib.SMTPException as e:
        raise DeliveryError(f"The mail server failed: {type(e).__name__}.") from None
    except OSError as e:
        raise DeliveryError(f"The mail server could not be reached: {e.strerror or type(e).__name__}.") from None


def send_to(c: dict, ws: dict, changes: list, d: Delivery) -> None:
    """Sends changes to one channel. Raises DeliveryError with a plain reason when it fails."""
    t = check_target(c["kind"], c["target"], d.allow_local) if c["kind"] in CHANNEL_KINDS else {"ok": False, "error": "Unknown channel kind."}
    if not t["ok"]:
        raise DeliveryError(t["error"])
    at = d.now()
    if c["kind"] == "webhook":
        body = m.webhook_body(ws, changes, d.base, at)
        ts = str(at)
        return _post(c["target"], body, {"x-trailbeam-timestamp": ts, "x-trailbeam-signature": m.sign(c.get("secret") or "", ts, body)}, d)
    if c["kind"] == "slack":
        return _post(c["target"], m.slack_body(ws, changes, d.base, at), {}, d)
    return _email(d, m.recipients(c), m.email_of(c, ws, changes, d.base, at))


def _scrub(detail: str, c: dict, d: Delivery) -> str:
    """No secret in a detail, whatever a library put in its error."""
    for s in (c.get("secret"), c.get("target") if c["kind"] != "email" else None, d.smtp.password if d.smtp else None):
        if s:
            detail = detail.replace(s, "…")
    return detail


def deliver(channels: list, ws: dict, changes: list, d: Delivery) -> list:
    """Delivers a check's changes to every channel that wants them. Returns each outcome, {"channel": its ID, "ok",
    "detail": why it failed or None, "at": ms}, for the caller to note on the channel."""
    out = []
    for c in channels:
        mine = [x for x in changes if m.wants(c, x)]
        if not mine:
            continue
        ok, detail = True, None
        try:
            send_to(c, ws, mine, d)
        except DeliveryError as e:
            ok, detail = False, _scrub(str(e), c, d)
        except Exception as e:  # a bug must not stop the other channels, nor leak a secret through its message
            # (logged by type only: its message or traceback could hold the target or a password)
            ok, detail = False, f"Sending failed: {type(e).__name__}."
        if ok:
            log.info("alert channel %s (%s): sent %d change(s)", c.get("id"), shown_target(c), len(mine))
        else:
            log.warning("alert channel %s (%s): %s", c.get("id"), shown_target(c), detail)
        out.append({"channel": c.get("id"), "ok": ok, "detail": detail[:300] if detail else None, "at": d.now()})
    return out
