"""Email from the server (QV-587): the operator's mail server, and the invite and password reset emails the People
page sends when one is set up. Sending is alert_delivery's SMTP sender, the one alert email channels use.

The mail server is the operator's setting, from the environment, never the API's::

    TRAILBEAM_SMTP_HOST=smtp.example.com       # off unless set
    TRAILBEAM_SMTP_FROM="Trailbeam <trailbeam@example.com>"
    TRAILBEAM_SMTP_PORT=587                    # default 587 (465 with TRAILBEAM_SMTP_TLS=ssl, 25 with none)
    TRAILBEAM_SMTP_TLS=starttls                # starttls, ssl or none
    TRAILBEAM_SMTP_USER=trailbeam              # optional; needs starttls or ssl
    TRAILBEAM_SMTP_PASSWORD_FILE=/run/secrets/smtp   # or the password itself in TRAILBEAM_SMTP_PASSWORD

A setting that doesn't make sense stops the server at start (``SmtpSettingsError``), so a typo never quietly means
"no email". Without TRAILBEAM_SMTP_HOST the server sends nothing, and the People page shows each one-time link for
the admin to pass on, as before.

**Links in emails** start with the public URL (``--public-url``, TRAILBEAM_PUBLIC_URL) when it is set, else the
address the admin's request came to. An email goes to someone else, so the public URL is the one to set.
"""
from __future__ import annotations

import html
import os
import urllib.parse
from dataclasses import dataclass
from typing import Callable, Mapping, Optional

from . import alert_delivery
from .alert_delivery import DeliveryError, SmtpConfig

ENV = "TRAILBEAM_SMTP_HOST"
TIMEOUT_S = 10.0
DEFAULT_PORT = {"starttls": 587, "ssl": 465, "none": 25}

__all__ = ["DeliveryError", "Mailer", "SmtpSettingsError", "check_public_url", "invite_email", "reset_email",
           "smtp_from_env"]


class SmtpSettingsError(ValueError):
    pass


def smtp_from_env(env: Optional[Mapping[str, str]] = None) -> Optional[SmtpConfig]:
    """The mail server from TRAILBEAM_SMTP_*, or None when TRAILBEAM_SMTP_HOST is not set. Raises
    SmtpSettingsError naming the setting that is wrong."""
    env = os.environ if env is None else env

    def get(name: str) -> Optional[str]:
        v = (env.get(f"TRAILBEAM_SMTP_{name}") or "").strip()
        return v or None

    host = get("HOST")
    if host is None:
        others = [f"TRAILBEAM_SMTP_{n}" for n in ("FROM", "PORT", "TLS", "USER", "PASSWORD", "PASSWORD_FILE")
                  if get(n) is not None]
        if others:
            raise SmtpSettingsError(f"{', '.join(others)} {'is' if len(others) == 1 else 'are'} set, but not "
                                    f"{ENV}: set the mail server's host too, or unset them")
        return None
    sender = get("FROM")
    if sender is None:
        raise SmtpSettingsError("TRAILBEAM_SMTP_FROM is required with TRAILBEAM_SMTP_HOST: the address emails come "
                                'from, such as "Trailbeam <trailbeam@example.com>"')
    tls = (get("TLS") or "starttls").lower()
    if tls not in DEFAULT_PORT:
        raise SmtpSettingsError(f'TRAILBEAM_SMTP_TLS is "starttls", "ssl" or "none", not {get("TLS")!r}')
    port_s = get("PORT")
    if port_s is None:
        port = DEFAULT_PORT[tls]
    elif not port_s.isdigit() or not 1 <= int(port_s) <= 65535:
        raise SmtpSettingsError(f"TRAILBEAM_SMTP_PORT is a port number from 1 to 65535, not {port_s!r}")
    else:
        port = int(port_s)
    user = get("USER")
    password_file = get("PASSWORD_FILE")
    password = env.get("TRAILBEAM_SMTP_PASSWORD") or None
    if password_file and password:
        raise SmtpSettingsError("set TRAILBEAM_SMTP_PASSWORD_FILE or TRAILBEAM_SMTP_PASSWORD, not both")
    if password_file:
        try:
            with open(password_file, encoding="utf-8") as f:
                password = f.read().strip("\r\n")
        except OSError as e:
            raise SmtpSettingsError(f"TRAILBEAM_SMTP_PASSWORD_FILE {password_file}: cannot read it: "
                                    f"{e.strerror or e}") from None
        if not password:
            raise SmtpSettingsError(f"TRAILBEAM_SMTP_PASSWORD_FILE {password_file} is empty")
    if password and not user:
        raise SmtpSettingsError("a mail server password is set, but not TRAILBEAM_SMTP_USER")
    if user and not password:
        raise SmtpSettingsError("TRAILBEAM_SMTP_USER is set, but no password (TRAILBEAM_SMTP_PASSWORD_FILE or "
                                "TRAILBEAM_SMTP_PASSWORD)")
    try:
        return SmtpConfig(host=host, sender=sender, port=port, tls=tls, user=user, password=password)
    except ValueError as e:
        raise SmtpSettingsError(f"TRAILBEAM_SMTP_*: {e}") from None


def check_public_url(url: str) -> str:
    """--public-url as links start with it: an absolute http or https URL with no query or fragment. Single
    sign-on asks more of it (https, oidc.check_url). Raises SmtpSettingsError."""
    u = urllib.parse.urlsplit((url or "").strip())
    if u.scheme not in ("https", "http") or not u.hostname:
        raise SmtpSettingsError(f"--public-url (TRAILBEAM_PUBLIC_URL) must be an absolute URL such as "
                                f"https://trailbeam.example.com, got {url!r}")
    if u.query or u.fragment:
        raise SmtpSettingsError(f"--public-url (TRAILBEAM_PUBLIC_URL) must not have a query or fragment, got {url!r}")
    return url.strip().rstrip("/")


# ── the emails ────────────────────────────────────────────────────────────────────────────────────────────────────
# Inline styles and tables only, since mail clients drop <style>; the hosted app's colours (lib/auth/emails.ts),
# without its header and footer pictures, which a self-hosted server has nowhere public to serve from.

SKY, INK, INK2, INK3, LINE = "#0a0c22", "#f6f1e7", "#c6c5e0", "#9a99c2", "#2b2f63"
SANS = "-apple-system,'Segoe UI',Helvetica,Arial,sans-serif"
SERIF = "Georgia,'Times New Roman',serif"


def _layout(*, subject: str, preview: str, heading: str, body: str, button: str, url: str, after: str,
            site: str) -> dict:
    e = html.escape
    page = f"""<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<meta name="color-scheme" content="dark"><meta name="supported-color-schemes" content="dark"><title>{e(heading)}</title></head>
<body style="margin:0;padding:0;background:{SKY};">
  <div style="display:none;max-height:0;overflow:hidden;opacity:0;">{e(preview)}</div>
  <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background:{SKY};"><tr><td align="center" style="padding:32px 12px;">
    <table role="presentation" width="600" cellpadding="0" cellspacing="0" style="width:100%;max-width:600px;background:{SKY};border:1px solid {LINE};border-radius:20px;">
      <tr><td style="padding:32px 40px 0;font-family:{SERIF};color:{INK};font-size:20px;">Trailbeam</td></tr>
      <tr><td style="padding:20px 40px 0;font-family:{SERIF};color:{INK};font-size:32px;line-height:1.15;">{e(heading)}</td></tr>
      <tr><td style="padding:16px 40px 0;font-family:{SANS};color:{INK2};font-size:16px;line-height:1.6;">{e(body)}</td></tr>
      <tr><td style="padding:28px 40px 0;">
        <a href="{e(url)}" style="display:inline-block;background:{INK};color:{SKY};font-family:{SANS};text-decoration:none;font-weight:600;font-size:15px;padding:13px 24px;border-radius:999px;">{e(button)}</a>
      </td></tr>
      <tr><td style="padding:28px 40px 32px;font-family:{SANS};color:{INK3};font-size:13px;line-height:1.6;">
        {e(after)}<br><br>If the button doesn't work, paste this into your browser:<br>
        <a href="{e(url)}" style="color:{INK3};word-break:break-all;">{e(url)}</a>
      </td></tr>
    </table>
  </td></tr></table>
</body></html>"""
    text = f"{heading}\n\n{body}\n\n{button}: {url}\n\n{after}\n\nTrailbeam: {site}"
    return {"subject": subject, "text": text, "html": page}


def _who(name: Optional[str], email: Optional[str]) -> str:
    return (name or "").strip() or (email or "").strip() or "An admin"


def invite_email(url: str, site: str, inviter_name: Optional[str], inviter_email: Optional[str],
                 days: int) -> dict:
    """The invite: {subject, text, html}. ``site``: the server's address, for the line at the end."""
    who = _who(inviter_name, inviter_email)
    return _layout(
        subject="Join Trailbeam",
        preview=f"{who} invited you to Trailbeam.",
        heading="Join Trailbeam",
        body=f"{who} invited you to Trailbeam at {site}, where the team follows its Amazon SQS messages. "
             "Open the link to choose your password.",
        button="Accept the invitation",
        url=url, site=site,
        after=f"The invitation works once, for {days} days. If you weren't expecting it, you can ignore this email.")


def reset_email(url: str, site: str, hours: int) -> dict:
    """A password reset an admin made: {subject, text, html}."""
    return _layout(
        subject="Reset your Trailbeam password",
        preview="Set a new password for your Trailbeam account.",
        heading="Choose a new password",
        body=f"An admin made a link to reset the password on your Trailbeam account at {site}. Open it to set a "
             "new one.",
        button="Set a new password",
        url=url, site=site,
        after=f"The link works once, for {hours} hours. Until you use it, your password hasn't changed.")


@dataclass
class Mailer:
    """Sends the server's emails through the operator's mail server. ``send`` is replaceable for tests."""
    smtp: SmtpConfig
    timeout_s: float = TIMEOUT_S
    send: Optional[Callable[[str, dict], None]] = None

    def __call__(self, to: str, mail: dict) -> None:
        """Sends one email. Raises DeliveryError with a plain reason."""
        if self.send is not None:
            self.send(to, mail)
            return
        alert_delivery.send_email(alert_delivery.Delivery(base="", smtp=self.smtp, timeout_s=self.timeout_s),
                                  [to], mail)
