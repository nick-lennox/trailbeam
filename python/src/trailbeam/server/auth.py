"""The shared token (``TRAILBEAM_TOKEN`` or ``--token-file``): one secret that, when set, every request needs.

Off by default, for running on one's own machine. When it is set:

- ingest (``POST /v1/traces``) needs ``Authorization: Bearer <token>``. OpenTelemetry exporters send it from
  ``OTEL_EXPORTER_OTLP_HEADERS``, the Collector from its exporter's ``headers``;
- the API and every page need the same header, or the session cookie the login page sets. A browser opening
  any page without either gets the login page (``/login``), which asks for the token once;
- ``GET /api/health`` answers without it, for health checks, but then says only ``status`` and ``version``.

The cookie holds an HMAC of the token, not the token: a leaked cookie works on the UI and API, never on ingest,
and changing the token ends every session. It is ``HttpOnly`` and ``SameSite=Strict``. It carries ``Secure``
when the request came over HTTPS (a proxy saying so in ``X-Forwarded-Proto``); put Trailbeam behind TLS when
the token crosses a network you don't trust.

One token for everyone: there are no users, roles or audit log (those are in the hosted app).
"""
from __future__ import annotations

import hashlib
import hmac
from http.cookies import CookieError, SimpleCookie
from typing import Optional

COOKIE = "trailbeam_session"
MIN_LENGTH = 16
MAX_AGE_S = 30 * 86_400


class TokenError(ValueError):
    pass


def check_token(token: str) -> str:
    """The token as given, if it is usable. Raises TokenError, saying why, otherwise."""
    t = token.strip()
    if len(t) < MIN_LENGTH:
        raise TokenError(f"the token must be at least {MIN_LENGTH} characters (for example the output of "
                         "openssl rand -hex 32); got " + (f"{len(t)}" if t else "an empty one"))
    if any(c.isspace() or not c.isprintable() or not c.isascii() for c in t):
        raise TokenError("the token must be printable ASCII without spaces: it goes in an HTTP header")
    return t


def read_token_file(path: str) -> str:
    try:
        with open(path, encoding="utf-8") as f:
            return check_token(f.read())
    except OSError as e:
        raise TokenError(f"cannot read the token file {path}: {e.strerror or e}") from None


class Auth:
    def __init__(self, token: str):
        self._token = check_token(token).encode()
        self._session = hmac.new(self._token, b"trailbeam ui session", hashlib.sha256).hexdigest()

    def bearer_ok(self, authorization: Optional[str]) -> bool:
        scheme, _, value = (authorization or "").strip().partition(" ")
        return scheme.lower() == "bearer" and hmac.compare_digest(value.strip().encode(), self._token)

    def token_ok(self, text: str) -> bool:
        return hmac.compare_digest(text.strip().encode(), self._token)

    def session_ok(self, cookie_header: Optional[str]) -> bool:
        if not cookie_header:
            return False
        try:
            c = SimpleCookie(cookie_header)
        except CookieError:
            return False
        m = c.get(COOKIE)
        return m is not None and hmac.compare_digest(m.value.encode(), self._session.encode())

    def request_ok(self, headers) -> bool:
        """The API and pages: the bearer header or the session cookie."""
        return self.bearer_ok(headers.get("Authorization")) or self.session_ok(headers.get("Cookie"))

    def set_cookie(self, secure: bool) -> str:
        return (f"{COOKIE}={self._session}; Path=/; HttpOnly; SameSite=Strict; Max-Age={MAX_AGE_S}"
                + ("; Secure" if secure else ""))


def clear_cookie() -> str:
    return f"{COOKIE}=; Path=/; HttpOnly; SameSite=Strict; Max-Age=0"


def safe_next(path: Optional[str]) -> str:
    """Where to go after logging in: a path on this server, never another site."""
    p = path or "/"
    if not p.startswith("/") or p.startswith("//") or "\\" in p or any(ord(c) < 0x20 for c in p):
        return "/"
    return p
