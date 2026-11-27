"""Sign-in through a proxy (users and roles, phase 3a; proposal on QV-149, section 1): oauth2-proxy, Pomerium,
Cloudflare Access and similar sign people in, and Trailbeam believes the email header the proxy adds.

**Off by default.** ``--proxy-auth-header X-Forwarded-Email`` (env ``TRAILBEAM_PROXY_AUTH_HEADER``) turns it on,
and the header is believed only from the proxy:

- **By address:** the request's TCP peer must be in ``--trusted-proxy`` (proxy.py), when that is set.
- **By shared secret:** with ``--proxy-auth-secret-file`` (or ``TRAILBEAM_PROXY_AUTH_SECRET``), the request must
  carry ``X-Trailbeam-Proxy-Secret: <the secret>``, which the proxy adds. For proxies whose address isn't fixed.

At least one of the two must be set, or the server doesn't start; with both, both must pass. A request that
fails them is treated as if it had no email header: anyone can send ``X-Forwarded-Email``, so it means
something only when the proxy added it. The proxy must also remove the header from what the browser sends (the
proxies above do), and Trailbeam must not be reachable except through it, or through addresses in
``--trusted-proxy``.

**Who it is.** The header holds one email (two headers are refused; Google IAP's ``accounts.google.com:`` prefix
is dropped). It is matched, ignoring case, with a person:

- **Nobody with that email:** a new person is created, a **member**, never an admin, with no password, and with
  the name from ``--proxy-auth-name-header`` if set, else the email's part before ``@``. An admin can make them an
  admin later (``trailbeam users set-role``).
- **A person the proxy created, or an admin linked** (``trailbeam users link-proxy``): signed in as them, with their
  role. A disabled person is refused.
- **A person with a local account who isn't linked:** refused (403), so whoever the proxy lets in with the same
  email can't take over a local account, an admin's included.

Only once the server has an admin (access.py): before that, accounts aren't used and the header changes nothing.

There is no session: every request through the proxy carries the header. What the store says about a person
(their role, disabled or not, linked or not) is kept ``CACHE_TTL_S`` per process, so a change reaches every
replica within 20 s, like a session's. Sign out ends the proxy's own session only if ``--proxy-auth-sign-out-url``
says where (oauth2-proxy: ``/oauth2/sign_out``).

Checking the proxy's signed token (Cloudflare Access, Pomerium, Google IAP, AWS ALB) instead of a plain header is
not here yet: it needs the ``sso`` extra of phase 3b (OIDC).
"""
from __future__ import annotations

import hmac
import logging
import re
import threading
import time
from collections import OrderedDict
from dataclasses import dataclass
from typing import Callable, Optional

from .accounts import AccountError, EmailTaken, check_email, check_name
from .proxy import TrustedProxies

log = logging.getLogger("trailbeam.server")

SECRET_HEADER = "X-Trailbeam-Proxy-Secret"
GOOGLE_IAP_PREFIX = "accounts.google.com:"  # Google IAP's email header: accounts.google.com:ada@example.com
CACHE_TTL_S = 20.0
CACHE_MAX = 10_000
WARN_EVERY_S = 60.0
MIN_SECRET = 16
_TOKEN = re.compile(r"[!#$%&'*+.^_`|~0-9A-Za-z-]+")  # an HTTP header name (RFC 9110 token)
# headers that already mean something here, or that a client sets: never an email header
_RESERVED = {"authorization", "cookie", "host", "x-trailbeam-key", SECRET_HEADER.lower(), "x-forwarded-for",
             "x-forwarded-proto", "x-forwarded-host", "content-type", "content-length", "origin", "referer"}

NOT_LINKED = ("{email} has a local account, which can't be signed in to through the proxy until an admin links it: "
              "trailbeam users link-proxy {email}. Or sign in with its password.")
DISABLED = "this account is disabled: ask an admin"
TWO_EMAILS = "the proxy sent more than one {header} header"
BAD_EMAIL = "the proxy's {header} header is not an email address"


class ProxyAuthError(ValueError):
    """The settings can't work; the message says why (the server doesn't start)."""


class ProxyRefused(Exception):
    """The proxy named someone who may not sign in this way: answer 403 with ``message``."""

    def __init__(self, message: str):
        super().__init__(message)
        self.message = message


@dataclass(frozen=True)
class ProxyAuthConfig:
    header: str
    name_header: Optional[str] = None
    secret: Optional[str] = None
    sign_out_url: Optional[str] = None
    by_address: bool = False  # --trusted-proxy is set: the peer must be in it


def check_header(name: str, what: str) -> str:
    n = (name or "").strip()
    if not _TOKEN.fullmatch(n):
        raise ProxyAuthError(f"{what}: {name!r} is not a header name")
    if n.lower() in _RESERVED:
        raise ProxyAuthError(f"{what}: {n} can't carry the email, it already means something else")
    return n


def check_sign_out_url(url: str) -> str:
    u = (url or "").strip()
    if not (u.startswith("/") and not u.startswith("//") or u.startswith("https://") or u.startswith("http://")):
        raise ProxyAuthError(f"the sign-out URL must be a path such as /oauth2/sign_out or an http(s) URL, got {url!r}")
    if any(c in u for c in "\r\n"):
        raise ProxyAuthError("the sign-out URL can't contain line breaks")
    return u


def configure(header: Optional[str], *, name_header: Optional[str] = None, secret: Optional[str] = None,
              sign_out_url: Optional[str] = None, proxies: TrustedProxies = TrustedProxies()) -> Optional[ProxyAuthConfig]:
    """The settings, checked; None when ``header`` isn't set (off). Raises ProxyAuthError."""
    if not (header or "").strip():
        for v, flag in ((name_header, "--proxy-auth-name-header"), (secret, "--proxy-auth-secret-file"),
                        (sign_out_url, "--proxy-auth-sign-out-url")):
            if v:
                raise ProxyAuthError(f"{flag} needs --proxy-auth-header (TRAILBEAM_PROXY_AUTH_HEADER)")
        return None
    h = check_header(header, "--proxy-auth-header")
    nh = check_header(name_header, "--proxy-auth-name-header") if name_header else None
    if nh is not None and nh.lower() == h.lower():
        raise ProxyAuthError("--proxy-auth-name-header must be another header than --proxy-auth-header")
    if secret is not None:
        secret = secret.strip()
        if len(secret) < MIN_SECRET or not secret.isascii() or not secret.isprintable():
            raise ProxyAuthError(f"the proxy's shared secret must be at least {MIN_SECRET} printable ASCII characters "
                                 "(openssl rand -hex 32 makes one)")
    if not proxies and secret is None:
        raise ProxyAuthError(f"--proxy-auth-header {h} needs the proxy's addresses (--trusted-proxy) or a shared "
                             f"secret it sends in {SECRET_HEADER} (--proxy-auth-secret-file), or anyone who reaches "
                             "this port could sign in as anyone")
    return ProxyAuthConfig(h, nh, secret, check_sign_out_url(sign_out_url) if sign_out_url else None, bool(proxies))


def _all(headers, name: str) -> list[str]:
    """Every value of a header, ignoring case: http.server's headers, or a plain dict in tests."""
    if hasattr(headers, "get_all"):
        return list(headers.get_all(name) or [])
    low = name.lower()
    return [v for k, v in headers.items() if k.lower() == low]


class ProxySignIn:
    """Who the proxy says a request is. Thread-safe; one per process (the cache is per process)."""

    def __init__(self, config: ProxyAuthConfig, accounts, proxies: TrustedProxies, *,
                 now_ms: Callable[[], int] = lambda: time.time_ns() // 1_000_000,
                 clock: Callable[[], float] = time.monotonic, cache_ttl_s: float = CACHE_TTL_S):
        self.config = config
        self.accounts = accounts
        self.proxies = proxies
        self.now_ms = now_ms
        self.clock = clock
        self.cache_ttl_s = cache_ttl_s
        self._cache: OrderedDict = OrderedDict()  # lower(email) -> (looked up at, user row or None)
        self._lock = threading.Lock()
        self._warned_at = float("-inf")

    def trusted(self, headers, peer: str) -> bool:
        """The request came from the proxy: its peer is a --trusted-proxy (if set), and it has the secret (if set)."""
        if self.config.by_address and not self.proxies.trusts(peer):
            return False
        if self.config.secret is not None:
            sent = _all(headers, SECRET_HEADER)
            if len(sent) != 1 or not hmac.compare_digest(sent[0].strip().encode("utf-8", "surrogateescape"),
                                                         self.config.secret.encode()):
                return False
        return True

    def email(self, headers, peer: str) -> Optional[str]:
        """The email the proxy names, or None: no header, or not from the proxy. Raises ProxyRefused."""
        values = _all(headers, self.config.header)
        if not values:
            return None
        if not self.trusted(headers, peer):
            self._warn(peer)
            return None
        if len(values) != 1:
            raise ProxyRefused(TWO_EMAILS.format(header=self.config.header))
        v = values[0].strip()
        if v[:len(GOOGLE_IAP_PREFIX)].lower() == GOOGLE_IAP_PREFIX:  # X-Goog-Authenticated-User-Email
            v = v[len(GOOGLE_IAP_PREFIX):]
        try:
            return check_email(v)
        except AccountError:
            raise ProxyRefused(BAD_EMAIL.format(header=self.config.header)) from None

    def user(self, headers, peer: str) -> Optional[dict]:
        """The person the proxy names (a ``users`` row), created as a member the first time; None when the request
        doesn't come from the proxy or has no email. Raises ProxyRefused, or the store's error."""
        email = self.email(headers, peer)
        if email is None:
            return None
        u = self._lookup(email)
        if u is None:
            u = self._create(email, headers)
        if u["proxy_linked_at_ms"] is None:
            raise ProxyRefused(NOT_LINKED.format(email=u["email"]))
        if u["disabled_at_ms"] is not None:
            raise ProxyRefused(DISABLED)
        return u

    def _lookup(self, email: str) -> Optional[dict]:
        key = email.lower()
        t = self.clock()
        with self._lock:
            hit = self._cache.get(key)
            if hit is not None and t - hit[0] < self.cache_ttl_s and hit[1] is not None:
                self._cache.move_to_end(key)
                return hit[1]
        u = self.accounts.user_by_email(email)
        if u is not None:
            self._remember(key, t, u)
        return u

    def _remember(self, key: str, t: float, u: dict) -> None:
        with self._lock:
            self._cache[key] = (t, u)
            self._cache.move_to_end(key)
            while len(self._cache) > CACHE_MAX:
                self._cache.popitem(last=False)

    def _create(self, email: str, headers) -> dict:
        name = email.split("@", 1)[0] or email
        if self.config.name_header:
            sent = _all(headers, self.config.name_header)
            if len(sent) == 1:
                try:
                    name = check_name(sent[0])
                except AccountError:
                    pass
        try:
            uid = self.accounts.create_user(email, name, "member", None, self.now_ms(), proxy=True)
            log.info("proxy sign-in: created member %s (first sign-in through the proxy)", uid)
        except EmailTaken:  # another request or replica created them a moment ago
            pass
        u = self.accounts.user_by_email(email)
        if u is None:  # removed in between
            raise ProxyRefused("this account was just removed: ask an admin")
        self._remember(email.lower(), self.clock(), u)
        return u

    def forget(self, email: Optional[str] = None) -> None:
        """Drop this process's cached people (one, or all): after a change this process made."""
        with self._lock:
            if email is None:
                self._cache.clear()
            else:
                self._cache.pop(email.lower(), None)

    def _warn(self, peer: str) -> None:
        t = self.clock()
        with self._lock:
            if t - self._warned_at < WARN_EVERY_S:
                return
            self._warned_at = t
        why = (f"{peer} is not in --trusted-proxy" if self.config.by_address and not self.proxies.trusts(peer)
               else f"the request lacks the right {SECRET_HEADER}")
        log.warning("ignored %s from %s: %s (the request is treated as signed out; said at most once a minute)",
                    self.config.header, peer, why)
