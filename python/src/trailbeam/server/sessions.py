"""Signed-in browsers: sessions kept in the store (the ``sessions`` table), with a short cache in each process.

- A session ID is 32 random bytes (``secrets``), sent to the browser in the cookie and never stored: the store
  keeps its SHA-256. A slow hash isn't needed, because the ID is random, not chosen by a person.
- Sessions last ``LIFETIME_MS`` (30 days, as the shared-token cookie does) from sign-in. The expiry is fixed; a
  request never extends it, so page views don't write. ``last_seen_ms`` is written at most once an hour
  (``TOUCH_EVERY_MS``).
- Each process keeps what it looked up for ``CACHE_TTL_S`` (20 s), sessions that don't exist included, so
  pages and API calls don't each read the store. Signing out, changing a person's password or role, disabling
  or removing them deletes their sessions in the store, and this process forgets them at once. **Other
  replicas see it within ``CACHE_TTL_S``**, when their cache entry expires.
- The cookie is new: ``trailbeam_sid``, not the shared token's ``trailbeam_session`` (auth.py), so old cookies
  never pass as a session. Over HTTPS it is ``__Host-trailbeam_sid`` with ``Secure``: the browser then refuses
  it from plain HTTP and from other subdomains. Always ``HttpOnly``, ``SameSite=Strict`` and ``Path=/``.

HTTPS means the request came over TLS to this server, or a proxy in ``--trusted-proxy`` said so in
``X-Forwarded-Proto`` (proxy.py).
"""
from __future__ import annotations

import hashlib
import secrets
import threading
import time
from collections import OrderedDict
from http.cookies import CookieError, SimpleCookie
from typing import Callable, NamedTuple, Optional

COOKIE = "trailbeam_sid"
SECURE_COOKIE = "__Host-" + COOKIE
ID_BYTES = 32
LIFETIME_MS = 30 * 86_400_000
TOUCH_EVERY_MS = 3_600_000
CACHE_TTL_S = 20.0
CACHE_MAX = 10_000


class SessionUser(NamedTuple):
    """Who a session belongs to, as the store said at most CACHE_TTL_S ago."""
    user_id: int
    email: str
    name: str
    role: str
    expires_at_ms: int


def new_id() -> str:
    return secrets.token_urlsafe(ID_BYTES)


def id_hash(session_id: str) -> str:
    return hashlib.sha256(session_id.encode("utf-8", "surrogatepass")).hexdigest()


def cookie_value(cookie_header: Optional[str]) -> Optional[str]:
    """The session ID in a Cookie header: ``__Host-trailbeam_sid`` if there, else ``trailbeam_sid``."""
    if not cookie_header:
        return None
    try:
        c = SimpleCookie(cookie_header)
    except CookieError:
        return None
    for name in (SECURE_COOKIE, COOKIE):
        m = c.get(name)
        if m is not None and 20 <= len(m.value) <= 100:
            return m.value
    return None


def set_cookie(session_id: str, secure: bool, max_age_s: int = LIFETIME_MS // 1000) -> str:
    name = SECURE_COOKIE if secure else COOKIE
    return (f"{name}={session_id}; Path=/; HttpOnly; SameSite=Strict; Max-Age={max(0, int(max_age_s))}"
            + ("; Secure" if secure else ""))


def clear_cookie(secure: bool) -> str:
    return set_cookie("", secure, 0)


class Sessions:
    """Start, look up and end sessions. Thread-safe; one per process (the cache is per process)."""

    def __init__(self, accounts, *, now_ms: Callable[[], int] = lambda: time.time_ns() // 1_000_000,
                 cache_ttl_s: float = CACHE_TTL_S, clock: Callable[[], float] = time.monotonic,
                 lifetime_ms: int = LIFETIME_MS):
        self.accounts = accounts
        self.now_ms = now_ms
        self.cache_ttl_s = cache_ttl_s
        self.clock = clock
        self.lifetime_ms = lifetime_ms
        self._cache: OrderedDict = OrderedDict()  # id hash -> (looked up at, SessionUser or None, last_seen_ms)
        self._lock = threading.Lock()

    def start(self, user_id: int) -> tuple[str, int]:
        """A new session for this person: (the session ID for the cookie, its expiry in ms)."""
        sid = new_id()
        now = self.now_ms()
        expires = now + self.lifetime_ms
        self.accounts.create_session(id_hash(sid), user_id, now, expires)
        return sid, expires

    def lookup(self, session_id: Optional[str]) -> Optional[SessionUser]:
        """The person of a live session, or None."""
        if not session_id:
            return None
        h = id_hash(session_id)
        now = self.now_ms()
        t = self.clock()
        with self._lock:
            hit = self._cache.get(h)
            if hit is not None and t - hit[0] < self.cache_ttl_s:
                self._cache.move_to_end(h)
                user, last_seen = hit[1], hit[2]
                if user is None or user.expires_at_ms <= now:
                    return None
                touch = now - last_seen >= TOUCH_EVERY_MS
                if touch:
                    self._cache[h] = (hit[0], user, now)
                found = True
            else:
                found = False
        if not found:
            row = self.accounts.session(h, now)
            user = None if row is None else SessionUser(int(row["user_id"]), row["email"], row["name"], row["role"],
                                                        int(row["expires_at_ms"]))
            last_seen = 0 if row is None else int(row["last_seen_ms"])
            touch = user is not None and now - last_seen >= TOUCH_EVERY_MS
            with self._lock:
                self._cache[h] = (t, user, now if touch else last_seen)
                self._cache.move_to_end(h)
                while len(self._cache) > CACHE_MAX:
                    self._cache.popitem(last=False)
            if user is None:
                return None
        if touch:
            try:
                self.accounts.touch_session(h, now)
            except Exception:  # "last seen" is a hint; a busy store doesn't fail the request
                pass
        return user

    def end(self, session_id: Optional[str]) -> None:
        """Sign out: this session ends in the store and in this process at once, on other replicas within
        ``cache_ttl_s``."""
        if not session_id:
            return
        h = id_hash(session_id)
        self.accounts.end_session(h)
        self.forget(h)

    def end_all(self, user_id: int) -> None:
        """Every session of a person (their password or role changed, or they were removed)."""
        self.accounts.end_sessions(user_id)
        self.forget_user(user_id)

    def forget(self, h: str) -> None:
        with self._lock:
            self._cache.pop(h, None)

    def forget_user(self, user_id: int) -> None:
        """Drop this process's cached sessions of a person (after a change that ended them in the store)."""
        with self._lock:
            for h in [h for h, (_, u, _) in self._cache.items() if u is not None and u.user_id == user_id]:
                del self._cache[h]
