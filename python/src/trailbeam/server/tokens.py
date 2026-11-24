"""Ingest tokens (``tbi_…``) and personal API tokens (``tbp_…``): making them, and checking them with a short cache.

- A token is its prefix and ``RANDOM_CHARS`` (40) letters and digits from ``secrets``: about 238 bits. The store
  keeps its SHA-256 (hex) and its first 8 characters, for display; the whole token is shown once, when it is
  made. A slow hash isn't needed: the token is random, not chosen by a person.
- **Ingest tokens** can only send spans (``POST /v1/traces``), as ``x-trailbeam-key: tbi_…`` or
  ``Authorization: Bearer tbi_…``. They read nothing, so a leaked service config can't be used to read bodies.
- **Personal tokens** act as their owner, with the owner's role, on the API and ``/metrics`` (not on ingest). They
  stop working when they are revoked or expire, or the owner is disabled or removed.
- Each process caches what it looked up, tokens that don't exist included: ingest tokens for
  ``INGEST_CACHE_TTL_S`` (30 s), personal tokens for ``PERSONAL_CACHE_TTL_S`` (20 s, as sessions), at most
  ``CACHE_MAX`` of each. Revoking a token here forgets it at once; **other replicas stop accepting it when
  their cache entry expires**. A changed role reaches personal tokens the same way.
- "Last used" is written at most once every ``TOUCH_EVERY_MS`` (a minute) per token and replica, on a short
  thread of its own, so the request it came with never waits for the writer.
"""
from __future__ import annotations

import hashlib
import logging
import secrets
import string
import threading
import time
from collections import OrderedDict
from typing import Callable, NamedTuple, Optional

INGEST_PREFIX = "tbi_"
PERSONAL_PREFIX = "tbp_"
RANDOM_CHARS = 40
ALPHABET = string.ascii_letters + string.digits
DISPLAY_CHARS = 8
INGEST_CACHE_TTL_S = 30.0
PERSONAL_CACHE_TTL_S = 20.0
CACHE_MAX = 10_000
TOUCH_EVERY_MS = 60_000

log = logging.getLogger("trailbeam.server")


class TokenUser(NamedTuple):
    """The owner of a personal token, as the store said at most PERSONAL_CACHE_TTL_S ago."""
    user_id: int
    email: str
    name: str
    role: str
    token_id: int


def new_token(prefix: str) -> str:
    return prefix + "".join(secrets.choice(ALPHABET) for _ in range(RANDOM_CHARS))


def token_hash(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8", "surrogatepass")).hexdigest()


def well_formed(token: Optional[str], prefix: str) -> bool:
    """Has the shape of a token of this kind. Anything else is refused without a store lookup or a cache entry."""
    return (bool(token) and len(token) == len(prefix) + RANDOM_CHARS and token.startswith(prefix)
            and all(c in ALPHABET for c in token[len(prefix):]))


class _Cache:
    """hash -> (looked up at, value or None, last touched ms), oldest dropped first. Thread-safe."""

    def __init__(self, ttl_s: float, clock: Callable[[], float]):
        self.ttl_s = ttl_s
        self.clock = clock
        self._d: OrderedDict = OrderedDict()
        self._lock = threading.Lock()

    def get(self, h: str):
        """(found, value)."""
        with self._lock:
            hit = self._d.get(h)
            if hit is None or self.clock() - hit[0] >= self.ttl_s:
                return False, None
            self._d.move_to_end(h)
            return True, hit[1]

    def put(self, h: str, value) -> None:
        with self._lock:
            old = self._d.get(h)
            self._d[h] = (self.clock(), value, old[2] if old else 0)
            self._d.move_to_end(h)
            while len(self._d) > CACHE_MAX:
                self._d.popitem(last=False)

    def should_touch(self, h: str, now_ms: int, every_ms: int) -> bool:
        """True once per ``every_ms`` for a cached hash; marks it touched."""
        with self._lock:
            hit = self._d.get(h)
            if hit is None or now_ms - hit[2] < every_ms:
                return False
            self._d[h] = (hit[0], hit[1], now_ms)
            return True

    def forget(self, match: Callable[[object], bool], unknown: bool = False) -> None:
        """Drop the entries ``match`` picks, and with ``unknown`` the "no such token" ones."""
        with self._lock:
            for h in [h for h, (_, v, _) in self._d.items() if (match(v) if v is not None else unknown)]:
                del self._d[h]


class Tokens:
    """Make, check and revoke tokens. One per process (the caches are per process)."""

    def __init__(self, accounts, *, now_ms: Callable[[], int] = lambda: time.time_ns() // 1_000_000,
                 clock: Callable[[], float] = time.monotonic, ingest_ttl_s: float = INGEST_CACHE_TTL_S,
                 personal_ttl_s: float = PERSONAL_CACHE_TTL_S, touch_every_ms: int = TOUCH_EVERY_MS,
                 touch_inline: bool = False):
        self.accounts = accounts
        self.now_ms = now_ms
        self.touch_every_ms = touch_every_ms
        self.touch_inline = touch_inline  # tests: write "last used" before returning
        self._ingest = _Cache(ingest_ttl_s, clock)
        self._personal = _Cache(personal_ttl_s, clock)

    # ---- making and revoking --------------------------------------------------------------------------

    def create_ingest(self, name: str, created_by: Optional[int] = None) -> tuple[int, str]:
        """A new ingest token: (its ID, the token, to show once)."""
        t = new_token(INGEST_PREFIX)
        tid = self.accounts.create_ingest_token(name, token_hash(t), t[:DISPLAY_CHARS], self.now_ms(), created_by)
        self._ingest.put(token_hash(t), tid)
        return tid, t

    def revoke_ingest(self, token_id: int) -> bool:
        ok = self.accounts.revoke_ingest_token(token_id, self.now_ms())
        self._ingest.forget(lambda v: v == token_id)
        return ok

    def create_personal(self, user_id: int, name: str, expires_at_ms: Optional[int] = None) -> tuple[int, str]:
        """A new personal API token for this person: (its ID, the token, to show once)."""
        t = new_token(PERSONAL_PREFIX)
        tid = self.accounts.create_personal_token(user_id, name, token_hash(t), t[:DISPLAY_CHARS], self.now_ms(),
                                                  expires_at_ms)
        return tid, t

    def revoke_personal(self, token_id: int, user_id: Optional[int] = None) -> bool:
        ok = self.accounts.revoke_personal_token(token_id, self.now_ms(), user_id)
        self._personal.forget(lambda v: v.token_id == token_id)
        return ok

    def forget_user(self, user_id: int) -> None:
        """Drop this process's cached personal tokens of a person (their role changed, they were disabled, enabled
        or removed), and the cached "no such token" answers, which may be theirs (a token of a disabled person)."""
        self._personal.forget(lambda v: v.user_id == user_id, unknown=True)

    # ---- checking -------------------------------------------------------------------------------------

    def ingest(self, token: Optional[str]) -> Optional[int]:
        """The ID of the live ingest token, or None."""
        if not well_formed(token, INGEST_PREFIX):
            return None
        h = token_hash(token)
        found, tid = self._ingest.get(h)
        if not found:
            row = self.accounts.ingest_token(h)
            tid = None if row is None else int(row["id"])
            self._ingest.put(h, tid)
        if tid is not None:
            self._touch(self._ingest, h, tid, self.accounts.touch_ingest_token)
        return tid

    def personal(self, token: Optional[str]) -> Optional[TokenUser]:
        """The owner of the live personal token, or None."""
        if not well_formed(token, PERSONAL_PREFIX):
            return None
        h = token_hash(token)
        found, user = self._personal.get(h)
        if not found:
            row = self.accounts.personal_token(h, self.now_ms())
            user = None if row is None else TokenUser(int(row["user_id"]), row["email"], row["name"], row["role"],
                                                      int(row["id"]))
            self._personal.put(h, user)
        if user is not None:
            self._touch(self._personal, h, user.token_id, self.accounts.touch_personal_token)
        return user

    def _touch(self, cache: _Cache, h: str, token_id: int, write: Callable[[int, int], None]) -> None:
        now = self.now_ms()
        if not cache.should_touch(h, now, self.touch_every_ms):
            return

        def run():
            try:
                write(token_id, now)
            except Exception as e:  # noqa: BLE001 - "last used" is a hint; a busy store doesn't fail anything
                log.debug("could not record that token %d was used: %s", token_id, e)

        if self.touch_inline:
            run()
        else:
            threading.Thread(target=run, name="trailbeam-token-touch", daemon=True).start()
