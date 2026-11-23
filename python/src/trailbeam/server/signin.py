"""Signing in with an email and password: the checks, both sign-in limits, and the session it starts.

**Per account (exact on every replica).** ``LOCK_AFTER`` failed sign-ins in a row lock the account for
``LOCK_MS``; the count is in the ``users`` table (``failed_count``, ``locked_until_ms``), so failures on
different replicas add up. A sign-in that works, or a new password, starts the count again. A locked account
is answered ``LOCKED`` whatever the password, after the same password hash as any other answer, so guesses
during the lock learn nothing. That answer does say the email has an account, but only after ``LOCK_AFTER``
failures, which the per-address limit slows down.

**Per client address (approximate, per replica).** ``AddressLimit``: more than ``ADDRESS_FAILURES`` failed
sign-ins from one address in ``ADDRESS_WINDOW_S`` and that address is answered "slow down" (429 with
Retry-After) without a password check, until the window has moved on. Kept in memory, so each replica counts
its own. The address is the TCP peer's, or the client a proxy in ``--trusted-proxy`` names (proxy.py); without
that setting, behind a proxy every request has the proxy's address, and this limit is shared by everyone.

**What the answer says.** An unknown email, a wrong password, a person who is disabled or has no password yet
all give ``WRONG``, and take the time of one password hash (passwords.Hasher.verify_dummy), so neither the
answer nor its timing says whether an email has an account.
"""
from __future__ import annotations

import threading
import time
from collections import OrderedDict, deque
from typing import Callable, NamedTuple, Optional

from .passwords import HashBusy, Hasher

LOCK_AFTER = 10
LOCK_MS = 15 * 60_000
ADDRESS_FAILURES = 20
ADDRESS_WINDOW_S = 600.0
ADDRESS_MAX = 50_000  # addresses remembered; the oldest are dropped first

OK, WRONG, LOCKED, SLOW_DOWN, BUSY = "ok", "wrong", "locked", "slow_down", "busy"


class Result(NamedTuple):
    outcome: str                        # OK, WRONG, LOCKED, SLOW_DOWN or BUSY
    user: Optional[dict] = None         # OK: the person (accounts.USER_COLUMNS)
    session_id: Optional[str] = None    # OK: for the cookie
    expires_at_ms: Optional[int] = None
    retry_after_s: Optional[int] = None  # LOCKED, SLOW_DOWN, BUSY


class AddressLimit:
    """Failed sign-ins per client address in a sliding window, in memory. Thread-safe."""

    def __init__(self, max_failures: int = ADDRESS_FAILURES, window_s: float = ADDRESS_WINDOW_S,
                 clock: Callable[[], float] = time.monotonic, max_addresses: int = ADDRESS_MAX):
        self.max_failures = max_failures
        self.window_s = window_s
        self.clock = clock
        self.max_addresses = max_addresses
        self._failures: OrderedDict = OrderedDict()  # address -> deque of failure times
        self._lock = threading.Lock()

    def retry_after(self, address: str) -> Optional[int]:
        """Seconds until this address may try again, or None if it may now."""
        now = self.clock()
        with self._lock:
            q = self._failures.get(address)
            if q is None:
                return None
            while q and now - q[0] >= self.window_s:
                q.popleft()
            if not q:
                del self._failures[address]
                return None
            if len(q) < self.max_failures:
                return None
            return max(1, int(q[0] + self.window_s - now + 0.999))

    def failed(self, address: str) -> None:
        now = self.clock()
        with self._lock:
            q = self._failures.get(address)
            if q is None:
                q = self._failures[address] = deque(maxlen=self.max_failures)
            q.append(now)
            self._failures.move_to_end(address)
            while len(self._failures) > self.max_addresses:
                self._failures.popitem(last=False)


class SignIn:
    def __init__(self, accounts, sessions, hasher: Optional[Hasher] = None, *,
                 address_limit: Optional[AddressLimit] = None,
                 now_ms: Callable[[], int] = lambda: time.time_ns() // 1_000_000,
                 lock_after: int = LOCK_AFTER, lock_ms: int = LOCK_MS):
        self.accounts = accounts
        self.sessions = sessions
        self.hasher = hasher or Hasher()
        self.address_limit = address_limit or AddressLimit()
        self.now_ms = now_ms
        self.lock_after = lock_after
        self.lock_ms = lock_ms

    def sign_in(self, email: str, password: str, address: str) -> Result:
        wait = self.address_limit.retry_after(address)
        if wait is not None:
            return Result(SLOW_DOWN, retry_after_s=wait)
        try:
            return self._sign_in(email, password, address)
        except HashBusy:
            return Result(BUSY, retry_after_s=1)

    def _sign_in(self, email: str, password: str, address: str) -> Result:
        user = self.accounts.user_by_email(email)
        if user is None or user["disabled_at_ms"] is not None or not user["password_hash"]:
            self.hasher.verify_dummy(password if isinstance(password, str) else "")
            self.address_limit.failed(address)
            return Result(WRONG)
        now = self.now_ms()
        right = self.hasher.verify(password, user["password_hash"])
        locked = user["locked_until_ms"]
        if locked is not None and locked > now:
            # the same answer whatever the password, so guesses during the lock learn nothing
            self.address_limit.failed(address)
            return Result(LOCKED, retry_after_s=max(1, (locked - now + 999) // 1000))
        if not right:
            self.address_limit.failed(address)
            self.accounts.sign_in_failed(user["id"], now, self.lock_after, self.lock_ms)
            return Result(WRONG)
        self.accounts.sign_in_ok(user["id"], now)
        if self.hasher.needs_rehash(user["password_hash"]):
            try:
                self.accounts.rehash_password(user["id"], user["password_hash"], self.hasher.hash(password))
            except Exception:  # weaker hash stays until the next sign-in
                pass
        sid, expires = self.sessions.start(user["id"])
        return Result(OK, user=user, session_id=sid, expires_at_ms=expires)
