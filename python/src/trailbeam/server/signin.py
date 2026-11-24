"""Signing in with an email and password: the checks, both sign-in limits, and the session it starts.

**Per account (exact on every replica).** ``LOCK_AFTER`` failed sign-ins in a row lock the account for
``LOCK_MS``; the count is in the ``users`` table (``failed_count``, ``locked_until_ms``), so failures on
different replicas add up. A sign-in that works, or a new password, starts the count again. A locked account
is answered ``LOCKED`` whatever the password, after the same password hash as any other answer, so guesses
during the lock learn nothing. That answer does say the email has an account, but only after ``LOCK_AFTER``
failures, which the per-address limit slows down.

**Per client address (approximate, per replica).** ``AddressLimit``: more than ``ADDRESS_FAILURES`` failed
sign-ins from one address in ``ADDRESS_WINDOW_S`` and that address is answered "slow down" (429 with
Retry-After) without a password check, until the window has moved on. A sign-in takes its place in the count
before its password check, so parallel sign-ins from one address get at most its remaining allowance of
checks between them. IPv6 addresses count by /64. Kept in memory, so each replica counts its own. The address is the TCP peer's, or the client a proxy in ``--trusted-proxy`` names (proxy.py); without
that setting, behind a proxy every request has the proxy's address, and this limit is shared by everyone.

**What the answer says.** An unknown email, a wrong password, a person who is disabled or has no password yet
all give ``WRONG``, and take the time of one password hash (passwords.Hasher.verify_dummy), so neither the
answer nor its timing says whether an email has an account.
"""
from __future__ import annotations

import ipaddress
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
    """Failed sign-ins per client address in a sliding window, in memory. Thread-safe.

    An attempt reserves its place before its password check (``begin``) and settles it after (``end``), so
    attempts from one address running at the same time count against its allowance at once: however many it
    sends in parallel, at most its remaining allowance get a password check. IPv6 addresses count by /64 (one
    customer's network: anyone with one has billions of addresses in it); IPv4 addresses, and IPv4 written as
    IPv6, count by the exact address.
    """

    def __init__(self, max_failures: int = ADDRESS_FAILURES, window_s: float = ADDRESS_WINDOW_S,
                 clock: Callable[[], float] = time.monotonic, max_addresses: int = ADDRESS_MAX):
        self.max_failures = max_failures
        self.window_s = window_s
        self.clock = clock
        self.max_addresses = max_addresses
        self._failures: OrderedDict = OrderedDict()  # key -> [deque of failure times, attempts in progress]
        self._lock = threading.Lock()

    @staticmethod
    def key(address: str) -> str:
        """What the limit counts by: the IPv6 /64, or the exact address otherwise."""
        try:
            ip = ipaddress.ip_address(address.split("%", 1)[0])
        except (ValueError, AttributeError):
            return address
        if ip.version == 6:
            if ip.ipv4_mapped is not None:
                return str(ip.ipv4_mapped)
            return str(ipaddress.IPv6Network((ip, 64), strict=False))
        return str(ip)

    def _wait(self, entry, now: float) -> Optional[int]:
        q, pending = entry
        while q and now - q[0] >= self.window_s:
            q.popleft()
        if len(q) + pending < self.max_failures:
            return None
        if len(q) < self.max_failures:  # full only with attempts still running: they settle within seconds
            return 1
        return max(1, int(q[0] + self.window_s - now + 0.999))

    def retry_after(self, address: str) -> Optional[int]:
        """Seconds until this address may try again, or None if it may now. Reserves nothing (see ``begin``)."""
        now = self.clock()
        with self._lock:
            entry = self._failures.get(self.key(address))
            return None if entry is None else self._wait(entry, now)

    def begin(self, address: str) -> Optional[int]:
        """Reserve one attempt for this address: None if reserved (call ``end`` once it is decided), otherwise
        the seconds until it may try again, and nothing is reserved."""
        now = self.clock()
        k = self.key(address)
        with self._lock:
            entry = self._failures.get(k)
            if entry is None:
                entry = self._failures[k] = [deque(), 0]
            wait = self._wait(entry, now)
            if wait is not None:
                return wait
            entry[1] += 1
            self._failures.move_to_end(k)
            self._trim()
            return None

    def end(self, address: str, failed: bool) -> None:
        """Settle an attempt ``begin`` reserved: ``failed`` counts it in the window; otherwise (it worked, or
        there was no password check) the place is given back."""
        now = self.clock()
        k = self.key(address)
        with self._lock:
            entry = self._failures.get(k)
            if entry is None:  # dropped while running (more than max_addresses addresses)
                entry = self._failures[k] = [deque(), 0]
            entry[1] = max(0, entry[1] - 1)
            if failed:
                entry[0].append(now)
                while len(entry[0]) > self.max_failures:
                    entry[0].popleft()
                self._failures.move_to_end(k)
                self._trim()
            elif not entry[0] and not entry[1]:
                del self._failures[k]

    def failed(self, address: str) -> None:
        """Count a failure that wasn't reserved with ``begin``."""
        if self.begin(address) is None:
            self.end(address, True)

    def _trim(self) -> None:
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
        wait = self.address_limit.begin(address)
        if wait is not None:
            return Result(SLOW_DOWN, retry_after_s=wait)
        failed = True  # an error after the password check counts too
        try:
            r = self._sign_in(email, password)
            failed = r.outcome != OK
            return r
        except HashBusy:  # no password check: the place is given back
            failed = False
            return Result(BUSY, retry_after_s=1)
        finally:
            self.address_limit.end(address, failed)

    def _sign_in(self, email: str, password: str) -> Result:
        user = self.accounts.user_by_email(email)
        if user is None or user["disabled_at_ms"] is not None or not user["password_hash"]:
            self.hasher.verify_dummy(password if isinstance(password, str) else "")
            return Result(WRONG)
        now = self.now_ms()
        right = self.hasher.verify(password, user["password_hash"])
        locked = user["locked_until_ms"]
        if locked is not None and locked > now:
            # the same answer whatever the password, so guesses during the lock learn nothing
            return Result(LOCKED, retry_after_s=max(1, (locked - now + 999) // 1000))
        if not right:
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
