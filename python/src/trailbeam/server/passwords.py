"""Password hashing for local accounts (users and roles, phase 1; QV-149 proposal, "Technical requirements").

scrypt from the standard library (``hashlib.scrypt``), so no new dependency:

- N = 2^15, r = 8, p = 3: 32 MiB per hash, one of OWASP's equivalent scrypt settings. ``maxmem`` is 64 MiB
  (``hashlib`` refuses more than 32 MiB by default). A 16-byte random salt and a 32-byte output.
- The parameters are stored with each hash, ``scrypt$15$8$3$<salt>$<hash>`` (salt and hash in unpadded
  URL-safe base64), so they can be raised later: ``needs_rehash`` says when a stored hash is weaker than now.
- At most ``MAX_CONCURRENT`` hashes run at once in a process (a semaphore). The server has a thread per
  request and no thread limit, so without it 50 sign-ins at once would need 1.6 GiB. A hash that can't start
  within ``WAIT_S`` raises ``HashBusy``; the server answers that 503 with Retry-After. At most ``MAX_WAITING``
  more may wait for a slot; beyond that a hash raises ``HashBusy`` at once, so a burst of sign-ins can't park
  hundreds of threads for ``WAIT_S`` each and make everyone else's wait that long.
- Passwords are at least ``MIN_LENGTH`` characters (the hosted app's rule) and at most ``MAX_BYTES`` bytes of
  UTF-8, so a huge password can't make a hash expensive.
- ``verify`` compares with ``hmac.compare_digest``. ``verify_dummy`` hashes against a fixed hash and always says
  no: the sign-in calls it when the email is unknown (or the person has no password, or is disabled), so the
  time an answer takes doesn't tell which emails have accounts.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import secrets
import threading
from typing import Optional

LOG2_N, R, P = 15, 8, 3
MAXMEM = 64 * 1024 * 1024
SALT_BYTES = 16
HASH_BYTES = 32
MIN_LENGTH = 10
MAX_BYTES = 1024
MAX_CONCURRENT = 2
WAIT_S = 10.0
MAX_WAITING = 2 * MAX_CONCURRENT
PREFIX = "scrypt"
# What a stored hash may ask for: a hash from a database someone else wrote can't make the server use more
# than MAXMEM (hashlib checks that too) or run for minutes.
MAX_LOG2_N, MAX_R, MAX_P = 20, 32, 16


class PasswordError(ValueError):
    """A password that can't be set: too short, too long, or not text. The message says which, for the person."""


class HashBusy(Exception):
    """``MAX_CONCURRENT`` hashes were running for ``WAIT_S``, or ``MAX_WAITING`` were already waiting: try again
    shortly (503)."""


def check(password: str) -> str:
    """The password, if it can be set. Raises PasswordError, saying why, otherwise. Spaces count and are kept."""
    if not isinstance(password, str):
        raise PasswordError("the password must be text")
    if len(password) < MIN_LENGTH:
        raise PasswordError(f"the password must be at least {MIN_LENGTH} characters")
    if len(password.encode("utf-8", "surrogatepass")) > MAX_BYTES:
        raise PasswordError(f"the password must be at most {MAX_BYTES} bytes")
    if any(c in "\x00\r\n" for c in password):
        raise PasswordError("the password can't contain line breaks or NUL")
    return password


def _b64(b: bytes) -> str:
    return base64.urlsafe_b64encode(b).rstrip(b"=").decode()


def _unb64(s: str) -> bytes:
    return base64.urlsafe_b64decode(s + "=" * (-len(s) % 4))


class Hasher:
    """Hashes and checks passwords, at most ``max_concurrent`` at once. Thread-safe; one per process."""

    def __init__(self, max_concurrent: int = MAX_CONCURRENT, wait_s: float = WAIT_S, *,
                 max_waiting: Optional[int] = None, log2_n: int = LOG2_N, r: int = R, p: int = P):
        if max_concurrent < 1:
            raise ValueError("max_concurrent must be at least 1")
        self.max_concurrent = max_concurrent
        self._slots = threading.BoundedSemaphore(max_concurrent)
        self.wait_s = wait_s
        self.max_waiting = 2 * max_concurrent if max_waiting is None else max_waiting
        self._waiting = 0
        self._waiting_lock = threading.Lock()
        self.params = (log2_n, r, p)
        self._dummy: Optional[str] = None
        self._dummy_lock = threading.Lock()

    def _scrypt(self, password: bytes, salt: bytes, log2_n: int, r: int, p: int, dklen: int) -> bytes:
        if not self._slots.acquire(blocking=False):
            with self._waiting_lock:
                if self._waiting >= self.max_waiting:
                    raise HashBusy(f"{self.max_concurrent} password checks running and {self._waiting} waiting")
                self._waiting += 1
            try:
                got = self._slots.acquire(timeout=self.wait_s)
            finally:
                with self._waiting_lock:
                    self._waiting -= 1
            if not got:
                raise HashBusy(f"{self.max_concurrent} password checks were already running for {self.wait_s:g} s")
        try:
            return hashlib.scrypt(password, salt=salt, n=1 << log2_n, r=r, p=p, maxmem=MAXMEM, dklen=dklen)
        finally:
            self._slots.release()

    def hash(self, password: str) -> str:
        """The stored form of a new password (``check`` first). Raises PasswordError or HashBusy."""
        pw = check(password).encode("utf-8")
        salt = secrets.token_bytes(SALT_BYTES)
        n, r, p = self.params
        return f"{PREFIX}${n}${r}${p}${_b64(salt)}${_b64(self._scrypt(pw, salt, n, r, p, HASH_BYTES))}"

    def verify(self, password: str, stored: Optional[str]) -> bool:
        """Whether the password matches the stored hash. A stored value that isn't a hash this version can read
        (None, damaged, unknown parameters) never matches, but still costs one hash. Raises HashBusy."""
        parsed = _parse(stored)
        if parsed is None or not isinstance(password, str):
            self.verify_dummy(password if isinstance(password, str) else "")
            return False
        pw = password.encode("utf-8", "surrogatepass")
        if len(pw) > MAX_BYTES:  # no hash: the length of the password, not the account, decides this
            return False
        log2_n, r, p, salt, want = parsed
        got = self._scrypt(pw, salt, log2_n, r, p, len(want))
        return hmac.compare_digest(got, want)

    def verify_dummy(self, password: str) -> bool:
        """The work of ``verify`` against a hash no password matches. Always False. Raises HashBusy."""
        with self._dummy_lock:
            if self._dummy is None:
                n, r, p = self.params
                salt = secrets.token_bytes(SALT_BYTES)
                self._dummy = f"{PREFIX}${n}${r}${p}${_b64(salt)}${_b64(secrets.token_bytes(HASH_BYTES))}"
        log2_n, r, p, salt, want = _parse(self._dummy)
        pw = (password or "").encode("utf-8", "surrogatepass")[:MAX_BYTES]
        hmac.compare_digest(self._scrypt(pw, salt, log2_n, r, p, len(want)), want)
        return False

    def needs_rehash(self, stored: Optional[str]) -> bool:
        """The stored hash is readable but uses other parameters than this Hasher: hash the password again
        after the next sign-in that works."""
        parsed = _parse(stored)
        return parsed is not None and parsed[:3] != self.params


def _parse(stored: Optional[str]):
    """(log2 N, r, p, salt, hash) of a stored hash, or None if it isn't one this version reads."""
    if not isinstance(stored, str):
        return None
    parts = stored.split("$")
    if len(parts) != 6 or parts[0] != PREFIX or not all(x.isdigit() for x in parts[1:4]):
        return None
    log2_n, r, p = (int(x) for x in parts[1:4])
    if not (1 <= log2_n <= MAX_LOG2_N and 1 <= r <= MAX_R and 1 <= p <= MAX_P) or 128 * r * (1 << log2_n) > MAXMEM // 2:
        return None
    try:
        salt, want = _unb64(parts[4]), _unb64(parts[5])
    except (ValueError, TypeError):
        return None
    if len(salt) < 8 or not 16 <= len(want) <= 64:
        return None
    return log2_n, r, p, salt, want
