"""Password hashing (passwords.py): the stored format, the limits, and attempts to get around them."""
import hashlib
import threading
import time

import pytest

from trailbeam.server import passwords
from trailbeam.server.passwords import HashBusy, Hasher, PasswordError

PW = "correct horse battery"


def fast(**kw):
    return Hasher(log2_n=10, r=8, p=1, **kw)  # quick to test; the defaults are checked once below


def test_the_default_parameters_are_the_reviewed_ones():
    h = Hasher()
    stored = h.hash(PW)
    kind, n, r, p, salt, out = stored.split("$")
    assert (kind, n, r, p) == ("scrypt", "15", "8", "3")
    assert len(passwords._unb64(salt)) == 16 and len(passwords._unb64(out)) == 32
    assert passwords._unb64(out) == hashlib.scrypt(PW.encode(), salt=passwords._unb64(salt), n=2 ** 15, r=8, p=3,
                                                   maxmem=64 * 2 ** 20, dklen=32)
    assert h.verify(PW, stored) and not h.verify(PW + "!", stored)
    assert not h.needs_rehash(stored)


def test_each_hash_has_its_own_salt():
    h = fast()
    a, b = h.hash(PW), h.hash(PW)
    assert a != b and h.verify(PW, a) and h.verify(PW, b)


@pytest.mark.parametrize("bad, why", [
    ("short", "at least 10"),
    ("x" * 1025, "at most 1024 bytes"),
    ("é" * 513, "at most 1024 bytes"),  # 1026 bytes of UTF-8 in 513 characters
    ("line\nbreak-password", "line breaks"),
    ("nul\x00-in-password", "NUL"),
])
def test_passwords_that_cant_be_set(bad, why):
    with pytest.raises(PasswordError, match=why):
        fast().hash(bad)


def test_spaces_and_unicode_are_kept():
    h = fast()
    for pw in ("  ten chars  ", "pässwörd-ünïcode", "x" * 1024):
        assert h.verify(pw, h.hash(pw))
    assert not h.verify("ten chars", h.hash("  ten chars  "))


@pytest.mark.parametrize("stored", [
    None, "", "plain", "scrypt$15$8$3$c2FsdA$aGFzaA",              # too short a salt and hash
    "bcrypt$15$8$3$AAAAAAAAAAAAAAAAAAAAAA$AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "scrypt$30$8$3$AAAAAAAAAAAAAAAAAAAAAA$AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",  # 2^30: would need 1 TiB
    "scrypt$15$64$1$AAAAAAAAAAAAAAAAAAAAAA$AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",  # r too large
    "scrypt$15$8$99$AAAAAAAAAAAAAAAAAAAAAA$AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",  # p too large
    "scrypt$-1$8$3$AAAAAAAAAAAAAAAAAAAAAA$AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "scrypt$15$8$3$!!!$AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
])
def test_a_stored_value_this_version_cant_read_never_matches(stored):
    """A damaged or tampered hash (someone with write access to the database) can't sign anyone in, nor make
    the server spend more memory or time than a normal hash."""
    h = fast()
    t = time.perf_counter()
    assert h.verify(PW, stored) is False
    assert time.perf_counter() - t < 5
    assert not h.needs_rehash(stored)


def test_a_password_over_the_cap_never_matches_but_costs_one_hash_like_any_other_answer():
    # QV-229: answering an over-long password without a hash for a known email, while an unknown email costs
    # one (verify_dummy), told which emails have accounts (about 6 ms against 190 ms)
    h = fast()
    over = PW + "x" * (passwords.MAX_BYTES - len(PW) + 1)
    stored = h.hash(PW)
    calls = []
    real = h._scrypt
    h._scrypt = lambda *a: calls.append(a) or real(*a)
    assert h.verify(over, stored) is False
    assert h.verify("x" * 5000, stored) is False
    assert len(calls) == 2
    assert all(len(c[0]) <= passwords.MAX_BYTES for c in calls)  # the cost stays capped


def test_verify_dummy_does_the_work_and_says_no():
    h = Hasher(log2_n=12, r=8, p=1)
    calls = []
    real = h._scrypt
    h._scrypt = lambda *a: calls.append(a) or real(*a)
    assert h.verify_dummy(PW) is False and h.verify_dummy("") is False and h.verify_dummy("x" * 5000) is False
    assert len(calls) == 3
    assert all(c[2:5] == (12, 8, 1) for c in calls)  # the same parameters as a real hash


def test_needs_rehash_when_the_parameters_were_raised():
    old = Hasher(log2_n=10, r=8, p=1).hash(PW)
    new = Hasher(log2_n=11, r=8, p=1)
    assert new.needs_rehash(old) and new.verify(PW, old)


def test_at_most_max_concurrent_hashes_run_at_once():
    h = fast(max_concurrent=2, wait_s=30)
    running, peak, lock = [0], [0], threading.Lock()
    real = hashlib.scrypt

    def counting(*a, **k):
        with lock:
            running[0] += 1
            peak[0] = max(peak[0], running[0])
        try:
            time.sleep(0.05)
            return real(*a, **k)
        finally:
            with lock:
                running[0] -= 1

    passwords.hashlib.scrypt, saved = counting, passwords.hashlib.scrypt
    try:
        threads = [threading.Thread(target=h.hash, args=(PW,)) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
    finally:
        passwords.hashlib.scrypt = saved
    assert peak[0] == 2


def test_a_hash_that_cant_start_in_time_is_busy():
    h = fast(max_concurrent=1, wait_s=0.05)
    with h._slots:
        with pytest.raises(HashBusy):
            h.hash(PW)
        with pytest.raises(HashBusy):
            h.verify_dummy(PW)
    assert h.verify(PW, h.hash(PW))  # the slot is back


def test_beyond_max_waiting_a_hash_is_busy_at_once():
    """QV-407: without a cap, every request beyond the slots waited WAIT_S, and so did everyone after them."""
    h = fast(max_concurrent=1, wait_s=30, max_waiting=2)
    assert passwords.MAX_WAITING == 2 * passwords.MAX_CONCURRENT and fast().max_waiting == 2 * fast().max_concurrent
    h._slots.acquire()
    waiters = [threading.Thread(target=h.hash, args=(PW,)) for _ in range(2)]
    for t in waiters:
        t.start()
    deadline = time.monotonic() + 5
    while h._waiting < 2 and time.monotonic() < deadline:
        time.sleep(0.01)
    assert h._waiting == 2
    started = time.monotonic()
    with pytest.raises(HashBusy):
        h.verify_dummy(PW)
    assert time.monotonic() - started < 1
    h._slots.release()
    for t in waiters:
        t.join(10)
    assert h._waiting == 0 and h.verify(PW, h.hash(PW))
