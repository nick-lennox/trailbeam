"""People, sessions and signing in (accounts.py, sessions.py, signin.py) on both stores, with attempts to break
them: forged and expired sessions, sessions of people changed or removed, both sign-in limits, and replicas
that see a change only when their cache expires.

The Postgres cases run when TRAILBEAM_TEST_POSTGRES_URL names a database, as in test_postgres.py."""
import os
import sys
import threading
from pathlib import Path

import pytest

from trailbeam.server import sessions as sessions_mod
from trailbeam.server.accounts import AccountError, Accounts, EmailTaken
from trailbeam.server.db import Database
from trailbeam.server.passwords import Hasher
from trailbeam.server.sessions import Sessions, cookie_value, id_hash
from trailbeam.server.signin import BUSY, LOCKED, OK, SLOW_DOWN, WRONG, AddressLimit, SignIn

ROOT = Path(__file__).resolve().parents[2]
PG = os.environ.get("TRAILBEAM_TEST_POSTGRES_URL")
PW = "correct horse battery"
NOW = 1_800_000_000_000  # ms


class Clock:
    def __init__(self):
        self.ms = NOW
        self.mono = 1000.0

    def now_ms(self):
        return self.ms

    def monotonic(self):
        return self.mono

    def advance(self, s: float):
        self.ms += int(s * 1000)
        self.mono += s


def _open(kind, tmp_path, memory=False):
    if kind == "sqlite":
        return Database(":memory:" if memory else str(tmp_path / "trailbeam.sqlite"))
    sys.path.insert(0, str(ROOT / "tests" / "conformance"))
    from pg_helpers import postgres_store
    return postgres_store()


@pytest.fixture(params=["sqlite", "sqlite-memory", pytest.param("postgres", marks=pytest.mark.skipif(
    not PG, reason="TRAILBEAM_TEST_POSTGRES_URL is not set"))])
def store(request, tmp_path):
    s = _open(request.param.split("-")[0], tmp_path, memory=request.param.endswith("memory"))
    yield s
    s.close()


@pytest.fixture
def clock():
    return Clock()


@pytest.fixture
def hasher():
    return Hasher(log2_n=10, r=8, p=1)  # fast; test_passwords.py checks the real parameters


@pytest.fixture
def acc(store):
    return Accounts(store)


def make(acc, hasher, email="ada@example.com", role="member", pw=PW):
    return acc.create_user(email, "Ada Lovelace", role, hasher.hash(pw), NOW)


def sign_in(acc, clock, hasher, **kw):
    s = Sessions(acc, now_ms=clock.now_ms, clock=clock.monotonic)
    return SignIn(acc, s, hasher, now_ms=clock.now_ms,
                  address_limit=kw.pop("address_limit", AddressLimit(clock=clock.monotonic)), **kw), s


# ---- people ---------------------------------------------------------------------------------------------

def test_create_and_read_a_person(acc, hasher):
    uid = make(acc, hasher)
    u = acc.user(uid)
    assert (u["email"], u["name"], u["role"], u["failed_count"], u["disabled_at_ms"]) == (
        "ada@example.com", "Ada Lovelace", "member", 0, None)
    assert acc.user_by_email("ADA@Example.COM")["id"] == uid
    assert acc.user_by_email("  ada@example.com ")["id"] == uid
    assert acc.user_by_email("bob@example.com") is None and acc.user_by_email("") is None
    assert [x["id"] for x in acc.users()] == [uid]


def test_emails_are_unique_ignoring_case(acc, hasher):
    make(acc, hasher)
    with pytest.raises(EmailTaken):
        make(acc, hasher, email="Ada@EXAMPLE.com")


@pytest.mark.parametrize("email, name, role", [
    ("not-an-email", "Ada", "member"), ("a@b@c", "Ada", "member"), ("x" * 250 + "@e.com", "Ada", "member"),
    ("ada@example.com", "", "member"), ("ada@example.com", "\x07bell", "member"),
    ("ada@example.com", "Ada", "viewer"), ("ada@example.com", "Ada", "Admin"), ("ada@example.com", "Ada", ""),
])
def test_what_cant_be_stored(acc, email, name, role):
    with pytest.raises(AccountError):
        acc.create_user(email, name, role, None, NOW)
    assert acc.users() == []


# ---- sessions -------------------------------------------------------------------------------------------

def test_a_session_is_stored_only_as_a_hash(store, acc, hasher, clock):
    uid = make(acc, hasher)
    s = Sessions(acc, now_ms=clock.now_ms, clock=clock.monotonic)
    sid, expires = s.start(uid)
    assert expires == NOW + 30 * 86_400_000
    assert len(sid) >= 43  # 32 random bytes, base64
    row = acc.session(id_hash(sid), NOW)
    assert row["user_id"] == uid and row["role"] == "member"
    assert acc.session(sid, NOW) is None  # the ID itself is not a key
    assert s.lookup(sid).user_id == uid
    assert s.lookup("forged-" + sid[7:]) is None and s.lookup("") is None and s.lookup(None) is None


def test_the_expiry_is_fixed_and_last_seen_is_written_at_most_hourly(acc, hasher, clock):
    uid = make(acc, hasher)
    s = Sessions(acc, now_ms=clock.now_ms, clock=clock.monotonic)
    sid, _ = s.start(uid)
    writes = []
    real = acc.touch_session
    acc.touch_session = lambda *a: writes.append(a) or real(*a)
    for _ in range(100):  # many page views within the hour: no write
        clock.advance(30)
        assert s.lookup(sid) is not None
    assert writes == []
    clock.advance(3600)
    assert s.lookup(sid) is not None and len(writes) == 1
    assert s.lookup(sid) is not None and len(writes) == 1
    assert acc.session(id_hash(sid), clock.ms)["expires_at_ms"] == NOW + 30 * 86_400_000  # never extended
    clock.advance(30 * 86_400)
    assert s.lookup(sid) is None


def test_an_expired_session_is_refused_even_from_the_cache(acc, hasher, clock):
    uid = make(acc, hasher)
    s = Sessions(acc, now_ms=clock.now_ms, clock=clock.monotonic, lifetime_ms=5_000, cache_ttl_s=60)
    sid, _ = s.start(uid)
    assert s.lookup(sid) is not None
    clock.advance(6)  # within the cache's 60 s, past the session's 5 s
    assert s.lookup(sid) is None


def test_expired_sessions_are_deleted_when_a_new_one_starts(store, acc, hasher, clock):
    uid = make(acc, hasher)
    s = Sessions(acc, now_ms=clock.now_ms, clock=clock.monotonic, lifetime_ms=1_000)
    old, _ = s.start(uid)
    clock.advance(2)
    s.start(uid)
    with acc._write() as tx:
        assert tx.one("SELECT COUNT(*) AS n FROM sessions")["n"] == 1


def test_sign_out_ends_the_session_here_at_once_and_on_other_replicas_within_the_cache_ttl(acc, hasher, clock):
    uid = make(acc, hasher)
    here = Sessions(acc, now_ms=clock.now_ms, clock=clock.monotonic)
    other = Sessions(acc, now_ms=clock.now_ms, clock=clock.monotonic)  # another replica: its own cache
    sid, _ = here.start(uid)
    assert here.lookup(sid) and other.lookup(sid)
    here.end(sid)
    assert here.lookup(sid) is None
    assert other.lookup(sid) is not None  # cached
    clock.advance(sessions_mod.CACHE_TTL_S)
    assert other.lookup(sid) is None


@pytest.mark.parametrize("change", ["password", "role", "disable", "remove", "end_all"])
def test_changing_a_person_ends_their_sessions(acc, hasher, clock, change):
    uid = make(acc, hasher)
    keep = make(acc, hasher, email="bob@example.com")
    here = Sessions(acc, now_ms=clock.now_ms, clock=clock.monotonic)
    other = Sessions(acc, now_ms=clock.now_ms, clock=clock.monotonic)
    a, _ = here.start(uid)
    b, _ = here.start(keep)
    assert other.lookup(a) and other.lookup(b)
    if change == "password":
        assert acc.set_password(uid, hasher.hash("another password"), NOW)
    elif change == "role":
        assert acc.set_role(uid, "admin", NOW)
    elif change == "disable":
        assert acc.set_disabled(uid, True, NOW)
    elif change == "remove":
        assert acc.remove_user(uid)
    else:
        here.end_all(uid)
    here.forget_user(uid)
    assert here.lookup(a) is None and here.lookup(b) is not None
    clock.advance(sessions_mod.CACHE_TTL_S)
    assert other.lookup(a) is None and other.lookup(b) is not None


def test_a_password_change_can_keep_the_session_that_made_it(acc, hasher, clock):
    uid = make(acc, hasher)
    s = Sessions(acc, now_ms=clock.now_ms, clock=clock.monotonic)
    mine, _ = s.start(uid)
    theirs, _ = s.start(uid)
    acc.set_password(uid, hasher.hash("another password"), NOW, keep_session=id_hash(mine))
    assert acc.session(id_hash(mine), NOW) is not None and acc.session(id_hash(theirs), NOW) is None


def test_changes_to_nobody_say_so(acc):
    assert not acc.set_password(99, "x", NOW) and not acc.set_role(99, "admin", NOW)
    assert not acc.set_disabled(99, True, NOW) and not acc.remove_user(99)
    with pytest.raises(AccountError):
        acc.set_role(99, "owner", NOW)


def test_the_cache_is_bounded(acc, hasher, clock, monkeypatch):
    monkeypatch.setattr(sessions_mod, "CACHE_MAX", 50)
    s = Sessions(acc, now_ms=clock.now_ms, clock=clock.monotonic)
    for i in range(200):  # random cookies are cached as "no session" too
        assert s.lookup(f"made-up-session-id-{i:04d}") is None
    assert len(s._cache) == 50


def test_cookies():
    sid = sessions_mod.new_id()
    plain, secure = sessions_mod.set_cookie(sid, False), sessions_mod.set_cookie(sid, True)
    assert plain.startswith(f"trailbeam_sid={sid}; Path=/; HttpOnly; SameSite=Strict; Max-Age=2592000")
    assert "Secure" not in plain and "Domain" not in plain
    assert secure.startswith(f"__Host-trailbeam_sid={sid}; Path=/;") and secure.endswith("; Secure")
    assert "Max-Age=0" in sessions_mod.clear_cookie(True) and sessions_mod.clear_cookie(True).startswith("__Host-")
    for secure in (False, True):
        assert cookie_value(f"trailbeam_sid=other-value-but-long-enough; __Host-trailbeam_sid={sid}", secure) == sid
        # the shared token's cookie (auth.py) is never a session
        assert cookie_value(f"trailbeam_session={sid}", secure) is None
        assert cookie_value("__Host-trailbeam_sid=short", secure) is None
        assert cookie_value('__Host-trailbeam_sid="unterminated', secure) is None and cookie_value(None, secure) is None
    assert cookie_value(f"a=1; trailbeam_sid={sid}", False) == sid
    assert cookie_value("trailbeam_sid=short", False) is None and cookie_value("trailbeam_sid=" + "x" * 200, False) is None
    # over HTTPS a plain trailbeam_sid is not a session: anyone who can set a cookie on the host (a sibling
    # subdomain, or whoever answers its plain http://) could plant one and sign the victim in as themselves
    assert cookie_value(f"a=1; trailbeam_sid={sid}", True) is None


# ---- signing in -----------------------------------------------------------------------------------------

def test_sign_in_starts_a_session(acc, hasher, clock):
    uid = make(acc, hasher)
    si, s = sign_in(acc, clock, hasher)
    r = si.sign_in("ADA@example.com", PW, "198.51.100.7")
    assert r.outcome == OK and r.user["id"] == uid
    assert s.lookup(r.session_id).user_id == uid
    assert acc.user(uid)["last_sign_in_ms"] == NOW


@pytest.mark.parametrize("case", ["unknown", "wrong", "disabled", "no_password", "too_long", "not_text"])
def test_every_kind_of_wrong_answer_is_the_same_and_costs_one_hash(acc, hasher, clock, case):
    uid = make(acc, hasher)
    email, pw = "ada@example.com", "wrong password!"
    if case == "unknown":
        email = "nobody@example.com"
    elif case == "disabled":
        acc.set_disabled(uid, True, NOW)
        pw = PW
    elif case == "no_password":
        acc.create_user("new@example.com", "New", "member", None, NOW)
        email = "new@example.com"
    elif case == "too_long":
        pw = "x" * 5000
    elif case == "not_text":
        pw = None
    si, _ = sign_in(acc, clock, hasher)
    calls = []
    real = hasher._scrypt
    hasher._scrypt = lambda *a: calls.append(a) or real(*a)
    r = si.sign_in(email, pw, "198.51.100.7")
    assert r.outcome == WRONG and r.session_id is None and r.user is None
    assert len(calls) == (0 if case == "too_long" else 1)


def test_the_account_locks_after_too_many_failures_whatever_the_address(acc, hasher, clock):
    uid = make(acc, hasher)
    si, _ = sign_in(acc, clock, hasher, lock_after=3, lock_ms=60_000)
    for i in range(3):  # three addresses: the per-account count is in the store
        assert si.sign_in("ada@example.com", "wrong password!", f"198.51.100.{i}").outcome == WRONG
    # locked: the right password and a wrong one get the same answer, so guesses learn nothing
    right = si.sign_in("ada@example.com", PW, "203.0.113.1")
    wrong = si.sign_in("ada@example.com", "wrong password!", "203.0.113.2")
    assert right.outcome == wrong.outcome == LOCKED and right.session_id is None
    assert 1 <= right.retry_after_s <= 60
    clock.advance(61)
    assert si.sign_in("ada@example.com", PW, "203.0.113.1").outcome == OK
    assert acc.user(uid)["failed_count"] == 0 and acc.user(uid)["locked_until_ms"] is None


def test_a_sign_in_that_works_starts_the_count_again(acc, hasher, clock):
    uid = make(acc, hasher)
    si, _ = sign_in(acc, clock, hasher, lock_after=3)
    for _ in range(5):
        si.sign_in("ada@example.com", "wrong password!", "198.51.100.1")
        si.sign_in("ada@example.com", "wrong password!", "198.51.100.1")
        assert si.sign_in("ada@example.com", PW, "198.51.100.1").outcome == OK
    assert acc.user(uid)["failed_count"] == 0


def test_failures_counted_at_once_on_many_threads_all_count(acc, hasher, clock):
    uid = make(acc, hasher)
    barrier = threading.Barrier(8)

    def fail():
        barrier.wait()
        acc.sign_in_failed(uid, NOW, 100, 60_000)

    threads = [threading.Thread(target=fail) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert acc.user(uid)["failed_count"] == 8


def test_a_new_password_clears_the_lock(acc, hasher, clock):
    uid = make(acc, hasher)
    si, _ = sign_in(acc, clock, hasher, lock_after=1)
    si.sign_in("ada@example.com", "wrong password!", "198.51.100.1")
    assert si.sign_in("ada@example.com", PW, "198.51.100.1").outcome == LOCKED
    acc.set_password(uid, hasher.hash("a new password"), NOW)
    assert si.sign_in("ada@example.com", "a new password", "198.51.100.1").outcome == OK


def test_too_many_failures_from_one_address_slow_it_down_without_a_hash(acc, hasher, clock):
    make(acc, hasher)
    limit = AddressLimit(max_failures=5, window_s=60, clock=clock.monotonic)
    si, _ = sign_in(acc, clock, hasher, address_limit=limit, lock_after=1000)
    for i in range(5):  # unknown emails count too: guessing emails is slowed down as well
        assert si.sign_in(f"guess{i}@example.com", "wrong password!", "198.51.100.7").outcome == WRONG
    calls = []
    real = hasher._scrypt
    hasher._scrypt = lambda *a: calls.append(a) or real(*a)
    r = si.sign_in("ada@example.com", PW, "198.51.100.7")
    assert r.outcome == SLOW_DOWN and 1 <= r.retry_after_s <= 60 and calls == []
    assert si.sign_in("ada@example.com", PW, "198.51.100.8").outcome == OK  # other addresses aren't affected
    clock.advance(61)
    assert si.sign_in("ada@example.com", PW, "198.51.100.7").outcome == OK


def test_the_address_limit_is_bounded():
    limit = AddressLimit(max_failures=2, window_s=60, max_addresses=100)
    for i in range(1000):
        limit.failed(f"10.0.{i // 256}.{i % 256}")
    assert len(limit._failures) == 100


def test_busy_hashing_is_its_own_answer(acc, clock):
    h = Hasher(log2_n=10, r=8, p=1, max_concurrent=1, wait_s=0.01)
    acc.create_user("ada@example.com", "Ada", "member", h.hash(PW), NOW)
    si, _ = sign_in(acc, clock, h)
    with h._slots:
        r = si.sign_in("ada@example.com", PW, "198.51.100.7")
    assert r.outcome == BUSY and r.retry_after_s == 1
    assert acc.user_by_email("ada@example.com")["failed_count"] == 0  # not counted as a failure


def test_a_weaker_hash_is_replaced_after_a_sign_in_that_works(acc, clock):
    old = Hasher(log2_n=10, r=8, p=1)
    uid = acc.create_user("ada@example.com", "Ada", "member", old.hash(PW), NOW)
    new = Hasher(log2_n=11, r=8, p=1)
    si, _ = sign_in(acc, clock, new)
    assert si.sign_in("ada@example.com", PW, "198.51.100.7").outcome == OK
    stored = acc.user(uid)["password_hash"]
    assert stored.startswith("scrypt$11$") and new.verify(PW, stored)
