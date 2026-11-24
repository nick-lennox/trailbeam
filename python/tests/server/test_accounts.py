"""People, sessions, tokens and the setup code as the store keeps them (accounts.py, QV-225), on every store:
SQLite in memory and on a file, and Postgres when TRAILBEAM_TEST_POSTGRES_URL is set. The rules that must hold
whatever the backend: emails unique ignoring case, always one active admin, a setup code used once, and
sessions and tokens gone with their person."""
import os
import sqlite3
import sys
import threading
from pathlib import Path

import pytest

from trailbeam.server import accounts, migrations
from trailbeam.server.accounts import AccountError, LastAdmin, UserExists
from trailbeam.server.db import Database
from trailbeam.server.retention import Retention

ROOT = Path(__file__).resolve().parents[2]
PG = os.environ.get("TRAILBEAM_TEST_POSTGRES_URL")
NOW = 1_795_000_000_000
DAY = 86_400_000


@pytest.fixture(params=["memory", "file", "postgres"])
def store(request, tmp_path):
    if request.param == "postgres":
        if not PG:
            pytest.skip("TRAILBEAM_TEST_POSTGRES_URL is not set")
        sys.path.insert(0, str(ROOT / "tests" / "conformance"))
        from pg_helpers import postgres_store
        s = postgres_store()
    else:
        s = Database(":memory:" if request.param == "memory" else str(tmp_path / "trailbeam.sqlite"))
    yield s
    s.close()


@pytest.fixture
def acc(store) -> accounts.Accounts:
    return store.accounts


def admin(acc, email="ada@example.com", **kw):
    return acc.create_user(email, "Ada", "admin", "hash-a", NOW, **kw)


# ---- people ----------------------------------------------------------------------------------------

def test_a_new_store_has_no_admin_and_no_tokens(acc):
    assert not acc.admin_exists()
    assert acc.users() == []
    assert not acc.any_ingest_token()


def test_a_user_is_stored_with_a_lower_cased_email(acc):
    u = acc.create_user("  Grace@Example.COM ", " Grace ", "member", "hash-g", NOW)
    assert (u.email, u.name, u.role, u.password_hash, u.failed_count) == ("grace@example.com", "Grace", "member",
                                                                         "hash-g", 0)
    assert acc.user_by_email("GRACE@example.com") == u == acc.user(u.id)
    assert not acc.admin_exists()
    admin(acc)
    assert acc.admin_exists()
    assert [x.email for x in acc.users()] == ["ada@example.com", "grace@example.com"]


def test_emails_are_unique_ignoring_case(acc):
    admin(acc)
    with pytest.raises(UserExists, match="ada@example.com already has an account"):
        acc.create_user("ADA@example.com", "Other", "member", None, NOW)
    assert len(acc.users()) == 1


def test_the_database_itself_refuses_an_email_that_differs_only_in_case(store):
    """Belt and braces: a row written around accounts.py is refused too (COLLATE NOCASE; lower(email) index)."""
    admin(store.accounts)
    if isinstance(store, Database):
        with store.lock:
            with pytest.raises(sqlite3.IntegrityError):
                store.conn.execute("INSERT INTO users (email, name, role, created_at_ms, updated_at_ms) "
                                   "VALUES ('Ada@Example.com', 'x', 'member', 0, 0)")
    else:
        from psycopg import errors
        with store.ingest.connection() as c:
            with pytest.raises(errors.UniqueViolation):
                c.execute("INSERT INTO users (email, name, role, created_at_ms, updated_at_ms) "
                          "VALUES ('Ada@Example.com', 'x', 'member', 0, 0)")


@pytest.mark.parametrize("email", ["", "ada", "ada@", "@example.com", "a b@example.com", "x@" + "e" * 260])
def test_what_is_not_an_email_is_refused(acc, email):
    with pytest.raises(AccountError, match="is not an email address"):
        acc.create_user(email, "Ada", "member", None, NOW)


def test_only_the_two_roles_exist(acc):
    with pytest.raises(AccountError, match="admin, member, not 'viewer'"):
        acc.create_user("v@example.com", "V", "viewer", None, NOW)
    u = acc.create_user("m@example.com", "M", "member", None, NOW)
    with pytest.raises(AccountError, match="not 'owner'"):
        acc.update_user(u.id, NOW, role="owner")


def test_the_last_admin_cant_be_removed_demoted_or_disabled(acc):
    a = admin(acc)
    for change in (lambda: acc.delete_user(a.id), lambda: acc.update_user(a.id, NOW, role="member"),
                   lambda: acc.update_user(a.id, NOW, disabled=True)):
        with pytest.raises(LastAdmin, match="ada@example.com is the only admin"):
            change()
    assert acc.user(a.id) == a
    b = acc.create_user("bob@example.com", "Bob", "admin", None, NOW)
    acc.update_user(a.id, NOW + 1, role="member")
    with pytest.raises(LastAdmin):
        acc.delete_user(b.id)
    acc.update_user(a.id, NOW + 2, role="admin")
    acc.delete_user(b.id)
    assert [u.email for u in acc.users()] == ["ada@example.com"]


def test_a_disabled_admin_doesnt_count(acc):
    a = admin(acc)
    b = acc.create_user("bob@example.com", "Bob", "admin", None, NOW)
    acc.update_user(b.id, NOW, disabled=True)
    assert acc.admin_exists()
    with pytest.raises(LastAdmin):
        acc.update_user(a.id, NOW, role="member")
    acc.update_user(b.id, NOW, disabled=False)
    acc.update_user(a.id, NOW, role="member")


def test_two_replicas_demoting_each_other_at_once_leave_an_admin(store, tmp_path):
    """Each write takes the accounts lock (Postgres) or the one writer (SQLite), so of two admins demoted at once
    by two connections, one stays admin."""
    acc = store.accounts
    a, b = admin(acc), acc.create_user("bob@example.com", "Bob", "admin", None, NOW)
    other = store.accounts if isinstance(store, Database) else type(store)(store.url).accounts
    results, barrier = [], threading.Barrier(2)

    def demote(x, uid):
        barrier.wait()
        try:
            x.update_user(uid, NOW, role="member")
            results.append("ok")
        except LastAdmin:
            results.append("last")
    ts = [threading.Thread(target=demote, args=(acc, a.id)), threading.Thread(target=demote, args=(other, b.id))]
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    assert sorted(results) == ["last", "ok"]
    assert acc.admin_exists()


def test_failed_sign_ins_lock_the_account_and_a_good_one_clears_them(acc):
    a = admin(acc)
    for n in range(1, 5):
        assert acc.failed_sign_in(a.id, NOW, limit=5, lock_ms=60_000).failed_count == n
    u = acc.failed_sign_in(a.id, NOW, limit=5, lock_ms=60_000)
    assert (u.failed_count, u.locked_until_ms) == (0, NOW + 60_000)
    acc.failed_sign_in(a.id, NOW, limit=5, lock_ms=60_000)
    acc.signed_in(a.id)
    u = acc.user(a.id)
    assert (u.failed_count, u.locked_until_ms) == (0, None)


def test_a_new_password_clears_the_lock(acc):
    a = admin(acc)
    acc.failed_sign_in(a.id, NOW, limit=1, lock_ms=60_000)
    u = acc.update_user(a.id, NOW + 5, password_hash="hash-new")
    assert (u.password_hash, u.locked_until_ms, u.updated_at_ms) == ("hash-new", None, NOW + 5)


def test_unknown_users_are_an_error(acc):
    for change in (lambda: acc.delete_user(99), lambda: acc.update_user(99, NOW, name="x"),
                   lambda: acc.failed_sign_in(99, NOW, 5, 1)):
        with pytest.raises(AccountError, match="there is no user 99"):
            change()


# ---- sessions --------------------------------------------------------------------------------------

def test_a_session_reads_back_with_its_user_until_it_expires(acc):
    a = admin(acc)
    acc.create_session("h1", a.id, NOW, NOW + 30 * DAY)
    s = acc.session("h1", NOW + 1)
    assert (s.user.email, s.created_at_ms, s.expires_at_ms, s.last_seen_ms) == ("ada@example.com", NOW,
                                                                              NOW + 30 * DAY, NOW)
    acc.touch_session("h1", NOW + 3_600_000)
    assert acc.session("h1", NOW + 2).last_seen_ms == NOW + 3_600_000
    assert acc.session("h1", NOW + 30 * DAY) is None
    assert acc.session("nope", NOW) is None
    acc.purge_sessions(NOW + 30 * DAY)
    acc.create_session("h1", a.id, NOW, NOW + DAY)  # purged, so the hash is free again


@pytest.mark.parametrize("change", [dict(role="member"), dict(password_hash="other"), dict(disabled=True)])
def test_a_new_role_or_password_or_disabling_ends_the_persons_sessions(acc, change):
    admin(acc)
    m = acc.create_user("m@example.com", "M", "admin", None, NOW)
    acc.create_session("h1", m.id, NOW, NOW + DAY)
    acc.create_session("h2", m.id, NOW, NOW + DAY)
    acc.update_user(m.id, NOW, **change)
    assert acc.session("h1", NOW) is None and acc.session("h2", NOW) is None


def test_a_new_name_keeps_the_sessions(acc):
    a = admin(acc)
    acc.create_session("h1", a.id, NOW, NOW + DAY)
    acc.update_user(a.id, NOW, name="Ada L", role="admin")  # the same role is no change
    assert acc.session("h1", NOW).user.name == "Ada L"


def test_signing_out_everywhere_and_removing_a_person_end_their_sessions(acc):
    admin(acc)
    m = acc.create_user("m@example.com", "M", "member", None, NOW)
    acc.create_session("h1", m.id, NOW, NOW + DAY)
    acc.create_session("h2", m.id, NOW, NOW + DAY)
    acc.delete_session("h1")
    assert acc.session("h1", NOW) is None and acc.session("h2", NOW) is not None
    assert acc.delete_sessions(m.id) == 1
    acc.create_session("h3", m.id, NOW, NOW + DAY)
    acc.delete_user(m.id)
    assert acc.session("h3", NOW) is None


# ---- tokens ----------------------------------------------------------------------------------------

def test_an_ingest_token_is_found_by_its_hash_until_revoked(acc):
    a = admin(acc)
    t = acc.create_token("ingest", "orders-api prod", "th1", "tbi_abcd", NOW, user_id=a.id)
    assert acc.any_ingest_token()
    found = acc.token_by_hash("th1")
    assert (found.kind, found.id, found.name, found.prefix, found.user_id, found.user) == (
        "ingest", t.id, "orders-api prod", "tbi_abcd", a.id, None)
    acc.tokens_used({("ingest", t.id): NOW + 60_000})
    acc.tokens_used({("ingest", t.id): NOW + 1})  # a replica with an older time doesn't move it back
    assert acc.tokens("ingest")[0].last_used_ms == NOW + 60_000
    r = acc.revoke_token("ingest", t.id, NOW + 2)
    assert r.revoked_at_ms == NOW + 2
    assert acc.token_by_hash("th1") is None and not acc.any_ingest_token()
    assert acc.tokens("ingest") == [] and [x.id for x in acc.tokens("ingest", revoked=True)] == [t.id]
    with pytest.raises(AccountError, match="there is no ingest token"):
        acc.revoke_token("ingest", t.id, NOW)


def test_an_ingest_token_outlives_the_admin_who_made_it(acc):
    admin(acc)
    b = acc.create_user("bob@example.com", "Bob", "admin", None, NOW)
    acc.create_token("ingest", "svc", "th1", "tbi_x", NOW, user_id=b.id)
    acc.delete_user(b.id)
    t = acc.token_by_hash("th1")
    assert t is not None and t.user_id is None


def test_a_personal_token_carries_its_owner_and_goes_with_them(acc):
    admin(acc)
    m = acc.create_user("m@example.com", "M", "member", None, NOW)
    t = acc.create_token("personal", "ci", "ph1", "tbp_abcd", NOW, user_id=m.id)
    found = acc.token_by_hash("ph1")
    assert (found.kind, found.id, found.user.email, found.user.role) == ("personal", t.id, "m@example.com", "member")
    acc.update_user(m.id, NOW, role="admin")
    assert acc.token_by_hash("ph1").user.role == "admin"  # the owner's role now, not when it was made
    acc.update_user(m.id, NOW, disabled=True)
    assert acc.token_by_hash("ph1") is None
    acc.update_user(m.id, NOW, disabled=False)
    assert acc.token_by_hash("ph1") is not None
    acc.delete_user(m.id)
    assert acc.token_by_hash("ph1") is None and acc.tokens("personal") == []


def test_personal_tokens_are_listed_and_revoked_per_owner(acc):
    a = admin(acc)
    m = acc.create_user("m@example.com", "M", "member", None, NOW)
    ta = acc.create_token("personal", "mine", "ph1", "tbp_a", NOW, user_id=a.id)
    tm = acc.create_token("personal", "theirs", "ph2", "tbp_m", NOW + 1, user_id=m.id)
    assert [t.id for t in acc.tokens("personal")] == [ta.id, tm.id]
    assert [t.id for t in acc.tokens("personal", user_id=m.id)] == [tm.id]
    with pytest.raises(AccountError):
        acc.revoke_token("personal", ta.id, NOW, user_id=m.id)  # not theirs
    acc.revoke_token("personal", tm.id, NOW, user_id=m.id)
    assert acc.token_by_hash("ph2") is None and acc.token_by_hash("ph1") is not None


def test_tokens_need_a_name_and_personal_ones_an_owner(acc):
    with pytest.raises(AccountError, match="token name can't be empty"):
        acc.create_token("ingest", " ", "h", "p", NOW)
    with pytest.raises(AccountError, match="needs an owner"):
        acc.create_token("personal", "x", "h", "p", NOW)
    with pytest.raises(AccountError, match="there is no user 5"):
        acc.create_token("personal", "x", "h", "p", NOW, user_id=5)
    assert acc.tokens("ingest") == [] and acc.tokens("personal") == []


# ---- the setup code --------------------------------------------------------------------------------

def test_the_setup_code_creates_the_first_admin_once(acc):
    acc.replace_setup_code("c1", NOW, NOW + 3_600_000)
    assert acc.setup_code_valid("c1", NOW)
    assert acc.use_setup_code("wrong", NOW, "ada@example.com", "Ada", "h") is None
    u = acc.use_setup_code("c1", NOW + 1, "Ada@example.com", "Ada", "h")
    assert (u.email, u.role) == ("ada@example.com", "admin")
    assert not acc.setup_code_valid("c1", NOW + 2)
    assert acc.use_setup_code("c1", NOW + 2, "eve@example.com", "Eve", "h") is None
    assert [x.email for x in acc.users()] == ["ada@example.com"]


def test_a_setup_code_expires_and_a_new_start_replaces_it(acc):
    acc.replace_setup_code("c1", NOW, NOW + 3_600_000)
    assert acc.use_setup_code("c1", NOW + 3_600_000, "ada@example.com", "Ada", "h") is None  # expired
    acc.replace_setup_code("c2", NOW, NOW + 3_600_000)
    acc.replace_setup_code("c3", NOW, NOW + 3_600_000)
    assert not acc.setup_code_valid("c2", NOW)
    assert acc.use_setup_code("c2", NOW, "ada@example.com", "Ada", "h") is None
    assert acc.use_setup_code("c3", NOW, "ada@example.com", "Ada", "h") is not None


def test_a_setup_code_creates_no_admin_once_one_exists(acc):
    acc.replace_setup_code("c1", NOW, NOW + 3_600_000)
    acc.create_user("bob@example.com", "Bob", "admin", "h", NOW)  # trailbeam users create, meanwhile
    assert acc.use_setup_code("c1", NOW, "eve@example.com", "Eve", "h") is None
    assert [x.email for x in acc.users()] == ["bob@example.com"]


def test_two_uses_of_one_setup_code_at_once_create_one_admin(store):
    acc = store.accounts
    acc.replace_setup_code("c1", NOW, NOW + 3_600_000)
    other = store.accounts if isinstance(store, Database) else type(store)(store.url).accounts
    barrier, made = threading.Barrier(2), []

    def use(x, email):
        barrier.wait()
        made.append(x.use_setup_code("c1", NOW, email, "X", "h"))
    ts = [threading.Thread(target=use, args=(acc, "a@example.com")),
          threading.Thread(target=use, args=(other, "b@example.com"))]
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    assert sum(u is not None for u in made) == 1
    assert len(acc.users()) == 1


def test_the_admin_from_the_environment_is_made_only_while_there_is_none(acc):
    """TRAILBEAM_ADMIN_EMAIL, read at start: only_if_no_admin is checked in the creating transaction."""
    assert admin(acc, only_if_no_admin=True) is not None
    assert acc.create_user("eve@example.com", "Eve", "admin", "h", NOW, only_if_no_admin=True) is None
    assert len(acc.users()) == 1


# ---- the tables beside the spans -------------------------------------------------------------------

def test_retention_and_vacuum_delete_no_account_rows(tmp_path):
    from spans import Consumer, to_proto
    from trailbeam.server.otlp import decode
    from trailbeam.server.retention import Purger

    sec = 1_000_000_000
    t0 = NOW * 1_000_000
    db = Database(str(tmp_path / "t.sqlite"))
    try:
        a = admin(db.accounts)
        db.accounts.create_session("h1", a.id, NOW, NOW + 400 * DAY)
        db.accounts.create_token("ingest", "svc", "th1", "tbi_x", NOW)
        db.accounts.create_token("personal", "ci", "ph1", "tbp_x", NOW, user_id=a.id)
        w = Consumer()
        d = decode(to_proto([w.send(t0, "old"), w.receive(t0 + sec, "old", "h-old")]).SerializeToString(),
                   "application/x-protobuf")
        db.insert(d.records)
        r = Purger(db, Retention(86_400), now=lambda: t0 + 10 * 86_400 * sec, pause_s=0).run_pass()
        assert r["deleted_spans"] == 2
        db.vacuum()
        assert db.accounts.admin_exists() and db.accounts.session("h1", NOW) is not None
        assert db.accounts.token_by_hash("th1") is not None and db.accounts.token_by_hash("ph1") is not None
    finally:
        db.close()


def test_the_auth_tables_are_in_the_schema_of_a_new_file():
    names = {s.split()[5] for s in migrations.statements(accounts.SQLITE_CREATE) if s.startswith("CREATE TABLE")}
    assert names == set(accounts.TABLES)
    assert migrations.MIGRATIONS[-1].name == "users, sessions and tokens"
