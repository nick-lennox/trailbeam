"""People, sessions, tokens and the setup code, as the store keeps them (users and roles, phase 1: QV-225).

The tables (SQLite schema 14, migrations.py; Postgres schema 7, pg_schema.py):

- ``users``: a person. ``email`` is the sign-in name, unique ignoring case (stored lower-cased by this module;
  SQLite also declares it ``COLLATE NOCASE``, Postgres has a unique index on ``lower(email)``). ``role`` is
  ``admin`` or ``member`` (checked here, not by a CHECK constraint: SQLite can't change one without rebuilding the
  table, and a later role must not need that). ``password_hash`` is the encoded scrypt hash (passwords.py), NULL
  for a person with no password yet. ``failed_count`` and ``locked_until_ms`` are the per-account sign-in limit:
  kept here, not in memory, so every replica sees the same count. ``disabled_at_ms``: the person can't sign in.
- ``sessions``: a signed-in browser. ``id_hash`` is the SHA-256 (hex) of the cookie's random ID; the ID itself is
  never stored. A session ends at ``expires_at_ms`` (fixed when it is made, no sliding), and ``last_seen_ms`` is
  written at most hourly. Removing a user removes their sessions.
- ``ingest_tokens``: a named token a service sends spans with (``tbi_…``). ``token_hash`` is its SHA-256 (hex);
  ``prefix`` its first characters, for display. ``revoked_at_ms`` set: refused.
- ``personal_tokens``: a person's API token (``tbp_…``), with that person's role. Removed with the person.
- ``setup_codes``: the one-time code of the setup link ``trailbeam start`` prints (SHA-256, hex). Each start
  replaces it; it is used up in the transaction that creates the first admin.

None of these tables holds spans: retention and ``--vacuum`` delete no row of them.

**Concurrency.** Every write goes through the store's writer (SQLite: its one writer connection and lock;
Postgres: a transaction that first takes ``ACCOUNTS_LOCK`` with ``pg_advisory_xact_lock``), so two replicas
can't both use one setup code, both create the first admin, or each demote one of the last two admins.
"""
from __future__ import annotations

import contextlib
import re
from dataclasses import dataclass
from typing import Any, Callable, ContextManager, Iterator, Optional

ROLES = ("admin", "member")
ADMIN = "admin"

# pg_advisory_xact_lock key of account writes: "trailbm" + 2 (pg_schema.MIGRATION_LOCK is + 1).
ACCOUNTS_LOCK = 0x7472_6169_6c62_6d02

EMAIL_MAX = 254
NAME_MAX = 200
TOKEN_NAME_MAX = 100
_EMAIL = re.compile(r"^[^@\s]+@[^@\s]+$")

SQLITE_CREATE = """
CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY,
    email TEXT NOT NULL COLLATE NOCASE UNIQUE,
    name TEXT NOT NULL,
    role TEXT NOT NULL,                  -- admin | member (accounts.ROLES)
    password_hash TEXT,                  -- passwords.py; NULL: no password set
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    failed_count INTEGER NOT NULL DEFAULT 0,   -- failed sign-ins since the last one that worked
    locked_until_ms INTEGER,             -- no sign-in before this
    disabled_at_ms INTEGER               -- set: can't sign in
);
CREATE INDEX IF NOT EXISTS users_role ON users (role);
CREATE TABLE IF NOT EXISTS sessions (
    id_hash TEXT PRIMARY KEY,            -- SHA-256 (hex) of the cookie's session ID
    user_id INTEGER NOT NULL REFERENCES users (id) ON DELETE CASCADE,
    created_at_ms INTEGER NOT NULL,
    expires_at_ms INTEGER NOT NULL,
    last_seen_ms INTEGER NOT NULL        -- written at most hourly
);
CREATE INDEX IF NOT EXISTS sessions_user ON sessions (user_id);
CREATE INDEX IF NOT EXISTS sessions_expires ON sessions (expires_at_ms);
CREATE TABLE IF NOT EXISTS ingest_tokens (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    token_hash TEXT NOT NULL UNIQUE,     -- SHA-256 (hex) of the token
    prefix TEXT NOT NULL,                -- its first characters, for display
    created_at_ms INTEGER NOT NULL,
    created_by INTEGER REFERENCES users (id) ON DELETE SET NULL,
    last_used_ms INTEGER,                -- written about once a minute per replica
    revoked_at_ms INTEGER
);
CREATE TABLE IF NOT EXISTS personal_tokens (
    id INTEGER PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users (id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    token_hash TEXT NOT NULL UNIQUE,
    prefix TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL,
    last_used_ms INTEGER,
    revoked_at_ms INTEGER
);
CREATE INDEX IF NOT EXISTS personal_tokens_user ON personal_tokens (user_id);
CREATE TABLE IF NOT EXISTS setup_codes (
    code_hash TEXT PRIMARY KEY,          -- SHA-256 (hex) of the setup link's code
    created_at_ms INTEGER NOT NULL,
    expires_at_ms INTEGER NOT NULL
);
"""

PG_CREATE = """
CREATE TABLE IF NOT EXISTS users (
    id BIGSERIAL PRIMARY KEY,
    email TEXT NOT NULL,
    name TEXT NOT NULL,
    role TEXT NOT NULL,
    password_hash TEXT,
    created_at_ms BIGINT NOT NULL,
    updated_at_ms BIGINT NOT NULL,
    failed_count INTEGER NOT NULL DEFAULT 0,
    locked_until_ms BIGINT,
    disabled_at_ms BIGINT
);
CREATE UNIQUE INDEX IF NOT EXISTS users_email ON users (lower(email));
CREATE INDEX IF NOT EXISTS users_role ON users (role);
CREATE TABLE IF NOT EXISTS sessions (
    id_hash TEXT PRIMARY KEY,
    user_id BIGINT NOT NULL REFERENCES users (id) ON DELETE CASCADE,
    created_at_ms BIGINT NOT NULL,
    expires_at_ms BIGINT NOT NULL,
    last_seen_ms BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS sessions_user ON sessions (user_id);
CREATE INDEX IF NOT EXISTS sessions_expires ON sessions (expires_at_ms);
CREATE TABLE IF NOT EXISTS ingest_tokens (
    id BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    token_hash TEXT NOT NULL UNIQUE,
    prefix TEXT NOT NULL,
    created_at_ms BIGINT NOT NULL,
    created_by BIGINT REFERENCES users (id) ON DELETE SET NULL,
    last_used_ms BIGINT,
    revoked_at_ms BIGINT
);
CREATE TABLE IF NOT EXISTS personal_tokens (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL REFERENCES users (id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    token_hash TEXT NOT NULL UNIQUE,
    prefix TEXT NOT NULL,
    created_at_ms BIGINT NOT NULL,
    last_used_ms BIGINT,
    revoked_at_ms BIGINT
);
CREATE INDEX IF NOT EXISTS personal_tokens_user ON personal_tokens (user_id);
CREATE TABLE IF NOT EXISTS setup_codes (
    code_hash TEXT PRIMARY KEY,
    created_at_ms BIGINT NOT NULL,
    expires_at_ms BIGINT NOT NULL
);
"""

TABLES = ("users", "sessions", "ingest_tokens", "personal_tokens", "setup_codes")


class AccountError(ValueError):
    """A change the rules don't allow (the message says which, in words for the person who asked)."""


class LastAdmin(AccountError):
    pass


class UserExists(AccountError):
    pass


@dataclass(frozen=True)
class User:
    id: int
    email: str
    name: str
    role: str
    password_hash: Optional[str]
    created_at_ms: int
    updated_at_ms: int
    failed_count: int
    locked_until_ms: Optional[int]
    disabled_at_ms: Optional[int]

    @property
    def active(self) -> bool:
        return self.disabled_at_ms is None


@dataclass(frozen=True)
class Session:
    id_hash: str
    user: User
    created_at_ms: int
    expires_at_ms: int
    last_seen_ms: int


@dataclass(frozen=True)
class Token:
    kind: str                     # "ingest" | "personal"
    id: int
    name: str
    prefix: str
    created_at_ms: int
    last_used_ms: Optional[int]
    revoked_at_ms: Optional[int]
    user_id: Optional[int] = None     # personal: the owner; ingest: who created it (None: the CLI, or removed)
    user: Optional[User] = None       # personal, from token_by_hash: the owner

    @property
    def active(self) -> bool:
        return self.revoked_at_ms is None


_USER_COLUMNS = ("id, email, name, role, password_hash, created_at_ms, updated_at_ms, failed_count, "
                 "locked_until_ms, disabled_at_ms")


def _user(r: Optional[dict], prefix: str = "") -> Optional[User]:
    if r is None:
        return None
    return User(**{k: r[prefix + k] for k in User.__dataclass_fields__})


def normal_email(email: str) -> str:
    """The email as stored and compared: trimmed and lower-cased. Raises AccountError if it isn't one."""
    e = (email or "").strip().lower()
    if not _EMAIL.match(e) or len(e) > EMAIL_MAX:
        raise AccountError(f"{email!r} is not an email address")
    return e


def check_role(role: str) -> str:
    if role not in ROLES:
        raise AccountError(f"the role must be one of {', '.join(ROLES)}, not {role!r}")
    return role


def _name(name: str, limit: int, what: str) -> str:
    n = (name or "").strip()
    if not n:
        raise AccountError(f"the {what} can't be empty")
    if len(n) > limit:
        raise AccountError(f"the {what} is longer than {limit} characters")
    return n


class Sql:
    """How Accounts reaches a store: ``write()`` and ``read()`` give a function ``q(sql, params) -> list[dict]``,
    with ``?`` placeholders. ``write()`` is one transaction, serialised with every other account write (see the
    module docstring); ``read()`` may see a snapshot a moment old. ``integrity`` is the store driver's unique
    violation exception."""

    integrity: type = Exception

    def write(self) -> ContextManager[Callable[..., list]]:
        raise NotImplementedError

    def read(self) -> ContextManager[Callable[..., list]]:
        raise NotImplementedError


class SqliteSql(Sql):
    def __init__(self, store):
        import sqlite3
        self.store = store
        self.integrity = sqlite3.IntegrityError

    @staticmethod
    def _q(c):
        def q(sql: str, params: tuple = ()) -> list:
            return [dict(r) for r in c.execute(sql, params).fetchall()]
        return q

    @contextlib.contextmanager
    def write(self) -> Iterator[Callable[..., list]]:
        with self.store.lock:
            c = self.store.conn
            c.execute("BEGIN IMMEDIATE")
            try:
                yield self._q(c)
                c.execute("COMMIT")
            except BaseException:
                c.execute("ROLLBACK")
                raise

    @contextlib.contextmanager
    def read(self) -> Iterator[Callable[..., list]]:
        with self.store.reader() as c:
            yield self._q(c)


class PostgresSql(Sql):
    def __init__(self, store):
        from psycopg import errors
        self.store = store
        self.integrity = errors.UniqueViolation

    @staticmethod
    def _q(c):
        def q(sql: str, params: tuple = ()) -> list:
            cur = c.execute(sql.replace("?", "%s"), params)
            return [dict(r) for r in cur.fetchall()] if cur.description else []
        return q

    @contextlib.contextmanager
    def write(self) -> Iterator[Callable[..., list]]:
        with self.store.ingest.connection() as c:
            with c.transaction():
                c.execute("SELECT pg_advisory_xact_lock(%s)", (ACCOUNTS_LOCK,))
                yield self._q(c)

    @contextlib.contextmanager
    def read(self) -> Iterator[Callable[..., list]]:
        with self.store.reads.connection() as c:
            yield self._q(c)


class Accounts:
    """People, sessions, tokens and the setup code of one store. Times are ms since the epoch, passed in, so
    tests choose them. Hashing is the caller's (passwords.py, tokens.py): this module stores what it is given."""

    def __init__(self, sql: Sql):
        self.sql = sql

    # ---- people ------------------------------------------------------------------------------

    def admin_exists(self) -> bool:
        """At least one admin who isn't disabled: the switch from the shared token to accounts."""
        with self.sql.read() as q:
            return bool(q("SELECT 1 AS one FROM users WHERE role = 'admin' AND disabled_at_ms IS NULL LIMIT 1"))

    def users(self) -> list[User]:
        with self.sql.read() as q:
            return [_user(r) for r in q(f"SELECT {_USER_COLUMNS} FROM users ORDER BY lower(email)")]

    def user(self, user_id: int) -> Optional[User]:
        with self.sql.read() as q:
            rows = q(f"SELECT {_USER_COLUMNS} FROM users WHERE id = ?", (user_id,))
        return _user(rows[0]) if rows else None

    def user_by_email(self, email: str) -> Optional[User]:
        try:
            e = normal_email(email)
        except AccountError:
            return None
        with self.sql.read() as q:
            rows = q(f"SELECT {_USER_COLUMNS} FROM users WHERE lower(email) = ?", (e,))
        return _user(rows[0]) if rows else None

    def create_user(self, email: str, name: str, role: str, password_hash: Optional[str], now_ms: int, *,
                    only_if_no_admin: bool = False) -> Optional[User]:
        """Add a person. ``only_if_no_admin``: only while no admin exists (``TRAILBEAM_ADMIN_EMAIL``, read at
        start), checked in the same transaction; None if one does. Raises UserExists for a taken email."""
        e, n, role = normal_email(email), _name(name, NAME_MAX, "name"), check_role(role)
        try:
            with self.sql.write() as q:
                if only_if_no_admin and _admins(q) > 0:
                    return None
                return _insert_user(q, e, n, role, password_hash, now_ms)
        except self.sql.integrity:
            raise UserExists(f"{e} already has an account") from None

    def update_user(self, user_id: int, now_ms: int, *, name: Optional[str] = None, role: Optional[str] = None,
                    password_hash: Optional[str] = None, disabled: Optional[bool] = None) -> User:
        """Change a person. A new role, a new password or disabling them also ends their sessions. Raises
        LastAdmin rather than leave no active admin, AccountError for an unknown user."""
        sets, params = ["updated_at_ms = ?"], [now_ms]
        if name is not None:
            sets.append("name = ?")
            params.append(_name(name, NAME_MAX, "name"))
        if role is not None:
            sets.append("role = ?")
            params.append(check_role(role))
        if password_hash is not None:
            sets += ["password_hash = ?", "failed_count = 0", "locked_until_ms = NULL"]
            params.append(password_hash)
        if disabled is not None:
            sets.append("disabled_at_ms = ?")
            params.append(now_ms if disabled else None)
        with self.sql.write() as q:
            before = _locked_user(q, user_id)
            was_admin = before.role == ADMIN and before.active
            stays_admin = (role or before.role) == ADMIN and (before.active if disabled is None else not disabled)
            if was_admin and not stays_admin and _admins(q) <= 1:
                raise LastAdmin(f"{before.email} is the only admin; make someone else an admin first")
            q(f"UPDATE users SET {', '.join(sets)} WHERE id = ?", tuple(params) + (user_id,))
            if (role is not None and role != before.role) or password_hash is not None or disabled:
                q("DELETE FROM sessions WHERE user_id = ?", (user_id,))
            return _user(q(f"SELECT {_USER_COLUMNS} FROM users WHERE id = ?", (user_id,))[0])

    def delete_user(self, user_id: int) -> User:
        """Remove a person, with their sessions and personal tokens. Raises LastAdmin for the last admin."""
        with self.sql.write() as q:
            u = _locked_user(q, user_id)
            if u.role == ADMIN and u.active and _admins(q) <= 1:
                raise LastAdmin(f"{u.email} is the only admin; make someone else an admin first")
            # explicit, not only ON DELETE CASCADE: SQLite enforces foreign keys only with PRAGMA foreign_keys
            q("DELETE FROM sessions WHERE user_id = ?", (user_id,))
            q("DELETE FROM personal_tokens WHERE user_id = ?", (user_id,))
            q("UPDATE ingest_tokens SET created_by = NULL WHERE created_by = ?", (user_id,))
            q("DELETE FROM users WHERE id = ?", (user_id,))
            return u

    def failed_sign_in(self, user_id: int, now_ms: int, limit: int, lock_ms: int) -> User:
        """Count a failed sign-in; the ``limit``-th in a row locks the account for ``lock_ms`` and starts the
        count again."""
        with self.sql.write() as q:
            u = _locked_user(q, user_id)
            n = u.failed_count + 1
            if n >= limit:
                q("UPDATE users SET failed_count = 0, locked_until_ms = ? WHERE id = ?", (now_ms + lock_ms, user_id))
            else:
                q("UPDATE users SET failed_count = ? WHERE id = ?", (n, user_id))
            return _user(q(f"SELECT {_USER_COLUMNS} FROM users WHERE id = ?", (user_id,))[0])

    def signed_in(self, user_id: int) -> None:
        """A sign-in worked: the failed count starts again (written only when there is one to clear)."""
        with self.sql.write() as q:
            q("UPDATE users SET failed_count = 0, locked_until_ms = NULL "
              "WHERE id = ? AND (failed_count > 0 OR locked_until_ms IS NOT NULL)", (user_id,))

    # ---- sessions ----------------------------------------------------------------------------

    def create_session(self, id_hash: str, user_id: int, now_ms: int, expires_at_ms: int) -> None:
        with self.sql.write() as q:
            q("INSERT INTO sessions (id_hash, user_id, created_at_ms, expires_at_ms, last_seen_ms) "
              "VALUES (?, ?, ?, ?, ?)", (id_hash, user_id, now_ms, expires_at_ms, now_ms))

    def session(self, id_hash: str, now_ms: int) -> Optional[Session]:
        """The live session with this hash, and its user; None if it is unknown, has expired, or its user is
        disabled. One read, through the read pool."""
        cols = ", ".join(f"u.{c} AS u_{c}" for c in _USER_COLUMNS.split(", "))
        with self.sql.read() as q:
            rows = q(f"SELECT s.id_hash, s.created_at_ms, s.expires_at_ms, s.last_seen_ms, {cols} "
                     "FROM sessions s JOIN users u ON u.id = s.user_id "
                     "WHERE s.id_hash = ? AND s.expires_at_ms > ? AND u.disabled_at_ms IS NULL", (id_hash, now_ms))
        if not rows:
            return None
        r = rows[0]
        return Session(r["id_hash"], _user(r, "u_"), r["created_at_ms"], r["expires_at_ms"], r["last_seen_ms"])

    def touch_session(self, id_hash: str, now_ms: int) -> None:
        with self.sql.write() as q:
            q("UPDATE sessions SET last_seen_ms = ? WHERE id_hash = ?", (now_ms, id_hash))

    def delete_session(self, id_hash: str) -> None:
        with self.sql.write() as q:
            q("DELETE FROM sessions WHERE id_hash = ?", (id_hash,))

    def delete_sessions(self, user_id: int) -> int:
        with self.sql.write() as q:
            n = len(q("SELECT id_hash FROM sessions WHERE user_id = ?", (user_id,)))
            q("DELETE FROM sessions WHERE user_id = ?", (user_id,))
            return n

    def purge_sessions(self, now_ms: int) -> None:
        """Delete expired sessions and setup codes."""
        with self.sql.write() as q:
            q("DELETE FROM sessions WHERE expires_at_ms <= ?", (now_ms,))
            q("DELETE FROM setup_codes WHERE expires_at_ms <= ?", (now_ms,))

    # ---- tokens ------------------------------------------------------------------------------

    def create_token(self, kind: str, name: str, token_hash: str, prefix: str, now_ms: int, *,
                     user_id: Optional[int] = None) -> Token:
        """``kind`` "ingest" (``user_id``: who created it, or None) or "personal" (``user_id``: the owner)."""
        n = _name(name, TOKEN_NAME_MAX, "token name")
        with self.sql.write() as q:
            if kind == "ingest":
                q("INSERT INTO ingest_tokens (name, token_hash, prefix, created_at_ms, created_by) "
                  "VALUES (?, ?, ?, ?, ?)", (n, token_hash, prefix, now_ms, user_id))
                r = q("SELECT id FROM ingest_tokens WHERE token_hash = ?", (token_hash,))
            elif kind == "personal":
                if user_id is None:
                    raise AccountError("a personal token needs an owner")
                _locked_user(q, user_id)
                q("INSERT INTO personal_tokens (user_id, name, token_hash, prefix, created_at_ms) "
                  "VALUES (?, ?, ?, ?, ?)", (user_id, n, token_hash, prefix, now_ms))
                r = q("SELECT id FROM personal_tokens WHERE token_hash = ?", (token_hash,))
            else:
                raise AccountError(f"unknown token kind {kind!r}")
        return Token(kind, r[0]["id"], n, prefix, now_ms, None, None, user_id)

    def tokens(self, kind: str, *, user_id: Optional[int] = None, revoked: bool = False) -> list[Token]:
        """Tokens of a kind, oldest first; personal ones of ``user_id`` only, if given. Revoked ones only with
        ``revoked``."""
        where, params = ([] if revoked else ["revoked_at_ms IS NULL"]), []
        if kind == "ingest":
            sql = "SELECT id, name, prefix, created_at_ms, last_used_ms, revoked_at_ms, created_by AS user_id " \
                  "FROM ingest_tokens"
        elif kind == "personal":
            sql = "SELECT id, name, prefix, created_at_ms, last_used_ms, revoked_at_ms, user_id FROM personal_tokens"
            if user_id is not None:
                where.append("user_id = ?")
                params.append(user_id)
        else:
            raise AccountError(f"unknown token kind {kind!r}")
        if where:
            sql += " WHERE " + " AND ".join(where)
        with self.sql.read() as q:
            rows = q(sql + " ORDER BY created_at_ms, id", tuple(params))
        return [Token(kind, r["id"], r["name"], r["prefix"], r["created_at_ms"], r["last_used_ms"],
                      r["revoked_at_ms"], r["user_id"]) for r in rows]

    def any_ingest_token(self) -> bool:
        """At least one ingest token not revoked: ingest needs a token from then on (proposal section 4)."""
        with self.sql.read() as q:
            return bool(q("SELECT 1 AS one FROM ingest_tokens WHERE revoked_at_ms IS NULL LIMIT 1"))

    def token_by_hash(self, token_hash: str) -> Optional[Token]:
        """The live token with this hash, of either kind; a personal one with its owner, None if the owner is
        disabled."""
        with self.sql.read() as q:
            rows = q("SELECT id, name, prefix, created_at_ms, last_used_ms, revoked_at_ms, created_by AS user_id "
                     "FROM ingest_tokens WHERE token_hash = ? AND revoked_at_ms IS NULL", (token_hash,))
            if rows:
                r = rows[0]
                return Token("ingest", r["id"], r["name"], r["prefix"], r["created_at_ms"], r["last_used_ms"], None,
                             r["user_id"])
            cols = ", ".join(f"u.{c} AS u_{c}" for c in _USER_COLUMNS.split(", "))
            rows = q(f"SELECT t.id, t.name, t.prefix, t.created_at_ms, t.last_used_ms, {cols} "
                     "FROM personal_tokens t JOIN users u ON u.id = t.user_id "
                     "WHERE t.token_hash = ? AND t.revoked_at_ms IS NULL AND u.disabled_at_ms IS NULL", (token_hash,))
        if not rows:
            return None
        r = rows[0]
        u = _user(r, "u_")
        return Token("personal", r["id"], r["name"], r["prefix"], r["created_at_ms"], r["last_used_ms"], None, u.id, u)

    def revoke_token(self, kind: str, token_id: int, now_ms: int, *, user_id: Optional[int] = None) -> Token:
        """Revoke a token (``user_id``: only if it is that person's personal token). Raises AccountError if
        there is no such live token."""
        table = {"ingest": "ingest_tokens", "personal": "personal_tokens"}.get(kind)
        if table is None:
            raise AccountError(f"unknown token kind {kind!r}")
        owner = " AND user_id = ?" if user_id is not None and kind == "personal" else ""
        params = (token_id,) + ((user_id,) if owner else ())
        with self.sql.write() as q:
            rows = q(f"SELECT id, name, prefix, created_at_ms, last_used_ms FROM {table} "
                     f"WHERE id = ? AND revoked_at_ms IS NULL{owner}", params)
            if not rows:
                raise AccountError(f"there is no {kind} token {token_id}")
            q(f"UPDATE {table} SET revoked_at_ms = ? WHERE id = ?", (now_ms, token_id))
        r = rows[0]
        return Token(kind, r["id"], r["name"], r["prefix"], r["created_at_ms"], r["last_used_ms"], now_ms, user_id)

    def tokens_used(self, used: dict[tuple[str, int], int]) -> None:
        """Write "last used" for ``{(kind, id): ms}`` in one transaction, never moving a time back."""
        if not used:
            return
        with self.sql.write() as q:
            for (kind, token_id), ms in sorted(used.items()):
                table = {"ingest": "ingest_tokens", "personal": "personal_tokens"}[kind]
                q(f"UPDATE {table} SET last_used_ms = ? WHERE id = ? AND (last_used_ms IS NULL OR last_used_ms < ?)",
                  (ms, token_id, ms))

    # ---- the setup code ----------------------------------------------------------------------

    def replace_setup_code(self, code_hash: str, now_ms: int, expires_at_ms: int) -> None:
        """The setup link's code, replacing any earlier one (each ``trailbeam start`` makes a new one)."""
        with self.sql.write() as q:
            q("DELETE FROM setup_codes")
            q("INSERT INTO setup_codes (code_hash, created_at_ms, expires_at_ms) VALUES (?, ?, ?)",
              (code_hash, now_ms, expires_at_ms))

    def setup_code_valid(self, code_hash: str, now_ms: int) -> bool:
        with self.sql.read() as q:
            return bool(q("SELECT 1 AS one FROM setup_codes WHERE code_hash = ? AND expires_at_ms > ?",
                          (code_hash, now_ms)))

    def use_setup_code(self, code_hash: str, now_ms: int, email: str, name: str, password_hash: str) -> Optional[User]:
        """Use up the setup code and create the first admin, in one transaction: None (and nothing changed) if
        the code is unknown or expired, or an admin exists already. Raises UserExists for a taken email."""
        e, n = normal_email(email), _name(name, NAME_MAX, "name")
        try:
            with self.sql.write() as q:
                if not q("SELECT 1 AS one FROM setup_codes WHERE code_hash = ? AND expires_at_ms > ?",
                         (code_hash, now_ms)) or _admins(q) > 0:
                    return None
                q("DELETE FROM setup_codes")
                return _insert_user(q, e, n, ADMIN, password_hash, now_ms)
        except self.sql.integrity:
            raise UserExists(f"{e} already has an account") from None


def _admins(q) -> int:
    return q("SELECT COUNT(*) AS n FROM users WHERE role = 'admin' AND disabled_at_ms IS NULL")[0]["n"]


def _locked_user(q, user_id: int) -> User:
    rows = q(f"SELECT {_USER_COLUMNS} FROM users WHERE id = ?", (user_id,))
    if not rows:
        raise AccountError(f"there is no user {user_id}")
    return _user(rows[0])


def _insert_user(q, email: str, name: str, role: str, password_hash: Optional[str], now_ms: int) -> User:
    # no RETURNING: SQLite has it from 3.35 only, and Python 3.10 may come with an older one
    q("INSERT INTO users (email, name, role, password_hash, created_at_ms, updated_at_ms) VALUES (?, ?, ?, ?, ?, ?)",
      (email, name, role, password_hash, now_ms, now_ms))
    return _user(q(f"SELECT {_USER_COLUMNS} FROM users WHERE lower(email) = ?", (email,))[0])
