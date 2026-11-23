"""People and their sessions in the store (account_tables.py), the same on SQLite and Postgres.

``Accounts(store)`` reads and writes the ``users`` and ``sessions`` tables of either store:

- **Reads** go through the store's read connections (SQLite: the pooled read-only connections; Postgres: the
  read pool), never the writer, so looking up a session doesn't wait for an export.
- **Writes** are short transactions on the store's writer (SQLite: the writer connection under its lock, like
  ``record_probes``; Postgres: a connection from the ingest pool). They are rare: a sign-in, a sign-out, a
  change to a person, and a session's "last seen" at most once an hour.

Emails are compared ignoring case as each store does it: SQLite's ``NOCASE`` folds ASCII letters only, Postgres's
``lower()`` folds every letter its locale knows. Emails are therefore kept as typed, and the code never relies on
two emails differing only in non-ASCII case.

Nothing here hashes passwords or makes session IDs: passwords.py and sessions.py do that, and pass in hashes.
"""
from __future__ import annotations

import contextlib
import re
from typing import Any, Iterator, Optional

ROLES = ("admin", "member")
MAX_EMAIL = 254
MAX_NAME = 200
_EMAIL = re.compile(r"[^@\s]+@[^@\s]+")

USER_COLUMNS = ("id", "email", "name", "role", "password_hash", "created_at_ms", "updated_at_ms",
                "disabled_at_ms", "failed_count", "locked_until_ms", "last_sign_in_ms")
_USER_SELECT = "SELECT " + ", ".join(USER_COLUMNS) + " FROM users"


class AccountError(ValueError):
    """A person that can't be stored as given; the message says why."""


class EmailTaken(AccountError):
    pass


def check_email(email: str) -> str:
    e = (email or "").strip()
    if len(e) > MAX_EMAIL or not _EMAIL.fullmatch(e):
        raise AccountError("the email must look like name@example.com")
    return e


def check_name(name: str) -> str:
    n = " ".join((name or "").split())
    if not n or len(n) > MAX_NAME or not n.isprintable():
        raise AccountError(f"the name must be 1 to {MAX_NAME} printable characters")
    return n


def check_role(role: str) -> str:
    if role not in ROLES:
        raise AccountError(f"the role must be one of {', '.join(ROLES)}, got {role!r}")
    return role


class Accounts:
    def __init__(self, store):
        self.store = store
        self.postgres = hasattr(store, "reads") and hasattr(store, "ingest")
        if self.postgres:
            import psycopg

            self._unique = psycopg.errors.UniqueViolation
        else:
            import sqlite3

            self._unique = sqlite3.IntegrityError

    # ---- the two kinds of connection --------------------------------------------------------------

    def _sql(self, q: str) -> str:
        return q.replace("?", "%s") if self.postgres else q

    def _read(self, q: str, args: tuple = ()) -> list[dict]:
        if self.postgres:
            with self.store.reads.connection() as c:
                return list(c.execute(self._sql(q), args).fetchall())
        with self.store.reader() as c:
            return [dict(r) for r in c.execute(q, args).fetchall()]

    @contextlib.contextmanager
    def _write(self) -> Iterator["_Tx"]:
        """One write transaction; commits when the block ends, rolls back if it raises."""
        if self.postgres:
            with self.store.ingest.connection() as c, c.transaction():
                yield _Tx(c, True)
            return
        s = self.store
        with s.lock:
            c = s.conn
            c.execute("BEGIN IMMEDIATE")
            try:
                yield _Tx(c, False)
                c.execute("COMMIT")
            except BaseException:
                c.execute("ROLLBACK")
                raise

    # ---- people -----------------------------------------------------------------------------------

    def create_user(self, email: str, name: str, role: str, password_hash: Optional[str], now_ms: int) -> int:
        """A new person; their ID. Raises AccountError (EmailTaken: the email, ignoring case, has an account)."""
        email, name, role = check_email(email), check_name(name), check_role(role)
        try:
            with self._write() as tx:
                return tx.insert_id(
                    "INSERT INTO users (email, name, role, password_hash, created_at_ms, updated_at_ms) "
                    "VALUES (?, ?, ?, ?, ?, ?)", (email, name, role, password_hash, now_ms, now_ms))
        except self._unique:
            raise EmailTaken(f"{email} already has an account") from None

    def user(self, user_id: int) -> Optional[dict]:
        rows = self._read(_USER_SELECT + " WHERE id = ?", (user_id,))
        return rows[0] if rows else None

    def user_by_email(self, email: str) -> Optional[dict]:
        e = (email or "").strip()
        if not e or len(e) > MAX_EMAIL:
            return None
        rows = self._read(_USER_SELECT + (" WHERE lower(email) = lower(?)" if self.postgres else " WHERE email = ?"),
                          (e,))
        return rows[0] if rows else None

    def users(self) -> list[dict]:
        return self._read(_USER_SELECT + " ORDER BY id")

    def set_password(self, user_id: int, password_hash: str, now_ms: int, *, keep_session: Optional[str] = None) -> bool:
        """A new password hash. Ends the person's sessions, all but ``keep_session`` (the hash of the session
        that changed it), in the same transaction. False: no such person. Also clears a sign-in lock."""
        with self._write() as tx:
            if not tx.run("UPDATE users SET password_hash = ?, updated_at_ms = ?, failed_count = 0, "
                          "locked_until_ms = NULL WHERE id = ?", (password_hash, now_ms, user_id)):
                return False
            tx.run("DELETE FROM sessions WHERE user_id = ? AND id_sha256 <> ?", (user_id, keep_session or ""))
        return True

    def rehash_password(self, user_id: int, old_hash: str, new_hash: str) -> bool:
        """Replace a hash with a stronger one of the same password (passwords.Hasher.needs_rehash), if it is
        still ``old_hash``. Sessions stay: the password is the same."""
        with self._write() as tx:
            return bool(tx.run("UPDATE users SET password_hash = ? WHERE id = ? AND password_hash = ?",
                               (new_hash, user_id, old_hash)))

    def set_role(self, user_id: int, role: str, now_ms: int) -> bool:
        """A new role; ends the person's sessions, so every replica sees it once their session caches expire.
        False: no such person."""
        role = check_role(role)
        with self._write() as tx:
            if not tx.run("UPDATE users SET role = ?, updated_at_ms = ? WHERE id = ?", (role, now_ms, user_id)):
                return False
            tx.run("DELETE FROM sessions WHERE user_id = ?", (user_id,))
        return True

    def set_disabled(self, user_id: int, disabled: bool, now_ms: int) -> bool:
        """Stop (or let again) a person signing in; disabling ends their sessions."""
        with self._write() as tx:
            if not tx.run("UPDATE users SET disabled_at_ms = ?, updated_at_ms = ? WHERE id = ?",
                          (now_ms if disabled else None, now_ms, user_id)):
                return False
            if disabled:
                tx.run("DELETE FROM sessions WHERE user_id = ?", (user_id,))
        return True

    def remove_user(self, user_id: int) -> bool:
        """Remove a person; their sessions, personal tokens and codes go with them (ON DELETE CASCADE)."""
        with self._write() as tx:
            return bool(tx.run("DELETE FROM users WHERE id = ?", (user_id,)))

    # ---- the per-account sign-in limit ------------------------------------------------------------

    def sign_in_failed(self, user_id: int, now_ms: int, lock_after: int, lock_ms: int) -> Optional[int]:
        """Count a failed sign-in. The ``lock_after``-th in a row locks the account until ``now_ms + lock_ms``
        and starts the count again. One statement, so failures on several replicas at once are all counted.
        Returns the lock's end if the account is locked now, else None."""
        with self._write() as tx:
            tx.run("UPDATE users SET "
                   "locked_until_ms = CASE WHEN failed_count + 1 >= ? THEN ? ELSE locked_until_ms END, "
                   "failed_count = CASE WHEN failed_count + 1 >= ? THEN 0 ELSE failed_count + 1 END "
                   "WHERE id = ?", (lock_after, now_ms + lock_ms, lock_after, user_id))
            r = tx.one("SELECT locked_until_ms FROM users WHERE id = ?", (user_id,))
        locked = r and r["locked_until_ms"]
        return locked if locked and locked > now_ms else None

    def sign_in_ok(self, user_id: int, now_ms: int) -> None:
        with self._write() as tx:
            tx.run("UPDATE users SET failed_count = 0, locked_until_ms = NULL, last_sign_in_ms = ? WHERE id = ?",
                   (now_ms, user_id))

    # ---- sessions -----------------------------------------------------------------------------------

    def create_session(self, id_sha256: str, user_id: int, now_ms: int, expires_at_ms: int) -> None:
        with self._write() as tx:
            tx.run("INSERT INTO sessions (id_sha256, user_id, created_at_ms, expires_at_ms, last_seen_ms) "
                   "VALUES (?, ?, ?, ?, ?)", (id_sha256, user_id, now_ms, expires_at_ms, now_ms))
            # ended sessions go when a new one starts: no thread of its own, and the index keeps it cheap
            tx.run("DELETE FROM sessions WHERE expires_at_ms <= ?", (now_ms,))

    def session(self, id_sha256: str, now_ms: int) -> Optional[dict]:
        """The live session with this hash and its person (``user_id``, ``email``, ``name``, ``role``,
        ``expires_at_ms``, ``last_seen_ms``), or None: unknown, expired, or the person is disabled. Read pool."""
        rows = self._read(
            "SELECT s.user_id, s.expires_at_ms, s.last_seen_ms, u.email, u.name, u.role FROM sessions s "
            "JOIN users u ON u.id = s.user_id "
            "WHERE s.id_sha256 = ? AND s.expires_at_ms > ? AND u.disabled_at_ms IS NULL", (id_sha256, now_ms))
        return rows[0] if rows else None

    def touch_session(self, id_sha256: str, now_ms: int) -> None:
        with self._write() as tx:
            tx.run("UPDATE sessions SET last_seen_ms = ? WHERE id_sha256 = ?", (now_ms, id_sha256))

    def end_session(self, id_sha256: str) -> bool:
        with self._write() as tx:
            return bool(tx.run("DELETE FROM sessions WHERE id_sha256 = ?", (id_sha256,)))

    def end_sessions(self, user_id: int) -> int:
        with self._write() as tx:
            return tx.run("DELETE FROM sessions WHERE user_id = ?", (user_id,))


class _Tx:
    def __init__(self, conn, postgres: bool):
        self.c = conn
        self.postgres = postgres

    def _q(self, q: str) -> str:
        return q.replace("?", "%s") if self.postgres else q

    def run(self, q: str, args: tuple = ()) -> int:
        """Rows changed."""
        return self.c.execute(self._q(q), args).rowcount

    def one(self, q: str, args: tuple = ()) -> Optional[Any]:
        r = self.c.execute(self._q(q), args).fetchone()
        return None if r is None else dict(r)

    def insert_id(self, q: str, args: tuple) -> int:
        if self.postgres:
            return self.c.execute(self._q(q) + " RETURNING id", args).fetchone()["id"]
        return self.c.execute(q, args).lastrowid  # no RETURNING: the SQLite of Python 3.10 may predate it
