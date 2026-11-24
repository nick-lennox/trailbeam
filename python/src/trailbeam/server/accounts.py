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


class LastAdmin(AccountError):
    """The change would leave no admin who can sign in: the server always keeps one (proposal section 2)."""


class AdminExists(AccountError):
    """The first admin was asked for (a setup link, TRAILBEAM_ADMIN_EMAIL), but an admin exists already."""


class BadSetupCode(AccountError):
    """The setup code is unknown, used, or out of date."""


class SsoRefused(AccountError):
    """A single sign-on that the rules refuse (oidc_sign_in); ``reason`` says which rule, the message says why."""

    def __init__(self, reason: str, message: str):
        super().__init__(message)
        self.reason = reason


SETUP = "setup"  # setup_codes.kind of the first admin's link (trailbeam start)
# pg_advisory_xact_lock key, taken by every write that can add or take away an admin, so two replicas (or a CLI and
# a replica) can't both create "the first admin", or each take away one of the last two
ADMIN_LOCK = 0x7472_6169_6c62_6d02
_LAST_ADMIN = "the server must keep at least one admin who can sign in: make someone else an admin first"


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
        # True for a SQLite :memory: store: only this process can change it (no replica or CLI reaches it), and
        # its reads wait for the writer, so access.State reads auth_state again only when ``changes`` moves
        self.only_this_process = not self.postgres and not getattr(store, "shared_reads", True)
        self.changes = 0  # people and ingest tokens created, removed or given a new role, by this process
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
    def _write(self, admins: bool = False) -> Iterator["_Tx"]:
        """One write transaction; commits when the block ends, rolls back if it raises. ``admins``: it can add or
        take away an admin, so on Postgres it first takes ADMIN_LOCK (SQLite's BEGIN IMMEDIATE already lets one
        writer in at a time)."""
        if self.postgres:
            with self.store.ingest.connection() as c, c.transaction():
                if admins:
                    c.execute("SELECT pg_advisory_xact_lock(%s)", (ADMIN_LOCK,))
                yield _Tx(c, True)
            return
        s = self.store
        with s.lock:
            # every write; under the lock, so a :memory: store's reads (which take it too) see the change
            self.changes += 1
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
        with self._write(admins=True) as tx:
            if role != "admin":
                _keep_an_admin(tx, user_id)
            if not tx.run("UPDATE users SET role = ?, updated_at_ms = ? WHERE id = ?", (role, now_ms, user_id)):
                return False
            tx.run("DELETE FROM sessions WHERE user_id = ?", (user_id,))
        return True

    def set_disabled(self, user_id: int, disabled: bool, now_ms: int) -> bool:
        """Stop (or let again) a person signing in; disabling ends their sessions. Raises LastAdmin for the last
        admin who can sign in."""
        with self._write(admins=disabled) as tx:
            if disabled:
                _keep_an_admin(tx, user_id)
            if not tx.run("UPDATE users SET disabled_at_ms = ?, updated_at_ms = ? WHERE id = ?",
                          (now_ms if disabled else None, now_ms, user_id)):
                return False
            if disabled:
                tx.run("DELETE FROM sessions WHERE user_id = ?", (user_id,))
        return True

    def remove_user(self, user_id: int) -> bool:
        """Remove a person; their sessions, personal tokens and codes go with them (ON DELETE CASCADE). Raises
        LastAdmin for the last admin who can sign in."""
        with self._write(admins=True) as tx:
            _keep_an_admin(tx, user_id)
            return bool(tx.run("DELETE FROM users WHERE id = ?", (user_id,)))

    def create_first_admin(self, email: str, name: str, password_hash: str, now_ms: int, *,
                           setup_code_sha256: Optional[str] = None) -> int:
        """The first admin: their ID. One transaction, under ADMIN_LOCK on Postgres, so of two at once (two replicas
        reading TRAILBEAM_ADMIN_EMAIL, or a setup link opened twice) one wins. With ``setup_code_sha256``, the
        setup code is used up in the same transaction. Raises AdminExists, BadSetupCode, EmailTaken, AccountError."""
        email, name = check_email(email), check_name(name)
        try:
            with self._write(admins=True) as tx:
                if tx.one("SELECT id FROM users WHERE role = 'admin' LIMIT 1") is not None:
                    raise AdminExists("this server has an admin already")
                if setup_code_sha256 is not None and not tx.run(
                        "UPDATE setup_codes SET used_at_ms = ? WHERE code_sha256 = ? AND kind = ? "
                        "AND used_at_ms IS NULL AND expires_at_ms > ?", (now_ms, setup_code_sha256, SETUP, now_ms)):
                    raise BadSetupCode("this setup link has been used or is out of date")
                return tx.insert_id(
                    "INSERT INTO users (email, name, role, password_hash, created_at_ms, updated_at_ms) "
                    "VALUES (?, ?, 'admin', ?, ?, ?)", (email, name, password_hash, now_ms, now_ms))
        except self._unique:
            raise EmailTaken(f"{email} already has an account") from None

    # ---- setup codes ------------------------------------------------------------------------------

    def create_setup_code(self, code_sha256: str, now_ms: int, expires_at_ms: int) -> None:
        """The first admin's setup code. It replaces every earlier one, so only the newest link works."""
        with self._write() as tx:
            tx.run("DELETE FROM setup_codes WHERE kind = ?", (SETUP,))
            tx.run("INSERT INTO setup_codes (code_sha256, kind, created_at_ms, expires_at_ms) VALUES (?, ?, ?, ?)",
                   (code_sha256, SETUP, now_ms, expires_at_ms))

    def setup_code_ok(self, code_sha256: str, now_ms: int) -> bool:
        """A setup code that can still be used (unused, in date). Read pool; create_first_admin checks again."""
        return bool(self._read("SELECT 1 AS ok FROM setup_codes WHERE code_sha256 = ? AND kind = ? "
                               "AND used_at_ms IS NULL AND expires_at_ms > ?", (code_sha256, SETUP, now_ms)))

    # ---- single sign-on (oidc.py) ---------------------------------------------------------------------

    def create_oidc_login(self, state_sha256: str, browser_sha256: str, nonce: str, code_verifier: str,
                          next_path: str, now_ms: int, expires_at_ms: int) -> None:
        """A sign-in sent to the identity provider. Sign-ins that ran out go at the same time: no thread of its own."""
        with self._write() as tx:
            tx.run("DELETE FROM oidc_logins WHERE expires_at_ms <= ?", (now_ms,))
            tx.run("INSERT INTO oidc_logins (state_sha256, browser_sha256, nonce, code_verifier, next_path, "
                   "created_at_ms, expires_at_ms) VALUES (?, ?, ?, ?, ?, ?, ?)",
                   (state_sha256, browser_sha256, nonce, code_verifier, next_path, now_ms, expires_at_ms))

    def take_oidc_login(self, state_sha256: str, now_ms: int) -> Optional[dict]:
        """The sign-in with this state, deleted as it is read, so it works once even when two replicas get the same
        callback; None when it is unknown, used or out of date. ``browser_sha256``, ``nonce``, ``code_verifier``,
        ``next_path``."""
        with self._write() as tx:
            row = tx.one("SELECT browser_sha256, nonce, code_verifier, next_path, expires_at_ms FROM oidc_logins "
                         "WHERE state_sha256 = ?", (state_sha256,))
            if row is None or not tx.run("DELETE FROM oidc_logins WHERE state_sha256 = ?", (state_sha256,)):
                return None
        return row if row["expires_at_ms"] > now_ms else None

    def oidc_identities(self, user_id: Optional[int] = None) -> list[dict]:
        q = "SELECT issuer, subject, user_id, email, created_at_ms, last_sign_in_ms FROM oidc_identities"
        if user_id is None:
            return self._read(q + " ORDER BY user_id, issuer, subject")
        return self._read(q + " WHERE user_id = ? ORDER BY issuer, subject", (user_id,))

    def oidc_sign_in(self, issuer: str, subject: str, email: str, name: str, role: Optional[str], now_ms: int, *,
                     role_from_provider: bool, link_email: bool, email_verified: bool,
                     allow_unverified_email: bool = False) -> tuple[dict, str]:
        """The person an identity provider's account signs in as: ``(user, how)``, ``how`` being ``known``, ``linked``
        (an existing local account with the same email, ``link_email`` on and the email verified) or ``created``. One
        transaction under ADMIN_LOCK, since it can create, promote or demote an admin.

        The email is the account's only tie to a person before its first sign-in, so it must be one the provider
        verified: an account is created from an unverified email only with ``allow_unverified_email``, and never
        linked from one. Linking is only to a local account (a password, or invited from the People page) that no
        provider account signs in to yet: else a second provider account claiming the same email would share it.

        ``role``: the role the provider's groups give (oidc.Rules.role), None for none. With ``role_from_provider``
        (groups are mapped to roles), a known person's role is set to it at every sign-in, except that the last admin
        who can sign in stays one, and None refuses them. Without it, ``role`` is used only for a new person.

        Raises SsoRefused: ``disabled``, ``no_role``, ``no_admin_yet`` (a new person who isn't an admin, while the
        server has none: their session would let them in nowhere), ``email_unverified`` (a new account, from an email
        the provider didn't verify), ``email_taken`` (an account has the email, and linking is off, the email isn't
        verified, or a provider account already signs in to it)."""
        email, name = check_email(email), check_name(name)
        if role is not None:
            check_role(role)
        with self._write(admins=True) as tx:
            known = tx.one("SELECT user_id FROM oidc_identities WHERE issuer = ? AND subject = ?", (issuer, subject))
            how = "known"
            if known is None:
                other = tx.one("SELECT id FROM users WHERE " + ("lower(email) = lower(?)" if self.postgres
                                                                else "email = ?"), (email,))
                if other is not None:
                    if not (link_email and email_verified) or tx.one(
                            "SELECT 1 FROM oidc_identities WHERE user_id = ? LIMIT 1", (other["id"],)) is not None:
                        raise SsoRefused("email_taken", f"{email} already has an account on this server, which "
                                         "single sign-on doesn't take over")
                    uid, how = int(other["id"]), "linked"
                else:
                    if not (email_verified or allow_unverified_email):
                        raise SsoRefused("email_unverified", f"the identity provider hasn't verified {email}, and "
                                         "--oidc-allow-unverified-email is off")
                    if role is None:
                        raise SsoRefused("no_role", "your groups at the identity provider give you no role here")
                    if role != "admin" and tx.one("SELECT id FROM users WHERE role = 'admin' LIMIT 1") is None:
                        raise SsoRefused("no_admin_yet", "this server has no admin yet, so only someone in an admin "
                                         "group can sign in first")
                    try:
                        uid = tx.insert_id(
                            "INSERT INTO users (email, name, role, password_hash, created_at_ms, updated_at_ms) "
                            "VALUES (?, ?, ?, NULL, ?, ?)", (email, name, role, now_ms, now_ms))
                    except self._unique:  # created a moment ago by another replica: the same refusal as above
                        raise SsoRefused("email_taken", f"{email} already has an account on this server") from None
                    how = "created"
                tx.run("INSERT INTO oidc_identities (issuer, subject, user_id, email, created_at_ms) "
                       "VALUES (?, ?, ?, ?, ?)", (issuer, subject, uid, email, now_ms))
            else:
                uid = int(known["user_id"])
            user = tx.one(_USER_SELECT + " WHERE id = ?", (uid,))
            if user["disabled_at_ms"] is not None:
                raise SsoRefused("disabled", "your account on this server is disabled")
            if how != "created" and role_from_provider and role != user["role"]:
                if role is None:
                    raise SsoRefused("no_role", "your groups at the identity provider give you no role here")
                try:
                    if role != "admin":
                        _keep_an_admin(tx, uid)
                    tx.run("UPDATE users SET role = ?, updated_at_ms = ? WHERE id = ?", (role, now_ms, uid))
                    tx.run("DELETE FROM sessions WHERE user_id = ?", (uid,))
                    user["role"] = role
                except LastAdmin:
                    pass  # the last admin stays one (the caller logs it): the server always keeps an admin
            tx.run("UPDATE users SET last_sign_in_ms = ?, failed_count = 0, locked_until_ms = NULL WHERE id = ?",
                   (now_ms, uid))
            tx.run("UPDATE oidc_identities SET last_sign_in_ms = ?, email = ? WHERE issuer = ? AND subject = ?",
                   (now_ms, email, issuer, subject))
        return user, how

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

    # ---- the live switch (access.py) ------------------------------------------------------------------

    def auth_state(self) -> tuple[bool, bool]:
        """(an admin exists, an ingest token exists). Any admin counts, disabled or not, and any ingest token,
        revoked or not: disabling someone or revoking a token never opens what an admin or a token closed."""
        r = self._read("SELECT EXISTS (SELECT 1 FROM users WHERE role = 'admin') AS admin, "
                       "EXISTS (SELECT 1 FROM ingest_tokens) AS ingest")[0]
        return bool(r["admin"]), bool(r["ingest"])

    # ---- ingest tokens (tokens.py makes them; only hashes come here) ---------------------------------

    def create_ingest_token(self, name: str, token_sha256: str, prefix: str, now_ms: int,
                            created_by: Optional[int] = None) -> int:
        name = check_name(name)
        with self._write() as tx:
            return tx.insert_id("INSERT INTO ingest_tokens (name, token_sha256, prefix, created_at_ms, created_by) "
                                "VALUES (?, ?, ?, ?, ?)", (name, token_sha256, prefix, now_ms, created_by))

    def ingest_tokens(self) -> list[dict]:
        return self._read("SELECT id, name, prefix, created_at_ms, created_by, last_used_ms, revoked_at_ms "
                          "FROM ingest_tokens ORDER BY id")

    def ingest_token(self, token_sha256: str) -> Optional[dict]:
        """The live (not revoked) ingest token with this hash: ``id``, ``name``. Read pool."""
        rows = self._read("SELECT id, name FROM ingest_tokens WHERE token_sha256 = ? AND revoked_at_ms IS NULL",
                          (token_sha256,))
        return rows[0] if rows else None

    def revoke_ingest_token(self, token_id: int, now_ms: int) -> bool:
        """False: no such token, or already revoked. The row stays, so lists show it."""
        with self._write() as tx:
            return bool(tx.run("UPDATE ingest_tokens SET revoked_at_ms = ? WHERE id = ? AND revoked_at_ms IS NULL",
                               (now_ms, token_id)))

    def touch_ingest_token(self, token_id: int, now_ms: int) -> None:
        with self._write() as tx:
            tx.run("UPDATE ingest_tokens SET last_used_ms = ? WHERE id = ?", (now_ms, token_id))

    # ---- personal API tokens ------------------------------------------------------------------------

    def create_personal_token(self, user_id: int, name: str, token_sha256: str, prefix: str, now_ms: int,
                              expires_at_ms: Optional[int] = None) -> int:
        """Raises AccountError when there is no such person."""
        name = check_name(name)
        with self._write() as tx:
            if tx.one("SELECT id FROM users WHERE id = ?", (user_id,)) is None:
                raise AccountError(f"no person with ID {user_id}")
            return tx.insert_id("INSERT INTO personal_tokens (user_id, name, token_sha256, prefix, created_at_ms, "
                                "expires_at_ms) VALUES (?, ?, ?, ?, ?, ?)",
                                (user_id, name, token_sha256, prefix, now_ms, expires_at_ms))

    def personal_tokens(self, user_id: Optional[int] = None) -> list[dict]:
        q = ("SELECT id, user_id, name, prefix, created_at_ms, expires_at_ms, last_used_ms, revoked_at_ms "
             "FROM personal_tokens")
        if user_id is None:
            return self._read(q + " ORDER BY id")
        return self._read(q + " WHERE user_id = ? ORDER BY id", (user_id,))

    def personal_token(self, token_sha256: str, now_ms: int) -> Optional[dict]:
        """The live personal token with this hash and its owner (``id``, ``user_id``, ``email``, ``name``,
        ``role``), or None: unknown, revoked, expired, or the owner is disabled. Read pool."""
        rows = self._read(
            "SELECT t.id, t.user_id, u.email, u.name, u.role FROM personal_tokens t JOIN users u ON u.id = t.user_id "
            "WHERE t.token_sha256 = ? AND t.revoked_at_ms IS NULL AND (t.expires_at_ms IS NULL OR t.expires_at_ms > ?) "
            "AND u.disabled_at_ms IS NULL", (token_sha256, now_ms))
        return rows[0] if rows else None

    def revoke_personal_token(self, token_id: int, now_ms: int, user_id: Optional[int] = None) -> bool:
        """False: no such token (of ``user_id``, if given), or already revoked."""
        q = "UPDATE personal_tokens SET revoked_at_ms = ? WHERE id = ? AND revoked_at_ms IS NULL"
        args: tuple = (now_ms, token_id)
        if user_id is not None:
            q, args = q + " AND user_id = ?", args + (user_id,)
        with self._write() as tx:
            return bool(tx.run(q, args))

    def touch_personal_token(self, token_id: int, now_ms: int) -> None:
        with self._write() as tx:
            tx.run("UPDATE personal_tokens SET last_used_ms = ? WHERE id = ?", (now_ms, token_id))


def _keep_an_admin(tx: "_Tx", user_id: int) -> None:
    """Raise LastAdmin if ``user_id`` is an admin who can sign in and nobody else is."""
    me = tx.one("SELECT role, disabled_at_ms FROM users WHERE id = ?", (user_id,))
    if me is None or me["role"] != "admin" or me["disabled_at_ms"] is not None:
        return
    if tx.one("SELECT id FROM users WHERE role = 'admin' AND disabled_at_ms IS NULL AND id <> ? LIMIT 1",
              (user_id,)) is None:
        raise LastAdmin(_LAST_ADMIN)


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
