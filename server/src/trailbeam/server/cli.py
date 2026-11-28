"""``trailbeam users ...`` and ``trailbeam tokens ...``: people and tokens, from the command line (phase 1 of users
and roles; proposal on QV-149, sections 3 and 4).

They work on the server's own store, SQLite or Postgres, with the same ``--db`` / ``--store-url`` (and
``TRAILBEAM_DB`` / ``TRAILBEAM_STORE_URL``) as the server, while it runs or not. In a container:
``docker exec -it <container> trailbeam users create --email ... --role admin``.

- The store must already be at this version's schema: they never upgrade it (the server does, when it starts), and
  they refuse a store at an older or newer version, or a SQLite file that doesn't exist yet, so a mistyped
  ``--db`` doesn't make a new, empty database. ``--db :memory:`` is the server's own and can't be reached.
- Running servers see a change within seconds: a new admin or the first ingest token within 5 s (access.py), a
  revoked token within 30 s, a changed role, a disabled or removed person within 20 s (their sessions are ended in
  the store at once).
- A new token is printed once, alone on stdout, so ``TOKEN=$(trailbeam tokens create --name orders-api)`` works;
  everything else goes to stderr. The store keeps only its hash.
- Passwords come from ``--password-file`` or are asked for twice; they are never taken as an argument, which
  would leave them in the shell history and the process list.
"""
from __future__ import annotations

import argparse
import contextlib
import getpass
import json
import os
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from . import migrations, passwords
from .access import SWITCHED_OFF, switch_on
from .accounts import ROLES, AccountError, Accounts
from .first_admin import read_password_file, FirstAdminError, default_name
from .retention import parse_duration
from .tokens import Tokens

DEFAULT_DB = "trailbeam.sqlite"


class CliError(Exception):
    """Said on stderr, exit status 2."""


def main(argv) -> int:
    p = build_parser()
    a = p.parse_args(argv)
    if not getattr(a, "run", None):
        p.parse_args([a.group, "--help"])
    try:
        if not (getattr(a, "accounts", False) or switch_on(os.environ)):
            sys.stderr.write(f"trailbeam {a.group}: {SWITCHED_OFF}, and pass --accounts here too. Without it, "
                             "the people and tokens this makes are ignored by the server\n")
            return 2
    except ValueError as e:
        sys.stderr.write(f"trailbeam {a.group}: {e}\n")
        return 2
    try:
        with open_accounts(a) as (acc, where):
            return a.run(a, acc, where) or 0
    except CliError as e:
        sys.stderr.write(f"trailbeam {a.group}: {e}\n")
        return 2
    except AccountError as e:
        sys.stderr.write(f"trailbeam {a.group}: {e}\n")
        return 1
    except KeyboardInterrupt:
        sys.stderr.write("\n")
        return 130


# ---- the store ---------------------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--db", default=os.environ.get("TRAILBEAM_DB", "").strip() or None,
                        help="the server's SQLite file (env TRAILBEAM_DB). Default: trailbeam.sqlite here, else the "
                             "one trailbeam start keeps (~/.trailbeam/trailbeam.sqlite)")
    common.add_argument("--store-url", default=os.environ.get("TRAILBEAM_STORE_URL", "").strip() or None,
                        help="the server's Postgres, postgres://... (env TRAILBEAM_STORE_URL), instead of --db")
    common.add_argument("--accounts", action="store_true",
                        help="users and roles are a preview, off unless the server runs with --accounts or "
                             "TRAILBEAM_ACCOUNTS=1; this says you know (env TRAILBEAM_ACCOUNTS=1)")
    p = argparse.ArgumentParser(prog="trailbeam", add_help=False)
    groups = p.add_subparsers(dest="group", required=True)

    u = groups.add_parser("users", help="people who sign in", description="People who sign in to the web screen "
                          "and the API: admins manage people and tokens, members can see everything.")
    us = u.add_subparsers(dest="command")
    c = us.add_parser("create", parents=[common], help="add a person",
                      description="Add a person. The first admin switches the server to accounts: the web screen "
                                  "and the API then need a sign-in, and TRAILBEAM_TOKEN works only for ingest and "
                                  "/metrics.")
    c.add_argument("--email", required=True, help="their email, the name they sign in with (no email is sent)")
    c.add_argument("--name", help="their name as shown (default: the email's part before @)")
    c.add_argument("--role", choices=ROLES, default="member", help="admin or member (default member)")
    c.add_argument("--password-file", help="a file holding their password; default: asked for twice")
    c.add_argument("--proxy", action="store_true", help="they sign in through the proxy (--proxy-auth-header): no "
                   "password is asked for, unless --password-file gives one")
    c.set_defaults(run=users_create)
    ls = us.add_parser("list", parents=[common], help="list people")
    ls.add_argument("--json", action="store_true", help="as JSON")
    ls.set_defaults(run=users_list)
    r = us.add_parser("set-role", parents=[common], help="make a person an admin or a member")
    r.add_argument("user", help="their email or ID")
    r.add_argument("role", choices=ROLES)
    r.set_defaults(run=users_set_role)
    pw = us.add_parser("reset-password", parents=[common], help="give a person a new password (ends their sessions)")
    pw.add_argument("user", help="their email or ID")
    pw.add_argument("--password-file", help="a file holding the new password; default: asked for twice")
    pw.set_defaults(run=users_reset_password)
    for name, run, text in (("disable", users_disable, "stop a person signing in, and end their sessions"),
                            ("enable", users_enable, "let a disabled person sign in again")):
        d = us.add_parser(name, parents=[common], help=text)
        d.add_argument("user", help="their email or ID")
        d.set_defaults(run=run)
    for name, run, text in (
            ("link-proxy", users_link_proxy, "let a person sign in through the proxy (--proxy-auth-header) as well"),
            ("unlink-proxy", users_unlink_proxy, "stop a person signing in through the proxy")):
        d = us.add_parser(name, parents=[common], help=text)
        d.add_argument("user", help="their email or ID")
        d.set_defaults(run=run)
    rm = us.add_parser("remove", parents=[common], help="remove a person, their sessions and personal tokens (someone "
                       "who signs in with single sign-on can come back as a new person: disable them instead)")
    rm.add_argument("user", help="their email or ID")
    rm.add_argument("--yes", action="store_true", help="don't ask first")
    rm.set_defaults(run=users_remove)

    t = groups.add_parser("tokens", help="ingest tokens and personal API tokens",
                          description="Ingest tokens (tbi_...) let a service send spans and nothing else. Personal API "
                                      "tokens (tbp_...) act as their owner on the API and /metrics, for scripts, CI "
                                      "and Prometheus.")
    ts = t.add_subparsers(dest="command")
    c = ts.add_parser("create", parents=[common], help="make a token; it is printed once")
    c.add_argument("--name", required=True, help='what it is for, e.g. "orders-api prod"')
    c.add_argument("--personal", action="store_true", help="a personal API token (needs --user) instead of an "
                                                           "ingest token")
    c.add_argument("--user", help="--personal: the owner's email or ID")
    c.add_argument("--expires", type=_duration, help="--personal: how long it works, e.g. 90d (default: until "
                                                     "revoked)")
    c.add_argument("--yes", action="store_true", help="don't ask before the first ingest token closes open ingest")
    c.set_defaults(run=tokens_create)
    ls = ts.add_parser("list", parents=[common], help="list tokens (never the tokens themselves)")
    ls.add_argument("--personal", action="store_true", help="personal API tokens instead of ingest tokens")
    ls.add_argument("--json", action="store_true", help="as JSON")
    ls.set_defaults(run=tokens_list)
    rv = ts.add_parser("revoke", parents=[common], help="stop a token working (on every replica within 30 s)")
    rv.add_argument("id", type=int, help="its ID, from trailbeam tokens list")
    rv.add_argument("--personal", action="store_true", help="a personal API token")
    rv.set_defaults(run=tokens_revoke)
    return p


def _duration(text: str) -> int:
    """``90d`` -> its length in milliseconds."""
    try:
        seconds = parse_duration(text)
    except ValueError as e:
        raise argparse.ArgumentTypeError(str(e)) from None
    if seconds is None:
        raise argparse.ArgumentTypeError("expected a duration such as 90d; leave --expires out for no end")
    return seconds * 1000


def sqlite_path(db: Optional[str]) -> str:
    """The SQLite file: --db (TRAILBEAM_DB), else trailbeam.sqlite here, else trailbeam start's. CliError if none."""
    if db:
        if db == ":memory:":
            raise CliError("--db :memory: is the server's own memory: nothing else can reach it, and its people and "
                           "tokens are lost when it stops. Use a file, or Postgres")
        if not Path(db).expanduser().is_file():
            raise CliError(f"{db}: no such file. Give the server's --db (or TRAILBEAM_DB); the server creates the "
                           "file when it first starts")
        return str(Path(db).expanduser())
    from .start import DB_FILE, default_data_dir

    for candidate in (Path(DEFAULT_DB), Path(default_data_dir()) / DB_FILE):
        if candidate.is_file():
            return str(candidate)
    raise CliError(f"no {DEFAULT_DB} here or in {default_data_dir()}: give the server's --db, or --store-url")


def check_version(version: Optional[int], latest: int, where: str) -> None:
    from . import __version__

    if version is None:
        raise CliError(f"{where} is not a Trailbeam store yet: start the server on it once")
    if version < latest:
        raise CliError(f"{where} is at schema version {version}; this trailbeam ({__version__}) needs {latest}. "
                       "Upgrade the server and start it once (it upgrades the store), then run this again")
    if version > latest:
        raise CliError(f"{where} is at schema version {version}, written by a newer Trailbeam; this one "
                       f"({__version__}) knows versions up to {latest}. Use the newer trailbeam")


class _OneConnection:
    """Postgres for Accounts (``store.reads`` and ``store.ingest``) on one connection: the CLI does one thing at a
    time, so it needs no pool."""

    def __init__(self, conn):
        self.conn = conn

    @contextlib.contextmanager
    def connection(self):
        yield self.conn


class _PgStore:
    def __init__(self, conn):
        self.reads = self.ingest = _OneConnection(conn)


@contextlib.contextmanager
def open_accounts(a):
    """(Accounts on the store, where it is, for messages); the store checked at this version's schema."""
    if a.store_url:
        from .__main__ import redact_url

        where = redact_url(a.store_url)
        if not a.store_url.startswith(("postgres://", "postgresql://")):
            raise CliError(f"--store-url: expected postgres://... or postgresql://..., got {where!r}")
        try:
            import psycopg
            from psycopg.rows import dict_row, tuple_row

            from . import pg_schema
        except ImportError as e:
            raise CliError(f"--store-url is a Postgres URL, but its driver is not installed ({e}): pip install "
                           "\"trailbeam[postgres]\"") from None
        try:
            conn = psycopg.connect(a.store_url, autocommit=True, row_factory=dict_row, connect_timeout=10)
        except psycopg.Error as e:
            raise CliError(f"{where}: {e}") from None
        try:
            with conn.cursor(row_factory=tuple_row) as cur:  # current_version reads plain tuples
                version = pg_schema.current_version(cur)
            check_version(version, pg_schema.LATEST, where)
            yield Accounts(_PgStore(conn)), where
        except migrations.SchemaError as e:
            raise CliError(f"{where}: {e}") from None
        finally:
            conn.close()
        return
    path = sqlite_path(a.db)
    try:
        ro = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=5)
        try:
            version = migrations.current_version(ro)
        finally:
            ro.close()
    except (sqlite3.Error, migrations.SchemaError) as e:
        raise CliError(f"{path}: {e}") from None
    check_version(version, migrations.LATEST, path)
    from .db import SqliteStore

    store = SqliteStore(path)  # WAL, and sqlite3's 5 s busy timeout while the server writes
    try:
        yield Accounts(store), path
    finally:
        store.close()


# ---- helpers -----------------------------------------------------------------------------------------

def _now_ms() -> int:
    return time.time_ns() // 1_000_000


def _say(text: str) -> None:
    sys.stderr.write(text + "\n")


def _interactive() -> bool:
    try:
        return sys.stdin.isatty() and sys.stderr.isatty()
    except (AttributeError, ValueError):
        return False


def _confirm(question: str, yes: bool, what: str) -> None:
    if yes:
        return
    if not _interactive():
        raise CliError(f"{what}: pass --yes to go ahead without a terminal to ask in")
    if input(f"{question} [y/N] ").strip().lower() not in ("y", "yes"):
        raise CliError("stopped; nothing was changed")


def _password(path: Optional[str]) -> str:
    if path:
        try:
            return passwords.check(read_password_file(path))
        except (FirstAdminError, passwords.PasswordError) as e:
            raise CliError(f"--password-file: {e}") from None
    if not _interactive():
        raise CliError("no terminal to ask for the password in: use --password-file")
    pw = getpass.getpass(f"Password (at least {passwords.MIN_LENGTH} characters): ")
    try:
        passwords.check(pw)
    except passwords.PasswordError as e:
        raise CliError(str(e)) from None
    if getpass.getpass("Password again: ") != pw:
        raise CliError("the two passwords are not the same; nothing was changed")
    return pw


def _user(acc: Accounts, ref: str) -> dict:
    u = acc.user(int(ref)) if ref.strip().isdigit() else acc.user_by_email(ref)
    if u is None:
        raise CliError(f"no person {ref!r}: see trailbeam users list")
    return u


def _when(ms: Optional[int]) -> str:
    if not ms:
        return "-"
    return datetime.fromtimestamp(ms / 1000, timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def _table(rows: list[list[str]], head: list[str]) -> None:
    widths = [max(len(str(x)) for x in col) for col in zip(head, *rows)]
    for r in [head] + rows:
        sys.stdout.write("  ".join(str(x).ljust(w) for x, w in zip(r, widths)).rstrip() + "\n")


SEEN_SOON = "Running servers see it within 20 seconds."


# ---- users -------------------------------------------------------------------------------------------

def users_create(a, acc: Accounts, where: str) -> int:
    first = a.role == "admin" and not acc.auth_state()[0]
    if first and acc.postgres:
        _say("Upgrade every replica to this version before creating the first admin: a replica on an older version "
             "keeps letting TRAILBEAM_TOKEN into the web screen and the API.")
    if not acc.auth_state()[0] and a.role != "admin":
        _say("Note: this server has no admin yet. Until it has one, accounts aren't used: use --role admin for the "
             "first account.")
    proxy = getattr(a, "proxy", False)
    h = None if proxy and not a.password_file else passwords.Hasher().hash(_password(a.password_file))
    name = a.name or default_name(a.email)
    if first:
        uid = acc.create_first_admin(a.email, name, h, _now_ms(), proxy=proxy)
    else:
        uid = acc.create_user(a.email, name, a.role, h, _now_ms(), proxy=proxy)
    _say(f"Created {a.role} {a.email} (ID {uid}) in {where}."
         + (" They sign in through the proxy" + (" or with their password." if h else "; they have no password.")
            if proxy else ""))
    if first:
        _say("This is the first admin: within 5 seconds, every server on this store asks for a sign-in on the web "
             "screen and the API (or a personal API token: trailbeam tokens create --personal). TRAILBEAM_TOKEN, if "
             "set, keeps working for ingest and /metrics only, and sign-ins made with it end.")
    return 0


def users_list(a, acc: Accounts, where: str) -> int:
    users = acc.users()
    now = _now_ms()
    out = [{"id": u["id"], "email": u["email"], "name": u["name"], "role": u["role"],
            "status": "disabled" if u["disabled_at_ms"] is not None else
            "locked" if (u["locked_until_ms"] or 0) > now else
            "no password" if not u["password_hash"] and u["proxy_linked_at_ms"] is None else "active",
            "created_at_ms": u["created_at_ms"], "last_sign_in_ms": u["last_sign_in_ms"],
            "proxy": u["proxy_linked_at_ms"] is not None} for u in users]
    if a.json:
        json.dump(out, sys.stdout, indent=2)
        sys.stdout.write("\n")
        return 0
    if not out:
        _say(f"No people in {where} yet: trailbeam users create --email <email> --role admin")
        return 0
    _table([[u["id"], u["email"], u["name"], u["role"], u["status"], "yes" if u["proxy"] else "-",
             _when(u["last_sign_in_ms"])] for u in out],
           ["ID", "EMAIL", "NAME", "ROLE", "STATUS", "PROXY", "LAST SIGN-IN"])
    return 0


def users_set_role(a, acc: Accounts, where: str) -> int:
    u = _user(acc, a.user)
    if u["role"] == a.role:
        _say(f"{u['email']} is already {a.role}.")
        return 0
    acc.set_role(u["id"], a.role, _now_ms())
    _say(f"{u['email']} is now {a.role}; their sessions have ended, so they sign in again. {SEEN_SOON}")
    return 0


def users_reset_password(a, acc: Accounts, where: str) -> int:
    u = _user(acc, a.user)
    pw = _password(a.password_file)
    acc.set_password(u["id"], passwords.Hasher().hash(pw), _now_ms(), remove_passkeys=True)
    _say(f"New password set for {u['email']}; their sessions have ended, their passkeys are removed, and a sign-in "
         f"lock is cleared. {SEEN_SOON}")
    return 0


def users_disable(a, acc: Accounts, where: str) -> int:
    u = _user(acc, a.user)
    acc.set_disabled(u["id"], True, _now_ms())
    _say(f"{u['email']} can no longer sign in, and their sessions and personal tokens stop working. {SEEN_SOON}")
    return 0


def users_enable(a, acc: Accounts, where: str) -> int:
    u = _user(acc, a.user)
    acc.set_disabled(u["id"], False, _now_ms())
    _say(f"{u['email']} can sign in again.")
    return 0


def users_link_proxy(a, acc: Accounts, where: str) -> int:
    u = _user(acc, a.user)
    if u["proxy_linked_at_ms"] is not None:
        _say(f"{u['email']} can already sign in through the proxy.")
        return 0
    acc.set_proxy_linked(u["id"], True, _now_ms())
    _say(f"{u['email']} can now sign in through the proxy too, as {u['role']}: whoever the proxy signs in with this "
         f"email is them. {SEEN_SOON}")
    return 0


def users_unlink_proxy(a, acc: Accounts, where: str) -> int:
    u = _user(acc, a.user)
    if u["proxy_linked_at_ms"] is None:
        _say(f"{u['email']} doesn't sign in through the proxy.")
        return 0
    acc.set_proxy_linked(u["id"], False, _now_ms())
    tail = "" if u["password_hash"] else (" They have no password, so they can't sign in at all now: "
                                          "trailbeam users reset-password gives them one.")
    _say(f"{u['email']} can no longer sign in through the proxy.{tail} {SEEN_SOON}")
    return 0


SSO_COMES_BACK = ("signs in with single sign-on: removing them doesn't keep them out. At their next sign-in the "
                  "identity provider lets them in again, as a new person with the default role. To keep them out, "
                  "disable them instead (trailbeam users disable)")


def users_remove(a, acc: Accounts, where: str) -> int:
    u = _user(acc, a.user)
    sso = bool(acc.oidc_identities(u["id"]))
    if sso:
        _say(f"Note: {u['email']} {SSO_COMES_BACK}.")
    _confirm(f"Remove {u['email']}, with their sessions and personal API tokens?", a.yes, "remove")
    acc.remove_user(u["id"])
    _say(f"Removed {u['email']}. {SEEN_SOON}")
    return 0


# ---- tokens ------------------------------------------------------------------------------------------

def _shared_token_set() -> bool:
    return bool(os.environ.get("TRAILBEAM_TOKEN", "").strip() or os.environ.get("TRAILBEAM_TOKEN_FILE", "").strip())


def tokens_create(a, acc: Accounts, where: str) -> int:
    t = Tokens(acc)
    if a.personal:
        if not a.user:
            raise CliError("--personal needs --user <email or ID>: the token acts as that person, with their role")
        u = _user(acc, a.user)
        expires = _now_ms() + a.expires if a.expires else None
        tid, token = t.create_personal(u["id"], a.name, expires)
        _say(f"Personal API token {tid} for {u['email']} ({u['role']}), "
             + (f"working until {_when(expires)}." if expires else "working until it is revoked.")
             + " Send it as Authorization: Bearer <token>. It is shown only now:")
    else:
        if a.user or a.expires:
            raise CliError("--user and --expires are for --personal tokens")
        if not acc.auth_state()[1] and not _shared_token_set():
            _say("This is the store's first ingest token. If the server has no TRAILBEAM_TOKEN, ingest is open now, "
                 "and within 5 seconds of this it needs a token: services that send without one get 401, and their "
                 "spans are lost.")
            _confirm("Create it?", a.yes, "the first ingest token closes open ingest")
        tid, token = t.create_ingest(a.name)
        _say(f"Ingest token {tid} ({a.name}). Give it to the service as x-trailbeam-key: <token> (or "
             "Authorization: Bearer <token>). It can only send spans. It is shown only now:")
    sys.stdout.write(token + "\n")
    return 0


def tokens_list(a, acc: Accounts, where: str) -> int:
    if a.personal:
        emails = {u["id"]: u["email"] for u in acc.users()}
        rows = [{**r, "owner": emails.get(r["user_id"])} for r in acc.personal_tokens()]
    else:
        rows = acc.ingest_tokens()
    now = _now_ms()
    for r in rows:
        r["status"] = ("revoked" if r["revoked_at_ms"] else
                       "expired" if (r.get("expires_at_ms") or now + 1) <= now else "active")
    if a.json:
        json.dump(rows, sys.stdout, indent=2)
        sys.stdout.write("\n")
        return 0
    if not a.personal and _shared_token_set():
        sys.stdout.write("legacy shared token (from TRAILBEAM_TOKEN): ingest and /metrics"
                         + (" (and, until an admin exists, the API and the web screen)\n"
                            if not acc.auth_state()[0] else "\n"))
    if not rows:
        _say("No " + ("personal API" if a.personal else "ingest") + " tokens yet: trailbeam tokens create --name <name>"
             + (" --personal --user <email>" if a.personal else ""))
        return 0
    if a.personal:
        _table([[r["id"], r["owner"], r["name"], r["prefix"] + "...", _when(r["created_at_ms"]),
                 _when(r["last_used_ms"]), _when(r["expires_at_ms"]) if r["expires_at_ms"] else "-", r["status"]]
                for r in rows], ["ID", "OWNER", "NAME", "TOKEN", "CREATED", "LAST USED", "EXPIRES", "STATUS"])
    else:
        _table([[r["id"], r["name"], r["prefix"] + "...", _when(r["created_at_ms"]), _when(r["last_used_ms"]),
                 r["status"]] for r in rows], ["ID", "NAME", "TOKEN", "CREATED", "LAST USED", "STATUS"])
    return 0


def tokens_revoke(a, acc: Accounts, where: str) -> int:
    t = Tokens(acc)
    kind = "personal API token" if a.personal else "ingest token"
    if not (t.revoke_personal(a.id) if a.personal else t.revoke_ingest(a.id)):
        raise CliError(f"no {kind} {a.id} that is still working: see trailbeam tokens list"
                       + (" --personal" if a.personal else ""))
    _say(f"Revoked {kind} {a.id}. Running servers refuse it within 30 seconds"
         + ("." if a.personal else "; the other ingest tokens keep working."))
    return 0
