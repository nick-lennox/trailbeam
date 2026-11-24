"""The first admin: the one-time setup link of ``trailbeam start``, and ``TRAILBEAM_ADMIN_EMAIL`` for headless installs.

**The setup link** (proposal on QV-149, section 3). While the store has no admin, ``trailbeam start`` makes a setup
code and prints ``http://<host>:<port>/setup#code=<code>`` in its terminal (never in the logs). Opening it shows
a form for an email, a name and a password; that account becomes the first admin and is signed in.

- The code is ``CODE_BYTES`` (32) random bytes; the store keeps only its SHA-256 (``setup_codes``).
- It works once, for ``LIFETIME_MS`` (1 hour), and each new code replaces the one before, so only the link the
  latest ``trailbeam start`` printed works.
- It is in the URL fragment (after ``#``), which browsers don't send: the page's script moves it into the form, so
  it never reaches the server's or a proxy's request log, and ``Referrer-Policy: no-referrer`` keeps it from
  other sites. The form posts it, and the code is used up and the admin created in one transaction
  (accounts.create_first_admin, under an advisory lock on Postgres), so of two forms sent at once one wins.
- Until it is used it is as good as an admin password, which is why it is short-lived and printed only where the
  person who ran ``trailbeam start`` sees it.

**Headless installs** (Docker, Compose, Kubernetes) get no link, because container logs are often readable by many
people. Either run ``trailbeam users create --email ... --role admin`` in the container, or set
``TRAILBEAM_ADMIN_EMAIL`` and ``TRAILBEAM_ADMIN_PASSWORD_FILE`` (and optionally ``TRAILBEAM_ADMIN_NAME``): read at
start, and only while the store has no admin, so changing them later changes nothing. The same lock as the setup
link means replicas starting together create one admin.
"""
from __future__ import annotations

import hashlib
import logging
import os
import secrets
from typing import Mapping, Optional

from . import passwords
from .accounts import AccountError, AdminExists, check_email

CODE_BYTES = 32
LIFETIME_MS = 3_600_000
PATH = "/setup"
EMAIL_ENV = "TRAILBEAM_ADMIN_EMAIL"
PASSWORD_FILE_ENV = "TRAILBEAM_ADMIN_PASSWORD_FILE"
NAME_ENV = "TRAILBEAM_ADMIN_NAME"
NO_ADMIN = ("this server has no admin account yet: create one with trailbeam users create --email <email> --role "
            "admin (in the container: docker exec -it <container> trailbeam users create ...), or set "
            f"{EMAIL_ENV} and {PASSWORD_FILE_ENV}")

log = logging.getLogger("trailbeam.server")


class FirstAdminError(ValueError):
    """TRAILBEAM_ADMIN_EMAIL and its password file can't be used as given: the server doesn't start."""


def new_code() -> str:
    return secrets.token_urlsafe(CODE_BYTES)


def code_hash(code: str) -> str:
    return hashlib.sha256(code.encode("utf-8", "surrogatepass")).hexdigest()


def issue_code(accounts, now_ms: int) -> str:
    """A new setup code, in date for LIFETIME_MS; the one before stops working."""
    code = new_code()
    accounts.create_setup_code(code_hash(code), now_ms, now_ms + LIFETIME_MS)
    return code


def link(base_url: str, code: str) -> str:
    return f"{base_url.rstrip('/')}{PATH}#code={code}"


def well_formed(code: Optional[str]) -> bool:
    return bool(code) and 20 <= len(code) <= 100 and all(c.isalnum() or c in "-_" for c in code)


def default_name(email: str) -> str:
    return email.split("@", 1)[0] or email


def read_password_file(path: str) -> str:
    """The password in a file, without the line break an editor or ``echo`` puts at its end."""
    try:
        with open(path, encoding="utf-8") as f:
            text = f.read(passwords.MAX_BYTES + 3)
    except OSError as e:
        raise FirstAdminError(f"can't read the password file {path}: {e.strerror or e}") from None
    except UnicodeDecodeError:
        raise FirstAdminError(f"the password file {path} is not UTF-8 text") from None
    if text.endswith("\n"):
        text = text[:-1]
    if text.endswith("\r"):
        text = text[:-1]
    return text


def from_env(accounts, hasher: passwords.Hasher, now_ms: int, env: Mapping[str, str] = os.environ) -> Optional[int]:
    """Create the first admin from TRAILBEAM_ADMIN_EMAIL and TRAILBEAM_ADMIN_PASSWORD_FILE when both are set and the
    store has no admin; their ID, or None (not set, or an admin exists). Raises FirstAdminError."""
    email = (env.get(EMAIL_ENV) or "").strip()
    path = (env.get(PASSWORD_FILE_ENV) or "").strip()
    if not email and not path:
        return None
    if not email or not path:
        raise FirstAdminError(f"{EMAIL_ENV} and {PASSWORD_FILE_ENV} go together: set both, or neither")
    if accounts is None:
        raise FirstAdminError(f"{EMAIL_ENV}: this store has no account tables")
    if accounts.auth_state()[0]:
        log.info("%s is set, but this server has an admin already: it is ignored", EMAIL_ENV)
        return None
    try:
        check_email(email)
    except AccountError as e:
        raise FirstAdminError(f"{EMAIL_ENV}: {e}") from None
    try:
        password = passwords.check(read_password_file(path))
    except passwords.PasswordError as e:
        raise FirstAdminError(f"{PASSWORD_FILE_ENV}: {e}") from None
    name = (env.get(NAME_ENV) or "").strip() or default_name(email)
    try:
        uid = accounts.create_first_admin(email, name, hasher.hash(password), now_ms)
    except AdminExists:  # another replica made it first
        log.info("%s: another replica created the first admin first", EMAIL_ENV)
        return None
    except AccountError as e:
        raise FirstAdminError(f"{EMAIL_ENV}: {e}") from None
    log.info("created the first admin, %s, from %s (user %d)", email, EMAIL_ENV, uid)
    return uid
