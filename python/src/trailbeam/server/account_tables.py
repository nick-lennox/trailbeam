"""The tables of people and tokens (users and roles, phase 1; proposal on QV-149, section 7): SQLite schema 15,
Postgres schema 8. The two single sign-on tables (phase 3b, oidc.py) came with SQLite schema 19, Postgres schema 13.

- ``users``: a person. ``email`` is the sign-in name, unique ignoring case (``COLLATE NOCASE`` in SQLite, a
  unique index on ``lower(email)`` in Postgres). ``role`` is ``admin`` or ``member``; it has no CHECK, so a
  later role doesn't need SQLite to copy the table, and the code that writes users checks it.
  ``password_hash`` holds the scrypt parameters with the hash (``scrypt$15$8$3$<salt>$<hash>``); NULL: no
  password set yet. ``failed_count`` and ``locked_until_ms`` are the per-account sign-in limit, kept here so it is
  exact across replicas. ``disabled_at_ms``: set when the person can no longer sign in.
- ``sessions``: one per signed-in browser. ``id_sha256`` is the SHA-256 (hex) of the cookie's 32 random bytes;
  the ID itself is never stored. The expiry is fixed when the session starts and never extended;
  ``last_seen_ms`` is written at most once an hour, so page views don't compete with ingest for the writer.
- ``ingest_tokens``: named service tokens (``tbi_…``) that can only send spans. ``token_sha256`` is the
  SHA-256 (hex) of the whole token; ``prefix`` its first 8 characters, for display. Revoked tokens keep their
  row (``revoked_at_ms``), so the list can show them.
- ``personal_tokens``: API tokens (``tbp_…``) that act as their owner, with the owner's role. Removed with the
  owner.
- ``users.proxy_linked_at_ms`` (SQLite schema 18, Postgres schema 12; QV-227): set when the person may sign in
  through the proxy (proxy_auth.py): the proxy created the account, or an admin linked it
  (``trailbeam users link-proxy``). NULL: a proxy that names this email is refused, so a local account can't be
  taken over by whoever the proxy lets in with the same email.
- ``setup_codes``: one-time codes. ``kind`` is ``setup`` (the first admin, from ``trailbeam start``); phase 2 adds
  invite and reset links, which name a ``user_id``. ``code_sha256`` as for tokens; ``used_at_ms`` is set in the
  same transaction that acts on the code, so a code works once even with several replicas.
- ``oidc_logins``: OpenID Connect sign-ins in progress, for at most 10 minutes (oidc.LOGIN_TTL_MS). The row is
  found by the SHA-256 of the ``state`` sent to the identity provider and is deleted when the browser comes back,
  so each works once, on whichever replica the browser returns to. ``browser_sha256`` is the SHA-256 of a random
  value kept in the browser's ``trailbeam_oidc`` cookie: the callback must come from the browser that started the
  sign-in. ``nonce`` and ``code_verifier`` (PKCE) are kept as they are: the ID token and the code exchange need
  them, and they are useless once the row is gone.
- ``oidc_identities``: which person an identity provider's account is. ``issuer`` and ``subject`` are the ID token's
  ``iss`` and ``sub``, which never change for an account (an email can). Removed with the person.

Times are milliseconds since the epoch, like ``schema_version.applied_at_ms``. None of these tables hold spans:
retention, ``--vacuum`` and the page queries never touch them (a vacuum rewrites the file with them in it).
A ``--db :memory:`` store loses them, people and tokens included, on every restart.
"""
from __future__ import annotations

TABLES = ("users", "sessions", "ingest_tokens", "personal_tokens", "setup_codes", "oidc_logins", "oidc_identities")

# Added after the tables were created (migrations.py 17, pg_schema.py 11); the CREATEs below are those of the first
# migration (SQLite 15, Postgres 8) and stay as they are.
PROXY_COLUMN = "proxy_linked_at_ms"
SQLITE_ADD_PROXY = f"ALTER TABLE users ADD COLUMN {PROXY_COLUMN} INTEGER"
PG_ADD_PROXY = f"ALTER TABLE users ADD COLUMN IF NOT EXISTS {PROXY_COLUMN} BIGINT"

SQLITE_CREATE = """
CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY,
    email TEXT NOT NULL COLLATE NOCASE,
    name TEXT NOT NULL,
    role TEXT NOT NULL,                  -- admin | member
    password_hash TEXT,                  -- scrypt$15$8$3$<salt>$<hash>; NULL: none set yet
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    disabled_at_ms INTEGER,
    failed_count INTEGER NOT NULL DEFAULT 0,  -- failed sign-ins since the last one that worked
    locked_until_ms INTEGER,
    last_sign_in_ms INTEGER
);
CREATE UNIQUE INDEX IF NOT EXISTS users_email ON users (email COLLATE NOCASE);
CREATE TABLE IF NOT EXISTS sessions (
    id_sha256 TEXT PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    created_at_ms INTEGER NOT NULL,
    expires_at_ms INTEGER NOT NULL,      -- fixed when it starts, never extended
    last_seen_ms INTEGER NOT NULL        -- written at most hourly
);
CREATE INDEX IF NOT EXISTS sessions_user ON sessions (user_id);
CREATE INDEX IF NOT EXISTS sessions_expires ON sessions (expires_at_ms);
CREATE TABLE IF NOT EXISTS ingest_tokens (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    token_sha256 TEXT NOT NULL UNIQUE,
    prefix TEXT NOT NULL,                -- the first 8 characters, for display
    created_at_ms INTEGER NOT NULL,
    created_by INTEGER REFERENCES users(id) ON DELETE SET NULL,
    last_used_ms INTEGER,                -- written about once a minute per replica
    revoked_at_ms INTEGER
);
CREATE TABLE IF NOT EXISTS personal_tokens (
    id INTEGER PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    token_sha256 TEXT NOT NULL UNIQUE,
    prefix TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL,
    expires_at_ms INTEGER,               -- NULL: until revoked
    last_used_ms INTEGER,
    revoked_at_ms INTEGER
);
CREATE INDEX IF NOT EXISTS personal_tokens_user ON personal_tokens (user_id);
CREATE TABLE IF NOT EXISTS setup_codes (
    code_sha256 TEXT PRIMARY KEY,
    kind TEXT NOT NULL,                  -- setup (phase 2: invite | reset)
    user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,  -- NULL for setup
    created_at_ms INTEGER NOT NULL,
    expires_at_ms INTEGER NOT NULL,
    used_at_ms INTEGER
);
"""

PG_CREATE = """
CREATE TABLE IF NOT EXISTS users (
    id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    email TEXT NOT NULL,
    name TEXT NOT NULL,
    role TEXT NOT NULL,
    password_hash TEXT,
    created_at_ms BIGINT NOT NULL,
    updated_at_ms BIGINT NOT NULL,
    disabled_at_ms BIGINT,
    failed_count INTEGER NOT NULL DEFAULT 0,
    locked_until_ms BIGINT,
    last_sign_in_ms BIGINT
);
CREATE UNIQUE INDEX IF NOT EXISTS users_email ON users (lower(email));
CREATE TABLE IF NOT EXISTS sessions (
    id_sha256 TEXT PRIMARY KEY,
    user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    created_at_ms BIGINT NOT NULL,
    expires_at_ms BIGINT NOT NULL,
    last_seen_ms BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS sessions_user ON sessions (user_id);
CREATE INDEX IF NOT EXISTS sessions_expires ON sessions (expires_at_ms);
CREATE TABLE IF NOT EXISTS ingest_tokens (
    id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    name TEXT NOT NULL,
    token_sha256 TEXT NOT NULL UNIQUE,
    prefix TEXT NOT NULL,
    created_at_ms BIGINT NOT NULL,
    created_by BIGINT REFERENCES users(id) ON DELETE SET NULL,
    last_used_ms BIGINT,
    revoked_at_ms BIGINT
);
CREATE TABLE IF NOT EXISTS personal_tokens (
    id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    token_sha256 TEXT NOT NULL UNIQUE,
    prefix TEXT NOT NULL,
    created_at_ms BIGINT NOT NULL,
    expires_at_ms BIGINT,
    last_used_ms BIGINT,
    revoked_at_ms BIGINT
);
CREATE INDEX IF NOT EXISTS personal_tokens_user ON personal_tokens (user_id);
CREATE TABLE IF NOT EXISTS setup_codes (
    code_sha256 TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    user_id BIGINT REFERENCES users(id) ON DELETE CASCADE,
    created_at_ms BIGINT NOT NULL,
    expires_at_ms BIGINT NOT NULL,
    used_at_ms BIGINT
);
"""

SQLITE_OIDC = """
CREATE TABLE IF NOT EXISTS oidc_logins (
    state_sha256 TEXT PRIMARY KEY,
    browser_sha256 TEXT NOT NULL,        -- SHA-256 of the trailbeam_oidc cookie's value
    nonce TEXT NOT NULL,
    code_verifier TEXT NOT NULL,         -- PKCE
    next_path TEXT NOT NULL,             -- where to go once signed in
    created_at_ms INTEGER NOT NULL,
    expires_at_ms INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS oidc_logins_expires ON oidc_logins (expires_at_ms);
CREATE TABLE IF NOT EXISTS oidc_identities (
    issuer TEXT NOT NULL,
    subject TEXT NOT NULL,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    email TEXT NOT NULL,                 -- as the identity provider last sent it, for the list
    created_at_ms INTEGER NOT NULL,
    last_sign_in_ms INTEGER,
    PRIMARY KEY (issuer, subject)
);
CREATE INDEX IF NOT EXISTS oidc_identities_user ON oidc_identities (user_id);
"""

PG_OIDC = """
CREATE TABLE IF NOT EXISTS oidc_logins (
    state_sha256 TEXT PRIMARY KEY,
    browser_sha256 TEXT NOT NULL,
    nonce TEXT NOT NULL,
    code_verifier TEXT NOT NULL,
    next_path TEXT NOT NULL,
    created_at_ms BIGINT NOT NULL,
    expires_at_ms BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS oidc_logins_expires ON oidc_logins (expires_at_ms);
CREATE TABLE IF NOT EXISTS oidc_identities (
    issuer TEXT NOT NULL,
    subject TEXT NOT NULL,
    user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    email TEXT NOT NULL,
    created_at_ms BIGINT NOT NULL,
    last_sign_in_ms BIGINT,
    PRIMARY KEY (issuer, subject)
);
CREATE INDEX IF NOT EXISTS oidc_identities_user ON oidc_identities (user_id);
"""
