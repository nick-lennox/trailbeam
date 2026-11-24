"""The trailbeam command, which runs Trailbeam: ``trailbeam --db trailbeam.sqlite --host 127.0.0.1 --port 4318``."""
from __future__ import annotations

import argparse
import errno
import logging
import math
import os
import signal
import socket
import sqlite3
import sys
import threading
import time

from .. import __version__
from . import access, auth, config, first_admin, oidc, proxy_auth as proxy_auth_mod, trace_links
from .accounts import Accounts
from .backpressure import DEFAULT_MAX_PENDING, DEFAULT_MAX_WAIT_S, IngestGate
from .db import SqliteStore, parse_queue_list
from .migrations import SchemaError
from .proxy import TrustedProxies, parse_networks
from .retention import DEFAULT as DEFAULT_RETENTION, Purger, Retention, parse_duration, parse_queue_retentions
from .server import has_account_tables, make_server


def main(argv=None) -> None:
    argv = sys.argv[1:] if argv is None else list(argv)
    if argv[:1] in (["--version"], ["-V"]):
        print(f"trailbeam {__version__}")
        return
    if argv[:1] == ["start"]:
        from .start import start
        return start(argv[1:])
    if argv[:1] in (["users"], ["tokens"]):
        from . import cli
        sys.exit(cli.main(argv))
    if argv[:1] == ["doctor"]:
        from . import doctor
        sys.exit(doctor.main(argv[1:]))
    serve(build_parser().parse_args(argv))


def build_parser(prog: str = "trailbeam", description: str = "") -> argparse.ArgumentParser:
    """Every flag of the trailbeam command; ``trailbeam start`` adds its own to the same set."""
    p = argparse.ArgumentParser(
        prog=prog, description=description or "Trailbeam: OTLP/HTTP ingest, JSON API and web screen on one port.",
        epilog="First time? trailbeam start creates an access token and a receipt-hash key, starts the server and "
               "prints the environment variables for your services (trailbeam start --help). "
               "No data arriving? trailbeam doctor checks why (trailbeam doctor --help). People and tokens: "
               "trailbeam users --help, trailbeam tokens --help.")
    p.add_argument("-V", "--version", action="version", version=f"trailbeam {__version__}",
                   help="print the version (trailbeam X.Y.Z) and exit")
    p.add_argument("--db", default=os.environ.get("TRAILBEAM_DB", "trailbeam.sqlite"),
                   help="SQLite file (env TRAILBEAM_DB, default trailbeam.sqlite)")
    p.add_argument("--store-url", default=os.environ.get("TRAILBEAM_STORE_URL") or None,
                   help="a Postgres you run, postgres://user:password@host:5432/db (env TRAILBEAM_STORE_URL), instead "
                        "of the SQLite file: needs pip install \"trailbeam[postgres]\". Trailbeam creates and upgrades "
                        "its tables there; installing, backing up and upgrading Postgres is yours. Default: SQLite")
    p.add_argument("--host", default=os.environ.get("TRAILBEAM_HOST", "127.0.0.1"),
                   help="listen address (env TRAILBEAM_HOST, default 127.0.0.1; use 0.0.0.0 in a container)")
    p.add_argument("--port", type=_port, default=_env(_port, "TRAILBEAM_PORT", 4318),
                   help="listen port (env TRAILBEAM_PORT, default 4318)")
    p.add_argument("--stuck-after", type=_positive_int,
                   default=_env(_positive_int, "TRAILBEAM_STUCK_AFTER_S", 300),
                   help="seconds after which a message without a final delete is stuck (env TRAILBEAM_STUCK_AFTER_S, default 300)")
    p.add_argument("--lambda-timeout", type=_positive_int,
                   default=_env(_positive_int, "TRAILBEAM_LAMBDA_TIMEOUT_S", 900),
                   help="seconds after which a Lambda receive without an outcome is inferred killed "
                        "(env TRAILBEAM_LAMBDA_TIMEOUT_S, default 900, the Lambda maximum)")
    p.add_argument("--config", default=os.environ.get("TRAILBEAM_CONFIG"),
                   help="a TOML file of per-queue settings: max_receive_count, store_bodies and retention, for each "
                        "queue under [queues.<name>], and the defaults under [defaults] (env TRAILBEAM_CONFIG; "
                        "README \"Configuration file\"). Checked at start: any problem stops the server. The flags "
                        "below, and their environment variables, override it")
    p.add_argument("--max-receive-count", type=_max_receive_counts,
                   default=_env(_max_receive_counts, "TRAILBEAM_MAX_RECEIVE_COUNT", None),
                   help="QUEUE=N[,QUEUE=N...]: the maxReceiveCount of each queue's redrive policy, so a timeline "
                        "can say when a message should have moved to its dead-letter queue but no receive "
                        "there has reported it (env TRAILBEAM_MAX_RECEIVE_COUNT; spans don't carry it)")
    p.add_argument("--store-bodies", type=parse_queue_list,
                   default=_env(parse_queue_list, "TRAILBEAM_STORE_BODIES", None),
                   help="QUEUE[,QUEUE...] or *: the queues whose message bodies are stored, for packages that "
                        "capture them. Bodies of every other queue are dropped before they are written "
                        "(env TRAILBEAM_STORE_BODIES, default none: no body is stored). Replaces the config "
                        "file's list")
    p.add_argument("--retention", type=_arg(lambda t: (parse_duration(t),)),
                   default=_env(lambda t: (parse_duration(t),), "TRAILBEAM_RETENTION", None),
                   help="how long spans are kept: a duration such as 7d, 36h or 90m, or forever "
                        f"(env TRAILBEAM_RETENTION, default {DEFAULT_RETENTION}). Whole messages are deleted, once "
                        "their newest span is older")
    p.add_argument("--retention-queues", type=_arg(parse_queue_retentions),
                   default=_env(parse_queue_retentions, "TRAILBEAM_RETENTION_QUEUES", None),
                   help="QUEUE=DURATION[,QUEUE=DURATION...]: per-queue retention, overriding --retention, e.g. "
                        "orders-dlq=30d,audit=forever. A message on several queues is kept as long as the longest "
                        "(env TRAILBEAM_RETENTION_QUEUES). Overrides the config file for the queues it names")
    p.add_argument("--purge-interval", type=_interval,
                   default=_env(_interval, "TRAILBEAM_PURGE_INTERVAL_S", 600.0),
                   help="seconds between retention passes; the first runs at start (env TRAILBEAM_PURGE_INTERVAL_S, "
                        "default 600)")
    p.add_argument("--purge-batch", type=_positive_int, default=_env(_positive_int, "TRAILBEAM_PURGE_BATCH", 50),
                   help="messages looked at per purge step, each one write transaction: smaller steps hold the "
                        "writer lock for less time (env TRAILBEAM_PURGE_BATCH, default 50)")
    p.add_argument("--purge-duty", type=_duty, default=_env(_duty, "TRAILBEAM_PURGE_DUTY", 0.25),
                   help="the most of the time the purge holds the writer lock, in (0, 1] "
                        "(env TRAILBEAM_PURGE_DUTY, default 0.25)")
    p.add_argument("--durability", type=_durability, default=_env(_durability, "TRAILBEAM_DURABILITY", "full"),
                   help="full (default): an export is answered 200 only once its spans are synced to disk. relaxed: "
                        "the SQLite file skips that sync per export, so exports are answered sooner, and an OS crash "
                        "or power loss can lose the last exports answered 200 before it (a crash of the server "
                        "alone loses none). SQLite only (env TRAILBEAM_DURABILITY)")
    p.add_argument("--vacuum", action="store_true",
                   help="rewrite the database file once so that retention gives deleted space back to the disk "
                        "(files created before retention, schema 8), then exit. Stop the server first; needs free disk "
                        "for a second copy of the file")
    p.add_argument("--token-file", default=os.environ.get("TRAILBEAM_TOKEN_FILE"),
                   help="a file holding the shared access token (env TRAILBEAM_TOKEN_FILE, or the token itself in "
                        "TRAILBEAM_TOKEN). When set, ingest and the API need Authorization: Bearer <token>, and the "
                        "web screen asks for it once. Default: no token, for local use")
    p.add_argument("--trusted-proxy", type=_arg(parse_networks), action="extend", default=None,
                   help="an address or CIDR range (repeatable, or comma-separated) of proxies in front of Trailbeam "
                        "whose X-Forwarded-For, X-Forwarded-Proto and X-Forwarded-Host are believed: the client's "
                        "address for sign-in limits and logs, and HTTPS for the session cookie (env "
                        "TRAILBEAM_TRUSTED_PROXY). Default: none, those headers are ignored")
    p.add_argument("--proxy-auth-header", default=os.environ.get("TRAILBEAM_PROXY_AUTH_HEADER", "").strip() or None,
                   help="sign-in through a proxy you run (oauth2-proxy, Pomerium, Cloudflare Access...): the header "
                        "holding the email it signed in, e.g. X-Forwarded-Email (env TRAILBEAM_PROXY_AUTH_HEADER). "
                        "Believed only from --trusted-proxy addresses and/or with --proxy-auth-secret-file; a "
                        "first-time email becomes a member. Default: off")
    p.add_argument("--proxy-auth-name-header",
                   default=os.environ.get("TRAILBEAM_PROXY_AUTH_NAME_HEADER", "").strip() or None,
                   help="the header holding the person's name, for people the proxy signs in the first time, e.g. "
                        "X-Forwarded-Preferred-Username (env TRAILBEAM_PROXY_AUTH_NAME_HEADER). Default: the "
                        "email's part before @")
    p.add_argument("--proxy-auth-secret-file",
                   default=os.environ.get("TRAILBEAM_PROXY_AUTH_SECRET_FILE", "").strip() or None,
                   help="a file holding a secret the proxy sends in X-Trailbeam-Proxy-Secret; the email header is "
                        "believed only with it (env TRAILBEAM_PROXY_AUTH_SECRET_FILE, or the secret itself in "
                        "TRAILBEAM_PROXY_AUTH_SECRET)")
    p.add_argument("--proxy-auth-sign-out-url",
                   default=os.environ.get("TRAILBEAM_PROXY_AUTH_SIGN_OUT_URL", "").strip() or None,
                   help="where Sign out sends someone the proxy signed in, to end the proxy's session, e.g. "
                        "/oauth2/sign_out (env TRAILBEAM_PROXY_AUTH_SIGN_OUT_URL). Default: no Sign out for them")
    p.add_argument("--ingest-max-pending", type=_positive_int,
                   default=_env(_positive_int, "TRAILBEAM_INGEST_MAX_PENDING", DEFAULT_MAX_PENDING),
                   help="exports in progress at once; one more is answered 429 with Retry-After "
                        f"(env TRAILBEAM_INGEST_MAX_PENDING, default {DEFAULT_MAX_PENDING})")
    p.add_argument("--ingest-max-wait", type=_positive_float,
                   default=_env(_positive_float, "TRAILBEAM_INGEST_MAX_WAIT_S", DEFAULT_MAX_WAIT_S),
                   help="seconds an export waits for the database writer before it is answered 503 with "
                        f"Retry-After (env TRAILBEAM_INGEST_MAX_WAIT_S, default {DEFAULT_MAX_WAIT_S:g})")
    p.add_argument("--pg-ingest-connections", type=_positive_int,
                   default=_env(_positive_int, "TRAILBEAM_PG_INGEST_CONNECTIONS", None),
                   help="Postgres only: the most connections a replica writes exports on at once "
                        f"(env TRAILBEAM_PG_INGEST_CONNECTIONS, default {PG_INGEST_CONNECTIONS})")
    p.add_argument("--pg-read-connections", type=_positive_int,
                   default=_env(_positive_int, "TRAILBEAM_PG_READ_CONNECTIONS", None),
                   help="Postgres only: the most connections a replica reads pages and the API on at once "
                        f"(env TRAILBEAM_PG_READ_CONNECTIONS, default {PG_READ_CONNECTIONS})")
    p.add_argument("--trace-link", type=_arg(trace_links.check),
                   default=_env(_arg(trace_links.check), "TRAILBEAM_TRACE_LINK", None),
                   help="a URL template that opens a step's trace in your tracing tool, such as "
                        "https://app.datadoghq.com/apm/trace/{trace_id} or http://jaeger:16686/trace/{trace_id}. "
                        "The timeline then shows Open trace on each recorded step. Placeholders: "
                        + ", ".join("{" + n + "}" for n in trace_links.PLACEHOLDERS)
                        + " (env TRAILBEAM_TRACE_LINK, default none: no links)")
    p.add_argument("--accounts", action="store_true", default=None,
                   help="switch on users and roles, a preview in this release that has not had its security review "
                        "yet: sign-in with email and password, personal and ingest tokens, the first admin's setup "
                        "link (env TRAILBEAM_ACCOUNTS=1). Default: off, the server uses TRAILBEAM_TOKEN alone, and "
                        "people and tokens in the store are ignored")
    sso = p.add_argument_group(
        "single sign-on (OpenID Connect)",
        'off unless --oidc-issuer is set; needs pip install "trailbeam[server,sso]". The identity provider sends '
        "people back to <public URL>/login/oidc/callback: register that redirect URI there")
    sso.add_argument("--public-url", default=os.environ.get("TRAILBEAM_PUBLIC_URL") or None,
                     help="the address people open Trailbeam at, such as https://trailbeam.example.com (env "
                          "TRAILBEAM_PUBLIC_URL). Needed for single sign-on: the request's Host can't be trusted")
    sso.add_argument("--oidc-issuer", default=os.environ.get("TRAILBEAM_OIDC_ISSUER") or None,
                     help="the identity provider's issuer URL, exactly as its ID tokens' iss, such as "
                          "https://id.example.com/realms/main (Keycloak), https://example.okta.com, "
                          "https://login.microsoftonline.com/<tenant>/v2.0 (Entra) or https://accounts.google.com "
                          "(env TRAILBEAM_OIDC_ISSUER)")
    sso.add_argument("--oidc-client-id", default=os.environ.get("TRAILBEAM_OIDC_CLIENT_ID") or None,
                     help="this server's client ID at the provider (env TRAILBEAM_OIDC_CLIENT_ID)")
    sso.add_argument("--oidc-client-secret-file", default=os.environ.get("TRAILBEAM_OIDC_CLIENT_SECRET_FILE") or None,
                     help="a file holding the client secret (env TRAILBEAM_OIDC_CLIENT_SECRET_FILE, or the secret "
                          "itself in TRAILBEAM_OIDC_CLIENT_SECRET). Without one, a public client with PKCE")
    sso.add_argument("--oidc-scopes", default=os.environ.get("TRAILBEAM_OIDC_SCOPES") or None,
                     help="scopes asked for (env TRAILBEAM_OIDC_SCOPES, default \"openid email profile\"; add the one "
                          "your provider needs for groups, such as groups on Okta)")
    sso.add_argument("--oidc-groups-claim", default=os.environ.get("TRAILBEAM_OIDC_GROUPS_CLAIM") or None,
                     help="the ID token claim that lists the person's groups (env TRAILBEAM_OIDC_GROUPS_CLAIM, "
                          "default groups; roles on some Entra setups)")
    sso.add_argument("--oidc-admin-groups", default=os.environ.get("TRAILBEAM_OIDC_ADMIN_GROUPS") or None,
                     help="comma-separated groups whose people are admins (env TRAILBEAM_OIDC_ADMIN_GROUPS). With "
                          "groups mapped, the provider decides each person's role at every sign-in")
    sso.add_argument("--oidc-member-groups", default=os.environ.get("TRAILBEAM_OIDC_MEMBER_GROUPS") or None,
                     help="comma-separated groups whose people are members (env TRAILBEAM_OIDC_MEMBER_GROUPS)")
    sso.add_argument("--oidc-default-role", choices=("member", "none"),
                     default=os.environ.get("TRAILBEAM_OIDC_DEFAULT_ROLE") or None,
                     help="the role of someone in none of those groups: member, or none to refuse them (env "
                          "TRAILBEAM_OIDC_DEFAULT_ROLE; default member, or none when --oidc-member-groups is set)")
    sso.add_argument("--oidc-allowed-domains", default=os.environ.get("TRAILBEAM_OIDC_ALLOWED_DOMAINS") or None,
                     help="comma-separated email domains that may sign in, with a verified email (env "
                          "TRAILBEAM_OIDC_ALLOWED_DOMAINS; for Google Workspace, its domain). Default: any account "
                          "the provider signs in")
    sso.add_argument("--oidc-link-email", action="store_true",
                     default=os.environ.get("TRAILBEAM_OIDC_LINK_EMAIL", "").strip().lower() in ("1", "true", "yes"),
                     help="let a provider account with a verified email sign in to the existing account with that "
                          "email (env TRAILBEAM_OIDC_LINK_EMAIL=true). Off by default: a provider that lets people "
                          "choose their email would let them take over accounts")
    sso.add_argument("--oidc-allow-unverified-email", action="store_true",
                     default=os.environ.get("TRAILBEAM_OIDC_ALLOW_UNVERIFIED_EMAIL", "").strip().lower()
                     in ("1", "true", "yes"),
                     help="create accounts for provider accounts whose email the provider hasn't verified (env "
                          "TRAILBEAM_OIDC_ALLOW_UNVERIFIED_EMAIL=true). Needed for Microsoft Entra, which sends no "
                          "email_verified. Off by default: someone could set another person's email at the "
                          "provider and get an account under it")
    sso.add_argument("--oidc-session-hours", default=os.environ.get("TRAILBEAM_OIDC_SESSION_HOURS") or None,
                     help="how long a single sign-on session lasts, in hours (env TRAILBEAM_OIDC_SESSION_HOURS, "
                          "default 12, at most 720). The provider is asked only at sign-in, so someone disabled "
                          "there keeps their session here until it runs out: disable them here too")
    sso.add_argument("--oidc-label", default=os.environ.get("TRAILBEAM_OIDC_LABEL") or None,
                     help='the sign-in button says "Sign in with <label>" (env TRAILBEAM_OIDC_LABEL, default '
                          "single sign-on), such as Okta")
    p.add_argument("--log-level", default=os.environ.get("TRAILBEAM_LOG_LEVEL", "INFO"))
    return p


def oidc_settings(a: argparse.Namespace):
    """Single sign-on's settings from the flags, checked, or None when it is off. Raises oidc.SettingsError."""
    if not getattr(a, "oidc_issuer", None):
        return oidc.make_settings(issuer=None, client_id=getattr(a, "oidc_client_id", None), public_url=None,
                                  admin_groups=getattr(a, "oidc_admin_groups", None),
                                  member_groups=getattr(a, "oidc_member_groups", None),
                                  allowed_domains=getattr(a, "oidc_allowed_domains", None))
    if a.oidc_client_secret_file:
        secret = oidc.read_secret_file(a.oidc_client_secret_file)
    else:
        secret = os.environ.get("TRAILBEAM_OIDC_CLIENT_SECRET") or None
    s = oidc.make_settings(issuer=a.oidc_issuer, client_id=a.oidc_client_id, public_url=a.public_url,
                           client_secret=secret, scopes=a.oidc_scopes, groups_claim=a.oidc_groups_claim,
                           admin_groups=a.oidc_admin_groups, member_groups=a.oidc_member_groups,
                           default_role=a.oidc_default_role, allowed_domains=a.oidc_allowed_domains,
                           link_email=a.oidc_link_email,
                           allow_unverified_email=a.oidc_allow_unverified_email, label=a.oidc_label,
                           session_hours=a.oidc_session_hours)
    oidc.require_pyjwt()
    return s


def serve(a: argparse.Namespace, on_listening=None) -> None:
    """Run the server with the parsed flags until SIGTERM or Ctrl-C. ``on_listening(host, port, token, app)``,
    if given, is called once the port is bound, before the first request is served."""
    logging.basicConfig(level=a.log_level.upper(), format="%(asctime)s %(levelname)s %(name)s %(message)s")
    try:
        cfg = config.load(a.config) if a.config else config.Config()
    except config.ConfigError as e:
        logging.getLogger("trailbeam.server").error("configuration file %s", e)
        sys.exit(2)
    s = config.merge(cfg, retention=a.retention, retention_queues=a.retention_queues, store_bodies=a.store_bodies,
                     max_receive_counts=a.max_receive_count, default_retention=parse_duration(DEFAULT_RETENTION))
    a.retention, a.retention_queues = s["retention"], s["retention_queues"]
    a.store_bodies, a.max_receive_count = s["store_bodies"], s["max_receive_counts"]
    if a.config:
        logging.getLogger("trailbeam.server").info(
            "configuration file %s: %d queue(s) with settings", a.config,
            len(set(cfg.max_receive_counts) | set(cfg.retention_queues) | (cfg.store_bodies - {"*"})))
    try:
        token = auth.read_token_file(a.token_file) if a.token_file else (
            auth.check_token(os.environ["TRAILBEAM_TOKEN"]) if os.environ.get("TRAILBEAM_TOKEN", "").strip() else None)
    except auth.TokenError as e:
        logging.getLogger("trailbeam.server").error("TRAILBEAM_TOKEN: %s", e)
        sys.exit(2)
    try:
        accounts_on = bool(a.accounts) or access.switch_on(os.environ)
    except ValueError as e:
        logging.getLogger("trailbeam.server").error("%s", e)
        sys.exit(2)
    if not accounts_on and os.environ.get(first_admin.EMAIL_ENV, "").strip():
        logging.getLogger("trailbeam.server").error(
            "%s is set, but %s", first_admin.EMAIL_ENV, access.SWITCHED_OFF)
        sys.exit(2)
    try:
        sso = oidc_settings(a)
    except oidc.SettingsError as e:
        logging.getLogger("trailbeam.server").error("single sign-on: %s", e)
        sys.exit(2)
    if sso is not None and not accounts_on:
        logging.getLogger("trailbeam.server").error(
            "single sign-on (--oidc-issuer, TRAILBEAM_OIDC_ISSUER) signs people in, but %s", access.SWITCHED_OFF)
        sys.exit(2)
    if not accounts_on and getattr(a, "proxy_auth_header", None):  # it signs people in: it needs users and roles
        logging.getLogger("trailbeam.server").error(
            "--proxy-auth-header (TRAILBEAM_PROXY_AUTH_HEADER) is set, but %s", access.SWITCHED_OFF)
        sys.exit(2)
    if a.store_bodies:
        where = redact_url(a.store_url) if a.store_url else a.db
        logging.getLogger("trailbeam.server").warning(body_warning(a.store_bodies, where, token is not None))
    retention = Retention(a.retention, a.retention_queues)
    where = redact_url(a.store_url) if a.store_url else a.db
    try:
        if a.store_url and a.durability != "full":
            raise StoreUrlError("--durability relaxed is for the SQLite file; with Postgres, set its own "
                                "synchronous_commit instead")
        if not a.store_url and (a.pg_ingest_connections or a.pg_read_connections):
            raise StoreUrlError("--pg-ingest-connections and --pg-read-connections (TRAILBEAM_PG_INGEST_CONNECTIONS, "
                                "TRAILBEAM_PG_READ_CONNECTIONS) are for a Postgres store (TRAILBEAM_STORE_URL); "
                                "SQLite has one writer")
        db = open_store(a.store_url, a.db, store_bodies=a.store_bodies, retention=retention,
                        durability=a.durability, ingest_connections=a.pg_ingest_connections,
                        read_connections=a.pg_read_connections)
    except (SchemaError, StoreUrlError) as e:
        logging.getLogger("trailbeam.server").error("%s", e)
        sys.exit(2)
    if a.durability == "relaxed":
        logging.getLogger("trailbeam.server").warning(
            "durability relaxed: exports are answered before their spans are synced to disk; an OS crash or "
            "power loss can lose the last ones answered 200")
    if db.migrated:
        logging.getLogger("trailbeam.server").info("upgraded %s to schema version %d", where, db.migrated[-1])
    if a.vacuum:
        if not isinstance(db, SqliteStore):
            logging.getLogger("trailbeam.server").error("--vacuum is for the SQLite file; Postgres reclaims space itself")
            sys.exit(2)
        sys.exit(vacuum(db, a.db))
    purger = Purger(db, retention, interval_s=a.purge_interval, batch=a.purge_batch, duty=a.purge_duty)
    if isinstance(db, SqliteStore) and db.storage()["auto_vacuum"] != "incremental" and not retention.keeps_everything():
        logging.getLogger("trailbeam.server").info(
            "%s was created without incremental vacuum: retention deletes old spans and new spans reuse the "
            "space, but the file does not shrink. To give the space back, stop the server and run trailbeam "
            "--db %s --vacuum once (it rewrites the file)", a.db, a.db)
    try:
        proxy_auth = proxy_auth_config(a)
    except proxy_auth_mod.ProxyAuthError as e:
        db.close()
        logging.getLogger("trailbeam.server").error("%s", e)
        sys.exit(2)
    try:
        httpd = make_server(db, a.host, a.port, stuck_after_s=a.stuck_after,
                            lambda_timeout_s=a.lambda_timeout, max_receive_counts=a.max_receive_count,
                            purger=purger, auth=auth.Auth(token) if token else None,
                            gate=IngestGate(a.ingest_max_pending, a.ingest_max_wait), trace_link=a.trace_link,
                            trusted_proxies=TrustedProxies(trusted_proxies(a)), proxy_auth=proxy_auth, oidc=sso,
                            **({} if accounts_on else {"accounts": None}))
    except OSError as e:
        db.close()
        sys.stderr.write(bind_error(e, a.host, a.port, getattr(a, "command", "trailbeam")) + "\n")
        sys.exit(2)
    try:  # TRAILBEAM_ADMIN_EMAIL + TRAILBEAM_ADMIN_PASSWORD_FILE: the first admin, only while there is none
        if first_admin.from_env(httpd.app.accounts, httpd.app.signin.hasher if httpd.app.signin else None,
                                time.time_ns() // 1_000_000) is not None:
            httpd.app.access.state.changed()
    except first_admin.FirstAdminError as e:
        httpd.server_close()
        db.close()
        logging.getLogger("trailbeam.server").error("%s", e)
        sys.exit(2)
    except Exception as e:  # noqa: BLE001 - the store failed; the server would not get far either
        httpd.server_close()
        db.close()
        logging.getLogger("trailbeam.server").error("%s: could not create the first admin: %s",
                                                    first_admin.EMAIL_ENV, e)
        sys.exit(2)
    if sso is not None:
        logging.getLogger("trailbeam.server").info(
            "single sign-on with %s (client %s): register the redirect URI %s at the identity provider. Its "
            "sessions last %g hours (--oidc-session-hours)", sso.issuer, sso.client_id, sso.redirect_uri,
            sso.session_hours)
    if not accounts_on:
        ignored = ignored_accounts(db)
        if ignored:
            logging.getLogger("trailbeam.server").warning(
                "the store holds %s, ignored: %s. Until then TRAILBEAM_TOKEN alone guards this server",
                ignored, access.SWITCHED_OFF)
    if httpd.app.proxies:
        logging.getLogger("trailbeam.server").info(
            "trusted proxies (X-Forwarded-For, -Proto and -Host believed from): %s",
            ", ".join(str(n) for n in httpd.app.proxies.networks))
    logging.getLogger("trailbeam.server").info("listening on http://%s:%d (OTLP: /v1/traces, API: /api/, "
                                               "metrics: /metrics), db %s",
                                        a.host, httpd.server_address[1], where)
    for level, message in access_log(httpd.app, token is not None, a.host):  # after "listening on": scripts read it first
        logging.getLogger("trailbeam.server").log(level, message)
    # docker stop and systemd send SIGTERM; Python ignores it as PID 1 unless a handler is set. Stop serving,
    # then the finally closes the SQLite file cleanly. From the handler on, everything is inside the try: a
    # SIGTERM during the next lines still stops the purger and closes the store (QV-434). The handler asks
    # serve_forever to return instead of raising (QV-502): see stop_on_signal.
    signal.signal(signal.SIGTERM, stop_on_signal(httpd))
    try:
        logging.getLogger("trailbeam.server").info("retention %s", retention.config())
        purger.start()
        if on_listening:
            on_listening(a.host, httpd.server_address[1], token, httpd.app)
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        purger.stop()
        httpd.server_close()
        db.close()


def bind_error(e: OSError, host: str, port: int, command: str = "trailbeam") -> str:
    """The one line printed when the listen port can't be bound, instead of a traceback. 4318 is the standard
    OTLP/HTTP port, so the usual cause is an OpenTelemetry Collector, Jaeger or another OTLP receiver."""
    if e.errno == errno.EADDRINUSE or getattr(e, "winerror", None) == 10048:  # WSAEADDRINUSE
        other = port + 1 if port < 65535 else port - 1
        return (f"trailbeam: port {port} on {host} is already in use (another OTLP receiver?). Stop it or run "
                f"{command} --port {other} and set TRAILBEAM_OTLP_TRACES_ENDPOINT to match.")
    if e.errno == errno.EACCES:
        return (f"trailbeam: not allowed to listen on port {port} on {host} (ports below 1024 need root). "
                f"Run {command} --port 4318, or another port from 1024.")
    if isinstance(e, socket.gaierror):
        return f"trailbeam: --host {host}: {e.strerror or e}. Use --host 127.0.0.1, or 0.0.0.0 for every address."
    if e.errno == errno.EADDRNOTAVAIL:
        return (f"trailbeam: can't listen on {host}: it is not an address of this machine. Use --host 127.0.0.1, "
                f"or 0.0.0.0 for every address.")
    return f"trailbeam: can't listen on port {port} on {host}: {e.strerror or e}"


class StoreUrlError(ValueError):
    pass


PG_INGEST_CONNECTIONS = 4  # PostgresStore's pools, when not set
PG_READ_CONNECTIONS = 8


def open_store(url, db_path: str, *, store_bodies, retention, durability: str = "full",
               ingest_connections=None, read_connections=None):
    """The Store: Postgres when ``url`` is a postgres:// one (its driver imported only then), else SQLite.
    ``ingest_connections``, ``read_connections``: Postgres's pool sizes (None: PG_INGEST_CONNECTIONS,
    PG_READ_CONNECTIONS)."""
    if not url:
        return SqliteStore(db_path, store_bodies=store_bodies, durability=durability)
    if not url.startswith(("postgres://", "postgresql://")):
        raise StoreUrlError(f"TRAILBEAM_STORE_URL: expected postgres://… or postgresql://…, got {redact_url(url)!r}")
    try:
        from .postgres import PostgresStore
    except ImportError as e:
        raise StoreUrlError(f"TRAILBEAM_STORE_URL is a Postgres URL, but its driver is not installed ({e}): "
                            "pip install \"trailbeam[postgres]\"") from None
    import psycopg
    try:
        return PostgresStore(url, store_bodies=store_bodies, retention=retention,
                             ingest_connections=ingest_connections or PG_INGEST_CONNECTIONS,
                             read_connections=read_connections or PG_READ_CONNECTIONS)
    except psycopg.Error as e:
        raise StoreUrlError(f"{redact_url(url)}: {e}") from None


def redact_url(url: str) -> str:
    """The URL without its password, for logs."""
    head, sep, rest = url.partition("://")
    creds, at, host = rest.rpartition("@")
    if not at or ":" not in creds:
        return url
    return f"{head}{sep}{creds.split(':', 1)[0]}:***@{host}"


def vacuum(db: SqliteStore, path: str) -> int:
    log = logging.getLogger("trailbeam.server")
    before = db.storage()
    log.info("vacuum: rewriting %s (%.1f MB, %.1f MB of it free), with incremental vacuum from now on", path,
             before["pages"] * before["page_size"] / 1e6, before["free_pages"] * before["page_size"] / 1e6)
    t0 = time.monotonic()
    try:
        db.vacuum()
    except sqlite3.Error as e:
        log.error("vacuum failed, the file is as it was: %s", e)
        return 1
    finally:
        after = db.storage()
        db.close()
    log.info("vacuum: done in %.1f s, %.1f MB, auto_vacuum %s", time.monotonic() - t0,
             after["pages"] * after["page_size"] / 1e6, after["auto_vacuum"])
    return 0


def _arg(parse):
    """An argparse type from a parser that raises ValueError."""
    def typ(text):
        try:
            return parse(text)
        except ValueError as e:
            raise argparse.ArgumentTypeError(str(e)) from None
    typ.__name__ = parse.__name__
    return typ


def proxy_auth_config(a: argparse.Namespace):
    """--proxy-auth-header and its settings (proxy_auth.py); None when it is off. Raises ProxyAuthError."""
    secret = None
    path = getattr(a, "proxy_auth_secret_file", None)
    if path:
        try:
            with open(path, encoding="utf-8") as f:
                secret = f.read().strip()
        except (OSError, UnicodeDecodeError) as e:
            raise proxy_auth_mod.ProxyAuthError(f"--proxy-auth-secret-file {path}: {e}") from None
    elif os.environ.get("TRAILBEAM_PROXY_AUTH_SECRET", "").strip():
        secret = os.environ["TRAILBEAM_PROXY_AUTH_SECRET"].strip()
    return proxy_auth_mod.configure(getattr(a, "proxy_auth_header", None),
                                    name_header=getattr(a, "proxy_auth_name_header", None), secret=secret,
                                    sign_out_url=getattr(a, "proxy_auth_sign_out_url", None),
                                    proxies=TrustedProxies(trusted_proxies(a)))


def trusted_proxies(a: argparse.Namespace) -> tuple:
    """--trusted-proxy, else TRAILBEAM_TRUSTED_PROXY, else none."""
    if a.trusted_proxy:
        return tuple(a.trusted_proxy)
    return _env(_arg(parse_networks), "TRAILBEAM_TRUSTED_PROXY", ())


def _env(parse, name: str, default):
    """The environment variable, parsed; ``default`` (not parsed) if it is not set or empty (compose files
    often pass ``${NAME:-}``), so the config file's value stands."""
    if not os.environ.get(name, "").strip():
        return default
    try:
        return parse(os.environ[name])
    except (ValueError, argparse.ArgumentTypeError) as e:
        raise SystemExit(f"trailbeam: {name}: {e}") from None


def _max_receive_counts(text: str) -> dict:
    """``orders=3,payments=5`` -> {"orders": 3, "payments": 5}."""
    out = {}
    for part in filter(None, (p.strip() for p in text.split(","))):
        name, sep, n = part.partition("=")
        if not sep or not name.strip() or not n.strip().isdigit() or not 1 <= int(n) <= 1000:
            raise argparse.ArgumentTypeError(f"expected QUEUE=N with N from 1 to 1000, got {part!r}")
        out[name.strip()] = int(n)
    return out


def _positive_int(text: str) -> int:
    try:
        n = int(text)
    except ValueError:
        n = 0
    if n < 1:
        raise argparse.ArgumentTypeError(f"expected a whole number of at least 1, got {text!r}")
    return n


def _port(text: str) -> int:
    try:
        n = int(text)
    except ValueError:
        n = -1
    if not 0 <= n <= 65535:
        hint = ""
        if text.strip().startswith("tcp://"):
            # QV-291: Kubernetes service links set <SERVICE>_PORT=tcp://<ip>:<port> for each Service in the namespace.
            hint = (" (a Kubernetes service link from a Service named trailbeam: set enableServiceLinks: false "
                    "in the pod spec, or TRAILBEAM_PORT to the port)")
        raise argparse.ArgumentTypeError(f"expected a port from 0 to 65535, got {text!r}{hint}")
    return n


def _interval(text: str) -> float:
    try:
        x = float(text)
    except ValueError:
        x = 0
    if not 0 < x < math.inf:
        raise argparse.ArgumentTypeError(f"expected seconds above 0, got {text!r}")
    return x


def _duty(text: str) -> float:
    try:
        x = float(text)
    except ValueError:
        x = 0
    if not 0 < x <= 1:
        raise argparse.ArgumentTypeError(f"expected a fraction above 0 and at most 1, got {text!r}")
    return x


def _durability(text: str) -> str:
    t = text.strip().lower()
    if t not in SqliteStore.DURABILITY:
        raise argparse.ArgumentTypeError(f"expected full or relaxed, got {text!r}")
    return t


def _positive_float(text: str) -> float:
    try:
        x = float(text)
    except ValueError:
        x = 0
    if not 0 < x <= 3600:
        raise argparse.ArgumentTypeError(f"expected seconds above 0 and at most 3600, got {text!r}")
    return x


def ignored_accounts(db) -> str:
    """With users and roles off: what in the store would have changed access with them on ('' for nothing)."""
    try:
        if not has_account_tables(db):
            return ""
        admin, ingest = Accounts(db).auth_state()
    except Exception:  # noqa: BLE001 - only a warning; the server itself doesn't read these tables
        return ""
    return " and ".join(w for w, on in (("an admin account", admin), ("ingest tokens", ingest)) if on)


def access_log(app, token: bool, host: str) -> list:
    """What start-up says about access (access.py): (logging level, message) pairs."""
    local = host in ("127.0.0.1", "localhost", "::1")
    mode, ingest = app._mode(), app._ingest_auth()
    out = []
    if mode == "accounts":
        out.append((logging.INFO, "this server has an admin: the API and the web screen need a sign-in or a "
                                  "personal API token (tbp_...)"))
        if token:
            out.append((logging.WARNING, "TRAILBEAM_TOKEN is set, and now that an admin exists it works only for "
                                         "ingest and /metrics, not for the API or the web screen"))
    elif token:
        out.append((logging.INFO, "access token set: ingest, the API and the web screen need it"))
    elif not local:
        out.append((logging.WARNING, f"no access token (TRAILBEAM_TOKEN) and listening on {host}: anyone who can "
                                     "reach this port can send spans and read every timeline"))
    if ingest == "open" and (mode == "accounts" or local):
        out.append((logging.WARNING, "ingest is open: there is no TRAILBEAM_TOKEN and no ingest token, so anyone "
                                     "who can reach this port can send spans"))
    elif ingest == "token" and not token:
        out.append((logging.INFO, "ingest needs an ingest token (tbi_...): the store holds at least one"))
    if mode != "accounts" and app.accounts is not None:
        out.append((logging.INFO, first_admin.NO_ADMIN))
    pa = getattr(app, "proxy_signin", None)
    if pa is not None:
        c = pa.config
        source = " and ".join(x for x in (
            "from --trusted-proxy addresses" if c.by_address else "",
            f"with the shared secret in {proxy_auth_mod.SECRET_HEADER}" if c.secret is not None else "") if x)
        out.append((logging.INFO, f"sign-in through a proxy: {c.header} is believed {source}; people it signs in "
                                  "the first time become members" + ("" if mode == "accounts" else
                                                                     ", once this server has an admin")))
    return out


def body_warning(store_bodies, db_path: str, token: bool = False) -> str:
    queues = "every queue" if "*" in store_bodies else "queues " + ", ".join(sorted(store_bodies))
    who = ("anyone with the access token, or who can read that file, can read those bodies." if token else
           "no access token is set (TRAILBEAM_TOKEN), so anyone who can reach this server, or read that file, can "
           "read those bodies. Keep it on a trusted network, or set a token.")
    return f"storing message bodies for {queues} (TRAILBEAM_STORE_BODIES or the config file) in {db_path}: {who}"


def stop_on_signal(httpd):
    """A signal handler that makes ``httpd.serve_forever()`` return, before or while it runs (QV-502).

    It used to raise KeyboardInterrupt. Python runs a handler wherever the main thread is, and an exception
    raised inside a garbage-collector callback (a WeakSet's, say) is printed as "Exception ignored" and dropped:
    the server went on serving and docker stop had to kill it. A handler that raises nothing can't be lost.
    shutdown() waits for serve_forever to return, so it runs on its own thread, not on the main one."""
    def handler(signum, frame):
        threading.Thread(target=httpd.shutdown, name="trailbeam-stop", daemon=True).start()
    return handler


if __name__ == "__main__":
    main()
