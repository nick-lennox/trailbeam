"""The trailbeam command, which runs Trailbeam: ``trailbeam --db trailbeam.sqlite --host 127.0.0.1 --port 4318``."""
from __future__ import annotations

import argparse
import logging
import math
import os
import signal
import sqlite3
import sys
import time

from . import auth, config
from .backpressure import DEFAULT_MAX_PENDING, DEFAULT_MAX_WAIT_S, IngestGate
from .db import SqliteStore, parse_queue_list
from .migrations import SchemaError
from .retention import DEFAULT as DEFAULT_RETENTION, Purger, Retention, parse_duration, parse_queue_retentions
from .server import make_server


def main(argv=None) -> None:
    p = argparse.ArgumentParser(prog="trailbeam", description="Trailbeam: OTLP/HTTP ingest, JSON API and web screen on one port.")
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
    p.add_argument("--ingest-max-pending", type=_positive_int,
                   default=_env(_positive_int, "TRAILBEAM_INGEST_MAX_PENDING", DEFAULT_MAX_PENDING),
                   help="exports in progress at once; one more is answered 429 with Retry-After "
                        f"(env TRAILBEAM_INGEST_MAX_PENDING, default {DEFAULT_MAX_PENDING})")
    p.add_argument("--ingest-max-wait", type=_positive_float,
                   default=_env(_positive_float, "TRAILBEAM_INGEST_MAX_WAIT_S", DEFAULT_MAX_WAIT_S),
                   help="seconds an export waits for the database writer before it is answered 503 with "
                        f"Retry-After (env TRAILBEAM_INGEST_MAX_WAIT_S, default {DEFAULT_MAX_WAIT_S:g})")
    p.add_argument("--log-level", default=os.environ.get("TRAILBEAM_LOG_LEVEL", "INFO"))
    a = p.parse_args(argv)
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
    if a.store_bodies:
        where = redact_url(a.store_url) if a.store_url else a.db
        logging.getLogger("trailbeam.server").warning(body_warning(a.store_bodies, where, token is not None))
    retention = Retention(a.retention, a.retention_queues)
    where = redact_url(a.store_url) if a.store_url else a.db
    try:
        if a.store_url and a.durability != "full":
            raise StoreUrlError("--durability relaxed is for the SQLite file; with Postgres, set its own "
                                "synchronous_commit instead")
        db = open_store(a.store_url, a.db, store_bodies=a.store_bodies, retention=retention,
                        durability=a.durability)
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
    httpd = make_server(db, a.host, a.port, stuck_after_s=a.stuck_after,
                        lambda_timeout_s=a.lambda_timeout, max_receive_counts=a.max_receive_count,
                        purger=purger, auth=auth.Auth(token) if token else None,
                        gate=IngestGate(a.ingest_max_pending, a.ingest_max_wait))
    if token:
        logging.getLogger("trailbeam.server").info("access token set: ingest, the API and the web screen need it")
    elif a.host not in ("127.0.0.1", "localhost", "::1"):
        logging.getLogger("trailbeam.server").warning(
            "no access token (TRAILBEAM_TOKEN) and listening on %s: anyone who can reach this port can send "
            "spans and read every timeline", a.host)
    logging.getLogger("trailbeam.server").info("listening on http://%s:%d (OTLP: /v1/traces, API: /api/, "
                                               "metrics: /metrics), db %s",
                                        a.host, httpd.server_address[1], where)
    # docker stop and systemd send SIGTERM; Python ignores it as PID 1 unless a handler is set. Stop the
    # same way as on Ctrl-C, so the SQLite file is closed cleanly.
    signal.signal(signal.SIGTERM, _interrupt)
    logging.getLogger("trailbeam.server").info("retention %s", retention.config())
    purger.start()
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        purger.stop()
        httpd.server_close()
        db.close()


class StoreUrlError(ValueError):
    pass


def open_store(url, db_path: str, *, store_bodies, retention, durability: str = "full"):
    """The Store: Postgres when ``url`` is a postgres:// one (its driver imported only then), else SQLite."""
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
        return PostgresStore(url, store_bodies=store_bodies, retention=retention)
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
        raise argparse.ArgumentTypeError(f"expected a port from 0 to 65535, got {text!r}")
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


def body_warning(store_bodies, db_path: str, token: bool = False) -> str:
    queues = "every queue" if "*" in store_bodies else "queues " + ", ".join(sorted(store_bodies))
    who = ("anyone with the access token, or who can read that file, can read those bodies." if token else
           "no access token is set (TRAILBEAM_TOKEN), so anyone who can reach this server, or read that file, can "
           "read those bodies. Keep it on a trusted network, or set a token.")
    return f"storing message bodies for {queues} (TRAILBEAM_STORE_BODIES or the config file) in {db_path}: {who}"


def _interrupt(signum, frame):
    raise KeyboardInterrupt


if __name__ == "__main__":
    main()
