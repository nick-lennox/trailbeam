"""The trailbeam command, which runs Trailbeam: ``trailbeam --db trailbeam.sqlite --host 127.0.0.1 --port 4318``."""
from __future__ import annotations

import argparse
import logging
import os
import signal
import sqlite3
import sys
import time

from . import config
from .db import SqliteStore, parse_queue_list
from .migrations import SchemaError
from .retention import DEFAULT as DEFAULT_RETENTION, Purger, Retention, parse_duration, parse_queue_retentions
from .server import make_server


def main(argv=None) -> None:
    p = argparse.ArgumentParser(prog="trailbeam", description="Trailbeam: OTLP/HTTP ingest, JSON API and web screen on one port.")
    p.add_argument("--db", default=os.environ.get("TRAILBEAM_DB", "trailbeam.sqlite"),
                   help="SQLite file (env TRAILBEAM_DB, default trailbeam.sqlite)")
    p.add_argument("--host", default=os.environ.get("TRAILBEAM_HOST", "127.0.0.1"),
                   help="listen address (env TRAILBEAM_HOST, default 127.0.0.1; use 0.0.0.0 in a container)")
    p.add_argument("--port", type=int, default=int(os.environ.get("TRAILBEAM_PORT", "4318")),
                   help="listen port (env TRAILBEAM_PORT, default 4318)")
    p.add_argument("--stuck-after", type=int, default=int(os.environ.get("TRAILBEAM_STUCK_AFTER_S", "300")),
                   help="seconds after which a message without a final delete is stuck (env TRAILBEAM_STUCK_AFTER_S, default 300)")
    p.add_argument("--lambda-timeout", type=int, default=int(os.environ.get("TRAILBEAM_LAMBDA_TIMEOUT_S", "900")),
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
    p.add_argument("--purge-interval", type=float, default=float(os.environ.get("TRAILBEAM_PURGE_INTERVAL_S", "600")),
                   help="seconds between retention passes; the first runs at start (env TRAILBEAM_PURGE_INTERVAL_S, "
                        "default 600)")
    p.add_argument("--purge-batch", type=int, default=int(os.environ.get("TRAILBEAM_PURGE_BATCH", "50")),
                   help="messages looked at per purge step, each one write transaction: smaller steps hold the "
                        "writer lock for less time (env TRAILBEAM_PURGE_BATCH, default 50)")
    p.add_argument("--purge-duty", type=float, default=float(os.environ.get("TRAILBEAM_PURGE_DUTY", "0.25")),
                   help="the most of the time the purge holds the writer lock, in (0, 1] "
                        "(env TRAILBEAM_PURGE_DUTY, default 0.25)")
    p.add_argument("--vacuum", action="store_true",
                   help="rewrite the database file once so that retention gives deleted space back to the disk "
                        "(files created before retention, schema 8), then exit. Stop the server first; needs free disk "
                        "for a second copy of the file")
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
    if a.store_bodies:
        logging.getLogger("trailbeam.server").warning(body_warning(a.store_bodies, a.db))
    try:
        db = SqliteStore(a.db, store_bodies=a.store_bodies)
    except SchemaError as e:
        logging.getLogger("trailbeam.server").error("%s", e)
        sys.exit(2)
    if db.migrated:
        logging.getLogger("trailbeam.server").info("upgraded %s to schema version %d", a.db, db.migrated[-1])
    if a.vacuum:
        sys.exit(vacuum(db, a.db))
    retention = Retention(a.retention, a.retention_queues)
    purger = Purger(db, retention, interval_s=a.purge_interval, batch=a.purge_batch, duty=a.purge_duty)
    if db.storage()["auto_vacuum"] != "incremental" and not retention.keeps_everything():
        logging.getLogger("trailbeam.server").info(
            "%s was created without incremental vacuum: retention deletes old spans and new spans reuse the "
            "space, but the file does not shrink. To give the space back, stop the server and run trailbeam "
            "--db %s --vacuum once (it rewrites the file)", a.db, a.db)
    httpd = make_server(db, a.host, a.port, stuck_after_s=a.stuck_after,
                        lambda_timeout_s=a.lambda_timeout, max_receive_counts=a.max_receive_count,
                        purger=purger)
    logging.getLogger("trailbeam.server").info("listening on http://%s:%d (OTLP: /v1/traces, API: /api/), db %s",
                                        a.host, httpd.server_address[1], a.db)
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


def body_warning(store_bodies, db_path: str) -> str:
    queues = "every queue" if "*" in store_bodies else "queues " + ", ".join(sorted(store_bodies))
    return (f"storing message bodies for {queues} (TRAILBEAM_STORE_BODIES or the config file) in {db_path}. Trailbeam has no "
            "logins: anyone who can reach this server, or read that file, can read those bodies. Keep it on "
            "a trusted network.")


def _interrupt(signum, frame):
    raise KeyboardInterrupt


if __name__ == "__main__":
    main()
