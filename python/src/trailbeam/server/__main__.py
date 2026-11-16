"""The trailbeam command, which runs Trailbeam: ``trailbeam --db trailbeam.sqlite --host 127.0.0.1 --port 4318``."""
from __future__ import annotations

import argparse
import logging
import os
import signal

from .db import Store
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
    p.add_argument("--max-receive-count", type=_max_receive_counts,
                   default=_max_receive_counts(os.environ.get("TRAILBEAM_MAX_RECEIVE_COUNT", "")),
                   help="QUEUE=N[,QUEUE=N...]: the maxReceiveCount of each queue's redrive policy, so a timeline "
                        "can say when a message should have moved to its dead-letter queue but no receive "
                        "there has reported it (env TRAILBEAM_MAX_RECEIVE_COUNT; spans don't carry it)")
    p.add_argument("--log-level", default=os.environ.get("TRAILBEAM_LOG_LEVEL", "INFO"))
    a = p.parse_args(argv)
    logging.basicConfig(level=a.log_level.upper(), format="%(asctime)s %(levelname)s %(name)s %(message)s")
    store = Store(a.db)
    httpd = make_server(store, a.host, a.port, stuck_after_s=a.stuck_after,
                        lambda_timeout_s=a.lambda_timeout, max_receive_counts=a.max_receive_count)
    logging.getLogger("trailbeam.server").info("listening on http://%s:%d (OTLP: /v1/traces, API: /api/), db %s",
                                        a.host, httpd.server_address[1], a.db)
    # docker stop and systemd send SIGTERM; Python ignores it as PID 1 unless a handler is set. Stop the
    # same way as on Ctrl-C, so the SQLite file is closed cleanly.
    signal.signal(signal.SIGTERM, _interrupt)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.server_close()
        store.close()


def _max_receive_counts(text: str) -> dict:
    """``orders=3,payments=5`` -> {"orders": 3, "payments": 5}."""
    out = {}
    for part in filter(None, (p.strip() for p in text.split(","))):
        name, sep, n = part.partition("=")
        if not sep or not name.strip() or not n.strip().isdigit() or not 1 <= int(n) <= 1000:
            raise argparse.ArgumentTypeError(f"expected QUEUE=N with N from 1 to 1000, got {part!r}")
        out[name.strip()] = int(n)
    return out


def _interrupt(signum, frame):
    raise KeyboardInterrupt


if __name__ == "__main__":
    main()
