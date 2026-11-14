"""qv2-store command line: ``qv2-store --db qv2.sqlite --host 127.0.0.1 --port 4318``."""
from __future__ import annotations

import argparse
import logging
import os

from .db import Store
from .server import make_server


def main(argv=None) -> None:
    p = argparse.ArgumentParser(prog="qv2-store", description="qv2 store: OTLP/HTTP ingest and JSON API.")
    p.add_argument("--db", default=os.environ.get("QV2_STORE_DB", "qv2.sqlite"),
                   help="SQLite file (env QV2_STORE_DB, default qv2.sqlite)")
    p.add_argument("--host", default=os.environ.get("QV2_STORE_HOST", "127.0.0.1"),
                   help="listen address (env QV2_STORE_HOST, default 127.0.0.1; use 0.0.0.0 in a container)")
    p.add_argument("--port", type=int, default=int(os.environ.get("QV2_STORE_PORT", "4318")),
                   help="listen port (env QV2_STORE_PORT, default 4318)")
    p.add_argument("--stuck-after", type=int, default=int(os.environ.get("QV2_STUCK_AFTER_S", "300")),
                   help="seconds after which a message without a final delete is stuck (env QV2_STUCK_AFTER_S, default 300)")
    p.add_argument("--lambda-timeout", type=int, default=int(os.environ.get("QV2_LAMBDA_TIMEOUT_S", "900")),
                   help="seconds after which a Lambda receive without an outcome is inferred killed "
                        "(env QV2_LAMBDA_TIMEOUT_S, default 900, the Lambda maximum)")
    p.add_argument("--log-level", default=os.environ.get("QV2_STORE_LOG_LEVEL", "INFO"))
    a = p.parse_args(argv)
    logging.basicConfig(level=a.log_level.upper(), format="%(asctime)s %(levelname)s %(name)s %(message)s")
    store = Store(a.db)
    httpd = make_server(store, a.host, a.port, stuck_after_s=a.stuck_after,
                        lambda_timeout_s=a.lambda_timeout)
    logging.getLogger("qv2_store").info("listening on http://%s:%d (OTLP: /v1/traces, API: /api/), db %s",
                                        a.host, httpd.server_address[1], a.db)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.server_close()
        store.close()


if __name__ == "__main__":
    main()
