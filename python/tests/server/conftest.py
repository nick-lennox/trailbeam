import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

from trailbeam.server.db import Store  # noqa: E402
from trailbeam.server.otlp import decode  # noqa: E402
from spans import to_proto  # noqa: E402


@pytest.fixture
def store():
    s = Store(":memory:")
    yield s
    s.close()


@pytest.fixture
def ingest(store):
    """Ingest spans as one OTLP protobuf request, the way the Collector or a package exports them."""
    def go(*spans, service="orders-worker"):
        d = decode(to_proto(list(spans), service=service).SerializeToString(), "application/x-protobuf")
        assert d.rejected == []
        return store.insert(d.records)
    return go


@pytest.fixture
def server(store):
    """Trailbeam running on a free port. Yields its base URL."""
    import threading

    from trailbeam.server.server import make_server

    httpd = make_server(store, "127.0.0.1", 0)
    t = threading.Thread(target=httpd.serve_forever, daemon=True)
    t.start()
    yield f"http://127.0.0.1:{httpd.server_address[1]}"
    httpd.shutdown()
    httpd.server_close()
