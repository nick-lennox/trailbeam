import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

from trailbeam.server.db import Database  # noqa: E402
from trailbeam.server.otlp import decode  # noqa: E402
from spans import to_proto  # noqa: E402


@pytest.fixture(params=["memory", "file"])
def db(request, tmp_path):
    """Every server test runs on both: ``:memory:`` (reads share the writer connection) and a WAL file
    (reads on separate read-only connections, as ``trailbeam --db``)."""
    s = Database(":memory:" if request.param == "memory" else str(tmp_path / "trailbeam.sqlite"))
    yield s
    s.close()


@pytest.fixture
def ingest(db):
    """Ingest spans as one OTLP protobuf request, the way the Collector or a package exports them."""
    def go(*spans, service="orders-worker"):
        d = decode(to_proto(list(spans), service=service).SerializeToString(), "application/x-protobuf")
        assert d.rejected == []
        return db.insert(d.records)
    return go


@pytest.fixture
def server(db):
    """Trailbeam running on a free port. Yields its base URL."""
    import threading

    from trailbeam.server.server import make_server

    httpd = make_server(db, "127.0.0.1", 0, ui_dir=None)  # the server-rendered pages; test_ui.py covers the shell
    t = threading.Thread(target=httpd.serve_forever, daemon=True)
    t.start()
    yield f"http://127.0.0.1:{httpd.server_address[1]}"
    httpd.shutdown()
    httpd.server_close()


@pytest.fixture(autouse=True)
def record_api_responses(monkeypatch):
    """Every API and ingest response a server test produces is checked against contract/openapi.yaml when
    the test ends (openapi_contract.py): a field, a status or a content type the file does not have fails
    the test that produced it. ``record_api_responses.seen`` holds what was answered."""
    from openapi_contract import Recorder

    rec = Recorder()
    rec.install(monkeypatch)
    yield rec
    if rec.problems:
        shown = "\n".join(rec.problems[:10])
        more = f"\n... and {len(rec.problems) - 10} more" if len(rec.problems) > 10 else ""
        pytest.fail(f"responses that contract/openapi.yaml does not allow:\n{shown}{more}", pytrace=False)
