"""The server under failure (QV-238): no acknowledged span lost or stored twice, and it recovers by itself.

The quick cases of bench/faults.py run here, each for a few seconds of steady ingest: SIGKILL mid-write and a
restart (both SQLite durabilities, and Postgres), every export sent twice (a client retry), and spans far out of
order from skewed clocks; on Postgres also two replicas with one killed. Each compares what was acknowledged
with what is stored, and what the API shows with a clean server given the same spans. The Postgres cases run
when TRAILBEAM_TEST_POSTGRES_URL is set. The slow ones (Postgres stopped for a minute, the disk filled for real,
Postgres restarted during a retention pass) need Docker: ``python bench/faults.py``, README "Fault tests".

A full disk is also tested here directly: SQLite's max_page_count gives the same SQLITE_FULL as a full file
system.
"""
import json
import os
import sqlite3
import sys
import urllib.error
import urllib.request
from pathlib import Path

import pytest

from spans import SEC, T0, Consumer, to_proto
from trailbeam.server.db import _disk_full

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "bench"))
import faults  # noqa: E402

PG = os.environ.get("TRAILBEAM_TEST_POSTGRES_URL")
QUICK = ["--phase-s", "2", "--kills", "2", "--sample", "60", "--workers", "3"]


def run_case(tmp_path, case, backend):
    rc = faults.main(["--case", case, "--backend", backend, *QUICK, "--out", str(tmp_path)])
    [r] = json.loads((tmp_path / "faults.json").read_text())
    assert r.get("ok") and rc == 0, json.dumps(r, indent=1, default=str)
    assert r["spans_acked"] > 0 and r["lost"] == 0 and r["doubled"] == 0
    return r


@pytest.mark.parametrize("case", ["kill9", "kill9-relaxed", "duplicate", "disorder"])
def test_sqlite_server_under_fault(tmp_path, case):
    run_case(tmp_path, case, "sqlite")


@pytest.mark.skipif(not PG, reason="TRAILBEAM_TEST_POSTGRES_URL is not set")
@pytest.mark.parametrize("case", ["kill9", "duplicate", "disorder", "replicas"])
def test_postgres_server_under_fault(tmp_path, case):
    run_case(tmp_path, case, "postgres")


# ---- a full disk -------------------------------------------------------------------------------------------

def post(url, data):
    req = urllib.request.Request(url + "/v1/traces", data=data, method="POST",
                                 headers={"Content-Type": "application/x-protobuf"})
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return r.status, dict(r.headers), r.read()
    except urllib.error.HTTPError as e:
        return e.code, dict(e.headers), e.read()


def export(first: int, n: int) -> bytes:
    c = Consumer()
    spans = []
    for i in range(first, first + n):
        spans += [c.send(T0 + i * SEC, f"m{i}"), c.receive(T0 + i * SEC + 1, f"m{i}", f"rh-{i}"),
                  c.delete(T0 + i * SEC + 2, f"rh-{i}", mid=f"m{i}")]
    return to_proto(spans).SerializeToString()


def test_a_full_disk_is_503_store_full_and_the_retry_is_stored_once(db, server):
    """Before QV-238 a full disk was answered 500, which OTLP exporters don't retry: the export was lost."""
    assert post(server, export(0, 5))[0] == 200
    pages = db.conn.execute("PRAGMA page_count").fetchone()[0]
    db.conn.execute(f"PRAGMA max_page_count = {pages}")  # no room for one more page
    big = export(100, 400)
    status, headers, _ = post(server, big)
    assert status == 503 and headers["Retry-After"] == "3"
    assert db.counts()["messages"] == 5  # nothing of it stored
    assert post(server, export(500, 1))[0] == 503  # every export, until there is space
    db.conn.execute("PRAGMA max_page_count = 1073741823")
    assert post(server, big)[0] == 200  # the exporter's retry
    assert post(server, big)[0] == 200  # and its duplicate, if the first answer was lost
    counts = db.counts()
    assert counts["messages"] == 405 and counts["spans"] == {"send": 405, "receive": 405, "delete": 405}


def test_a_full_disk_is_counted_as_rejected_store_full(db, server):
    db.conn.execute(f"PRAGMA max_page_count = {db.conn.execute('PRAGMA page_count').fetchone()[0]}")
    assert post(server, export(0, 400))[0] == 503
    with urllib.request.urlopen(server + "/metrics") as r:
        text = r.read().decode()
    assert 'trailbeam_ingest_rejected_requests_total{reason="store_full"} 1' in text


def test_disk_full_is_recognised_by_code_and_by_message():
    e = sqlite3.OperationalError("database or disk is full")
    assert _disk_full(e)
    e.sqlite_errorcode = 13  # SQLITE_FULL
    assert _disk_full(e)
    e2 = sqlite3.OperationalError("database is locked")
    e2.sqlite_errorcode = 5
    assert not _disk_full(e2) and not _disk_full(ValueError("database or disk is full"))
