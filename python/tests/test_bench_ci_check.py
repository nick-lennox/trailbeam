# SPDX-License-Identifier: AGPL-3.0-only (imports the server; LICENSING.md)
"""The CI perf regression check (bench/ci_check.py): its plan rule, and that it passes on the server's SQL and
fails on a scanning join."""
import sqlite3
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "bench"))

import ci_check  # noqa: E402

from trailbeam.server import db as dbmod  # noqa: E402


@pytest.fixture
def conn():
    c = sqlite3.connect(":memory:")
    c.executescript("""CREATE TABLE spans (row INTEGER PRIMARY KEY, kind TEXT, receipt_hmac TEXT, key_id TEXT,
                                          time_ns INTEGER, mid TEXT);
                       CREATE INDEX by_hash ON spans (receipt_hmac, key_id);
                       CREATE INDEX by_hash_only ON spans (receipt_hmac);
                       CREATE INDEX by_kind ON spans (kind, time_ns);""")
    yield c
    c.close()


def test_a_search_by_the_hash_index_passes(conn):
    r = ci_check.check_plan(conn, "SELECT row FROM spans INDEXED BY by_hash WHERE receipt_hmac = ? AND key_id = ?")
    assert r["problems"] == [] and "by_hash" in r["plan"]


@pytest.mark.parametrize("sql, why", [
    ("SELECT row FROM spans NOT INDEXED WHERE receipt_hmac = ? AND key_id = ?", "scans spans"),
    ("SELECT row FROM spans INDEXED BY by_kind WHERE kind = 'receive' AND receipt_hmac = ? AND key_id = ?",
     "not (receipt_hmac, key_id)"),
    ("SELECT row FROM spans INDEXED BY by_hash_only WHERE receipt_hmac = ? AND key_id = ?",
     "not (receipt_hmac, key_id)"),
    ("SELECT row FROM spans INDEXED BY by_hash WHERE receipt_hmac = ?", "does not bind"),
])
def test_anything_else_fails(conn, sql, why):
    problems = ci_check.check_plan(conn, sql)["problems"]
    assert problems and why in problems[0]


def test_the_servers_joins_pass_and_break_plans_makes_them_fail(tmp_path, monkeypatch):
    path = str(tmp_path / "t.sqlite")
    assert set(ci_check.join_statements()) == set(ci_check.JOIN_STATEMENTS)
    assert all(not p["problems"] for p in ci_check.plans(path).values())
    for name, sql in ci_check.join_statements().items():
        monkeypatch.setattr(dbmod, name, sql)  # restored after the test
    ci_check.break_plans()
    broken = ci_check.plans(path)
    assert broken and all(p["problems"] and "scans spans" in p["problems"][0] for p in broken.values())
