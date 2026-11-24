"""The benchmark harness (bench/harness.py): its seeding writes the rows the server's insert writes, and a
small run completes and writes its result files."""
import json
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "bench"))

import harness  # noqa: E402
import loadgen  # noqa: E402

from trailbeam.server import summary_table  # noqa: E402
from trailbeam.server.db import Database  # noqa: E402

PROFILE = loadgen.Profile(queues=6, tenants=4, body_size=300, body_queues=2, redelivery=0.2, visibility_change=0.2,
                          dlq=0.1, stuck=0.1, lambda_consumer=0.3, late_join=0.4, batch_spans=40)


def rows(path, table="spans"):
    c = sqlite3.connect(path)
    c.row_factory = sqlite3.Row
    out = sorted((dict(r) for r in c.execute(f"SELECT * FROM {table}")), key=lambda r: [str(v) for v in r.values()])
    c.close()
    return out


def test_seeding_writes_the_rows_insert_writes(tmp_path):
    store = loadgen.body_queue_names(PROFILE)
    by_insert, by_seed = str(tmp_path / "insert.sqlite"), str(tmp_path / "seed.sqlite")
    db = Database(by_insert, store_bodies=store)
    s = harness.Seeder(by_seed, store)
    for batch in loadgen.Stream(PROFILE, seed=9).batches_of_messages(400):
        recs = loadgen.records(batch)
        db.insert(recs)
        s.write(loadgen.records(batch))
    db.close()
    s.close()
    a, b = rows(by_insert), rows(by_seed)
    assert len(a) > 1000
    assert a == b
    for t in summary_table.TABLES + ("bodies", "step_rollup"):  # and the summary tables, bodies and steps insert keeps
        assert rows(by_insert, t) == rows(by_seed, t), t
    assert rows(by_seed, "bodies") and any(r["body_id"] for r in a)
    assert any(r["mid_source"] == "receipt_hash" for r in a) and any(r["body_dropped"] for r in a)


def test_seed_grows_a_file_in_steps(tmp_path):
    path = str(tmp_path / "grow.sqlite")
    first = harness.seed(path, 3000, PROFILE, log=lambda *_: None)
    assert first["spans"] >= 3000
    second = harness.seed(path, 6000, PROFILE, log=lambda *_: None)
    assert second["spans"] >= 6000 and second["added"] > 0
    assert harness.span_count(path) == second["spans"]
    assert Database(path).counts()["unjoined_settles"] == 0


def test_a_small_run(tmp_path):
    out = tmp_path / "out"
    harness.main(["--out", str(out), "--sizes", "2k", "--rates", "2,5", "--step-seconds", "2",
                  "--under-load-seconds", "2", "--page-repeats", "2", "--growth-spans", "2000",
                  "--sdk-seconds", "1", "--processes", "1", "--queues", "5", "--tenants", "3", "--body-queues", "2",
                  "--overload-seconds", "6"])
    res = json.loads((out / "results.json").read_text())
    size = res["sizes"][0]
    assert size["spans"] >= 2000
    assert set(size["pages_idle"]) == {"messages", "messages_one_queue", "message_detail", "duplicates", "stuck",
                                       "tenants", "tenant_search", "body_search", "body_search_one_queue", "body_search_no_match"}
    assert all(v["errors"] == 0 for v in size["pages_idle"].values())
    assert size["ingest"][0]["errors"] == 0
    assert size["breakdown"]["insert_ms"] > 0
    assert size["sdk"]["emitted_spans"] > 0
    ov = size["overload"]  # QV-74: every span the server answered 200 for is stored
    assert ov["acked_not_stored"] == 0 and ov.get("server_inserted_not_stored") == 0
    assert ov["sdk"]["http_attempts"] >= ov["sdk"]["export_calls"] > 0
    assert "## Above the ceiling" in (out / "summary.md").read_text()
    assert len(res["growth"]) == 5
    assert "## Ingest" in (out / "summary.md").read_text()
    assert (out / "results.csv").read_text().startswith("db_spans")


def test_a_page_that_times_out_restarts_the_server_and_is_skipped_at_larger_sizes(tmp_path, monkeypatch):
    out = tmp_path / "out"
    real_get, real_init, started = harness.get, harness.Server.__init__, []

    def slow_pages(port, path, timeout=300):  # every page times out; the start-up probe does not
        if path == "/api/none":
            return real_get(port, path, timeout)
        raise TimeoutError

    def counted(self, *a, **kw):
        started.append(1)
        real_init(self, *a, **kw)

    monkeypatch.setattr(harness, "get", slow_pages)
    monkeypatch.setattr(harness.Server, "__init__", counted)
    harness.main(["--out", str(out), "--sizes", "1k,1500", "--rates", "2", "--step-seconds", "1",
                  "--under-load-seconds", "1", "--page-repeats", "2", "--skip-pages", "tenants",
                  "--growth-spans", "0", "--sdk-seconds", "0", "--overload-seconds", "0", "--processes", "1",
                  "--queues", "3", "--tenants", "2", "--body-queues", "1"])
    res = json.loads((out / "results.json").read_text())
    first, second = res["sizes"]
    assert "tenants" not in first["pages_idle"] and first["pages_skipped"] == {"tenants": "--skip-pages"}
    assert all(v["timeouts"] == 1 and v["n"] == 1 for v in first["pages_idle"].values())
    # Per size: its first server, one restart per timed-out page (first size only), then one server for each
    # ramp step and one for the run under pages, each on the restored seeded file.
    assert len(started) == (1 + 9 + 2) + (1 + 2)
    assert first["ingest"][0]["errors"] == 0  # the ramp ran against a fresh server
    assert second["pages_idle"] == {} and second["pages_under_ingest"] == {}
    assert set(second["pages_skipped"]) == {"messages", "messages_one_queue", "message_detail", "duplicates",
                                            "stuck", "tenants", "tenant_search", "body_search", "body_search_one_queue", "body_search_no_match"}
    assert "skipped" in (out / "summary.md").read_text()


def test_every_ingest_step_starts_from_the_seeded_file(tmp_path):
    """At thousands of spans/s one ramp writes more spans than a small database holds: each step, the run
    under pages and the next size must start from the seeded file, not from what earlier steps added."""
    out = tmp_path / "out"
    harness.main(["--out", str(out), "--sizes", "2k,4k", "--rates", "20,40", "--step-seconds", "2",
                  "--under-load-seconds", "2", "--page-repeats", "1", "--growth-spans", "0",
                  "--sdk-seconds", "0", "--overload-seconds", "0", "--processes", "1", "--queues", "5", "--tenants", "3",
                  "--body-queues", "2"])
    res = json.loads((out / "results.json").read_text())
    small, big = res["sizes"]
    assert small["steps_restore_seeded_file"]
    assert [s["db_spans_before"] for s in small["ingest"]] == [small["spans"]] * len(small["ingest"])
    assert small["ingest_under_pages_db_spans_before"] == small["spans"]
    assert big["spans"] < small["spans"] + 4000 // 2 + 3 * 512 + 600  # seeded to 4k plus the breakdown's exports
    assert small["breakdown"]["cold_insert_ms"] > 0
    assert not (tmp_path / "out" / "bench.seeded.sqlite").exists()
