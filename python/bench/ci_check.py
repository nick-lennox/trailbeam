"""CI performance regression check: the receipt-hash join plans, and a small harness pass with thresholds.

    python python/bench/ci_check.py --out ci-perf
    python python/bench/ci_check.py --out ci-perf-broken --break-plan   # must catch a broken plan

QV-68 found the settle join scanning every stored receive: inserts got 100 to 10,000 times slower and no test
noticed. This check fails the build on that kind of regression, on a ~100k-span file:

1. **Plans.** ``EXPLAIN QUERY PLAN`` of the server's join statements (``db.SETTLE_JOIN_SQL``,
   ``db.RECEIVE_JOIN_FROM_SQL``, ``db.RECEIVE_JOIN_SQL``) on the seeded file, as the server leaves it (no
   statistics) and again after ``ANALYZE``. Each must only SEARCH ``spans`` by an index whose leading columns
   are ``(receipt_hmac, key_id)``, with both columns bound. Any SCAN of ``spans`` fails. The rule is read from
   the schema (``PRAGMA index_info``), not from index names, so renaming or splitting the hash index (QV-78)
   doesn't need a change here.
2. **Insert.** In-process ``Database.insert`` of ``--exports`` exports of 512 generated spans on a copy of the
   seeded file, after one untimed warm-up export: p95 ms per export and spans/s.
3. **Server.** ``python -m trailbeam.server`` on another copy: the read pages idle (p95), then the load
   generator at ``--ingest-spans-per-s`` for ``--ingest-seconds`` (errors, achieved rate, export p99).

The thresholds are set far from today's numbers: the runners are shared machines, and the regression this is
for is 10x or more. ``--break-plan`` rewrites the join statements in this process to ``NOT INDEXED`` (a scan)
and runs steps 1 and 2. It exits 0 only if both the plan check and the insert threshold failed, so CI shows on
every run that the check still catches the QV-68 bug.

Output: ``report.json`` and ``summary.md`` in ``--out``; the summary also goes to ``$GITHUB_STEP_SUMMARY``.
Exit status 1 if a check failed.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sqlite3
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import harness  # noqa: E402
import loadgen  # noqa: E402

from trailbeam.server import db as dbmod  # noqa: E402
from trailbeam.server.otlp import decode  # noqa: E402

JOIN_STATEMENTS = ("SETTLE_JOIN_SQL", "RECEIVE_JOIN_FROM_SQL", "RECEIVE_JOIN_SQL")
HASH_COLUMNS = ["receipt_hmac", "key_id"]
PLAN_ROW = re.compile(r"^(SCAN|SEARCH) (\w+)(?: AS \w+)?(?: USING (?:COVERING )?INDEX (\w+))?(?: \((.*)\))?")

# Pages the check times. The body-search pages in harness.page_paths are bounded scans by design and are measured
# by the full harness; their regressions are not what this check is for.
PAGES = ("messages", "messages_one_queue", "message_detail", "duplicates", "stuck", "tenants", "tenant_search")


def join_statements() -> dict:
    return {n: getattr(dbmod, n) for n in JOIN_STATEMENTS if hasattr(dbmod, n)}


def break_plans() -> None:
    """Make every join statement scan: the QV-68 bug, in this process only."""
    for name, sql in join_statements().items():
        setattr(dbmod, name, re.sub(r"INDEXED BY \w+", "NOT INDEXED", sql))


def check_plan(conn: sqlite3.Connection, sql: str) -> dict:
    """The plan of one join statement, and why it fails (empty if it passes)."""
    rows = [r[3] for r in conn.execute("EXPLAIN QUERY PLAN " + sql, (None,) * sql.count("?"))]
    problems, searched = [], False
    for detail in rows:
        m = PLAN_ROW.match(detail)
        if not m or m.group(2) != "spans":
            continue  # e.g. USE TEMP B-TREE FOR ORDER BY: a few rows, after the lookup
        verb, _, index, cond = m.groups()
        if verb == "SCAN":
            problems.append(f"scans spans: {detail}")
            continue
        cols = [r[2] for r in conn.execute(f"PRAGMA index_info({index})")] if index else []
        if cols[:2] != HASH_COLUMNS:
            problems.append(f"searches spans by {index or 'rowid'} {cols}, not (receipt_hmac, key_id): {detail}")
        elif cond is None or not all(f"{c}=?" in cond for c in HASH_COLUMNS):
            problems.append(f"does not bind receipt_hmac and key_id: {detail}")
        else:
            searched = True
    if not searched and not problems:
        problems.append("no SEARCH of spans in the plan")
    return {"plan": "; ".join(rows), "problems": problems}


def plans(path: str) -> dict:
    """Every join statement's plan on ``path`` as the server leaves it, and after ANALYZE (on that file)."""
    out = {}
    db = dbmod.Database(path)  # applies the schema and migrations, as the server would
    db.close()
    conn = sqlite3.connect(path)
    try:
        for stats in ("no statistics", "after ANALYZE"):
            if stats == "after ANALYZE":
                conn.execute("ANALYZE")
            for name, sql in join_statements().items():
                out[f"{name}, {stats}"] = check_plan(conn, sql)
    finally:
        conn.close()
    return out


def insert_timing(path: str, profile: loadgen.Profile, store, exports: int) -> dict:
    """Database.insert of ``exports`` 512-span exports on ``path``, after one untimed warm-up export."""
    db = dbmod.Database(path, store_bodies=store)
    stream = loadgen.Stream(profile, seed=4242 + harness.span_count(path))
    lat, spans = [], 0
    try:
        db.insert(decode(loadgen.encode(stream.batch()), "application/x-protobuf").records)
        for _ in range(exports):
            records = decode(loadgen.encode(stream.batch()), "application/x-protobuf").records
            s = time.perf_counter()
            db.insert(records)
            lat.append(time.perf_counter() - s)
            spans += len(records)
    finally:
        db.close()
    return {"exports": exports, "spans_per_export": spans // max(exports, 1),
            "p50_ms": harness._ms(loadgen.percentile(lat, 0.5)), "p95_ms": harness._ms(loadgen.percentile(lat, 0.95)),
            "spans_per_s": round(spans / sum(lat), 1) if lat else None}


def server_pass(path: str, profile: loadgen.Profile, store, args, log_path: str) -> dict:
    srv = harness.Server(path, store_bodies=store, log_path=log_path)
    try:
        pages = {k: v for k, v in harness.page_paths(path, profile).items() if k in PAGES}
        idle = harness.time_pages(srv.port, pages, repeats=args.page_repeats, budget_s=30, timeout_s=60)
        rate = args.ingest_spans_per_s / profile.mean_spans_per_message()
        ingest = loadgen.run(srv.otlp, profile, rate, args.ingest_seconds, concurrency=4, processes=1,
                             timeout=10, grace=10)
        for k in ("profile", "processes", "concurrency", "seed"):
            ingest.pop(k, None)
    finally:
        srv.stop()
    return {"pages_idle": idle, "ingest": ingest}


def verdicts(report: dict, args) -> list[dict]:
    """One row per check: name, measured value, limit, passed."""
    out = []

    def add(name, value, limit, ok, unit=""):
        out.append({"check": name, "value": value, "limit": limit, "unit": unit, "passed": bool(ok)})

    for name, p in report["plans"].items():
        add(f"plan: {name}", "; ".join(p["problems"]) or p["plan"], "SEARCH spans by (receipt_hmac, key_id)",
            not p["problems"])
    ins = report["insert"]
    add("insert: p95 per 512-span export", ins["p95_ms"], args.max_insert_p95_ms,
        ins["p95_ms"] is not None and ins["p95_ms"] <= args.max_insert_p95_ms, "ms")
    add("insert: spans/s in-process", ins["spans_per_s"], args.min_insert_spans_per_s,
        (ins["spans_per_s"] or 0) >= args.min_insert_spans_per_s, "spans/s (min)")
    srv = report.get("server")
    if srv:
        for page, r in srv["pages_idle"].items():
            add(f"page idle p95: {page}", r["p95_ms"], args.max_page_p95_ms,
                r["p95_ms"] is not None and r["p95_ms"] <= args.max_page_p95_ms and not r["errors"]
                and not r["timeouts"], "ms")
        ing = srv["ingest"]
        add("ingest: errors", ing["errors"], 0, ing["errors"] == 0)
        add("ingest: achieved spans/s", ing["achieved_spans_per_s"], round(0.95 * ing["target_spans_per_s"], 1),
            ing["achieved_spans_per_s"] >= 0.95 * ing["target_spans_per_s"], "spans/s (min)")
        add("ingest: export p99", ing["export_p99_ms"], args.max_export_p99_ms,
            ing["export_p99_ms"] is not None and ing["export_p99_ms"] <= args.max_export_p99_ms, "ms")
    return out


def summary(report: dict) -> str:
    failed = [v for v in report["checks"] if not v["passed"]]
    if report["break_plan"]:
        head = ("### Perf check on a deliberately broken plan: " +
                ("caught (plan and insert checks failed, as they must)" if report["caught"] else
                 "NOT caught: the check no longer detects a scanning join"))
    else:
        head = f"### Perf regression check: {'passed' if not failed else f'{len(failed)} failed'}"
    lines = [head, "", f"{report['spans']:,} spans, {report['file_mb']} MB, seeded in {report['seed_seconds']} s. "
             f"SQLite {sqlite3.sqlite_version}, Python {sys.version.split()[0]}.", "",
             "| check | value | limit | |", "|---|---|---|---|"]
    for v in report["checks"]:
        value = str(v["value"]).replace("|", "\\|")
        lines.append(f"| {v['check']} | {value} | {v['limit']} {v['unit']} | {'ok' if v['passed'] else '**FAIL**'} |")
    return "\n".join(lines) + "\n"


def run(args) -> dict:
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    work = Path(args.workdir or tempfile.mkdtemp(prefix="tb-ci-perf-"))
    work.mkdir(parents=True, exist_ok=True)
    seeded = str(work / "seeded.sqlite")
    for p in (seeded, seeded + "-wal", seeded + "-shm"):
        if os.path.exists(p):
            os.remove(p)
    profile = loadgen.Profile(queues=50, tenants=50, body_size=1024, body_queues=5)
    store = tuple(loadgen.body_queue_names(profile))
    if args.break_plan:
        break_plans()
    sd = harness.seed(seeded, harness.parse_size(args.spans), profile, store_bodies=store, days=7)
    report = {"spans": sd["spans"], "seed_seconds": sd["seconds"], "file_mb": round(harness.file_bytes(seeded) / 1e6, 1),
              "break_plan": args.break_plan, "statements": join_statements()}

    def copy(name):
        dst = str(work / name)
        harness.restore_file(seeded, dst)
        return dst

    report["plans"] = plans(copy("plans.sqlite"))
    report["insert"] = insert_timing(copy("insert.sqlite"), profile, store, args.exports)
    if not (args.break_plan or args.no_server):
        report["server"] = server_pass(copy("server.sqlite"), profile, store, args, str(out / "server.log"))
    report["checks"] = verdicts(report, args)
    failed = {v["check"].split(":")[0] for v in report["checks"] if not v["passed"]}
    report["caught"] = {"plan", "insert"} <= failed
    report["passed"] = report["caught"] if args.break_plan else not failed
    (out / "report.json").write_text(json.dumps(report, indent=2))
    text = summary(report)
    (out / "summary.md").write_text(text)
    if os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a") as f:
            f.write(text + "\n")
    if not args.workdir:
        for p in work.iterdir():
            p.unlink()
        work.rmdir()
    return report


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="Fail on a join-plan or ingest performance regression (QV-77).")
    p.add_argument("--out", default="ci-perf", help="directory for report.json, summary.md, server.log")
    p.add_argument("--workdir", default=None, help="directory for the SQLite files (default: a temporary one, removed)")
    p.add_argument("--spans", default="100k", help="spans to seed")
    p.add_argument("--exports", type=int, default=None,
                   help="512-span exports timed in-process (default 20; 2 with --break-plan, where each takes ~25 s)")
    p.add_argument("--page-repeats", type=int, default=10)
    p.add_argument("--ingest-spans-per-s", type=float, default=1000, help="the small-tier SLO rate")
    p.add_argument("--ingest-seconds", type=float, default=20)
    p.add_argument("--no-server", action="store_true", help="plans and in-process insert only")
    p.add_argument("--break-plan", action="store_true",
                   help="make the joins scan (in this process) and succeed only if the check catches it")
    # Limits, from the numbers in bench/README.md ("CI regression check"): 5 to 7 times today's, because the
    # runners share a machine, and still 10 to 70 times better than the QV-68 bug measures.
    p.add_argument("--max-insert-p95-ms", type=float, default=600)
    p.add_argument("--min-insert-spans-per-s", type=float, default=1500)
    p.add_argument("--max-page-p95-ms", type=float, default=300, help="the small-tier SLO's list-page p95")
    p.add_argument("--max-export-p99-ms", type=float, default=1000)
    args = p.parse_args(argv)
    if args.exports is None:
        args.exports = 2 if args.break_plan else 20
    report = run(args)
    print(summary(report))
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
