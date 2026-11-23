# Benchmarks: load generator and harness

Two tools for measuring how far one Trailbeam server goes: a load generator that sends realistic
Trailbeam span traffic to any OTLP/HTTP URL, and a harness that seeds a database to a size, runs ingest
at increasing rates and times the read pages. No AWS and no SQS emulator: the generator writes the spans
the packages would emit (contract schema version 3) without making SQS calls.

Setup, from the repository root:

```sh
python -m venv .venv && . .venv/bin/activate
pip install -e 'python[test]'
```

## Load generator: `loadgen.py`

```sh
trailbeam --db /tmp/load.sqlite &                         # or any OTLP/HTTP traces URL
python python/bench/loadgen.py --url http://127.0.0.1:4318/v1/traces --rate 50 --duration 30
```

It prints a JSON result: target and achieved spans/s and messages/s, export p50/p99/max latency, errors
by HTTP status, and how far exports fell behind their schedule (`behind_p99_s`). Exports are open loop:
each one is due at a fixed time, so a slow server shows up as latency, lag and errors. It never shows up
as a lower rate that looks fine.

| Option | Default | What it varies |
|---|---|---|
| `--rate` | 100 | messages per second (each message is 2 to 12 spans; the result has the mean) |
| `--duration` | 30 | seconds |
| `--queues` | 10 | 1 to 500 queues (`bench-q000`…), each with a dead-letter queue `<queue>-dlq` |
| `--tenants` | 20 | tenants on `trailbeam.tenant`, Zipf-like (a few big ones, a long tail); 0 = none |
| `--body-size` | none | `none`, `1k`, `64k`, `256k`: a body captured on send and receive spans |
| `--body-queues` | all | bodies only on the first N queues. The server keeps them only for queues in its `--store-bodies` |
| `--redelivery` | 0.05 | per delivery: the visibility timeout expires and SQS delivers it again |
| `--visibility-change` | 0.05 | per delivery: released early with `ChangeMessageVisibility` 0 |
| `--dlq` | 0.01 | per message: fails `--max-receive-count` (3) deliveries and is received from the DLQ |
| `--stuck` | 0.01 | per message: never deleted (half never received) |
| `--lambda-consumer` | 0.2 | per message: consumed by a Lambda (receive with no start, outcome spans, no delete) |
| `--late-join` | 0 | per delivery: its settle spans are exported `--late-by-batches` (2) exports before its receive, by another process, with no message ID, so only the receipt-hash join links them |
| `--batch-spans` | 512 | spans per export, as `BatchSpanProcessor`. At low rates an export goes every 5 s, so batches are smaller |
| `--concurrency`, `--processes` | 4, 1 | concurrent exports per process, generator processes. One process makes about 30k spans/s without bodies, about 7k/s with 64 KB bodies |
| `--seed` | new each run | fixes the trace and span IDs. Span times are the time each span is generated, so a second run with the same seed is stored again: the server skips only the same span again (trace ID, span ID and end time, as an exporter retry sends it). The result records the seed used |
| `--sdk` | off | send through the packages' export path (`trailbeam._export.CountingSpanProcessor`, queue `TRAILBEAM_MAX_QUEUE_SIZE` or `--sdk-max-queue-size`) and `OTLPSpanExporter`, and count the spans lost, from the exporter's side: never exported (queue full) or in a failed export. It also reports what the drop counter (`trailbeam_dropped`) and its log lines (`logged_dropped`) said, which must equal `dropped_spans`. Against a Trailbeam server it also reads `/api/health` before and after for the spans received |
| `--sdk-processor` | `trailbeam` | `stock`: the SDK's `BatchSpanProcessor` with its defaults (queue 2,048) instead, as QV-68 measured |

Extra request headers for another endpoint (for example an ingest key):
`LOADGEN_HEADERS='Authorization=Bearer …' python python/bench/loadgen.py --url https://…/v1/traces`.

## Harness: `harness.py`

```sh
python python/bench/harness.py --out bench-results --sizes 10k,100k,1M,10M --fresh
```

For each size, smallest first, growing the same SQLite file from the previous size's seeded file:

1. **Seed** the file to the size, with messages spread over the last 7 days (`--seed-days`). By default:
   50 queues, 50 tenants, 1 KB bodies on 5 queues, which are the server's `--store-bodies`.
2. **Pages, idle.** Start `python -m trailbeam.server` (unchanged) on the file. Time Messages, Messages for
   one queue, message detail (20 random messages), Duplicates, Stuck, Tenants, tenant search and body
   search, each `--page-repeats` times (fewer if a page uses up `--page-budget-s`).
   A page that takes longer than `--page-timeout-s` (300) is recorded as a timeout. The harness then
   restarts the server, because the server keeps running that query, and every other request and export
   would wait behind it on the database lock. That page is skipped at larger sizes, which `summary.md`
   shows as "skipped". `--skip-pages stuck,…` leaves pages out from the start.
3. **Ingest ramp** at each of `--rates` messages/s for `--step-seconds`, until a step is not steady: errors,
   under 95% of the target, or export p99 above `--max-export-p99-ms` (2000). Records server CPU.
   Every step starts a fresh server on a copy of the seeded file, so the size measured is the size seeded:
   at a ceiling of thousands of spans/s one ramp writes more spans than a 10k or 100k file holds. That also
   drops a backlog of exports a step that was not steady leaves behind. Files above `--restore-max-gb` (4)
   are not copied (at 10M, 17.5 GB, a copy per step would also empty the page cache); their steps run on
   the growing file. Each step records `db_spans_before`, and `summary.md` shows it.
4. **Pages under ingest.** From the seeded file again, the pages in a loop, while ingest runs at the highest steady rate. The
   export latency of that ingest is recorded too.
5. **Above the ceiling** (`--overload-seconds`, 30; 0 skips it). From the seeded file again, the load
   generator at `--overload-factor` (2) times the first rate that was not steady, and the OTel SDK
   (`--sdk` mode, at the steady rate) alongside it, so its exporter meets a server that is already full.
   The load generator runs `--overload-concurrency` (16) exports per process here, not the ramp's
   `--concurrency`: a fleet of producers has more exports in flight than the server takes at once
   (`TRAILBEAM_INGEST_MAX_PENDING`, 16). With only 8 in flight the clients just slow down, so the backlog
   stays on the client side and the server is never full.
   Records the HTTP statuses each saw, the SDK exporter's retries and the `Retry-After` values it got, and
   the server's `/metrics` rejections. Then it checks every span: `acked_not_stored` is spans in a 200
   response that are not in the file. That is silent loss on the server side, and it must be 0.
   `stored_not_acked` is spans committed after the client gave up waiting. Their retry is a duplicate,
   which the server skips.
6. **Breakdown.** Server stopped, the seeded file restored: where a 512-span export's time goes in-process,
   averaged over 3 exports after one untimed warm-up export on a cold page cache (`cold_insert_ms`). That covers decode,
   `Database.insert` and its two join queries, plus the query plans of both joins (the server's own SQL,
   `db.SETTLE_JOIN_SQL` and `db.RECEIVE_JOIN_SQL`).

Then the file growth per million spans with no bodies and with 1 KB (stored and not), 64 KB and 256 KB
bodies (`--growth-spans`). The SDK drop count runs against the largest database (`--sdk-seconds`).

Output in `--out`: `results.json` (everything), `results.csv` (one row per measurement), `summary.md`, and
`server.log`. Results are not committed.

Seeding does the receipt-hash join in memory and writes rows in bulk instead of through the server's
`Database.insert`, so it is faster than ingest (before QV-70 the settle join scanned every stored receive,
and `insert` could not have seeded 1M spans at all). The seeded rows are the same rows `insert` writes,
column for column; `tests/test_bench_harness.py` checks that. The live ingest
in steps 3 and 4 goes through the real server.

Disk: about 1.9 GB per million spans, with or without 1 KB bodies on a tenth of the queues (QV-77); `summary.md` has the measured
figure. Seeding runs at about 25k spans/s on one core.

## CI regression check: `ci_check.py`

The `perf` job in `.github/workflows/ci.yml` runs this on every pull request (QV-77). It exists because of
QV-68: the settle join scanned every stored receive, inserts got 100 to 10,000 times slower, and no test
noticed. On a seeded 100k-span file (same traffic as the harness) it checks:

1. **Join plans.** `EXPLAIN QUERY PLAN` of `db.SETTLE_JOIN_SQL`, `db.RECEIVE_JOIN_FROM_SQL` and
   `db.RECEIVE_JOIN_SQL`, without statistics (as the server leaves the file) and after `ANALYZE`. Each must
   SEARCH `spans` by an index whose leading columns are `(receipt_hmac, key_id)`, with both bound. The rule
   reads the columns from `PRAGMA index_info`, so renaming an index doesn't break it.
2. **Insert.** In-process `Database.insert` of 20 exports of 512 spans: p95 per export and spans/s.
3. **Server.** The read pages idle (p95), then 20 s of ingest at 1,000 spans/s through `python -m
   trailbeam.server`: errors, achieved rate, export p99.

```sh
python python/bench/ci_check.py --out ci-perf                       # exit 1 on a regression
python python/bench/ci_check.py --out ci-perf-broken --break-plan   # exit 0 only if it catches a scan
```

`--break-plan` rewrites the three statements to `NOT INDEXED` in its own process (the QV-68 bug) and runs
steps 1 and 2. CI runs it on every PR, so each run shows that the check still fails on a scanning join.
`report.json` and `summary.md` go to `--out`, and the summary to the job's step summary.

Measured on the QV-70 machine (Ryzen 7 7700X, WSL2, SQLite 3.45.1), main at `e595f20`, and the limits set
from it. The limits are wide because the runners share a machine; the bug they are for is 10x or more.

| check | correct plan | broken plan | limit |
|---|---|---|---|
| insert p95 per 512-span export | 47.6 ms | 11,799 ms | ≤ 600 ms |
| insert, in-process | 11,833 spans/s | 44 spans/s | ≥ 1,500 spans/s |
| slowest checked page, idle p95 (Tenants) | 5.2 ms | not run | ≤ 300 ms |
| ingest at 1,000 spans/s: export p99 | 181 ms | not run | ≤ 1,000 ms |
| ingest: errors, achieved rate | 0, 1,000 spans/s | not run | 0, ≥ 950 spans/s |

Both passes take under a minute here (29 s and 41 s). Every limit is a flag (`--max-insert-p95-ms`,
`--min-insert-spans-per-s`, `--max-page-p95-ms`, `--max-export-p99-ms`). The body-search pages are bounded scans by
design and are left to the full harness.

## Postgres smoke run: `pg_smoke.py`

A short check of the Postgres store (QV-80), not the full harness: seed a database to a size through
`PostgresStore.insert`, start the server with `--store-url`, time the pages idle, then run ingest at a rate while
the pages are timed in a loop.

```sh
pip install -e 'python[test,postgres]'
createdb bench
python python/bench/pg_smoke.py --store-url postgresql://postgres@127.0.0.1:5432/bench --spans 1M --seed-only
python python/bench/pg_smoke.py --store-url postgresql://postgres@127.0.0.1:5432/bench --spans 1M \
    --spans-per-s 1000 --seconds 120 --out bench-results/pg-smoke
```

Seeding resumes where it stopped, so it can be split into runs. Same traffic as the harness (50 queues, 50
tenants, 1 KB bodies on 5 queues). Seeding leaves Postgres a checkpoint and autovacuum to do: measure after
they have finished, or the first minutes show their I/O as export latency.

Result on the QV-70 machine (Ryzen 7 7700X, WSL2, Postgres 16.2 on the same box, `fsync` and
`synchronous_commit` on, `shared_buffers=1GB`), 1.0 to 1.2M spans (2.6 to 3.1 GB), 120 s at 1,000 spans/s:
export p50 297 ms, p99 456 ms, no errors. List pages p95 under that ingest: Messages 5.4 ms, one queue 5.0, Duplicates 5.0,
Stuck 5.1, message detail 2.1, Tenants 298, tenant search 41, body search 124 (one queue 209). The first run
straight after seeding, during a 125 s checkpoint, had an export p99 of 4.1 s.

## Tenants page before and after: `tenants_page.py`

Seeds harness traffic (50 queues, 50 tenants, 1 KB bodies on 5 queues, 7 days) into a SQLite file or a
Postgres schema and times the Tenants page for the last hour, day and 7 days, ending at the newest span. Run it
with the old code first (`PYTHONPATH=<old checkout>/python/src`), then with the new code on the same data and
`--compare` the first run's JSON: the counts must be equal and p95 within the contract's 1 %. The second run
opens the store with the new code, so it times the schema upgrade too. `--insert N` then times N exports of
live traffic (it adds spans: run it on a copy). QV-128 measured the hourly tenant rollup with it at 1M spans.

`--current-hour N` first adds N spans of messages sent between the start of the current hour and now, as a heavy
ingest leaves them, and the ranges then end now, as the page's default does. Run it early in an hour: the spans
must all be added before the hour ends. With `--compare`, the second run asks exactly the first run's ranges.

**QV-224** (the current hour from the rollup), on the WSL2 desktop, Postgres 16 in Docker (`--memory 3g`,
`shared_buffers=1GB`), idle, p95 over 10 runs before and 30 after, same data and ranges:

```sh
python python/bench/tenants_page.py --store-url $PG --schema t224 --spans 2M --out base.json   # 7 days of traffic
PYTHONPATH=<main>/python/src python python/bench/tenants_page.py --store-url $PG --schema t224 \
  --current-hour 2.5M --out before.json   # run early in an hour; repeat until enough is in it
python python/bench/tenants_page.py --store-url $PG --schema t224 --out after.json --compare before.json
```

| store, data | range | before | after |
|---|---|---|---|
| Postgres, 3.32M tenant events, 2.06M of them in the current hour | last hour | 2,193 ms | 21 ms |
| same | last day | 2,070 ms | 55 ms |
| same | last 7 days | 2,299 ms | 248 ms |
| SQLite, 941K tenant events, 626K in the current hour | last day | 646 ms | 9.2 ms |

The answers matched (`--compare`). The page still reads raw events for the hour at the start of its range, up to
half an hour of them. Forcing that path for the 2.06M events of the current hour took 261 ms on Postgres (p95) and
314 ms for 626K events on SQLite (p50), against 2,070 ms and 646 ms when they were fetched into Python one by one.
