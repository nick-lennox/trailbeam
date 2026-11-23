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

## Postgres with several replicas: `pg_medium.py`

The medium-tier run (QV-82): `--replicas` servers on one Postgres, each sent an equal share of the rate by its
own load generator, as a load balancer would. Subcommands: `seed` (parallel, resumable), `ramp` (ingest at each
of `--spans-per-s` for `--step-seconds`), `pages` (the pages idle, then in a loop under ingest), `retention`
(a partition drop while ingest runs) and `state`. Each appends to `--out`/results.jsonl. The module docstring
has the commands.

### 4M and 7M spans, before and after QV-127 (QV-180)

QV-82 found ingest falling off between 1M and 4M spans on our desktop. QV-127 then time-bucketed the two lookup
tables (Postgres schema 5). QV-180 repeated QV-82's 4M and 7M runs with the same settings on `main` at `3559c67`.

**Same as QV-82:** the shared WSL2 desktop (Ryzen 7 7700X, 16 threads, 11.7 GB RAM, virtual disk). Postgres 16.16
in Docker with `--memory 5g`, `shared_buffers=1GB`, `max_wal_size=4GB`, `checkpoint_timeout=15min` and
`effective_cache_size=3GB`, with `fsync` and `synchronous_commit` on. 2 replicas, each with its own load
generator (2 processes × 4 concurrent exports, 10 s export timeout). Seed and servers both use `6d` retention.
The seed is spread over 5.5 days and ends an hour ago. Steps are 60 s at 2,000, 4,000, 6,000 and 8,000 spans/s,
and the ramp is run twice per size: cold (the first after seeding) and warm.

```sh
python python/bench/pg_medium.py seed --store-url "$URL" --out "$OUT" --replicas 2 --seed-days 5.5 \
    --seed-retention 6d --server-retention 6d --seed-procs 6 --spans 4M      # then 7M
python python/bench/pg_medium.py ramp  … --spans-per-s 2000,4000,6000,8000 --step-seconds 60 --max-export-p99-ms 9000
python python/bench/pg_medium.py pages … --spans-per-s 2000 --seconds 120 --page-repeats 10   # 1000 at 7M
```

**Not the same as QV-82**, so read the comparison with these in mind:

- **The machine was quieter.** During QV-82, about 30 CI containers and other agents used most of the RAM:
  100–700 MB free and a load average of 6–12. During QV-180, no other containers were running and the load
  average was 2.5–4.5 outside my own seeding: 2.3 GB free and 7.7 GB available (checked at the start and
  between steps, not continuously). Another agent's run (QV-173) was active at the same time. I didn't check
  what it ran.
- **After each seed, `VACUUM ANALYZE` and a `CHECKPOINT`** before the first ramp. QV-82 measured after
  autovacuum had caught up instead, which should come to the same state.
- **Ingest adds spans.** Every ramp step writes into the database, so the "4M" ramps ran from 4.03M to 5.34M
  spans, and the "7M" ramps from 7.03M to 9.42M. All of that lands in today's partitions, so they're far
  larger than in real traffic, as in QV-82.

Ingest ramp. Cells: achieved spans/s, export p50 / p99 in ms. Errors are 503s or export timeouts.

| size | run | 2,000 | 4,000 | 6,000 | 8,000 | server CPU per replica |
|---|---|---|---|---|---|---|
| ~4M | QV-82, warm | 2,000; 528 / **1,716** | 3,967; 397 / 2,614 | 5,448; 991 / 3,828, 20 errors | — | 14 / 29 / 45% |
| 4.0M | QV-180, cold | 1,991; 197 / 2,631, 1 error (ramp stops) | | | | 28 and 18% (the two replicas) |
| 4.1–5.3M | QV-180, warm | 2,000; 190 / **275** | 4,000; 193 / 236 | 6,000; 193 / **241** | 7,879; 828 / 1,862 | 17 / 35 / 51 / 93% |
| ~7M | QV-82, warm | 1,931; 1,010 / **4,627**, 8 errors | 3,375; 2,058 / 4,790, 55 errors | — | — | 16 / 33% |
| 7.0–8.2M | QV-180, cold | 2,000; 254 / **1,332** | 4,000; 207 / 308 | 5,998; 202 / **1,086** | 7,617; 939 / 2,171 | 16 / 33 / 50 / 93% |
| 8.2–9.4M | QV-180, warm | 2,000; 289 / 1,540 | 4,000; 241 / 997 | 5,984; 219 / **1,261** | 7,380; 695 / 2,333, under 95% | 15 / 32 / 51 / 72% |

- **Throughput no longer falls off from 4M to 9.4M spans** on this machine. 6,000 spans/s held at every size
  with no errors. At 7M, QV-82 didn't hold 2,000. 8,000 spans/s reached 7,400–7,900, with the replicas at
  93% of a core in three of the four steps, so near 8,000 the two replicas are the limit.
- **Export p99 still grows with size:** at 6,000 spans/s it was 241 ms at 4–5M and 1.1–1.3 s at 7–9M. The
  medium tier's p99 ≤ 500 ms held at up to 6,000 spans/s at 4–5M, and at 7–9M only at 4,000 spans/s in the cold
  run (308 ms). The 2,000 step's p99 at 7M (1.3–1.5 s) is above the 4,000 step's in both runs. It's the first
  step on freshly started servers, so it probably includes their warm-up (estimate, not checked).
- **How much of this is QV-127, and how much the quieter machine, this run can't separate.** Postgres'
  statement statistics for both 7M ramps (`pg_stat_statements`), per export, QV-180 against QV-82:

  | statement | mean | blocks read from outside `shared_buffers` |
  |---|---|---|
  | span `INSERT` | 34 ms (QV-82: 300–400) | 339 (270–310) |
  | `pg_advisory_xact_lock` (waits behind the inserts) | 44 ms (240–320) | 0 |
  | `INSERT INTO hash_places` | 5.0 ms (120–150) | 91 (130) |
  | `UPDATE spans` | 32 ms (100–120) | 45 |

  The span `INSERT` doesn't touch the lookup tables, and it read about as many blocks as before, yet took
  a tenth of the time. So those reads were mostly served by the OS page cache, which had more room this time.
  The `hash_places` insert reads fewer blocks, as QV-127 intended: only the last 3 days' lookup partitions are
  live (checked: the 4 older days were sealed). To separate the two causes,
  run QV-82's commit (`39718ce`) on the same quiet machine.

Pages, p95 in ms, from the first replica. Idle has 10 samples per page, so its p95 is close to the slowest call.

| page | QV-82 4M idle / under 2,000 spans/s | QV-180 5.3M idle / under 2,000 | QV-82 7M idle / under 1,000 | QV-180 9.4M idle / under 1,000 |
|---|---|---|---|---|
| messages | 19 / 30 | 43 / 23 | 395 / 10 | 113 / 19 |
| messages, one queue | 194 / 10 | 117 / 16 | 310 / 6 | 167 / 29 |
| message detail | 15 / 13 | 6 / 6 | 22 / 26 | 6 / 11 |
| duplicates | 11 / 8 | 10 / 14 | 12 / 8 | 11 / 11 |
| stuck | 234 / 7 | 163 / 21 | 316 / 20 | 194 / 49 |
| **tenants** | **1,697 / 1,502** | **1,200 / 1,115** | **2,837 / 2,770** | **2,083 / 1,857** |
| tenant search | 179 / 49 | 13 / 31 | 195 / 41 | 22 / 24 |
| body search | 1,372 / 222 | 1,266 / 404 | 2,065 / 214 | 2,055 / 495 |
| body search, one queue | 14,139 / 460 | 1,336 / 566 | 27,777 / 503 | 1,761 / 681 |

Export during the page loop: at 5.3M and 2,000 spans/s, p50 261 ms, p99 952 ms and no errors (QV-82 at 4M:
p99 2,660 ms). At 9.4M and 1,000 spans/s, p50 414 ms, p99 1,593 ms and no errors (QV-82 at 7M: 941 spans/s, p99
9,935 ms, 13 errors).

- **The Tenants page is still over 1 s here, for a different reason than before.** It reads the hourly
  rollup (QV-128) for the whole hours of its window (the last day), but every tenant event of the hours at its
  two ends. The ramps had just written 2.55M of the database's 6.0M tenant events into the current hour, so the
  page read those one by one. Its cost now follows how much was ingested in the last hour, not how much is
  stored. At a steady 2,000 spans/s that's up to about 7M events an hour (estimate). Fixed in QV-224 (below).

**Disk:** 2.9 GB per million spans as seeded (11.7 GB at 4.03M, 19.0 GB at 7.03M), and 25.7 GB at 9.42M after
the ramps. The data directory peaked at 29 GB, with WAL. The database, the container and its data were deleted
afterwards. `df` showed 110 GB used before the run and 110 GB after.

**Seeding** ran at 18,100 spans/s to 2M, 14,300 from 2M to 4M and 8,400 from 4M to 7M (6 processes), against
QV-82's 12,200, 6,600 and 3,700–3,900. `seed` now counts the spans stored instead of using the planner's
estimate: right after a seed nothing has analyzed the new partitions, and a resumed seed asked for 4M stored
5.5M.

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

## Fault tests: `faults.py`

What is stored and shown when things break under steady ingest (QV-238). Each case starts a server, sends
steady traffic from 4 exporter threads (64-span exports, about 2,000–5,000 spans/s here), injects one fault,
lets the server recover with no help, then checks:

- **stored**: every span answered `200` is in the database exactly once (*lost* and *doubled* must be 0). A
  span never answered `200` may be stored or not; the exporter sends it again, and a stored copy is skipped.
- **shown**: `/api/stats` and up to 200 sampled timelines (`/api/messages/{id}`) equal what a clean SQLite
  server shows for the same stored spans, sent once and in order.
- **recovered**: `/api/ready` is `200` and new exports are answered `200` after the fault ends, with no
  restart or other action by hand. For SIGKILL the process is started again at once, as a restart policy does.

The exporter threads retry the way an OTLP exporter with the Collector's `retry_on_failure` does: on a connection
error or `429`/`502`/`503`/`504`, with backoff and `Retry-After`, for up to 300 s. Any other status is final and
the export is *dropped*, and the case fails.

```sh
python bench/faults.py --list
python bench/faults.py                                   # every case, both stores: about 12 minutes
python bench/faults.py --case disk-full --backend sqlite --out /tmp/faults
```

Postgres runs in a container of its own (`postgres:16`), started and removed by the script. The SQLite disk-full
case runs the server in an image built from the checkout. A full disk is a size-limited tmpfs for the data
directory: a real `ENOSPC`, with no root needed. With `--postgres-url` (or `TRAILBEAM_TEST_POSTGRES_URL`) the
cases that don't stop or fill Postgres use that server instead, one database per case, dropped afterwards.
Docker objects are named `qv238-*` and removed at the end (`--keep` keeps them).

| Case | What happens |
|---|---|
| `kill9` | SIGKILL the server 3 times mid-write, each time starting it again 1 s later |
| `kill9-relaxed` | the same with `--durability relaxed` (SQLite) |
| `pg-down` | `docker stop` Postgres for 60 s, then start it; the server is not touched |
| `disk-full` | a ballast file fills the store's disk to the last 2 MB for 60 s, then is deleted |
| `retention-restart` | spans 10–13 days old are stored with a 2-day retention; while the pass deletes them, Postgres is killed and restarted (`docker restart -t 0`), or for SQLite the server is SIGKILLed |
| `replicas` | two replicas on one Postgres behind a round-robin balancer that skips a dead one; SIGKILL one, start it 8 s later |
| `duplicate` | every export answered `200` is sent again (a client retry after a lost answer) |
| `disorder` | spans shuffled over a window of 40 exports, so settles come before receives before sends, from 8 services with clocks off by 0 s to ±1 h |

**In CI** (`tests/server/test_faults.py`, 2 s phases): `kill9`, `kill9-relaxed`, `duplicate` and `disorder` on
SQLite in the Python job; `kill9`, `duplicate`, `disorder` and `replicas` on Postgres in the Postgres job; and a
full SQLite disk through `max_page_count`, which gives the same `SQLITE_FULL`. `pg-down`, `disk-full` with a
real disk, and `retention-restart` need Docker and a minute or more each, so they run from this script only.

### Results (2026-11-23, QV-238)

On a desktop with 16 threads and 11 GB of RAM under WSL2, the data on its virtual disk, `postgres:16`, the default settings above.
*Unacked, stored* is 0 in every case: whatever was cut off was sent again and answered `200` in the end.

| Case | Store | Result | Spans sent | Answered 200 | Lost | Doubled | Dropped | Shown = reference | Recovered after (s) |
|---|---|---|---:|---:|---:|---:|---:|---|---|
| kill9 | SQLite | pass | 113,344 | 113,344 | 0 | 0 | 0 | yes | 0.6, 0.6, 0.6 |
| kill9 | Postgres | pass | 57,344 | 57,344 | 0 | 0 | 0 | yes | 0.6, 0.6, 0.6 |
| kill9-relaxed | SQLite | pass | 104,512 | 104,512 | 0 | 0 | 0 | yes | 0.6, 2.2, 0.8 |
| pg-down (60 s) | Postgres | pass | 35,648 | 35,648 | 0 | 0 | 0 | yes | 5.2 after Postgres was back; `/api/ready` 503 while down |
| disk-full (60 s) | SQLite | pass | 70,400 | 70,400 | 0 | 0 | 0 | yes | 0.2 after space was freed |
| disk-full (60 s) | Postgres | pass | 26,816 | 26,816 | 0 | 0 | 0 | yes | 0.6 after space was freed |
| retention-restart | SQLite | pass | 148,032 | 148,032 | 0 | 0 | 0 | yes | 0.2; every expired span deleted 30 s later |
| retention-restart | Postgres | pass | 70,080 | 70,080 | 0 | 0 | 0 | yes | 2.4; every expired span deleted 31 s later |
| replicas | Postgres | pass | 55,808 | 55,808 | 0 | 0 | 0 | yes | 0.2 via the other replica; 0.2 for the killed one, once started |
| duplicate | SQLite | pass | 38,656 | 38,656 | 0 | 0 | 0 | yes | n/a; `/api/health` counted 38,656 duplicates |
| duplicate | Postgres | pass | 26,176 | 26,176 | 0 | 0 | 0 | yes | n/a; 26,176 duplicates counted |
| disorder | SQLite | pass | 52,672 | 52,672 | 0 | 0 | 0 | yes | n/a |
| disorder | Postgres | pass | 24,000 | 24,000 | 0 | 0 | 0 | yes | n/a |

"Spans sent" for retention-restart includes the expired ones, which are meant to be deleted. Each one was checked
to be gone.

**Faults found, and fixed in QV-238:**

1. **A full SQLite disk lost exports.** It was answered `500 internal error`, which OTLP exporters don't retry. In
   the first `disk-full` run, 1,982 exports (about 127,000 spans) were dropped by the exporters in 60 s. The failed
   `COMMIT` had already rolled back, so the `ROLLBACK` after it raised "no transaction is active" and hid the
   cause in the log. Now both stores answer `503` `store_full` with `Retry-After: 3`, and Postgres' disk-full
   error is logged as "out of disk space", not "unreachable". Postgres was already answered `503`, as unreachable.
2. **An export queued behind more than one group commit could be answered `500`.** The writer whose own export
   wasn't in the first `GROUP_MAX_SPANS` (4,096) queued spans committed one group and returned nothing. The
   server answered `500` and the export was stored later by another writer. Now it writes groups until its own
   is in. Reproduced by queueing 9 exports of 512 spans ahead of one, and tested in `test_group_commit.py`.
3. A client that hung up before its answer printed a `BrokenPipeError` traceback. It now logs one line.

**Filed, for a decision ([QV-300](/QV/issues/QV-300)):** a Postgres statement that runs past `statement_timeout` (60 s) is answered `500`,
so the exporter drops the export. A `503` would have it retried, but an export that is always too slow would then
be retried until the exporter gives up. Not seen in these runs.

**Not tested here:** an OS crash or power loss (the README's guarantee for it comes from SQLite's and Postgres'
documentation), a network partition between replicas and Postgres (`pg-down` covers Postgres being unreachable),
and Postgres failover to a standby.
