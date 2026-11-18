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
| `--seed` | new each run | fixes the trace and span IDs. The server skips a span it already stored, so two runs with the same seed against one server measure the second as duplicates, which are almost free. The result records the seed used |
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
5. **Breakdown.** Server stopped, the seeded file restored: where a 512-span export's time goes in-process,
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

Disk: about 1 GB per million spans with 1 KB bodies on a tenth of the queues; `summary.md` has the measured
figure. Seeding runs at about 25k spans/s on one core.
