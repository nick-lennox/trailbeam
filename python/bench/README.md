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
| `--sdk` | off | send through the OTel SDK's `BatchSpanProcessor` and `OTLPSpanExporter` with their defaults, and count the spans lost (needs a Trailbeam server at the URL: it reads `/api/health` before and after) |

Extra request headers for another endpoint (for example an ingest key):
`LOADGEN_HEADERS='Authorization=Bearer …' python python/bench/loadgen.py --url https://…/v1/traces`.

## Harness: `harness.py`

```sh
python python/bench/harness.py --out bench-results --sizes 10k,100k,1M,10M --fresh
```

For each size, smallest first, growing the same SQLite file:

1. **Seed** the file to the size, with messages spread over the last 7 days (`--seed-days`). By default:
   50 queues, 50 tenants, 1 KB bodies on 5 queues, which are the server's `--store-bodies`.
2. **Pages, idle.** Start `python -m trailbeam.server` (unchanged) on the file. Time Messages, Messages for
   one queue, message detail (20 random messages), Duplicates, Stuck, Tenants, tenant search and body
   search, each `--page-repeats` times (fewer if a page uses up `--page-budget-s`).
3. **Ingest ramp** at each of `--rates` messages/s for `--step-seconds`, until a step is not steady: errors,
   under 95% of the target, or export p99 above `--max-export-p99-ms` (2000). Records server CPU.
4. **Pages under ingest.** The pages again, in a loop, while ingest runs at the highest steady rate. The
   export latency of that ingest is recorded too.
5. **Breakdown.** Server stopped: where a 512-span export's time goes in-process. That covers decode,
   `Database.insert` and its two join queries, plus the query plan of the settle join.

Then the file growth per million spans with no bodies and with 1 KB (stored and not), 64 KB and 256 KB
bodies (`--growth-spans`). The SDK drop count runs against the largest database (`--sdk-seconds`).

Output in `--out`: `results.json` (everything), `results.csv` (one row per measurement), `summary.md`, and
`server.log`. Results are not committed.

Seeding does the receipt-hash join in memory instead of through the server's `Database.insert`. The
server's insert gets slower with every stored span, because the settle join's query does not use the
receipt-hash index (QV-68), so it could not seed 1M spans in a reasonable time. The seeded rows are the
same rows `insert` writes, column for column; `tests/test_bench_harness.py` checks that. The live ingest
in steps 3 and 4 goes through the real server.

Disk: about 1 GB per million spans with 1 KB bodies on a tenth of the queues; `summary.md` has the measured
figure. Seeding runs at about 25k spans/s on one core.
