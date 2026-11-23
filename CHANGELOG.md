# Changelog

## Unreleased

- **`trailbeam doctor`: why no data is arriving.** It checks, in plain lines, that the server answers, that the
  ingest port accepts connections, that the token is accepted, that a test span sent now is stored and read
  back, and when the last real span arrived. Each failed check prints the likely cause and the fix, and the exit
  code is 1 when one fails. By default it uses the address and token `trailbeam start` created
  ([README](README.md#troubleshooting-no-data-arriving)). The test span is a *probe* (span contract section 17):
  the server stores it apart from the spans (new table `probes`: SQLite schema 13, Postgres schema 6, both
  applied on start), so it never shows as a message or in a count, and reads it back at
  `GET /api/probes/{id}`. `GET /api/health` also gives `data.last_span_ns`, the time of the span stored last.
  A server of 0.3 or before rejects the probe; `trailbeam doctor` says so and the other checks still run.
- **`npx trailbeam start`: the server from npm, without Python.** The npm package has a `trailbeam` command.
  `npx trailbeam start` runs the container image of the same version (`ghcr.io/nick-lennox/trailbeam:X.Y.Z`)
  with its data in the Docker volume `trailbeam-data`, and prints the same first-run output as the Python
  `trailbeam start`: the address, the token and the environment variables for your services. It takes the
  same flags and `TRAILBEAM_` variables; `--host`/`--port` become the published port, and `--data-dir`, `--db`,
  `--config` and `--token-file` name files on your machine (`--store-url` is not available: the image has
  SQLite only). If Docker is missing or not running it says so in one line. `require('trailbeam')` is
  unchanged and there are no new dependencies
  ([Node.js README](node/README.md#run-the-server-npx-trailbeam-start)).
- **`trailbeam start`: first-run setup in one command.** On its first run in a data directory (default
  `~/.trailbeam`, or `--data-dir` / `TRAILBEAM_DATA_DIR`) it creates an access token and a receipt-hash key,
  each in a file only its owner can read, and keeps the SQLite file there. It then starts the server and prints
  the web screen's address, the token, and the environment variables to paste into a Python and a Node.js
  service. Later runs read the same files, so the token and key don't change. Every `trailbeam` flag works
  after `start`; `--db`, `--store-url` and `--token-file` (or `TRAILBEAM_TOKEN`) replace the file in the data
  directory. `trailbeam` with flags, as the container image runs it, is unchanged: it creates no files and
  turns on no token.
- **A team install from one file: `docker-compose.yaml`.** `cp .env.example .env`, fill in the token, the
  receipt-hash key and the Postgres password, then `docker compose up -d --build --wait`: the Trailbeam server
  and Postgres 16, a named volume for each, health checks, `restart: unless-stopped`, and
  `max_locks_per_transaction = 256` on Postgres. Compose refuses to start without the token. CI starts it from
  `.env.example`, sends a span and checks the data survives `docker compose down && up`
  ([Team install](README.md#team-install-docker-compose-with-postgres)).
- **Postgres: the lookup tables are time-bucketed (schema 5).** `mid_places` and `hash_places` are partitioned
  by day like the spans, so ingest writes only to recent partitions and retention drops them whole (before, it
  deleted their rows in batches). Older days are sealed into a Bloom filter, about 2.5 bytes per key, kept by
  each replica. A lookup reads an older day only for the keys the filter can't rule out: a late settle, a
  message older than three days, or about 1 in 15,000 other keys. Sealing runs at the start of every retention
  pass, also when every queue is kept forever: with Postgres the passes now run then too, deleting nothing
  (they also create the coming days' partitions ahead of ingest, which they didn't before). Joins and summaries are unchanged; late
  settles still join their receive from any day. At 1M spans over 7 days, ingest touches 25 MB of lookup
  indexes instead of 92 MB. Upgrading a 1M-span database took 8 s. The effect at 4M–100M spans hasn't been
  measured.

### Upgrading from 0.3.0 (Postgres)

**Stop every 0.3.0 replica, then start the new version.** At start the server upgrades the database from
Postgres schema 4 to 5, one day partition at a time: it copies each day's lookup rows into that day's new
partitions, then drops the old tables. On our test machine that took 8 s for 1M spans, and the first
retention pass after it sealed the 5 older days into the filter (4.5 s). A 0.3.0 replica left running would
write lookup rows the filter doesn't know about, so don't run 0.3.0 and this version against one database at
the same time. Once the database is at schema 5, a 0.3.0 server refuses to start on it. If the upgrade is
stopped part way, the next start runs it again; rows already copied are left as they are. A database at an older schema (from
0.2.0) goes through schema 4 first, as the 0.3.0 notes describe. SQLite files are unchanged.

## 0.3.0: Postgres for teams that outgrow SQLite

The first published release: `pip install trailbeam`, `npm install trailbeam`, and the container image
`ghcr.io/nick-lennox/trailbeam`. 0.2.0 was tagged but never published.

- **The Tenants page reads an hourly rollup per tenant, on SQLite and Postgres.** Before, it added up every
  tenant event in its range on each request, so it got slower as traffic grew (Postgres: p95 1.5 s at 4M spans,
  2.8 s at 7M). Now each store keeps, per hour and tenant, the sends, first deliveries with their times in
  queue, and dead-letter moves, kept up to date as spans arrive and taken out when retention deletes them. The
  page reads the rollup for the whole hours in its range and the raw events for the part-hours at either end.
  At 1M spans on our desktop, idle, p95 for the last day went from 78 ms to 17 ms on SQLite and from 184 ms to
  44 ms on Postgres; for the last 7 days from 553 ms to 72 ms and from 670 ms to 193 ms. Every count is still
  exact. Ingest time per export was the same within the noise of the measurement.
- **Contract change: `p95_in_queue_ms` is within 1% of the exact value** (`contract/tenants.md`). The rollup
  keeps the times in queue as counts per bucket (a sketch, after DDSketch), which can be added up across hours
  but not sorted. Times up to 100 ms come back exact; above that the p95 is at most 0.91% off the nearest-rank
  value. The same spans always give the same p95. `contract/fixtures/tenants-expected.json` keeps the exact
  values and gains `p95_relative_error` (0.01), which a result must be within.
- **Postgres: the connection pools are settings.** `--pg-ingest-connections` (`TRAILBEAM_PG_INGEST_CONNECTIONS`,
  default 4) and `--pg-read-connections` (`TRAILBEAM_PG_READ_CONNECTIONS`, default 8) set how many exports and
  reads one replica runs on Postgres at once. Before, they were fixed at 4 and 8.
- **Postgres: an export no longer locks every day's partitions.** Before, each export locked about 25
  partitions and indexes per day partition of history until it committed, so a store with months of history
  could fill Postgres's lock table ("out of shared memory", `max_locks_per_transaction`). Now an export locks
  only the partitions its own messages are in: 85 lock entries for a typical export, with 10 day partitions or
  400. Reading one message's timeline is bounded the same way. Two small lookup tables make this possible:
  one row per message ID and one per receipt handle. On our test machine, seeding 205,000 spans wrote 29% more
  WAL (852 MB against 658 MB), the database grew 6% (33 MB), and the seed took 48 s instead of 90 s.
- **Postgres: a list page no longer locks every day's partitions.** The message, duplicate and stuck lists,
  tenant search and activity, `/api/stats` and `/api/health` read 8 day partitions per query. Each query runs
  in its own transaction, so it releases its locks before the next one starts. Newest-first lists start with
  the newest day and stop once the page is full. With 400 day partitions, the most lock entries any page held
  at once fell from 3,586 to 124. With 10 it is about the same as before (124 against 103). The answers are
  unchanged. With 400 partitions, the first page of messages takes 1.6 ms instead of 24 ms and tenant search
  151 ms instead of 490 ms. The lists that must read every partition got slower: stuck messages take 62 ms instead of 32 ms,
  and `/api/health` 55 ms instead of 48 ms.
- **Postgres: keeping a message's summary up to date no longer scans the day's partition.** Three summary
  tables get an index on the message ID (schema 3). On a day partition of 500,000 rows, each of those deletes
  took 8–26 ms and now takes 0.1 ms.
- **Postgres: a new day's partition no longer stalls ingest behind a `pg_dump`.** A partition is now created as
  a table of its own and then attached, which needs no lock that an open transaction or an export holds, so a
  `pg_dump` can run at any time of day. Retention creates the partitions for today and the next two days. Before,
  a read transaction open for 25 s made exports wait 24 s. Attaching still waits at most 1 second for a schema
  change running at the same time; an export that needs a partition that can't be created then tries 3 times,
  then is answered `503` with `Retry-After`. Exports that need new partitions at the same moment, on one
  replica or several, take turns, and one whose partitions another has made stops waiting. Before, the one that
  lost the race could fail ("type ... already exists"), and under load one that waited more than 2 s for another
  export of its replica was answered `503`.
- **Postgres: a new day's partition is analyzed once a replica has written 1,000 spans to it.** Until then
  Postgres has no statistics for it and plans it as empty. On a 200,000-row partition, that made the
  receipt-hash update take 55–68 ms instead of 0.6 ms.
- **Postgres: the server says when `max_locks_per_transaction` is too low.** At start and as partitions are
  added, the server works out the value it needs and logs a warning with it. Since list pages read 8 day
  partitions a query, that value no longer grows with the partitions: a stock Postgres is enough for one replica
  with up to about 80 read connections. A request that fails on a full lock table is logged as that, and an
  export gets 503 with `Retry-After`, not "Postgres is unreachable". README: Postgres lock table.

### Upgrading from 0.2.0 (SQLite)

At start the server upgrades the file to schema 12, which builds the hourly tenant rollup from the stored
message summaries, without reading the spans: 7 seconds for a 1M-span file on our test machine, during which
the server doesn't accept spans. Messages moved to a dead-letter queue more than once get their summary
rewritten first (rare). If it is stopped part way, the next start runs it again.

### Upgrading from 0.2.0 (Postgres)

**Stop every 0.2.0 replica, then start the new version.** At start the server upgrades the database to
Postgres schema 4, one day partition at a time. Schema 4 builds the hourly tenant rollup of each partition
from its message summaries: 3.9 seconds for 1M spans on our test machine. A replica of an earlier build left
running would store spans without adding them to the rollup, and the Tenants page would undercount them, so
this applies to builds of `main` at schema 2 or 3 too. Schema 2 fills the lookup tables from the stored spans. On our
test machine that took 1 second for 205,000 spans. Schema 3 builds the message-ID indexes: 0.4 s for 500,000
rows in each of the three tables. Each partition's index build blocks writes to that partition only, and no
replica is writing during the upgrade anyway. Reads (and a `pg_dump`) carry on. A database already at schema 2
(from a build of `main`) gets only the schema-3 step, and the same rule applies: stop every replica first.
Don't run 0.2.0 and this version against one database at the same time. A 0.2.0 replica doesn't write the lookup tables, so the spans it stores are missing from
joins and timelines. Once the database is upgraded, a 0.2.0 server refuses to start on it. If the upgrade
is stopped part way, the database stays at the last schema it finished, and the next start goes on from
there.

## 0.2.0: production single-node

The first tagged release. One Trailbeam server on SQLite, sized and documented for production: retention,
pagination, backpressure, metrics, a shared access token, backups, and a CI check that fails the build on the
kind of slowdown that made inserts 100 to 10,000 times slower before this release. Postgres is an optional,
bring-your-own store, and several replicas can share one Postgres.

Version numbers: the Python package (`trailbeam`, SDK and server) is 0.2.0. The Node package and
`@trailbeam/ui` keep their own versions.

### Server

- **Retention, 7 days by default**, per queue, or `forever`; a background purge that holds the writer at most a
  quarter of the time. New files give deleted space back to the disk. `--vacuum` rewrites an older file
  once so it does too.
- **Pages read per-message summaries**, so a page costs about the same at any size. `/api/messages`,
  `/api/duplicates` and `/api/stuck` page with `limit` and `next_cursor` and take `from_ms`/`to_ms`. Tenant
  search and body search are bounded by a time window (default the last day).
- **Ingest:** the receipt-hash joins are index lookups at any size. Schema 11 cuts the disk writes per span
  by about a third. Group commit across concurrent exports. Reads use their own WAL connections and no
  longer wait for the writer.
- **Backpressure:** ingest answers 429 or 503 with `Retry-After` instead of queueing without limit
  (`--ingest-max-pending`, `--ingest-max-wait`).
- **`GET /metrics`** (Prometheus), **`GET /api/ready`** (fails while the store is unreachable), and a cheap
  `GET /api/health`. Exact counts moved to `GET /api/stats`.
- **One shared access token** (`TRAILBEAM_TOKEN` or `--token-file`), off by default, for ingest, the API and
  the web screen.
- **Configuration file** (`--config trailbeam.toml`) with per-queue settings, checked at start.
- **Message bodies are stored once** per message, not on every span that carries them.
- **`--durability relaxed`**, opt-in: skips the sync per export. An OS crash or power loss can lose the last
  moments of exports answered 200. `full` stays the default.
- **Postgres store** (`--store-url`), partitioned by day and retention class, and **several replicas** on one
  Postgres; retention runs on one replica at a time.
- **Versioned schema:** the server upgrades an older file at start and refuses a newer one.
- `contract/openapi.yaml` describes the API and the ingest endpoint; a test keeps it and the server in step.

### SDK

- Counts and logs dropped spans; the default export queue is 16,384 spans. The Collector docs show a persistent
  queue.

### Docs and tests

- README: [sizing one SQLite server](README.md#sizing-one-sqlite-server) (disk per span, the retention limit,
  the measured range, when to move to Postgres) and [backup and restore](README.md#backup-and-restore-sqlite).
- `python/bench/`: load generator, harness, Postgres smoke run, and `ci_check.py`, which runs in CI on every
  pull request: the join plans must use the hash indexes, with insert, page and ingest limits on a 100k-span
  file. CI also runs it on a deliberately broken plan, which it must catch.
- A store conformance suite from `contract/fixtures`, run against SQLite and Postgres.

### Upgrading from an earlier build

Back up the database file first ([Backup and restore](README.md#backup-and-restore-sqlite)). The upgrade is
one way: an older Trailbeam refuses the upgraded file.

1. **Retention is now on, at 7 days.** The first start deletes every message whose newest span is older
   than that. To keep everything, start with `TRAILBEAM_RETENTION=forever` (or `--retention forever`).
2. **The file is upgraded when the server starts**, up to schema 11. Two steps take time on a large file:
   - *Schema 10* moves message bodies into a table of their own, one copy per body.
   - *Schema 11* rewrites the spans table and its summaries. That took about 5 minutes at 1M spans on our
     test machine (305 s with the disk busy), and we estimate about an hour at 10M. While it runs, the server
     doesn't accept spans. It needs free disk about the size of the file, and afterwards the file is larger by
     about that much (2.1 GB to 3.7 GB at 1M spans); new spans reuse that space. If it's stopped part way,
     the file stays at the version before, and the next start runs it again.
3. **New duplicate rule (SQLite):** a span is skipped as a duplicate only when its trace ID, span ID **and
   end time** match one already stored. Exporter retries resend the span unchanged, so they are still
   skipped. A span sent again with the same IDs but a different end time is now stored.
4. **`GET /api/health` no longer counts the table.** Its `data` is now `{"last_row": n}`. The old
   `data` object (`spans` by kind, `messages`, `unjoined_settles`) is at `GET /api/stats`, unchanged. That
   call reads the whole table, so it takes seconds on a large file. The container's `HEALTHCHECK` only
   checks the status and is not affected.
5. **List endpoints page.** `/api/messages`, `/api/duplicates` and `/api/stuck` return at most `limit`
   messages (default 100, at most 1,000). Follow `next_cursor` for more.
6. **Ingest can answer 429 or 503 with `Retry-After`** when the server is overloaded. OTLP exporters and
   the Collector retry these.
7. **Disk space:** a file created before schema 8 reuses freed space but doesn't shrink. Run
   `trailbeam --db <file> --vacuum` once, with the server stopped, if you want the space back.
