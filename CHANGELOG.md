# Changelog

## Unreleased

- **Postgres: the lookup tables are time-bucketed (schema 4).** `mid_places` and `hash_places` are partitioned
  by day like the spans, so ingest writes only to recent partitions and retention drops them whole (before, it
  deleted their rows in batches). Older days are sealed into a Bloom filter, about 2.5 bytes per key, kept by
  each replica. A lookup reads an older day only for the keys the filter can't rule out: a late settle, a
  message older than three days, or about 1 in 15,000 other keys. Joins and summaries are unchanged; late
  settles still join their receive from any day. At 1M spans over 7 days, ingest touches 25 MB of lookup
  indexes instead of 92 MB. Upgrading a 1M-span database took 8 s. The effect at 4M–100M spans hasn't been
  measured.
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

### Upgrading from 0.2.0 (Postgres)

**Stop every 0.2.0 replica, then start the new version.** At start the server upgrades the database to
Postgres schema 3, one day partition at a time. Schema 2 fills the lookup tables from the stored spans. On our
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
