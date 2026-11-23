# Changelog

## Unreleased

- **`--trusted-proxy` (`TRAILBEAM_TRUSTED_PROXY`): the addresses of your load balancer or TLS proxy.**
  Trailbeam believes `X-Forwarded-For`, `X-Forwarded-Proto` and `X-Forwarded-Host` only from them, and uses
  the client address in `X-Forwarded-For` for logs and (with users and roles) the sign-in limit per
  address. **Changed:** `X-Forwarded-Proto: https` was believed from anyone; now it counts only from a
  trusted proxy. If Trailbeam runs behind a TLS proxy, add the proxy's address with `--trusted-proxy`, or
  the login cookie loses its `Secure` flag (it keeps working). [`deploy/replicas`](deploy/replicas/compose.yaml)
  sets it for its nginx. (QV-225)
- **Forms posted from other sites are refused.** `POST /login` (and every later route that changes
  something, but not ingest) answers `403` when the browser says the request came from another site's page
  (`Sec-Fetch-Site: cross-site` or `same-site`, or an `Origin` that isn't the server). curl and scripts,
  which send neither header, are not affected. (QV-225)
- **Passwords and sessions for users and roles, not switched on yet.** scrypt password hashing (N=2^15,
  r=8, p=3, at most 2 hashes at once per process), sessions in the store with a 20-second cache per process,
  a lock per account after 10 failed sign-ins in a row and a limit per client address. Nothing uses them
  until a later release adds sign-in. (QV-225)
- **Tables for people and tokens (users and roles, phase 1).** The store gains `users`, `sessions`,
  `ingest_tokens`, `personal_tokens` and `setup_codes`: SQLite schema 14, Postgres schema 7. They are created
  empty and nothing uses them yet, so the server behaves exactly as before. Emails are unique ignoring case.
  Retention and `--vacuum` leave the tables alone. With `--db :memory:`, people and tokens are lost on every
  restart, like the spans. **A database upgraded to this version can't be opened by 0.3.0 or 0.4.0 again**:
  back it up before upgrading. (QV-225)
- **Python: a down or slow Trailbeam server no longer holds up your process's exit.** Shutdown
  (`uninstall()`, or interpreter exit) waited for the OTLP exporter's retries and 10 s request timeout, so a
  short script took 6 s longer to exit with the server unreachable and 10 s longer with it slow. It now waits
  at most `TRAILBEAM_SHUTDOWN_TIMEOUT_MS` (default 2000) and counts what is left as `shutdown` drops. The
  Lambda wrapper's flushes now keep to `flush_timeout_millis` (the SDK ignored it). The drop warning now says
  what to check for each reason: for `export_failed`, the server's address and the token in
  `OTEL_EXPORTER_OTLP_HEADERS`, instead of advice about high volume. The Python README lists what happens to
  spans in each case. (QV-169)
- **Node.js: the same.** A short script with the server unreachable took 8 s longer to exit, 10 s with it slow,
  because the event loop waited for the OTLP exporter's retries and request timeout. `uninstall()` and the
  export when the event loop empties now keep to `TRAILBEAM_SHUTDOWN_TIMEOUT_MS` (2 s), the drop warning gives
  the same per-reason advice, and drops held back by the once-a-minute limit are printed at exit instead of
  being lost from the count. The Node.js README lists what happens to spans in each case. (QV-169)
- **Runnable examples.** [`examples/`](examples/README.md) has one small service for each common setup:
  Python with boto3, a Python Lambda function with an SQS trigger, Node.js with AWS SDK v3, and a Node.js
  Lambda function with an SQS trigger. Each runs with one `docker compose` command against ElasticMQ and a
  local Trailbeam, and sends three orders: one processed, one retried after the visibility timeout, one
  moved to the dead-letter queue. CI runs every example and checks the three timelines. (QV-172)
- **The Tenants page no longer slows down after heavy ingest.** It read every raw event of the current hour,
  so right after a burst of traffic it took 1–2 s on Postgres (QV-180). It now reads the current hour from the
  hourly rollup, less the few events newer than the window, and adds up the raw events it still reads in SQL.
  With 2M events in the current hour, the last day went from 2.07 s to 55 ms (p95, Postgres) and from 646 ms
  to 9 ms (SQLite, 626K events), with the same answers. (QV-224)
- **Body search takes plain text.** On the Messages page and in `GET /api/messages?body=`, `ord-1003` is
  searched as `body:ord-1003` instead of being refused with a 400. The **Body** field shows an example, and
  on a server that keeps no bodies (`/api/health` lists no `store_bodies`) it is off, with a line saying how to
  turn bodies on. A term for another field still fails, now as ``Can't read `queue:orders`. Use text, or
  `body.<path>:<value>`.``, without the HTTP status. (QV-215)
- **`trailbeam start` prints the token and key only on the run that created them.** Later runs print the
  command that shows each one (`cat ~/.trailbeam/token`) instead, and the export lines read the files with
  `$(cat ...)`, so a server restarted under systemd, `docker run -d`, nohup or CI no longer writes the token and
  the receipt-hash key into journald, `docker logs` or the build log on every start, and pasted export lines
  keep them out of shell history. A token given with `--token-file` or `TRAILBEAM_TOKEN` is never printed.
  `npx trailbeam start` with the Docker volume prints a `docker run ... cat /data/token` command. The first
  run is unchanged. (QV-203)
- **Kubernetes: a Helm chart.** `deploy/helm/trailbeam` installs the server as a Deployment of 2 replicas
  behind a Service, on a Postgres you run (`store.url` or a Secret), or on a trial Postgres in the cluster
  (`postgresql.enabled=true`). The token and receipt-hash key are generated once into a Secret and kept on
  upgrade, or come from your own Secret. Liveness probe on `/api/health`, readiness on `/api/ready`, and a
  startup probe long enough for a schema upgrade. CI installs it on a kind cluster and sends one span. It
  replaces the example manifest `deploy/kubernetes/trailbeam.yaml`; `helm template` renders plain manifests.
  Releases now also publish `ghcr.io/nick-lennox/trailbeam:<version>-postgres`, the image with the Postgres
  driver, which is the chart's default ([Kubernetes](README.md#kubernetes-helm-chart)). (QV-173)
- **Messages: Receives, not Deliveries.** The column on Messages, Duplicates and Stuck is now **Receives**, with a
  tooltip that defines it (how many times SQS returned the message from `ReceiveMessage`) and what *handled* means
  (a receive that ended in a delete or a successful Lambda record). Cells read `2 · 1 handled` instead of
  `2 (1 ok)`, and the count is Ember when the message was received more than once, so that row stands out. The
  timeline summary says **Receives** `2 (1 handled)` too, on the UI and the no-script pages (QV-209).
- **`trailbeam doctor`: why no data is arriving.** It checks, in plain lines, that the server answers, that the
  ingest port accepts connections, that the token is accepted, that a test span sent now is stored and read
  back, and when the last real span arrived. Each failed check prints the likely cause and the fix, and the exit
  code is 1 when one fails. By default it uses the address and token `trailbeam start` created
  ([README](README.md#troubleshooting-no-data-arriving)): without `--url`, the server of
  `TRAILBEAM_OTLP_TRACES_ENDPOINT`, or the address `trailbeam start` keeps in `address` in its data directory,
  so `trailbeam start --port N` is checked on N and not on 4318. The test span is a *probe* (span contract section 17):
  the server stores it apart from the spans (new table `probes`: SQLite schema 13, Postgres schema 6, both
  applied on start), so it never shows as a message or in a count, and reads it back at
  `GET /api/probes/{id}` (at most 8 probes an export, in one write). `GET /api/health` also gives `data.last_span_ns`, the time of the span stored last.
  A server of 0.3 or before rejects the probe; `trailbeam doctor` says so and the other checks still run.
  Once upgraded, a SQLite file can no longer be opened by 0.3.0, which refuses it at start (restore a backup to go back).
- **Upgrading from 0.3.0 is checked in CI.** A new job starts 0.3.0 (the PyPI package, and the published image),
  stores 1,200 messages' spans, then runs this version on the same SQLite file, volume or Postgres database. It
  checks that every timeline, list and tenant count reads as before, that messages in flight across the upgrade
  are joined, that the token and sessions keep working, and that 0.3.0 then refuses the upgraded SQLite file or
  Postgres database, which it can't read ([Upgrading](README.md#upgrading)).
- **`trailbeam start` on an existing database says what its new token and key change.** On a data directory with
  only a 0.3.0 SQLite file (0.3.0 had no `start`), `start` makes a token and a receipt-hash key, as on a first
  run. Now it also logs a warning: with the token on, services that send without it are refused (401); the
  services keep the key they had until they are given the new one. The warning says how to keep the old token
  and key. Nothing else changes. (QV-170)
- **Team install: `.env` readable by its owner only.** The README, the `docker-compose.yaml` header and
  `.env.example` now say `install -m 600 .env.example .env` instead of `cp`, which left the token and the
  Postgres password readable by every user of the server (QV-204).
- **`npx trailbeam start` says which image it runs; image versions are write-once.** Before the server starts,
  one stderr line names the image: its reference, local image ID and registry digest, and, when
  `TRAILBEAM_IMAGE` is set, that it replaces the default image. The image is pulled (only when missing) and
  inspected first, then run with `--pull never`, so the line names what runs. The release workflow now stops
  before pushing `ghcr.io/nick-lennox/trailbeam:X.Y.Z` when that tag is already published, so re-running a
  release can't change what an already-published npm version runs (QV-205).
- **`trailbeam start` keeps the data directory private.** Others on the machine could read the spans (the
  SQLite file was created with the umask, often 644) or plant a `token` that the server then used. Now an
  existing data directory has to be yours and not writable by every user, else `trailbeam start` (and
  `npx trailbeam start --data-dir`) stops with one line and status 2; one the group or others can use is made
  700. A `token` or `receipt-hash-key` that is a symlink or another user's is refused; one others can read is made
  600 instead of only warned about. The SQLite file and its `-wal` and `-shm` are created 600 (wherever `--db`
  puts them); an existing one of another user's is refused, and one others can read is made 600.
- **A first-run screen, and one design for every page.** While the store holds no span, `/` shows how to send
  the first message: install the package (a Python / Node.js switch), the OTLP endpoint of this server with a
  Copy button, the environment variables (with the token header when the server has a token, never the token),
  and a five-line example. A waiting line polls `/api/health` every 2 s and says why nothing has arrived yet when
  the ingest counters show a reason (spans from other instrumentation set aside, spans rejected, nothing at all
  after 2 minutes). When the first span is stored the page shows that message, a link to its timeline and the
  Messages list, without a reload. Messages, Duplicates and Stuck are now drawn by the shared UI in the same dark
  design as the timeline and Tenants, and on an empty store they say no service has sent spans yet instead of
  "No stuck messages." The Tenants page cites the span contract's section 15.2, with a link. Without the UI
  build, the HTML pages answer as before, with the same empty-store wording.

- **Port already in use: one line, not a traceback.** When the listen port is taken (4318 is the standard
  OTLP/HTTP port, so an OpenTelemetry Collector, Jaeger or another OTLP receiver often holds it), `trailbeam`
  and `trailbeam start` print `trailbeam: port 4318 on 127.0.0.1 is already in use (another OTLP receiver?).
  Stop it or run trailbeam --port 4319 and set TRAILBEAM_OTLP_TRACES_ENDPOINT to match.` and exit with status 2.
  A port it may not use, or a `--host` that isn't an address of the machine, is also one line.
  `npx trailbeam start` checks the port before it runs Docker and prints the same line (with
  `npx trailbeam start --port 4319`) instead of Docker's "port is already allocated".

- **Node.js: a short script no longer loses its spans.** When the event loop empties, the package now
  exports the spans still queued, so a script that sends a message and ends without
  `await trailbeam.uninstall()` delivers them, as the Python package already did. `process.exit()` and
  signals skip that step; spans still queued then are lost, and the package now prints one line to stderr
  with the count and the fix (`await trailbeam.uninstall()` before `process.exit()`)
  ([Node.js README](node/README.md#client-hooks)). (QV-156)

- **Node.js: `trailbeam.uninstall()` no longer fails when the Trailbeam server is unreachable.** With nothing
  listening on the OTLP endpoint, `await trailbeam.uninstall()` (and `trailbeam.forceFlush()`) rejected with
  `ECONNREFUSED`, which ended the process with status 1. Both now resolve; the spans that could not be sent are
  reported only as drops (the `TRAILBEAM_SPANS_DROPPED` warning and `exportStats()`), as in Python.
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
0.2.0) goes through schema 4 first, as the 0.3.0 notes describe. SQLite files are unchanged: 0.3.0 still opens them.

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
