<p align="center">
  <a href="https://trailbeam.app">
    <img src="assets/banner.gif" alt="Trailbeam: messages cross a pre-dawn sky as trails of light, watched by an observatory on a mountain peak" width="880">
  </a>
</p>

<p align="center">
  <strong>The whole life of one Amazon SQS message, step by step.</strong>
</p>

<p align="center">
  <a href="https://trailbeam.app">Website</a> |
  <a href="https://docs.trailbeam.app">Docs</a> |
  <a href="https://docs.trailbeam.app/quickstart">Quickstart</a> |
  <a href="https://docs.trailbeam.app/concepts/how-it-works">How it works</a>
</p>

<p align="center">
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-Apache--2.0-blue" alt="License: Apache-2.0"></a>
  <img src="https://img.shields.io/badge/status-pre--release%200.1-orange" alt="Status: pre-release 0.1">
  <img src="https://img.shields.io/badge/python-3.10%2B-3776AB" alt="Python 3.10 or later">
  <img src="https://img.shields.io/badge/node.js-20%2B-5FA04E" alt="Node.js 20 or later">
</p>

## What is Trailbeam?

Trailbeam builds a timeline for each Amazon SQS message: sent, received, visibility changed, deleted,
moved to a dead-letter queue, and the Lambda outcome. You look up a message by its message ID and
read what happened to it, in order, with the service that did each step.

Metrics and tracing tools show queues in aggregate, or the spans your code emitted. Trailbeam
answers a narrower question: where did this one message go?

> [!NOTE]
> Trailbeam is pre-release 0.1. This repository is private, and nothing is published to PyPI, npm or
> a container registry yet. Install the packages and build the image from a clone.

## Features

- **One timeline per message.** Every send, receive, delete and visibility change your services
  make, joined into a single history you open by message ID.
- **Recorded and inferred steps are kept apart.** Trailbeam orders two events only when an observed
  fact proves the order, and marks every step it inferred, such as a visibility timeout that expired.
- **Deletes joined to their receives.** A delete carries a receipt handle, not a message ID. Trailbeam
  joins the two through a keyed hash of the handle, so the handle itself never leaves your process.
- **Dead-letter moves, as SQS reports them.** A move is shown when a receive on the dead-letter queue
  carries `DeadLetterQueueSourceArn`. It is never guessed from settings.
- **Lambda outcomes.** A handler wrapper records how each record ended: success, a reported batch
  item failure, or an exception.
- **Tenants.** Name the message attribute or JSON body field that holds your customer, and
  Trailbeam shows per tenant: messages sent, their share, p95 time in queue and dead-lettered
  messages. You can also list one tenant's messages and search tenants by name.
- **Works with the OpenTelemetry you already run.** The packages use their own tracer provider with
  an always-on sampler, so your sampling never drops a message's history.
- **Hands off your queues.** Trailbeam makes no SQS calls of its own and never calls `ReceiveMessage`,
  because a receive raises a message's receive count.
- **One container to run.** Trailbeam takes OTLP/HTTP, keeps spans in SQLite, and serves a JSON API
  and a web screen on one port.

## Quickstart

The [quickstart](https://docs.trailbeam.app/quickstart) runs Trailbeam on your laptop with Docker and an SQS emulator.
It needs no AWS account.

```sh
gh repo clone nick-lennox/trailbeam && cd trailbeam
docker compose -f demo/compose.yaml up -d --build --wait
# then open http://127.0.0.1:4318/ and click a message ID
```

## Add it to your services

Add the hooks to every service that sends, receives or deletes on the queues you care about. A
service without them leaves gaps in a message's history.

```sh
pip install trailbeam      # Python
npm install trailbeam      # Node.js
```

Neither is published yet. Until they are, install from a clone: `pip install ./python`, `npm install ./node`.

**Python (boto3)**

```python
import boto3
import trailbeam.boto

trailbeam.boto.install()  # once per process
sqs = boto3.client("sqs")
```

**Node.js (AWS SDK v3)**

```js
const { SQSClient } = require("@aws-sdk/client-sqs");
const trailbeam = require("trailbeam");

const sqs = trailbeam.instrument(new SQSClient({}));
```

**AWS Lambda**

```python
import trailbeam.aws_lambda

@trailbeam.aws_lambda.wrap
def handler(event, context):
    ...
```

Point the packages at Trailbeam and give every service on the same queues the same hash key:

```sh
export TRAILBEAM_OTLP_TRACES_ENDPOINT=http://localhost:4318/v1/traces
export TRAILBEAM_RECEIPT_HASH_KEY=$(openssl rand -hex 32)   # generate once, share with every service
export OTEL_SERVICE_NAME=orders-worker
```

Full guides: [Python](python/README.md), [Node.js](node/README.md), and the Lambda sections in each.

### Tenants

To see traffic per customer, tell the packages where the tenant is. Set the same value in every
service that sends or receives on those queues:

```sh
export TRAILBEAM_TENANT_ATTRIBUTE=tenant        # a message attribute, tried first
export TRAILBEAM_TENANT_BODY_PATH=customer.id   # else a dotted path in a JSON body
```

You can also pass `tenant={"message_attribute": ..., "body_path": ...}` to `trailbeam.boto.install()`
or `tenant: { messageAttribute, bodyPath }` to `install()` in Node.js (and the same to the Lambda
wrappers); an option given in code replaces both environment variables. Spans then carry
`trailbeam.tenant` (span contract, section 15.2). Trailbeam has no setting of its own for this: it
reads the tenant the packages recorded. Reading the body path parses the body in your process, but
the body is not exported.

The **Tenants** page (`/tenants`) shows, for the last hour, day or week: messages sent, share of all
sends, p95 time in queue (send to first receive) and dead-lettered messages, per tenant. A tenant's
name links to its messages (`/?tenant=acme`), and the search box in the header suggests tenants
named in the page's range (default the last day) as you type. The same data is in the JSON API:
`GET /api/tenants`, `GET /api/tenants/search?q=` (with `since_s` or `from_ms`/`to_ms`, and `queue`) and
`GET /api/messages?tenant=`. How every number is counted is in
[`contract/tenants.md`](contract/tenants.md). The hosted app counts them the same way and is tested
against the same recorded spans.

## Self-hosting

Trailbeam is one container. It receives OTLP on `/v1/traces` and serves the web screen and the JSON
API on the same port.

The list endpoints (`/api/messages`, `/api/duplicates`, `/api/stuck`) return at most `limit` messages
(default 100, at most 1,000) and a `next_cursor`: pass it back as `cursor` for the next page, `null`
on the last one. `from_ms`/`to_ms` (milliseconds since the epoch, both included) keep messages last
seen in that range (Stuck: `from_ms`, first seen). Lists read a per-message summary the server keeps
up to date as spans arrive, so a page costs about the same however much history is stored.

```sh
docker build -t trailbeam .
docker run -d --name trailbeam -p 127.0.0.1:4318:4318 -v trailbeam-data:/data trailbeam
```

Or without Docker: `pip install "trailbeam[server]"`, then run `trailbeam`.

With no access token set (the default), anyone who can reach the port can send spans and read every
timeline, so keep it on `127.0.0.1` or a trusted network, or set a token ([Access token](#access-token)). See
[Run Trailbeam](https://docs.trailbeam.app/setup/run) and, to forward spans through an OpenTelemetry Collector, the
[Collector configuration](collector/README.md).

### Access token

One shared token protects ingest, the API and the web screen. It is off by default for local use:

```sh
openssl rand -hex 32 > trailbeam.token
docker run ... -v $PWD/trailbeam.token:/run/secrets/trailbeam-token:ro \
  -e TRAILBEAM_TOKEN_FILE=/run/secrets/trailbeam-token trailbeam
# or trailbeam --token-file trailbeam.token, or TRAILBEAM_TOKEN=<token> in the environment
```

When it is set:

- **Ingest** (`POST /v1/traces`) needs `Authorization: Bearer <token>`, or it gets `401`. The packages
  use the standard OpenTelemetry exporter, so set
  `OTEL_EXPORTER_OTLP_HEADERS=Authorization=Bearer%20<token>` in your services. A Collector sets it in its
  `otlphttp` exporter's `headers`.
- **The API** needs the same header. `GET /api/health` still answers without it, for health checks, but
  then returns only `status` and `version`.
- **The web screen** asks for the token once, on a sign-in page. It then sets a session cookie
  (`HttpOnly`, `SameSite=Strict`, 30 days) that holds an HMAC of the token, not the token itself. The
  cookie works for the pages and the API but not for ingest. Changing the token signs everyone out.
  `/logout` removes the cookie.

The token must be at least 16 printable ASCII characters, with no spaces; otherwise Trailbeam refuses
to start. It travels in plain HTTP, so put Trailbeam behind a TLS proxy when it crosses a network you
don't trust. The cookie gets `Secure` when the proxy sends `X-Forwarded-Proto: https`. There is one
token for everyone: no users, roles or audit log (those are in the hosted app).

### Configuration file

Per-queue settings go in a TOML file, so a few hundred queues don't turn into a few hundred flags:

```toml
# trailbeam.toml
[defaults]
retention = "7d"            # every queue; a duration or "forever" (default 7d)
store_bodies = false        # true: store every queue's bodies

[queues.orders]
max_receive_count = 5       # the redrive policy's maxReceiveCount (1 to 1000)
store_bodies = true
retention = "30d"

[queues."orders-dlq"]       # quote names TOML keys can't hold
retention = "forever"
```

```sh
docker run ... -v $PWD/trailbeam.toml:/etc/trailbeam.toml:ro -e TRAILBEAM_CONFIG=/etc/trailbeam.toml trailbeam
# or trailbeam --config trailbeam.toml
```

Trailbeam reads the file once, at start. It checks the whole file before it opens the database: an
unknown table or setting (a typo), a wrong type or a bad value stops it with exit status 2. The error
lists every problem with its place, for example `queues.orders.max_receive_count: expected a whole number
from 1 to 1000, got 5000`.

The flags and their environment variables still work and override the file. `--max-receive-count` and
`--retention-queues` replace the file's value for the queues they name and leave the rest as they are.
`--store-bodies` and `--retention` replace the file's list and default. An environment variable that is
set but empty counts as not set. `GET /api/health` shows the result under `config`.

### Message bodies

Trailbeam stores no message body unless you opt queues in on the server as well as in the package:

```sh
docker run ... -e TRAILBEAM_STORE_BODIES=orders,payments trailbeam   # or trailbeam --store-bodies orders,payments
```

or `store_bodies = true` for those queues in the [configuration file](#configuration-file).

A package exports a body only for the queues in its own capture list, redacted
([span contract 15](contract/span-contract.md#15-message-contents-revision-32)).
`TRAILBEAM_STORE_BODIES` is the second gate: for any queue not in it (`*` means every queue), Trailbeam
removes the body attributes before writing the span, and records that it did. The default is no queue.

Each body is written to disk once per message, even though the send span and every receive span carry
it; the spans point at that one copy. (A receive that carried a different text keeps its own.) With 1 KB
bodies on every queue, that cut the space bodies take by 63% in our benchmark: 500 MB instead of 1,341 MB
per million spans. A file from an earlier version is converted when this version first opens it (schema
10, one pass over the stored bodies). Run `trailbeam --vacuum` once afterwards to give the space back.

The bodies it keeps are on the message page's **Body** tab, one per message (the send's copy, else the
first receive's). A message with no body says why: the package's capture status, a producer that isn't
instrumented, a package from before 3.2, or a body this server dropped because its queue isn't in
`TRAILBEAM_STORE_BODIES`. The Messages page searches them with `body.*` terms such as
`body.order.sku:A-100` or `body.amount:>=100`, and so does `GET /api/messages?body=...`. The rules are in
[`contract/body-view.md`](contract/body-view.md); the hosted app follows the same ones and draws the same view.

**A body search is bounded.** Bodies have no index, so a search reads them. It reads only the messages
last seen in a time window: `from_ms`/`to_ms`, or the last `since_s` seconds, by default a day. Pass
`queue` to search one queue's messages. It goes newest first and stops after `limit` matches or 5,000
messages read, whichever comes first. `next_cursor` continues from where it stopped, so a page can have
few matches, or none, and still have a next one. `body_search` in the response says what was covered:
`scanned` (messages read), `searched_from_ms` to `to_ms`, and `complete` (the whole window was read). A
search therefore costs about the same however many bodies are stored. Tenant search
(`/api/tenants/search`) takes the same `from_ms`/`to_ms`/`since_s` and `queue`.

**Who can read them.** Anyone Trailbeam lets in can read every body it keeps, in the Body tab and from
`/api/messages/{id}/body`. Without an [access token](#access-token), that is anyone who can reach it.
Anyone who can read its SQLite file can read them there too. Trailbeam logs a warning at startup when
the list is set. Set a token, or keep it on a trusted network. Redaction happens in the packages, not in Trailbeam. Taking a queue
off the list stops its bodies being shown or searched, but doesn't delete the ones already in the file;
they go when retention deletes their messages ([Retention](#retention)).

### Retention

Trailbeam keeps spans for **7 days** by default, then deletes them, bodies included. Set a different
retention for every queue, override it for some queues, or keep data forever:

```sh
docker run ... -e TRAILBEAM_RETENTION=14d -e TRAILBEAM_RETENTION_QUEUES=orders-dlq=30d,audit=forever trailbeam
# or trailbeam --retention 14d --retention-queues orders-dlq=30d,audit=forever
```

or `retention` under `[defaults]` and `[queues.<name>]` in the [configuration file](#configuration-file).

A retention is a duration in `s`, `m`, `h` or `d` (`90m`, `36h`, `7d`), or `forever`.
`TRAILBEAM_RETENTION=forever` with no per-queue overrides turns the purge off.

**Messages are deleted whole, never in part.** A message is deleted once its *newest* span is older than
the retention of its queue, so a timeline is either complete or gone. That has three consequences:

- A message whose spans straddle the cutoff (for example, sent 8 days ago and received again yesterday)
  keeps all of its spans, including the old ones, until its last span expires.
- A message with spans on several queues, such as one moved to a dead-letter queue, is kept for as long
  as the longest retention among those queues. `orders-dlq=forever` keeps those messages' spans on `orders` too.
- A settle span that no receive has claimed yet (a delete or visibility change whose receipt handle
  Trailbeam never saw) belongs to no message. It is deleted by its own time and queue.

Age is the span's end time, measured against the server's clock.

**The purge runs in the background.** It starts when the server starts, then runs again every
`TRAILBEAM_PURGE_INTERVAL_S` seconds (default 600). It looks at up to `TRAILBEAM_PURGE_BATCH` messages
(default 50) per short write transaction and pauses between them so it holds the writer for at most
`TRAILBEAM_PURGE_DUTY` of the time (default 0.25). Ingest and pages never wait behind a whole pass.
`GET /api/health` shows the settings and the last pass under `config.retention`.

**How fast the purge deletes.** Deleting spans costs about as much disk work as writing them, and it
shares SQLite's one writer with ingest. On our test machine (1M stored spans, 1 KB bodies, ingest at
1,000 spans/s during the pass) the purge deleted about 220 spans/s at the default batch and about 80
spans/s at `--purge-batch 15`. Exports stayed under 2 s p99 in both, with one 30 s window at 2.5 s at
the default batch. So retention keeps up only while your *average* ingest is below the purge rate; above
that the file keeps growing. Measured on one machine; your disk will give different numbers. Raising
`TRAILBEAM_PURGE_DUTY` deletes faster but makes ingest wait longer.

**Disk space.** A database file created by this version gives the deleted space back to the disk
(SQLite incremental vacuum, a few pages at a time after each pass). A file created by an earlier version
reuses the freed space for new spans, so it stops growing, but it does not shrink. To shrink it, run a
one-time rewrite while the server is stopped:

```sh
trailbeam --db /data/trailbeam.sqlite --vacuum
```

This copies the whole file (SQLite `VACUUM`), so it needs enough free disk for a second copy. Run it
once; after that the file gives space back like a new one.

**Upgrading a database file.** The server upgrades an older file when it starts, keeping every span.
One upgrade in this version rewrites the spans table and its summaries. That cuts the disk writes per
span by about a third. It takes a while on a large file: about 5 minutes per million spans on our test
machine. While it runs, the server doesn't accept spans, and it needs free disk about the size of the
file. Afterwards the file is larger by about that much free space, which new spans reuse. If the
upgrade is stopped part way, the file stays at the version before, and the next start runs it again.
A span the server already has is skipped when it arrives again with the same trace ID, span ID and end
time, as an exporter retry sends it.

### Postgres (bring your own)

SQLite is the default and needs nothing else. When you outgrow one file, point Trailbeam at a Postgres you
already run:

```sh
pip install "trailbeam[server,postgres]"
TRAILBEAM_STORE_URL=postgres://trailbeam:<password>@db.internal:5432/trailbeam trailbeam
# or trailbeam --store-url postgres://...
```

Trailbeam creates and upgrades its own tables in that database (schema version in `schema_version`). Installing,
sizing, backing up and upgrading Postgres is up to you. **Supported: Postgres 14 and later**; CI runs the
storage conformance suite on 14 and 16. The Postgres driver is an extra: a SQLite install doesn't get it, and it
is loaded only when the URL is a Postgres one. Existing SQLite history is not copied over: a new Postgres store
starts empty.

Retention works as described above, with one difference: spans are kept in **daily partitions** per retention
(a partition per day for each distinct retention you configure), and the purge drops whole partitions instead
of deleting rows, so it costs about the same however much it removes. A message expires once the *day* of its
newest span has ended more than its retention ago, so `7d` keeps a message for 7 to 8 days. Messages are still
deleted whole: when a message's spans fall in two partitions (it crossed midnight, or moved to a dead-letter
queue kept longer), the purge first moves its older spans in with the newer ones. If you change a queue's
retention after its spans were stored, partitions holding queues with different retentions are cleaned by a
bounded background delete, outside ingest.

Ingest and pages use separate connection pools (4 and 8 connections). Each export is one transaction, and an
export whose client hangs up or times out is cancelled in the database.

### Several replicas on one Postgres

With Postgres, Trailbeam servers are stateless: run as many replicas as you like behind a load balancer, for
Kubernetes or for high availability. No sticky sessions are needed.

- **Joins are correct across replicas.** A delete and its receive may reach different replicas, in either
  order; the receipt-hash join and every message's summary are kept under Postgres locks, so the timelines are
  exactly those of a single Trailbeam (the test suite sends each span to a random replica in a random order and
  compares).
- **Starting together is fine.** The schema is created or upgraded by one replica under a Postgres advisory
  lock; the others wait for it.
- **Retention runs on one replica at a time**, the one holding a lease (a Postgres advisory lock). When that
  replica stops or dies, Postgres releases the lease and another replica takes over at its next pass. Give
  every replica the same retention and per-queue configuration.
- **Health checks.** `GET /api/health` is liveness: it answers while the process runs, even when the
  database is unreachable, so a database outage doesn't restart every replica. `GET /api/ready` is readiness:
  `503` while the store is unreachable, so the load balancer takes that replica out of rotation. Neither needs
  the token. While the database can't be reached, exports are answered `503` with `Retry-After`; nothing is
  stored and the exporter sends them again.
- **The shared token** (`TRAILBEAM_TOKEN`) is set to the same value on every replica. Its session cookie is
  derived from the token, so a sign-in on one replica works on all of them.
- **A replica that dies loses nothing acknowledged.** An export is acknowledged (`200`) only once its
  transaction has committed. One in flight on a replica that dies isn't acknowledged, so the exporter retries it
  (the OpenTelemetry OTLP exporter retries connection errors and `503`), and the load balancer sends it to
  another replica. A span stored twice is skipped by its key.
- The ingest counters in `/api/health` are per replica: each counts what it received.

Examples: [`deploy/replicas/compose.yaml`](deploy/replicas/compose.yaml), two replicas and Postgres behind
nginx (`docker compose -f deploy/replicas/compose.yaml up -d --build --wait`). And
[`deploy/kubernetes/trailbeam.yaml`](deploy/kubernetes/trailbeam.yaml), a Deployment of two replicas with liveness
and readiness probes and a Service, on a Postgres you run. The image needs the Postgres driver:
`docker build --build-arg TRAILBEAM_EXTRAS=server,postgres .`

### When Trailbeam is overloaded: 429, 503 and Retry-After

Trailbeam answers an export it can't take right away. It doesn't leave it hanging until the exporter
times out. Both checks run before anything is written. A 429 or 503 means none of the export's spans
were stored. A 200 means all of them were committed. Trailbeam never answers 200 and then drops a span.

| Answer | When | Setting |
|---|---|---|
| `429 Too Many Requests` | This many exports are already in progress (decoding, waiting for the database writer, or writing). The export isn't decoded. | `TRAILBEAM_INGEST_MAX_PENDING` / `--ingest-max-pending`, default 16 |
| `503 Service Unavailable` | The export waited this long for the SQLite writer, behind the exports ahead of it | `TRAILBEAM_INGEST_MAX_WAIT_S` / `--ingest-max-wait`, default 2 |

Both answers carry `Retry-After` in whole seconds, from 1 to 3. The value is how long the exports
already waiting should take to write. The body is an OTLP `google.rpc.Status`. OTLP exporters retry
429 and 503 and wait the `Retry-After` time
([OTLP/HTTP throttling](https://opentelemetry.io/docs/specs/otlp/#otlphttp-throttling)). That includes
the OTel SDKs' `OTLPSpanExporter` and the Collector's `otlphttp` exporter. The Python exporter retries
only within its 10 s export timeout, which is why the wait and `Retry-After` are kept short. If every
retry is rejected too, the SDK drops that batch on its side. Trailbeam counts each rejection in
`trailbeam_ingest_rejected_requests_total`, so the drop shows up there and isn't silent. To ride out
longer overloads, send through a Collector with a sending queue ([`collector/`](collector/README.md)).

### Metrics: `GET /metrics`

Trailbeam serves Prometheus metrics on its own port, at `/metrics`. In 0.1 that endpoint has no
authentication, like the rest of Trailbeam. Labels never hold span data: routes are templates such as
`/api/messages/{id}`.

| Metric | Type | What |
|---|---|---|
| `trailbeam_ingest_spans_total{result}` | counter | Spans in accepted exports: `inserted`, `duplicate` (already stored, e.g. a retry), `invalid` (rejected by the span contract), `ignored` (not Trailbeam spans). `rate()` gives spans/s |
| `trailbeam_ingest_requests_total{code}` | counter | Exports to `/v1/traces` by response status |
| `trailbeam_ingest_rejected_requests_total{reason}` | counter | 429s (`queue_full`) and 503s (`writer_behind`) |
| `trailbeam_ingest_request_duration_seconds{outcome}` | histogram | Export latency, from body received to response: `accepted`, `rejected`, `error` |
| `trailbeam_ingest_writer_wait_seconds` | histogram | Time an export waited for the SQLite writer |
| `trailbeam_ingest_write_duration_seconds` | histogram | One export's write transaction |
| `trailbeam_ingest_queue_depth`, `trailbeam_ingest_queue_capacity` | gauge | Exports in progress, and the most allowed before a 429 |
| `trailbeam_ingest_max_wait_seconds`, `trailbeam_ingest_retry_after_seconds` | gauge | The 503 threshold, and the Retry-After a rejected export would get now |
| `trailbeam_join_duration_seconds{join}` | histogram | One receipt-hash join at insert: `settle` or `receive` |
| `trailbeam_http_request_duration_seconds{route}` | histogram | API and page latency by route |
| `trailbeam_http_requests_total{route,code}` | counter | API and page requests by route and status |
| `trailbeam_db_file_bytes{file}` | gauge | SQLite file (`main`) and write-ahead log (`wal`) size |
| `trailbeam_db_last_row` | gauge | Highest span row ID (one index step, not a count) |
| `trailbeam_build_info{version}`, `trailbeam_start_time_seconds` | gauge | Version and start time |

Retention purge metrics come with the retention setting.

A scrape config for Prometheus:

```yaml
scrape_configs:
  - job_name: trailbeam
    scrape_interval: 15s
    metrics_path: /metrics
    static_configs:
      - targets: ["trailbeam:4318"]   # host:port Trailbeam listens on
```

Useful alerts and queries:

```promql
sum(rate(trailbeam_ingest_spans_total{result="inserted"}[5m]))                      # spans/s stored
sum by (reason) (rate(trailbeam_ingest_rejected_requests_total[5m])) > 0           # backpressure: alert
histogram_quantile(0.99, sum by (le) (rate(trailbeam_ingest_request_duration_seconds_bucket{outcome="accepted"}[5m])))
histogram_quantile(0.95, sum by (le, route) (rate(trailbeam_http_request_duration_seconds_bucket[5m])))
trailbeam_ingest_queue_depth / trailbeam_ingest_queue_capacity > 0.8               # close to 429s
```

## Open source and the hosted app

Everything that tells you what happened to your messages is open source and runs on your own
machine. The hosted app at [trailbeam.app](https://trailbeam.app) adds what needs hosting or a team.
It is not generally available yet.

| | Open source | Hosted app |
|---|---|---|
| The packages for your services | yes | yes, the same packages with an ingest key |
| A message's timeline, lookup by message ID | yes | yes |
| Lambda error text on the timeline, errors grouped by cause | yes | yes |
| Stuck messages and duplicate deliveries | yes | yes |
| Overview of flows between services and queues | coming | yes |
| Event explorer | coming | coming |
| JSON API | yes | |
| Accounts, workspaces and teams | | yes |
| Dashboards | | yes |
| Retention: global, per queue, or forever | yes | yes |
| Hosting and backups | | yes |

Both show the same timeline from the same rules: the hosted app draws it with this repository's
`@trailbeam/ui`, and its message pages are checked against this repository's code and the shared span
fixtures in [`contract/fixtures/`](contract/fixtures/).

## How it works

1. OpenTelemetry hooks in your services emit one span per message for each SQS call your code makes:
   `SendMessage`, `ReceiveMessage`, `DeleteMessage`, `ChangeMessageVisibility` and their batch forms.
   A Lambda wrapper does the same for SQS events.
2. The spans go over OTLP/HTTP to Trailbeam, directly or through an OpenTelemetry Collector.
3. Trailbeam keeps the spans in SQLite. It joins each delete to the receive whose receipt handle it
   used, through a keyed hash of the handle.
4. When you open a message, Trailbeam builds its timeline.

Full explanation: [How Trailbeam works](https://docs.trailbeam.app/concepts/how-it-works).

## What Trailbeam does not do

- It makes no SQS calls of its own, and never calls `ReceiveMessage` on your queues. Trailbeam makes
  no AWS calls.
- It does not store message bodies unless you opt the queue in, both in the package and on the server
  ([Message bodies](#message-bodies)). It does not record message attributes, except the one you name
  as the tenant attribute.
- It does not export receipt handles, only a keyed hash of each one.

## Documentation

The documentation is at [docs.trailbeam.app](https://docs.trailbeam.app).

| Page | Contents |
|---|---|
| [Quickstart](https://docs.trailbeam.app/quickstart) | From a clone to one message's timeline, on the local demo |
| [How Trailbeam works](https://docs.trailbeam.app/concepts/how-it-works) | Components, data flow, what is recorded and what is inferred |
| [Run Trailbeam](https://docs.trailbeam.app/setup/run) | Run Trailbeam, natively or in a container |
| [HTTP API](https://docs.trailbeam.app/reference/api) | Trailbeam's JSON API |
| [Vocabulary](https://docs.trailbeam.app/concepts/vocabulary) | Terms |

The reference for each part of this repository is next to its code: [`python/`](python/README.md),
[`node/`](node/README.md), [`collector/`](collector/README.md) and [`demo/`](demo/README.md). The span
contract, the specification the packages and Trailbeam both follow, is
[`contract/span-contract.md`](contract/span-contract.md).

## Repository layout

| Directory | Contents |
|---|---|
| [`python/`](python/) | Python package `trailbeam`: boto3 hooks, a Lambda handler wrapper, and Trailbeam itself (`trailbeam.server`, installed with the `server` extra) |
| [`node/`](node/) | Node.js package `trailbeam`: AWS SDK v3 hooks and a Lambda handler wrapper |
| [`ui/`](ui/) | `@trailbeam/ui`: the views Trailbeam's web screen and the hosted app share (the timeline and the Tenants table so far) |
| [`Dockerfile`](Dockerfile) | The Trailbeam container: `trailbeam[server]` with SQLite on a volume |
| [`collector/`](collector/) | OpenTelemetry Collector configuration |
| [`demo/`](demo/) | Local demo on ElasticMQ with Docker Compose, and the concurrency check (`demo/check.sh`) |
| [`contract/`](contract/) | The span contract, the receipt-hash test vectors, the Tenants query ([`tenants.md`](contract/tenants.md)) and the recorded contract 3.2 spans both Trailbeam and the hosted app test against ([`fixtures/`](contract/fixtures/)) |

## Development

Run each line from the repository root:

```sh
(cd python && pip install -e '.[test]' && pytest)   # the Python package and Trailbeam itself
(cd node && npm ci && npm test)
(cd ui && npm ci && npm test && npm run build)   # the shared UI, built into the Python package
pytest demo/check/tests
```

The Node.js tests that need an SQS emulator are skipped unless `TRAILBEAM_TEST_SQS_ENDPOINT` is set, for
example to `http://localhost:9324`. [`.github/workflows/ci.yml`](.github/workflows/ci.yml) also runs
the Collector tests, the container smoke test, the demo and the concurrency check.

## Contributing

Issues and pull requests are welcome. Before opening a pull request, run the tests above for the
part you changed. If your change alters a span, update
[`contract/span-contract.md`](contract/span-contract.md) in the same pull request.

## License

[Apache-2.0](LICENSE).
