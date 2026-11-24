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
  <a href="https://pypi.org/project/trailbeam/"><img src="https://img.shields.io/pypi/v/trailbeam" alt="PyPI version"></a>
  <a href="https://www.npmjs.com/package/trailbeam"><img src="https://img.shields.io/npm/v/trailbeam" alt="npm version"></a>
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
> The latest release, 0.4.0, is on PyPI and npm (`trailbeam`) and on ghcr.io
> (`ghcr.io/nick-lennox/trailbeam`). This README follows `main`, which can be ahead of the release:
> see Unreleased in the [CHANGELOG](CHANGELOG.md).

## Features

- **One timeline per message.** Every message your services send or receive, and every delete and
  visibility change they make, joined into a single history you open by message ID. Failed sends,
  empty receives and moves back from a dead-letter queue are not recorded
  ([what is not recorded](https://docs.trailbeam.app/concepts/what-is-recorded#what-is-not-recorded)).
- **Purges.** A `PurgeQueue` call is recorded for the queue. A message whose last step ended before the
  purge started reads "purged (observed: PurgeQueue at …)" and leaves Stuck. One whose last step ended
  during the purge or within 60 s after it stays on Stuck, marked "possibly purged": SQS may or may not
  have deleted it. A message that SQS drops when its retention period ends can't be seen, so it stays on
  Stuck until Trailbeam's own retention removes it.
- **Recorded and inferred steps are kept apart.** Trailbeam orders two events only when an observed
  fact proves the order, and marks every step it inferred, such as a visibility timeout that expired.
- **Deletes joined to their receives.** A delete carries a receipt handle, not a message ID. Trailbeam
  joins the two through a keyed hash of the handle, so the handle itself never leaves your process.
- **Dead-letter moves, as SQS reports them.** A move is shown when a receive on the dead-letter queue
  carries `DeadLetterQueueSourceArn`. It is never guessed from settings.
- **Lambda outcomes.** A handler wrapper records how each record ended: success, a reported batch
  item failure, or an exception.
- **Message bodies, opt-in.** For queues you opt in, both in the package and on the server, the
  packages export a redacted copy of each body. A message's **Body** tab shows it, and the Messages
  page searches bodies with `body.*` terms ([Message bodies](#message-bodies)).
- **Lambda error text.** For queues you list, the Lambda wrapper records the handler's error text, and
  the timeline groups errors by cause.
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

Start the server. This runs the published image, keeps its data in a Docker volume and listens on
this machine only:

```sh run variant=docker wait=http://127.0.0.1:4318/api/ready
docker run -d --name trailbeam -p 127.0.0.1:4318:4318 -v trailbeam-data:/data ghcr.io/nick-lennox/trailbeam
```

No Docker? Run `pip install "trailbeam[server]"`, then `trailbeam`. It keeps spans in `trailbeam.sqlite`
in the current directory.

Install the package in a service that uses SQS:

```sh run variant=docker
pip install trailbeam      # Python
npm install trailbeam      # Node.js
```

Instrument the client where you create it:

```python file=app.py variant=docker
import boto3
import trailbeam.boto

trailbeam.boto.install()  # once per process
sqs = boto3.client("sqs")
```

```js file=app.js variant=docker
const { SQSClient } = require("@aws-sdk/client-sqs");
const trailbeam = require("trailbeam");

const sqs = trailbeam.instrument(new SQSClient({}));
```

Send, receive or delete a message, then open http://127.0.0.1:4318/ and click its message ID. A Node.js
script that ends by calling `process.exit()` must call `await trailbeam.uninstall()` first; otherwise the
package sends its last spans when the script ends.
If Trailbeam is down, slow, or given the wrong token, your service keeps running at full speed and its exit
waits at most 2 s; the lost spans are counted and logged. What happens to spans in each case:
[Python](python/README.md#when-the-server-is-down-or-the-process-ends),
[Node.js](node/README.md#when-the-server-is-down-or-the-process-ends).

On the same machine nothing else is needed: the packages send to `http://localhost:4318/v1/traces` by
default. Each service prints a warning that it uses the published default receipt-hash key until you set
your own. For a server on another machine, an access token and a shared key, see
[Add it to your services](#add-it-to-your-services) and [Self-hosting](#self-hosting).

### `trailbeam start`: a token and key made for you

After `pip install "trailbeam[server]"`, `trailbeam start` creates an access token
and a receipt-hash key in `~/.trailbeam` (`--data-dir` or `TRAILBEAM_DATA_DIR` to choose another directory),
readable only by you, and keeps the SQLite file there. It starts the server on `http://127.0.0.1:4318/` and
prints the token and the environment variables for your services:

<!-- doc-test variant=start bg wait=http://127.0.0.1:4318/api/ready
pip install "trailbeam[server]"
trailbeam start
-->

```sh expect variant=start
export TRAILBEAM_OTLP_TRACES_ENDPOINT=http://127.0.0.1:4318/v1/traces
export OTEL_EXPORTER_OTLP_HEADERS=Authorization=Bearer%20<token>
export TRAILBEAM_RECEIPT_HASH_KEY=<key>
export OTEL_SERVICE_NAME=my-python-service
```

Because the token is on, services need these settings, and the web screen asks for the token once. Later
runs reuse the same token and key, and print them in full only on the run that created them: stdout often ends
up in a log (systemd, `docker run -d`, nohup, CI). A later run prints the command that shows each one
(`cat ~/.trailbeam/token`, `cat ~/.trailbeam/receipt-hash-key`), and its export lines read the files, so
pasting them doesn't put the secrets in your shell history either:

```sh run variant=start
export OTEL_EXPORTER_OTLP_HEADERS=Authorization=Bearer%20$(cat ~/.trailbeam/token)
export TRAILBEAM_RECEIPT_HASH_KEY=$(cat ~/.trailbeam/receipt-hash-key)
```

A token you give with `--token-file` or `TRAILBEAM_TOKEN` is never printed. If `TRAILBEAM_RECEIPT_HASH_KEY` is
already set (at least 16 characters), `trailbeam start` uses that key and makes no key file, so services you set
up before keep joining: the export lines say `$TRAILBEAM_RECEIPT_HASH_KEY`, and if the data directory already
holds a different key, the environment variable wins and one warning says so. Every `trailbeam` flag works after
`start` (`trailbeam start --help`). Stop it with Ctrl-C. `npx trailbeam start` does the
same with Docker and no Python ([Node.js README](node/README.md#run-the-server-npx-trailbeam-start)). Both need
0.4.0 or later. Nothing on the screen? Run `trailbeam doctor` ([Troubleshooting](#troubleshooting-no-data-arriving)).

### Runnable examples

[`examples/`](examples/README.md) has the smallest complete service for each common setup, each started
with one command against an SQS emulator and a local Trailbeam (Docker, no AWS account):
[Python with boto3](examples/python-boto3/README.md),
[Python Lambda with an SQS trigger](examples/python-lambda/README.md),
[Node.js with AWS SDK v3](examples/node-sdk-v3/README.md) and
[Node.js Lambda with an SQS trigger](examples/node-lambda/README.md). Each sends three orders whose
timelines show a message processed, a retry after the visibility timeout, and a move to the dead-letter
queue. CI runs every one of them.

```sh norun="CI runs every example in its own job (examples, .github/workflows/ci.yml)"
git clone https://github.com/nick-lennox/trailbeam.git && cd trailbeam
docker compose -f examples/python-boto3/compose.yaml up --build
# then open the links it prints
```

### See a richer example

The [demo](demo/README.md) runs Trailbeam, an SQS emulator and sample services with Docker and no AWS
account. It builds images from a clone and starts seven containers:

```sh norun="CI runs the demo in its own job (demo, .github/workflows/ci.yml)"
git clone https://github.com/nick-lennox/trailbeam.git && cd trailbeam
docker compose -f demo/compose.yaml up -d --build --wait
# then open http://127.0.0.1:4318/ and click a message ID
```

### Try it on your own service without AWS

Run ElasticMQ, an SQS emulator, and point the AWS SDK at it. It accepts any keys:

```sh run variant=docker wait=http://127.0.0.1:9324/?Action=ListQueues
docker run -d --name sqs -p 127.0.0.1:9324:9324 softwaremill/elasticmq-native:1.6.15
export AWS_ENDPOINT_URL_SQS=http://localhost:9324
export AWS_REGION=us-east-1           # read by the AWS SDK for JavaScript
export AWS_DEFAULT_REGION=us-east-1   # read by boto3
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
```

Keep port 9324: ElasticMQ puts it in queue URLs, and the JavaScript SDK sends requests to that host.
Create a queue from your code (`CreateQueue`), add the hooks below, and send. A Python and a Node.js
script that do this end to end are in
[Try Trailbeam without AWS](https://docs.trailbeam.app/get-started/without-aws).

<!-- doc-test variant=docker
pip install boto3
npm install @aws-sdk/client-sqs
cat app.py - > send.py <<'EOF'
url = sqs.create_queue(QueueName="orders")["QueueUrl"]
sqs.send_message(QueueUrl=url, MessageBody="from python")
for m in sqs.receive_message(QueueUrl=url, WaitTimeSeconds=1).get("Messages", []):
    sqs.delete_message(QueueUrl=url, ReceiptHandle=m["ReceiptHandle"])
EOF
python send.py
cat app.js - > send.js <<'EOF'
const { CreateQueueCommand, SendMessageCommand } = require("@aws-sdk/client-sqs");
(async () => {
  const { QueueUrl } = await sqs.send(new CreateQueueCommand({ QueueName: "orders" }));
  await sqs.send(new SendMessageCommand({ QueueUrl, MessageBody: "from node" }));
})();
EOF
node send.js
for i in $(seq 20); do
  n=$(curl -fsS http://127.0.0.1:4318/api/messages | python -c 'import json, sys; print(len(json.load(sys.stdin)["messages"]))')
  [ "$n" = 2 ] && break; sleep 1
done
test "$n" = 2
-->

## Add it to your services

Add the hooks to every service that sends, receives or deletes on the queues you care about. A
service without them leaves gaps in a message's history.

Install and instrument boto3 and AWS SDK v3 clients as in the [Quickstart](#quickstart). The Python
package instruments every boto3 and botocore client in the process, but not aiobotocore. The Node.js
package instruments only the clients you pass to `trailbeam.instrument()`, and only AWS SDK v3. For AWS
Lambda, wrap the handler:

```python file=handler.py variant=docker
import trailbeam.aws_lambda

@trailbeam.aws_lambda.wrap
def handler(event, context):
    ...
```

<!-- doc-test variant=docker
python -c 'import handler'
-->

The packages send to `http://localhost:4318/v1/traces` unless `TRAILBEAM_OTLP_TRACES_ENDPOINT` says
otherwise, so a service on the server's machine needs no endpoint. Give every service on the same queues
the same receipt-hash key, and a service name. For a server on another host, set the endpoint too:

```sh run variant=docker
export TRAILBEAM_OTLP_TRACES_ENDPOINT=http://trailbeam.internal:4318/v1/traces   # only if the server is elsewhere
export TRAILBEAM_RECEIPT_HASH_KEY=$(openssl rand -hex 32)   # generate once, share with every service
export OTEL_SERVICE_NAME=orders-worker
```

If the server has an [access token](#access-token), also set
`OTEL_EXPORTER_OTLP_HEADERS=Authorization=Bearer%20<token>`.

Full guides: [Python](python/README.md), [Node.js](node/README.md), and the Lambda sections in each. A Java
jar for the AWS SDK for Java v2 is in [`java/`](java/README.md); it is pre-release and not published yet.

### Supported versions

<!-- compat-matrix:start (generated from compat/matrix.toml by compat/matrix.py; don't edit by hand) -->

| | Supported and tested every night |
|---|---|
| Python | 3.10, 3.11, 3.12, 3.13, 3.14, each with the SQLite built into it |
| boto3 / botocore | 1.34.0 / 1.34.0 to the newest |
| OpenTelemetry for Python | API and SDK 1.28.0, `opentelemetry-instrumentation-botocore` 0.49b0, to the newest |
| Node.js | 20, 22, 24 |
| AWS SDK for JavaScript v3 (`@aws-sdk/client-sqs`) | 3.572.0 to the newest |
| Postgres (the server's store) | 14, 15, 16, 17, 18 |

Each Python version runs with the oldest and the newest Python dependencies, and each Node.js version with the oldest and the newest AWS SDK, so 10 Python runs, 6 Node.js runs and 5 Postgres runs ([compat/matrix.toml](compat/matrix.toml), [the nightly workflow](.github/workflows/compat.yml)).

Not supported:

- The Python package instruments boto3 and botocore clients only, not aiobotocore (asyncio) clients.
- The Node.js package instruments AWS SDK for JavaScript v3 clients only, not v2 (`aws-sdk`).

<!-- compat-matrix:end -->

To change what is tested, edit [compat/matrix.toml](compat/matrix.toml) and run `python3 compat/matrix.py readme`.

### Tenants

To see traffic per customer, tell the packages where the tenant is. Set the same value in every
service that sends or receives on those queues:

```sh run variant=docker
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
sends, p95 time in queue (send to first receive, within 1% of the exact value) and dead-lettered messages,
per tenant. A tenant's
name links to its messages (`/?tenant=acme`), and the search box in the header suggests tenants
named in the page's range (default the last day) as you type. The same data is in the JSON API:
`GET /api/tenants`, `GET /api/tenants/search?q=` (with `since_s` or `from_ms`/`to_ms`, and `queue`) and
`GET /api/messages?tenant=`. How every number is counted is in
[`contract/tenants.md`](contract/tenants.md). The hosted app counts them the same way and is tested
against the same recorded spans.

## Troubleshooting: no data arriving

Set it up and see nothing? Run `trailbeam doctor` (0.4.0 or later; 0.3.0 stops with
`unrecognized arguments: doctor`). It comes with the Python package (installing `trailbeam`
is enough, the server extra is not needed) and checks, one line each:

1. **Server reachable**: `GET /api/health` answers, and it is Trailbeam;
2. **Ingest port accepts connections**: the address your services send to (4318 by default) is open and is
   Trailbeam's `/v1/traces`;
3. **Token accepted**: by the API and by ingest (or the server has no token);
4. **Test span stored and read back**: it sends a test span and reads it back. The test span is marked as such
   (span contract, [section 17](contract/span-contract.md#17-the-test-span-of-trailbeam-doctor-qv-148)), so it
   never shows as a message or in any count;
5. **Last real span**: when the newest span from your services was stored. If none was, it says whether spans
   arrived that are not Trailbeam's (the hooks are not installed) or broke the span contract.

```console norun="sample output: the hidden steps after it run trailbeam doctor and check these lines"
$ trailbeam doctor
trailbeam doctor: server http://127.0.0.1:4318, ingest http://127.0.0.1:4318/v1/traces, token: OTEL_EXPORTER_OTLP_HEADERS

  ok    Server reachable: Trailbeam X.Y.Z answers at http://127.0.0.1:4318
  ok    Ingest port accepts connections: 127.0.0.1:4318 accepts connections (http://127.0.0.1:4318/v1/traces)
  ok    Token accepted: the API and ingest accept the token from OTEL_EXPORTER_OTLP_HEADERS
  ok    Test span stored and read back: sent to http://127.0.0.1:4318/v1/traces and read back from http://127.0.0.1:4318 in 7 ms
  FAIL  Last real span: no span stored yet
        Likely cause: Trailbeam works (the test span went through), so your services are not sending to it: ...
        Fix: in each service, call trailbeam.boto.install() (Python), ... and set TRAILBEAM_OTLP_TRACES_ENDPOINT=...

1 of 5 checks failed.
```

<!-- doc-test variant=start
trailbeam doctor || test $? = 1
-->

<!-- doc-test expect variant=start
ok    Server reachable: Trailbeam <version> answers at http://127.0.0.1:4318
ok    Ingest port accepts connections: 127.0.0.1:4318 accepts connections (http://127.0.0.1:4318/v1/traces)
ok    Token accepted: the API and ingest accept the token from OTEL_EXPORTER_OTLP_HEADERS
ok    Test span stored and read back
FAIL  Last real span: no span stored yet
1 of 5 checks failed.
-->

Each failed check says the likely cause and the fix ([every failure](https://docs.trailbeam.app/reference/troubleshooting#no-data-arrives)). A check that needs an earlier one that failed is skipped.
The exit code is 1 when a check failed, so it can run in a script or a CI step.

It uses the token given with `--token-file` or `TRAILBEAM_TOKEN`; else the one your services send, the Bearer
token in `OTEL_EXPORTER_OTLP_HEADERS` (or `OTEL_EXPORTER_OTLP_TRACES_HEADERS`), so with the variables
`trailbeam start` prints set it checks the exact header the services send; else the token `trailbeam start`
created (`~/.trailbeam/token`, or the one in `TRAILBEAM_DATA_DIR`). It checks the server given with `--url` (or `TRAILBEAM_URL`,
or the port in `TRAILBEAM_PORT`). Without one, it checks the server `trailbeam start` started: the host and port
of `TRAILBEAM_OTLP_TRACES_ENDPOINT` (the variable `trailbeam start` prints), or the address it last listened on,
which it keeps in the data directory (`~/.trailbeam/address`). So after `trailbeam start --port 4319`, with the
printed variables set, plain `trailbeam doctor` checks 4319. When neither is there, it checks
`http://127.0.0.1:4318`. The first line says which server it checked and where that address came from. For the
Compose team install with another `TRAILBEAM_PORT`, give `--url` (or set `TRAILBEAM_PORT` in the shell). When
your services send somewhere else than the server, such as through a Collector, give that address with
`--ingest-url`; it reads `TRAILBEAM_OTLP_TRACES_ENDPOINT` as the services do, and then takes the server from
`--url` or from the address `trailbeam start` kept. Run it where your services run to check what they can reach. After `npx trailbeam start` the token
is in the Docker volume, so give the one it printed: `TRAILBEAM_TOKEN=<token> trailbeam doctor`. The
image has the command too: `docker exec <container> trailbeam doctor --token-file /data/token` checks from
inside the container (`docker ps` gives its name).

## Self-hosting

Trailbeam is one container. It receives OTLP on `/v1/traces` and serves the web screen and the JSON
API on the same port.

The [Quickstart](#quickstart) starts it with `docker run` or with `pip install "trailbeam[server]"` and
`trailbeam`; both take the flags below. To build the image yourself: `docker build -t trailbeam .` from a
clone.

### Team install (Docker Compose with Postgres)

For a team: one server several people's services send to, on its own Postgres, from one file. The [team guide](https://docs.trailbeam.app/get-started/team) takes you from here to the first instrumented services.
[`docker-compose.yaml`](docker-compose.yaml) runs the Trailbeam server and Postgres 16, each with a named
volume, health checks and `restart: unless-stopped`. The token and passwords come from a `.env` file:

```sh run variant=team
git clone https://github.com/nick-lennox/trailbeam.git && cd trailbeam
install -m 600 .env.example .env   # only you can read it
```

Fill in `TRAILBEAM_TOKEN`, `TRAILBEAM_RECEIPT_HASH_KEY` and `POSTGRES_PASSWORD` in `.env` (make each with
`openssl rand -hex 32`), then start it:

<!-- doc-test variant=team
sed -i -e "s/^TRAILBEAM_TOKEN=.*/TRAILBEAM_TOKEN=$DOC_TOKEN/" \
  -e "s/^TRAILBEAM_RECEIPT_HASH_KEY=.*/TRAILBEAM_RECEIPT_HASH_KEY=$DOC_RECEIPT_HASH_KEY/" \
  -e "s/^POSTGRES_PASSWORD=.*/POSTGRES_PASSWORD=$(openssl rand -hex 32)/" \
  -e "s/^TRAILBEAM_BIND=.*/TRAILBEAM_BIND=127.0.0.1/" .env
-->

```sh run variant=team timeout=1200
docker compose up -d --build --wait
# then open http://<this machine>:4318/ and sign in with the token
```

`.env` holds the token and the Postgres password, so keep it readable by its owner only: `install -m 600`
does that, where a plain `cp` would leave it readable by every user on the server. For an existing `.env`, run
`chmod 600 .env`.

In every service that uses the Trailbeam packages, set the endpoint, the token and the same receipt-hash key:

```sh norun="settings for your services, not commands; the hidden step after it sends a span with the token"
TRAILBEAM_OTLP_TRACES_ENDPOINT=http://<this machine>:4318/v1/traces
OTEL_EXPORTER_OTLP_HEADERS=Authorization=Bearer%20<TRAILBEAM_TOKEN>
TRAILBEAM_RECEIPT_HASH_KEY=<the same key as in .env>
```

<!-- doc-test variant=team
TRAILBEAM_TOKEN=$DOC_TOKEN python3 python/smoke/team_check.py http://127.0.0.1:4318 send readme-team-check
-->

- **The token is required.** With `TRAILBEAM_BIND=0.0.0.0` (as in `.env.example`) the port listens on every
  interface; set `127.0.0.1`, or remove the line, for this machine only. Compose refuses to start without
  `TRAILBEAM_TOKEN` and `POSTGRES_PASSWORD`.
- **Docker publishes the port past ufw and firewalld**, so a firewall rule on this machine does not close it
  ([Docker and ufw](https://docs.docker.com/engine/network/packet-filtering-firewalls/)). With `0.0.0.0` it is
  open on every network the machine is on.
- **The port is plain HTTP.** The bearer token, the login form and the session cookie (no `Secure` flag) cross
  the network in clear: put a TLS proxy in front when it crosses a network you don't trust.
- **The data stays** across `docker compose down` and `up`, and across restarts of the machine: spans are in the
  `postgres-data` volume. `docker compose down -v` deletes them. Back up with `pg_dump`, and keep a copy of `.env`
  with it ([Backup and restore](#backup-and-restore-postgres)): `docker compose exec -T postgres pg_dump -U trailbeam -Fc trailbeam > trailbeam.dump`.
- **Postgres starts with `max_locks_per_transaction = 256`** instead of the stock 64, so a larger read pool or a
  second server on the same database needs no Postgres restart ([lock table](#postgres-lock-table-max_locks_per_transaction)).
  Its port is not published: only the Trailbeam container connects.
- **Settings** in `.env`: `TRAILBEAM_RETENTION` (default `7d`), `TRAILBEAM_STORE_BODIES` and the port. For
  per-queue settings, mount a [configuration file](#configuration-file) in `docker-compose.yaml`.
- **Upgrading:** `git pull`, then `docker compose up -d --build --wait` ([Upgrades](#upgrades)). Compose builds
  the image from the checkout with the Postgres driver, so it runs the code you pulled. Releases from 0.4.0
  also publish that image as `ghcr.io/nick-lennox/trailbeam:<version>-postgres`, the Helm chart's default.

CI starts it from `.env.example` on every change, sends a span with the token and checks it is still there
after `docker compose down && up`.

With no access token set (the default), anyone who can reach the port can send spans and read every
timeline, so keep it on `127.0.0.1` or a trusted network, or set a token ([Access token](#access-token)). See
[Run Trailbeam](https://docs.trailbeam.app/setup/run) and, to forward spans through an OpenTelemetry Collector, the
[Collector configuration](collector/README.md).

### Kubernetes (Helm chart)

[`deploy/helm/trailbeam`](deploy/helm/trailbeam) installs the server as a Deployment of 2 replicas behind a
Service on port 4318, on a Postgres you run, with the token and the receipt-hash key in a Secret. Helm 3 or 4
(tested with 3.22 and 4.3); from a clone:

```sh norun="needs a Kubernetes cluster: CI installs the chart on kind (kubernetes, .github/workflows/ci.yml)"
helm install trailbeam ./deploy/helm/trailbeam \
  --set store.url='postgresql://trailbeam:<password>@<host>:5432/trailbeam' \
  --set image.repository=<registry>/trailbeam --set image.tag=<tag>
kubectl rollout status deployment/trailbeam
kubectl get secret trailbeam -o jsonpath='{.data.token}' | base64 -d; echo    # the access token
```

- **Postgres** comes from `store.url`, `store.existingSecret` (a Secret with the key `store-url`), or, for a
  trial, `postgresql.enabled=true`: one Postgres pod on an 8 GiB volume, with no backups and no replication.
- **The token and the receipt-hash key** are generated once and kept on `helm upgrade`, or come from your own
  Secret (`auth.existingSecret`, keys `token` and `receipt-hash-key`). Use your own Secret with Argo CD or Flux,
  which render without the cluster. Every service that uses the Trailbeam packages needs that same
  receipt-hash key.
- **Probes:** liveness on `/api/health`, readiness on `/api/ready`, and a startup probe that allows 30 minutes for
  a schema upgrade. **Requests:** 250m CPU and 256 MiB per replica, no CPU limit, 1 GiB memory limit; the
  figures behind them are in [`values.yaml`](deploy/helm/trailbeam/values.yaml).
- **The image needs the Postgres driver.** Releases from 0.4.0 publish `ghcr.io/nick-lennox/trailbeam:<version>-postgres`,
  the chart's default. To run your own build, build and push it and set `image.repository` and `image.tag`:
  `docker build --build-arg TRAILBEAM_EXTRAS=server,postgres -t <registry>/trailbeam:<tag> .`
- **Plain manifests:** `helm template trailbeam ./deploy/helm/trailbeam --set store.existingSecret=<secret>
  --set auth.existingSecret=<secret> > trailbeam.yaml` renders them once, for `kubectl apply` without Helm.

CI installs the chart on a kind cluster on every change, sends one span with the token and checks that an
upgrade keeps the token ([`deploy/helm/check.sh`](deploy/helm/check.sh)). Docs:
[Run on Kubernetes](https://docs.trailbeam.app/run/kubernetes).

### Access token

One shared token protects ingest, the API and the web screen. It is off by default for local use:

```sh norun="docker run ... stands for the Quickstart's command"
openssl rand -hex 32 > trailbeam.token
docker run ... -v $PWD/trailbeam.token:/run/secrets/trailbeam-token:ro \
  -e TRAILBEAM_TOKEN_FILE=/run/secrets/trailbeam-token ghcr.io/nick-lennox/trailbeam
# or trailbeam --token-file trailbeam.token, or TRAILBEAM_TOKEN=<token> in the environment
```

When it is set:

- **Ingest** (`POST /v1/traces`) needs `Authorization: Bearer <token>` or `x-trailbeam-key: <token>`, or it
  gets `401`. The packages
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
don't trust. The cookie gets `Secure` when that proxy is in `--trusted-proxy` (below) and sends
`X-Forwarded-Proto: https`. Until the server has an admin account, the token is the same for everyone (see
the next section).

A form posted to Trailbeam from another site's page (`/login`, and the routes that change things) is
refused with `403`: the browser's `Sec-Fetch-Site` header, or else its `Origin`, must say the request came
from Trailbeam's own pages. Requests without either header (curl, scripts) are not affected, and neither is
ingest.

### Accounts, ingest tokens and personal API tokens

> **A preview, off by default.** Users and roles are in this release for trying out, but haven't had their
> security review yet, so the server ignores them unless it runs with `--accounts` (or `TRAILBEAM_ACCOUNTS=1`).
> Off, the server is guarded by `TRAILBEAM_TOKEN` alone, as in 0.4.0: `trailbeam start` prints no setup link,
> `/setup` doesn't exist, `TRAILBEAM_ADMIN_EMAIL` stops the server at start, and `trailbeam users` and
> `trailbeam tokens` refuse to run without `--accounts` (or the variable). A store that already holds an admin or
> ingest tokens keeps them, and the server warns at start that it ignores them. Everything below assumes the
> switch is on, on every replica.

The server can also hold people (an email, a password and a role, `admin` or `member`) and named tokens, in its
own store (SQLite or Postgres). Nothing changes until the first admin exists. After that:

| | Who it lets in | Works for |
|---|---|---|
| Signing in with an email and password (`/login`) | the person, with their role | the web screen and the API |
| Personal API token, `tbp_` and 40 letters and digits, as `Authorization: Bearer` | its owner, with the owner's role | the API and `/metrics` (scripts, CI, Prometheus) |
| Ingest token, `tbi_` and 40 letters and digits, as `x-trailbeam-key` or `Authorization: Bearer` | a service | `POST /v1/traces` only |
| `TRAILBEAM_TOKEN`, if set | anyone who has it | `POST /v1/traces` and `/metrics` only |

- **Admins and members** can both read everything the server shows, message bodies included. Only admins manage
  people and tokens.
- **`TRAILBEAM_TOKEN` stops opening the web screen and the API** once an admin exists, and its old sign-in
  cookie stops working. Running services and Prometheus keep working. The server logs a warning at every start
  while the token is set.
- **Every replica switches within 5 seconds** of the first admin being created: each checks the store at least
  that often. Upgrade every replica before creating the first admin, because an older replica would still let
  `TRAILBEAM_TOKEN` into the web screen.
- **Ingest needs a token** when `TRAILBEAM_TOKEN` is set or the store holds at least one ingest token. A revoked
  token still counts, so revoking the last one doesn't open ingest again. Otherwise ingest stays open, and the
  server logs a warning at every start. Creating the first ingest token therefore closes ingest that was open.
- **Ingest tokens can't read anything**, so a leaked service config can't be used to read message bodies.
  The server keeps only a SHA-256 of each token and its first 8 characters. It sees a revoked token within
  30 seconds on every replica, and a revoked or disabled person's sessions and personal tokens within
  20 seconds.
- `x-trailbeam-key` is the header the hosted app takes, so a service moves between the two by changing only
  the address and the key. When a request has both headers, only `x-trailbeam-key` is read.
- `GET /api/me` says how a request was let in, and as whom: use it to check a personal token.
- Signing in has two limits. 10 failed attempts in a row lock that account for 15 minutes, on every replica.
  20 failures from one client address in 10 minutes get "try again later" from that replica. Behind a proxy,
  set `--trusted-proxy` (below), or every request seems to come from the proxy.
- A `--db :memory:` store loses its people and tokens when the server stops.

### The first admin, and people and tokens from the command line

**With `trailbeam start`**, while the store has no admin, the terminal shows a one-time link next to the address:

```console norun="sample output of trailbeam start; the code is new on every start"
Trailbeam is running: open http://127.0.0.1:4318/
  Create your admin account (the link works once, for an hour; keep it to yourself):
    http://127.0.0.1:4318/setup#code=3q2-7wEjUt0yKxGm1fB6c4V8nH0pZr5sLaD9oQeTiWk
```

Open it, and give an email, a name and a password (at least 10 characters). That account becomes the first admin
and is signed in. Until the link is used, it is as good as an admin password, so it works once, for an hour, and
each start replaces it. It is printed only when the output is a terminal, never in the log. The code sits after
`#`, which browsers don't send to the server or a proxy, so it is not in their request logs either.

**In Docker, Compose or Kubernetes** there is no link, because container logs are often readable by many people.
The server logs `this server has no admin account yet` instead. Either run the command in the container:

```sh norun="needs a running container; test_cli.py runs these commands against SQLite and Postgres"
docker exec -it trailbeam trailbeam users create --accounts --email ada@example.com --role admin
# asks for the password twice; or --password-file /run/secrets/admin-password
```

or give the server `TRAILBEAM_ADMIN_EMAIL` and `TRAILBEAM_ADMIN_PASSWORD_FILE` (a file holding the password,
for example a Docker or Kubernetes secret), and optionally `TRAILBEAM_ADMIN_NAME`. They are read at start and only
while the store has no admin, so changing them later changes nothing. Replicas started together create one admin.

**People and tokens** are managed with `trailbeam users` and `trailbeam tokens`, on the server's own store, while
it runs. They take the server's `--db` or `--store-url` (or `TRAILBEAM_DB` / `TRAILBEAM_STORE_URL`, which a
container already has). Without either they use `trailbeam.sqlite` in the current directory, else the one
`trailbeam start` keeps in `~/.trailbeam`.

```sh norun="needs a store; test_cli.py runs these commands against SQLite and Postgres"
trailbeam users create --email bob@example.com --name "Bob" --role member   # asks for the password
trailbeam users list
trailbeam users set-role bob@example.com admin       # or member
trailbeam users reset-password bob@example.com      # an admin resets forgotten passwords: no email is sent
trailbeam users disable bob@example.com              # enable, or remove --yes
trailbeam tokens create --name "orders-api prod"     # an ingest token, for a service
trailbeam tokens create --name ci --personal --user bob@example.com --expires 90d   # a personal API token
trailbeam tokens list                                # --personal for personal API tokens
trailbeam tokens revoke 3                            # --personal for a personal API token
```

- A new token is printed once, alone on stdout (`TOKEN=$(trailbeam tokens create --name orders-api)` works), and
  the store keeps only its hash. Messages go to stderr.
- Creating the first ingest token says first that it closes ingest on a server without `TRAILBEAM_TOKEN`, and
  asks. Without a terminal, pass `--yes`.
- The server always keeps an admin: the last admin who can sign in can't be made a member, disabled or removed.
- Changing someone's role or password, or disabling or removing them, ends their sessions. Running servers see it
  within 20 seconds, a revoked token within 30 seconds, and a new admin or a first ingest token within 5 seconds.
- The commands never create or upgrade a store: they refuse a file that doesn't exist, a store at an older or
  newer schema version (start the new server once first), and `--db :memory:`, which only the server can reach.
- In the web screen, the header shows who is signed in, with **Sign out**.

**The People page** (`/people`, linked in the header for admins; members get "Not allowed") lists everyone who can
sign in, with their role, status (active, invited, locked for now, or disabled) and last sign-in. From it an admin:

- **adds a person**: an email, a name (the part before `@` if left out) and a role. The page then shows their
  one-time invite link, once, to copy and send them privately;
- **changes a role** between admin and member, or **disables** and **enables** someone;
- makes a **new invite link** for someone who hasn't used theirs, or a **password reset link** for someone who has a
  password (for example when they are locked out after too many wrong passwords);
- **removes** a person, after a confirmation.

The controls that would leave no admin who can sign in (making the last one a member, disabling or removing them)
are off, and the server refuses them anyway.

**The Tokens page** (`/tokens`, linked in the header for anyone signed in) has two parts:

- **Ingest tokens**, for admins only: one per service that sends spans. Create one with a name (the token is shown
  once, to copy into the service's `OTEL_EXPORTER_OTLP_HEADERS`), see when each was created, by whom and when it was
  last used, and revoke one after a confirmation. Revoked tokens stay in the list. If `TRAILBEAM_TOKEN` is set, it
  is listed as "legacy shared token (from TRAILBEAM_TOKEN)", without its value; it is set in the server's
  environment, so it is stopped by removing it there and restarting, not from the page. While ingest is open (no
  `TRAILBEAM_TOKEN` and no ingest token yet), the page says so, and creating the first token needs a tick to
  confirm that services without a token are refused from then on.
- **Your personal API tokens**, for everyone: tokens that act as you, with your role, for scripts and the API.
  Create one that works for 30 days, 90 days (the default), a year or until revoked, and revoke your own. Nobody
  else, admins included, sees them.

Without the web screen's build, `/people` and `/tokens` say to use `trailbeam users`, `trailbeam tokens` or the API.

**People and tokens over the API.** Admins can do the same through `/api/users` and `/api/tokens/ingest`, and
anyone signed in manages their own personal API tokens with `/api/me/tokens` (all in
[`contract/openapi.yaml`](contract/openapi.yaml)). Requests that change something send JSON:

```sh norun="needs a server with an admin; test_people_api.py makes these requests against SQLite and Postgres"
ADMIN="Authorization: Bearer tbp_..."   # an admin's personal API token
curl -H "$ADMIN" -H 'Content-Type: application/json' -d '{"email": "cy@example.com", "role": "member"}' \
  http://127.0.0.1:4318/api/users                       # the answer holds Cy's one-time invite link
curl -H "$ADMIN" -X PATCH -H 'Content-Type: application/json' -d '{"role": "admin"}' http://127.0.0.1:4318/api/users/3
curl -H "$ADMIN" -X POST -H 'Content-Type: application/json' -d '{}' http://127.0.0.1:4318/api/users/3/link
curl -H "$ADMIN" -H 'Content-Type: application/json' -d '{"name": "orders-api prod"}' http://127.0.0.1:4318/api/tokens/ingest
```

- Adding a person gives no password: the answer holds a one-time **invite link**
  (`/set-password#code=...`, 7 days) for them to choose one. `POST /api/users/{id}/link` makes a new one, or for
  someone who has a password a **reset link** (24 hours). Choosing a password from a link ends the person's other
  sessions. Each new link replaces the last, and the store keeps only its hash. Trailbeam sends no email:
  pass the link on privately, because until it is used it opens that person's account.
- Two roles, `admin` and `member`. The last admin who can sign in can't be made a member, disabled or removed
  (`409`).
- The first ingest token of a server without `TRAILBEAM_TOKEN` needs `"close_open_ingest": true`, because it
  closes ingest that was open. The ingest token list shows the shared token, if set, as "legacy shared token
  (from TRAILBEAM_TOKEN)", never its value.
- These routes refuse requests from another site's pages, and bodies that aren't `application/json`.

### Behind a proxy (`--trusted-proxy`)

Behind a load balancer or TLS proxy, every request comes from the proxy's address. Tell Trailbeam which
addresses are your proxies, so it believes the `X-Forwarded-For`, `X-Forwarded-Proto` and
`X-Forwarded-Host` headers they add:

```sh norun="starts a server that keeps running; python/tests/server/test_proxy.py tests the flag and the variable"
trailbeam --trusted-proxy 10.0.0.0/8      # repeatable, or TRAILBEAM_TRUSTED_PROXY=10.0.0.5,10.0.1.0/24
```

- The client's address (for the sign-in limits and the logs) is then the rightmost `X-Forwarded-For` entry
  that isn't one of your proxies. Entries further left were written by the client and are ignored.
- `X-Forwarded-Proto: https` from a trusted proxy makes the session cookie `Secure`.
- From any other address, and when `--trusted-proxy` isn't set (the default), the three headers are ignored.

Make your proxy set `X-Forwarded-For` (nginx: `proxy_set_header X-Forwarded-For
$proxy_add_x_forwarded_for;`), and don't let clients reach Trailbeam's port directly: anyone who can is
believed if their address is in the range. [`deploy/replicas`](deploy/replicas/compose.yaml) shows both.

### Sign-in through a proxy (oauth2-proxy, Pomerium, Cloudflare Access)

If your team already signs in to internal tools through a proxy, Trailbeam can believe the email header it adds,
instead of asking for its own password. It is off unless you turn it on, and the header is believed only from the
proxy: from its addresses (`--trusted-proxy`), with a shared secret the proxy sends, or both. It signs people in,
so it needs users and roles switched on (`--accounts`); without them the server refuses to start.

```sh norun="starts a server that keeps running; python/tests/server/test_proxy_auth.py tests every setting"
trailbeam --accounts --proxy-auth-header X-Forwarded-Email --trusted-proxy 10.0.0.5 \
  --proxy-auth-name-header X-Forwarded-Preferred-Username --proxy-auth-sign-out-url /oauth2/sign_out
```

| Setting | Environment variable | What it does |
|---|---|---|
| `--proxy-auth-header` | `TRAILBEAM_PROXY_AUTH_HEADER` | The header with the signed-in email. Turns the feature on. |
| `--trusted-proxy` | `TRAILBEAM_TRUSTED_PROXY` | The proxy's addresses: the header is believed only from them. |
| `--proxy-auth-secret-file` | `TRAILBEAM_PROXY_AUTH_SECRET_FILE` or `TRAILBEAM_PROXY_AUTH_SECRET` | A secret (16 characters or more) the proxy sends in `X-Trailbeam-Proxy-Secret`. For proxies without a fixed address. |
| `--proxy-auth-name-header` | `TRAILBEAM_PROXY_AUTH_NAME_HEADER` | The person's name, for new people. Default: the email's part before `@`. |
| `--proxy-auth-sign-out-url` | `TRAILBEAM_PROXY_AUTH_SIGN_OUT_URL` | Where **Sign out** goes, to end the proxy's session. Without it, people the proxy signs in get no Sign out. |

The server doesn't start with `--proxy-auth-header` alone: without `--trusted-proxy` or a secret, anyone who can
reach the port could sign in as anyone. With both, a request needs both.

**Who gets in, as what.** It works once the server has an admin (until then accounts aren't used):

- **An email Trailbeam hasn't seen** gets a new account, a **member**, with no password. Never an admin: make
  someone an admin with `trailbeam users set-role`.
- **An email with a local account** (made with a password, by `trailbeam users create` or a setup link) is
  **refused** until an admin links it, so whoever the proxy lets in with that email can't take the account over:
  `trailbeam users link-proxy ada@example.com`. Linked, the proxy signs them in with their role, and their
  password still works. `trailbeam users unlink-proxy` undoes it.
- `trailbeam users create --email ... --proxy` makes an account for the proxy ahead of time, with no password
  (useful for an admin who only signs in through the proxy). `trailbeam users list` shows who the proxy can sign
  in to.
- A disabled person is refused. A request with two email headers, or one that isn't an email, is refused.
- From any address but the proxy's, or without the secret, the header is ignored and the request counts as signed
  out. The server logs that at most once a minute, which helps when `--trusted-proxy` is wrong.
- Changes to a person (role, disabled, linked) reach every replica within 20 seconds.

Personal API tokens (`Authorization: Bearer tbp_...`) still work through the proxy and take precedence over its
header. Ingest and `/metrics` never look at it.

**Keep the proxy the only way in.** Anyone who can reach Trailbeam from an address in `--trusted-proxy` is
believed, so give it the proxy's own address, not a whole network, and don't publish Trailbeam's port. The proxy
must also remove the header from what browsers send, which oauth2-proxy, Pomerium and Cloudflare Access do. Send
spans and Prometheus to Trailbeam's port directly from inside the network (with an ingest token), not through
the proxy.

**oauth2-proxy**, in front of the web screen, with Compose. oauth2-proxy sends `X-Forwarded-Email` and
`X-Forwarded-Preferred-Username` to the address it proxies to (its `pass_user_headers` setting, on by default):

```yaml norun="needs an OAuth app at your identity provider; the settings are the ones test_proxy_auth.py tests"
services:
  oauth2-proxy:
    image: quay.io/oauth2-proxy/oauth2-proxy:v7.7.1
    command:
      - --http-address=0.0.0.0:4180
      - --upstream=http://trailbeam:4318
      - --provider=oidc
      - --oidc-issuer-url=https://accounts.google.com   # or Okta, Entra ID, Keycloak...
      - --email-domain=example.com                      # who may sign in at all
      - --reverse-proxy=true
      - --cookie-secure=true
    environment:
      OAUTH2_PROXY_CLIENT_ID: ${OAUTH2_PROXY_CLIENT_ID}
      OAUTH2_PROXY_CLIENT_SECRET: ${OAUTH2_PROXY_CLIENT_SECRET}
      OAUTH2_PROXY_COOKIE_SECRET: ${OAUTH2_PROXY_COOKIE_SECRET}   # openssl rand -base64 32 | tr -- '+/' '-_'
    ports: ["443:4180"]   # behind your TLS proxy or load balancer
    networks:
      default:
        ipv4_address: 172.30.0.5
  trailbeam:
    build: .                                            # this repository's Dockerfile
    environment:
      TRAILBEAM_ACCOUNTS: "1"                           # users and roles: proxy sign-in needs them
      TRAILBEAM_PROXY_AUTH_HEADER: X-Forwarded-Email
      TRAILBEAM_PROXY_AUTH_NAME_HEADER: X-Forwarded-Preferred-Username
      TRAILBEAM_PROXY_AUTH_SIGN_OUT_URL: /oauth2/sign_out
      TRAILBEAM_TRUSTED_PROXY: 172.30.0.5               # oauth2-proxy only
    # no ports: published: people reach it through oauth2-proxy, services on the internal network
networks:
  default:
    ipam:
      config: [{subnet: 172.30.0.0/24}]
```

Then create the first admin (`trailbeam users create --email you@example.com --role admin`, add `--proxy` to sign
in only through oauth2-proxy), and everyone else is a member the first time they open Trailbeam. Don't use
oauth2-proxy's `skip_auth_routes` setting for Trailbeam's pages; if you route anything past it with that setting,
keep `skip_auth_strip_headers` on (the default), so a client can't send its own `X-Forwarded-Email` there.

Other proxies: Pomerium sends the email as `X-Pomerium-Claim-Email` (with `pass_identity_headers`), Cloudflare
Access as `Cf-Access-Authenticated-User-Email`, Google IAP as `X-Goog-Authenticated-User-Email` (its
`accounts.google.com:` prefix is dropped), and the AWS ALB in a signed token only. Cloudflare
Access has no fixed address, so use the shared secret there, added with a Cloudflare rule or a proxy in between.
Checking these proxies' **signed** tokens (safer than a plain header) comes with the OIDC support, as part of the
optional `sso` extra.

### Configuration file

Per-queue settings go in a TOML file, so a few hundred queues don't turn into a few hundred flags:

<!-- doc-test variant=config
pip install "trailbeam[server]"
-->

```toml file=trailbeam.toml variant=config
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

```sh norun="docker run ... stands for the Quickstart's command"
docker run ... -v $PWD/trailbeam.toml:/etc/trailbeam.toml:ro -e TRAILBEAM_CONFIG=/etc/trailbeam.toml ghcr.io/nick-lennox/trailbeam
# or trailbeam --config trailbeam.toml
```

<!-- doc-test variant=config bg wait=http://127.0.0.1:4318/api/ready
trailbeam --config trailbeam.toml
-->

<!-- doc-test variant=config
curl -fsS http://127.0.0.1:4318/api/health | python -c '
import json, sys
config = json.dumps(json.load(sys.stdin)["config"])
assert "orders-dlq" in config and "30d" in config, config'
-->

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

```sh norun="docker run ... stands for the Quickstart's command"
docker run ... -e TRAILBEAM_STORE_BODIES=orders,payments ghcr.io/nick-lennox/trailbeam   # or trailbeam --store-bodies orders,payments
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
`TRAILBEAM_STORE_BODIES`. The Messages page searches them with plain text (`ord-1003`, the same as
`body:ord-1003`) or `body.*` terms such as `body.order.sku:A-100` or `body.amount:>=100`, and so does
`GET /api/messages?body=...`. On a server that keeps no bodies, the page's **Body** field is off and says so. The rules are in
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

```sh norun="docker run ... stands for the Quickstart's command"
docker run ... -e TRAILBEAM_RETENTION=14d -e TRAILBEAM_RETENTION_QUEUES=orders-dlq=30d,audit=forever ghcr.io/nick-lennox/trailbeam
# or trailbeam --retention 14d --retention-queues orders-dlq=30d,audit=forever
```

or `retention` under `[defaults]` and `[queues.<name>]` in the [configuration file](#configuration-file).

A retention is a duration in `s`, `m`, `h` or `d` (`90m`, `36h`, `7d`), or `forever`.
`TRAILBEAM_RETENTION=forever` with no per-queue overrides turns the purge off: nothing is deleted. With
Postgres the passes still run, every `--purge-interval`, to create the coming days' partitions and seal the
older days' lookups ([sizing](#sizing-postgres)).

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

**How fast the purge deletes.** The purge shares SQLite's one writer with ingest. On our test machine
(1M stored spans, 1 KB bodies, ingest at 1,000 spans/s during the pass) it deleted about 1,530 spans/s at
the default batch and about 550 spans/s at `--purge-batch 15`. Export p99 during the pass stayed at what
ingest alone gives (worst 30 s window 243 ms, against 268 ms with no purge). So retention keeps up while
your *average* ingest is below the purge rate, and at the default batch that rate is above the 1,000
spans/s the server is sized for. Above it, the file keeps growing. Measured on one machine; your disk will
give different numbers. Raising `TRAILBEAM_PURGE_DUTY` deletes faster but makes ingest wait longer.

**Disk space.** A database file created by this version gives the deleted space back to the disk
(SQLite incremental vacuum, a few pages at a time after each pass). A file created by an earlier version
reuses the freed space for new spans, so it stops growing, but it does not shrink. To shrink it, run a
one-time rewrite while the server is stopped:

```sh norun="/data is the container's volume, and the server must be stopped"
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
Another (schema 12) builds the hourly tenant rollup from the stored summaries: 7 seconds for 1M spans on the
same machine.
A span the server already has is skipped when it arrives again with the same trace ID, span ID and end
time, as an exporter retry sends it.

### Postgres (bring your own)

SQLite is the default and needs nothing else. Postgres is optional: point Trailbeam at a Postgres you already run.

```sh norun="needs your own Postgres server"
pip install "trailbeam[server,postgres]"
TRAILBEAM_STORE_URL=postgres://trailbeam:<password>@db.internal:5432/trailbeam trailbeam
# or trailbeam --store-url postgres://...
```

Trailbeam creates and upgrades its own tables in that database. Installing, sizing, backing up and upgrading
Postgres stays up to you; this section is what Trailbeam needs from it. The Postgres driver (psycopg 3) is an
extra: a SQLite install doesn't get it, and it is loaded only when the URL is a Postgres one.

**Existing SQLite history is not copied over.** A new Postgres store starts empty. A `trailbeam migrate`
command that copies a SQLite file into Postgres is planned, but it isn't available yet. Until then you can keep
the old SQLite server running next to the new one, with your exporters sending to the new one, for as long as you
need the old timelines.

#### When to move from SQLite

- **You need more than one server**, for high availability or several replicas on Kubernetes. SQLite is one file
  on one server's disk. With Postgres the servers are stateless ([Several replicas](#several-replicas-on-one-postgres)).
- **Retention can't keep up.** SQLite deletes expired spans row by row and shares its one writer with ingest:
  about 1,530 spans/s on our test machine ([Retention](#retention)). When your average ingest is above what
  the purge deletes on your disk, the file keeps growing. Postgres drops a whole day at once (at 1M stored
  spans: 4 ms to detach the partition, 12 ms per table to drop it), however much it holds.
- **The history doesn't fit one server.** Export latency rises once the SQLite file is larger than RAM
  ([Sizing one SQLite server](#sizing-one-sqlite-server)).
- **You already run and back up Postgres**, and you'd rather keep Trailbeam's data there.

Postgres isn't a speed upgrade by itself. On our desktop test machine at 1M spans, export p99 was 336 ms with
SQLite at 1,000 spans/s and 1.3 s with one Trailbeam on Postgres at 2,000 spans/s. Those runs weren't at the
same rate, so they aren't a direct comparison.

#### The database, the role and the URL

The simplest setup is a database of its own, owned by a role for Trailbeam:

```sql
CREATE ROLE trailbeam LOGIN PASSWORD '...';
CREATE DATABASE trailbeam OWNER trailbeam;
```

Trailbeam keeps its tables in the connection's current schema (`public` by default). It creates and drops
partitions of them while it runs, so the role must be able to create tables in that schema and must own them.
It needs no extension and no superuser. On Postgres 15 and later, a role that doesn't own the database can't
create tables in `public`, and Trailbeam stops at start with `permission denied for schema public`.

To share a database with other applications, give Trailbeam a schema of its own and choose it in the URL. Two
Trailbeams can share one database this way, each in its own schema:

```sql
CREATE SCHEMA trailbeam AUTHORIZATION trailbeam;
```

```sh norun="needs your own Postgres server"
TRAILBEAM_STORE_URL='postgres://trailbeam@db.internal:5432/shared?options=-csearch_path%3Dtrailbeam'
```

The URL goes to libpq unchanged, so libpq's [connection parameters](https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-PARAMKEYWORDS)
and [environment variables](https://www.postgresql.org/docs/current/libpq-envars.html) work: `?sslmode=verify-full`
for TLS, and `PGPASSWORD` or a [password file](https://www.postgresql.org/docs/current/libpq-pgpass.html) to keep the
password out of the URL. The server log shows the URL with the password hidden. The URL is set by
`TRAILBEAM_STORE_URL` or `--store-url` only; the [configuration file](#configuration-file) doesn't take it.

**Connections.** Each replica opens up to 14 by default: up to 4 for ingest and up to 8 for pages and the API
(opened as needed, 2 when idle), 1 held by the replica that runs retention, and a short-lived one when it creates
partitions. Set `max_connections` to that per replica plus your other clients. The two pools are settings:

| Flag | Environment variable | Default | What it is |
|---|---|---|---|
| `--pg-ingest-connections` | `TRAILBEAM_PG_INGEST_CONNECTIONS` | 4 | exports a replica writes at once |
| `--pg-read-connections` | `TRAILBEAM_PG_READ_CONNECTIONS` | 8 | pages and API reads a replica runs at once |

More ingest connections help only while Postgres has CPU and disk to spare: exports that touch the same
messages wait for each other anyway. A bigger read pool raises what the [lock table](#postgres-lock-table-max_locks_per_transaction)
must hold (Trailbeam warns at start when it is short). An export that must search older days' lookups (a late
settle, a message older than three days) borrows a read connection for it while holding its ingest connection,
so pages and API reads filling the read pool can hold such exports up. They are for Postgres only: with SQLite, setting either
stops the server with an error. Every connection runs with
`statement_timeout = 60s`; an export whose statement runs past it is answered `503` (`store_timeout`,
`Retry-After: 3`), with nothing stored. An export that waits longer than `TRAILBEAM_INGEST_MAX_WAIT_S` (2 s by default) for a free
ingest connection is answered `503` with `Retry-After`, as one waiting for SQLite's writer is
([overload](#when-trailbeam-is-overloaded-429-503-and-retry-after)).
Each export is one transaction, and an export whose client hangs up or times out is cancelled in the database.

**Connect directly, not through a transaction pooler.** Trailbeam pools its own connections and relies on
session state: the retention lease and the schema upgrade are session-level advisory locks, and each connection
sets its timeouts once when it opens. Poolers in transaction mode break that
([PgBouncer's list](https://www.pgbouncer.org/features.html#sql-feature-map-for-pooling-modes)). That includes
PgBouncer in transaction or statement mode, Neon's pooled (`-pooler`) host and Supabase's transaction pooler
(port 6543). Use the direct connection string. A pooler in session mode keeps the session state, but it adds
nothing to Trailbeam's own pools, and we haven't tested one.

#### Supported versions and managed services

**Postgres 14 and later.** Retention needs `DETACH PARTITION … CONCURRENTLY`, which is new in 14. CI runs the
storage conformance suite on 14 and 16 on every change, and on 14 to 18 every night ([Supported
versions](#supported-versions)). The setup, backup and restore steps here were checked on stock Postgres 14 and 16.

A managed Postgres should work if it gives you version 14 or later, a role that can own a database or schema,
and a direct connection. **We haven't run Trailbeam on any managed service yet.**

| Service | What to use |
|---|---|
| Amazon RDS for PostgreSQL, Aurora PostgreSQL | The instance or cluster endpoint, not RDS Proxy (a pooler Trailbeam doesn't need). TLS with `sslmode=verify-full` and the [RDS CA bundle](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/UsingWithRDS.SSL.html) as `sslrootcert`. |
| Google Cloud SQL for PostgreSQL | The instance's IP, or the [Cloud SQL Auth Proxy](https://cloud.google.com/sql/docs/postgres/sql-proxy), which forwards connections without pooling them. |
| Neon | The direct connection string, not the pooled one ([Neon: connection pooling](https://neon.com/docs/connect/connection-pooling)). Scale to zero hasn't been tested with Trailbeam. |

#### Retention on Postgres

Retention works as described [above](#retention), with one difference: spans are kept in **daily partitions**
(UTC days), one set per retention you configure, and the purge drops whole partitions instead of deleting rows,
so it costs about the same however much it removes. A message expires once the *day* of its newest span has
ended more than its retention ago, so `7d` keeps a message for 7 to 8 days. Messages are still deleted whole:
when a message's spans fall in two partitions (it crossed midnight, or moved to a dead-letter queue kept longer),
the purge first moves its older spans in with the newer ones. If you change a queue's retention after its spans
were stored, partitions holding queues with different retentions are cleaned by a bounded background delete,
outside ingest. At the start of each pass, the replica running retention creates the partitions for today and
the next two days, so ingest rarely has to.

Trailbeam creates a partition as a table of its own and then attaches it, which doesn't have to wait for open
transactions that have read Trailbeam's tables (a `pg_dump` is one for its whole run) and doesn't hold up exports.
It conflicts only with a schema change or a manual `ANALYZE` of a whole table running at the same time, and waits
at most 1 second for those. A retention pass that can't get the lock tries again at the next pass, a day or more
before the partition is needed. An export that needs a partition nobody has made yet (a span more than two days
early, or a late one for a day that has no partition) tries three times, then is answered `503` with
`Retry-After`, nothing stored, and the exporter sends it again.

Trailbeam analyzes a new day's partitions itself once a replica has written 1,000 spans to them, instead of
waiting for autovacuum. Until a partition has planner statistics, Postgres treats it as empty and picks slow
plans for it.

#### Sizing Postgres

This isn't a sizing guide yet: the medium-tier benchmark (10,000 spans/s with 100M spans stored, 2 replicas)
hasn't been run on a representative machine. What we have measured so far, on our shared desktop test machine
(16 threads, 11.7 GB RAM shared with other work, WSL2 virtual disk), with 1–8M spans, 2 replicas and Postgres 16
with `shared_buffers = 1GB`:

- **Disk: about 2.8 GB per million spans**, with 1 KB bodies stored on 5 of 50 queues, plus the WAL
  (`max_wal_size`). With `7d` retention a message is kept 7 to 8 days, so plan for 8 days of your average
  ingest, plus room for one more day. For example, 100 spans/s on average is about 70M spans, or about 200 GB.
- **RAM is the first limit.** Ingest writes at random index positions in today's partitions. Before this
  version it also wrote to two lookup tables that weren't partitioned (`hash_places` and `mid_places`, about
  90 MB of index per million spans, growing with everything stored). That was the likely cause of the falloff
  measured here: at about 1M spans this machine held 6,000 spans/s with export p99 630 ms; at 4M, where those
  indexes no longer stayed cached, 2,000 spans/s had p99 1.7 s, and at 7M it didn't hold 2,000 spans/s.
- **The lookup tables are now time-bucketed** (Postgres schema 5). They are partitioned by day like the spans,
  and dropped with them by retention. Only the last three days' partitions are written to and searched for
  every export. Older days are *sealed*: their keys go into a compact filter, about 2.5 bytes per key (about
  3.3 MB per million spans of history, measured at 1M), which each replica keeps in memory. An older day's
  partition is read only for the rare keys the filter can't rule out, such as a late settle or a false
  positive (about 1 in 15,000 keys). At 1M spans over 7 days, ingest touches 25 MB of lookup indexes instead
  of 92 MB. So give Postgres enough memory for the last three days' indexes, not for all of history. This holds with
  `forever` retention too: retention's passes seal the older days whether or not anything expires.
- **Measured with this change, from 4M to 9.4M spans** (QV-180, same machine and settings, on a quieter day):
  ingest held 6,000 spans/s at every size with no errors, where before it didn't hold 2,000 at 7M. Export p99
  at 6,000 spans/s was 241 ms at 4–5M and 1.1–1.3 s at 7–9M, so the p99 ≤ 500 ms target holds only to about
  5M here. Near 8,000 spans/s the two replicas ran out of CPU. The machine had more free RAM than in the
  earlier run, and the run can't separate that from the change
  ([details](python/bench/README.md#4m-and-7m-spans-before-and-after-qv-127-qv-180)). **The largest size we
  have measured is 9.4M spans (26 GB).** Above that it's not measured, and the 100M figure stays an estimate:
  12–16 GB of RAM, from before this change.
- **Commit latency adds to export latency.** An export keeps its locks on the messages it touches until it
  commits, so a slow WAL flush holds up the exports behind it. Put the database on storage with a fast
  fsync: local NVMe, EBS gp3, or a managed service's SSD.
- **The Trailbeam servers use about one CPU core per 5,000–6,000 spans/s** in total (2 replicas at 50% of a core
  each at 6,000 spans/s, and at 77% each at 6,900). These numbers are for that machine, not a target.
- **The tenants page** added up every tenant event in its time window on each request, so it got slower as
  data grew: p95 1.5 s at 4M spans and 2.8 s at 7M. The other pages stayed under 500 ms (p95) under ingest. It
  now reads an hourly rollup per tenant: at 1M spans on the same desktop, idle, p95 went from 184 ms to 44 ms
  for the last day and from 670 ms to 193 ms for the last 7 days. That rollup covers only the whole hours of the
  window, though, and the page still read every raw event of the current hour. Right after heavy ingest that
  made it slow again: 1.1 s at 5.3M spans and 1.9–2.1 s at 9.4M in QV-180, with 2.5M events in the current
  hour. Since QV-224 the current hour is read from the rollup too, less the few events newer than the window. With
  2.06M of 3.3M tenant events in the current hour, idle, p95 for the last day went from 2.07 s to 55 ms, for the last hour
  from 2.19 s to 21 ms and for the last 7 days from 2.30 s to 248 ms, with the same answers. The hour at the start
  of the window still reads up to half an hour of raw events, which Postgres added up at about 2M per 250 ms
  here. At a steady 2,000 spans/s that is about 3.6M events, or roughly 450 ms (an estimate; not measured under
  ingest).

To trade durability for latency, as `--durability relaxed` does for SQLite, set it on Trailbeam's role:
`ALTER ROLE trailbeam SET synchronous_commit = off;`. A crash of Postgres can then lose the exports answered `200`
in the last moments before it (up to three times `wal_writer_delay`, 600 ms by default). The database stays
consistent ([Postgres: asynchronous commit](https://www.postgresql.org/docs/current/wal-async-commit.html)).

#### Backup and restore (Postgres)

Use Postgres' own backups. Everything Trailbeam stores is in its database (or schema). The access token and the
receipt-hash key are not, and a restore needs both: keep them with the backups. Without the token every service
and browser has to be given a new one; without the key, services that start hashing with a new one leave the
deletes of messages received before the restore unjoined (`key_mismatch`; see [the SQLite
section](#backup-and-restore-sqlite) for why). Where they are:

- **Team install (Docker Compose):** `.env` (`TRAILBEAM_TOKEN`, `TRAILBEAM_RECEIPT_HASH_KEY`, and `POSTGRES_PASSWORD`).
- **Helm chart:** the Secret `<release>-trailbeam` (keys `token` and `receipt-hash-key`), or your
  `auth.existingSecret`. On a new cluster the chart generates new values unless you give it the old ones:
  recreate the Secret and set `auth.existingSecret`, or set `auth.token` and `auth.receiptHashKey`.
- **Your own setup:** wherever `TRAILBEAM_TOKEN` or `--token-file` and the services' `TRAILBEAM_RECEIPT_HASH_KEY` come from.

Keep your configuration file too, if you use one. Protect the backups like the data: they hold the stored message
bodies, and with the token anyone can read every timeline.

- **Snapshots and point-in-time recovery** (the automated backups of RDS and Cloud SQL, Neon's restore,
  `pg_basebackup` with WAL archiving, pgBackRest) copy the files and take no table locks. Prefer them.
- **`pg_dump`** works while Trailbeam ingests, at any time of day, and doesn't stop Trailbeam creating partitions
  or storing exports.

```sh norun="needs your own Postgres server"
pg_dump -Fc -f trailbeam-$(date +%F).dump "postgres://trailbeam@db.internal/trailbeam"
```

To restore, create an empty database owned by the role, restore into it as that role, and refresh the planner
statistics, which a dump doesn't carry. Without that, Postgres has no statistics for the restored tables until
autovacuum analyzes them, and it can pick slow plans meanwhile.

```sh norun="needs your own Postgres server"
createdb -O trailbeam trailbeam_restored        # as an administrator
pg_restore --no-owner --role=trailbeam -d "postgres://trailbeam@db.internal/trailbeam_restored" trailbeam-2026-11-19.dump
psql "postgres://trailbeam@db.internal/trailbeam_restored" -c ANALYZE
```

Use `pg_dump` and `pg_restore` of the same major version as the server you restore onto (on Debian or Ubuntu,
`postgresql-client-16` for Postgres 16, in `/usr/lib/postgresql/16/bin`; it comes from the
[PostgreSQL apt repository](https://www.postgresql.org/download/linux/ubuntu/) when your release's own default is
another version), or run them in a `postgres:16` container.
A newer `pg_restore` sends settings an older server doesn't know: version 18 restoring into Postgres 16 reports
`unrecognized configuration parameter "transaction_timeout"` and exits 1.

Then point `TRAILBEAM_STORE_URL` at the restored database, give the server the same token, and the services the
same key. Restore into an empty database, before Trailbeam first starts on it: a server that started first has
created its tables, and `pg_restore` then fails on them.

If Trailbeam has a schema of its own, add `-n trailbeam` to `pg_dump`, and restore as a role allowed to create
that schema. As with SQLite, the purge deletes whatever is past retention at its next pass, so set a longer
`--retention` first if you restored the backup to look at old timelines.

**Team install on a new machine.** Copy `.env` (with `install -m 600`) and the dump, start Postgres alone, restore,
then start the rest:

```sh norun="a team install's Compose project; CI runs the same pg_dump and pg_restore (backup-restore, .github/workflows/ci.yml)"
docker compose up -d --wait postgres
docker compose exec -T postgres pg_restore -U trailbeam -d trailbeam --no-owner < trailbeam.dump
docker compose exec -T postgres psql -U trailbeam -d trailbeam -c ANALYZE
docker compose up -d --build --wait
```

`python/smoke/backup_restore_check.py --store postgres` runs `pg_dump` while the server ingests, restores with
`createdb -O`, `pg_restore --no-owner --role` and `ANALYZE` into a new database owned by the same role, and checks
the restored server as for SQLite (CI runs it on Postgres 16). The team install steps above were run by hand
(QV-239), not in CI. A `trailbeam backup` command would add nothing here: snapshots, point-in-time
recovery and `pg_dump` are the tools a Postgres operator already runs, and they are safe while Trailbeam ingests.

#### Upgrades

**Trailbeam.** Each server checks the schema version (the `schema_version` table) when it starts. If the
version is older, it upgrades the schema, one replica at a time under an advisory lock while the others wait. A
server older than the schema refuses to start (`written by a newer Trailbeam`), so it can't damage the data, but it
can't run on the upgraded database either: to go back, restore a backup taken before the upgrade. A replica only
checks the version when it starts. So when a release changes the Postgres schema, stop every replica before you
start the new version, unless that release's notes say a rolling upgrade is safe ([CHANGELOG](CHANGELOG.md)
says which releases change it). An older replica still running would store spans the new schema doesn't index.
0.4.0 upgrades a 0.3.0 (or 0.2.0) database to Postgres schema 6: stop every older replica first. Schema 5
partitions the two lookup tables, one day at a time: 8 s for a database of 1M spans on our test machine, and
about as long again per further million (an estimate). The first retention pass after it then seals the older
days (4.5 s at 1M).

**Postgres.** For a minor version, use your package manager's or provider's update. While the database restarts,
`/api/ready` answers `503` and exports are answered `503` with `Retry-After`, so exporters send them again. For a
major version (to 14 or later), use `pg_upgrade`, your provider's major-version upgrade, or a dump and restore.
Before Postgres 18, `pg_upgrade` doesn't carry the planner statistics over, so run
`vacuumdb --all --analyze-in-stages` afterwards.

#### Postgres lock table (`max_locks_per_transaction`)

Each statement locks the partitions it reads, and their indexes (7 to 15 tables and indexes per partition, depending
on the statement), until its transaction ends. The locks are held in Postgres's shared lock table, whose size is set
at server start: `max_locks_per_transaction` × (`max_connections` + `autovacuum_max_workers` + 1 +
`max_worker_processes` + `max_wal_senders` + `max_prepared_transactions`) objects, and twice that (object,
connection) pairs. When it is full, statements fail with `out of shared memory` / `You might need to increase
max_locks_per_transaction`. The table is shared, so whatever fills it makes exports fail too.

Exports and reads of one message lock only the partitions that hold its spans. Pages (the lists of messages,
which walk across all of time), `/api/health` and the counts read 8 partitions per statement, each statement in
its own transaction. So what this store locks at once doesn't grow with the number of partitions, and the stock
setting (64) is enough for one replica with the default read pool (8 connections), however long the retention.
Trailbeam still works it out, counting every table and index of 8 partitions (31 each) for every read connection,
so the value is an upper bound. At start, and as partitions are added (at most once an hour), it logs a warning
when the server is short, with the value to set:

```
WARNING trailbeam.server.postgres Postgres lock table: ... Set max_locks_per_transaction to at least 158 (and restart Postgres) ...
```

On a stock Postgres that takes a read pool of more than about 80 connections. With several replicas on one
database, multiply by their number. An export that fails on a full lock table is answered 503 with `Retry-After`
(nothing stored; the client sends it again), and logged as a full lock table, not as an unreachable database; so
is a page that fails. `max_locks_per_transaction` needs a Postgres restart; on managed Postgres it is a
parameter-group setting. It costs shared memory: on a stock Postgres 16, `max_locks_per_transaction = 640`
instead of 64 makes it 170 MB instead of 143 MB.

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
nginx (`docker compose -f deploy/replicas/compose.yaml up -d --build --wait`). And the
[Helm chart](#kubernetes-helm-chart), two replicas with liveness and readiness probes and a Service.

### Durability: `--durability`

By default (`full`), an export is answered 200 only after its spans are committed and synced to disk
(SQLite `synchronous = FULL`). On a slow or shared disk that sync is most of an export's time: on the
QV-78 desktop benchmark machine (a WSL2 virtual disk), about 150–180 ms at p99 for one 512-span export.
On an AWS EBS gp3 volume the same sync is about 35 ms at p99, and `relaxed` made no measurable
difference there (see [Sizing](#sizing-one-sqlite-server)).

`--durability relaxed` (env `TRAILBEAM_DURABILITY=relaxed`) skips the sync per export (SQLite
`synchronous = NORMAL`): the log is synced at checkpoints, about once a second. A crash of the
Trailbeam process loses nothing that was answered 200. An OS crash or power loss can lose the exports
answered 200 in the last moments before it, and the file stays consistent. The server logs a warning at
start when it's set. It applies to the SQLite file only; with Postgres, use its `synchronous_commit` ([Sizing Postgres](#sizing-postgres)).

### Delivery guarantee: what a `200` promises

Trailbeam answers an export `200` only after its spans are committed. Any other answer (`429`, `503`, or no
answer at all because the connection dropped) means "send it again", and OTLP exporters do. If the first copy
was stored after all (it was committed, but the answer was lost), the copy is skipped as a duplicate. SQLite
matches on trace ID, span ID and end time; Postgres on trace ID and span ID within the span's day. So with
an exporter that retries, delivery is at least once and each span is stored once. What a `200` survives
depends on the store and its setting:

| Store, setting | Trailbeam process killed | Postgres killed | OS crash or power loss |
|---|---|---|---|
| SQLite, `--durability full` (default) | nothing answered `200` lost | n/a | nothing answered `200` lost: synced per export |
| SQLite, `--durability relaxed` | nothing answered `200` lost | n/a | the exports answered in the last moments before it (about a second) can be lost; the file stays consistent |
| Postgres, `synchronous_commit = on` (Postgres' default) | nothing answered `200` lost | nothing answered `200` lost | nothing answered `200` lost |
| Postgres, `synchronous_commit = off` | nothing answered `200` lost | up to 3 × `wal_writer_delay` (600 ms by default) of exports | the same |

The first two columns are tested (QV-238; [fault tests](python/bench/README.md#fault-tests-faultspy)): SIGKILL
of the server mid-write, three times, for both SQLite settings and Postgres, and Postgres killed during a
retention pass. No answered span was lost or stored twice. The OS-crash column is what SQLite's
[`synchronous`](https://www.sqlite.org/pragma.html#pragma_synchronous) and Postgres'
[asynchronous commit](https://www.postgresql.org/docs/current/wal-async-commit.html) promise. It is not tested
here, and it assumes the disk honours fsync. The same tests cover the other failures:

- **Postgres down** (stopped for a minute): exports are answered `503` (`store_unreachable`, `Retry-After: 1`)
  and `/api/ready` is `503`. When Postgres is back, the server takes exports again within seconds, with no
  restart.
- **Disk full**, on either store: exports are answered `503` (`store_full`, `Retry-After: 3`) with nothing
  stored. Once space is freed they are stored again, with no restart. Until QV-238, SQLite answered a full disk
  with `500`, which exporters don't retry, so those spans were dropped. `/api/ready` stays `200` while a SQLite
  disk is full; watch `trailbeam_ingest_rejected_requests_total{reason="store_full"}` and the disk itself.
- **A Postgres statement timeout** (an export's statement past `statement_timeout`, 60 s: a lock pile-up, a
  very slow disk): the export is answered `503` (`store_timeout`, `Retry-After: 3`) with nothing stored, and
  logged at ERROR with its size. It is stored when the exporter sends it again, once the database is faster.
  Until QV-300 this was a `500`, which exporters don't retry. An export that is always too slow is retried
  until the exporter's budget runs out (below), each try holding an ingest connection for up to 60 s: watch
  `trailbeam_ingest_rejected_requests_total{reason="store_timeout"}`. Most exporters time out a request well
  before 60 s (the Python exporter after 10 s) and hang up; such an export is cancelled in the database and sent
  again, as before.
- **Several replicas**: a killed replica loses nothing it answered, and the load balancer sends the retries to
  the others ([Several replicas](#several-replicas-on-one-postgres)).
- **Order and clocks**: arrival order doesn't change what is stored or shown. Spans shuffled across 40 exports,
  some from clocks an hour ahead or behind, give the same timelines as the same spans sent in order. A span is
  stored with the times it was sent with. One already older than the retention is stored, then deleted by the
  next pass.

What a `200` can't cover is an export that is never answered `200`: then the exporter's retry budget decides.
The Python OTel exporter retries only within its 10 s export timeout. The Collector's `retry_on_failure`
retries for 5 minutes by default, from a sending queue in memory. Send through a Collector to ride out outages
longer than a few seconds ([`collector/`](collector/README.md)).

### Sizing one SQLite server

**Disk is the limit to plan for.** One SQLite server holds *average ingest × retention* spans, at about
**2 GB per million spans** (1.9 GB measured at 300k and 1M spans, with or without 1 KB bodies on a tenth
of the queues, and 2.1 GB at 10M). Retention keeps the file at that size as long as the average stays
under the purge rate (about 1,530 spans/s on our test machine, [Retention](#retention)). In our test
traffic a message is about 3.2 spans, so divide by 3 for messages/s.

| average ingest | 1 day | 7 days (default) | 30 days |
|---|---|---|---|
| 10 spans/s | 0.9M spans, 1.7 GB | 6M, 12 GB | 26M, 52 GB |
| 50 spans/s | 4.3M, 9 GB | 30M, 60 GB | 130M, 260 GB |
| 200 spans/s | 17M, 35 GB | 121M, 240 GB | 518M, 1 TB |
| 1,000 spans/s | 86M, 170 GB | 605M, 1.2 TB | 2.6B, 5.2 TB |

Disk sizes are spans × 2 GB per million, before the free space an upgrade leaves ([Upgrading a database
file](#retention)). Keep free disk about the size of the file on top: an upgrade and a
[backup](#backup-and-restore-sqlite) each need it.

**1,000 spans/s is a peak rate for SQLite, not a sustained one.** One server takes bursts of 1,000 spans/s
and stores every export it answers 200 (measured below). Held for 7 days, that rate is 605M spans and about
1.2 TB, 60 times the largest file we have measured. What we have measured:

- **Live ingest and pages:** at 1M spans (2 GB). The table below.
- **In-process inserts at 10M spans** (21 GB file, 12 GB RAM): 2,030–2,480 spans/s, against 9,560–9,770 at 1M.
  The writer slows once the file is larger than RAM. We haven't measured live export latency or pages at 10M.
- **Past 10M spans:** not measured. At the 7-day default, 10M spans is an average of about 16 spans/s.

**When to move to [Postgres](#postgres-bring-your-own):**

- you need more than one replica, or a server you can restart without stopping ingest;
- the data you keep is past about 10M spans (20 GB), the largest SQLite file we've measured, or much larger
  than the machine's RAM;
- export p99 on `/metrics` keeps rising as the file grows;
- or the average ingest you need is near the purge rate.

The Postgres store has been measured at 1M spans too (`python/bench/README.md`), not yet at larger sizes.

**Which machine.**

- **Recommended:** RAM larger than the database file and more than 2 vCPUs. That's where the list-page target
  holds: p95 ≤ 300 ms (on the desktop below, 153 ms for the slowest page while ingesting at 1M spans).
- **Minimum:** an AWS t3.small class machine (2 vCPU, 2 GB RAM, EBS gp3), with the measured numbers below.
  At 1M spans and 1,000 spans/s its targets are export p99 ≤ 3 s idle and ≤ 6 s while pages load, and ingest
  keeps up with nothing acknowledged lost. The Tenants page was slower there than the 300 ms target, before it
  read an hourly rollup; it hasn't been measured on that machine since (at 1M spans on the desktop, the rollup
  took the last day from 78 ms to 17 ms, idle).

#### Latency at 1M spans

Measured with the benchmark harness (`python/bench/harness.py --sizes 1M --rates 1000sp`): 1M stored
spans, 1,000 spans/s ingest, the load generator on the same machine. Export p99 is over one 30 s step
(about 60 exports) idle, and over 60 s while the read pages are timed.

| machine | durability | export p99, idle | export p99, while pages load | slowest page p95 while ingesting |
|---|---|---|---|---|
| desktop, 16 threads, 11.7 GB RAM, WSL2 virtual disk | `full` (default) | 336 ms | 583 ms | `tenants`, 153 ms |
| same | `relaxed` | 199 ms | 365 ms | `tenants`, 160 ms |
| AWS t3.small, 2 vCPU, 2 GB RAM, EBS gp3 | `full` (default) | 1.5–2.3 s | 1.4–4.4 s | `tenants`, 0.5–0.76 s |
| same | `relaxed` | 1.5–1.6 s | 1.5–3.7 s | `tenants`, 0.53–0.61 s |

On the t3.small, ingest kept up at 1,000 spans/s, and every 200 was stored. The 1M file (2 GB) is larger
than the machine's RAM, so writes wait on disk reads, and the server shares its 2 vCPUs with the load
generator. At 300k spans (a file that fits in RAM) export p99 was still about 1.3 s for both modes.
The sync per export was not the limit there. For export p99 under about 500 ms at 1M spans, plan for
RAM larger than the database file and more than 2 vCPUs. The gain from more CPU and RAM is an
estimate; it hasn't been measured on a larger instance.

### Backup and restore (SQLite)

Everything Trailbeam stores is in the database file (`--db`, `/data/trailbeam.sqlite` in the container). The
file is in WAL mode: committed writes are in `trailbeam.sqlite-wal` until the server copies them into the
file, about once a second. **Don't back up by copying the file while the server runs.** A copy of the file
without its `-wal`, or of both at slightly different moments, can be missing recent spans or be corrupt.

**Backup, with the server running.** Use SQLite's backup API, which copies one consistent snapshot. Python's
`sqlite3` module is all it needs, and the container has it:

```sh norun="the paths of a server's own machine; the Docker form below is run"
python3 -c 'import sqlite3, sys; s = sqlite3.connect(sys.argv[1]); d = sqlite3.connect(sys.argv[2]); s.backup(d); d.close(); s.close()' \
    /data/trailbeam.sqlite /backups/trailbeam-$(date +%F).sqlite
```

In Docker, write the copy to the volume, take it out, and delete it there:

```sh run variant=docker
docker exec trailbeam python -c 'import sqlite3, sys; s = sqlite3.connect(sys.argv[1]); d = sqlite3.connect(sys.argv[2]); s.backup(d); d.close(); s.close()' \
    /data/trailbeam.sqlite /data/backup.sqlite
docker cp trailbeam:/data/backup.sqlite ./trailbeam-$(date +%F).sqlite
docker exec trailbeam rm /data/backup.sqlite
```

The copy needs free disk about the size of the file. Ingest carries on while it runs. On our test machine
(2.1 GB file, ingest at 1,000 spans/s), the backup took 5.6 s with the file in the page cache and 12 s
with it mostly out. Export p99 over that minute went from 80 ms to 192 ms, with no errors. While a backup
runs, the server can't copy the log into the file, so `-wal` grows by what arrives meanwhile: roughly
10 MB per second of backup at 1,000 spans/s (an estimate from about 2.4 log pages per span). The server cuts it
back to 64 MB once it has caught up.

We don't recommend `VACUUM INTO` for a running server. In the same test it took 4.6 s, but export p99 rose
to 747 ms and the log reached about 210 MB.

**Check a backup** before you rely on it. This prints `ok` (about a minute for a 2 GB file here):

<!-- doc-test variant=docker
cp "trailbeam-$(date +%F).sqlite" trailbeam-2026-11-19.sqlite
-->

```sh run variant=docker
python3 -c 'import sqlite3, sys; print(sqlite3.connect(sys.argv[1]).execute("PRAGMA integrity_check").fetchone()[0])' \
    trailbeam-2026-11-19.sqlite
```

<!-- doc-test expect variant=docker
ok
-->

**Restore.** Stop the server, delete the old `-wal` and `-shm` files, put the backup in place of the file,
and start the server. Delete those two files first: SQLite would apply a leftover log from the old file to
the restored one and corrupt it.

```sh norun="the paths of a server's own machine; the Docker form below is run"
rm -f /data/trailbeam.sqlite-wal /data/trailbeam.sqlite-shm
cp trailbeam-2026-11-19.sqlite /data/trailbeam.sqlite
```

In Docker, from the directory that has the backup:

```sh run variant=docker wait=http://127.0.0.1:4318/api/ready
docker stop trailbeam
docker run --rm --user root -v trailbeam-data:/data -v "$PWD":/restore:ro --entrypoint sh ghcr.io/nick-lennox/trailbeam -c \
  'rm -f /data/trailbeam.sqlite-wal /data/trailbeam.sqlite-shm && cp /restore/trailbeam-2026-11-19.sqlite /data/trailbeam.sqlite && chown trailbeam: /data/trailbeam.sqlite'
docker start trailbeam
```

<!-- doc-test variant=docker
test "$(curl -fsS http://127.0.0.1:4318/api/messages | python -c 'import json, sys; print(len(json.load(sys.stdin)["messages"]))')" = 2
-->

A backup from an older version is upgraded when the server starts ([Upgrading a database
file](#retention)). The purge then deletes whatever is past retention, so set a longer `--retention` first
if you restored the backup to look at old timelines.

**Keep the access token and the receipt-hash key with the backup.** Neither is in the database file, and a
restore without them doesn't give back a working server:

- **The token.** Without it, a new machine gets a new token (`trailbeam start` makes one), and every service's
  `OTEL_EXPORTER_OTLP_HEADERS` and every browser signed in to the web screen stop working until they are given it.
- **The receipt-hash key.** The server never reads it, but every service hashes receipt handles with it, and a
  delete or visibility change is joined to its delivery by that hash. If the services carry on with a new key
  after the restore, the deletes of messages received before it don't join their receives: those timelines
  show `key_mismatch`. `trailbeam start` makes a new key when the data
  directory has none, so restore the file before the first start.

Where they are: `trailbeam start` keeps both next to the database, as `token` and `receipt-hash-key` in the data
directory (`~/.trailbeam`; `/data` in the Docker volume of `npx trailbeam start`). With `docker run` they are
where you keep them: the token in `TRAILBEAM_TOKEN` or `--token-file`, the key in your services' settings. Keep
your configuration file too, if you use one. Copy the two files with the backup:

```sh norun="the paths of a server's own machine; CI runs these steps (backup-restore, .github/workflows/ci.yml)"
cp -p ~/.trailbeam/token ~/.trailbeam/receipt-hash-key /backups/
docker cp trailbeam:/data/token ./token && docker cp trailbeam:/data/receipt-hash-key ./receipt-hash-key   # in Docker
```

A backup holds the stored message bodies (`--store-bodies`), and with the token anyone can read every timeline,
so protect the backup like the data: readable by its owner only, and not in the same place as the server.

**Restore on a new machine** (`trailbeam start`). Put the three files in a new data directory, readable by you
only, then start:

```sh norun="the paths of a server's own machine; CI runs these steps (backup-restore, .github/workflows/ci.yml)"
install -d -m 700 ~/.trailbeam
install -m 600 trailbeam-2026-11-19.sqlite ~/.trailbeam/trailbeam.sqlite
install -m 600 token receipt-hash-key ~/.trailbeam/
trailbeam start
```

`trailbeam start` doesn't print the token or key when it finds them (it prints a `cat` command for each): if
it prints them in full, it has made new ones, so the restore is missing a file. In Docker (the volume of
`npx trailbeam start`), restore into a new volume and start as before:

```sh norun="CI runs these steps with this checkout's image (backup-restore, .github/workflows/ci.yml)"
docker volume create trailbeam-data
docker run --rm --user root -v trailbeam-data:/data -v "$PWD":/restore:ro --entrypoint sh ghcr.io/nick-lennox/trailbeam -c \
  'install -m 600 -o trailbeam -g trailbeam /restore/trailbeam-2026-11-19.sqlite /data/trailbeam.sqlite &&
   install -m 600 -o trailbeam -g trailbeam /restore/token /restore/receipt-hash-key /data/'
npx trailbeam start
```

`python/smoke/backup_restore_check.py` runs these steps in CI (with `trailbeam start` and with the image): it
takes the backup while the server ingests and the API is read, restores it on a new data directory or volume,
and checks that the restored server serves exactly the exports answered before some moment during the backup,
that the token still works, and that deletes sent after the restore join receives from before it.

**Why there is no `trailbeam backup` command.** The backup API is SQLite's own consistent snapshot, the
command above is all it takes, and the check shows it is safe under load. The two ways to get it wrong are a
plain `cp` of the live file (in the same check, a `cp` passed `integrity_check` but had lost exports already
answered 200) and leaving out the token or key; both are covered above.

**Continuous replication.** [Litestream](https://litestream.io/) streams a SQLite file's WAL to object storage.
It should work with Trailbeam, because the server only runs PASSIVE checkpoints, but we haven't tested it.
With Postgres (`--store-url`), use Postgres' own backups: see [Backup and restore (Postgres)](#backup-and-restore-postgres).

### Upgrading

Take a backup ([SQLite](#backup-and-restore-sqlite), [Postgres](#backup-and-restore-postgres)), stop the
old server, and start the new version on the same database file, Docker volume or Postgres database, with the
same flags and token. The new version upgrades the schema by itself at start. CI runs this from 0.3.0 to
the current version on every change, for SQLite (the PyPI package and the image) and Postgres, and checks that every
message, list and tenant count reads the same as before
([`python/smoke/upgrade_check.py`](python/smoke/upgrade_check.py)).

- **SQLite.** The new version moves the file to schema 16 (from 0.3.0's 12: a small table for the test span of
  `trailbeam doctor`, corrected counts for FIFO sends SQS deduplicated, the empty tables of people and tokens,
  and each span's `service.version`; only the summaries of FIFO messages sent more than once are rewritten).
  Once it has, **0.4.0 and 0.3.0 can't open it again**: they stop at start with `the database is at schema
  version 16, written by a newer Trailbeam ... Refusing to start, so nothing is lost`. To go back, restore the
  backup you took first.
- **Postgres.** Stop every replica before you start the new version. It moves the database to schema 10 (from
  0.3.0's 4, about 8 s per 1M spans, see the [changelog](CHANGELOG.md)). Once it has, 0.4.0 and 0.3.0 stop at
  start with `the database is at Postgres schema version 10, written by a newer Trailbeam ... Refusing to
  start, so nothing is lost`. To go back, restore the backup you took first.
- **Accounts stay off** unless every replica runs with `--accounts` (`TRAILBEAM_ACCOUNTS=1`), a preview in this
  release, and then until you create the first admin. Upgrade every replica first: a replica still on 0.4.0
  or 0.3.0 keeps letting `TRAILBEAM_TOKEN` into the web screen and the API after the others have switched.
  `trailbeam users create` reminds you on Postgres. Once an admin exists, `TRAILBEAM_TOKEN` opens only ingest and
  `/metrics`, and browsers signed in with it are signed out: give Prometheus and scripts a personal API token
  only if they read the API. With `--db :memory:`, people and tokens are lost at every restart.
- **The token and the receipt-hash key** don't change. Signed-in browsers stay signed in. Services keep their
  `TRAILBEAM_RECEIPT_HASH_KEY`: the hashing is the same in 0.3.0 and 0.4.0, so services on either package
  version can send spans for the same messages.
- **Moving to `trailbeam start`.** `trailbeam start --data-dir DIR` uses `DIR/trailbeam.sqlite`, and
  `npx trailbeam start` uses the Docker volume `trailbeam-data`, the same volume as the Quickstart's
  `docker run`. Before the first `start`, put the token your services send in `DIR/token`, or start with
  `--token-file`. If your 0.3.0 server ran without a token, `start` turns one on, and services that don't
  send it are refused (401) until they get `OTEL_EXPORTER_OTLP_HEADERS`. Put the key your services use in
  `DIR/receipt-hash-key`, so `start` prints that key and not a new one. If `start` has to make either file
  for an existing database, it logs a warning saying so.

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

### List endpoints and pages

The list endpoints (`/api/messages`, `/api/duplicates`, `/api/stuck`) return at most `limit` messages
(default 100, at most 1,000) and a `next_cursor`: pass it back as `cursor` for the next page, `null`
on the last one. `from_ms`/`to_ms` (milliseconds since the epoch, both included) keep messages last
seen in that range (Stuck: `from_ms`, first seen). Lists read a per-message summary the server keeps
up to date as spans arrive, so a page costs about the same however much history is stored.

### Metrics: `GET /metrics`

Trailbeam serves Prometheus metrics on its own port, at `/metrics`. When an
[access token](#access-token) is set, `/metrics` needs it too (in Prometheus, `authorization.credentials_file`);
without one, anyone who can reach the port can read it. Labels never hold span data: routes are templates such as
`/api/messages/{id}`.

| Metric | Type | What |
|---|---|---|
| `trailbeam_ingest_spans_total{result}` | counter | Spans in accepted exports: `inserted`, `duplicate` (already stored, e.g. a retry), `invalid` (rejected by the span contract), `ignored` (not Trailbeam spans). `rate()` gives spans/s |
| `trailbeam_ingest_requests_total{code}` | counter | Exports to `/v1/traces` by response status |
| `trailbeam_ingest_rejected_requests_total{reason}` | counter | 429s (`queue_full`) and 503s (`writer_behind`; `store_unreachable`: the database can't be reached; `store_full`: its disk is full; `store_timeout`: a Postgres statement timed out) |
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
| `trailbeam_stuck_messages` | gauge | Messages stuck now, as the Stuck page lists them (no final delete after `TRAILBEAM_STUCK_AFTER_S`), counted up to 1000 |
| `trailbeam_stuck_oldest_age_seconds` | gauge | How long ago the oldest stuck message was first seen; 0 when none is |
| `trailbeam_dead_lettered_messages_recent` | gauge | Messages moved to a dead-letter queue in the last 15 minutes |
| `trailbeam_stuck_after_seconds` | gauge | The stuck threshold, `TRAILBEAM_STUCK_AFTER_S` |

The stuck and dead-letter gauges are read from the store (at most every 10 seconds), so every replica of one
store serves the same value: take `max` across replicas, not `sum`. They have no queue or tenant label, which
would make the number of series grow with your queues and customers; the Stuck page and the queue filter say which.

There are no retention purge metrics yet: the server logs each purge pass that deletes spans, and any pass that fails.

A scrape config for Prometheus:

```yaml norun="a Prometheus configuration; the hidden step after it reads /metrics"
scrape_configs:
  - job_name: trailbeam
    scrape_interval: 15s
    metrics_path: /metrics
    static_configs:
      - targets: ["trailbeam:4318"]   # host:port Trailbeam listens on
```

<!-- doc-test variant=docker
curl -fsS http://127.0.0.1:4318/metrics | grep '^trailbeam_build_info'
-->

Useful alerts and queries:

```promql
sum(rate(trailbeam_ingest_spans_total{result="inserted"}[5m]))                      # spans/s stored
sum by (reason) (rate(trailbeam_ingest_rejected_requests_total[5m])) > 0           # backpressure: alert
histogram_quantile(0.99, sum by (le) (rate(trailbeam_ingest_request_duration_seconds_bucket{outcome="accepted"}[5m])))
histogram_quantile(0.95, sum by (le, route) (rate(trailbeam_http_request_duration_seconds_bucket[5m])))
trailbeam_ingest_queue_depth / trailbeam_ingest_queue_capacity > 0.8               # close to 429s
max(trailbeam_stuck_messages) > 0                                                   # a message is stuck: alert
max(trailbeam_dead_lettered_messages_recent) > 0                                    # a dead-letter move: alert
```

Ready-made files, each checked in CI against the real tool ([`deploy/monitoring`](deploy/monitoring/)):

- **Datadog:** the Agent's OpenMetrics check ([`datadog/openmetrics.yaml`](deploy/monitoring/datadog/openmetrics.yaml)),
  which collects these as `trailbeam.stuck_messages` and so on, and two monitors to import
  ([`datadog/monitors`](deploy/monitoring/datadog/monitors/)): messages stuck, and messages moved to a dead-letter queue.
- **Prometheus:** a scrape config and alert rules with their unit tests ([`prometheus`](deploy/monitoring/prometheus/)).
- **Grafana:** a dashboard to import ([`grafana/trailbeam-dashboard.json`](deploy/monitoring/grafana/trailbeam-dashboard.json)).

### Trace links: `--trace-link`

Every recorded step of a timeline comes from a span with a trace ID. Give Trailbeam a URL template for your
tracing tool and each of those steps shows **Open trace**, a link to the same trace there:

| Tool | `TRAILBEAM_TRACE_LINK` |
|---|---|
| Datadog | `https://app.datadoghq.com/apm/trace/{trace_id}` (your site's host, such as `app.datadoghq.eu`) |
| Grafana Tempo | `https://grafana.example.com/explore?schemaVersion=1&panes=%7B%22t%22:%7B%22datasource%22:%22tempo%22,%22queries%22:%5B%7B%22refId%22:%22A%22,%22queryType%22:%22traceql%22,%22query%22:%22{trace_id}%22%7D%5D,%22range%22:%7B%22from%22:%22{from_ms}%22,%22to%22:%22{to_ms}%22%7D%7D%7D` (your Tempo data source's UID for `tempo`) |
| Honeycomb | `https://ui.honeycomb.io/<team>/environments/<environment>/trace?trace_id={trace_id}&span={span_id}&trace_start_ts={from_s}&trace_end_ts={to_s}` |
| Jaeger | `http://jaeger.example.com:16686/trace/{trace_id}` |

The placeholders are `{trace_id}` (32 hex digits), `{trace_id_64}` (its low 64 bits as a decimal number, Datadog's
own form), `{span_id}`, `{start_ms}` and `{end_ms}` (the span's times, Unix milliseconds), and `{from_ms}` and
`{to_ms}` (an hour either side of them), and `{from_s}` and `{to_s}` (the same, in Unix seconds). Any other `{name}` stops the server at start, and so does a template that
isn't an http or https URL. `GET /api/messages/{id}` then has each link as `span.trace_url`.

A Trailbeam span is a child of the OpenTelemetry AWS SDK instrumentation's span for the same call, so it has your
service's trace ID. The link opens that trace when your service sends its OpenTelemetry traces to the tool and the
tool's sampling kept it; otherwise the tool finds no trace. Without the AWS SDK instrumentation (or in the Lambda
wrapper with no active span), a Trailbeam span starts a trace of its own, which only Trailbeam has.

## Open source and the hosted app

Everything about your messages is open source: the packages, the span contract, Trailbeam itself and
the views in [`@trailbeam/ui`](ui/). That covers a message's timeline, stuck messages and duplicate
deliveries, tenants, message bodies (opt-in, redacted in your process) and Lambda error text. The
hosted app at [trailbeam.app](https://trailbeam.app) is closed source. It adds what a team or a hosted
service needs: accounts and roles, several workspaces and AWS accounts, and hosting. It is not
generally available yet.

What each has today:

| | Open source | Hosted app |
|---|---|---|
| The packages for your services | yes | yes, the same packages, sending with an ingest key |
| A message's timeline, lookup by message ID | yes | yes |
| Stuck messages and duplicate deliveries | yes | yes |
| Tenants: sends, share, p95 time in queue, dead-lettered, per tenant | yes | yes |
| Message bodies, opt-in per queue, with `body.*` search | yes, for queues in `TRAILBEAM_STORE_BODIES`; anyone who can reach the server can read them | yes, for queues an admin opts in; owners and admins only |
| Lambda error text on the timeline, errors grouped by cause | yes | yes |
| JSON API | yes | |
| Overview of flows between services and queues, event explorer, dashboards | not yet | yes |
| Queue settings, depth and Lambda triggers read from AWS, and redrive | | yes, with a read-only role you connect (redrive needs a second, separate role) |
| Retention: global, per queue, or forever | yes | yes |
| Accounts, sign-in and invitations | | yes |
| Roles (owner, admin, member) | | yes |
| Several workspaces, several AWS accounts per workspace | | yes |
| Hosting | | yes, not generally available |
| SSO | | planned |
| A log of who viewed a message body | | planned |
| Billing | | planned |
| Backups | | planned |

Open-source Trailbeam makes no AWS calls, so it reads nothing from AWS and cannot redrive. "Planned"
means the hosted app does not have it yet.

Both draw the same views from the same rules. The hosted app installs this repository's
`@trailbeam/ui` as a normal dependency, and both are tested against the same recorded contract 3.2
spans in [`contract/fixtures/`](contract/fixtures/), with the expected tenant and body results there
too. App-only code (accounts, roles, billing, hosting) stays out of this repository.

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
  ([Message bodies](#message-bodies)). It records no message attributes except the one you name as the
  tenant attribute and the SQS Extended Client's payload size (`ExtendedPayloadSize`, else the legacy
  `SQSLargePayloadSize`), which it records as `trailbeam.body.offloaded_size` when the body is an S3
  pointer. To read them, the receive hooks add those names to `MessageAttributeNames`.
- It does not export receipt handles, only a keyed hash of each one.

## Documentation

The documentation is at [docs.trailbeam.app](https://docs.trailbeam.app).

| Page | Contents |
|---|---|
| [Quickstart](https://docs.trailbeam.app/quickstart) | From a clone to one message's timeline, on the local demo |
| [Set up Trailbeam for your team](https://docs.trailbeam.app/get-started/team) | One server for a team, the token shared, the first services instrumented and checked with `trailbeam doctor` |
| [How Trailbeam works](https://docs.trailbeam.app/concepts/how-it-works) | Components, data flow, what is recorded and what is inferred |
| [Run Trailbeam](https://docs.trailbeam.app/setup/run) | Run Trailbeam, natively or in a container |
| [HTTP API](https://docs.trailbeam.app/reference/api) | Trailbeam's JSON API |
| [Vocabulary](https://docs.trailbeam.app/concepts/vocabulary) | Terms |
| [Troubleshooting](https://docs.trailbeam.app/reference/troubleshooting) | Every `trailbeam doctor` failure, and other symptoms, with the cause and the fix |

What changed in each release, with upgrade notes: [CHANGELOG.md](CHANGELOG.md).

The reference for each part of this repository is next to its code: [`python/`](python/README.md),
[`node/`](node/README.md), [`java/`](java/README.md), [`collector/`](collector/README.md), [`demo/`](demo/README.md) and
[`examples/`](examples/README.md). The span
contract, the specification the packages and Trailbeam both follow, is
[`contract/span-contract.md`](contract/span-contract.md).

## Repository layout

| Directory | Contents |
|---|---|
| [`python/`](python/) | Python package `trailbeam`: boto3 hooks, a Lambda handler wrapper, and Trailbeam itself (`trailbeam.server`, installed with the `server` extra) |
| [`node/`](node/) | Node.js package `trailbeam`: AWS SDK v3 hooks and a Lambda handler wrapper |
| [`java/`](java/) | Java jar `trailbeam-aws-sdk-v2` (pre-release): a global AWS SDK for Java v2 interceptor and a Lambda handler wrapper |
| [`ui/`](ui/) | `@trailbeam/ui`: the views Trailbeam's web screen and the hosted app share (the timeline with Lambda errors grouped by cause, the Tenants table, and the Body tab with `body.*` search) |
| [`Dockerfile`](Dockerfile) | The Trailbeam container: `trailbeam[server]` with SQLite on a volume |
| [`collector/`](collector/) | OpenTelemetry Collector configuration |
| [`demo/`](demo/) | Local demo on ElasticMQ with Docker Compose, and the concurrency check (`demo/check.sh`) |
| [`contract/`](contract/) | The span contract, the receipt-hash test vectors, the server's API and ingest endpoint as OpenAPI ([`openapi.yaml`](contract/openapi.yaml)), the Tenants query ([`tenants.md`](contract/tenants.md)) and the recorded contract 3.2 spans both Trailbeam and the hosted app test against ([`fixtures/`](contract/fixtures/)) |

## Development

Run each line from the repository root:

```sh norun="the repository's tests: CI runs them in its python, node and ui jobs"
(cd python && pip install -e '.[test]' && pytest)   # the Python package and Trailbeam itself
(cd node && npm ci && npm test)
(cd ui && npm ci && npm test && npm run build)   # the shared UI, built into the Python package
pytest demo/check/tests
```

The Node.js tests that need an SQS emulator are skipped unless `TRAILBEAM_TEST_SQS_ENDPOINT` is set, for
example to `http://localhost:9324`. [`.github/workflows/ci.yml`](.github/workflows/ci.yml) also runs
the Collector tests, the container smoke test, the demo and the concurrency check.
[`.github/workflows/docs.yml`](.github/workflows/docs.yml) runs this README's commands and samples, and those
of the docs pages, against the commit; a block that isn't run says why (`norun="..."` after its language).

## Contributing

Issues and pull requests are welcome. Before opening a pull request, run the tests above for the
part you changed. If your change alters a span, update
[`contract/span-contract.md`](contract/span-contract.md) in the same pull request.

## License

[Apache-2.0](LICENSE).
