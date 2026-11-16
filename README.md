<p align="center">
  <a href="https://trailbeam.app">
    <img src="docs/assets/banner.gif" alt="Trailbeam: messages cross a pre-dawn sky as trails of light, watched by an observatory on a mountain peak" width="880">
  </a>
</p>

<p align="center">
  <strong>The whole life of one Amazon SQS message, step by step.</strong>
</p>

<p align="center">
  <a href="https://trailbeam.app">Website</a> |
  <a href="https://docs.trailbeam.app">Docs</a> |
  <a href="docs/quickstart.md">Quickstart</a> |
  <a href="docs/architecture.md">How it works</a>
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
- **Works with the OpenTelemetry you already run.** The packages use their own tracer provider with
  an always-on sampler, so your sampling never drops a message's history.
- **Hands off your queues.** Trailbeam makes no SQS calls of its own and never calls `ReceiveMessage`,
  because a receive raises a message's receive count.
- **One container to run.** Trailbeam takes OTLP/HTTP, keeps spans in SQLite, and serves a JSON API
  and a web screen on one port.

## Quickstart

The [quickstart](docs/quickstart.md) runs Trailbeam on your laptop with Docker and an SQS emulator.
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

## Self-hosting

Trailbeam is one container. It receives OTLP on `/v1/traces` and serves the web screen and the JSON
API on the same port.

```sh
docker build -t trailbeam .
docker run -d --name trailbeam -p 127.0.0.1:4318:4318 -v trailbeam-data:/data trailbeam
```

Or without Docker: `pip install "trailbeam[server]"`, then run `trailbeam`.

There is no authentication in 0.1, so keep it on `127.0.0.1` or a trusted network. See
[Run Trailbeam](docs/running.md) and, to forward spans through an OpenTelemetry Collector, the
[Collector configuration](collector/README.md).

## Open source and the hosted app

Everything that tells you what happened to your messages is open source and runs on your own
machine. The hosted app at [trailbeam.app](https://trailbeam.app) adds what needs hosting or a team.
It is not generally available yet.

| | Open source | Hosted app |
|---|---|---|
| The packages for your services | yes | yes, the same packages with an ingest key |
| A message's timeline, lookup by message ID | yes | yes |
| Stuck messages and duplicate deliveries | yes | yes |
| Overview of flows between services and queues | coming | yes |
| Event explorer | coming | coming |
| JSON API | yes | |
| Accounts, workspaces and teams | | yes |
| Dashboards | | yes |
| Hosting, retention and backups | | yes |

Both show the same timeline from the same rules: the hosted app's message pages are checked against
this repository's code.

## How it works

1. OpenTelemetry hooks in your services emit one span per message for each SQS call your code makes:
   `SendMessage`, `ReceiveMessage`, `DeleteMessage`, `ChangeMessageVisibility` and their batch forms.
   A Lambda wrapper does the same for SQS events.
2. The spans go over OTLP/HTTP to Trailbeam, directly or through an OpenTelemetry Collector.
3. Trailbeam keeps the spans in SQLite. It joins each delete to the receive whose receipt handle it
   used, through a keyed hash of the handle.
4. When you open a message, Trailbeam builds its timeline.

Full explanation: [How Trailbeam works](docs/architecture.md).

## What Trailbeam does not do

- It makes no SQS calls of its own, and never calls `ReceiveMessage` on your queues. Trailbeam makes
  no AWS calls.
- It does not record message bodies or message attributes.
- It does not export receipt handles, only a keyed hash of each one.

## Documentation

| Page | Kind | Contents |
|---|---|---|
| [`docs/quickstart.md`](docs/quickstart.md) | tutorial | From a clone to one message's timeline, on the local demo |
| [`docs/architecture.md`](docs/architecture.md) | explanation | Components, data flow, what is recorded and what is inferred, failure cases |
| [`python/README.md`](python/README.md) | how-to | Add the hooks to boto3 code and Lambda handlers in Python |
| [`node/README.md`](node/README.md) | how-to | Add the hooks to AWS SDK v3 code and Lambda handlers in Node.js |
| [`docs/running.md`](docs/running.md) | how-to | Run Trailbeam, natively or in a container |
| [`collector/README.md`](collector/README.md) | how-to | Run the Collector configuration |
| [`demo/README.md`](demo/README.md) | how-to and results | The local demo, and the 0.1 concurrency check and its results |
| [`docs/span-contract.md`](docs/span-contract.md) | reference | The spans the packages emit and Trailbeam reads |
| [`docs/api.md`](docs/api.md) | reference | Trailbeam's HTTP API |
| [`docs/glossary.md`](docs/glossary.md) | reference | Terms |

## Repository layout

| Directory | Contents |
|---|---|
| [`python/`](python/) | Python package `trailbeam`: boto3 hooks, a Lambda handler wrapper, and Trailbeam itself (`trailbeam.server`, installed with the `server` extra) |
| [`node/`](node/) | Node.js package `trailbeam`: AWS SDK v3 hooks and a Lambda handler wrapper |
| [`Dockerfile`](Dockerfile) | The Trailbeam container: `trailbeam[server]` with SQLite on a volume |
| [`collector/`](collector/) | OpenTelemetry Collector configuration |
| [`demo/`](demo/) | Local demo on ElasticMQ with Docker Compose, and the concurrency check (`demo/check.sh`) |
| [`docs/`](docs/) | The pages listed above, and the receipt-hash test vectors |

## Development

Run each line from the repository root:

```sh
(cd python && pip install -e '.[test]' && pytest)   # the Python package and Trailbeam itself
(cd node && npm ci && npm test)
pytest demo/check/tests
```

The Node.js tests that need an SQS emulator are skipped unless `TRAILBEAM_TEST_SQS_ENDPOINT` is set, for
example to `http://localhost:9324`. [`.github/workflows/ci.yml`](.github/workflows/ci.yml) also runs
the Collector tests, the container smoke test, the demo and the concurrency check.

## Contributing

Issues and pull requests are welcome. Before opening a pull request, run the tests above for the
part you changed. If your change alters a span or Trailbeam's API, update
[`docs/span-contract.md`](docs/span-contract.md) or [`docs/api.md`](docs/api.md) in the
same pull request.

## License

[Apache-2.0](LICENSE).
