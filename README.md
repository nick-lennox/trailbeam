# qv2

qv2 builds a timeline for each Amazon SQS message: sent, received, visibility changed, deleted, moved
to a dead-letter queue, and the Lambda outcome. You look up a message by its message ID.

Status: pre-release 0.1, unreleased and private. Nothing is published to PyPI, npm or a container
registry.

## How it works, in short

1. OpenTelemetry hooks in your services emit one span per message for each SQS call your code makes:
   `SendMessage`, `ReceiveMessage`, `DeleteMessage`, `ChangeMessageVisibility` and their batch forms.
   A Lambda wrapper does the same for SQS events.
2. The spans go over OTLP/HTTP to the qv2 store, directly or through an OpenTelemetry Collector.
3. The store keeps the spans in SQLite. It joins each delete to the receive whose receipt handle it
   used, through a keyed hash of the handle.
4. When you open a message, the store builds its timeline. It orders two events only when an observed
   fact proves the order. It marks every event that it inferred, such as a visibility timeout that
   expired.

Full explanation: [How qv2 works](docs/architecture.md).

What qv2 does not do:

- It makes no SQS calls of its own, and never calls `ReceiveMessage` on your queues. The store makes
  no AWS calls.
- It does not record message bodies or message attributes.
- It does not export receipt handles, only a keyed hash of each one.

## Try it

The [quickstart](docs/quickstart.md) runs qv2 on your laptop with Docker and an SQS emulator. It needs
no AWS account.

```sh
gh repo clone nick-lennox/qv2 && cd qv2
docker compose -f demo/compose.yaml up -d --build --wait
# then open http://127.0.0.1:4318/ and click a message ID
```

## Documentation

| Page | Kind | Contents |
|---|---|---|
| [`docs/quickstart.md`](docs/quickstart.md) | tutorial | From a clone to one message's timeline, on the local demo |
| [`docs/architecture.md`](docs/architecture.md) | explanation | Components, data flow, what is recorded and what is inferred, failure cases |
| [`python/README.md`](python/README.md) | how-to | Add the hooks to boto3 code and Lambda handlers in Python |
| [`node/README.md`](node/README.md) | how-to | Add the hooks to AWS SDK v3 code and Lambda handlers in Node.js |
| [`server/README.md`](server/README.md) | how-to | Run the store, natively or in a container |
| [`collector/README.md`](collector/README.md) | how-to | Run the Collector configuration |
| [`demo/README.md`](demo/README.md) | how-to and results | The local demo, and the 0.1 concurrency check and its results |
| [`docs/span-contract.md`](docs/span-contract.md) | reference | The spans the packages emit and the store reads |
| [`docs/store-api.md`](docs/store-api.md) | reference | The store's HTTP API |
| [`docs/glossary.md`](docs/glossary.md) | reference | Terms |

## Repository layout

| Directory | Contents |
|---|---|
| [`python/`](python/) | Python package `qv2`: boto3 hooks and a Lambda handler wrapper |
| [`node/`](node/) | Node.js package `@qv2/sdk`: AWS SDK v3 hooks and a Lambda handler wrapper |
| [`server/`](server/) | The store (`qv2-store`): OTLP/HTTP ingest, SQLite, JSON API and web screen, and its Dockerfile |
| [`collector/`](collector/) | OpenTelemetry Collector configuration |
| [`demo/`](demo/) | Local demo on ElasticMQ with Docker Compose, and the concurrency check (`demo/check.sh`) |
| [`docs/`](docs/) | The pages listed above, and the receipt-hash test vectors |

## Tests

Run each line from the repository root:

```sh
(cd python && pip install -e '.[test]' && pytest)
(cd node && npm ci && npm test)
pip install -e './server[test]' -e './python[test]' && pytest server/tests
pytest demo/check/tests
```

The Node.js tests that need an SQS emulator are skipped unless `QV2_TEST_SQS_ENDPOINT` is set, for
example to `http://localhost:9324`. [`.github/workflows/ci.yml`](.github/workflows/ci.yml) also runs
the Collector tests, the container smoke test, the demo and the concurrency check.

## License

[Apache-2.0](LICENSE).
