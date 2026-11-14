# qv2

Message-level timelines for Amazon SQS. Pre-release (0.1, unreleased) and private; nothing is published.

qv2 records what happened to each SQS message (sent, received, processed, deleted, visibility
changed) through OpenTelemetry hooks in your own services, and shows the history of any message by ID
in a self-hosted container.

Each step on a timeline is a time interval: from when the SQS request was sent to when its response came
back. A step is shown before another only if it ended before the other started. Steps whose intervals
overlap are shown side by side, labelled **concurrent, order unknown**, because the times cannot say which
one SQS did first; qv2 never re-orders them by guessing what caused what. See the
[quickstart](docs/quickstart.md#3-look-up-a-message-1-minute) for what that looks like, and
[`docs/store-api.md`](docs/store-api.md#events-and-their-order) for the exact rule. This relies on every
process reading the same clock; in 0.1 the store does not correct for clock offset between hosts.

## Try it

**[Quickstart](docs/quickstart.md):** from a fresh clone to a message's timeline in a few minutes, with
Docker and an SQS emulator. No AWS account needed.

```sh
git clone https://github.com/nick-lennox/qv2 && cd qv2
docker compose -f demo/compose.yaml up -d --build --wait
# open http://127.0.0.1:4318/ and click a message ID
```

## Layout

| Directory | Contents |
|---|---|
| [`python/`](python/) | `qv2` Python package: boto3 hooks and a Lambda handler wrapper |
| [`node/`](node/) | `@qv2/sdk` Node.js package: AWS SDK v3 hooks and a Lambda handler wrapper |
| [`server/`](server/) | Store (SQLite) and web screen, in one container |
| [`collector/`](collector/) | OpenTelemetry Collector configuration that keeps every SQS span |
| [`demo/`](demo/) | Local demo on ElasticMQ with Docker Compose, and the 0.1 concurrency check (`demo/check.sh`) |
| [`docs/`](docs/) | [`quickstart.md`](docs/quickstart.md); [`span-contract.md`](docs/span-contract.md): the spans the packages emit and the store reads; [`store-api.md`](docs/store-api.md): the store's JSON API |

## Tests

```sh
cd python && pip install -e '.[test]' && pytest
cd node && npm test
pip install -e './server[test]' -e './python[test]' && pytest server/tests
pytest demo/check/tests
```

## License

[Apache-2.0](LICENSE).
