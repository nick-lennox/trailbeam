# qv2

Message-level timelines for Amazon SQS. Pre-release and private; nothing here is usable yet.

qv2 records what happened to each SQS message (sent, received, processed, deleted, visibility
changed) through OpenTelemetry hooks in your own services, and shows the history of any message by ID
in a self-hosted container.

## Layout

| Directory | Contents |
|---|---|
| [`python/`](python/) | `qv2` Python package: boto3 hooks and a Lambda handler wrapper |
| [`node/`](node/) | `@qv2/sdk` Node.js package: AWS SDK v3 hooks and a Lambda handler wrapper |
| [`server/`](server/) | Store (SQLite) and web screen, in one container |
| [`collector/`](collector/) | OpenTelemetry Collector configuration that keeps every SQS span |
| [`demo/`](demo/) | Local demo against an SQS emulator; no AWS account needed |
| [`docs/`](docs/) | [`span-contract.md`](docs/span-contract.md): the spans the packages emit and the store reads |

## Tests

```sh
cd python && pip install -e '.[test]' && pytest
cd node && npm test
```

## License

[Apache-2.0](LICENSE).
