# qv2 (Python)

OpenTelemetry hooks for boto3 SQS calls and a Lambda handler wrapper (pre-release, not published).
Emits the spans defined in [`docs/span-contract.md`](../docs/span-contract.md).

## Install from the repository

```sh
pip install -e 'python[test]'   # from the repository root
```

## boto3 hooks

```python
import qv2.boto
qv2.boto.install()   # once per process, before or after creating SQS clients
```

This hooks `SendMessage(Batch)`, `ReceiveMessage`, `DeleteMessage(Batch)` and
`ChangeMessageVisibility(Batch)` through `opentelemetry-instrumentation-botocore`. If your
application already instruments botocore, qv2 keeps that setup and chains your hooks after its own.

| Setting | Default |
|---|---|
| `QV2_RECEIPT_HASH_KEY` (or `install(receipt_hash_key=...)`) | the published default key, with a warning |
| `QV2_OTLP_TRACES_ENDPOINT` | `http://localhost:4318/v1/traces` |
| `OTEL_SERVICE_NAME`, `OTEL_RESOURCE_ATTRIBUTES` | `unknown_service`, random `service.instance.id` |

## Tests

```sh
cd python && pytest -q
```

The hook tests use a local SQS emulator and never AWS: every `AWS_*` variable is removed and dummy
credentials are set. By default they start moto's server in-process. To use ElasticMQ or LocalStack
instead, set `QV2_TEST_SQS_ENDPOINT` (for example `http://localhost:9324`).
