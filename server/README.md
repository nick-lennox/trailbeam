# server

The qv2 store (`qv2-store`): receives the qv2 spans of [`docs/span-contract.md`](../docs/span-contract.md)
over OTLP/HTTP, keeps them in SQLite, and joins every delete and visibility change to its message by
receipt-handle hash. The web screen and the container image are built on top of it (separate task).

```sh
pip install -e server            # Python 3.10+, depends only on opentelemetry-proto
qv2-store --db qv2.sqlite --port 4318
```

| Option | Environment | Default | |
|---|---|---|---|
| `--db` | `QV2_STORE_DB` | `qv2.sqlite` | SQLite file (WAL mode) |
| `--host` | `QV2_STORE_HOST` | `127.0.0.1` | Use `0.0.0.0` in a container |
| `--port` | `QV2_STORE_PORT` | `4318` | Use 4319 when the [Collector](../collector/) runs on the same host |
| `--stuck-after` | `QV2_STUCK_AFTER_S` | `300` | Seconds without a final delete after which a message is listed as stuck |
| `--lambda-timeout` | `QV2_LAMBDA_TIMEOUT_S` | `900` | Seconds after which a Lambda receive with no outcome is inferred killed |
| `--log-level` | `QV2_STORE_LOG_LEVEL` | `INFO` | |

With the default port, the qv2 packages can export to the store directly, without a Collector.

## Ingest

`POST /v1/traces`, OTLP/HTTP, as the [OTLP specification](https://opentelemetry.io/docs/specs/otlp/#otlphttp)
defines it: `application/x-protobuf` or `application/json`, optionally `Content-Encoding: gzip`
(the Collector's default). Only spans with instrumentation scope `qv2` are read; others are counted and
ignored. A qv2 span that breaks the contract (another `qv2.schema_version`, a required attribute missing)
is not stored and is reported in the response's `partial_success`. A span already stored (same trace and
span ID, for example an exporter retry) is skipped.

## The join

`DeleteMessage` and `ChangeMessageVisibility` take a receipt handle, not a message ID, and the process
that deletes is often not the one that received. Each settle span is joined to the `qv2 receive` with
the same `qv2.receipt_handle.hmac` and `qv2.receipt_handle.key_id`, which gives both its message and the
exact delivery whose handle was used. Spans arrive in any order, so the join runs when either side
arrives. A batch delete produces one span per entry, and each entry joins on its own. Hashes made with
different keys never join. See [`src/qv2_store/db.py`](src/qv2_store/db.py).

## JSON API

`GET /api/messages/{id}` (timeline), `GET /api/messages` (recent), `GET /api/duplicates`,
`GET /api/stuck`, `GET /api/health`. Documented in [`docs/store-api.md`](../docs/store-api.md).
Inferred steps (visibility-timeout expiry, the move to a dead-letter queue, the delete Lambda does after
a success, a Lambda invocation killed without an outcome, receives not observed) are always marked
`inferred: true`, without a time.

## Tests

```sh
pip install -e 'server[test]' -e 'python[test]'   # python/ only for the end-to-end test
pytest server/tests
```

- `test_join.py`: the join, including a batch delete of three messages in one call with one failed
  entry, a batch holding handles from two consumers, deletes that arrive before their receive, a late
  delete with the handle of an earlier delivery, key mismatches and restarts.
- `test_ingest.py`: OTLP/HTTP protobuf, JSON and gzip, ignored and rejected spans, bad requests.
- `test_timeline.py`: timelines and inferred steps (visibility-timeout expiry, `ChangeMessageVisibility`
  to 0, a stale delete, a poison message moving to the dead-letter queue, Lambda success, failure and
  kill, unobserved receives, clock skew, notices), duplicate deliveries and stuck messages.
- `test_api.py`: the JSON API over HTTP.
- `test_end_to_end.py`: the real `qv2` boto3 hooks on moto's local SQS emulator export to a running
  store; a batch delete from a second installation (empty handle map) joins all three messages by hash.
