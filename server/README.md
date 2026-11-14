# server

The qv2 store (`qv2-store`): receives the qv2 spans of [`docs/span-contract.md`](../docs/span-contract.md)
over OTLP/HTTP, keeps them in SQLite, and joins every delete and visibility change to its message by
receipt-handle hash, and serves a web screen and a JSON API. The [`Dockerfile`](Dockerfile) runs it all in one container.

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

## Container

One image runs the store, its OTLP endpoint and the web screen on port 4318, with SQLite in `/data`.
Build it locally (no image is published):

```sh
docker build -t qv2-store server
docker run -d --name qv2 -p 127.0.0.1:4318:4318 -v qv2-data:/data qv2-store
```

Open `http://127.0.0.1:4318/`, and export to `http://127.0.0.1:4318/v1/traces` (or point the
[Collector](../collector/) there). The options above are set with `-e`, for example
`-e QV2_STUCK_AFTER_S=600`; inside the image `QV2_STORE_HOST` is `0.0.0.0` and `QV2_STORE_DB` is
`/data/qv2.sqlite`. Publish the port on `127.0.0.1` as above unless the network is trusted: there is no
authentication in 0.1. The store runs as UID 10001; a named volume gets the right owner automatically, a
bind-mounted directory must be writable by that UID (or add `--user $(id -u)`). `docker stop` closes the
database cleanly. The image has a `HEALTHCHECK` on `/api/health`.

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

## Web screen

Open `http://127.0.0.1:4318/` in a browser (plain server-rendered HTML, no JavaScript):

- `/`: look up a message by ID, and the most recent messages (`?queue=`, `?limit=`).
- `/message?id=<message ID>`: the message's timeline in order. Steps the store inferred are drawn with a
  dashed, striped border and an **INFERRED** label, with the time window they fall in and the reason,
  instead of a time. Notices (such as a receipt-hash key mismatch) are shown above the summary.
- `/duplicates`: duplicate deliveries (`?reason=received_more_than_once|succeeded_more_than_once`).
- `/stuck`: stuck messages (`?older_than_s=`, default `--stuck-after`).

The pages are built from the same objects as the JSON API below, so they show exactly what it returns.

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
- `test_web.py`: the web screen over HTTP: inferred steps marked, timeline order, DLQ moves, duplicates and
  stuck lists, filters, HTML escaping of message IDs, 400 and 404 pages.
- `test_main.py`: the `qv2-store` command serves the screen and exits cleanly on SIGTERM.
- `test_end_to_end.py`: the real `qv2` boto3 hooks on moto's local SQS emulator export to a running
  store; a batch delete from a second installation (empty handle map) joins all three messages by hash.

### Container smoke test

[`smoke/smoke_test.py`](smoke/smoke_test.py) (standard library only) checks a running store: it posts
[`smoke/recorded-spans.pb`](smoke/recorded-spans.pb), real spans recorded with the `qv2` boto3 hooks on
moto's local SQS emulator by [`smoke/record_spans.py`](smoke/record_spans.py) (a redelivery after a
visibility timeout, a `ChangeMessageVisibility` to 0, a message never received), then checks that the
timeline pages show them in order with the inferred steps marked, and the duplicate and stuck lists. CI
builds the image, runs it on a volume, runs the smoke test, then restarts the container on the same volume
and runs it again.

```sh
docker build -t qv2-store server && docker run -d --name qv2 -p 127.0.0.1:4318:4318 qv2-store
python3 server/smoke/smoke_test.py http://127.0.0.1:4318
```
