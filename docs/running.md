# Run Trailbeam

Trailbeam is one Python process, the `trailbeam` command. It does four things on one port:

1. receives the Trailbeam spans of [`docs/span-contract.md`](span-contract.md) over OTLP/HTTP;
2. keeps each span as one row in a SQLite file;
3. joins each delete and visibility change to its receive by receipt-handle hash;
4. serves a JSON API and a web screen, and builds each timeline when it is requested.

It makes no AWS calls and needs no AWS credentials. The [`Dockerfile`](../Dockerfile) runs it in one
container. How it fits with the other parts: [How Trailbeam works](architecture.md).

## Run it natively

```sh
pip install "trailbeam[server]"    # Python 3.10+; from a clone: pip install -e "python[server]"
trailbeam --db trailbeam.sqlite --port 4318
```

The `server` extra adds what Trailbeam needs on top of the package your services use. Without it,
`pip install trailbeam` installs only the hooks for boto3 and Lambda.

| Option | Environment | Default | |
|---|---|---|---|
| `--db` | `TRAILBEAM_DB` | `trailbeam.sqlite` | SQLite file (WAL mode) |
| `--host` | `TRAILBEAM_HOST` | `127.0.0.1` | Use `0.0.0.0` in a container |
| `--port` | `TRAILBEAM_PORT` | `4318` | Use 4319 when the [Collector](../collector/README.md) runs on the same host |
| `--stuck-after` | `TRAILBEAM_STUCK_AFTER_S` | `300` | Seconds without a final delete after which a message is listed as stuck |
| `--lambda-timeout` | `TRAILBEAM_LAMBDA_TIMEOUT_S` | `900` | Seconds after which a Lambda receive with no outcome is inferred killed |
| `--max-receive-count` | `TRAILBEAM_MAX_RECEIVE_COUNT` | none | `QUEUE=N,...` with N from 1 to 1000: each queue's `maxReceiveCount`, so a timeline can say "receive count exceeded `maxReceiveCount`; not seen on the DLQ yet" |
| `--log-level` | `TRAILBEAM_LOG_LEVEL` | `INFO` | |

With the default port, the Trailbeam packages can export to Trailbeam directly, without a Collector.

## Container

One image runs Trailbeam, its OTLP endpoint and the web screen on port 4318, with SQLite in `/data`.
Build it locally (no image is published):

```sh
docker build -t trailbeam .
docker run -d --name trailbeam -p 127.0.0.1:4318:4318 -v trailbeam-data:/data trailbeam
```

Open `http://127.0.0.1:4318/`, and export to `http://127.0.0.1:4318/v1/traces` (or point the
[Collector](../collector/README.md) there). The options above are set with `-e`, for example
`-e TRAILBEAM_STUCK_AFTER_S=600`; inside the image `TRAILBEAM_HOST` is `0.0.0.0` and `TRAILBEAM_DB` is
`/data/trailbeam.sqlite`. Publish the port on `127.0.0.1` as above unless the network is trusted: there is no
authentication in 0.1. Trailbeam runs as UID 10001; a named volume gets the right owner automatically, a
bind-mounted directory must be writable by that UID (or add `--user $(id -u)`). `docker stop` closes the
database cleanly. The image has a `HEALTHCHECK` on `/api/health`.

## Ingest

`POST /v1/traces`, OTLP/HTTP, as the [OTLP specification](https://opentelemetry.io/docs/specs/otlp/#otlphttp)
defines it: `application/x-protobuf` or `application/json`, optionally `Content-Encoding: gzip`
(the Collector's default) or `deflate`. Status codes and limits:
[API](api.md#post-v1traces-ingest). Only spans with instrumentation scope `trailbeam` are read; others are counted and
ignored. A Trailbeam span that breaks the contract (another `trailbeam.schema_version`, a required attribute missing)
is not stored and is reported in the response's `partial_success`. A span already stored (same trace and
span ID, for example an exporter retry) is skipped.

## The join

`DeleteMessage` and `ChangeMessageVisibility` take a receipt handle, not a message ID, and the process
that deletes is often not the one that received. Each settle span is joined to the `trailbeam receive` with
the same `trailbeam.receipt_handle.hmac` and `trailbeam.receipt_handle.key_id`, which gives both its message and the
exact delivery whose handle was used. Spans arrive in any order, so the join runs when either side
arrives. A batch delete produces one span per entry, and each entry joins on its own. Hashes made with
different keys never join. See [`db.py`](../python/src/trailbeam/server/db.py).

## Web screen

Open `http://127.0.0.1:4318/` in a browser. The message list, duplicates and stuck pages are
server-rendered HTML. A message's timeline is drawn by the shared UI ([`ui/`](../ui/), the same view the
hosted app uses), which reads `/api/messages/{id}` in the browser; it comes with the container and with
releases. A Trailbeam installed from a clone without building it (`cd ui && npm ci && npm run build`)
serves an HTML timeline instead, with the same steps.


- `/`: look up a message by ID, and the most recent messages (`?queue=`, `?limit=`).
- `/message?id=<message ID>`: the message's timeline. Each step shows its interval, from when its SQS
  request was sent to when the response came back. A Lambda receive shows only "no later than" one
  time. A step is drawn above another only when an observed fact proves the order: the clocks, the
  call order in one process, SQS's receive count, or SQS's timestamps. Each step names the fact under
  it. Steps that no fact orders are drawn in one dashed box with a `concurrent` badge and the label
  "concurrent, order unknown". Inside the box each step is a bar on one time scale (a waterfall),
  sorted by request start time, never by kind or cause
  ([API](api.md#events-and-their-order)). Steps Trailbeam inferred have a dashed,
  striped border and an `inferred` badge, with their time window and reason instead of a time. Notices,
  such as a receipt-hash key mismatch, are shown above the summary.
- `/duplicates`: duplicate deliveries (`?reason=received_more_than_once|succeeded_more_than_once`).
- `/stuck`: stuck messages (`?older_than_s=`, default `--stuck-after`).

The pages are built from the same objects as the JSON API below, so they show exactly what it returns.

## JSON API

`GET /api/messages/{id}` (timeline), `GET /api/messages` (recent), `GET /api/duplicates`,
`GET /api/stuck`, `GET /api/health`. Documented in [`api.md`](api.md).
Inferred steps (visibility-timeout expiry, the delete Lambda does after a success, a Lambda invocation
killed without an outcome, receives not observed) are always marked `inferred: true`, without a time.
The move to a dead-letter queue is never inferred. It is shown only when SQS reports it with
`DeadLetterQueueSourceArn` on the DLQ receive (`source: sqs_dlq_source`, contract §9b).

## Tests

Trailbeam's tests are in [`python/tests/server/`](../python/tests/server/) and run with the package's own:

```sh
cd python && pip install -e '.[test]' && pytest tests/server
```

- `test_join.py`: the join, including a batch delete of three messages in one call with one failed
  entry, a batch holding handles from two consumers, deletes that arrive before their receive, a late
  delete with the handle of an earlier delivery, key mismatches and restarts.
- `test_ingest.py`: OTLP/HTTP protobuf, JSON and gzip, ignored and rejected spans, bad requests.
- `test_timeline.py`: timelines and inferred steps (visibility-timeout expiry, `ChangeMessageVisibility`
  to 0, a stale delete, a poison message whose move to the dead-letter queue SQS reports, and one whose
  move nobody has seen yet, Lambda success, failure and
  kill, unobserved receives, clock skew, notices), duplicate deliveries and stuck messages.
- `test_api.py`: the JSON API over HTTP.
- `test_web.py`: the web screen over HTTP: inferred steps marked, timeline order, DLQ moves, duplicates and
  stuck lists, filters, HTML escaping of message IDs, 400 and 404 pages.
- `test_main.py`: the `trailbeam` command serves the screen and exits cleanly on SIGTERM.
- `test_end_to_end.py`: the real `trailbeam` boto3 hooks on moto's local SQS emulator export to a running
  Trailbeam; a batch delete from a second installation (empty handle map) joins all three messages by hash.

### Container smoke test

[`smoke_test.py`](../python/smoke/smoke_test.py) (standard library only) checks a running Trailbeam: it posts
[`recorded-spans.pb`](../python/smoke/recorded-spans.pb), real spans recorded with the `trailbeam` boto3 hooks on
moto's local SQS emulator by [`record_spans.py`](../python/smoke/record_spans.py) (a redelivery after a
visibility timeout, a `ChangeMessageVisibility` to 0, a message never received), then checks that the
timeline pages show them in order with the inferred steps marked, and the duplicate and stuck lists. CI
builds the image, runs it on a volume, runs the smoke test, then restarts the container on the same volume
and runs it again.

```sh
docker build -t trailbeam . && docker run -d --name trailbeam -p 127.0.0.1:4318:4318 trailbeam
python3 python/smoke/smoke_test.py http://127.0.0.1:4318
```
