# demo

qv2 on your machine against an SQS emulator: no AWS account, no AWS credentials. Needs Docker with
Compose v2. Step-by-step from a fresh clone: [`docs/quickstart.md`](../docs/quickstart.md).

```sh
docker compose -f demo/compose.yaml up -d --build
# open http://127.0.0.1:4318/
docker compose -f demo/compose.yaml down        # add -v to delete the stored timelines too
```

## What runs

| Service | What it is |
|---|---|
| `sqs` | [ElasticMQ](https://github.com/softwaremill/elasticmq) 1.6.15 (`softwaremill/elasticmq-native`), an SQS-compatible emulator. Not published to the host. |
| `collector` | OpenTelemetry Collector contrib 0.165.0 with [`collector/qv2-collector.yaml`](../collector/qv2-collector.yaml) copied in ([`collector-image/Dockerfile`](collector-image/Dockerfile)). Receives OTLP from the services below and forwards the qv2 spans to the store. |
| `qv2` | The qv2 store and web screen ([`server/`](../server/)), on `127.0.0.1:4318` (set `QV2_DEMO_PORT` to change the host port). SQLite on the `qv2-data` volume. |
| `producer` | Python, [`python/qv2demo.py`](python/qv2demo.py) with the `qv2` boto3 hooks: one message a second to the queue `orders`. |
| `consumer-python` | Python, 4 consumer threads on `orders`, plus one thread that empties `orders-dlq`. |
| `consumer-node` | Node.js, [`node/qv2demo.js`](node/qv2demo.js) with `@qv2/sdk`: 4 concurrent consumers on `orders`. |
| `deleter` | Python, a separate process that deletes the messages the consumers hand to it, by receipt handle. |

The Python and Node.js consumers share the queue, so a message can be received by one and redelivered to
the other. `orders` has a visibility timeout of 10 s and a redrive policy to `orders-dlq` after 3 receives.
The images are built locally from this repository; nothing is pulled from or pushed to a registry except
the ElasticMQ, Collector, Python and Node.js base images.

## What to look at

Every message body is `{"kind": ..., "n": ...}`, and the kind decides what the consumer does:

| Kind | Share | Consumer behaviour | What the timeline shows |
|---|---|---|---|
| `ok` | most | deletes it | sent, received, deleted |
| `retry_once` | 1 in 10 | `ChangeMessageVisibility` to 0 on the first receive, deletes on the next | a visibility change, an **inferred** visibility-timeout expiry, a second delivery |
| `xproc` | 1 in 10 | hands the receipt handle to the `deleter` process | a delete from another process, joined by receipt-handle hash |
| `slow` | 1 in 10 | works for 13 s, past the 10 s visibility timeout, then deletes with its old handle | a duplicate delivery (`/duplicates`), and a late delete |
| `poison` | 1 in 25 | always fails | three deliveries, an **inferred** move to the dead-letter queue, a receive on `orders-dlq` |

Pick a message on the start page, or look at `/duplicates` and `/stuck` (stuck after 60 s in the demo).
The JSON API is under `/api/` ([`docs/store-api.md`](../docs/store-api.md)).

## Notes

- **Receipt-hash key.** The services hash receipt handles with `QV2_RECEIPT_HASH_KEY`. If it is not set
  when you run `docker compose up`, they use the published default key and the timeline page shows a
  notice saying so. To use your own: `QV2_RECEIPT_HASH_KEY=$(openssl rand -hex 32) docker compose -f demo/compose.yaml up -d`.
- **ElasticMQ and late deletes.** ElasticMQ rejects a delete with the handle of an earlier delivery
  (`ReceiptHandleIsInvalid`), so the `slow` messages' late deletes show as `failed`. Amazon SQS accepts
  such a delete; the store shows it as `stale_handle` there.
- **Truth files.** With `--truth DIR`, each driver process writes one JSON line per SQS call it makes, to
  its own file. The demo services write none.
- The AWS keys in [`compose.yaml`](compose.yaml) are dummy values for the emulator, not credentials.
