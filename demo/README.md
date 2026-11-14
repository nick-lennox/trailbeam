# demo

qv2 on your machine against an SQS emulator: no AWS account, no AWS credentials. Needs Docker with
Compose v2. Step-by-step from a fresh clone: [`docs/quickstart.md`](../docs/quickstart.md).

```sh
docker compose -f demo/compose.yaml up -d --build
# open http://127.0.0.1:4318/
docker compose -f demo/compose.yaml down        # add -v to delete the stored timelines too
```

### More than one checkout on the same machine

The Compose project is called `qv2-demo`. Its containers, its volumes (`qv2-demo_qv2-data` with the stored
timelines, `qv2-demo_truth`), its network and its local image tags (`qv2-demo-store:local` and so on) all
carry that name, so a second checkout started the same way **replaces** the first one's containers and
uses its volume. To run a second checkout alongside, give it its own project name and port:

```sh
export COMPOSE_PROJECT_NAME=qv2-other     # or: docker compose -p qv2-other ...
QV2_DEMO_PORT=4400 docker compose -f demo/compose.yaml up -d --build --wait
demo/check.sh python                      # the check uses COMPOSE_PROJECT_NAME too
```

Every later `docker compose` command for that checkout (`ps`, `logs`, `down`) needs the same name.

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
  its own file. The demo services write none; the [concurrency check](#concurrency-check) uses them.
- The AWS keys in [`compose.yaml`](compose.yaml) are dummy values for the emulator, not credentials.

## Concurrency check

The qv2 0.1 "done means" check: with three processes of four consumers each, every one of 200 messages
gets a complete timeline in the right order, in Python and in Node.js separately.

```sh
demo/check.sh python          # or: demo/check.sh node; optional second argument: message count (200)
DOCKER=docker.exe demo/check.sh python   # in WSL without Docker Desktop's WSL integration
```

`check.sh` runs `docker` by default; set `DOCKER` to the command to use instead. In WSL without the
integration, plain `docker` fails with `failed to connect to the docker API at unix:///var/run/docker.sock`.

[`check.sh`](check.sh) starts `sqs`, `collector` and `qv2`, creates a fresh queue for the run, then starts
(with `docker compose run`, each in its own container) one deleter process and three consumer processes of
four consumers each (threads in Python, concurrent receive loops in Node.js), all in the chosen language,
and a producer that sends 200 messages: the QV-11 recheck B2 mix of `ok`, `retry_once` (1 in 7) and
`xproc` (1 in 7, deleted by the deleter process). Every process writes a truth file. When every message is
deleted, the processes are stopped (which flushes their spans) and [`check/check.py`](check/check.py)
reads each message's timeline from the store's JSON API and compares it with the truth files. A message
passes only if its timeline is:

| Check | Meaning |
|---|---|
| `complete` | its observed events are exactly the SQS calls the processes made: send, every receive, every visibility change, the delete |
| `in_order` | in the store's order, they are those calls in the order they happened |
| `joined` | each delete and visibility change is attached to the delivery whose receipt handle was really used, with the real result |
| `receive_count` | each receive shows the real `ApproximateReceiveCount` |
| `final` | the message's state is `deleted` |

Each check is evaluated on its own: `joined` and `receive_count` compare the events whatever order the
store shows them in, so a message that fails on order still has its joins and receive counts measured. The
summary counts each column separately, and `failed_only_in_order` counts the messages whose only failure is
order.

The summary is printed and saved in `demo/results/<run>.txt`, the per-message report in
`demo/results/<run>-report.json`. Exit status 0 only if all 200 pass. Each run uses its own queue and a
fresh random receipt-hash key, so runs don't mix and the demo can keep running alongside.
[`check/tests/`](check/tests/) tests the comparison itself (`pytest demo/check/tests`).

### Result: not 200 of 200 yet

Measured on 2026-11-13 (ElasticMQ 1.6.15, Docker Desktop 4.70 on WSL2, all processes on one host), three
runs per language:

| Language | complete | in order (passed) | final | processes / consumers seen |
|---|---|---|---|---|
| Python | 200, 200, 200 | **173, 192, 191** | 200, 200, 200 | 3 + 1 deleter / 12 |
| Node.js | 200, 200, 200 | **183, 184, 185** | 200, 200, 200 | 3 + 1 deleter / 12 |

Every span arrives and every delete joins to its message, including the 28 deleted by another process.
The failures are all **order**, in two patterns:

- **A receive shown before its send** (most failures). A consumer's long poll returned the message before
  the producer's `SendMessage` call returned, by 0.0003 to 1.3 ms.
- **A redelivery shown before the `ChangeMessageVisibility` to 0 that caused it**, by 0.06 to 5 ms.
  Another consumer received the message before the change call returned.

The cause is the timestamps, not the check. The span contract
([`docs/span-contract.md`](../docs/span-contract.md), sections 2 and 4) makes every qv2 span zero-length, emitted
from the response, with the time it was emitted. The store orders events by that time. But SQS acts on a
call before its response gets back, so on a fast network a receive can return before the send that caused
it does. These are not clock problems (all processes here share one clock). The QV-11 prototype passed 200
of 200 against Amazon SQS. Its settle spans were stamped at the request, and its send spans at the response
as now; that sends never lost the race there is probably due to the network round trip to AWS (an
estimate, not measured). The fix is open: record when
the call started as well, and have the store order by cause where it is known (a message's send comes
before its first receive; an accepted visibility change to 0 comes before the next delivery).
