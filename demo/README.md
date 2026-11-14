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
- **"Concurrent, order unknown".** Each step is an interval from request to response, and a step is
  drawn above another only if it ended before the other started. Overlapping steps share one dashed
  box. In the demo that is most often a send and its first receive: idle consumers are already waiting in
  a long poll when the message is sent. See the [quickstart](../docs/quickstart.md#3-look-up-a-message-1-minute).
- **One clock.** All demo containers run on one host and read one clock, so their intervals can be
  compared. Across hosts, the store relies on the hosts' clocks and does not correct for an offset
  between them (a known limit of 0.1).
- **Truth files.** With `--truth DIR`, each driver process writes one JSON line per SQS call it makes, to
  its own file. The demo services write none; the [concurrency check](#concurrency-check) uses them.
- The AWS keys in [`compose.yaml`](compose.yaml) are dummy values for the emulator, not credentials.

## Concurrency check

The qv2 0.1 "done means" check: with three processes of four consumers each, every one of 200 messages
gets a complete timeline that never shows an effect before its cause, in Python and in Node.js separately.

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
| `in_order` | the timeline never shows an effect definitely before its cause (below) |
| `joined` | each delete and visibility change is attached to the delivery whose receipt handle was really used, with the real result |
| `receive_count` | each receive shows the real `ApproximateReceiveCount` |
| `final` | the message's state is `deleted` |

**In order.** The store shows each step as an interval (request sent to response received) and groups
steps whose intervals overlap as "concurrent, order unknown" ([`docs/store-api.md`](../docs/store-api.md#events-and-their-order)).
The checker takes the cause/effect pairs from the truth files: the send and each receive; receive *k* and
receive *k*+1; receive *k* and each delete or visibility change that used its handle; an accepted
`ChangeMessageVisibility` on delivery *k* and receive *k*+1. With the store's intervals and groups, a pair is

- **violated** if the effect's interval ends before the cause's starts, or the store puts the effect in an
  earlier group, or orders the pair although the intervals do not prove it (cause end < effect start);
- **concurrent** if both are in one group;
- **in order** otherwise.

A message is `in_order` when it is complete and none of its pairs is violated.

So that `in_order` cannot pass just by putting everything in one group, the summary also reports
`with_concurrent_group` (messages with at least one concurrent group) and `pairs_in_order`,
`pairs_concurrent`, `pairs_violated`, with `pairs_by_kind` per kind of pair. Two diagnostics split the
concurrent pairs: `concurrent_via_chain`, whose own intervals do not overlap (they share a group only
because other steps overlap both), and `concurrent_tied`, where the cause ended at exactly the effect's
start (a tie at the clock's resolution).

Each check is evaluated on its own: `joined` and `receive_count` compare the events whatever order the
store shows them in, so a message that fails on order still has its joins and receive counts measured. The
summary counts each column separately, and `failed_only_in_order` counts the messages whose only failure is
order.

The summary is printed and saved in `demo/results/<run>.txt`, the per-message report in
`demo/results/<run>-report.json`. Exit status 0 only if all 200 pass. Each run uses its own queue and a
fresh random receipt-hash key, so runs don't mix and the demo can keep running alongside.
[`check/tests/`](check/tests/) tests the comparison itself (`pytest demo/check/tests`).

### Result: 200 of 200, with most send/receive pairs left concurrent

Measured in CI on 2026-11-14 (GitHub Actions `ubuntu-latest`, ElasticMQ 1.6.15, all processes on one
host), three runs per language with span contract v2 and the rule above:

| Language | complete | in order | joined | receive count | final | messages with a concurrent group | pairs in order / concurrent / violated |
|---|---|---|---|---|---|---|---|
| Python | 200, 200, 200 | **200, 200, 200** | 200, 200, 200 | 200, 200, 200 | 200, 200, 200 | 197, 200, 196 | 186/330/0, 165/351/0, 188/328/0 |
| Node.js | 200, 200, 200 | **200, 200, 200** | 200, 200, 200 | 200, 200, 200 | 200, 200, 200 | 149, 181, 121 | 279/237/0, 172/344/0, 318/198/0 |

Each run: 3 consumer processes of 4 consumers each, plus 1 deleter process; 29 messages with a visibility
change, 28 deleted by the deleter process. No pair was violated in any run.

Pairs left concurrent, by kind (of 229 send/receive, 29 receive/receive, 229 receive/settle and 29
change-visibility/receive pairs per run):

| Kind | Python | Node.js |
|---|---|---|
| send, receive | 221, 227, 219 | 145, 186, 112 |
| receive *k*, receive *k*+1 | 24, 27, 23 | 12, 19, 6 |
| receive, its settle | 56, 68, 58 (all via chain in the last two runs) | 53, 110, 53 (third run: 42 tied at 1 ms, 11 via chain) |
| change visibility, next receive | 29, 28, 28 | 27, 27, 27 |

Why so many:

- **Long polling.** An idle consumer waits in `ReceiveMessage` (`WaitTimeSeconds`). The poll that returns
  a message was usually sent before the message was, so its interval contains the send; the same holds
  for the redelivery after a `ChangeMessageVisibility` to 0, where another consumer was already waiting.
  The order is true but not proven by the times, so the store leaves it concurrent. Node.js resolves more
  send/receive pairs than Python because its consumers are more often busy (not waiting) when a message
  arrives [estimate, not measured].
- **Chains.** Groups are connected components of "overlaps": a redelivery's long poll overlaps both the
  first receive and the visibility change after it, so all three are one group, although the first two
  are ordered by their own times.
- **Node.js clock resolution.** `Date.now()` is in milliseconds. Against a local emulator, a delete is
  often sent in the same millisecond its receive returned, which is a tie, not a proven order.

With span contract v1 and the old rule (the store's list order compared with the truth), the same check
passed 173 to 192 of 200 in Python and 183 to 185 in Node.js (QV-20): spans were stamped at the response,
and a receive could come back before the send that caused it.
