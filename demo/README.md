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
| `poison` | 1 in 25 | always fails | three deliveries, a move to the dead-letter queue **reported by SQS** (`DeadLetterQueueSourceArn` on the `orders-dlq` receive, never inferred), a receive on `orders-dlq` |

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
| `edges_verified` | every edge the store claims in `order.edges` holds: each of its sources recomputes from the events' facts, and no order the edges imply contradicts the truth files (below) |
| `joined` | each delete and visibility change is attached to the delivery whose receipt handle was really used, with the real result |
| `receive_count` | each receive shows the real `ApproximateReceiveCount` |
| `sqs_facts` | each receive shows the `SentTimestamp` and `ApproximateFirstReceiveTimestamp` SQS really returned for it (below) |
| `in_brackets` | Python only: each step's start and end lie inside its call's truth bracket (below) |
| `final` | the message's state is `deleted` |
| `dlq_move` | the timeline shows a move to a dead-letter queue exactly when the message was received on a second queue: observed (`source: sqs_dlq_source`), from the first queue to the second, and never inferred |

**The dead-letter path (`--dlq`).** `demo/check.sh --dlq python|node` uses the `check-dlq` mix: the same
mix plus a `poison_fast` message 1 in 20 (10 of 200). The consumers release it at once on every receive
(`ChangeMessageVisibility` to 0). After `maxReceiveCount` (3) receives, SQS moves it to `<queue>-dlq`, and
each consumer process's DLQ reader receives and deletes it there. Its truth lines name the queue. The
summary counts `dlq_messages`, `dlq_moves_observed` and `dlq_moves_inferred`, which must be 0. CI's
ElasticMQ runs use `--dlq`. The real-SQS runs don't, because the move on real SQS is not approved yet
(QV-33).

**In order.** The store shows a step before another only when an observed fact proves it, and says which
(`order.edges`, each with its sources: `clock`, `process_order`, `receive_count`, `sqs_timestamps`;
[`docs/store-api.md`](../docs/store-api.md#events-and-their-order)). What no fact orders is "concurrent,
order unknown". The checker takes the cause/effect pairs from the truth files: the send and each receive;
receive *k* and receive *k*+1; receive *k* and each delete or visibility change that used its handle; an
accepted `ChangeMessageVisibility` on delivery *k* and receive *k*+1. A pair is

- **violated** if the effect's interval ends before the cause's starts, or the store proves the effect
  before the cause, or puts the effect in an earlier group, or puts the cause in an earlier group without
  an edge chain that proves it;
- **in order** ("resolved") if the store's edges prove the cause before the effect;
- **concurrent** otherwise.

A message is `in_order` when it is complete and none of its pairs is violated.

**Edges are checked, not trusted** (`edges_verified`, QV-34). The pair checks above would pass a store that
put every send before every receive and labelled it `sqs_timestamps`: it agrees with what caused what. So
the checker recomputes every source of every edge from the facts the store reports for the two events
(the same rules as [`order.py`](../server/src/qv2_store/order.py), written again in the checker):

- `clock`: `from.end < to.start`;
- `process_order`: the same call-order scope and `from.returned < to.issued` (the events' `process_order`);
- `receive_count`: two receives on the same queue, with counts that go up;
- `sqs_timestamps`: the send and the first receive (count 1) on the same queue, with
  `sent_timestamp_ms < first_receive_timestamp_ms`.

An edge with no source, an unknown source, a source that does not recompute, or an end that is not an
observed step fails the message. Then every order the edges imply (their transitive closure) is compared
with the truth files. Each truth line brackets its call (`t_start_ns` just before it was made, `t_end_ns`
just after it returned, on the one host's clock), so "A before B" is contradicted when B's call returned
before A's was made. That is the only order one host's brackets prove, and it is sound: a true edge has A
return before B is issued, which is inside both brackets. It catches a false edge between any two steps,
including two no cause links (a stale delete and the next receive). The summary counts `edges_checked` and
`implied_orders_checked`, and the failures `edges_unverified` and `implied_orders_contradicting_truth`.

**The facts are checked too** (QV-38). Recomputing a source only proves the store's facts agree with its
edges. A store whose facts are wrong (QV-35's "fake2": `first_receive_timestamp_ms` one millisecond after
`sent_timestamp_ms`, and call-order ticks in one scope for a receive and a delete made in two processes)
passed every check above and the floor. So the truth files now record the facts too, and the checker
compares them:

- `sqs_facts`: each driver asks SQS for `SentTimestamp` and `ApproximateFirstReceiveTimestamp` itself (not
  through the qv2 hooks) and writes them on the receive's truth line (`sent_ms`, `first_receive_ms`, with `rc`).
  The store's `sent_timestamp_ms` and `first_receive_timestamp_ms` must be equal to them, and a missing value
  counts as different. Summary: `receives_sqs_facts_compared`, `receives_sqs_facts_wrong`.
- `process_order` holds only between two calls the truth shows were made by one process (same `host` and
  `pid`), since the call-order scope is a process (Python) or a realm in one (Node.js). Otherwise the edge
  fails `edges_verified`.
- `in_brackets`: the Python driver's truth and the qv2 hooks' spans both read `time.time_ns()`, so each truth
  line says `same_clock_as_spans`, and each step's `start` and `end` must lie inside its call's bracket
  (`t_start_ns <= start <= end <= t_end_ns`). That checks the times the `clock` source is recomputed from.
  Node.js lines don't say it: the truth has whole milliseconds and the spans add a sub-millisecond part,
  so the two can't be compared to the nanosecond, and `in_brackets` holds there with nothing compared.
  Summary: `events_checked_in_brackets` (0 for Node.js), `events_outside_brackets`.

Call-order ticks within one process are not recorded in the truth. A false order between two steps of one
process still fails, because the truth contradicts it (above).

**The floor.** So that `in_order` cannot pass by leaving things concurrent, the run also fails unless at
least 90% of the send→receive pairs and 90% of the receive→delete pairs are resolved (`floor` in the
summary; `--floor` to change it, which CI does not). On the emulator `check.sh` reports the floor but doesn't
gate on it ([the floor in CI](#the-floor-in-ci)); with `--aws` it gates. `resolution` gives, per kind (send→receive,
receive→delete, release→redelivery, receive *n*→receive *n*+1), the pairs resolved and left concurrent, and
by source: `by_source` (pairs whose direct edge has that source; a pair can count under several),
`only_source` (that source alone), `via_chain` (resolved only through other steps) and `chain_sources`.
Release→redelivery has no floor, because SQS gives no time for a redelivery; it is reported anyway. The
older columns stay: `with_concurrent_group`, `pairs_in_order`, `pairs_concurrent`, `pairs_violated`,
`pairs_by_kind`, and the diagnostics `concurrent_via_chain` and `concurrent_tied`.

Each check is evaluated on its own: `joined` and `receive_count` compare the events whatever order the
store shows them in, so a message that fails on order still has its joins and receive counts measured. The
summary counts each column separately, and `failed_only_in_order` counts the messages whose only failure is
order.

The summary is printed and saved in `demo/results/<run>.txt`, the per-message report in
`demo/results/<run>-report.json`. Exit status 0 only if all 200 pass and, where it gates (`--aws`), the floor holds. Each run uses its own queue and a
fresh random receipt-hash key, so runs don't mix and the demo can keep running alongside.
[`check/tests/`](check/tests/) tests the comparison itself (`pytest demo/check/tests`).

### Result with the four order sources (QV-31): 200 of 200 in order, floor fails on send→receive

Span contract v3, ElasticMQ 1.6.15, all processes on one host. One run per language **locally** (WSL 2
on Windows, Docker Desktop, 2026-11-14) and one **in CI** (GitHub Actions `ubuntu-latest`, run
43624764060, commit d16190e; the floor was reported but not yet gating):

| | Python local | Python CI | Node.js local | Node.js CI |
|---|---|---|---|---|
| complete, in order, joined, receive count, final | 200 each | 200 each | 200 each | 200 each |
| pairs violated | 0 | 0 | 0 | 0 |
| messages with a concurrent group | 195 | 189 | 194 | 86 |
| **send→receive** resolved | **10 / 229 (4.4%)** | **18 / 229 (7.9%)** | **8 / 229 (3.5%)** | **159 / 229 (69.4%)** |
| **receive→delete** resolved | **200 / 200** | **200 / 200** | **200 / 200** | **200 / 200** |
| release→redelivery resolved (no floor) | 0 / 29 | 0 / 29 | 0 / 29 | 1 / 29 |
| receive *n*→*n*+1 resolved | 29 / 29 | 29 / 29 | 29 / 29 | 29 / 29 |

By source (pairs whose direct edge has it; "alone" = the only source of that edge):

| Kind | Python local | Python CI | Node.js local | Node.js CI |
|---|---|---|---|---|
| send→receive | SQS timestamps 6, clock 3, chain 1 | SQS timestamps 12, clock 6, chain 1 | SQS timestamps 6, clock 2 | SQS timestamps 133 (63 alone), clock 96 (26 alone) |
| receive→delete | clock 200, process order 172 | clock 200, process order 172 | process order 172 (141 alone), clock 59 (28 alone) | process order 172 (34 alone), clock 166 (28 alone) |
| receive *n*→*n*+1 | receive count 29 (27 alone), clock 2 | receive count 29 (25 alone), clock 4 | receive count 29 (28 alone) | receive count 29 (7 alone), clock 22, process order 6 |
| release→redelivery | none | none | none | clock and process order 1 |

So the check fails, in both languages, locally and in CI, on one column: **send→receive is far below
90%**. The floor is not lowered. Why it can't be reached honestly with these sources on this workload:

- **The clocks and the call order can't apply.** An idle consumer is already waiting in a long poll
  (`ReceiveMessage` with `WaitTimeSeconds`) when the message is sent, so the receive's interval contains
  the send, and the producer and consumer are different processes.
- **SQS's timestamps tie.** ElasticMQ stamps `SentTimestamp` and `ApproximateFirstReceiveTimestamp` in
  milliseconds, and a waiting long poll gets the message within the same millisecond. Measured for QV-31
  with 12 waiting consumers and 200 sends: the two were equal for 196 of 200 messages (1 ms apart for 3,
  4 ms for 1). Equal timestamps prove nothing, so the pair stays concurrent. CI resolves more in Node.js
  because the runner is slower and more receives land a millisecond or more after the send.
- Ordering it anyway by "a receive comes after its send" would be ordering by cause, which the store
  does not do.

Receive→delete passes everywhere: the call order resolves every same-process delete (172 of 200; in
Node.js 141 of them locally only by it, because the delete is issued in the same millisecond the receive
returned), and the clocks resolve the 28 deletes made by the deleter process. Release→redelivery stays
concurrent for the same long-poll reason: the consumer that gets the redelivery was already waiting
before the `ChangeMessageVisibility` returned, usually in another thread.

### The same workload on real SQS: the floor holds in both languages

`demo/check.sh --aws python|node` runs the unchanged workload against real SQS instead of ElasticMQ
(the Collector, the store and every process stay on this host). It reads `AWS_ACCESS_KEY_ID` and
`AWS_SECRET_ACCESS_KEY` from the environment, passes them to the containers by name only, uses queues
`qv2-measure-<run>` and `<run>-dlq`, and deletes them when the run ends. CI runs it too (see
[the floor in CI](#the-floor-in-ci)).

Measured once per language on 2026-11-14, from this machine (WSL 2, Docker Desktop) against SQS in
`us-east-1`:

| | Python, real SQS | Node.js, real SQS | Python, ElasticMQ local | Node.js, ElasticMQ local |
|---|---|---|---|---|
| complete, in order, joined, receive count, final | 200 each | 200 each | 200 each | 200 each |
| pairs violated | 0 | 0 | 0 | 0 |
| messages with a concurrent group | 29 | 29 | 195 | 194 |
| **send→receive** resolved | **229 / 229** | **229 / 229** | 10 / 229 (4.4%) | 8 / 229 (3.5%) |
| **receive→delete** resolved | **200 / 200** | **200 / 200** | 200 / 200 | 200 / 200 |
| release→redelivery resolved (no floor) | 0 / 29 | 0 / 29 | 0 / 29 | 0 / 29 |
| receive *n*→*n*+1 resolved | 29 / 29 | 29 / 29 | 29 / 29 | 29 / 29 |

By source on real SQS: send→receive is SQS timestamps 200 (all alone) plus 28 (Python) or 29 (Node.js)
redelivery receives ordered after the send through a chain, and clock 1 in Python; receive→delete is
clock 200 and process order 172 in Python, process order 172 (111 alone) and clock 89 (28 alone) in
Node.js; receive *n*→*n*+1 is receive count 29.

So on real SQS `SentTimestamp` and `ApproximateFirstReceiveTimestamp` were at least 1 ms apart for every
one of the 200 first receives in each language, where ElasticMQ tied 196 of 200. The only pairs left
concurrent are the 29 release→redelivery pairs, which have no floor.

### The floor in CI

CI runs the check twice per language:

- **`concurrency-check` (ElasticMQ).** Gates everything except the floor: 200 of 200 complete, in order,
  edges verified, joined, with the right receive counts and final state. It still reports the resolved
  shares per kind and source, and the floor's result, in the job summary. The floor doesn't gate there
  because ElasticMQ stamps `SentTimestamp` and `ApproximateFirstReceiveTimestamp` in the same millisecond,
  so send→receive stays at 4–9% there. That is the emulator's behaviour, not the store's. `check.sh` does
  the same on the emulator by default (`--floor-report-only`), locally and in CI, so a run without AWS can
  pass; `QV2_CHECK_FLOOR=0.9` makes it gate there too.
- **`concurrency-check-aws` (real SQS, `us-east-1`).** Runs `check.sh --aws` with the 90% floor gating, as
  well as everything above. It needs the repository secrets `QV2_AWS_ACCESS_KEY_ID` and
  `QV2_AWS_SECRET_ACCESS_KEY`. It fails, rather than skips, when they are missing. Fork pull requests get no
  secrets, so it runs on pushes and same-repo pull requests only. One run at a time (`concurrency: real-sqs`);
  about 2,000 SQS requests per language (estimated).

### Earlier result (contract v2, clock intervals only): 200 of 200, with most send/receive pairs left concurrent

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
