# Conformance kit

A language-neutral check that a Trailbeam package emits the spans of the
[span contract](../contract/span-contract.md). A small **driver** in the package's language runs one
scripted [scenario](SCENARIO.md) against a local SQS emulator and writes down what SQS did (the
**ground truth**). The kit collects the spans the package exported and compares the two: one span per
message, the required attributes and their types, the receipt-handle HMAC, call-order ticks and intervals,
and no receipt handle or key in any attribute. A failing package gets a report that says, call by call,
what is missing or wrong.

The Python and Node.js packages, client hooks and Lambda wrappers, run it in CI (`Conformance kit` in
[`ci.yml`](../.github/workflows/ci.yml)), and so does the Java package (`Java package` in the same file, driver in
[`drivers/java`](drivers/java), with agent variants for the OpenTelemetry Java agent and dd-java-agent; its
Lambda wrapper in [`drivers/java-lambda`](drivers/java-lambda)), and so does the .NET package (`.NET package`,
driver in [`drivers/dotnet`](drivers/dotnet), with variants beside OpenTelemetry.Instrumentation.AWS). New
language packages add a driver and must pass it.

```
python conformance/kit.py run conformance/drivers/python/driver.json conformance/drivers/node/driver.json \
    conformance/drivers/python-lambda/driver.json conformance/drivers/node-lambda/driver.json
```

Needs Docker, Python 3.9+ (standard library only for the kit itself), and whatever the drivers need: for
these, a Python with `pip install -e ./python boto3` first on `PATH` (the Lambda poller runs on the kit's own
Python and needs boto3), and `npm ci` in `node/`. Each variant takes about 10 to 15 s, most of it the
scenarios' visibility-timeout waits; the Lambda drivers first build their function image once (about 15 s
when the base image is cached).

The Lambda wrappers have a scenario of their own, played against the Lambda base image's Runtime Interface
Emulator: [SCENARIO-LAMBDA.md](SCENARIO-LAMBDA.md).

## How it works

```
 driver (any language)          bench (containers, random local ports)
 ┌──────────────────────┐       ┌───────────────────────────────┐
 │ SDK + Trailbeam pkg  │──SQS─▶│ ElasticMQ 1.6.15              │
 │                      │──OTLP▶│ OTel Collector 0.165.0 ─▶ file│──▶ spans.jsonl ─┐
 │ writes ground truth  │       └───────────────────────────────┘                  ├─▶ checker ─▶ report
 └──────────┬───────────┘                                                          │
            └────────────────────────────── truth.jsonl ───────────────────────────┘
```

1. `kit.py run` starts ElasticMQ and a Collector whose only exporter is a file
   ([`bench/collector.yaml`](bench/collector.yaml)). Both get random ports on 127.0.0.1, container names
   of their own and a Docker network of their own (as `sqs` and `collector`), so several runs can share a
   machine. The Collector stamps each span with `kit.collector.received_unix_nano` (when it got it), and
   changes nothing else.
2. It runs each driver's `setup` once, if it has one (the Lambda drivers build their function image). For
   each **variant** of each driver it runs the driver once with a fresh run name and a fresh random
   receipt-hash key, then waits until the run's spans have reached the file.
3. [`checker.py`](checker.py) builds the spans the contract requires from the ground truth, recomputes
   the HMACs, matches them to the exported spans with scope `trailbeam` and `service.name` = the run name,
   and prints the report. `kit.py run` exits 1 if any run fails.

The checker has no per-language code. Everything a language differs in lives in its driver.

### What is checked

| Contract | Check |
|---|---|
| 1, 4 | every expected span is there, once: one per message sent, per message received, per handle settled, per purge; nothing else (a second span for one message is a duplicate) |
| 1, 3 | span name, kind INTERNAL, scope `trailbeam` with a version, `trailbeam.schema_version = 3`, `trailbeam.kind`, `trailbeam.source = client`, `messaging.*`, `aws.sqs.queue.url` verbatim, `process.pid`, `messaging.client.id` = resource `service.instance.id` |
| 3 | types (an int sent as a string fails), no `""` or `-1` for an absent value, no `trailbeam.*` attribute the contract does not define (a misspelt name would be silently ignored by Trailbeam) |
| 4.1 to 4.3 | message ID, batch entry ID, receive count, sent timestamp (should), first-receive timestamp (if sent), DLQ source ARN, visibility timeout (0 included), `trailbeam.result` and `trailbeam.error_code` per handle or entry, message ID on a settle only for handles this process received |
| 4.2 | SQS's response has `ApproximateReceiveCount`, `SentTimestamp`, `ApproximateFirstReceiveTimestamp` (and `DeadLetterQueueSourceArn` on the DLQ) although the driver didn't ask: the package added them; and the `SenderId` the driver did ask for is still there |
| 7 | `trailbeam.receipt_handle.hmac` and `.key_id` recomputed from the raw handle and the run's key; no raw handle, and not the key, in any attribute |
| 2, 2a | every span of one call has the same interval and ticks; `issued < returned`; for consecutive calls `returned(n) < issued(n + 1)` and `end(n) ≤ start(n + 1)`; one `trailbeam.order.scope` per process |
| 15 | `messaging.message.body.size` and `trailbeam.body.capture_status = off`; with capture on, `captured` and the body |
| 18 | one `trailbeam purge` with `trailbeam.result`, no message ID or HMAC |
| scenario | the truth has every operation, a redelivery, a DLQ receive, a failed batch entry and a failed call, so a driver that skipped a step can't pass by checking less |

For the Lambda wrappers (a truth with `"source": "lambda"`), the checker uses
[SCENARIO-LAMBDA.md](SCENARIO-LAMBDA.md)'s rules instead:

| Contract | Check |
|---|---|
| 1, 4.2, 4.4 | one `trailbeam receive` and one `trailbeam outcome` per record of each invocation, nothing else |
| 3 | `trailbeam.source = lambda`, `messaging.operation.name = LambdaEvent`, types, no unknown `trailbeam.*` |
| 2, 3 | receive: `trailbeam.start_observed = false` and zero-length; outcome: zero-length, no `start_observed`; both inside the invocation (after the poller's POST, around the handler's start, before the response) |
| 2a | receive: `trailbeam.order.returned`, no `issued`; outcome: `issued = returned`, after the receive's tick; one moment and tick per invocation and span name; invocations in order |
| 4.4 | `trailbeam.outcome` derived from the function's response (`success`, `failed_reported`, `failed_invalid_response`, `failed_exception`), `trailbeam.error_type` = the error type the runtime reported |
| 4.2, 6 | `faas.name`, `faas.invocation_id` (the request ID the poller gave), `trailbeam.queue.arn` = `eventSourceARN`, `aws.sqs.queue.url` built from it, receive count, HMAC, sent timestamp, DLQ source ARN |
| 15.2 | `trailbeam.batch.size`, `trailbeam.batch.index`, `trailbeam.batch.reported_failure_count` (omitted after an invalid list or a throw) |
| 4.2 | flushed before the handler ran: the Collector received every receive span before the handler's first line |
| 5, 7, 15 | `messaging.client.id` = the log stream name; no raw handle or key; body and error text only when turned on |
| scenario | the truth has all four outcomes, a batch of two, a reported failure count above 0, a redelivery and a DLQ record; the poller deleted exactly what Lambda would |

A "should" of the contract that is missing (for example `trailbeam.sent_timestamp_ms`, or the message ID
on a settle span) is a warning, shown with `~`; everything else fails the run.

Not checked in this version: tenant and FIFO attributes, redaction (the contract's shared vectors cover it),
`thread.name`; for the Lambda wrappers, a timeout or out-of-memory kill (the RIE can't kill an invocation
the way Lambda does) and the wrapper beside the stock OpenTelemetry Lambda instrumentation (QV-301 ran it).

## Variants: the agent matrix

A variant is one run of the same driver with other settings. Each driver's `driver.json` lists its
variants; `--variant NAME` runs only some. The Python and Node.js drivers have four:

| Variant | Settings | Extra check (`parent`) |
|---|---|---|
| `alone` | only Trailbeam | none |
| `app-otel` | the application's own tracer provider and the stock SDK instrumentation, set up first | `exported`: every Trailbeam span is a child of an exported span in its trace (the stock span) |
| `app-otel-sampled-0` | as `app-otel`, with `OTEL_TRACES_SAMPLER=traceidratio`, `OTEL_TRACES_SAMPLER_ARG=0` | `unexported`: every Trailbeam span has a parent, all 20 are exported, and no span of another scope was: the always-on provider of contract section 2, checked rather than assumed |
| `capture` | `TRAILBEAM_CAPTURE_BODY_QUEUES=*` | `capture: true`: every send and receive carries `trailbeam.body` |

Agents are variants too. A Java driver adds, for example:

```json
{"name": "otel-agent", "parent": "exported", "receive_roots": true,
 "env": {"JAVA_TOOL_OPTIONS": "-javaagent:{env:OTEL_JAVA_AGENT_JAR}"}},
{"name": "otel-agent-sampled-0", "parent": "unexported", "receive_roots": true,
 "env": {"JAVA_TOOL_OPTIONS": "-javaagent:{env:OTEL_JAVA_AGENT_JAR}",
         "OTEL_TRACES_SAMPLER": "traceidratio", "OTEL_TRACES_SAMPLER_ARG": "0"}},
{"name": "dd-agent", "parent": "unexported", "receive_roots": true,
 "env": {"JAVA_TOOL_OPTIONS": "-javaagent:{env:DD_JAVA_AGENT_JAR}"}}
```

`receive_roots: true` lets `ReceiveMessage` spans have no parent. The Java agents make no span for a receive
unless a span is already active, so a receive in a poll loop has nothing to be a child of. Every other span is
still checked, and so is a receive that has a parent. dd-agent is `unexported`: with no Datadog agent running
its spans go nowhere, but the trace header it puts on each request still gives every Trailbeam span a parent.

## Driver configuration (`driver.json`)

| Key | Meaning |
|---|---|
| `name` | shown in reports and part of the run name |
| `command` | the driver's command line, a list |
| `cwd` | working directory (default: the repository root) |
| `env` | variables for every variant |
| `setup` | optional: a command run once before the variants (the Lambda drivers build their image); a failure fails every variant of the driver, with the end of its log |
| `variants` | list of `{name, env, prefix, parent, receive_roots, capture, about}`: `prefix` is put before `command` (a wrapper such as `opentelemetry-instrument`); `parent` is `any`, `exported` or `unexported` (above); `receive_roots: true` lets receives be roots (above); `capture: true` expects captured bodies |

Strings may use `{repo}`, `{kit}`, `{driver_dir}` and `{env:NAME}` (the kit's own environment; a missing
variable is an error, so a CI job can't skip an agent by accident).

### What the kit sets

`KIT_RUN`, `KIT_SQS_ENDPOINT`, `KIT_TRUTH_FILE`, `OTEL_SERVICE_NAME` (= the run name),
`TRAILBEAM_OTLP_TRACES_ENDPOINT` and `OTEL_EXPORTER_OTLP_ENDPOINT` (the Collector),
`TRAILBEAM_RECEIPT_HASH_KEY` (random per run), `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` = `x`,
`AWS_REGION` = `elasticmq`. For drivers that run code in a container: `KIT_DOCKER_NETWORK` (the bench's
network), `KIT_OTLP_ENDPOINT_IN_NETWORK` and `KIT_SQS_ENDPOINT_IN_NETWORK` (the bench as seen from it),
`KIT_BENCH_TAG` (prefix the images you build with `<tag>-`, and the kit removes them at the end) and
`KIT_WORKDIR`. `setup` gets `KIT_BENCH_TAG`, `KIT_WORKDIR` and `KIT_DOCKER_NETWORK`. Every `TRAILBEAM_*`, `OTEL_*`, `AWS_*` and `KIT_*` variable of the caller's
environment is removed first; the variant's `env` comes last.

## Adding a language

1. Write a driver that runs [SCENARIO.md](SCENARIO.md) and writes the ground truth. Start from the Python
   or Node.js driver; on the JVM, from [`drivers/java`](drivers/java) (AWS SDK for Java v2).
2. Add a `driver.json` with its variants, including the agents the package must work beside.
3. Run `python conformance/kit.py run path/to/driver.json` until it passes, then add it to the CI job.

## Reading a failure

```
node / app-otel (run kit-fixture-node-app-otel): FAIL, 19 of 20 expected spans found, 20 Trailbeam spans exported (6 problems)
  call 4 DeleteMessage on kit-fixture-node-app-otel, message 22a9b3af-a2bb-4471-b0ea-a65471b4920c
    - trailbeam.receipt_handle.hmac: expected ebd1c1f3dc151ebaf541af5c9cc9b1cf, got 00000000000000000000000000000000  [contract 4]
  call 5 DeleteMessageBatch on kit-fixture-node-app-otel, entry bogus, a handle this process did not receive
    - trailbeam.result: expected failed, got ok  [contract 4]
    - trailbeam.error_code: missing (expected ReceiptHandleIsInvalid)  [contract 4]
  call 6 ReceiveMessage on kit-fixture-node-app-otel, message cfed04fd-ad69-442d-8295-6723a080aa59, receive count 2
    - trailbeam.receive_count: expected int 2, got string 2  [contract 4]
  call 17 PurgeQueue on kit-fixture-node-app-otel-purge, the purge
    - missing span: no 'trailbeam purge' was exported  [contract 1, 4]
  span trailbeam send op=SendMessage message=22a9b3af-a2bb-4471-b0ea-a65471b4920c
    - duplicate span: a second span for the same message/handle in one call (one span per message)  [contract 2]
  (- must: fails the run; ~ should: a warning)
```

Problems are grouped by the call in the ground truth (numbered as in the truth file), then spans no call
explains. A span with a wrong HMAC or message ID is still paired with its call (by operation, message ID
of a receive, batch entry ID), so the report shows the expected and the exported value side by side; when
every hash was made with another key, one line at the top says so. `[contract N]` is the section of the
span contract. The work directory (printed at the end, or `--workdir`) keeps each run's ground truth,
driver log, `<run>.run.json` (run name, key, variant), the Collector's `spans.jsonl` and `report.json`. To
check again without rerunning:

```
python conformance/kit.py check --truth W/<run>.truth.jsonl --spans W/collector/spans.jsonl --settings W/<run>.run.json
```

## Receipt handles

The ground-truth files hold raw receipt handles from the bench's ElasticMQ, so that the kit can recompute
each HMAC. They are local to the bench, worthless outside it, and the kit's CI job does not upload them.
The checker fails a run that exports any handle SQS returned. The test fixtures
([`tests/fixtures`](tests/fixtures)) are recorded runs with every handle replaced by a synthetic one and
every HMAC recomputed with a fixed key ([`tests/make_fixture.py`](tests/make_fixture.py)).

## Tests

`pytest conformance/tests`: the four recorded runs (client hooks and Lambda wrappers, Python and Node.js)
pass, and each of about 27 single-rule breakages of the client-hook runs and 40 of the Lambda runs fails with
the line that names it.

A broken Lambda wrapper reads like this (the Node.js recording, with five rules broken):

```
node-lambda / alone (run kit-fixture-node-lambda-alone): FAIL, 14 of 14 expected spans found, 14 Trailbeam spans exported (7 problems)
  call 2 invocation (report) on kit-fixture-node-lambda-alone, receive of record 0, message 866ac766-fad8-4a57-91ef-ea7549211f69
    - times: a zero-length span is required (its start is not observed, and the wrapper must not put any other time in it); got start 1795468038078000000, end 1795468038081000000, 3.000 ms apart  [contract 2]
    - not flushed before the handler ran: the Collector received this receive span 5000.0 ms after the handler started (a timeout or out-of-memory kill would lose it)  [contract 4.2]
  ...
  call 4 invocation (dlq) on kit-fixture-node-lambda-alone-dlq, receive of record 0, message 866ac766-fad8-4a57-91ef-ea7549211f69
    - trailbeam.queue.arn: missing (expected arn:aws:sqs:elasticmq:000000000000:kit-fixture-node-lambda-alone-dlq)  [contract 4.2, 6]
  call 5 invocation (invalid) on kit-fixture-node-lambda-alone, outcome of record 0, message 549c11e6-9511-4fec-bb0d-f7bb3ca69aeb, failed_invalid_response
    - trailbeam.outcome: expected failed_invalid_response, got failed_reported  [contract 4.4]
  call 6 invocation (throw) on kit-fixture-node-lambda-alone, outcome of record 0, message 3865febc-de50-451c-948c-c8ac8022d4a1, failed_exception
    - trailbeam.error_type: missing (expected Error)  [contract 4.4]
```

The work directory also keeps each Lambda run's function log (`<run>.function.log`: the RIE's and the
handler's output) and each driver's `<driver>.setup.log`.
