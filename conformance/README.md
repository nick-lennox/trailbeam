# Conformance kit

A language-neutral check that a Trailbeam package emits the spans of the
[span contract](../contract/span-contract.md). A small **driver** in the package's language runs one
scripted [scenario](SCENARIO.md) against a local SQS emulator and writes down what SQS did (the
**ground truth**). The kit collects the spans the package exported and compares the two: one span per
message, the required attributes and their types, the receipt-handle HMAC, call-order ticks and intervals,
and no receipt handle or key in any attribute. A failing package gets a report that says, call by call,
what is missing or wrong.

The Python and Node.js packages run it in CI (`Conformance kit` in [`ci.yml`](../.github/workflows/ci.yml)),
and so does the Java package (`Java package` in the same file, driver in [`drivers/java`](drivers/java), with
agent variants for the OpenTelemetry Java agent and dd-java-agent). New language packages add a driver and must
pass it.

```
python conformance/kit.py run conformance/drivers/python/driver.json conformance/drivers/node/driver.json
```

Needs Docker, Python 3.9+ (standard library only for the kit itself), and whatever the drivers need: for
these two, a Python with `pip install -e ./python boto3` first on `PATH`, and `npm ci` in `node/`. Each
variant takes about 10 s, most of it the scenario's visibility-timeout waits.

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
   ([`bench/collector.yaml`](bench/collector.yaml)). Both get random ports on 127.0.0.1 and container names
   of their own, so several runs can share a machine.
2. For each **variant** of each driver it runs the driver once with a fresh run name and a fresh random
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

A "should" of the contract that is missing (for example `trailbeam.sent_timestamp_ms`, or the message ID
on a settle span) is a warning, shown with `~`; everything else fails the run.

Not checked in this version: the Lambda wrapper (QV-318 stage 2, with the Runtime Interface Emulator),
tenant and FIFO attributes, redaction (the contract's shared vectors cover it), `thread.name`.

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
{"name": "otel-agent", "parent": "exported",
 "env": {"JAVA_TOOL_OPTIONS": "-javaagent:{env:OTEL_JAVA_AGENT_JAR}"}},
{"name": "otel-agent-sampled-0", "parent": "unexported",
 "env": {"JAVA_TOOL_OPTIONS": "-javaagent:{env:OTEL_JAVA_AGENT_JAR}",
         "OTEL_TRACES_SAMPLER": "traceidratio", "OTEL_TRACES_SAMPLER_ARG": "0"}},
{"name": "dd-agent", "parent": "any",
 "env": {"JAVA_TOOL_OPTIONS": "-javaagent:{env:DD_JAVA_AGENT_JAR}"}}
```

## Driver configuration (`driver.json`)

| Key | Meaning |
|---|---|
| `name` | shown in reports and part of the run name |
| `command` | the driver's command line, a list |
| `cwd` | working directory (default: the repository root) |
| `env` | variables for every variant |
| `variants` | list of `{name, env, prefix, parent, capture, about}`: `prefix` is put before `command` (a wrapper such as `opentelemetry-instrument`); `parent` is `any`, `exported` or `unexported` (above); `capture: true` expects captured bodies |

Strings may use `{repo}`, `{kit}`, `{driver_dir}` and `{env:NAME}` (the kit's own environment; a missing
variable is an error, so a CI job can't skip an agent by accident).

### What the kit sets

`KIT_RUN`, `KIT_SQS_ENDPOINT`, `KIT_TRUTH_FILE`, `OTEL_SERVICE_NAME` (= the run name),
`TRAILBEAM_OTLP_TRACES_ENDPOINT` and `OTEL_EXPORTER_OTLP_ENDPOINT` (the Collector),
`TRAILBEAM_RECEIPT_HASH_KEY` (random per run), `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` = `x`,
`AWS_REGION` = `elasticmq`. Every `TRAILBEAM_*`, `OTEL_*`, `AWS_*` and `KIT_*` variable of the caller's
environment is removed first; the variant's `env` comes last.

## Adding a language

1. Write a driver that runs [SCENARIO.md](SCENARIO.md) and writes the ground truth. Start from the Python
   or Node.js driver; for Java, the QV-312 spike's driver (`qv312-spike-evidence.tgz` on QV-312) has the
   SDK v2 calls.
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

`pytest conformance/tests`: the two recorded runs pass, and each of about 27 single-rule breakages of them
fails with the line that names it.
