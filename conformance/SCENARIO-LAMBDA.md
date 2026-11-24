# The Lambda scenario and its ground truth

The Lambda wrappers (`trailbeam.aws_lambda.wrap` in Python, `wrapHandler` in Node.js,
`TrailbeamSqsLambdaWrapper` in Java) don't call SQS: Lambda's
event source mapping receives the messages, invokes the function with a batch of records, and deletes the
ones that succeeded. So a Lambda driver has two parts:

- a **function image**: the AWS Lambda base image (`public.ecr.aws/lambda/python:3.12`,
  `public.ecr.aws/lambda/nodejs:22`), which has the Runtime Interface Emulator (RIE), with the package from
  this checkout installed and a handler wrapped the way the package's README says
  ([`drivers/python-lambda`](drivers/python-lambda), [`drivers/node-lambda`](drivers/node-lambda),
  [`drivers/java-lambda`](drivers/java-lambda), on `public.ecr.aws/lambda/java:21`, whose Dockerfile builds the
  jar from `java/` first);
- the **poller** ([`drivers/lambda-poller/poller.py`](drivers/lambda-poller/poller.py)), shared by every
  language, which plays the event source mapping against the RIE and writes the ground truth. It started
  from QV-301's `drivers/lambda-poller/poller.py`. Like Lambda's own poller it has no Trailbeam hooks.

```
 poller (host, boto3)                                  bench network
 ┌───────────────────────────────┐  ReceiveMessage    ┌──────────────┐
 │ plays the event source mapping│◀──────────────────▶│ ElasticMQ    │
 │                               │  DeleteMessageBatch└──────────────┘
 │                               │  POST event        ┌──────────────────────────┐ OTLP ┌───────────┐
 │                               │───────────────────▶│ function: RIE + wrapper  │─────▶│ Collector │──▶ spans.jsonl
 │ writes ground truth           │◀───────────────────│ + handler (KIT_HANDLER)  │      └───────────┘
 └───────────────────────────────┘  response / error  └──────────────────────────┘
```

`kit.py` runs the driver's `setup` once (`poller.py build`: builds the image from `python/` or `node/` and the
driver's directory), then `poller.py run` once per variant. The function container joins the bench's
Docker network and exports to the Collector there; the poller reaches the RIE on a random local port.

## The function

Each record's body is `{"case": ..., "n": ...}`. The handler, the same in both languages:

1. prints one `KIT_HANDLER {...}` line first: `started_unix_nano` (its wall clock at its first line; in Node.js,
   where `Date.now()` has whole milliseconds and `performance.now()` drifts from the wall clock, counted back
   from the next `Date.now()` tick),
   `request_id` and `function_name` from its context, `log_stream` from `AWS_LAMBDA_LOG_STREAM_NAME`, and the
   batch's message IDs;
2. on the dead-letter queue (every record's `eventSourceARN` ends in `-dlq`), returns `{"batchItemFailures": []}`;
3. throws if any record is `throw`, with message `conformance kit: the handler threw (case throw)`
   (`RuntimeError` in Python, `Error` in Node.js);
4. returns `{"batchItemFailures": [{"itemIdentifier": "not-a-record-of-this-batch"}]}` if any record is `invalid`;
5. otherwise lists every `report` record in `batchItemFailures`; a lone `success` record gets a plain return
   (`None` / `undefined`), so "no list at all" is covered too.

## Steps

The poller creates `R-dlq` and `R` (`VisibilityTimeout = 2`, `RedrivePolicy` with `maxReceiveCount = 2`), starts
the function with `AWS_LAMBDA_FUNCTION_NAME = R-fn` and a log stream name of Lambda's form, and runs:

| Case | What happens | Outcomes |
|---|---|---|
| `success` | send one `success`; invoke with it; delete it | `success`, reported failure count 0 |
| `report` | send `report` and `success`; one invocation with both; delete the `success` one | `failed_reported`, `success`, count 1 |
| `report-retry` | after the visibility timeout, `report` again (receive count 2) | `failed_reported` |
| `dlq` | after the timeout, one receive on `R`: ElasticMQ moves `report` to `R-dlq` (no invocation); invoke with it from `R-dlq`, with `DeadLetterQueueSourceArn`; delete it there | `success` |
| `invalid` | send `invalid`; invoke; delete nothing | `failed_invalid_response`, no count |
| `throw` | send `throw`; invoke; the RIE answers `{"errorType", "errorMessage"}`; delete nothing | `failed_exception`, `error_type`, no count |

After `invalid` and `throw` the poller takes the message out of the queue itself (not Lambda behaviour, so
the next case starts with an empty queue; it makes no span). That is 6 invocations and 7 records: the
checker expects one `trailbeam receive` and one `trailbeam outcome` per record, 14 spans.

The event is the shape of AWS's sample events: `messageId`, `receiptHandle`, `body`, `attributes` (every
system attribute SQS returned), `messageAttributes`, `md5OfBody`, `eventSource = aws:sqs`, `eventSourceARN`
(the queue's `QueueArn`), `awsRegion`. Whether real Lambda records carry `DeadLetterQueueSourceArn` is not
verified (contract 4.2); the poller passes it on so the wrapper's copy of it is checked.

The poller sends `X-Amzn-RequestId` with each invocation: the RIE uses it as the request ID, so the truth
knows each `faas.invocation_id` without asking the package. Lambda's deletes follow its rules with
`ReportBatchItemFailures` on: a function error deletes nothing, a list with an `itemIdentifier` that is not
in the batch (or is empty) fails the whole batch, otherwise every record not listed is deleted. The checker
re-derives the outcomes from the responses and fails the run (`[kit]`) if the poller's deletes disagree.

## Ground-truth file

The header has `"source": "lambda"` (that is what makes the checker use the Lambda rules), `driver`,
`runtime`, `package` (versions in the image), `function_name` and `log_stream`. Then one line per invocation:

| Field | Value |
|---|---|
| `op` | `LambdaInvoke` |
| `call`, `case` | invocation number, the case above |
| `queue_url`, `event_source_arn` | the queue the poller received from, and its ARN as put in the records |
| `request_id` | the request ID the poller gave the RIE |
| `invoke_sent_unix_nano`, `invoke_returned_unix_nano` | the poller's wall clock around the POST |
| `records` | `[{message_id, receipt_handle, body, attributes}]`, in event order; handles raw (README.md, "Receipt handles") |
| `response` | the function's response, or `null` when it threw |
| `function_error` | `{type, message}` from the RIE when the handler threw, else `null` |
| `deleted` | the message IDs the poller deleted after the invocation |
| `handler` | the handler's `KIT_HANDLER` line: `started_unix_nano`, `request_id`, `function_name`, `log_stream` |

## What the checker expects

Per record, a `trailbeam receive` and a `trailbeam outcome`, matched by span name, `faas.invocation_id` and
`messaging.message.id` (then by name and message ID, so a wrong invocation ID is shown as a wrong value, not
as a missing span). On both:

- `trailbeam.source = lambda`, `messaging.operation.name = LambdaEvent`, schema version 3, scope `trailbeam`
  with a version, kind INTERNAL;
- `messaging.destination.name` and `aws.sqs.queue.url` built from `eventSourceARN`
  (`https://sqs.<region>.amazonaws.com/<account>/<name>`, contract 6), not the URL the poller used;
- `faas.name` (the function name the runtime gave the handler), `faas.invocation_id` (the request ID);
- `trailbeam.batch.size`, `trailbeam.batch.index` (position in the event);
- `messaging.client.id` = the log stream name (contract 5), `process.pid`, one `trailbeam.order.scope`.

The receive also: `trailbeam.queue.arn` = `eventSourceARN`, message ID, receive count, receipt-handle HMAC and
key ID, sent timestamp (should), DLQ source ARN when the record has it, body size and capture status;
`trailbeam.start_observed = false` (a bool); **zero-length** (start = end: its start is not observed and no
other time may be put there); `trailbeam.order.returned` and **no** `trailbeam.order.issued`; its time between
the poller's POST and the handler's start; and **flushed before the handler ran**: the bench's Collector
stamps every span with `kit.collector.received_unix_nano` when it receives it, and the receive's stamp must
be before the handler's `started_unix_nano`, give or take 1 ms. The Collector stamps a span before it answers
the export, and a wrapper that waits for that answer starts the handler after it, so a correct wrapper passes
on any machine (the function, the Collector and the poller read one wall clock: they share the host's kernel).
The 1 ms is for the two processes' readings of that clock, which can tie or cross by tens of microseconds; a
wrapper that doesn't flush is late by its batch delay or the handler's run time, seconds rather than a
millisecond.

The outcome also: `trailbeam.outcome`, derived from the response as above; `trailbeam.error_type` = the RIE's
`errorType` on `failed_exception`; `trailbeam.batch.reported_failure_count` when the handler returned a valid
list or none, omitted after an invalid list or a throw; `trailbeam.error_message` only with error text on (the
`capture` variant), else absent; zero-length; `issued = returned`, one tick, after the receive's tick; its
time between the handler's start and the poller having the response.

Across the run: every receive of one invocation has the same time and tick, likewise every outcome; and the
invocations, made one after the other, are ordered by their ticks. No raw handle or key in any attribute.
The scenario check fails a run whose truth lacks any of the four outcomes, a batch of two, a reported failure
count above 0, a redelivery or a record from the DLQ.
