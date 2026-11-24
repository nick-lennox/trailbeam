# Trailbeam for the AWS SDK for Java v2

**Pre-release, not published yet.** This module is being built in stages under QV-319. The Maven coordinates
below are placeholders until the Maven Central namespace is chosen.

Add one dependency and every SQS call made through the AWS SDK for Java v2 emits the spans of the
[Trailbeam span contract](../contract/span-contract.md). You don't change any code or add an agent flag:

```xml
<dependency>
  <groupId>dev.trailbeam</groupId>
  <artifactId>trailbeam-aws-sdk-v2</artifactId>
  <version>0.1.0-SNAPSHOT</version>
</dependency>
```

The jar registers a global `ExecutionInterceptor` through the SDK's classpath file
`software/amazon/awssdk/global/handlers/execution.interceptors`, so the SDK loads it into every client: sync and
async clients, and frameworks built on them. The first SQS call logs `Trailbeam interceptor active` once, through
`System.Logger` (logger `trailbeam`). If you don't see that line, see [Uber jars](#uber-jars) below.

It works with no agent, beside the OpenTelemetry Java agent (also when the agent's sampler drops everything),
and beside dd-java-agent. The [conformance kit](../conformance) checks all four in CI. The jar has its own
always-on tracer provider with a relocated copy of the OpenTelemetry SDK (`dev.trailbeam.sqs.shaded.otel`), so it
never conflicts with your application's OpenTelemetry version. It doesn't change or suppress the agent's spans.

Requires Java 11 or later and AWS SDK for Java v2.

## What it records

| SQS call | Spans |
|---|---|
| `SendMessage`, `SendMessageBatch` | one `trailbeam send` per message SQS accepted, with body size, tenant and FIFO fields, and the redacted body if you turned capture on |
| `ReceiveMessage` | one `trailbeam receive` per message, with the keyed receipt-handle hash, receive count, timestamps and dead-letter source ARN |
| `DeleteMessage`, `DeleteMessageBatch` | one `trailbeam delete` per receipt handle, with its result and error code |
| `ChangeMessageVisibility`, `ChangeMessageVisibilityBatch` | one `trailbeam change_visibility` per receipt handle |
| `PurgeQueue` | one `trailbeam purge` |

To read the receive count and the other system attributes, the interceptor adds their names to each
`ReceiveMessage` request. It also asks for the tenant message attribute (if you configured one) and the SQS
Extended Client's size attributes. It keeps every name you asked for, and leaves `All` (and `.*`) alone. That
changes what SQS returns, not the message. It never makes an SQS call of its own.

Each span's parent is the stock SDK span for the same call, read from the trace header the agent puts on the
request. A `ReceiveMessage` in a polling loop has no stock span (the OpenTelemetry agent creates one only when a
span is already active), so its `trailbeam receive` is a root span. Trailbeam reads only its own spans, so its
timeline is the same either way.

## Configuration

Environment variables, as for the Python and Node.js packages:

| Variable | Default |
|---|---|
| `TRAILBEAM_OTLP_TRACES_ENDPOINT` | `http://localhost:4318/v1/traces` (OTLP over HTTP, protobuf) |
| `TRAILBEAM_RECEIPT_HASH_KEY` | the published default key, with one warning. Use the same key on every service that uses the same queues (contract section 7). |
| `OTEL_SERVICE_NAME`, `OTEL_RESOURCE_ATTRIBUTES` | `unknown_service`, and a random `service.instance.id` |
| `TRAILBEAM_MAX_QUEUE_SIZE` | 16384 spans waiting for export |

### Tenants and message bodies

These follow contract section 15, the same rules as the Python and Node.js packages, tested against the same
vectors. Every send and receive span carries the body's size. The rest is off until you turn it on:

| Variable | Default |
|---|---|
| `TRAILBEAM_TENANT_ATTRIBUTE` | none. A message attribute holding the tenant (`trailbeam.tenant`), tried first. |
| `TRAILBEAM_TENANT_BODY_PATH` | none. A dotted path into a JSON body (`customer.id`). The value must be a string or a number. Reading it doesn't export the body. |
| `TRAILBEAM_CAPTURE_BODY_QUEUES` | none: no body leaves the process. Comma-separated queue names, or `*`. |
| `TRAILBEAM_REDACT_KEYS` | added to the default keys (`password`, `secret`, `token`, `authorization`, `api_key`, `ssn`, `card_number`, `cvv` and others). A key with a `.` matches that path only. |
| `TRAILBEAM_REDACT_PATTERNS` | `aws_access_key,bearer,email,long_number`. Set it to an empty value for none. |
| `TRAILBEAM_CAPTURE_MAX_BYTES` | 16384, at most 262144. A longer redacted body is not exported, and not cut. |

A captured body is redacted inside your process before it is exported. In JSON, the values of the redact keys
are replaced by `"[REDACTED]"`, and strings are matched against the patterns. Text bodies get `key=value` pairs
and the patterns. The span's `trailbeam.body.capture_status` says what happened (`captured`, `off`,
`md5_mismatch`, `redaction_failed`, `exceeds_capture_limit` or `offloaded_to_s3`). Redaction is pattern
matching, so turn capture on only for queues whose contents you are willing to store, and check what you get.
Trailbeam's server stores bodies only for the queues in its own `TRAILBEAM_STORE_BODIES`, so you need both.

The Python and Node.js packages refuse a bad redact key, an unknown pattern or an out-of-range limit when they
are installed. This jar has no install step, and it never throws into your SQS calls, so it logs one warning
instead and exports no body at all: a listed queue's spans say `redaction_failed` until the setting is fixed.

For the Lambda wrapper, `TRAILBEAM_CAPTURE_ERROR_QUEUES` (comma-separated queue names, or `*`; none by default)
turns on the thrown error's message on each `trailbeam outcome`, after the text rules, cut to 1,024 characters
(contract section 15.5).

Spans are exported in the background and flushed when the JVM shuts down. A program that has to be sure they
have left before it continues can call `dev.trailbeam.sqs.Trailbeam.flush(timeout, unit)`.

## AWS Lambda

A function invoked by an SQS event source mapping doesn't call `ReceiveMessage` or `DeleteMessage` itself:
Lambda does. The interceptor can't see those calls, so the jar also has a handler wrapper. For each record it
emits one `trailbeam receive` and exports it before your handler runs, so a timeout or an out-of-memory kill
can't lose it. After your handler returns or throws, it emits one `trailbeam outcome` per record: `success`,
`failed_reported` (the record is in `batchItemFailures`), `failed_invalid_response` (the list names a record
that isn't in the batch, so Lambda fails the whole batch) or `failed_exception`, with the exception's class
name. What your handler returns, and what it throws, reach Lambda unchanged. A function that also makes SQS
calls of its own gets their spans from the interceptor as usual.

The function needs `com.amazonaws:aws-lambda-java-core` and `com.amazonaws:aws-lambda-java-events` 3.10.0 or
later (for `SQSBatchResponse`), which an SQS function has already. It doesn't need the AWS SDK.

**No code change.** Set the function's handler to the wrapper, and name your handler in an environment variable,
the way the OpenTelemetry Java Lambda wrappers work:

| Setting | Value |
|---|---|
| Handler | `dev.trailbeam.sqs.TrailbeamSqsLambdaWrapper::handleRequest` |
| `TRAILBEAM_LAMBDA_HANDLER` | your handler as Lambda would name it: `com.example.OrdersHandler` (a `RequestHandler<SQSEvent, SQSBatchResponse>`, or `RequestHandler<SQSEvent, Void>`), or `com.example.OrdersHandler::handle` for a public method taking `(SQSEvent, Context)` or `(SQSEvent)` |
| `TRAILBEAM_LAMBDA_REPORT_BATCH_ITEM_FAILURES` | `true` by default. Set it to `false` if the event source mapping doesn't have `ReportBatchItemFailures` turned on: Lambda then ignores `batchItemFailures`, and so does Trailbeam. |

Your handler may return an `SQSBatchResponse`, a `Map` of the same shape, or nothing. If
`TRAILBEAM_LAMBDA_HANDLER` is missing or names a class that can't be loaded, the function fails at init with a
message that says so.

**Or extend the base class:**

```java
public class OrdersHandler extends TrailbeamSqsHandler {
  @Override
  protected SQSBatchResponse handleSqsEvent(SQSEvent event, Context context) {
    ...
    return new SQSBatchResponse(failures); // or null when every record succeeded
  }
}
```

Override `reportBatchItemFailures()` to return `false` for a mapping without `ReportBatchItemFailures`.

The spans' `messaging.client.id` is the log stream name (`AWS_LAMBDA_LOG_STREAM_NAME`), which names one
execution environment. Their parent is the invocation's X-Ray trace header, when the runtime sets one; otherwise
they are root spans. Each flush waits at most 2 seconds and never fails the invocation.

**SnapStart.** One snapshot becomes many execution environments, so after a restore the jar starts a new
call-order scope and a new `service.instance.id`, and before the snapshot it flushes. It does this through the
`org.crac` API that SnapStart runs, when `io.github.crac:org-crac` is on the classpath (AWS's SnapStart guide
tells you to add it). Without it, call `dev.trailbeam.sqs.Trailbeam.afterRestore()` from your own restore hook.

## Registering it by hand

If the classpath file isn't there, register the interceptor on the client:

```java
SqsClient sqs = SqsClient.builder()
    .overrideConfiguration(c -> c.addExecutionInterceptor(new TrailbeamSqsExecutionInterceptor()))
    .build();
```

This is safe even when the global registration also works: each call still gets one span per message.

## Uber jars

Maven Shade and Gradle Shadow keep only one copy of a resource file by default. If another library on your
classpath ships its own `software/amazon/awssdk/global/handlers/execution.interceptors` (for example the AWS
X-Ray SDK or OpenTelemetry's `aws-sdk-2.2-autoconfigure`), one of the two files is lost unless you append them.
With Maven Shade:

```xml
<transformer implementation="org.apache.maven.plugins.shade.resource.AppendingTransformer">
  <resource>software/amazon/awssdk/global/handlers/execution.interceptors</resource>
</transformer>
```

With Gradle Shadow, use `append 'software/amazon/awssdk/global/handlers/execution.interceptors'`. A Spring Boot
executable jar keeps dependencies as nested jars, so it needs no extra setup.

## Building

```
mvn -f java/pom.xml install                      # unit tests, relocated jar
mvn -f conformance/drivers/java/pom.xml package  # the conformance driver, on the installed jar
OTEL_JAVA_AGENT_JAR=... DD_JAVA_AGENT_JAR=... python conformance/kit.py run conformance/drivers/java/driver.json
```
