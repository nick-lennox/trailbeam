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

The Lambda handler wrapper (and with it `TRAILBEAM_CAPTURE_ERROR_QUEUES`) comes in a later stage of QV-319.

Spans are exported in the background and flushed when the JVM shuts down. A program that has to be sure they
have left before it continues can call `dev.trailbeam.sqs.Trailbeam.flush(timeout, unit)`.

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
