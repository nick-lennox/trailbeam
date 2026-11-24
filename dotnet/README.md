# Trailbeam for the AWS SDK for .NET

**Pre-release, not published yet.** This package is being built in stages under QV-320. The NuGet package ID
`Trailbeam.Sqs` is a placeholder until the board decides how Trailbeam publishes to NuGet.

Add one line at startup, before the first SQS client is created, and every SQS call made through the AWS SDK for
.NET v4 emits the spans of the [Trailbeam span contract](../contract/span-contract.md):

```csharp
Trailbeam.Sqs.TrailbeamSqs.Register();
```

`Register` adds a pipeline handler to every `AmazonSQSClient` created after it, through the SDK's
`RuntimePipelineCustomizerRegistry`, which is the hook OpenTelemetry.Instrumentation.AWS uses too. A client
created before the call isn't traced. In an ASP.NET Core or worker app, put it at the top of `Program.cs`, before
`builder.Services.AddAWSService<IAmazonSQS>()` resolves a client. Calling it again does nothing.

It works with no other tracing, and beside OpenTelemetry.Instrumentation.AWS 1.21.0, in either registration order
and when the application's sampler drops everything. The [conformance kit](../conformance) checks all of these in
CI. Trailbeam has its own always-on tracer provider and exporter, so the application's sampler, exporter and
`OTEL_EXPORTER_OTLP_*` settings (its headers, for one) never apply to Trailbeam's spans. It doesn't change or
suppress the stock spans.

Requires the AWS SDK for .NET v4 (`AWSSDK.SQS` 4.0.2.5 or later) on .NET 8 or later. It is also built for .NET
Standard 2.0 (.NET Framework 4.7.2 and later), which is not tested yet. It depends on `OpenTelemetry.Exporter.OpenTelemetryProtocol`
1.15.3 or later. .NET has no relocation, so if your application uses OpenTelemetry too, both share one copy of
its assemblies, at the higher of the two versions. CI runs the kit on .NET 10 with `AWSSDK.SQS` 4.1 and
OpenTelemetry 1.21. The version matrix is a later stage of QV-320.

## What it records

| SQS call | Spans |
|---|---|
| `SendMessage`, `SendMessageBatch` | one `trailbeam send` per message SQS accepted, with body size, tenant and FIFO fields, and the redacted body if you turned capture on |
| `ReceiveMessage` | one `trailbeam receive` per message, with the keyed receipt-handle hash, receive count, timestamps and dead-letter source ARN |
| `DeleteMessage`, `DeleteMessageBatch` | one `trailbeam delete` per receipt handle, with its result and error code |
| `ChangeMessageVisibility`, `ChangeMessageVisibilityBatch` | one `trailbeam change_visibility` per receipt handle |
| `PurgeQueue` | one `trailbeam purge` |

To read the receive count and the other system attributes, the handler adds their names to each
`ReceiveMessage` request before the SDK marshals it. It also asks for the tenant message attribute (if you
configured one) and the SQS Extended Client's size attributes. It keeps every name you asked for, and leaves `All`
(and `.*`) alone. If you use only the deprecated `AttributeNames`, it adds them there. Your request object is put
back as it was once the call returns. That changes what SQS returns, not the message. It never makes an SQS call
of its own.

Each span's parent is the span active when the call was made. With OpenTelemetry.Instrumentation.AWS, that is
the SDK's own span for the call (the SDK v4 starts it in its outermost handler), so every Trailbeam span,
receives included, is a child of the stock span. Without an active span, Trailbeam falls back to an
`X-Amzn-Trace-Id` or `traceparent` header that another handler (the AWS X-Ray SDK, say) put on the request.
Otherwise the span is a root. Trailbeam reads only its own spans, so its timeline is the same either way.

## Configuration

Environment variables, as for the Python, Node.js and Java packages:

| Variable | Default |
|---|---|
| `TRAILBEAM_OTLP_TRACES_ENDPOINT` | `http://localhost:4318/v1/traces` (OTLP over HTTP, protobuf) |
| `TRAILBEAM_RECEIPT_HASH_KEY` | the published default key, with one warning. Use the same key on every service that uses the same queues (contract section 7). |
| `OTEL_SERVICE_NAME`, `OTEL_RESOURCE_ATTRIBUTES` | `unknown_service`, and a random `service.instance.id` |
| `OTEL_RESOURCE_ATTRIBUTES=service.version=1.4.2` | no version. With it, Trailbeam shows each consumer's version on the timeline and on the lists, which can be filtered by `version`. |
| `TRAILBEAM_MAX_QUEUE_SIZE` | 16384 spans waiting for export |

### Tenants and message bodies

These follow contract section 15, the same rules as the other packages, tested against the same vectors. Every
send and receive span carries the body's size. The rest is off until you turn it on:

| Variable | Default |
|---|---|
| `TRAILBEAM_TENANT_ATTRIBUTE` | none. A message attribute holding the tenant (`trailbeam.tenant`), tried first. |
| `TRAILBEAM_TENANT_BODY_PATH` | none. A dotted path into a JSON body (`customer.id`). The value must be a string or a number. Reading it doesn't export the body. |
| `TRAILBEAM_CAPTURE_BODY_QUEUES` | none: no body leaves the process. Comma-separated queue names, or `*`. |
| `TRAILBEAM_REDACT_KEYS` | added to the default keys (`password`, `secret`, `token`, `authorization`, `api_key`, `ssn`, `card_number`, `cvv` and others). A key with a `.` matches that path only. |
| `TRAILBEAM_REDACT_PATTERNS` | `aws_access_key,bearer,email,long_number`. Set it to an empty value for none. |
| `TRAILBEAM_CAPTURE_MAX_BYTES` | 16384, at most 262144. A longer redacted body is not exported, and not cut. |

A captured body is redacted inside your process before it is exported. The span's
`trailbeam.body.capture_status` says what happened (`captured`, `off`, `md5_mismatch`, `redaction_failed`,
`exceeds_capture_limit` or `offloaded_to_s3`). Redaction is pattern matching, so turn capture on only for queues
whose contents you are willing to store, and check what you get. Trailbeam's server stores bodies only for the
queues in its own `TRAILBEAM_STORE_BODIES`, so you need both.

`Register` never throws into your SQS calls. A bad redact key, an unknown pattern or an out-of-range limit gets
one warning on standard error, and no body is exported at all: a listed queue's spans say `redaction_failed`
until the setting is fixed (as in the Java package).

Spans are exported in the background and flushed when the process exits. A program that has to be sure they
have left before it continues can call `TrailbeamSqs.Flush(TimeSpan)`. Diagnostics go to the EventSource
`Trailbeam-Sqs` (`dotnet-trace`, `dotnet-monitor` or an `EventListener`).

## AWS Lambda

Not yet: the wrapper for `Amazon.Lambda.SQSEvents` is the next stage of QV-320.

## Building

```
dotnet test dotnet/test/Trailbeam.Sqs.Tests                    # unit tests (contract vectors, a real client over a stub HTTP layer)
dotnet build -c Release conformance/drivers/dotnet             # the conformance driver
python conformance/kit.py run conformance/drivers/dotnet/driver.json
```
