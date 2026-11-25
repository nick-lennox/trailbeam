# Trailbeam for the AWS SDK for Go v2

**Pre-release, not published yet.** This module is being built in stages under QV-321. The module path below is
a placeholder until the first release is tagged.

One line where the AWS config is built, and every SQS call made through the AWS SDK for Go v2 emits the spans of
the [Trailbeam span contract](../contract/span-contract.md):

```go
import "github.com/nick-lennox/trailbeam/go/trailbeamsqs"

cfg, err := config.LoadDefaultConfig(ctx)
trailbeamsqs.AppendMiddlewares(&cfg.APIOptions)
client := sqs.NewFromConfig(cfg)

// before the program exits, so the last spans are exported:
defer trailbeamsqs.Shutdown(context.Background())
```

This is the same hook as otelaws (`otelaws.AppendMiddlewares(&cfg.APIOptions)`) and dd-trace-go
(`awstrace.AppendMiddleware(&cfg)`), and it works beside either, in any order. Clients of other services are left
alone. Adding it twice emits each span once. The first SQS call logs `trailbeam: middleware active` once, through
`log/slog`.

The package has its own always-on tracer provider (the OpenTelemetry Go SDK, exporting OTLP over HTTP), so your
application's sampler never drops a Trailbeam span. It doesn't change or suppress your own spans.

Requires Go 1.23 or later, aws-sdk-go-v2 `service/sqs` v1.32.0 or later and OpenTelemetry Go v1.38.0 or later.
CI runs the unit tests on Go 1.23 with those minimum versions and on the newest Go. It also runs the
[conformance kit](../conformance) on the newest SDK, alone, beside otelaws (also with the application's sampler
at 0) and beside dd-trace-go v2.

## What it records

| SQS call | Spans |
|---|---|
| `SendMessage`, `SendMessageBatch` | one `trailbeam send` per message SQS accepted, with the body size and FIFO fields |
| `ReceiveMessage` | one `trailbeam receive` per message, with the keyed receipt-handle hash, receive count, timestamps and dead-letter source ARN |
| `DeleteMessage`, `DeleteMessageBatch` | one `trailbeam delete` per receipt handle, with its result and error code |
| `ChangeMessageVisibility`, `ChangeMessageVisibilityBatch` | one `trailbeam change_visibility` per receipt handle |
| `PurgeQueue` | one `trailbeam purge` |

The middleware hooks only SQS clients: SNS publishes, EventBridge `PutEvents` and S3 uploads are not recorded. It
never reads a message body, so the timeline of a message that came through SNS, EventBridge or an S3 notification
starts at its first receive, with no step for where it came from (contract sections 21, 23, 25 and 27; the Python
and Node.js packages do).

To read the receive count and the other system attributes, the middleware adds their names to each
`ReceiveMessage` request's `MessageSystemAttributeNames`, or to the deprecated `AttributeNames` if that is the only
list you use. It works on a copy of your input, keeps every name you asked for, and leaves `All` alone. That
changes what SQS returns, not the message. It never makes an SQS call of its own.

Each span's parent is the span in the call's `context.Context` when the middleware reads it, after every other
middleware has started its span: otelaws's span for the same call, or your own span. Without one, it is the
request's `traceparent` header, then `X-Amzn-Trace-Id`. dd-trace-go keeps its span in the context as a Datadog
span, not an OpenTelemetry one, and puts no trace header on the request, so beside it Trailbeam's spans are root
spans. Trailbeam reads only its own spans, so its timeline is the same either way.

## Configuration

Environment variables, as for the other packages:

| Variable | Default |
|---|---|
| `TRAILBEAM_OTLP_TRACES_ENDPOINT` | `http://localhost:4318/v1/traces` (OTLP over HTTP, protobuf) |
| `TRAILBEAM_RECEIPT_HASH_KEY` | the published default key, with one warning. Use the same key on every service that uses the same queues (contract section 7). |
| `OTEL_SERVICE_NAME`, `OTEL_RESOURCE_ATTRIBUTES` | `unknown_service`, and a random `service.instance.id` |
| `TRAILBEAM_MAX_QUEUE_SIZE` | 16384 spans waiting for export |

Tenants and opt-in body capture (contract section 15) and the aws-lambda-go wrapper are later stages of QV-321.
