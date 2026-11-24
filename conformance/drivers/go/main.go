// Conformance driver for the Go package (aws-sdk-go-v2 + trailbeamsqs). Runs SCENARIO.md and writes the
// ground truth to $KIT_TRUTH_FILE. The kit sets every other variable (README.md, "What the kit sets").
//
// KIT_APP_TRACING says what tracing the application has of its own, set up as a real service would:
//   - otel: a global OpenTelemetry tracer provider (its sampler from OTEL_TRACES_SAMPLER) exporting over OTLP,
//     and the stock otelaws middleware on the same config, appended after Trailbeam's;
//   - datadog: dd-trace-go v2's tracer and its aws-sdk-go-v2 middleware (no Datadog agent: its spans go
//     nowhere).
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"runtime"
	"runtime/debug"
	"time"

	ddaws "github.com/DataDog/dd-trace-go/contrib/aws/aws-sdk-go-v2/v2/aws"
	"github.com/DataDog/dd-trace-go/v2/ddtrace/tracer"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/aws/smithy-go"
	"github.com/nick-lennox/trailbeam/go/trailbeamsqs"
	"go.opentelemetry.io/contrib/instrumentation/github.com/aws/aws-sdk-go-v2/otelaws"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

var (
	ctx   = context.Background()
	truth *os.File
	sqsc  *sqs.Client
)

func write(row map[string]interface{}) {
	b, err := json.Marshal(row)
	must(err)
	_, err = truth.Write(append(b, '\n'))
	must(err)
}

func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "driver:", err)
		os.Exit(1)
	}
}

func moduleVersion(path string) string {
	if bi, ok := debug.ReadBuildInfo(); ok {
		for _, m := range bi.Deps {
			if m.Path == path {
				if m.Replace != nil && m.Replace.Version == "" {
					return "(local)"
				}
				return m.Version
			}
		}
	}
	return "?"
}

func errorCode(err error) string {
	var api smithy.APIError
	if errors.As(err, &api) && api.ErrorCode() != "" {
		return api.ErrorCode()
	}
	return fmt.Sprintf("%T", err)
}

func send(q string, n int) string {
	body := fmt.Sprintf(`{"n": %d}`, n)
	r, err := sqsc.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: &q, MessageBody: &body})
	must(err)
	write(map[string]interface{}{"op": "SendMessage", "queue_url": q, "ok": true, "message_id": *r.MessageId, "body": body})
	return *r.MessageId
}

func receive(q string, asked ...string) []types.Message {
	in := &sqs.ReceiveMessageInput{QueueUrl: &q, MaxNumberOfMessages: 10, WaitTimeSeconds: 1}
	for _, a := range asked {
		in.MessageSystemAttributeNames = append(in.MessageSystemAttributeNames, types.MessageSystemAttributeName(a))
	}
	r, err := sqsc.ReceiveMessage(ctx, in)
	must(err)
	msgs := []map[string]interface{}{}
	for _, m := range r.Messages {
		attrs := m.Attributes
		if attrs == nil {
			attrs = map[string]string{}
		}
		msgs = append(msgs, map[string]interface{}{"message_id": *m.MessageId, "receipt_handle": *m.ReceiptHandle,
			"body": *m.Body, "attributes": attrs})
	}
	if asked == nil {
		asked = []string{}
	}
	write(map[string]interface{}{"op": "ReceiveMessage", "queue_url": q, "ok": true, "asked_system_attributes": asked,
		"messages": msgs})
	return r.Messages
}

func receiveUntil(q string, want []string, asked ...string) map[string]types.Message {
	got := map[string]types.Message{}
	for i := 0; i < 30; i++ {
		for _, m := range receive(q, asked...) {
			got[*m.MessageId] = m
		}
		all := true
		for _, id := range want {
			if _, ok := got[id]; !ok {
				all = false
			}
		}
		if all {
			return got
		}
	}
	must(fmt.Errorf("timed out waiting for %v on %s", want, q))
	return nil
}

func del(q, handle string) {
	row := map[string]interface{}{"op": "DeleteMessage", "queue_url": q, "receipt_handle": handle, "ok": true}
	if _, err := sqsc.DeleteMessage(ctx, &sqs.DeleteMessageInput{QueueUrl: &q, ReceiptHandle: &handle}); err != nil {
		row["ok"] = false
		row["error_code"] = errorCode(err)
	}
	write(row)
}

type entry struct {
	id, handle string
	visibility *int32
}

func batchRows(entries []entry, failed []types.BatchResultErrorEntry) []map[string]interface{} {
	codes := map[string]string{}
	for _, f := range failed {
		codes[*f.Id] = aws.ToString(f.Code)
	}
	var rows []map[string]interface{}
	for _, e := range entries {
		code, isFailed := codes[e.id]
		row := map[string]interface{}{"id": e.id, "receipt_handle": e.handle, "ok": !isFailed}
		if e.visibility != nil {
			row["visibility_timeout"] = *e.visibility
		}
		if isFailed {
			row["error_code"] = code
		}
		rows = append(rows, row)
	}
	return rows
}

func main() {
	run := os.Getenv("KIT_RUN")
	var err error
	truth, err = os.Create(os.Getenv("KIT_TRUTH_FILE"))
	must(err)

	cfg, err := config.LoadDefaultConfig(ctx)
	must(err)
	// The one line the README asks for.
	trailbeamsqs.AppendMiddlewares(&cfg.APIOptions)

	appTracing := os.Getenv("KIT_APP_TRACING")
	var appProvider *sdktrace.TracerProvider
	switch appTracing {
	case "otel":
		exp, err := otlptracehttp.New(ctx) // OTEL_EXPORTER_OTLP_ENDPOINT
		must(err)
		appProvider = sdktrace.NewTracerProvider(sdktrace.WithBatcher(exp)) // sampler from OTEL_TRACES_SAMPLER
		otel.SetTracerProvider(appProvider)
		otelaws.AppendMiddlewares(&cfg.APIOptions)
	case "datadog":
		must(tracer.Start(tracer.WithAgentAddr("127.0.0.1:9"), tracer.WithLogStartup(false)))
		ddaws.AppendMiddleware(&cfg)
	case "", "none":
		appTracing = "none"
	default:
		must(fmt.Errorf("unknown KIT_APP_TRACING %q", appTracing))
	}

	sqsc = sqs.NewFromConfig(cfg, func(o *sqs.Options) { o.BaseEndpoint = aws.String(os.Getenv("KIT_SQS_ENDPOINT")) })

	write(map[string]interface{}{"truth": 1, "driver": "go", "language": "go",
		"sdk":     "aws-sdk-go-v2 service/sqs " + moduleVersion("github.com/aws/aws-sdk-go-v2/service/sqs"),
		"package": "trailbeamsqs " + trailbeamsqs.Version, "app_tracing": appTracing, "go": runtime.Version()})

	// 1. Queues (not recorded)
	dlq := *must2(sqsc.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String(run + "-dlq")})).QueueUrl
	attrs := must2(sqsc.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{QueueUrl: &dlq,
		AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameQueueArn}}))
	redrive, _ := json.Marshal(map[string]string{"deadLetterTargetArn": attrs.Attributes["QueueArn"], "maxReceiveCount": "2"})
	q := *must2(sqsc.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: &run,
		Attributes: map[string]string{"VisibilityTimeout": "2", "RedrivePolicy": string(redrive)}})).QueueUrl
	pq := *must2(sqsc.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String(run + "-purge")})).QueueUrl

	// 2. Send and batch send
	ids := map[int]string{1: send(q, 1)}
	bodies := map[string]string{"e2": `{"n": 2}`, "e3": `{"n": 3}`}
	br := must2(sqsc.SendMessageBatch(ctx, &sqs.SendMessageBatchInput{QueueUrl: &q, Entries: []types.SendMessageBatchRequestEntry{
		{Id: aws.String("e2"), MessageBody: aws.String(bodies["e2"])}, {Id: aws.String("e3"), MessageBody: aws.String(bodies["e3"])}}}))
	var rows []map[string]interface{}
	for _, s := range br.Successful {
		rows = append(rows, map[string]interface{}{"id": *s.Id, "ok": true, "message_id": *s.MessageId, "body": bodies[*s.Id]})
		ids[int((*s.Id)[1]-'0')] = *s.MessageId
	}
	for _, f := range br.Failed {
		rows = append(rows, map[string]interface{}{"id": *f.Id, "ok": false, "error_code": aws.ToString(f.Code)})
	}
	write(map[string]interface{}{"op": "SendMessageBatch", "queue_url": q, "ok": true, "entries": rows})

	// 3. Receive all three; these receives ask for one system attribute of their own
	got := receiveUntil(q, []string{ids[1], ids[2], ids[3]}, "SenderId")

	// 4. Delete n=1; batch delete n=2 and a bogus handle (one failed entry)
	del(q, *got[ids[1]].ReceiptHandle)
	dEntries := []entry{{id: "d2", handle: *got[ids[2]].ReceiptHandle}, {id: "bogus", handle: "not-a-receipt-handle"}}
	var dIn []types.DeleteMessageBatchRequestEntry
	for _, e := range dEntries {
		dIn = append(dIn, types.DeleteMessageBatchRequestEntry{Id: aws.String(e.id), ReceiptHandle: aws.String(e.handle)})
	}
	dr := must2(sqsc.DeleteMessageBatch(ctx, &sqs.DeleteMessageBatchInput{QueueUrl: &q, Entries: dIn}))
	write(map[string]interface{}{"op": "DeleteMessageBatch", "queue_url": q, "ok": true, "entries": batchRows(dEntries, dr.Failed)})

	// 5. Visibility-timeout retry: n=3 comes back with receive count 2
	time.Sleep(3 * time.Second)
	receiveUntil(q, []string{ids[3]})

	// 6. DLQ move: ElasticMQ moves n=3 on the next receive of the source queue
	time.Sleep(3 * time.Second)
	receive(q)
	d := receiveUntil(dlq, []string{ids[3]})
	del(dlq, *d[ids[3]].ReceiptHandle)

	// 7. ChangeMessageVisibility(0), receive again, ChangeMessageVisibilityBatch, delete
	ids[4] = send(q, 4)
	h := *receiveUntil(q, []string{ids[4]})[ids[4]].ReceiptHandle
	_, err = sqsc.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{QueueUrl: &q, ReceiptHandle: &h, VisibilityTimeout: 0})
	must(err)
	write(map[string]interface{}{"op": "ChangeMessageVisibility", "queue_url": q, "receipt_handle": h, "visibility_timeout": 0, "ok": true})
	h = *receiveUntil(q, []string{ids[4]})[ids[4]].ReceiptHandle
	thirty := int32(30)
	vEntries := []entry{{id: "v4", handle: h, visibility: &thirty}}
	vr := must2(sqsc.ChangeMessageVisibilityBatch(ctx, &sqs.ChangeMessageVisibilityBatchInput{QueueUrl: &q,
		Entries: []types.ChangeMessageVisibilityBatchRequestEntry{{Id: aws.String("v4"), ReceiptHandle: &h, VisibilityTimeout: 30}}}))
	write(map[string]interface{}{"op": "ChangeMessageVisibilityBatch", "queue_url": q, "ok": true, "entries": batchRows(vEntries, vr.Failed)})
	del(q, h)

	// 8. A delete with an invalid handle: the call fails
	del(q, "definitely-not-a-valid-receipt-handle")

	// 9. Purge
	_, err = sqsc.PurgeQueue(ctx, &sqs.PurgeQueueInput{QueueUrl: &pq})
	must(err)
	write(map[string]interface{}{"op": "PurgeQueue", "queue_url": pq, "ok": true})

	// 10. Flush
	sctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	must(trailbeamsqs.Shutdown(sctx))
	if appProvider != nil {
		must(appProvider.Shutdown(sctx))
	}
	if appTracing == "datadog" {
		tracer.Stop()
	}
	must(truth.Close())
	out, _ := json.Marshal(map[string]interface{}{"driver": "go", "run": run, "ids": ids})
	fmt.Println(string(out))
}

func must2[T any](v T, err error) T {
	must(err)
	return v
}
