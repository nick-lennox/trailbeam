package trailbeamsqs

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

const queueURL = "http://localhost:9324/000000000000/orders"

func vectors(t *testing.T) map[string]json.RawMessage {
	t.Helper()
	raw, err := os.ReadFile("../../contract/span-contract-vectors.json")
	if err != nil {
		t.Fatal(err)
	}
	var d map[string]json.RawMessage
	if err := json.Unmarshal(raw, &d); err != nil {
		t.Fatal(err)
	}
	return d
}

func TestReceiptHashVectors(t *testing.T) {
	d := vectors(t)
	var vs []struct{ Name, Key, ReceiptHandle, Hmac, KeyID string }
	var raw []map[string]string
	_ = json.Unmarshal(d["vectors"], &raw)
	for _, v := range raw {
		vs = append(vs, struct{ Name, Key, ReceiptHandle, Hmac, KeyID string }{v["name"], v["key"], v["receipt_handle"], v["hmac"], v["key_id"]})
	}
	if len(vs) < 3 {
		t.Fatalf("only %d vectors", len(vs))
	}
	for _, v := range vs {
		h := newReceiptHasher(v.Key)
		if got := h.hash(v.ReceiptHandle); got != v.Hmac {
			t.Errorf("%s: hmac %s, want %s", v.Name, got, v.Hmac)
		}
		if h.keyID != v.KeyID {
			t.Errorf("%s: key id %s, want %s", v.Name, h.keyID, v.KeyID)
		}
	}
	if id := newReceiptHasher(defaultKey).keyID; id != "b5e633f7" {
		t.Errorf("default key id %s", id)
	}
	if s := newReceiptHasher("a-secret-key-0123456789").String(); strings.Contains(s, "secret") {
		t.Errorf("hasher shows its key: %s", s)
	}
}

func setHeader(req interface{}, k, v string) {
	if r, ok := req.(*smithyhttp.Request); ok {
		r.Header.Set(k, v)
	}
}

// EndpointConnectionError is an error raised before any response (the purge vectors' "thrown").
type EndpointConnectionError struct{}

func (EndpointConnectionError) Error() string { return "could not connect" }

func TestPurgeVectors(t *testing.T) {
	var pv struct {
		SpanName string   `json:"span_name"`
		Present  []string `json:"present"`
		Cases    []struct {
			Name       string                 `json:"name"`
			QueueURL   string                 `json:"queue_url"`
			Response   map[string]interface{} `json:"response"`
			Attributes map[string]interface{} `json:"attributes"`
			Absent     []string               `json:"absent"`
		} `json:"cases"`
	}
	if err := json.Unmarshal(vectors(t)["purge"], &pv); err != nil {
		t.Fatal(err)
	}
	if len(pv.Cases) < 4 {
		t.Fatalf("only %d purge cases", len(pv.Cases))
	}
	for _, c := range pv.Cases {
		t.Run(c.Name, func(t *testing.T) {
			exp := installTest(t, nil)
			stub := newStub()
			if code, ok := c.Response["error_code"].(string); ok {
				stub.on("PurgeQueue", func(map[string]interface{}) (int, interface{}) { return sqsError(code) })
			}
			if _, ok := c.Response["thrown"]; ok {
				stub.fail = EndpointConnectionError{}
			}
			_, _ = stub.client().PurgeQueue(context.Background(), &sqs.PurgeQueueInput{QueueUrl: aws.String(c.QueueURL)})
			spans := exp.GetSpans()
			if len(spans) != 1 || spans[0].Name != pv.SpanName {
				t.Fatalf("want one %q, got %d spans", pv.SpanName, len(spans))
			}
			got := attrs(spans[0])
			for k, want := range c.Attributes {
				if f, ok := want.(float64); ok {
					want = int64(f)
				}
				if got[k] != want {
					t.Errorf("%s = %v (%T), want %v", k, got[k], got[k], want)
				}
			}
			for _, k := range pv.Present {
				if _, ok := got[k]; !ok {
					t.Errorf("%s missing", k)
				}
			}
			for _, k := range c.Absent {
				if _, ok := got[k]; ok {
					t.Errorf("%s present: %v", k, got[k])
				}
			}
			if got["trailbeam.order.issued"].(int64) >= got["trailbeam.order.returned"].(int64) {
				t.Errorf("issued %v not before returned %v", got["trailbeam.order.issued"], got["trailbeam.order.returned"])
			}
		})
	}
}

// scenarioStub answers every operation the way SQS would for a queue holding two messages.
func scenarioStub() *stubSQS {
	return newStub().
		on("SendMessage", func(req map[string]interface{}) (int, interface{}) {
			return 200, map[string]interface{}{"MessageId": "m-1", "MD5OfMessageBody": md5hex(req["MessageBody"].(string))}
		}).
		on("SendMessageBatch", func(req map[string]interface{}) (int, interface{}) {
			var ok []map[string]interface{}
			var failed []map[string]interface{}
			for _, e := range req["Entries"].([]interface{}) {
				e := e.(map[string]interface{})
				if e["Id"] == "bad" {
					failed = append(failed, map[string]interface{}{"Id": "bad", "Code": "InvalidMessageContents", "SenderFault": true})
					continue
				}
				ok = append(ok, map[string]interface{}{"Id": e["Id"], "MessageId": "m-" + e["Id"].(string),
					"MD5OfMessageBody": md5hex(e["MessageBody"].(string))})
			}
			return 200, map[string]interface{}{"Successful": ok, "Failed": failed}
		}).
		on("ReceiveMessage", func(req map[string]interface{}) (int, interface{}) {
			return 200, map[string]interface{}{"Messages": []map[string]interface{}{
				{"MessageId": "m-1", "ReceiptHandle": "handle-1", "Body": `{"n": 1}`, "MD5OfBody": md5hex(`{"n": 1}`),
					"Attributes": map[string]string{"ApproximateReceiveCount": "2", "SentTimestamp": "1700000000000",
						"ApproximateFirstReceiveTimestamp": "1700000000500", "SenderId": "AIDA"}},
				{"MessageId": "m-2", "ReceiptHandle": "handle-2", "Body": "héllo", "MD5OfBody": md5hex("héllo"),
					"Attributes": map[string]string{"ApproximateReceiveCount": "1",
						"DeadLetterQueueSourceArn": "arn:aws:sqs:us-east-1:000000000000:src",
						"MessageGroupId":           "g1", "SequenceNumber": "18849496460467696128"}},
			}}
		}).
		on("DeleteMessage", func(req map[string]interface{}) (int, interface{}) {
			if req["ReceiptHandle"] == "bogus" {
				return sqsError("com.amazonaws.sqs#ReceiptHandleIsInvalid")
			}
			return 200, map[string]interface{}{}
		}).
		on("DeleteMessageBatch", func(req map[string]interface{}) (int, interface{}) {
			return 200, map[string]interface{}{"Successful": []map[string]string{{"Id": "d2"}},
				"Failed": []map[string]interface{}{{"Id": "bogus", "Code": "ReceiptHandleIsInvalid", "SenderFault": true}}}
		}).
		on("ChangeMessageVisibilityBatch", func(req map[string]interface{}) (int, interface{}) {
			return 200, map[string]interface{}{"Successful": []map[string]string{{"Id": "v1"}}}
		})
}

func TestSpansForEveryOperation(t *testing.T) {
	exp := installTest(t, map[string]string{"TRAILBEAM_RECEIPT_HASH_KEY": "test-key-0123456789abcdef",
		"OTEL_SERVICE_NAME": "svc", "OTEL_RESOURCE_ATTRIBUTES": "service.instance.id=inst-1,service.version=1.4.2"})
	stub := scenarioStub()
	c := stub.client()
	ctx := context.Background()
	h := newReceiptHasher("test-key-0123456789abcdef")

	if _, err := c.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: aws.String(queueURL), MessageBody: aws.String(`{"n": 1}`),
		MessageGroupId: aws.String("g1")}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.SendMessageBatch(ctx, &sqs.SendMessageBatchInput{QueueUrl: aws.String(queueURL), Entries: []types.SendMessageBatchRequestEntry{
		{Id: aws.String("e2"), MessageBody: aws.String("two")}, {Id: aws.String("bad"), MessageBody: aws.String("x")}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: aws.String(queueURL)}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.DeleteMessage(ctx, &sqs.DeleteMessageInput{QueueUrl: aws.String(queueURL), ReceiptHandle: aws.String("handle-1")}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.DeleteMessageBatch(ctx, &sqs.DeleteMessageBatchInput{QueueUrl: aws.String(queueURL), Entries: []types.DeleteMessageBatchRequestEntry{
		{Id: aws.String("d2"), ReceiptHandle: aws.String("handle-2")}, {Id: aws.String("bogus"), ReceiptHandle: aws.String("nope")}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{QueueUrl: aws.String(queueURL),
		ReceiptHandle: aws.String("handle-1"), VisibilityTimeout: 0}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.ChangeMessageVisibilityBatch(ctx, &sqs.ChangeMessageVisibilityBatchInput{QueueUrl: aws.String(queueURL),
		Entries: []types.ChangeMessageVisibilityBatchRequestEntry{{Id: aws.String("v1"), ReceiptHandle: aws.String("handle-x"), VisibilityTimeout: 30}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.DeleteMessage(ctx, &sqs.DeleteMessageInput{QueueUrl: aws.String(queueURL), ReceiptHandle: aws.String("bogus")}); err == nil {
		t.Fatal("the bogus delete should fail")
	}
	// Only the application's own calls reached SQS (contract section 15: no calls of its own).
	want := []string{"SendMessage", "SendMessageBatch", "ReceiveMessage", "DeleteMessage", "DeleteMessageBatch",
		"ChangeMessageVisibility", "ChangeMessageVisibilityBatch", "DeleteMessage"}
	if got := stub.ops(); !reflect.DeepEqual(got, want) {
		t.Fatalf("SQS calls %v, want %v", got, want)
	}

	spans := exp.GetSpans()
	if len(spans) != 10 {
		for _, s := range spans {
			t.Log(describe(s))
		}
		t.Fatalf("%d spans, want 10", len(spans))
	}
	for _, s := range spans {
		a := attrs(s)
		if s.InstrumentationScope.Name != "trailbeam" || s.InstrumentationScope.Version != Version {
			t.Errorf("scope %v", s.InstrumentationScope)
		}
		if s.SpanKind != trace.SpanKindInternal {
			t.Errorf("%s kind %v", s.Name, s.SpanKind)
		}
		for k, v := range map[string]interface{}{"trailbeam.schema_version": int64(3), "trailbeam.source": "client",
			"messaging.system.name": "aws.sqs", "messaging.destination.name": "orders", "aws.sqs.queue.url": queueURL,
			"messaging.client.id": "inst-1", "process.pid": int64(os.Getpid()), "trailbeam.order.scope": order.scope} {
			if a[k] != v {
				t.Errorf("%s: %s = %v, want %v", s.Name, k, a[k], v)
			}
		}
		for k, v := range a {
			if v == "" {
				t.Errorf("%s: %s is empty", s.Name, k)
			}
		}
		if s.EndTime.Before(s.StartTime) {
			t.Errorf("%s ends before it starts", s.Name)
		}
		if s.Parent.IsValid() {
			t.Errorf("%s has a parent with no span in the context", s.Name)
		}
	}
	res := map[string]string{}
	for _, kv := range spans[0].Resource.Attributes() {
		res[string(kv.Key)] = kv.Value.AsString()
	}
	if res["service.name"] != "svc" || res["service.instance.id"] != "inst-1" || res["service.version"] != "1.4.2" {
		t.Errorf("resource %v", res)
	}

	sends := spansNamed(exp, spanSend)
	if len(sends) != 2 {
		t.Fatalf("%d sends", len(sends))
	}
	a := attrs(sends[0])
	check(t, a, map[string]interface{}{"trailbeam.kind": "sqs.send", "messaging.operation.type": "send",
		"messaging.operation.name": "SendMessage", "messaging.message.id": "m-1", "messaging.message.body.size": int64(8),
		"trailbeam.body.capture_status": "off", "trailbeam.message_group_id": "g1"}, "trailbeam.batch_entry_id")
	check(t, attrs(sends[1]), map[string]interface{}{"messaging.operation.name": "SendMessageBatch",
		"messaging.message.id": "m-e2", "trailbeam.batch_entry_id": "e2", "messaging.message.body.size": int64(3)})

	recv := spansNamed(exp, spanReceive)
	if len(recv) != 2 {
		t.Fatalf("%d receives", len(recv))
	}
	check(t, attrs(recv[0]), map[string]interface{}{"trailbeam.kind": "sqs.receive", "messaging.operation.type": "receive",
		"messaging.message.id": "m-1", "trailbeam.receive_count": int64(2), "trailbeam.sent_timestamp_ms": int64(1700000000000),
		"trailbeam.first_receive_timestamp_ms": int64(1700000000500), "trailbeam.receipt_handle.hmac": h.hash("handle-1"),
		"trailbeam.receipt_handle.key_id": h.keyID, "messaging.message.body.size": int64(8)}, "trailbeam.dead_letter_queue_source_arn")
	check(t, attrs(recv[1]), map[string]interface{}{"messaging.message.id": "m-2", "trailbeam.receive_count": int64(1),
		"trailbeam.dead_letter_queue_source_arn": "arn:aws:sqs:us-east-1:000000000000:src", "trailbeam.message_group_id": "g1",
		"trailbeam.sequence_number": "18849496460467696128", "messaging.message.body.size": int64(6)}, "trailbeam.sent_timestamp_ms")
	if recv[0].StartTime != recv[1].StartTime || recv[0].EndTime != recv[1].EndTime ||
		attrs(recv[0])["trailbeam.order.issued"] != attrs(recv[1])["trailbeam.order.issued"] {
		t.Error("the spans of one receive must share its interval and ticks")
	}

	dels := spansNamed(exp, spanDelete)
	if len(dels) != 4 {
		t.Fatalf("%d deletes", len(dels))
	}
	check(t, attrs(dels[0]), map[string]interface{}{"trailbeam.kind": "sqs.settle", "messaging.operation.type": "settle",
		"messaging.operation.name": "DeleteMessage", "messaging.message.id": "m-1", "trailbeam.result": "ok",
		"trailbeam.receipt_handle.hmac": h.hash("handle-1")}, "trailbeam.error_code", "trailbeam.batch_entry_id")
	check(t, attrs(dels[1]), map[string]interface{}{"messaging.operation.name": "DeleteMessageBatch", "trailbeam.batch_entry_id": "d2",
		"messaging.message.id": "m-2", "trailbeam.result": "ok"}, "trailbeam.error_code")
	check(t, attrs(dels[2]), map[string]interface{}{"trailbeam.batch_entry_id": "bogus", "trailbeam.result": "failed",
		"trailbeam.error_code": "ReceiptHandleIsInvalid", "trailbeam.receipt_handle.hmac": h.hash("nope")}, "messaging.message.id")
	check(t, attrs(dels[3]), map[string]interface{}{"messaging.operation.name": "DeleteMessage", "trailbeam.result": "failed",
		"trailbeam.error_code": "ReceiptHandleIsInvalid"}, "messaging.message.id")

	cvs := spansNamed(exp, spanChangeVisibility)
	if len(cvs) != 2 {
		t.Fatalf("%d change_visibility", len(cvs))
	}
	// handle-1 was deleted successfully, so the map no longer has its message ID.
	check(t, attrs(cvs[0]), map[string]interface{}{"messaging.operation.name": "ChangeMessageVisibility",
		"trailbeam.visibility_timeout": int64(0), "trailbeam.result": "ok"}, "messaging.message.id", "trailbeam.batch_entry_id")
	check(t, attrs(cvs[1]), map[string]interface{}{"messaging.operation.name": "ChangeMessageVisibilityBatch",
		"trailbeam.visibility_timeout": int64(30), "trailbeam.batch_entry_id": "v1", "trailbeam.result": "ok"})

	// Section 2a: consecutive calls are ordered by their ticks.
	sort.SliceStable(spans, func(i, j int) bool {
		return attrs(spans[i])["trailbeam.order.issued"].(int64) < attrs(spans[j])["trailbeam.order.issued"].(int64)
	})
	for i := 1; i < len(spans); i++ {
		p, q := attrs(spans[i-1]), attrs(spans[i])
		if p["trailbeam.order.issued"] == q["trailbeam.order.issued"] {
			continue
		}
		if p["trailbeam.order.returned"].(int64) >= q["trailbeam.order.issued"].(int64) {
			t.Errorf("call returned at %v, next issued at %v", p["trailbeam.order.returned"], q["trailbeam.order.issued"])
		}
	}
}

func check(t *testing.T, a map[string]interface{}, want map[string]interface{}, absent ...string) {
	t.Helper()
	for k, v := range want {
		if a[k] != v {
			t.Errorf("%s = %v (%T), want %v", k, a[k], a[k], v)
		}
	}
	for _, k := range absent {
		if v, ok := a[k]; ok {
			t.Errorf("%s should be absent, is %v", k, v)
		}
	}
}

func names(v interface{}) []string {
	var out []string
	if l, ok := v.([]interface{}); ok {
		for _, x := range l {
			out = append(out, x.(string))
		}
	}
	return out
}

func TestReceiveAsksForAttributes(t *testing.T) {
	installTest(t, nil)
	stub := scenarioStub()
	c := stub.client()
	ctx := context.Background()

	in := &sqs.ReceiveMessageInput{QueueUrl: aws.String(queueURL), MessageSystemAttributeNames: []types.MessageSystemAttributeName{"SenderId"}}
	if _, err := c.ReceiveMessage(ctx, in); err != nil {
		t.Fatal(err)
	}
	got := names(stub.last("ReceiveMessage").body["MessageSystemAttributeNames"])
	want := append([]string{"SenderId"}, receiveSystemAttributes...)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("asked for %v, want %v", got, want)
	}
	if len(in.MessageSystemAttributeNames) != 1 {
		t.Errorf("the caller's input was changed: %v", in.MessageSystemAttributeNames)
	}

	// Only the deprecated AttributeNames: it stays the one used.
	legacy := &sqs.ReceiveMessageInput{QueueUrl: aws.String(queueURL), AttributeNames: []types.QueueAttributeName{"SentTimestamp"}} //nolint:staticcheck
	if _, err := c.ReceiveMessage(ctx, legacy); err != nil {
		t.Fatal(err)
	}
	body := stub.last("ReceiveMessage").body
	if got := names(body["AttributeNames"]); len(got) != len(receiveSystemAttributes) || got[0] != "SentTimestamp" {
		t.Errorf("legacy names %v", got)
	}
	if _, ok := body["MessageSystemAttributeNames"]; ok {
		t.Errorf("legacy caller got MessageSystemAttributeNames too")
	}

	// All is left alone.
	all := &sqs.ReceiveMessageInput{QueueUrl: aws.String(queueURL), MessageSystemAttributeNames: []types.MessageSystemAttributeName{"All"}}
	if _, err := c.ReceiveMessage(ctx, all); err != nil {
		t.Fatal(err)
	}
	if got := names(stub.last("ReceiveMessage").body["MessageSystemAttributeNames"]); !reflect.DeepEqual(got, []string{"All"}) {
		t.Errorf("All became %v", got)
	}
}

// stockMiddleware starts a span in an Initialize After middleware, as otelaws and dd-trace-go do.
func stockMiddleware(tp trace.TracerProvider, started *trace.SpanContext) func(*middleware.Stack) error {
	return func(stack *middleware.Stack) error {
		return stack.Initialize.Add(middleware.InitializeMiddlewareFunc("Stock", func(ctx context.Context, in middleware.InitializeInput,
			next middleware.InitializeHandler) (middleware.InitializeOutput, middleware.Metadata, error) {
			ctx, span := tp.Tracer("stock").Start(ctx, "SQS.call")
			defer span.End()
			*started = span.SpanContext()
			return next.HandleInitialize(ctx, in)
		}), middleware.After)
	}
}

func TestParentIsTheStockSpanWhateverTheOrder(t *testing.T) {
	for _, sampler := range []sdktrace.Sampler{sdktrace.AlwaysSample(), sdktrace.NeverSample()} {
		exp := installTest(t, nil)
		app := sdktrace.NewTracerProvider(sdktrace.WithSampler(sampler))
		var stock trace.SpanContext
		// Trailbeam's middleware appended first, the stock one after it (the stock span then starts inside
		// Trailbeam's Initialize middleware), and the other way round: the stock span is the parent either way.
		for _, stockFirst := range []bool{false, true} {
			exp.Reset()
			var opts []func(*middleware.Stack) error
			if stockFirst {
				opts = append(opts, stockMiddleware(app, &stock))
			}
			AppendMiddlewares(&opts)
			if !stockFirst {
				opts = append(opts, stockMiddleware(app, &stock))
			}
			c := sqs.New(sqs.Options{Region: "us-east-1", BaseEndpoint: aws.String("http://sqs.stub"),
				Credentials: aws.AnonymousCredentials{}, HTTPClient: scenarioStub(), APIOptions: opts, RetryMaxAttempts: 1})
			if _, err := c.SendMessage(context.Background(), &sqs.SendMessageInput{QueueUrl: aws.String(queueURL), MessageBody: aws.String("x")}); err != nil {
				t.Fatal(err)
			}
			spans := exp.GetSpans()
			if len(spans) != 1 {
				t.Fatalf("%d spans", len(spans))
			}
			if spans[0].Parent.SpanID() != stock.SpanID() || spans[0].SpanContext.TraceID() != stock.TraceID() {
				t.Errorf("sampler %v, stock first %v: parent %v, want the stock span %v", sampler.Description(), stockFirst, spans[0].Parent, stock)
			}
			if !spans[0].SpanContext.IsSampled() {
				t.Errorf("sampler %v: the Trailbeam span was not sampled", sampler.Description())
			}
		}
	}
}

func TestParentFromTraceparentHeader(t *testing.T) {
	exp := installTest(t, nil)
	tp := "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00"
	c := scenarioStub().client(func(stack *middleware.Stack) error {
		return stack.Build.Add(middleware.BuildMiddlewareFunc("Header", func(ctx context.Context, in middleware.BuildInput,
			next middleware.BuildHandler) (middleware.BuildOutput, middleware.Metadata, error) {
			setHeader(in.Request, "traceparent", tp)
			return next.HandleBuild(ctx, in)
		}), middleware.After)
	})
	if _, err := c.SendMessage(context.Background(), &sqs.SendMessageInput{QueueUrl: aws.String(queueURL), MessageBody: aws.String("x")}); err != nil {
		t.Fatal(err)
	}
	s := exp.GetSpans()[0]
	if s.Parent.TraceID().String() != "4bf92f3577b34da6a3ce929d0e0e4736" || s.Parent.SpanID().String() != "00f067aa0ba902b7" {
		t.Errorf("parent %v", s.Parent)
	}
}

func TestAppendTwiceEmitsOnce(t *testing.T) {
	exp := installTest(t, nil)
	stub := scenarioStub()
	var twice []func(*middleware.Stack) error
	AppendMiddlewares(&twice)
	c := stub.client(twice...)
	if _, err := c.SendMessage(context.Background(), &sqs.SendMessageInput{QueueUrl: aws.String(queueURL), MessageBody: aws.String("x")}); err != nil {
		t.Fatal(err)
	}
	if n := len(exp.GetSpans()); n != 1 {
		t.Errorf("%d spans, want 1", n)
	}
}

func TestOtherCallsEmitNothing(t *testing.T) {
	exp := installTest(t, nil)
	stub := newStub().on("GetQueueUrl", func(map[string]interface{}) (int, interface{}) {
		return 200, map[string]string{"QueueUrl": queueURL}
	})
	if _, err := stub.client().GetQueueUrl(context.Background(), &sqs.GetQueueUrlInput{QueueName: aws.String("orders")}); err != nil {
		t.Fatal(err)
	}
	if n := len(exp.GetSpans()); n != 0 {
		t.Errorf("%d spans for GetQueueUrl", n)
	}
}

func TestFailedCallStillEmitsSettleSpans(t *testing.T) {
	exp := installTest(t, nil)
	stub := scenarioStub()
	stub.fail = errors.New("connection reset")
	_, _ = stub.client().DeleteMessageBatch(context.Background(), &sqs.DeleteMessageBatchInput{QueueUrl: aws.String(queueURL),
		Entries: []types.DeleteMessageBatchRequestEntry{{Id: aws.String("a"), ReceiptHandle: aws.String("h-a")}, {Id: aws.String("b"), ReceiptHandle: aws.String("h-b")}}})
	// A failed send or receive has no message, so no span.
	_, _ = stub.client().SendMessage(context.Background(), &sqs.SendMessageInput{QueueUrl: aws.String(queueURL), MessageBody: aws.String("x")})
	spans := exp.GetSpans()
	if len(spans) != 2 {
		t.Fatalf("%d spans, want 2", len(spans))
	}
	for _, s := range spans {
		a := attrs(s)
		if a["trailbeam.result"] != "failed" || a["trailbeam.error_code"] == nil {
			t.Errorf("%s", describe(s))
		}
	}
}

func TestHandleMap(t *testing.T) {
	now := time.Unix(0, 0)
	m := newHandleMap(2, time.Hour, func() time.Time { return now })
	m.put("a", "1")
	m.put("b", "2")
	m.put("c", "3")
	if m.get("a") != "" || m.get("b") != "2" || m.size() != 2 {
		t.Errorf("bound: a=%q b=%q size=%d", m.get("a"), m.get("b"), m.size())
	}
	now = now.Add(2 * time.Hour)
	if m.get("b") != "" {
		t.Error("expired entry returned")
	}
	m.put("d", "4")
	if m.size() != 1 {
		t.Errorf("expired entries kept: %d", m.size())
	}
	m.remove("d")
	if m.get("d") != "" {
		t.Error("removed entry returned")
	}
}

func TestTraceHeaders(t *testing.T) {
	p := parentFromHeaders("", "Root=1-5759e988-bd862e3fe1be46a994272793;Parent=53995c3f42cd8ad8;Sampled=1")
	if p.TraceID().String() != "5759e988bd862e3fe1be46a994272793" || p.SpanID().String() != "53995c3f42cd8ad8" {
		t.Errorf("xray %v", p)
	}
	for _, bad := range []string{"", "Root=1-5759e988-bd862e3fe1be46a994272793", "Root=2-x;Parent=53995c3f42cd8ad8"} {
		if parentFromHeaders("garbage", bad).IsValid() {
			t.Errorf("%q parsed", bad)
		}
	}
}

func TestResourceDefaults(t *testing.T) {
	res := map[string]string{}
	for _, kv := range buildResource(func(string) string { return "" }).Attributes() {
		res[string(kv.Key)] = kv.Value.AsString()
	}
	if res["service.name"] != "unknown_service" || len(res["service.instance.id"]) != 36 {
		t.Errorf("resource %v", res)
	}
}
