package trailbeamsqs

import (
	"context"
	"errors"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/aws/smithy-go"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

const (
	initializeID  = "TrailbeamSQSInitialize"
	deserializeID = "TrailbeamSQSParent"
)

// AppendMiddlewares adds Trailbeam's middleware to apiOptions, so that every SQS client built from them
// emits Trailbeam spans. Pass the APIOptions of the aws.Config the clients are made from:
//
//	trailbeamsqs.AppendMiddlewares(&cfg.APIOptions)
//
// Clients of other services are left alone. Adding it twice (to the config and again to one client's
// options) emits each span once.
func AppendMiddlewares(apiOptions *[]func(*middleware.Stack) error) {
	*apiOptions = append(*apiOptions, addMiddlewares)
}

func addMiddlewares(stack *middleware.Stack) error {
	if _, ok := stack.Initialize.Get(initializeID); ok {
		return nil
	}
	// After every Initialize middleware that was added with middleware.Before, and as close to the SDK's
	// own steps as an After middleware can be: the start is read before the request is serialized, signed and
	// sent, and before any retry (contract section 2).
	if err := stack.Initialize.Add(middleware.InitializeMiddlewareFunc(initializeID, initialize), middleware.After); err != nil {
		return nil
	}
	// The parent is read in the innermost step, once every Initialize middleware has run whatever order they
	// were added in: otelaws and dd-trace-go start their span in an After middleware too.
	_ = stack.Deserialize.Add(middleware.DeserializeMiddlewareFunc(deserializeID, readParent), middleware.After)
	return nil
}

// call is the state of one SQS call, shared by the two middlewares through the context.
type call struct {
	operation string
	issued    int64
	start     time.Time

	once   sync.Once
	mu     sync.Mutex
	parent trace.SpanContext
}

func (c *call) setParent(p trace.SpanContext) {
	c.once.Do(func() {
		c.mu.Lock()
		c.parent = p
		c.mu.Unlock()
	})
}

func (c *call) getParent() trace.SpanContext {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.parent
}

type callKey struct{}

// receiveSystemAttributes are the system attributes the receive request asks for (contract section 4.2).
var receiveSystemAttributes = []string{
	"ApproximateReceiveCount",
	"SentTimestamp",
	"ApproximateFirstReceiveTimestamp",
	"AWSTraceHeader",
	"DeadLetterQueueSourceArn", // 3.1: set only on a receive from a dead-letter queue
	"MessageGroupId",           // 3.2: FIFO queues (and fair standard queues)
	"SequenceNumber",           // 3.2: FIFO queues only
}

func initialize(ctx context.Context, in middleware.InitializeInput, next middleware.InitializeHandler) (
	out middleware.InitializeOutput, md middleware.Metadata, err error,
) {
	op := operationName(in.Parameters)
	if op == "" || ctx.Value(callKey{}) != nil {
		return next.HandleInitialize(ctx, in)
	}
	var c *call
	func() {
		defer func() { _ = recover() }()
		r := get()
		infoOnce("active", "trailbeam: middleware active, SQS calls are traced (trailbeam "+Version+
			", exporting to "+r.endpoint+")")
		if p, ok := in.Parameters.(*sqs.ReceiveMessageInput); ok && p != nil {
			in.Parameters = withReceiveAttributeNames(p)
		}
		// Section 2a: the issued tick just before the start time is read.
		issued := tick()
		c = &call{operation: op, issued: issued, start: time.Now()}
		ctx = context.WithValue(ctx, callKey{}, c)
	}()
	out, md, err = next.HandleInitialize(ctx, in)
	if c != nil {
		// Section 2: the span ends when the response (or error) arrived; the returned tick just after.
		end := time.Now()
		returned := tick()
		func() {
			defer func() { _ = recover() }()
			finish(get(), c, in.Parameters, out.Result, err, end, returned)
		}()
	}
	return out, md, err
}

// readParent records the call's parent: the span in the context (otelaws, or the application's own span),
// else the trace header on the request (traceparent, then X-Amzn-Trace-Id).
func readParent(ctx context.Context, in middleware.DeserializeInput, next middleware.DeserializeHandler) (
	middleware.DeserializeOutput, middleware.Metadata, error,
) {
	if c, ok := ctx.Value(callKey{}).(*call); ok {
		func() {
			defer func() { _ = recover() }()
			p := trace.SpanContextFromContext(ctx)
			if !p.IsValid() {
				if req, ok := in.Request.(*smithyhttp.Request); ok {
					p = parentFromHeaders(req.Header.Get("traceparent"), req.Header.Get("X-Amzn-Trace-Id"))
				}
			}
			c.setParent(p)
		}()
	}
	return next.HandleDeserialize(ctx, in)
}

func operationName(params interface{}) string {
	switch params.(type) {
	case *sqs.SendMessageInput:
		return "SendMessage"
	case *sqs.SendMessageBatchInput:
		return "SendMessageBatch"
	case *sqs.ReceiveMessageInput:
		return "ReceiveMessage"
	case *sqs.DeleteMessageInput:
		return "DeleteMessage"
	case *sqs.DeleteMessageBatchInput:
		return "DeleteMessageBatch"
	case *sqs.ChangeMessageVisibilityInput:
		return "ChangeMessageVisibility"
	case *sqs.ChangeMessageVisibilityBatchInput:
		return "ChangeMessageVisibilityBatch"
	case *sqs.PurgeQueueInput:
		return "PurgeQueue"
	}
	return ""
}

// withReceiveAttributeNames asks for the system attributes Trailbeam needs, keeping everything the caller
// asked for, on a copy: the caller's input is not changed. A caller that uses only the deprecated
// AttributeNames keeps using it. "All" is left alone.
func withReceiveAttributeNames(p *sqs.ReceiveMessageInput) *sqs.ReceiveMessageInput {
	asked := map[string]bool{}
	for _, n := range p.AttributeNames { //nolint:staticcheck // the deprecated field is read on purpose
		asked[string(n)] = true
	}
	for _, n := range p.MessageSystemAttributeNames {
		asked[string(n)] = true
	}
	if asked["All"] {
		return p
	}
	var missing []string
	for _, n := range receiveSystemAttributes {
		if !asked[n] {
			missing = append(missing, n)
		}
	}
	if len(missing) == 0 {
		return p
	}
	cp := *p
	if len(p.AttributeNames) > 0 && len(p.MessageSystemAttributeNames) == 0 { //nolint:staticcheck
		names := append([]types.QueueAttributeName(nil), p.AttributeNames...) //nolint:staticcheck
		for _, n := range missing {
			names = append(names, types.QueueAttributeName(n))
		}
		cp.AttributeNames = names //nolint:staticcheck
		return &cp
	}
	names := append([]types.MessageSystemAttributeName(nil), p.MessageSystemAttributeNames...)
	for _, n := range missing {
		names = append(names, types.MessageSystemAttributeName(n))
	}
	cp.MessageSystemAttributeNames = names
	return &cp
}

// -- spans ------------------------------------------------------------------------------------------

// emitter emits the spans of one call: every span shares the call's interval and ticks.
type emitter struct {
	r        *runtime
	c        *call
	end      time.Time
	returned int64
}

func (e emitter) span(name, queueURL string, attrs []attribute.KeyValue) {
	e.r.emit(name, queueURL, attrs, e.c, e.end, e.returned)
}

func finish(r *runtime, c *call, params, result interface{}, err error, end time.Time, returned int64) {
	e := emitter{r, c, end, returned}
	switch p := params.(type) {
	case *sqs.SendMessageInput:
		if res, ok := result.(*sqs.SendMessageOutput); ok && res != nil && err == nil {
			onSend(e, p, res)
		}
	case *sqs.SendMessageBatchInput:
		if res, ok := result.(*sqs.SendMessageBatchOutput); ok && res != nil && err == nil {
			onSendBatch(e, p, res)
		}
	case *sqs.ReceiveMessageInput:
		if res, ok := result.(*sqs.ReceiveMessageOutput); ok && res != nil && err == nil {
			onReceive(e, p, res)
		}
	case *sqs.PurgeQueueInput:
		onPurge(e, p, result, err)
	default:
		onSettle(e, params, result, err)
	}
}

func onSend(e emitter, p *sqs.SendMessageInput, res *sqs.SendMessageOutput) {
	id := str(res.MessageId)
	if id == "" {
		return
	}
	a := []attribute.KeyValue{attribute.String("messaging.message.id", id)}
	a = contents(a, p.MessageBody)
	a = fifo(a, str(p.MessageGroupId), str(res.SequenceNumber))
	e.span(spanSend, str(p.QueueUrl), a)
}

func onSendBatch(e emitter, p *sqs.SendMessageBatchInput, res *sqs.SendMessageBatchOutput) {
	entries := map[string]types.SendMessageBatchRequestEntry{}
	for _, x := range p.Entries {
		entries[str(x.Id)] = x
	}
	// Only entries SQS accepted: a failed entry has no message ID and produces no span (section 4.1).
	for _, ok := range res.Successful {
		id := str(ok.MessageId)
		if id == "" {
			continue
		}
		a := []attribute.KeyValue{attribute.String("messaging.message.id", id)}
		a = putString(a, "trailbeam.batch_entry_id", str(ok.Id))
		if sent, found := entries[str(ok.Id)]; found { // the body comes from the request entry with the same Id
			a = contents(a, sent.MessageBody)
			a = fifo(a, str(sent.MessageGroupId), str(ok.SequenceNumber))
		}
		e.span(spanSend, str(p.QueueUrl), a)
	}
}

func onReceive(e emitter, p *sqs.ReceiveMessageInput, res *sqs.ReceiveMessageOutput) {
	for _, m := range res.Messages {
		handle, id := str(m.ReceiptHandle), str(m.MessageId)
		if handle == "" || id == "" {
			continue
		}
		e.r.handles.put(handle, id)
		attrs := m.Attributes
		a := []attribute.KeyValue{attribute.String("messaging.message.id", id)}
		a = hashAttrs(a, e.r, handle)
		a = putInt(a, "trailbeam.receive_count", attrs["ApproximateReceiveCount"])
		a = putInt(a, "trailbeam.sent_timestamp_ms", attrs["SentTimestamp"])
		a = putInt(a, "trailbeam.first_receive_timestamp_ms", attrs["ApproximateFirstReceiveTimestamp"])
		a = putString(a, "trailbeam.aws_trace_header", attrs["AWSTraceHeader"])
		a = putString(a, "trailbeam.dead_letter_queue_source_arn", attrs["DeadLetterQueueSourceArn"])
		a = fifo(a, attrs["MessageGroupId"], attrs["SequenceNumber"])
		a = contents(a, m.Body)
		e.span(spanReceive, str(p.QueueUrl), a)
	}
}

// onPurge emits one span per PurgeQueue call, ok or failed (3.3, section 18).
func onPurge(e emitter, p *sqs.PurgeQueueInput, result interface{}, err error) {
	code := callError(result, err)
	a := []attribute.KeyValue{attribute.String("trailbeam.result", resultOf(code))}
	a = putString(a, "trailbeam.error_code", plainCode(code))
	e.span(spanPurge, str(p.QueueUrl), a)
}

// settle is one receipt handle (or batch entry) of a settle call.
type settle struct {
	entryID, handle   string
	visibilityTimeout *int32
}

func onSettle(e emitter, params, result interface{}, err error) {
	var (
		name, queueURL string
		batch          bool
		entries        []settle
		okIDs          = map[string]bool{}
		failed         = map[string]string{}
	)
	collectFailed := func(fs []types.BatchResultErrorEntry) {
		for _, f := range fs {
			code := str(f.Code)
			if code == "" {
				code = "Failed"
			}
			failed[str(f.Id)] = code
		}
	}
	switch p := params.(type) {
	case *sqs.DeleteMessageInput:
		name, queueURL = spanDelete, str(p.QueueUrl)
		entries = append(entries, settle{handle: str(p.ReceiptHandle)})
	case *sqs.DeleteMessageBatchInput:
		name, queueURL, batch = spanDelete, str(p.QueueUrl), true
		for _, x := range p.Entries {
			entries = append(entries, settle{entryID: str(x.Id), handle: str(x.ReceiptHandle)})
		}
		if res, ok := result.(*sqs.DeleteMessageBatchOutput); ok && res != nil {
			for _, s := range res.Successful {
				okIDs[str(s.Id)] = true
			}
			collectFailed(res.Failed)
		}
	case *sqs.ChangeMessageVisibilityInput:
		v := p.VisibilityTimeout
		name, queueURL = spanChangeVisibility, str(p.QueueUrl)
		entries = append(entries, settle{handle: str(p.ReceiptHandle), visibilityTimeout: &v})
	case *sqs.ChangeMessageVisibilityBatchInput:
		name, queueURL, batch = spanChangeVisibility, str(p.QueueUrl), true
		for _, x := range p.Entries {
			v := x.VisibilityTimeout
			entries = append(entries, settle{entryID: str(x.Id), handle: str(x.ReceiptHandle), visibilityTimeout: &v})
		}
		if res, ok := result.(*sqs.ChangeMessageVisibilityBatchOutput); ok && res != nil {
			for _, s := range res.Successful {
				okIDs[str(s.Id)] = true
			}
			collectFailed(res.Failed)
		}
	default:
		return
	}
	// If the call failed as a whole (an error response, or an error before any response), every handle in
	// it gets a failed span (section 4.3).
	whole := callError(result, err)
	for _, s := range entries {
		if s.handle == "" {
			continue
		}
		code := whole
		if code == "" && batch && !okIDs[s.entryID] {
			code = failed[s.entryID]
			if code == "" {
				code = "MissingFromResponse"
			}
		}
		code = plainCode(code)
		a := hashAttrs(nil, e.r, s.handle)
		a = putString(a, "messaging.message.id", e.r.handles.get(s.handle))
		if batch {
			a = putString(a, "trailbeam.batch_entry_id", s.entryID)
		}
		if name == spanChangeVisibility && s.visibilityTimeout != nil {
			a = append(a, attribute.Int64("trailbeam.visibility_timeout", int64(*s.visibilityTimeout))) // 0 is a real value
		}
		a = append(a, attribute.String("trailbeam.result", resultOf(code)))
		a = putString(a, "trailbeam.error_code", code)
		e.span(name, queueURL, a)
		if code == "" && name == spanDelete {
			e.r.handles.remove(s.handle)
		}
	}
}

// -- attribute helpers --------------------------------------------------------------------------------

// callError is the error code of a whole call, or "" if SQS answered with success: the service's error
// code, else the name of the innermost error's type.
func callError(result interface{}, err error) string {
	if err == nil {
		if result == nil || reflect.ValueOf(result).IsNil() {
			return "NoResponse"
		}
		return ""
	}
	var api smithy.APIError
	if errors.As(err, &api) {
		if code := api.ErrorCode(); code != "" {
			return code
		}
		return typeName(api)
	}
	switch {
	case errors.Is(err, context.Canceled):
		return "Canceled"
	case errors.Is(err, context.DeadlineExceeded):
		return "DeadlineExceeded"
	}
	inner := err
	for i := 0; i < 16; i++ {
		u := errors.Unwrap(inner)
		if u == nil {
			break
		}
		inner = u
	}
	return typeName(inner)
}

func typeName(err error) string {
	t := reflect.TypeOf(err)
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if n := t.Name(); n != "" && n != "errorString" {
		return n
	}
	return "Error"
}

// plainCode drops a protocol namespace (com.amazonaws.sqs#QueueDoesNotExist), section 18.
func plainCode(code string) string {
	return code[strings.LastIndex(code, "#")+1:]
}

func resultOf(code string) string {
	if code == "" {
		return "ok"
	}
	return "failed"
}

func hashAttrs(a []attribute.KeyValue, r *runtime, handle string) []attribute.KeyValue {
	return append(a, attribute.String("trailbeam.receipt_handle.hmac", r.hasher.hash(handle)),
		attribute.String("trailbeam.receipt_handle.key_id", r.hasher.keyID))
}

// contents: 3.2 (section 15), the body's size; capture is off.
func contents(a []attribute.KeyValue, body *string) []attribute.KeyValue {
	if body == nil {
		return a
	}
	return append(a, attribute.Int64("messaging.message.body.size", int64(len(*body))),
		attribute.String("trailbeam.body.capture_status", "off"))
}

func fifo(a []attribute.KeyValue, groupID, sequenceNumber string) []attribute.KeyValue {
	a = putString(a, "trailbeam.message_group_id", groupID)
	return putString(a, "trailbeam.sequence_number", sequenceNumber)
}

// putString omits an absent value, never sending "" (section 3).
func putString(a []attribute.KeyValue, name, value string) []attribute.KeyValue {
	if value == "" {
		return a
	}
	return append(a, attribute.String(name, value))
}

func putInt(a []attribute.KeyValue, name, value string) []attribute.KeyValue {
	v, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
	if err != nil {
		return a
	}
	return append(a, attribute.Int64(name, v))
}

func str(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}
