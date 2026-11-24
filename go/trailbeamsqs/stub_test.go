package trailbeamsqs

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/smithy-go/middleware"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// stubSQS is a canned SQS endpoint at the HTTP layer: a real sqs.Client runs its whole middleware stack
// (serialize, sign, retry, deserialize) against it. Each handler gets the decoded JSON request.
type stubSQS struct {
	mu       sync.Mutex
	requests []stubRequest
	handlers map[string]func(req map[string]interface{}) (int, interface{})
	// fail, when set, is returned instead of any response (an error before any response).
	fail error
}

type stubRequest struct {
	op      string
	body    map[string]interface{}
	headers http.Header
}

func newStub() *stubSQS {
	return &stubSQS{handlers: map[string]func(map[string]interface{}) (int, interface{}){}}
}

func (s *stubSQS) on(op string, h func(req map[string]interface{}) (int, interface{})) *stubSQS {
	s.handlers[op] = h
	return s
}

func (s *stubSQS) Do(r *http.Request) (*http.Response, error) {
	op := strings.TrimPrefix(r.Header.Get("X-Amz-Target"), "AmazonSQS.")
	raw, _ := io.ReadAll(r.Body)
	var body map[string]interface{}
	_ = json.Unmarshal(raw, &body)
	s.mu.Lock()
	s.requests = append(s.requests, stubRequest{op, body, r.Header.Clone()})
	fail := s.fail
	h := s.handlers[op]
	s.mu.Unlock()
	if fail != nil {
		return nil, fail
	}
	status, out := 200, interface{}(map[string]interface{}{})
	if h != nil {
		status, out = h(body)
	}
	b, _ := json.Marshal(out)
	return &http.Response{StatusCode: status, Header: http.Header{"Content-Type": {"application/x-amz-json-1.0"}},
		Body: io.NopCloser(bytes.NewReader(b)), ContentLength: int64(len(b)), Request: r}, nil
}

func (s *stubSQS) ops() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []string
	for _, r := range s.requests {
		out = append(out, r.op)
	}
	return out
}

func (s *stubSQS) last(op string) stubRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := len(s.requests) - 1; i >= 0; i-- {
		if s.requests[i].op == op {
			return s.requests[i]
		}
	}
	return stubRequest{}
}

func sqsError(code string) (int, interface{}) {
	return 400, map[string]interface{}{"__type": code, "message": "stub error " + code}
}

func md5hex(s string) string {
	sum := md5.Sum([]byte(s))
	return hex.EncodeToString(sum[:])
}

// client builds an sqs.Client on the stub, with Trailbeam's middleware and any extra API options.
func (s *stubSQS) client(extra ...func(*middleware.Stack) error) *sqs.Client {
	var apiOptions []func(*middleware.Stack) error
	AppendMiddlewares(&apiOptions)
	apiOptions = append(apiOptions, extra...)
	return sqs.New(sqs.Options{
		Region:           "us-east-1",
		BaseEndpoint:     aws.String("http://sqs.stub"),
		Credentials:      aws.AnonymousCredentials{},
		HTTPClient:       s,
		APIOptions:       apiOptions,
		RetryMaxAttempts: 1,
	})
}

// installTest replaces the runtime with one whose spans go to an in-memory exporter.
func installTest(t *testing.T, env map[string]string) *tracetest.InMemoryExporter {
	t.Helper()
	exp := tracetest.NewInMemoryExporter()
	r := newRuntime(func(k string) string { return env[k] }, sdktrace.NewSimpleSpanProcessor(exp))
	mu.Lock()
	old := instance
	instance = r
	mu.Unlock()
	t.Cleanup(func() {
		_ = r.provider.Shutdown(context.Background())
		mu.Lock()
		instance = old
		mu.Unlock()
	})
	return exp
}

// attrs is a span's attributes as plain Go values (int64, string, bool).
func attrs(s tracetest.SpanStub) map[string]interface{} {
	m := map[string]interface{}{}
	for _, kv := range s.Attributes {
		m[string(kv.Key)] = kv.Value.AsInterface()
	}
	return m
}

func spansNamed(exp *tracetest.InMemoryExporter, name string) []tracetest.SpanStub {
	var out []tracetest.SpanStub
	for _, s := range exp.GetSpans() {
		if s.Name == name {
			out = append(out, s)
		}
	}
	return out
}

func describe(s tracetest.SpanStub) string { return fmt.Sprintf("%s %v", s.Name, attrs(s)) }
