package trailbeamsqs

import (
	"context"
	"crypto/rand"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// Version is the package version, which is also the instrumentation scope version.
const Version = "0.1.0-dev"

const (
	schemaVersion       = 3
	scopeName           = "trailbeam"
	defaultKey          = "trailbeam-default-receipt-hash-key"
	defaultEndpoint     = "http://localhost:4318/v1/traces"
	minKeyBytes         = 16
	defaultMaxQueueSize = 16384

	spanSend             = "trailbeam send"
	spanReceive          = "trailbeam receive"
	spanDelete           = "trailbeam delete"
	spanChangeVisibility = "trailbeam change_visibility"
	spanPurge            = "trailbeam purge"
)

// runtime is Trailbeam's state in this process: its own always-on tracer provider (contract section 2), the
// receipt-hash key (section 7), the consumer identity (section 5) and the handle map (section 8).
type runtime struct {
	provider *sdktrace.TracerProvider
	tracer   trace.Tracer
	hasher   *receiptHasher
	clientID string
	endpoint string
	handles  *handleMap
	pid      int64
}

var (
	mu       sync.Mutex
	instance *runtime
	warned   sync.Map
)

// get returns the runtime, started on the first SQS call.
func get() *runtime {
	mu.Lock()
	defer mu.Unlock()
	if instance == nil {
		instance = newRuntime(os.Getenv, nil)
	}
	return instance
}

// Flush exports every span emitted so far, or returns ctx's error when ctx ends first.
func Flush(ctx context.Context) error {
	mu.Lock()
	r := instance
	mu.Unlock()
	if r == nil {
		return nil
	}
	return r.provider.ForceFlush(ctx)
}

// Shutdown flushes and stops the tracer provider. Call it before the program exits: spans wait up to five
// seconds in memory before they are exported. A later SQS call starts a new provider.
func Shutdown(ctx context.Context) error {
	mu.Lock()
	r := instance
	instance = nil
	mu.Unlock()
	if r == nil {
		return nil
	}
	return r.provider.Shutdown(ctx)
}

// newRuntime builds the runtime from env. Tests pass their own processor; nil means the OTLP exporter.
func newRuntime(env func(string) string, processor sdktrace.SpanProcessor) *runtime {
	res := buildResource(env)
	r := &runtime{
		endpoint: nonEmpty(env("TRAILBEAM_OTLP_TRACES_ENDPOINT"), defaultEndpoint),
		hasher:   hasherFromKey(env("TRAILBEAM_RECEIPT_HASH_KEY")),
		handles:  newHandleMap(handleMaxEntries, handleMaxAge, time.Now),
		pid:      int64(os.Getpid()),
	}
	for _, kv := range res.Attributes() {
		if kv.Key == "service.instance.id" {
			r.clientID = kv.Value.AsString()
		}
	}
	if processor == nil {
		exp, err := otlptracehttp.New(context.Background(), otlptracehttp.WithEndpointURL(r.endpoint))
		if err != nil {
			warnOnce("exporter", "trailbeam: cannot export spans to "+r.endpoint+": "+err.Error())
		} else {
			processor = sdktrace.NewBatchSpanProcessor(exp,
				sdktrace.WithMaxQueueSize(positiveInt(env("TRAILBEAM_MAX_QUEUE_SIZE"), defaultMaxQueueSize)))
		}
	}
	opts := []sdktrace.TracerProviderOption{sdktrace.WithSampler(sdktrace.AlwaysSample()), sdktrace.WithResource(res)}
	if processor != nil {
		opts = append(opts, sdktrace.WithSpanProcessor(processor))
	}
	r.provider = sdktrace.NewTracerProvider(opts...)
	r.tracer = r.provider.Tracer(scopeName, trace.WithInstrumentationVersion(Version))
	return r
}

// emit emits one Trailbeam span with the attributes every Trailbeam span carries (section 3) and attrs on
// top. The span is the interval [c.start, end] of the SQS call (section 2), with that call's ticks (2a).
func (r *runtime) emit(name, queueURL string, attrs []attribute.KeyValue, c *call, end time.Time, returned int64) {
	all := make([]attribute.KeyValue, 0, len(attrs)+14)
	all = append(all,
		attribute.Int64("trailbeam.schema_version", schemaVersion),
		attribute.String("trailbeam.kind", kind(name)),
		attribute.String("trailbeam.source", "client"),
		attribute.String("messaging.system.name", "aws.sqs"),
		attribute.String("messaging.operation.type", operationType(name)),
		attribute.String("messaging.operation.name", c.operation),
		attribute.String("messaging.client.id", r.clientID),
		attribute.Int64("process.pid", r.pid),
		attribute.String("trailbeam.order.scope", order.scope),
		attribute.Int64("trailbeam.order.issued", c.issued),
		attribute.Int64("trailbeam.order.returned", returned),
	)
	if queueURL != "" {
		all = append(all, attribute.String("aws.sqs.queue.url", queueURL))
		if q := queueName(queueURL); q != "" {
			all = append(all, attribute.String("messaging.destination.name", q))
		}
	}
	all = append(all, attrs...)
	start := c.start
	if end.Before(start) {
		start = end
	}
	opts := []trace.SpanStartOption{trace.WithSpanKind(trace.SpanKindInternal), trace.WithTimestamp(start),
		trace.WithAttributes(all...)}
	parent := context.Background()
	if p := c.getParent(); p.IsValid() {
		parent = trace.ContextWithSpanContext(parent, p)
	} else {
		opts = append(opts, trace.WithNewRoot())
	}
	_, span := r.tracer.Start(parent, name, opts...)
	span.End(trace.WithTimestamp(end))
}

func kind(spanName string) string {
	switch spanName {
	case spanSend:
		return "sqs.send"
	case spanReceive:
		return "sqs.receive"
	case spanPurge:
		return "sqs.purge"
	default:
		return "sqs.settle"
	}
}

func operationType(spanName string) string {
	switch spanName {
	case spanSend:
		return "send"
	case spanReceive:
		return "receive"
	case spanPurge:
		return "purge" // a custom value: the semantic conventions define none for a purge
	default:
		return "settle"
	}
}

// queueName is the last path segment of the queue URL (section 3).
func queueName(u string) string {
	u = strings.TrimRight(u, "/")
	return u[strings.LastIndex(u, "/")+1:]
}

// -- configuration ----------------------------------------------------------------------------------

func hasherFromKey(key string) *receiptHasher {
	switch {
	case key == "":
		warnOnce("default-key", "trailbeam: no receipt-hash key configured (TRAILBEAM_RECEIPT_HASH_KEY); using the "+
			"published default key, which gives no protection. Set the same key on every service that uses these queues.")
		key = defaultKey
	case len(key) < minKeyBytes:
		warnOnce("short-key", fmt.Sprintf("trailbeam: the receipt-hash key is shorter than %d bytes", minKeyBytes))
	}
	return newReceiptHasher(key)
}

// buildResource reads OTEL_RESOURCE_ATTRIBUTES, then OTEL_SERVICE_NAME (default unknown_service, section 5),
// and adds a random service.instance.id unless one is configured.
func buildResource(env func(string) string) *resource.Resource {
	values := map[string]string{}
	var keys []string
	set := func(k, v string) {
		if _, ok := values[k]; !ok {
			keys = append(keys, k)
		}
		values[k] = v
	}
	for _, pair := range strings.Split(env("OTEL_RESOURCE_ATTRIBUTES"), ",") {
		k, v, ok := strings.Cut(pair, "=")
		k, v = strings.TrimSpace(k), strings.TrimSpace(v)
		if !ok || k == "" {
			continue
		}
		if d, err := url.PathUnescape(v); err == nil {
			v = d
		}
		if v != "" {
			set(k, v)
		}
	}
	if s := strings.TrimSpace(env("OTEL_SERVICE_NAME")); s != "" {
		set("service.name", s)
	}
	if s := values["service.name"]; s == "" || strings.HasPrefix(s, "unknown_service") {
		set("service.name", "unknown_service")
	}
	if values["service.instance.id"] == "" {
		set("service.instance.id", newUUID())
	}
	kvs := []attribute.KeyValue{
		attribute.String("telemetry.sdk.language", "go"),
		attribute.String("telemetry.sdk.name", "opentelemetry"),
	}
	for _, k := range keys {
		kvs = append(kvs, attribute.String(k, values[k]))
	}
	return resource.NewSchemaless(kvs...)
}

// newUUID returns a random UUID v4.
func newUUID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

func positiveInt(raw string, dflt int) int {
	v, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil || v <= 0 {
		return dflt
	}
	return v
}

func nonEmpty(v, dflt string) string {
	if v = strings.TrimSpace(v); v == "" {
		return dflt
	}
	return v
}

func warnOnce(tag, msg string) {
	if _, seen := warned.LoadOrStore(tag, true); !seen {
		slog.Warn(msg)
	}
}

func infoOnce(tag, msg string) {
	if _, seen := warned.LoadOrStore(tag, true); !seen {
		slog.Info(msg)
	}
}
