package trailbeamsqs

import (
	"strings"

	"go.opentelemetry.io/otel/trace"
)

// parentFromHeaders reads a parent from the request's trace headers when the context has no span: W3C
// traceparent (otelaws puts one there when the application set a propagator), else X-Amzn-Trace-Id (the
// SDK sets it from _X_AMZN_TRACE_ID inside Lambda). The zero SpanContext means none.
func parentFromHeaders(traceparent, xray string) trace.SpanContext {
	if p := fromTraceparent(traceparent); p.IsValid() {
		return p
	}
	return fromXray(xray)
}

// fromTraceparent parses 00-<trace id>-<span id>-<flags>.
func fromTraceparent(v string) trace.SpanContext {
	p := strings.Split(strings.TrimSpace(v), "-")
	if len(p) < 4 || len(p[1]) != 32 || len(p[2]) != 16 {
		return trace.SpanContext{}
	}
	return remote(p[1], p[2])
}

// fromXray parses Root=1-5759e988-bd862e3fe1be46a994272793;Parent=53995c3f42cd8ad8;Sampled=1.
func fromXray(v string) trace.SpanContext {
	var root, parent string
	for _, part := range strings.Split(v, ";") {
		k, val, ok := strings.Cut(part, "=")
		if !ok {
			continue
		}
		switch strings.TrimSpace(k) {
		case "Root":
			root = strings.TrimSpace(val)
		case "Parent":
			parent = strings.TrimSpace(val)
		}
	}
	if len(root) != 35 || !strings.HasPrefix(root, "1-") || root[10] != '-' || parent == "" {
		return trace.SpanContext{}
	}
	return remote(root[2:10]+root[11:], parent)
}

func remote(traceID, spanID string) trace.SpanContext {
	tid, err := trace.TraceIDFromHex(strings.ToLower(traceID))
	if err != nil {
		return trace.SpanContext{}
	}
	sid, err := trace.SpanIDFromHex(strings.ToLower(spanID))
	if err != nil {
		return trace.SpanContext{}
	}
	return trace.NewSpanContext(trace.SpanContextConfig{TraceID: tid, SpanID: sid,
		TraceFlags: trace.FlagsSampled, Remote: true})
}
