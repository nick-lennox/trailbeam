package app.trailbeam.sqs;

import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.TraceState;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * The parent of a Trailbeam span, read from the trace headers that stock instrumentation (the OpenTelemetry
 * Java agent, dd-java-agent) put on the outgoing HTTP request. Read in {@code beforeTransmission}, after every
 * interceptor has modified the request, because the SDK runs global interceptors in {@code HashMap} order, not
 * classpath order (QV-312 report, section 3, point 2).
 */
final class TraceHeaders {
  private TraceHeaders() {}

  static SpanContext parent(Map<String, List<String>> headers) {
    SpanContext p = fromXray(first(headers, "x-amzn-trace-id"));
    if (p == null) {
      p = fromTraceparent(first(headers, "traceparent"));
    }
    return p;
  }

  static String first(Map<String, List<String>> headers, String name) {
    for (Map.Entry<String, List<String>> e : headers.entrySet()) {
      if (e.getKey() != null && e.getKey().toLowerCase(Locale.ROOT).equals(name)
          && e.getValue() != null && !e.getValue().isEmpty()) {
        return e.getValue().get(0);
      }
    }
    return null;
  }

  /** {@code Root=1-5759e988-bd862e3fe1be46a994272793;Parent=53995c3f42cd8ad8;Sampled=1}. */
  static SpanContext fromXray(String value) {
    if (value == null) {
      return null;
    }
    String root = null;
    String parent = null;
    for (String part : value.split(";")) {
      int eq = part.indexOf('=');
      if (eq < 0) {
        continue;
      }
      String k = part.substring(0, eq).trim();
      String v = part.substring(eq + 1).trim();
      if (k.equals("Root")) {
        root = v;
      } else if (k.equals("Parent")) {
        parent = v;
      }
    }
    if (root == null || parent == null || root.length() != 35 || !root.startsWith("1-") || root.charAt(10) != '-') {
      return null;
    }
    String traceId = (root.substring(2, 10) + root.substring(11)).toLowerCase(Locale.ROOT);
    SpanContext sc = SpanContext.createFromRemoteParent(
        traceId, parent.toLowerCase(Locale.ROOT), TraceFlags.getSampled(), TraceState.getDefault());
    return sc.isValid() ? sc : null;
  }

  /** W3C {@code 00-<trace id>-<span id>-<flags>}. */
  static SpanContext fromTraceparent(String value) {
    if (value == null) {
      return null;
    }
    String[] p = value.trim().split("-");
    if (p.length < 4 || p[1].length() != 32 || p[2].length() != 16) {
      return null;
    }
    SpanContext sc = SpanContext.createFromRemoteParent(
        p[1].toLowerCase(Locale.ROOT), p[2].toLowerCase(Locale.ROOT), TraceFlags.getSampled(), TraceState.getDefault());
    return sc.isValid() ? sc : null;
  }
}
