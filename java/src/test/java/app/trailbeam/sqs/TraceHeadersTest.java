package app.trailbeam.sqs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.opentelemetry.api.trace.SpanContext;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class TraceHeadersTest {
  @Test
  void xrayHeader() {
    SpanContext p = TraceHeaders.parent(Map.of("X-Amzn-Trace-Id",
        List.of("Root=1-5759e988-bd862e3fe1be46a994272793;Parent=53995c3f42cd8ad8;Sampled=1")));
    assertEquals("5759e988bd862e3fe1be46a994272793", p.getTraceId());
    assertEquals("53995c3f42cd8ad8", p.getSpanId());
  }

  @Test
  void traceparentWhenNoXray() {
    SpanContext p = TraceHeaders.parent(Map.of("traceparent",
        List.of("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01")));
    assertEquals("0af7651916cd43dd8448eb211c80319c", p.getTraceId());
    assertEquals("b7ad6b7169203331", p.getSpanId());
  }

  @Test
  void xrayWithoutParentIsNoParent() {
    assertNull(TraceHeaders.parent(Map.of("X-Amzn-Trace-Id", List.of("Root=1-5759e988-bd862e3fe1be46a994272793"))));
    assertNull(TraceHeaders.parent(Map.of("x-amzn-trace-id", List.of("garbage"))));
    assertNull(TraceHeaders.parent(Map.of()));
    assertNull(TraceHeaders.parent(Map.of("traceparent", List.of("00-00000000000000000000000000000000-b7ad6b7169203331-01"))));
  }
}
