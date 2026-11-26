package app.trailbeam.sqs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** QV-550: an unusable TRAILBEAM_OTLP_TRACES_ENDPOINT warns once and does not break the runtime (section 20.4). */
class EndpointTest {
  final List<LogRecord> records = new ArrayList<>();
  final Logger log = Logger.getLogger("trailbeam");
  final Handler handler = new Handler() {
    @Override
    public void publish(LogRecord r) {
      records.add(r);
    }

    @Override
    public void flush() {}

    @Override
    public void close() {}
  };

  @AfterEach
  void tearDown() {
    log.removeHandler(handler);
    Trailbeam.install(k -> null, SimpleSpanProcessor.create(InMemorySpanExporter.create()));
  }

  @Test
  void unusableEndpointWarnsOnceAndTheRuntimeIsBuilt() {
    log.addHandler(handler);
    for (String bad : new String[] {"not a url", "localhost:4318"}) {
      assertNull(Trailbeam.exporter(bad), bad);
      Map<String, String> env = new HashMap<>();
      env.put("TRAILBEAM_OTLP_TRACES_ENDPOINT", bad);
      // Built without throwing, so get() returns it and later SQS calls do not build it again.
      Trailbeam t = Trailbeam.install(env::get, null);
      assertSame(t, Trailbeam.get());
      assertEquals(bad, t.endpoint);
      t.tracer.spanBuilder("trailbeam send").startSpan().end(); // no processor: nothing to throw
    }
    List<String> warnings = new ArrayList<>();
    for (LogRecord r : records) {
      if (r.getLevel() == Level.WARNING && r.getMessage().contains("TRAILBEAM_OTLP_TRACES_ENDPOINT")) {
        warnings.add(r.getMessage());
      }
    }
    assertEquals(1, warnings.size(), warnings.toString());
    assertTrue(warnings.get(0).contains("\"not a url\""), warnings.get(0));
  }

  @Test
  void usableEndpoint() {
    assertNotNull(Trailbeam.exporter("http://localhost:4318/v1/traces"));
    assertNotNull(Trailbeam.exporter("https://collector.example/v1/traces"));
  }
}
