package app.trailbeam.sqs;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Contract section 15 against the {@code capture} vectors that Python and Node.js are tested against. */
class CaptureTest {
  static List<String> strings(JsonNode n) {
    List<String> out = new ArrayList<>();
    if (n != null) {
      n.forEach(x -> out.add(x.asText()));
    }
    return out;
  }

  /** The vector's options, as Python's resolve_config(tenant, capture) reads them. */
  static Capture.Config config(JsonNode tenant, JsonNode capture) {
    tenant = tenant == null ? com.fasterxml.jackson.databind.node.NullNode.getInstance() : tenant;
    capture = capture == null ? com.fasterxml.jackson.databind.node.NullNode.getInstance() : capture;
    return new Capture.Config(
        tenant.hasNonNull("message_attribute") ? tenant.get("message_attribute").asText() : null,
        tenant.hasNonNull("body_path") ? tenant.get("body_path").asText() : null,
        new HashSet<>(strings(capture.get("body_queues"))),
        new HashSet<>(strings(capture.get("error_queues"))),
        strings(capture.get("redact_keys")),
        capture.has("redact_patterns") ? strings(capture.get("redact_patterns")) : null,
        capture.hasNonNull("max_body_bytes") ? capture.get("max_body_bytes").asInt() : null);
  }

  /** Message attributes as {name: StringValue} (client: StringValue, Lambda record: stringValue). */
  static Map<String, String> attributes(JsonNode attrs, boolean lambdaRecord) {
    Map<String, String> out = new HashMap<>();
    for (Iterator<Map.Entry<String, JsonNode>> it = attrs.fields(); it.hasNext(); ) {
      Map.Entry<String, JsonNode> a = it.next();
      JsonNode v = a.getValue().get(lambdaRecord ? "stringValue" : "StringValue");
      if (v != null && v.isTextual()) {
        out.put(a.getKey(), v.asText());
      }
    }
    return out;
  }

  static JsonNode capture() throws Exception {
    return ContractVectorsTest.vectors().get("capture");
  }

  @Test
  void redactionVectors() throws Exception {
    int n = 0;
    for (JsonNode v : capture().get("redaction")) {
      String name = v.get("name").asText();
      Capture.Redacted r = Capture.redactBody(v.get("body").asText(), config(null, v.get("capture")));
      assertEquals(v.get("redacted").asText(), r.text, name);
      assertEquals(v.get("redacted_count").asInt(), r.count, name);
      assertEquals(v.get("format").asText(), r.format, name);
      n++;
    }
    assertTrue(n >= 17);
  }

  @Test
  void tenantVectors() throws Exception {
    for (JsonNode v : capture().get("tenant")) {
      boolean lambda = v.get("lambda_record").asBoolean();
      String[] got = Capture.tenant(config(v.get("tenant"), null), attributes(v.get("message_attributes"), lambda),
          v.get("body").asText());
      JsonNode want = v.get("expected");
      if (want.isNull()) {
        assertNull(got, v.get("name").asText());
      } else {
        assertArrayEquals(new String[] {want.get("tenant").asText(), want.get("source").asText()}, got,
            v.get("name").asText());
      }
    }
  }

  @Test
  void offloadedSizeVectors() throws Exception {
    for (JsonNode v : capture().get("offloaded_size")) {
      boolean lambda = v.get("lambda_record").asBoolean();
      Map<String, Object> got = Capture.contentAttrs(Capture.Config.off(), "q", v.get("body").asText(),
          attributes(v.get("message_attributes"), lambda), null);
      JsonNode want = v.get("expected");
      assertEquals(want.isNull() ? null : want.asLong(), got.get("trailbeam.body.offloaded_size"),
          v.get("name").asText());
      assertEquals("off", got.get("trailbeam.body.capture_status"));
    }
  }

  @Test
  void errorMessageVectors() throws Exception {
    for (JsonNode v : capture().get("error_message")) {
      String got = Capture.errorMessage(config(null, v.get("capture")), v.get("queue").asText(),
          v.get("message").asText());
      JsonNode want = v.get("expected");
      assertEquals(want.isNull() ? null : want.asText(), got, v.get("name").asText());
    }
  }

  @Test
  void environmentAndBadValues() {
    Map<String, String> env = new HashMap<>();
    env.put("TRAILBEAM_CAPTURE_BODY_QUEUES", "orders, payments");
    env.put("TRAILBEAM_TENANT_ATTRIBUTE", "tenant");
    env.put("TRAILBEAM_CAPTURE_MAX_BYTES", "100");
    Capture.Config c = Capture.Config.fromEnv(env::get);
    assertTrue(c.bodyOn("orders") && c.bodyOn("payments") && !c.bodyOn("other"));
    assertEquals("tenant", c.tenantAttribute);
    assertEquals(100, c.maxBodyBytes);
    assertNull(c.invalid);
    assertEquals(4, c.redactPatterns.size());

    env.put("TRAILBEAM_REDACT_PATTERNS", "");
    assertEquals(0, Capture.Config.fromEnv(env::get).redactPatterns.size(), "set but empty: no patterns");

    // Python and Node.js refuse these at install time. The Java package has no install call, so it fails
    // closed: a listed queue's body is not exported, and says why.
    for (String[] bad : new String[][] {
        {"TRAILBEAM_REDACT_KEYS", "a b"}, {"TRAILBEAM_REDACT_PATTERNS", "nope"},
        {"TRAILBEAM_CAPTURE_MAX_BYTES", "262145"}, {"TRAILBEAM_CAPTURE_MAX_BYTES", "lots"}}) {
      Map<String, String> e = new HashMap<>(env);
      e.remove("TRAILBEAM_REDACT_PATTERNS");
      e.put(bad[0], bad[1]);
      Capture.Config b = Capture.Config.fromEnv(e::get);
      assertNotNull(b.invalid, bad[0] + "=" + bad[1]);
      Map<String, Object> got = Capture.contentAttrs(b, "orders", "password=x", null, null);
      assertEquals("redaction_failed", got.get("trailbeam.body.capture_status"), bad[0]);
      assertFalse(got.containsKey("trailbeam.body"));
      assertNull(Capture.errorMessage(b, "orders", "boom"));
    }
  }

  @Test
  void statuses() {
    Capture.Config c = new Capture.Config(null, null, new HashSet<>(List.of("q")), new HashSet<>(),
        List.of(), null, 100);
    assertEquals("off", Capture.contentAttrs(c, "other", "x", null, null).get("trailbeam.body.capture_status"));
    assertEquals("md5_mismatch",
        Capture.contentAttrs(c, "q", "x", null, StubSqs.md5("y")).get("trailbeam.body.capture_status"));
    assertEquals("captured",
        Capture.contentAttrs(c, "q", "x", null, StubSqs.md5("x").toUpperCase()).get("trailbeam.body.capture_status"));
    assertEquals("exceeds_capture_limit",
        Capture.contentAttrs(c, "q", "y".repeat(101), null, null).get("trailbeam.body.capture_status"));
    String deep = "[".repeat(200) + "]".repeat(200);
    assertEquals("redaction_failed", Capture.contentAttrs(c, "q", deep, null, null).get("trailbeam.body.capture_status"));

    Map<String, Object> sns = Capture.contentAttrs(c, "q",
        "{\"Type\":\"Notification\",\"TopicArn\":\"t\",\"Message\":\"m\"}", null, null);
    assertEquals("json", sns.get("trailbeam.body.format"));
    assertEquals(0L, sns.get("trailbeam.body.redacted_count"));
    assertEquals("sns", sns.get("trailbeam.body.envelope"));
    assertEquals("eventbridge", Capture.contentAttrs(c, "q",
        "{\"detail-type\":\"x\",\"detail\":{},\"source\":\"s\"}", null, null).get("trailbeam.body.envelope"));
  }

  @Test
  void sizesAndNumbers() {
    assertEquals(2L, Capture.utf8Length("\u00e9"));
    assertEquals(4L, Capture.utf8Length("\ud83d\ude00"));
    assertEquals(3L, Capture.utf8Length("\ud800"), "a lone surrogate counts as Python's surrogatepass does");
    assertEquals("12345678901234567890", Capture.numberText("12345678901234567890"));
    assertEquals("0", Capture.numberText("-0"));
    assertEquals("1.5", Capture.numberText("1.5"));
    assertEquals("Infinity", Capture.numberText("1E400"));
    assertEquals("-Infinity", Capture.numberText("-1E400"));
    assertEquals("9".repeat(256), Capture.numberText("9".repeat(256)));
    assertNull(Capture.numberText("9".repeat(257)), "longer than a tenant may be");
  }

  /** QV-548: a huge integer at the tenant path must not cost BigInteger's quadratic conversion. */
  @Test
  void hugeNumberAtTenantPathIsCheap() {
    Capture.Config c = new Capture.Config(null, "t", new HashSet<>(), new HashSet<>(), List.of(), null, null);
    String digits = "1".repeat(1_048_000);
    String body = "{\"t\":" + digits + "}"; // about 1 MiB, SQS's largest body
    String decimal = "{\"t\":1." + "0".repeat(1_047_990) + "}";
    Capture.contentAttrs(c, "q", "{\"t\":1}", null, null); // load and warm the code path once
    long best = Long.MAX_VALUE;
    for (int i = 0; i < 3; i++) {
      long t0 = System.nanoTime();
      Map<String, Object> attrs = Capture.contentAttrs(c, "q", body, null, null);
      best = Math.min(best, System.nanoTime() - t0);
      assertFalse(attrs.containsKey("trailbeam.tenant"), "more than 256 characters: no tenant, as before");
      assertEquals((long) body.length(), attrs.get("messaging.message.body.size"));
    }
    assertTrue(best < 50_000_000L, "content attributes took " + best / 1_000_000 + " ms");
    // A long decimal still prints as the double it is, and as quickly.
    long t0 = System.nanoTime();
    assertEquals("1.0", Capture.contentAttrs(c, "q", decimal, null, null).get("trailbeam.tenant"));
    assertTrue(System.nanoTime() - t0 < 200_000_000L);
  }

  @Test
  void loneSurrogateFailsClosed() {
    Capture.Config c = new Capture.Config(null, null, new HashSet<>(List.of("q")), new HashSet<>(),
        List.of(), null, null);
    // The escape decodes to a lone surrogate; redacting the string rewrites it, and that cannot be UTF-8.
    String body = "{\"note\": \"\\ud800 mail a@b.co\"}";
    assertEquals("redaction_failed", Capture.contentAttrs(c, "q", body, null, null).get("trailbeam.body.capture_status"));
    // Left as written, the escape is plain ASCII and exports fine.
    assertEquals("captured", Capture.contentAttrs(c, "q", "{\"note\": \"\\ud800\"}", null, null)
        .get("trailbeam.body.capture_status"));
  }
}
