package dev.trailbeam.sqs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequestEntry;
import software.amazon.awssdk.services.sqs.model.MessageSystemAttributeName;
import software.amazon.awssdk.services.sqs.model.ReceiptHandleIsInvalidException;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequestEntry;

/**
 * The interceptor on a real SDK client over a canned HTTP layer. The interceptor is loaded the way users get it:
 * from {@code execution.interceptors} on the classpath, with no registration in these tests.
 */
class InterceptorTest {
  static final String KEY = "test-key-0123456789abcdef";
  static final ObjectMapper JSON = new ObjectMapper();

  InMemorySpanExporter spans;
  ReceiptHasher hasher = new ReceiptHasher(KEY);

  @BeforeEach
  void setUp() {
    spans = InMemorySpanExporter.create();
    Map<String, String> env = new HashMap<>();
    env.put("TRAILBEAM_RECEIPT_HASH_KEY", KEY);
    env.put("OTEL_SERVICE_NAME", "orders-worker");
    Trailbeam.install(env::get, SimpleSpanProcessor.create(spans));
  }

  static String str(SpanData s, String k) {
    return s.getAttributes().get(AttributeKey.stringKey(k));
  }

  static Long num(SpanData s, String k) {
    return s.getAttributes().get(AttributeKey.longKey(k));
  }

  /** A stub that answers the scenario's calls on one queue. */
  static StubSqs.Res answer(StubSqs.Req r) {
    switch (r.operation) {
      case "SendMessage":
        try {
          String body = JSON.readTree(r.body).get("MessageBody").asText();
          return StubSqs.Res.ok("{\"MessageId\":\"m1\",\"MD5OfMessageBody\":\"" + StubSqs.md5(body) + "\"}");
        } catch (IOException e) {
          throw new IllegalStateException(e);
        }
      case "SendMessageBatch":
        return StubSqs.Res.ok("{\"Successful\":[{\"Id\":\"e2\",\"MessageId\":\"m2\",\"MD5OfMessageBody\":\""
            + StubSqs.md5("{\"n\": 2}") + "\"}],\"Failed\":[{\"Id\":\"e3\",\"Code\":\"InvalidParameterValue\","
            + "\"SenderFault\":true}]}");
      case "ReceiveMessage":
        return StubSqs.Res.ok("{\"Messages\":[{\"MessageId\":\"m1\",\"ReceiptHandle\":\"rh-1\",\"MD5OfBody\":\""
            + StubSqs.md5("{\"n\": 1}") + "\",\"Body\":\"{\\\"n\\\": 1}\",\"Attributes\":{"
            + "\"ApproximateReceiveCount\":\"2\",\"SentTimestamp\":\"1700000000000\","
            + "\"ApproximateFirstReceiveTimestamp\":\"1700000000500\","
            + "\"DeadLetterQueueSourceArn\":\"arn:aws:sqs:us-east-1:000000000000:orders-src\"}}]}");
      case "DeleteMessage":
        if (r.body.contains("not-a-handle")) {
          return StubSqs.Res.error("com.amazonaws.sqs#ReceiptHandleIsInvalid", "ReceiptHandleIsInvalid;Sender");
        }
        return StubSqs.Res.ok("{}");
      case "DeleteMessageBatch":
        return StubSqs.Res.ok("{\"Successful\":[{\"Id\":\"d1\"}],\"Failed\":[{\"Id\":\"bogus\","
            + "\"Code\":\"ReceiptHandleIsInvalid\",\"SenderFault\":true}]}");
      case "ChangeMessageVisibility":
        return StubSqs.Res.ok("{}");
      case "ChangeMessageVisibilityBatch":
        return StubSqs.Res.ok("{\"Successful\":[{\"Id\":\"v1\"}],\"Failed\":[]}");
      default:
        return StubSqs.Res.ok("{}");
    }
  }

  @Test
  void sendReceiveSettle() throws Exception {
    StubSqs stub = new StubSqs(InterceptorTest::answer);
    try (SqsClient sqs = stub.client()) {
      sqs.sendMessage(b -> b.queueUrl(StubSqs.URL).messageBody("{\"n\": 1}"));
      sqs.sendMessageBatch(b -> b.queueUrl(StubSqs.URL).entries(
          SendMessageBatchRequestEntry.builder().id("e2").messageBody("{\"n\": 2}").build(),
          SendMessageBatchRequestEntry.builder().id("e3").messageBody("{\"n\": 3}").build()));
      sqs.receiveMessage(b -> b.queueUrl(StubSqs.URL).messageSystemAttributeNames(MessageSystemAttributeName.SENDER_ID));
      sqs.changeMessageVisibility(b -> b.queueUrl(StubSqs.URL).receiptHandle("rh-1").visibilityTimeout(0));
      sqs.deleteMessageBatch(b -> b.queueUrl(StubSqs.URL).entries(
          DeleteMessageBatchRequestEntry.builder().id("d1").receiptHandle("rh-1").build(),
          DeleteMessageBatchRequestEntry.builder().id("bogus").receiptHandle("not-a-handle").build()));
      assertThrows(ReceiptHandleIsInvalidException.class,
          () -> sqs.deleteMessage(b -> b.queueUrl(StubSqs.URL).receiptHandle("not-a-handle")));
    }
    List<SpanData> all = spans.getFinishedSpanItems();
    assertEquals(List.of("trailbeam send", "trailbeam send", "trailbeam receive", "trailbeam change_visibility",
        "trailbeam delete", "trailbeam delete", "trailbeam delete"),
        all.stream().map(SpanData::getName).collect(Collectors.toList()));

    for (SpanData s : all) {
      assertEquals("trailbeam", s.getInstrumentationScopeInfo().getName());
      assertNotNull(s.getInstrumentationScopeInfo().getVersion());
      assertEquals(SpanKind.INTERNAL, s.getKind());
      assertEquals(3L, num(s, "trailbeam.schema_version"));
      assertEquals("client", str(s, "trailbeam.source"));
      assertEquals("aws.sqs", str(s, "messaging.system.name"));
      assertEquals("orders", str(s, "messaging.destination.name"));
      assertEquals(StubSqs.URL, str(s, "aws.sqs.queue.url"));
      assertEquals(s.getResource().getAttribute(AttributeKey.stringKey("service.instance.id")),
          str(s, "messaging.client.id"));
      assertEquals("orders-worker", s.getResource().getAttribute(AttributeKey.stringKey("service.name")));
      assertEquals(ProcessHandle.current().pid(), num(s, "process.pid"));
      assertEquals(Thread.currentThread().getName(), str(s, "thread.name"));
      assertEquals(CallOrder.scope(), str(s, "trailbeam.order.scope"));
      assertTrue(num(s, "trailbeam.order.issued") < num(s, "trailbeam.order.returned"));
      assertTrue(s.getStartEpochNanos() <= s.getEndEpochNanos());
      assertFalse(s.getParentSpanContext().isValid(), "no stock instrumentation here: a root span");
      for (AttributeKey<?> k : s.getAttributes().asMap().keySet()) {
        Object v = s.getAttributes().get(k);
        assertFalse(v.toString().isEmpty(), k + " is empty");
        assertFalse(v.toString().contains("rh-1") || v.toString().contains(KEY), k + " exports a handle or the key");
      }
    }

    SpanData send = all.get(0);
    assertEquals("SendMessage", str(send, "messaging.operation.name"));
    assertEquals("send", str(send, "messaging.operation.type"));
    assertEquals("sqs.send", str(send, "trailbeam.kind"));
    assertEquals("m1", str(send, "messaging.message.id"));
    assertEquals(8L, num(send, "messaging.message.body.size"));
    assertEquals("off", str(send, "trailbeam.body.capture_status"));
    assertNull(str(send, "trailbeam.batch_entry_id"));

    SpanData batchSend = all.get(1); // e3 failed: no span for it
    assertEquals("m2", str(batchSend, "messaging.message.id"));
    assertEquals("e2", str(batchSend, "trailbeam.batch_entry_id"));

    SpanData receive = all.get(2);
    assertEquals("sqs.receive", str(receive, "trailbeam.kind"));
    assertEquals("m1", str(receive, "messaging.message.id"));
    assertEquals(2L, num(receive, "trailbeam.receive_count"));
    assertEquals(1700000000000L, num(receive, "trailbeam.sent_timestamp_ms"));
    assertEquals(1700000000500L, num(receive, "trailbeam.first_receive_timestamp_ms"));
    assertEquals("arn:aws:sqs:us-east-1:000000000000:orders-src", str(receive, "trailbeam.dead_letter_queue_source_arn"));
    assertEquals(hasher.hash("rh-1"), str(receive, "trailbeam.receipt_handle.hmac"));
    assertEquals(hasher.keyId(), str(receive, "trailbeam.receipt_handle.key_id"));
    assertEquals("off", str(receive, "trailbeam.body.capture_status"));

    // The receive asked for the attributes Trailbeam needs, keeping the caller's SenderId.
    JsonNode asked = JSON.readTree(stub.requests.get(2).body).get("MessageSystemAttributeNames");
    List<String> names = new java.util.ArrayList<>();
    asked.forEach(n -> names.add(n.asText()));
    assertEquals("SenderId", names.get(0));
    assertTrue(names.containsAll(TrailbeamSqsExecutionInterceptor.RECEIVE_SYSTEM_ATTRIBUTES), names.toString());

    SpanData cv = all.get(3);
    assertEquals("settle", str(cv, "messaging.operation.type"));
    assertEquals("sqs.settle", str(cv, "trailbeam.kind"));
    assertEquals(0L, num(cv, "trailbeam.visibility_timeout"));
    assertEquals("ok", str(cv, "trailbeam.result"));
    assertEquals("m1", str(cv, "messaging.message.id"), "this process received the handle (section 8)");

    SpanData d1 = all.get(4);
    SpanData bogus = all.get(5);
    assertEquals("d1", str(d1, "trailbeam.batch_entry_id"));
    assertEquals("ok", str(d1, "trailbeam.result"));
    assertEquals("m1", str(d1, "messaging.message.id"));
    assertEquals("bogus", str(bogus, "trailbeam.batch_entry_id"));
    assertEquals("failed", str(bogus, "trailbeam.result"));
    assertEquals("ReceiptHandleIsInvalid", str(bogus, "trailbeam.error_code"));
    assertNull(str(bogus, "messaging.message.id"));
    assertEquals(num(d1, "trailbeam.order.issued"), num(bogus, "trailbeam.order.issued"), "one call, one interval");
    assertEquals(d1.getStartEpochNanos(), bogus.getStartEpochNanos());

    SpanData failedDelete = all.get(6);
    assertEquals("failed", str(failedDelete, "trailbeam.result"));
    assertEquals("ReceiptHandleIsInvalid", str(failedDelete, "trailbeam.error_code"));
    assertEquals(hasher.hash("not-a-handle"), str(failedDelete, "trailbeam.receipt_handle.hmac"));

    // Consecutive calls: returned(n) < issued(n + 1), end(n) <= start(n + 1).
    for (int i = 1; i < all.size(); i++) {
      SpanData a = all.get(i - 1);
      SpanData b = all.get(i);
      if (num(a, "trailbeam.order.issued").equals(num(b, "trailbeam.order.issued"))) {
        continue;
      }
      assertTrue(num(a, "trailbeam.order.returned") < num(b, "trailbeam.order.issued"));
      assertTrue(a.getEndEpochNanos() <= b.getStartEpochNanos());
    }
  }

  @Test
  void legacyAttributeNamesStayLegacy() throws Exception {
    StubSqs stub = new StubSqs(InterceptorTest::answer);
    try (SqsClient sqs = stub.client()) {
      sqs.receiveMessage(b -> b.queueUrl(StubSqs.URL).attributeNamesWithStrings("SenderId"));
    }
    JsonNode body = JSON.readTree(stub.requests.get(0).body);
    assertNull(body.get("MessageSystemAttributeNames"), body.toString());
    List<String> names = new java.util.ArrayList<>();
    body.get("AttributeNames").forEach(n -> names.add(n.asText()));
    assertEquals("SenderId", names.get(0));
    assertTrue(names.containsAll(TrailbeamSqsExecutionInterceptor.RECEIVE_SYSTEM_ATTRIBUTES));
  }

  @Test
  void allIsLeftAlone() throws Exception {
    StubSqs stub = new StubSqs(InterceptorTest::answer);
    try (SqsClient sqs = stub.client()) {
      sqs.receiveMessage(b -> b.queueUrl(StubSqs.URL).messageSystemAttributeNames(MessageSystemAttributeName.ALL));
    }
    JsonNode asked = JSON.readTree(stub.requests.get(0).body).get("MessageSystemAttributeNames");
    assertEquals(1, asked.size());
    assertEquals("All", asked.get(0).asText());
  }

  @Test
  void handRegisteredOnTopOfTheGlobalOneEmitsOnce() {
    StubSqs stub = new StubSqs(InterceptorTest::answer);
    try (SqsClient sqs = stub.client(new TrailbeamSqsExecutionInterceptor())) {
      sqs.sendMessage(b -> b.queueUrl(StubSqs.URL).messageBody("{\"n\": 1}"));
    }
    assertEquals(1, spans.getFinishedSpanItems().size());
  }

  /** Puts a W3C trace header on the request, as stock instrumentation does in modifyHttpRequest. */
  static final class StockHeader implements ExecutionInterceptor {
    @Override
    public SdkHttpRequest modifyHttpRequest(Context.ModifyHttpRequest context, ExecutionAttributes ea) {
      return context.httpRequest().toBuilder()
          .putHeader("traceparent", "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01").build();
    }
  }

  @Test
  void parentFromTheStockTraceHeader() {
    StubSqs stub = new StubSqs(InterceptorTest::answer);
    try (SqsClient sqs = stub.client(new StockHeader())) {
      sqs.sendMessage(b -> b.queueUrl(StubSqs.URL).messageBody("{\"n\": 1}"));
    }
    SpanData s = spans.getFinishedSpanItems().get(0);
    assertEquals("0af7651916cd43dd8448eb211c80319c", s.getTraceId());
    assertEquals("b7ad6b7169203331", s.getParentSpanId());
  }

  @Test
  void otherOperationsAndServicesAreIgnored() {
    StubSqs stub = new StubSqs(r -> StubSqs.Res.ok("{\"QueueUrl\":\"" + StubSqs.URL + "\"}"));
    try (SqsClient sqs = stub.client()) {
      sqs.getQueueUrl(b -> b.queueName("orders"));
      sqs.createQueue(b -> b.queueName("orders"));
    }
    assertEquals(0, spans.getFinishedSpanItems().size());
  }

  static final class EndpointConnectionError extends IOException {
    EndpointConnectionError() {
      super("no response");
    }
  }

  /** The purge vectors of contract section 18: exactly these attributes, and the ones the process decides. */
  @Test
  void purgeVectors() throws Exception {
    JsonNode purge = ContractVectorsTest.vectors().get("purge");
    for (JsonNode c : purge.get("cases")) {
      setUp();
      JsonNode response = c.get("response");
      Function<StubSqs.Req, StubSqs.Res> handler;
      if (response.has("thrown")) {
        assertEquals("EndpointConnectionError", response.get("thrown").asText());
        handler = r -> StubSqs.Res.thrown(new EndpointConnectionError());
      } else if (response.has("error_code")) {
        String code = response.get("error_code").asText();
        handler = r -> StubSqs.Res.error(code, null);
      } else {
        handler = r -> StubSqs.Res.ok("{}");
      }
      StubSqs stub = new StubSqs(handler);
      try (SqsClient sqs = stub.client()) {
        sqs.purgeQueue(b -> b.queueUrl(c.get("queue_url").asText()));
      } catch (RuntimeException expected) {
        assertFalse(response.has("ok"), c.get("name").asText());
      }
      List<SpanData> out = spans.getFinishedSpanItems();
      String name = c.get("name").asText();
      assertEquals(1, out.size(), name);
      SpanData s = out.get(0);
      assertEquals(purge.get("span_name").asText(), s.getName(), name);
      Iterator<Map.Entry<String, JsonNode>> it = c.get("attributes").fields();
      while (it.hasNext()) {
        Map.Entry<String, JsonNode> e = it.next();
        Object got = e.getValue().isInt()
            ? num(s, e.getKey())
            : str(s, e.getKey());
        Object want = e.getValue().isInt() ? (Object) e.getValue().asLong() : e.getValue().asText();
        assertEquals(want, got, name + ": " + e.getKey());
      }
      for (JsonNode p : purge.get("present")) {
        assertTrue(s.getAttributes().asMap().keySet().stream().anyMatch(k -> k.getKey().equals(p.asText())),
            name + ": " + p.asText());
      }
      for (JsonNode a : c.get("absent")) {
        assertFalse(s.getAttributes().asMap().keySet().stream().anyMatch(k -> k.getKey().equals(a.asText())),
            name + ": " + a.asText());
      }
      assertTrue(num(s, "trailbeam.order.issued") < num(s, "trailbeam.order.returned"));
    }
  }
}
