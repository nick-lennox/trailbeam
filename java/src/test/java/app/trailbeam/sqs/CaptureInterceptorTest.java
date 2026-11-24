package app.trailbeam.sqs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.awscore.retry.AwsRetryStrategy;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequestEntry;

/** Section 15 through a real SDK client: body capture, tenant, and the receive's message attribute names. */
class CaptureInterceptorTest {
  static final ObjectMapper JSON = new ObjectMapper();
  static final String BODY = "{\"customer\": {\"id\": \"acme\"}, \"password\": \"hunter2\"}";
  static final String REDACTED = "{\"customer\": {\"id\": \"acme\"}, \"password\": \"[REDACTED]\"}";

  InMemorySpanExporter spans;

  @BeforeEach
  void setUp() {
    spans = InMemorySpanExporter.create();
    Map<String, String> env = new HashMap<>();
    env.put("TRAILBEAM_RECEIPT_HASH_KEY", InterceptorTest.KEY);
    env.put("TRAILBEAM_CAPTURE_BODY_QUEUES", "orders");
    env.put("TRAILBEAM_TENANT_ATTRIBUTE", "tenant");
    env.put("TRAILBEAM_TENANT_BODY_PATH", "customer.id");
    Trailbeam.install(env::get, SimpleSpanProcessor.create(spans));
  }

  static StubSqs.Res answer(StubSqs.Req r) {
    switch (r.operation) {
      case "SendMessage":
        return StubSqs.Res.ok("{\"MessageId\":\"m1\",\"MD5OfMessageBody\":\"" + StubSqs.md5(BODY) + "\"}");
      case "SendMessageBatch":
        return StubSqs.Res.ok("{\"Successful\":[{\"Id\":\"e1\",\"MessageId\":\"m2\",\"MD5OfMessageBody\":\""
            + StubSqs.md5("{\"customer\": {\"id\": \"c1\"}}") + "\"}],\"Failed\":[{\"Id\":\"e2\","
            + "\"Code\":\"InvalidParameterValue\",\"SenderFault\":true}]}");
      case "ReceiveMessage":
        return StubSqs.Res.ok("{\"Messages\":[{\"MessageId\":\"m1\",\"ReceiptHandle\":\"rh-1\",\"MD5OfBody\":\""
            + StubSqs.md5(BODY) + "\",\"Body\":" + StubSqs.json(BODY) + ",\"Attributes\":{},"
            + "\"MessageAttributes\":{\"tenant\":{\"DataType\":\"String\",\"StringValue\":\"globex\"}}}]}");
      default:
        return StubSqs.Res.ok("{}");
    }
  }

  /** The stub's client, without the SDK's own MD5 checks (the stub sends no MD5 of message attributes). */
  static SqsClient client(StubSqs stub) {
    return SqsClient.builder()
        .httpClient(stub)
        .region(Region.US_EAST_1)
        .endpointOverride(URI.create("http://localhost:9324"))
        .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("x", "x")))
        .checksumValidationEnabled(false)
        .overrideConfiguration(c -> c.retryStrategy(AwsRetryStrategy.doNotRetry()))
        .build();
  }

  @Test
  void sendBatchAndReceiveCaptureTheRedactedBodyAndTenant() throws Exception {
    StubSqs stub = new StubSqs(CaptureInterceptorTest::answer);
    try (SqsClient sqs = client(stub)) {
      sqs.sendMessage(b -> b.queueUrl(StubSqs.URL).messageBody(BODY).messageAttributes(Map.of("tenant",
          MessageAttributeValue.builder().dataType("String").stringValue("globex").build())));
      sqs.sendMessageBatch(b -> b.queueUrl(StubSqs.URL).entries(
          SendMessageBatchRequestEntry.builder().id("e2").messageBody("{\"customer\": {\"id\": \"c2\"}}").build(),
          SendMessageBatchRequestEntry.builder().id("e1").messageBody("{\"customer\": {\"id\": \"c1\"}}").build()));
      sqs.receiveMessage(b -> b.queueUrl(StubSqs.URL).messageAttributeNames("color"));
    }
    List<SpanData> all = spans.getFinishedSpanItems();
    assertEquals(3, all.size());

    SpanData send = all.get(0);
    assertEquals("globex", InterceptorTest.str(send, "trailbeam.tenant"), "the attribute is tried first");
    assertEquals("message_attribute", InterceptorTest.str(send, "trailbeam.tenant.source"));
    assertEquals("captured", InterceptorTest.str(send, "trailbeam.body.capture_status"));
    assertEquals(REDACTED, InterceptorTest.str(send, "trailbeam.body"));
    assertEquals("json", InterceptorTest.str(send, "trailbeam.body.format"));
    assertEquals(1L, InterceptorTest.num(send, "trailbeam.body.redacted_count"));
    assertEquals((long) BODY.length(), InterceptorTest.num(send, "messaging.message.body.size"));

    SpanData batch = all.get(1); // only the accepted entry, with the body of the request entry with its Id
    assertEquals("m2", InterceptorTest.str(batch, "messaging.message.id"));
    assertEquals("{\"customer\": {\"id\": \"c1\"}}", InterceptorTest.str(batch, "trailbeam.body"));
    assertEquals("c1", InterceptorTest.str(batch, "trailbeam.tenant"));
    assertEquals("body_path", InterceptorTest.str(batch, "trailbeam.tenant.source"));

    SpanData receive = all.get(2);
    assertEquals("trailbeam receive", receive.getName());
    assertEquals("globex", InterceptorTest.str(receive, "trailbeam.tenant"));
    assertEquals(REDACTED, InterceptorTest.str(receive, "trailbeam.body"));
    assertEquals("captured", InterceptorTest.str(receive, "trailbeam.body.capture_status"));

    // The receive asked for the tenant attribute and the Extended Client's size attributes, after the caller's.
    List<String> asked = new ArrayList<>();
    JSON.readTree(stub.requests.get(2).body).get("MessageAttributeNames").forEach(n -> asked.add(n.asText()));
    assertEquals(List.of("color", "tenant", "ExtendedPayloadSize", "SQSLargePayloadSize"), asked);
  }

  @Test
  void allMessageAttributesAreLeftAlone() throws Exception {
    for (String all : new String[] {"All", ".*"}) {
      StubSqs stub = new StubSqs(CaptureInterceptorTest::answer);
      try (SqsClient sqs = client(stub)) {
        sqs.receiveMessage(b -> b.queueUrl(StubSqs.URL).messageAttributeNames(all));
      }
      JsonNode asked = JSON.readTree(stub.requests.get(0).body).get("MessageAttributeNames");
      assertEquals(1, asked.size(), all);
      assertEquals(all, asked.get(0).asText());
    }
  }

  @Test
  void otherQueuesSayOff() {
    StubSqs stub = new StubSqs(CaptureInterceptorTest::answer);
    try (SqsClient sqs = client(stub)) {
      sqs.sendMessage(b -> b.queueUrl("http://localhost:9324/000000000000/payments").messageBody(BODY));
    }
    SpanData send = spans.getFinishedSpanItems().get(0);
    assertEquals("off", InterceptorTest.str(send, "trailbeam.body.capture_status"));
    assertNull(InterceptorTest.str(send, "trailbeam.body"));
    assertEquals("acme", InterceptorTest.str(send, "trailbeam.tenant"), "the tenant does not need capture");
  }
}
