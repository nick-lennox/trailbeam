package app.trailbeam.sqs;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.amazonaws.services.lambda.runtime.ClientContext;
import com.amazonaws.services.lambda.runtime.CognitoIdentity;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSBatchResponse;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** The Lambda wrapper (contract sections 4.2, 4.4, 15.2 and 15.5), called the way the Lambda runtime calls it. */
class LambdaWrapperTest {
  static final String KEY = "test-key-0123456789abcdef";
  static final String ARN = "arn:aws:sqs:eu-west-1:123456789012:orders";
  static final String STREAM = "2026/11/24/[$LATEST]0123456789abcdef0123456789abcdef";

  InMemorySpanExporter spans;
  Map<String, String> env;
  ReceiptHasher hasher = new ReceiptHasher(KEY);

  /** What the test handlers saw and do; static because the runtime creates the handler by its class name. */
  static volatile Object nextResult;
  static volatile Throwable nextError;
  static volatile int receivesSeenByHandler;
  static InMemorySpanExporter current;

  @BeforeEach
  void setUp() {
    env = new HashMap<>();
    env.put("TRAILBEAM_RECEIPT_HASH_KEY", KEY);
    env.put("AWS_LAMBDA_LOG_STREAM_NAME", STREAM);
    env.put("AWS_LAMBDA_FUNCTION_NAME", "orders-fn-from-env");
    install();
    nextResult = null;
    nextError = null;
    receivesSeenByHandler = -1;
  }

  /** A fresh exporter each time: replacing the runtime shuts the old one's exporter down. */
  void install() {
    spans = InMemorySpanExporter.create();
    current = spans;
    Trailbeam.install(env::get, SimpleSpanProcessor.create(spans));
  }

  static void handlerRan() {
    receivesSeenByHandler = (int) current.getFinishedSpanItems().stream()
        .filter(s -> s.getName().equals("trailbeam receive")).count();
  }

  static Object respond() throws Throwable {
    handlerRan();
    if (nextError != null) {
      throw nextError;
    }
    return nextResult;
  }

  /** {@code RequestHandler<SQSEvent, SQSBatchResponse>}, named in TRAILBEAM_LAMBDA_HANDLER. */
  public static class Typed implements RequestHandler<SQSEvent, SQSBatchResponse> {
    @Override
    public SQSBatchResponse handleRequest(SQSEvent event, Context context) {
      try {
        return (SQSBatchResponse) respond();
      } catch (Throwable t) {
        throw TrailbeamSqsLambdaWrapper.<RuntimeException>rethrow(t);
      }
    }
  }

  /** A POJO handler with a method that returns a Map and may throw a checked exception. */
  public static class Pojo {
    public Map<String, Object> handle(SQSEvent event, Context context) throws Throwable {
      @SuppressWarnings("unchecked")
      Map<String, Object> r = (Map<String, Object>) respond();
      return r;
    }

    public void oneArg(SQSEvent event) throws Throwable {
      respond();
    }
  }

  /** The base-class option. */
  public static class Based extends TrailbeamSqsHandler {
    @Override
    protected SQSBatchResponse handleSqsEvent(SQSEvent event, Context context) {
      try {
        return (SQSBatchResponse) respond();
      } catch (Throwable t) {
        throw TrailbeamSqsLambdaWrapper.<RuntimeException>rethrow(t);
      }
    }
  }

  TrailbeamSqsLambdaWrapper wrapper(String handler) {
    env.put("TRAILBEAM_LAMBDA_HANDLER", handler);
    return new TrailbeamSqsLambdaWrapper(env::get);
  }

  static SQSEvent.SQSMessage record(String id, String body, int receiveCount) {
    SQSEvent.SQSMessage m = new SQSEvent.SQSMessage();
    m.setMessageId(id);
    m.setReceiptHandle("rh-" + id);
    m.setBody(body);
    m.setMd5OfBody(StubSqs.md5(body));
    m.setEventSource("aws:sqs");
    m.setEventSourceArn(ARN);
    m.setAwsRegion("eu-west-1");
    Map<String, String> a = new HashMap<>();
    a.put("ApproximateReceiveCount", String.valueOf(receiveCount));
    a.put("SentTimestamp", "1700000000000");
    a.put("ApproximateFirstReceiveTimestamp", "1700000000500");
    m.setAttributes(a);
    m.setMessageAttributes(new HashMap<>());
    return m;
  }

  static SQSEvent event(SQSEvent.SQSMessage... records) {
    SQSEvent e = new SQSEvent();
    e.setRecords(Arrays.asList(records));
    return e;
  }

  static SQSBatchResponse failures(String... ids) {
    List<SQSBatchResponse.BatchItemFailure> l = new ArrayList<>();
    for (String id : ids) {
      l.add(new SQSBatchResponse.BatchItemFailure(id));
    }
    return new SQSBatchResponse(l);
  }

  static String str(SpanData s, String k) {
    return s.getAttributes().get(AttributeKey.stringKey(k));
  }

  static Long num(SpanData s, String k) {
    return s.getAttributes().get(AttributeKey.longKey(k));
  }

  List<SpanData> named(String name) {
    return spans.getFinishedSpanItems().stream().filter(s -> s.getName().equals(name)).collect(Collectors.toList());
  }

  @Test
  void successWithNoListEmitsReceivesBeforeTheHandlerAndOutcomesAfter() {
    SQSBatchResponse r = wrapper(Typed.class.getName()).handleRequest(
        event(record("m1", "{\"n\":1}", 1), record("m2", "hello", 3)), new Ctx("req-1"));
    assertNull(r);
    assertEquals(2, receivesSeenByHandler, "every receive is exported before the handler runs");

    List<SpanData> receives = named("trailbeam receive");
    List<SpanData> outcomes = named("trailbeam outcome");
    assertEquals(2, receives.size());
    assertEquals(2, outcomes.size());
    for (int i = 0; i < 2; i++) {
      SpanData s = receives.get(i);
      assertEquals("sqs.receive", str(s, "trailbeam.kind"));
      assertEquals("lambda", str(s, "trailbeam.source"));
      assertEquals("LambdaEvent", str(s, "messaging.operation.name"));
      assertEquals("receive", str(s, "messaging.operation.type"));
      assertEquals("orders", str(s, "messaging.destination.name"));
      assertEquals("https://sqs.eu-west-1.amazonaws.com/123456789012/orders", str(s, "aws.sqs.queue.url"));
      assertEquals(ARN, str(s, "trailbeam.queue.arn"));
      assertEquals(STREAM, str(s, "messaging.client.id"));
      assertEquals("orders-fn", str(s, "faas.name"));
      assertEquals("req-1", str(s, "faas.invocation_id"));
      assertEquals(2L, num(s, "trailbeam.batch.size"));
      assertEquals((long) i, num(s, "trailbeam.batch.index"));
      assertEquals(Boolean.FALSE, s.getAttributes().get(AttributeKey.booleanKey("trailbeam.start_observed")));
      assertEquals(s.getStartEpochNanos(), s.getEndEpochNanos(), "zero-length: its start is not observed");
      assertNull(num(s, "trailbeam.order.issued"));
      assertEquals(hasher.hash("rh-m" + (i + 1)), str(s, "trailbeam.receipt_handle.hmac"));
      assertEquals(hasher.keyId(), str(s, "trailbeam.receipt_handle.key_id"));
      assertEquals(1700000000000L, num(s, "trailbeam.sent_timestamp_ms"));
      assertEquals(1700000000500L, num(s, "trailbeam.first_receive_timestamp_ms"));
      assertEquals("off", str(s, "trailbeam.body.capture_status"));
      assertNull(str(s, "trailbeam.body"));
      assertFalse(s.getParentSpanContext().isValid(), "no active span: a root span");
    }
    assertEquals(1L, num(receives.get(0), "trailbeam.receive_count"));
    assertEquals(3L, num(receives.get(1), "trailbeam.receive_count"));
    assertEquals(7L, num(receives.get(0), "messaging.message.body.size"));
    assertEquals(num(receives.get(0), "trailbeam.order.returned"), num(receives.get(1), "trailbeam.order.returned"));

    for (int i = 0; i < 2; i++) {
      SpanData s = outcomes.get(i);
      assertEquals("sqs.outcome", str(s, "trailbeam.kind"));
      assertEquals("process", str(s, "messaging.operation.type"));
      assertEquals("success", str(s, "trailbeam.outcome"));
      assertEquals(0L, num(s, "trailbeam.batch.reported_failure_count"));
      assertEquals((long) i, num(s, "trailbeam.batch.index"));
      assertEquals(num(s, "trailbeam.order.issued"), num(s, "trailbeam.order.returned"));
      assertTrue(num(s, "trailbeam.order.issued") > num(receives.get(0), "trailbeam.order.returned"));
      assertEquals(str(receives.get(0), "trailbeam.order.scope"), str(s, "trailbeam.order.scope"));
      assertEquals(s.getStartEpochNanos(), s.getEndEpochNanos());
      assertNull(s.getAttributes().get(AttributeKey.booleanKey("trailbeam.start_observed")));
      assertNull(str(s, "trailbeam.error_type"));
    }
  }

  @Test
  void reportedFailuresAreFailedReportedAndCounted() {
    nextResult = failures("m2");
    SQSBatchResponse r = wrapper(Typed.class.getName()).handleRequest(
        event(record("m1", "a", 1), record("m2", "b", 1)), new Ctx("req-2"));
    assertSame(nextResult, r, "the handler's response reaches Lambda unchanged");
    Map<String, String> got = outcomesById();
    assertEquals("success", got.get("m1"));
    assertEquals("failed_reported", got.get("m2"));
    for (SpanData s : named("trailbeam outcome")) {
      assertEquals(1L, num(s, "trailbeam.batch.reported_failure_count"));
    }
  }

  @Test
  void anUnknownItemIdentifierFailsTheWholeBatchWithNoCount() {
    nextResult = failures("not-a-record-of-this-batch");
    wrapper(Typed.class.getName()).handleRequest(event(record("m1", "a", 1), record("m2", "b", 1)), new Ctx("r"));
    assertEquals(Arrays.asList("failed_invalid_response", "failed_invalid_response"),
        new ArrayList<>(outcomesById().values()));
    for (SpanData s : named("trailbeam outcome")) {
      assertNull(num(s, "trailbeam.batch.reported_failure_count"));
    }
  }

  @Test
  void anEmptyListIsSuccessAndANullIdentifierIsInvalid() {
    nextResult = new SQSBatchResponse(Collections.emptyList());
    wrapper(Typed.class.getName()).handleRequest(event(record("m1", "a", 1)), new Ctx("r"));
    assertEquals("success", outcomesById().get("m1"));
    spans.reset();
    nextResult = failures((String) null);
    wrapper(Typed.class.getName()).handleRequest(event(record("m1", "a", 1)), new Ctx("r"));
    assertEquals("failed_invalid_response", outcomesById().get("m1"));
  }

  @Test
  void withoutReportBatchItemFailuresTheListIsIgnored() {
    env.put("TRAILBEAM_LAMBDA_REPORT_BATCH_ITEM_FAILURES", "false");
    nextResult = failures("m1");
    wrapper(Typed.class.getName()).handleRequest(event(record("m1", "a", 1)), new Ctx("r"));
    assertEquals("success", outcomesById().get("m1"));
  }

  @Test
  void aThrowIsFailedExceptionWithTheClassNameAndIsRethrownAsItIs() {
    RuntimeException boom = new IllegalArgumentException("card 4111 1111 1111 1111 declined");
    nextError = boom;
    TrailbeamSqsLambdaWrapper w = wrapper(Typed.class.getName());
    RuntimeException thrown = assertThrows(RuntimeException.class,
        () -> w.handleRequest(event(record("m1", "a", 1), record("m2", "b", 1)), new Ctx("r")));
    assertSame(boom, thrown);
    for (SpanData s : named("trailbeam outcome")) {
      assertEquals("failed_exception", str(s, "trailbeam.outcome"));
      assertEquals("java.lang.IllegalArgumentException", str(s, "trailbeam.error_type"));
      assertNull(str(s, "trailbeam.error_message"), "error text is off by default");
      assertNull(num(s, "trailbeam.batch.reported_failure_count"));
    }
  }

  @Test
  void errorTextIsRecordedRedactedForListedQueues() {
    env.put("TRAILBEAM_CAPTURE_ERROR_QUEUES", "orders");
    env.put("TRAILBEAM_CAPTURE_BODY_QUEUES", "*");
    install();
    nextError = new IllegalStateException("card 4111 1111 1111 1111 declined");
    TrailbeamSqsLambdaWrapper w = wrapper(Typed.class.getName());
    assertThrows(IllegalStateException.class, () -> w.handleRequest(event(record("m1", "{\"n\":1}", 1)), new Ctx("r")));
    SpanData o = named("trailbeam outcome").get(0);
    assertEquals("card [REDACTED] declined", str(o, "trailbeam.error_message"));
    SpanData r = named("trailbeam receive").get(0);
    assertEquals("captured", str(r, "trailbeam.body.capture_status"));
    assertEquals("{\"n\":1}", str(r, "trailbeam.body"));
    assertEquals("json", str(r, "trailbeam.body.format"));
  }

  @Test
  void aPojoMethodCanReturnAMapAndThrowACheckedException() {
    Map<String, Object> resp = new HashMap<>();
    resp.put("batchItemFailures", Collections.singletonList(Collections.singletonMap("itemIdentifier", "m1")));
    nextResult = resp;
    SQSBatchResponse r = wrapper(Pojo.class.getName() + "::handle")
        .handleRequest(event(record("m1", "a", 1), record("m2", "b", 1)), new Ctx("r"));
    assertEquals("m1", r.getBatchItemFailures().get(0).getItemIdentifier());
    assertEquals("failed_reported", outcomesById().get("m1"));
    assertEquals("success", outcomesById().get("m2"));

    spans.reset();
    nextResult = null;
    nextError = new IOException("disk");
    TrailbeamSqsLambdaWrapper w = wrapper(Pojo.class.getName() + "::oneArg");
    Throwable thrown = assertThrows(Throwable.class, () -> w.handleRequest(event(record("m1", "a", 1)), new Ctx("r")));
    assertSame(nextError, thrown, "a checked exception reaches Lambda as it is, not wrapped");
    assertEquals("java.io.IOException", str(named("trailbeam outcome").get(0), "trailbeam.error_type"));
  }

  @Test
  void theBaseClassRecordsTheSameSpans() {
    nextResult = failures("m1");
    SQSBatchResponse r = new Based().handleRequest(event(record("m1", "a", 2)), new Ctx("req-b"));
    assertSame(nextResult, r);
    assertEquals(1, receivesSeenByHandler);
    assertEquals("failed_reported", outcomesById().get("m1"));
    assertEquals("req-b", str(named("trailbeam receive").get(0), "faas.invocation_id"));
  }

  @Test
  void anEventWithoutSqsRecordsIsPassedThroughWithNoSpans() {
    SQSEvent.SQSMessage other = record("m1", "a", 1);
    other.setEventSource("aws:kinesis");
    wrapper(Typed.class.getName()).handleRequest(event(other), new Ctx("r"));
    assertEquals(0, receivesSeenByHandler);
    assertTrue(spans.getFinishedSpanItems().isEmpty());
  }

  @Test
  void theFunctionNameFallsBackToTheEnvironmentAndTheClientIdToTheInstanceId() {
    env.remove("AWS_LAMBDA_LOG_STREAM_NAME");
    Ctx ctx = new Ctx("r");
    ctx.functionName = null;
    wrapper(Typed.class.getName()).handleRequest(event(record("m1", "a", 1)), ctx);
    SpanData s = named("trailbeam receive").get(0);
    assertEquals("orders-fn-from-env", str(s, "faas.name"));
    assertEquals(Trailbeam.get().clientId, str(s, "messaging.client.id"));
  }

  @Test
  void theXrayHeaderOfTheInvocationIsTheParent() {
    System.setProperty("com.amazonaws.xray.traceHeader",
        "Root=1-5759e988-bd862e3fe1be46a994272793;Parent=53995c3f42cd8ad8;Sampled=1");
    try {
      wrapper(Typed.class.getName()).handleRequest(event(record("m1", "a", 1)), new Ctx("r"));
    } finally {
      System.clearProperty("com.amazonaws.xray.traceHeader");
    }
    SpanData s = named("trailbeam receive").get(0);
    assertEquals("5759e988bd862e3fe1be46a994272793", s.getTraceId());
    assertEquals("53995c3f42cd8ad8", s.getParentSpanId());
  }

  @Test
  void aMissingOrWrongHandlerFailsTheInitWithAPlainMessage() {
    env.remove("TRAILBEAM_LAMBDA_HANDLER");
    IllegalStateException e = assertThrows(IllegalStateException.class, () -> new TrailbeamSqsLambdaWrapper(env::get));
    assertTrue(e.getMessage().contains("TRAILBEAM_LAMBDA_HANDLER"));
    assertThrows(IllegalStateException.class, () -> wrapper("com.example.Missing"));
    assertThrows(IllegalStateException.class, () -> wrapper(Pojo.class.getName() + "::nope"));
    assertThrows(IllegalStateException.class, () -> wrapper(TrailbeamSqsLambdaWrapper.class.getName()));
  }

  @Test
  void queueFromArn() {
    assertArrayEquals(new String[] {"orders", "https://sqs.eu-west-1.amazonaws.com/123456789012/orders"},
        LambdaRecorder.queueFromArn(ARN));
    assertArrayEquals(new String[] {"weird", null}, LambdaRecorder.queueFromArn("not:an:arn:weird"));
    assertArrayEquals(new String[] {null, null}, LambdaRecorder.queueFromArn(null));
  }

  @Test
  void snapStartRestoreStartsANewScopeAndANewInstanceId() throws Exception {
    Trailbeam before = Trailbeam.get();
    String scope = CallOrder.scope();
    Object hook = SnapStart.proxy(org.crac.Resource.class);
    ((org.crac.Resource) hook).beforeCheckpoint(null);
    ((org.crac.Resource) hook).afterRestore(null);
    assertNotEquals(scope, CallOrder.scope());
    Trailbeam after = Trailbeam.get();
    assertTrue(before != after);
    assertNotEquals(before.clientId, after.clientId);
    SnapStart.register(); // org.crac is on the test classpath: registers without an error
  }

  Map<String, String> outcomesById() {
    Map<String, String> out = new java.util.LinkedHashMap<>();
    for (SpanData s : named("trailbeam outcome")) {
      out.put(str(s, "messaging.message.id"), str(s, "trailbeam.outcome"));
    }
    return out;
  }

  /** A Lambda context with a function name and request ID. */
  static final class Ctx implements Context {
    final String requestId;
    String functionName = "orders-fn";

    Ctx(String requestId) {
      this.requestId = requestId;
    }

    @Override public String getAwsRequestId() { return requestId; }
    @Override public String getLogGroupName() { return "/aws/lambda/orders-fn"; }
    @Override public String getLogStreamName() { return STREAM; }
    @Override public String getFunctionName() { return functionName; }
    @Override public String getFunctionVersion() { return "$LATEST"; }
    @Override public String getInvokedFunctionArn() { return "arn:aws:lambda:eu-west-1:123456789012:function:orders-fn"; }
    @Override public CognitoIdentity getIdentity() { return null; }
    @Override public ClientContext getClientContext() { return null; }
    @Override public int getRemainingTimeInMillis() { return 30000; }
    @Override public int getMemoryLimitInMB() { return 512; }
    @Override public LambdaLogger getLogger() { return null; }
  }
}
