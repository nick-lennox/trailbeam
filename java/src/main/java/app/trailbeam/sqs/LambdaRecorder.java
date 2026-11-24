package app.trailbeam.sqs;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.SQSBatchResponse;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.trace.SpanContext;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * The Lambda wrapper's spans (contract sections 4.2, 4.4 and 15.2), shared by {@link TrailbeamSqsHandler} and
 * {@link TrailbeamSqsLambdaWrapper}. For each SQS record it emits one {@code trailbeam receive} and flushes it
 * before the handler runs, so a timeout or out-of-memory kill cannot lose it. After the handler returns or
 * throws it emits one {@code trailbeam outcome} per record. A killed invocation has no outcome; Trailbeam infers
 * it.
 */
final class LambdaRecorder {
  static final String OPERATION = "LambdaEvent";
  static final String SUCCESS = "success";
  static final String FAILED_REPORTED = "failed_reported";
  static final String FAILED_INVALID_RESPONSE = "failed_invalid_response";
  static final String FAILED_EXCEPTION = "failed_exception";
  static final long FLUSH_TIMEOUT_MS = 2000;

  /** The user's handler, called once per invocation. */
  interface Handler {
    Object call(SQSEvent event, Context context) throws Throwable;
  }

  private final Function<String, String> env;

  LambdaRecorder(Function<String, String> env) {
    this.env = env;
  }

  /**
   * Runs {@code handler} on {@code event}, with the spans around it, and returns what it returned or throws what
   * it threw. Nothing the recorder does can fail the invocation: a failure to emit is logged at debug level.
   */
  Object invoke(Handler handler, SQSEvent event, Context context, boolean reportBatchItemFailures) throws Throwable {
    List<SQSEvent.SQSMessage> records = sqsRecords(event);
    if (records.isEmpty()) {
      return handler.call(event, context);
    }
    Trailbeam tb = null;
    SpanContext parent = null;
    try {
      tb = Trailbeam.get();
      parent = parent(env);
      receives(tb, records, context, parent);
    } catch (RuntimeException | LinkageError e) {
      debug("receive spans", e);
    }
    List<String> ids = new ArrayList<>(records.size());
    for (SQSEvent.SQSMessage r : records) {
      ids.add(r.getMessageId());
    }
    Object result;
    try {
      result = handler.call(event, context);
    } catch (Throwable err) {
      Map<String, String> outcomes = new HashMap<>();
      for (String id : ids) {
        outcomes.put(id, FAILED_EXCEPTION);
      }
      safeOutcomes(tb, records, context, outcomes, parent, err);
      throw err;
    }
    safeOutcomes(tb, records, context, decideOutcomes(ids, result, reportBatchItemFailures), parent, null);
    return result;
  }

  // -- spans -------------------------------------------------------------------------------------

  private void receives(Trailbeam tb, List<SQSEvent.SQSMessage> records, Context context, SpanContext parent) {
    String clientId = clientId();
    // Lambda's poller made the ReceiveMessage call, out of sight of the wrapper. All the wrapper knows is that it
    // happened no later than now (section 2): the span ends now, and its start is marked as not observed rather
    // than invented. Likewise the call order (section 2a): one "returned" tick, and no "issued" tick.
    long t = Trailbeam.nowNanos();
    CallOrder.Tick returned = CallOrder.tick();
    String thread = Thread.currentThread().getName();
    int size = records.size();
    for (int index = 0; index < size; index++) {
      SQSEvent.SQSMessage r = records.get(index);
      String arn = r.getEventSourceArn();
      String[] queue = queueFromArn(arn);
      Map<String, String> a = r.getAttributes() == null ? Collections.emptyMap() : r.getAttributes();
      AttributesBuilder b = Attributes.builder().put("messaging.message.id", r.getMessageId());
      if (!empty(r.getReceiptHandle())) {
        b.put("trailbeam.receipt_handle.hmac", tb.hasher.hash(r.getReceiptHandle()));
        b.put("trailbeam.receipt_handle.key_id", tb.hasher.keyId());
      }
      putLong(b, "trailbeam.receive_count", a.get("ApproximateReceiveCount"));
      putLong(b, "trailbeam.sent_timestamp_ms", a.get("SentTimestamp"));
      putLong(b, "trailbeam.first_receive_timestamp_ms", a.get("ApproximateFirstReceiveTimestamp"));
      put(b, "trailbeam.aws_trace_header", a.get("AWSTraceHeader"));
      put(b, "trailbeam.dead_letter_queue_source_arn", a.get("DeadLetterQueueSourceArn"));
      put(b, "trailbeam.queue.arn", arn);
      faas(b, context);
      b.put("trailbeam.batch.size", (long) size);
      b.put("trailbeam.batch.index", (long) index);
      put(b, "trailbeam.message_group_id", a.get("MessageGroupId"));
      put(b, "trailbeam.sequence_number", a.get("SequenceNumber"));
      try {
        for (Map.Entry<String, Object> c : Capture.contentAttrs(tb.capture, queue[0], r.getBody(),
            stringValues(r.getMessageAttributes()), r.getMd5OfBody()).entrySet()) {
          Object v = c.getValue();
          if (v instanceof Long) {
            b.put(c.getKey(), (Long) v);
          } else {
            b.put(c.getKey(), (String) v);
          }
        }
      } catch (RuntimeException | StackOverflowError e) {
        debug("content attributes", e);
        b.put("trailbeam.body.capture_status", Capture.STATUS_REDACTION_FAILED);
      }
      b.put("trailbeam.start_observed", false);
      tb.emit(Trailbeam.SPAN_RECEIVE, OPERATION, "lambda", clientId, queue[0], queue[1], b.build(), parent, t, t,
          thread, null, returned);
    }
    flush();
  }

  private void safeOutcomes(Trailbeam tb, List<SQSEvent.SQSMessage> records, Context context,
                            Map<String, String> outcomes, SpanContext parent, Throwable err) {
    if (tb == null) {
      return;
    }
    try {
      outcomes(tb, records, context, outcomes, parent, err);
    } catch (RuntimeException | LinkageError e) {
      debug("outcome spans", e);
    }
  }

  private void outcomes(Trailbeam tb, List<SQSEvent.SQSMessage> records, Context context,
                        Map<String, String> outcomes, SpanContext parent, Throwable err) {
    String clientId = clientId();
    int size = records.size();
    Long reported = err != null ? null : reportedFailureCount(outcomes);
    // The outcome is decided now, after the handler returned or threw: one moment, one tick.
    long t = Trailbeam.nowNanos();
    CallOrder.Tick tick = CallOrder.tick();
    String thread = Thread.currentThread().getName();
    for (int index = 0; index < size; index++) {
      SQSEvent.SQSMessage r = records.get(index);
      String[] queue = queueFromArn(r.getEventSourceArn());
      String outcome = outcomes.get(r.getMessageId());
      AttributesBuilder b = Attributes.builder()
          .put("messaging.message.id", r.getMessageId())
          .put("trailbeam.outcome", outcome);
      if (FAILED_EXCEPTION.equals(outcome) && err != null) {
        b.put("trailbeam.error_type", errorType(err));
        put(b, "trailbeam.error_message", Capture.errorMessage(tb.capture, queue[0], err.getLocalizedMessage()));
      }
      faas(b, context);
      b.put("trailbeam.batch.size", (long) size);
      b.put("trailbeam.batch.index", (long) index);
      if (reported != null) {
        b.put("trailbeam.batch.reported_failure_count", reported);
      }
      tb.emit(Trailbeam.SPAN_OUTCOME, OPERATION, "lambda", clientId, queue[0], queue[1], b.build(), parent, t, t,
          thread, tick, tick);
    }
    flush();
  }

  /** Flush, but never hold the invocation longer than the flush timeout or fail it. */
  private static void flush() {
    try {
      Trailbeam.flush(FLUSH_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    } catch (RuntimeException e) {
      debug("flush", e);
    }
  }

  /** One Lambda execution environment = one log stream (section 5); else the runtime's service.instance.id. */
  private String clientId() {
    String stream = env.apply("AWS_LAMBDA_LOG_STREAM_NAME");
    return empty(stream) ? null : stream;
  }

  private void faas(AttributesBuilder b, Context context) {
    String name = context == null ? null : context.getFunctionName();
    put(b, "faas.name", empty(name) ? env.apply("AWS_LAMBDA_FUNCTION_NAME") : name);
    put(b, "faas.invocation_id", context == null ? null : context.getAwsRequestId());
  }

  /**
   * The span active at invocation time (section 2): the Lambda runtime puts the invocation's X-Ray trace header in
   * the system property {@code com.amazonaws.xray.traceHeader} (and, on older runtimes, {@code _X_AMZN_TRACE_ID}).
   * The OpenTelemetry Java agent's Lambda span is made from the same header. Without one, the spans are roots.
   */
  static SpanContext parent(Function<String, String> env) {
    String header = System.getProperty("com.amazonaws.xray.traceHeader");
    if (empty(header)) {
      header = env.apply("_X_AMZN_TRACE_ID");
    }
    return empty(header) ? null : TraceHeaders.fromXray(header);
  }

  // -- decisions ---------------------------------------------------------------------------------

  /** The SQS records of the event, or none if it is not an SQS event. */
  static List<SQSEvent.SQSMessage> sqsRecords(SQSEvent event) {
    if (event == null || event.getRecords() == null) {
      return Collections.emptyList();
    }
    List<SQSEvent.SQSMessage> out = new ArrayList<>();
    for (SQSEvent.SQSMessage r : event.getRecords()) {
      if (r != null && "aws:sqs".equals(r.getEventSource()) && !empty(r.getMessageId())) {
        out.add(r);
      }
    }
    return out;
  }

  /**
   * Outcome per record ID after the handler returned {@code result} (section 4.4), by Lambda's partial batch
   * response rules: no {@code batchItemFailures}, null or empty means success; an entry whose
   * {@code itemIdentifier} is missing, empty or not in the batch fails the whole batch. {@code result} is an
   * {@link SQSBatchResponse}, a {@code Map} of the same shape, or anything else (no list).
   */
  static Map<String, String> decideOutcomes(List<String> recordIds, Object result, boolean reportBatchItemFailures) {
    Set<String> ids = new HashSet<>(recordIds);
    Set<String> failed = new HashSet<>();
    List<?> items = reportBatchItemFailures ? failureItems(result) : null;
    if (items != null) {
      boolean valid = true;
      for (Object item : items) {
        String ident = itemIdentifier(item);
        if (ident != null && ids.contains(ident)) {
          failed.add(ident);
        } else {
          valid = false;
        }
      }
      if (!valid) {
        Map<String, String> out = new LinkedHashMap<>();
        for (String id : recordIds) {
          out.put(id, FAILED_INVALID_RESPONSE);
        }
        return out;
      }
    }
    Map<String, String> out = new LinkedHashMap<>();
    for (String id : recordIds) {
      out.put(id, failed.contains(id) ? FAILED_REPORTED : SUCCESS);
    }
    return out;
  }

  private static List<?> failureItems(Object result) {
    if (result instanceof SQSBatchResponse) {
      return ((SQSBatchResponse) result).getBatchItemFailures();
    }
    if (result instanceof Map) {
      Object items = ((Map<?, ?>) result).get("batchItemFailures");
      if (items == null) {
        return null;
      }
      // Not a list: Lambda can't read it, so the batch fails (an invalid response).
      return items instanceof List ? (List<?>) items : Collections.singletonList(null);
    }
    return null;
  }

  private static String itemIdentifier(Object item) {
    Object ident = null;
    if (item instanceof SQSBatchResponse.BatchItemFailure) {
      ident = ((SQSBatchResponse.BatchItemFailure) item).getItemIdentifier();
    } else if (item instanceof Map) {
      ident = ((Map<?, ?>) item).get("itemIdentifier");
    }
    return ident instanceof String && !((String) ident).isEmpty() ? (String) ident : null;
  }

  /** Records listed in a valid {@code batchItemFailures} (0 if none); null if the list was invalid. */
  static Long reportedFailureCount(Map<String, String> outcomes) {
    long n = 0;
    for (String v : outcomes.values()) {
      if (FAILED_INVALID_RESPONSE.equals(v)) {
        return null;
      }
      if (FAILED_REPORTED.equals(v)) {
        n++;
      }
    }
    return n;
  }

  /**
   * The thrown class's name, never its message (messages often contain message contents). The fully qualified
   * name, which is what Lambda's Java runtime reports as the invocation's {@code errorType}.
   */
  static String errorType(Throwable err) {
    return err.getClass().getName();
  }

  /** {@code arn:aws:sqs:<region>:<account>:<name>} to {name, https://sqs.<region>.amazonaws.com/<account>/<name>}. */
  static String[] queueFromArn(String arn) {
    String[] parts = arn == null ? new String[0] : arn.split(":", -1);
    if (parts.length < 6 || parts[5].isEmpty()) {
      return new String[] {parts.length == 0 ? null : parts[parts.length - 1], null};
    }
    String name = String.join(":", java.util.Arrays.copyOfRange(parts, 5, parts.length));
    return new String[] {name, "https://sqs." + parts[3] + ".amazonaws.com/" + parts[4] + "/" + name};
  }

  private static Map<String, String> stringValues(Map<String, SQSEvent.MessageAttribute> attributes) {
    if (attributes == null || attributes.isEmpty()) {
      return Collections.emptyMap();
    }
    Map<String, String> out = new HashMap<>();
    for (Map.Entry<String, SQSEvent.MessageAttribute> a : attributes.entrySet()) {
      if (a.getValue() != null && a.getValue().getStringValue() != null) {
        out.put(a.getKey(), a.getValue().getStringValue());
      }
    }
    return out;
  }

  private static void put(AttributesBuilder b, String name, String value) {
    if (!empty(value)) { // section 3: an absent value is omitted, never ""
      b.put(name, value);
    }
  }

  private static void putLong(AttributesBuilder b, String name, String value) {
    if (empty(value)) {
      return;
    }
    try {
      b.put(name, Long.parseLong(value.trim()));
    } catch (NumberFormatException ignored) {
    }
  }

  private static boolean empty(String s) {
    return s == null || s.isEmpty();
  }

  private static void debug(String what, Throwable t) {
    Trailbeam.LOG.log(System.Logger.Level.DEBUG, "trailbeam: Lambda " + what + " failed", t);
  }
}
