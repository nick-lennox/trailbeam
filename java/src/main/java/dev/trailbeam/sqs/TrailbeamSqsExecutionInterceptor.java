package dev.trailbeam.sqs;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.trace.SpanContext;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.SdkRequest;
import software.amazon.awssdk.core.SdkResponse;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttribute;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.core.interceptor.SdkExecutionAttribute;
import software.amazon.awssdk.services.sqs.model.BatchResultErrorEntry;
import software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityBatchRequest;
import software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityBatchRequestEntry;
import software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityBatchResponse;
import software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityBatchResultEntry;
import software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequestEntry;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchResponse;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchResultEntry;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import software.amazon.awssdk.services.sqs.model.PurgeQueueRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequestEntry;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchResponse;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchResultEntry;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageResponse;

/**
 * Trailbeam's AWS SDK for Java v2 interceptor: one span per SQS message, receipt handle or purge (contract
 * sections 4.1 to 4.3 and 18).
 *
 * <p>The SDK loads it for every client from {@code software/amazon/awssdk/global/handlers/execution.interceptors}
 * in this jar, so adding the dependency is the whole setup. If that file is lost (an uber jar that merged
 * another library's copy of it), register it by hand:
 * {@code SqsClient.builder().overrideConfiguration(c -> c.addExecutionInterceptor(new TrailbeamSqsExecutionInterceptor()))}.
 * Registering it by hand as well as from the classpath emits each span once.
 *
 * <p>The simple class name must stay unique: the SDK keys global interceptors by simple class name and drops a
 * second one with the same name without a word (QV-312 report, section 3, point 1). A generic name would vanish
 * beside the OpenTelemetry Java agent's {@code TracingExecutionInterceptor}.
 *
 * <p>The hooks never throw into the application's call.
 */
public final class TrailbeamSqsExecutionInterceptor implements ExecutionInterceptor {
  /**
   * The state of one SQS call. Its owner is the instance that claimed the call in {@code beforeExecution}: a
   * second instance on the same client (a hand registration on top of the global one) leaves the call alone.
   */
  static final class Call {
    final TrailbeamSqsExecutionInterceptor owner;
    final String operation;
    final String threadName;
    final CallOrder.Tick issued;
    final long startNanos;
    volatile SpanContext parent;

    Call(TrailbeamSqsExecutionInterceptor owner, String operation, String threadName, CallOrder.Tick issued,
         long startNanos) {
      this.owner = owner;
      this.operation = operation;
      this.threadName = threadName;
      this.issued = issued;
      this.startNanos = startNanos;
    }
  }

  // The SDK refuses two ExecutionAttributes with one name in a class loader; the suffix keeps two copies of
  // this jar in two class loaders (two webapps over one shared SDK) from colliding.
  static final ExecutionAttribute<Call> CALL = new ExecutionAttribute<>(
      "trailbeam.call@" + Integer.toHexString(System.identityHashCode(TrailbeamSqsExecutionInterceptor.class)));

  /** System attributes the receive hook asks for (section 4.2). */
  static final List<String> RECEIVE_SYSTEM_ATTRIBUTES = Collections.unmodifiableList(Arrays.asList(
      "ApproximateReceiveCount",
      "SentTimestamp",
      "ApproximateFirstReceiveTimestamp",
      "AWSTraceHeader",
      "DeadLetterQueueSourceArn", // 3.1: set only on a receive from a dead-letter queue
      "MessageGroupId", // 3.2: FIFO queues (and fair standard queues)
      "SequenceNumber")); // 3.2: FIFO queues only

  static final Set<String> TRACKED = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
      "SendMessage", "SendMessageBatch", "ReceiveMessage", "DeleteMessage", "DeleteMessageBatch",
      "ChangeMessageVisibility", "ChangeMessageVisibilityBatch", "PurgeQueue")));

  public TrailbeamSqsExecutionInterceptor() {}

  // -- hooks -------------------------------------------------------------------------------------

  @Override
  public void beforeExecution(Context.BeforeExecution context, ExecutionAttributes ea) {
    try {
      if (!"Sqs".equals(ea.getAttribute(SdkExecutionAttribute.SERVICE_NAME))) {
        return;
      }
      String op = ea.getAttribute(SdkExecutionAttribute.OPERATION_NAME);
      if (op == null || !TRACKED.contains(op) || ea.getAttribute(CALL) != null) {
        return;
      }
      Trailbeam tb = Trailbeam.get();
      Trailbeam.infoOnce("active", "Trailbeam interceptor active: SQS calls are traced (trailbeam "
          + Trailbeam.VERSION + ", exporting to " + tb.endpoint + ")");
      // The thread is read here: an async client completes the call on an SDK thread (sdk-async-response-*).
      String thread = Thread.currentThread().getName();
      // Section 2a: the issued tick just before the start time is read. This hook runs before the SDK
      // serializes, signs and sends the request (and before any retry), so the start is a lower bound.
      CallOrder.Tick issued = CallOrder.tick();
      ea.putAttribute(CALL, new Call(this, op, thread, issued, nowNanos()));
    } catch (Throwable t) {
      debug("beforeExecution", t);
    }
  }

  @Override
  public SdkRequest modifyRequest(Context.ModifyRequest context, ExecutionAttributes ea) {
    SdkRequest request = context.request();
    try {
      if (mine(ea) != null && request instanceof ReceiveMessageRequest) {
        ReceiveMessageRequest r = addReceiveAttributeNames((ReceiveMessageRequest) request);
        return addMessageAttributeNames(r, Trailbeam.get().capture.tenantAttribute);
      }
    } catch (Throwable t) {
      debug("modifyRequest", t);
    }
    return request;
  }

  @Override
  public void beforeTransmission(Context.BeforeTransmission context, ExecutionAttributes ea) {
    try {
      Call call = mine(ea);
      if (call == null) {
        return;
      }
      // Every interceptor's modifyHttpRequest has run by now, whatever order the chain is in, so the stock
      // instrumentation's trace header (if any) is on the request.
      SpanContext parent = TraceHeaders.parent(context.httpRequest().headers());
      if (parent != null) {
        call.parent = parent;
      }
    } catch (Throwable t) {
      debug("beforeTransmission", t);
    }
  }

  @Override
  public void afterExecution(Context.AfterExecution context, ExecutionAttributes ea) {
    finish(ea, context.request(), context.response(), null);
  }

  @Override
  public void onExecutionFailure(Context.FailedExecution context, ExecutionAttributes ea) {
    finish(ea, context.request(), context.response().orElse(null), context.exception());
  }

  // -- receive request ---------------------------------------------------------------------------

  /**
   * Asks for the system attributes Trailbeam needs, keeping everything the caller asked for. A caller that
   * uses only the deprecated {@code attributeNames} keeps using it; SDK releases before
   * {@code messageSystemAttributeNames} existed have only that.
   */
  static ReceiveMessageRequest addReceiveAttributeNames(ReceiveMessageRequest r) {
    List<String> legacy = new ArrayList<>(r.attributeNamesAsStrings());
    List<String> current;
    try {
      current = new ArrayList<>(r.messageSystemAttributeNamesAsStrings());
    } catch (NoSuchMethodError e) { // an SDK older than messageSystemAttributeNames
      current = null;
    }
    Set<String> asked = new HashSet<>(legacy);
    if (current != null) {
      asked.addAll(current);
    }
    if (asked.contains("All")) {
      return r;
    }
    List<String> missing = new ArrayList<>();
    for (String n : RECEIVE_SYSTEM_ATTRIBUTES) {
      if (!asked.contains(n)) {
        missing.add(n);
      }
    }
    if (missing.isEmpty()) {
      return r;
    }
    if (current == null || (!legacy.isEmpty() && current.isEmpty())) {
      legacy.addAll(missing);
      return legacyAttributeNames(r, legacy);
    }
    current.addAll(missing);
    return r.toBuilder().messageSystemAttributeNamesWithStrings(current).build();
  }

  /**
   * 3.2 (section 15.2): asks for the configured tenant attribute and the Extended Client's size attributes,
   * keeping everything the caller asked for. That changes what SQS returns, not the message's state.
   */
  static ReceiveMessageRequest addMessageAttributeNames(ReceiveMessageRequest r, String tenantAttribute) {
    List<String> asked = new ArrayList<>(r.messageAttributeNames());
    if (asked.contains("All") || asked.contains(".*")) {
      return r;
    }
    List<String> wanted = new ArrayList<>();
    if (tenantAttribute != null) {
      wanted.add(tenantAttribute);
    }
    wanted.addAll(Capture.OFFLOADED_SIZE_ATTRIBUTES);
    boolean changed = false;
    for (String name : wanted) {
      if (!asked.contains(name)) {
        asked.add(name);
        changed = true;
      }
    }
    return changed ? r.toBuilder().messageAttributeNames(asked).build() : r;
  }

  @SuppressWarnings("deprecation")
  private static ReceiveMessageRequest legacyAttributeNames(ReceiveMessageRequest r, List<String> names) {
    return r.toBuilder().attributeNamesWithStrings(names).build();
  }

  // -- spans -------------------------------------------------------------------------------------

  private void finish(ExecutionAttributes ea, SdkRequest request, SdkResponse response, Throwable error) {
    Call call;
    try {
      call = mine(ea);
    } catch (Throwable t) {
      return;
    }
    if (call == null) {
      return;
    }
    // Section 2: the span ends when the response (or error) arrived; the returned tick just after.
    long endNanos = nowNanos();
    CallOrder.Tick returned = CallOrder.tick();
    try {
      Trailbeam tb = Trailbeam.get();
      Emit emit = new Emit(tb, call, endNanos, returned);
      if (request instanceof SendMessageRequest) {
        onSend(emit, (SendMessageRequest) request, response);
      } else if (request instanceof SendMessageBatchRequest) {
        onSendBatch(emit, (SendMessageBatchRequest) request, response);
      } else if (request instanceof ReceiveMessageRequest) {
        onReceive(emit, (ReceiveMessageRequest) request, response);
      } else if (request instanceof PurgeQueueRequest) {
        onPurge(emit, (PurgeQueueRequest) request, response, error);
      } else {
        onSettle(emit, request, response, error);
      }
    } catch (Throwable t) {
      debug("afterExecution", t);
    }
  }

  /** Emits the spans of one call: every span shares the call's interval and ticks. */
  private static final class Emit {
    final Trailbeam tb;
    final Call call;
    final long endNanos;
    final CallOrder.Tick returned;

    Emit(Trailbeam tb, Call call, long endNanos, CallOrder.Tick returned) {
      this.tb = tb;
      this.call = call;
      this.endNanos = endNanos;
      this.returned = returned;
    }

    void span(String name, String queueUrl, AttributesBuilder attrs) {
      tb.emit(name, call.operation, queueUrl, attrs.build(), call.parent, call.startNanos, endNanos,
          call.threadName, call.issued, returned);
    }
  }

  private static void onSend(Emit e, SendMessageRequest req, SdkResponse response) {
    if (!(response instanceof SendMessageResponse)) {
      return;
    }
    SendMessageResponse r = (SendMessageResponse) response;
    if (empty(r.messageId())) {
      return;
    }
    AttributesBuilder b = Attributes.builder().put("messaging.message.id", r.messageId());
    contents(e.tb, b, req.queueUrl(), req.messageBody(), stringValues(req.messageAttributes()),
        r.md5OfMessageBody());
    fifo(b, req.messageGroupId(), r.sequenceNumber());
    e.span(Trailbeam.SPAN_SEND, req.queueUrl(), b);
  }

  private static void onSendBatch(Emit e, SendMessageBatchRequest req, SdkResponse response) {
    if (!(response instanceof SendMessageBatchResponse)) {
      return;
    }
    Map<String, SendMessageBatchRequestEntry> entries = new HashMap<>();
    for (SendMessageBatchRequestEntry entry : req.entries()) {
      entries.put(entry.id(), entry);
    }
    // Only entries SQS accepted: a failed entry has no message ID and produces no span (section 4.1).
    for (SendMessageBatchResultEntry ok : ((SendMessageBatchResponse) response).successful()) {
      if (empty(ok.messageId())) {
        continue;
      }
      AttributesBuilder b = Attributes.builder().put("messaging.message.id", ok.messageId());
      putIfPresent(b, "trailbeam.batch_entry_id", ok.id());
      SendMessageBatchRequestEntry sent = entries.get(ok.id());
      if (sent != null) { // the body comes from the request entry with the same Id
        contents(e.tb, b, req.queueUrl(), sent.messageBody(), stringValues(sent.messageAttributes()),
            ok.md5OfMessageBody());
        fifo(b, sent.messageGroupId(), ok.sequenceNumber());
      }
      e.span(Trailbeam.SPAN_SEND, req.queueUrl(), b);
    }
  }

  private static void onReceive(Emit e, ReceiveMessageRequest req, SdkResponse response) {
    if (!(response instanceof ReceiveMessageResponse)) {
      return;
    }
    for (Message m : ((ReceiveMessageResponse) response).messages()) {
      String handle = m.receiptHandle();
      String id = m.messageId();
      if (empty(handle) || empty(id)) {
        continue;
      }
      e.tb.handles.put(handle, id);
      Map<String, String> a = m.attributesAsStrings();
      AttributesBuilder b = Attributes.builder().put("messaging.message.id", id);
      hash(e.tb, b, handle);
      putLong(b, "trailbeam.receive_count", a.get("ApproximateReceiveCount"));
      putLong(b, "trailbeam.sent_timestamp_ms", a.get("SentTimestamp"));
      putLong(b, "trailbeam.first_receive_timestamp_ms", a.get("ApproximateFirstReceiveTimestamp"));
      putIfPresent(b, "trailbeam.aws_trace_header", a.get("AWSTraceHeader"));
      putIfPresent(b, "trailbeam.dead_letter_queue_source_arn", a.get("DeadLetterQueueSourceArn"));
      fifo(b, a.get("MessageGroupId"), a.get("SequenceNumber"));
      contents(e.tb, b, req.queueUrl(), m.body(), stringValues(m.messageAttributes()), m.md5OfBody());
      e.span(Trailbeam.SPAN_RECEIVE, req.queueUrl(), b);
    }
  }

  /** 3.3 (section 18): one span per PurgeQueue call, ok or failed. SQS does not say which messages went. */
  private static void onPurge(Emit e, PurgeQueueRequest req, SdkResponse response, Throwable error) {
    String code = callError(response, error);
    AttributesBuilder b = Attributes.builder().put("trailbeam.result", code == null ? "ok" : "failed");
    putIfPresent(b, "trailbeam.error_code", plainCode(code));
    e.span(Trailbeam.SPAN_PURGE, req.queueUrl(), b);
  }

  /** One handle (or batch entry) of a settle call: (entry ID, receipt handle, visibility timeout). */
  private static final class Settle {
    final String entryId;
    final String handle;
    final Integer visibilityTimeout;

    Settle(String entryId, String handle, Integer visibilityTimeout) {
      this.entryId = entryId;
      this.handle = handle;
      this.visibilityTimeout = visibilityTimeout;
    }
  }

  private static void onSettle(Emit e, SdkRequest request, SdkResponse response, Throwable error) {
    String spanName;
    String queueUrl;
    boolean batch;
    List<Settle> entries = new ArrayList<>();
    Set<String> okIds = new HashSet<>();
    Map<String, String> failed = new HashMap<>();
    if (request instanceof DeleteMessageRequest) {
      DeleteMessageRequest r = (DeleteMessageRequest) request;
      spanName = Trailbeam.SPAN_DELETE;
      queueUrl = r.queueUrl();
      batch = false;
      entries.add(new Settle(null, r.receiptHandle(), null));
    } else if (request instanceof DeleteMessageBatchRequest) {
      DeleteMessageBatchRequest r = (DeleteMessageBatchRequest) request;
      spanName = Trailbeam.SPAN_DELETE;
      queueUrl = r.queueUrl();
      batch = true;
      for (DeleteMessageBatchRequestEntry x : r.entries()) {
        entries.add(new Settle(x.id(), x.receiptHandle(), null));
      }
      if (response instanceof DeleteMessageBatchResponse) {
        DeleteMessageBatchResponse res = (DeleteMessageBatchResponse) response;
        for (DeleteMessageBatchResultEntry ok : res.successful()) {
          okIds.add(ok.id());
        }
        collectFailed(res.failed(), failed);
      }
    } else if (request instanceof ChangeMessageVisibilityRequest) {
      ChangeMessageVisibilityRequest r = (ChangeMessageVisibilityRequest) request;
      spanName = Trailbeam.SPAN_CHANGE_VISIBILITY;
      queueUrl = r.queueUrl();
      batch = false;
      entries.add(new Settle(null, r.receiptHandle(), r.visibilityTimeout()));
    } else if (request instanceof ChangeMessageVisibilityBatchRequest) {
      ChangeMessageVisibilityBatchRequest r = (ChangeMessageVisibilityBatchRequest) request;
      spanName = Trailbeam.SPAN_CHANGE_VISIBILITY;
      queueUrl = r.queueUrl();
      batch = true;
      for (ChangeMessageVisibilityBatchRequestEntry x : r.entries()) {
        entries.add(new Settle(x.id(), x.receiptHandle(), x.visibilityTimeout()));
      }
      if (response instanceof ChangeMessageVisibilityBatchResponse) {
        ChangeMessageVisibilityBatchResponse res = (ChangeMessageVisibilityBatchResponse) response;
        for (ChangeMessageVisibilityBatchResultEntry ok : res.successful()) {
          okIds.add(ok.id());
        }
        collectFailed(res.failed(), failed);
      }
    } else {
      return;
    }

    // If the call failed as a whole (an error response, or an exception before any response), every handle
    // in it gets a failed span (section 4.3).
    String callError = callError(response, error);
    for (Settle s : entries) {
      if (empty(s.handle)) {
        continue;
      }
      String err;
      if (callError != null) {
        err = callError;
      } else if (!batch || okIds.contains(s.entryId)) {
        err = null;
      } else {
        err = failed.getOrDefault(s.entryId, "MissingFromResponse");
      }
      AttributesBuilder b = Attributes.builder();
      hash(e.tb, b, s.handle);
      putIfPresent(b, "messaging.message.id", e.tb.handles.get(s.handle));
      if (batch) {
        putIfPresent(b, "trailbeam.batch_entry_id", s.entryId);
      }
      if (Trailbeam.SPAN_CHANGE_VISIBILITY.equals(spanName) && s.visibilityTimeout != null) {
        b.put("trailbeam.visibility_timeout", (long) s.visibilityTimeout); // 0 is a real value
      }
      b.put("trailbeam.result", err == null ? "ok" : "failed");
      putIfPresent(b, "trailbeam.error_code", err);
      e.span(spanName, queueUrl, b);
      if (err == null && Trailbeam.SPAN_DELETE.equals(spanName)) {
        e.tb.handles.remove(s.handle);
      }
    }
  }

  private static void collectFailed(List<BatchResultErrorEntry> entries, Map<String, String> out) {
    for (BatchResultErrorEntry f : entries) {
      out.put(f.id(), empty(f.code()) ? "Failed" : f.code());
    }
  }

  // -- attribute helpers ---------------------------------------------------------------------------

  /** The error of a whole call, or null if SQS answered with success. */
  static String callError(SdkResponse response, Throwable error) {
    if (error == null) {
      return response == null ? "NoResponse" : null;
    }
    // The SDK wraps errors: CompletionException on async clients, SdkClientException around an I/O error.
    // The name reported is the service's error code, else the first exception that is not a wrapper.
    Throwable t = error;
    for (int i = 0; i < 8 && t != null; i++) {
      if (t instanceof AwsServiceException) {
        AwsErrorDetails d = ((AwsServiceException) t).awsErrorDetails();
        if (d != null && !empty(d.errorCode())) {
          return d.errorCode();
        }
        return t.getClass().getSimpleName();
      }
      boolean wrapper = t instanceof java.util.concurrent.CompletionException
          || t instanceof java.util.concurrent.ExecutionException
          || t.getClass() == software.amazon.awssdk.core.exception.SdkClientException.class;
      if (!wrapper || t.getCause() == null) {
        return t.getClass().getSimpleName();
      }
      t = t.getCause();
    }
    return error.getClass().getSimpleName();
  }

  /** An error code without a protocol namespace ({@code com.amazonaws.sqs#QueueDoesNotExist}), section 18. */
  static String plainCode(String code) {
    if (empty(code)) {
      return null;
    }
    String plain = code.substring(code.lastIndexOf('#') + 1);
    return plain.isEmpty() ? null : plain;
  }

  private static void hash(Trailbeam tb, AttributesBuilder b, String handle) {
    b.put("trailbeam.receipt_handle.hmac", tb.hasher.hash(handle));
    b.put("trailbeam.receipt_handle.key_id", tb.hasher.keyId());
  }

  /** 3.2 (section 15): body size, tenant, and the body itself for a queue that opted in. */
  private static void contents(Trailbeam tb, AttributesBuilder b, String queueUrl, String body,
                               Map<String, String> attributes, String md5) {
    String queue = queueUrl == null ? null : Trailbeam.queueName(queueUrl);
    for (Map.Entry<String, Object> a : Capture.contentAttrs(tb.capture, queue, body, attributes, md5).entrySet()) {
      Object v = a.getValue();
      if (v instanceof Long) {
        b.put(a.getKey(), (Long) v);
      } else {
        b.put(a.getKey(), (String) v);
      }
    }
  }

  /** Each message attribute's {@code StringValue}, by name (binary attributes have none). */
  private static Map<String, String> stringValues(Map<String, MessageAttributeValue> attributes) {
    if (attributes == null || attributes.isEmpty()) {
      return Collections.emptyMap();
    }
    Map<String, String> out = new HashMap<>();
    for (Map.Entry<String, MessageAttributeValue> a : attributes.entrySet()) {
      if (a.getValue() != null && a.getValue().stringValue() != null) {
        out.put(a.getKey(), a.getValue().stringValue());
      }
    }
    return out;
  }

  private static void fifo(AttributesBuilder b, String groupId, String sequenceNumber) {
    putIfPresent(b, "trailbeam.message_group_id", groupId);
    putIfPresent(b, "trailbeam.sequence_number", sequenceNumber);
  }

  private static void putIfPresent(AttributesBuilder b, String name, String value) {
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

  private Call mine(ExecutionAttributes ea) {
    Call c = ea.getAttribute(CALL);
    return c != null && c.owner == this ? c : null;
  }

  static long nowNanos() {
    return Trailbeam.nowNanos();
  }

  private static void debug(String hook, Throwable t) {
    Trailbeam.LOG.log(System.Logger.Level.DEBUG, "trailbeam " + hook + " hook failed", t);
  }
}
