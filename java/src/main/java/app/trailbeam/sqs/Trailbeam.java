package app.trailbeam.sqs;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.exporter.otlp.http.trace.OtlpHttpSpanExporter;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.SdkTracerProviderBuilder;
import io.opentelemetry.sdk.trace.SpanProcessor;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import java.io.InputStream;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * The Trailbeam runtime: its own always-on tracer provider (contract section 2), the receipt-hash key (section
 * 7), the consumer identity (section 5) and the code that emits every Trailbeam span with the attributes of
 * section 3.
 *
 * <p>Nothing here needs to be called: the interceptor starts the runtime on the first SQS call, and a JVM
 * shutdown hook flushes it. {@link #flush} and {@link #shutdown} are for programs that must be sure every span
 * has left before they go on, such as a Lambda handler or a test.
 *
 * <p>Configuration is read from the environment: {@code TRAILBEAM_OTLP_TRACES_ENDPOINT} (default
 * {@code http://localhost:4318/v1/traces}), {@code TRAILBEAM_RECEIPT_HASH_KEY}, {@code TRAILBEAM_MAX_QUEUE_SIZE},
 * {@code OTEL_SERVICE_NAME} and {@code OTEL_RESOURCE_ATTRIBUTES}.
 */
public final class Trailbeam {
  static final int SCHEMA_VERSION = 3;
  static final String SCOPE_NAME = "trailbeam";
  static final String DEFAULT_KEY = "trailbeam-default-receipt-hash-key";
  static final String DEFAULT_ENDPOINT = "http://localhost:4318/v1/traces";
  static final int MIN_KEY_BYTES = 16;
  static final int DEFAULT_MAX_QUEUE_SIZE = 16384;

  static final String SPAN_SEND = "trailbeam send";
  static final String SPAN_RECEIVE = "trailbeam receive";
  static final String SPAN_DELETE = "trailbeam delete";
  static final String SPAN_CHANGE_VISIBILITY = "trailbeam change_visibility";
  static final String SPAN_PURGE = "trailbeam purge";
  static final String SPAN_OUTCOME = "trailbeam outcome";

  static final System.Logger LOG = System.getLogger("trailbeam");
  static final String VERSION = readVersion();

  private static final Object LOCK = new Object();
  private static volatile Trailbeam instance;
  private static final Set<String> WARNED = Collections.synchronizedSet(new HashSet<>());

  final SdkTracerProvider provider;
  final Tracer tracer;
  final ReceiptHasher hasher;
  final String clientId;
  final String endpoint;
  final HandleMap handles = new HandleMap();
  /** Section 15: tenant and opt-in body capture, from the environment. */
  final Capture.Config capture;
  private final long pid = ProcessHandle.current().pid();

  private Trailbeam(Function<String, String> env, SpanProcessor processor) {
    Resource resource = resource(env);
    this.clientId = resource.getAttribute(AttributeKey.stringKey("service.instance.id"));
    this.endpoint = nonEmpty(env.apply("TRAILBEAM_OTLP_TRACES_ENDPOINT"), DEFAULT_ENDPOINT);
    if (processor == null) {
      OtlpHttpSpanExporter exporter = exporter(endpoint);
      if (exporter != null) {
        processor = BatchSpanProcessor.builder(exporter)
            .setMaxQueueSize(positiveInt(env.apply("TRAILBEAM_MAX_QUEUE_SIZE"), DEFAULT_MAX_QUEUE_SIZE))
            .build();
      }
    }
    // Without a processor (an unusable endpoint) the provider still works: the SQS calls go on, no span is
    // exported, and the runtime is built once instead of failing again on every call.
    SdkTracerProviderBuilder builder = SdkTracerProvider.builder()
        .setSampler(Sampler.alwaysOn())
        .setResource(resource);
    if (processor != null) {
      builder.addSpanProcessor(processor);
    }
    this.provider = builder.build();
    this.tracer = provider.get(SCOPE_NAME, VERSION);
    this.hasher = hasher(env.apply("TRAILBEAM_RECEIPT_HASH_KEY"));
    this.capture = Capture.Config.fromEnv(env);
    if (capture.invalid != null) {
      warnOnce("capture", "trailbeam: " + capture.invalid + " (TRAILBEAM_REDACT_KEYS, TRAILBEAM_REDACT_PATTERNS or "
          + "TRAILBEAM_CAPTURE_MAX_BYTES); no body or error text is exported until it is fixed");
    }
  }

  /**
   * The OTLP exporter for {@code endpoint}, or null (and one warning) when it is not an http or https URL. The
   * OTel builder throws for such a value ({@code "not a url"}, or {@code "localhost:4318"} without a scheme);
   * that must not reach the SQS call (section 20.4).
   */
  static OtlpHttpSpanExporter exporter(String endpoint) {
    try {
      return OtlpHttpSpanExporter.builder().setEndpoint(endpoint).build();
    } catch (RuntimeException e) {
      warnOnce("endpoint", "trailbeam: TRAILBEAM_OTLP_TRACES_ENDPOINT=\"" + endpoint + "\" is not a usable http or "
          + "https URL (" + e.getMessage() + "); no spans are exported until it is fixed");
      return null;
    }
  }

  // -- the process-wide runtime ------------------------------------------------------------------

  /** The runtime, started on first use. */
  static Trailbeam get() {
    Trailbeam t = instance;
    if (t != null) {
      return t;
    }
    synchronized (LOCK) {
      if (instance == null) {
        instance = new Trailbeam(System::getenv, null);
        SnapStart.register();
        Thread hook = new Thread(Trailbeam::shutdown, "trailbeam-shutdown");
        try {
          Runtime.getRuntime().addShutdownHook(hook);
        } catch (IllegalStateException ignored) { // already shutting down
        }
      }
      return instance;
    }
  }

  /** Tests: replace the runtime with one that sends its spans to {@code processor} (null: the OTLP exporter). */
  static Trailbeam install(Function<String, String> env, SpanProcessor processor) {
    synchronized (LOCK) {
      Trailbeam old = instance;
      instance = new Trailbeam(env, processor);
      if (old != null) {
        old.provider.shutdown().join(5, TimeUnit.SECONDS);
      }
      return instance;
    }
  }

  /** Exports every span emitted so far. Returns false if that did not finish within the timeout. */
  public static boolean flush(long timeout, TimeUnit unit) {
    Trailbeam t = instance;
    if (t == null) {
      return true;
    }
    CompletableResultCode r = t.provider.forceFlush().join(timeout, unit);
    return r.isSuccess();
  }

  /** Flushes and stops the runtime. A later SQS call starts a new one. Called by the JVM shutdown hook. */
  public static void shutdown() {
    Trailbeam t;
    synchronized (LOCK) {
      t = instance;
      instance = null;
    }
    if (t != null) {
      t.provider.shutdown().join(5, TimeUnit.SECONDS);
    }
  }

  /**
   * After a Lambda SnapStart restore (or any CRaC restore): starts a new call-order scope and a new runtime, so
   * that the many execution environments made from one snapshot share neither a scope nor a
   * {@code service.instance.id} (QV-312 report, section 3, point 6). The jar calls this itself through
   * {@code org.crac} when that library is on the classpath; call it from your own {@code afterRestore} hook
   * otherwise. Spans emitted before the snapshot should have been flushed already: the old runtime is stopped
   * without waiting for its export.
   */
  public static void afterRestore() {
    CallOrder.newScope();
    Trailbeam t;
    synchronized (LOCK) {
      t = instance;
      instance = null;
    }
    if (t != null) {
      t.provider.shutdown();
    }
  }

  /** The package version, which is also the instrumentation scope version. */
  public static String version() {
    return VERSION;
  }

  // -- emitting ----------------------------------------------------------------------------------

  /**
   * Emits one Trailbeam span with the attributes every Trailbeam span carries (section 3), {@code attrs} on
   * top. The span is the interval {@code [startNanos, endNanos]} of the SQS call (section 2), with that call's
   * order ticks (section 2a).
   */
  void emit(String name, String operation, String queueUrl, Attributes attrs, SpanContext parent,
            long startNanos, long endNanos, String threadName, CallOrder.Tick issued, CallOrder.Tick returned) {
    String queue = queueUrl == null || queueUrl.isEmpty() ? null : queueName(queueUrl);
    emit(name, operation, "client", clientId, queue, queueUrl, attrs, parent, startNanos, endNanos, threadName,
        issued, returned);
  }

  /**
   * As above, for any source: {@code client} (the interceptor) or {@code lambda} (the Lambda wrapper, which names
   * the queue from the record's event source ARN and identifies the consumer by its log stream, sections 5 and 6).
   */
  void emit(String name, String operation, String source, String clientId, String queueName, String queueUrl,
            Attributes attrs, SpanContext parent, long startNanos, long endNanos, String threadName,
            CallOrder.Tick issued, CallOrder.Tick returned) {
    AttributesBuilder b = Attributes.builder()
        .put("trailbeam.schema_version", (long) SCHEMA_VERSION)
        .put("trailbeam.kind", kind(name))
        .put("trailbeam.source", source)
        .put("messaging.system.name", "aws.sqs")
        .put("messaging.operation.type", operationType(name))
        .put("messaging.operation.name", operation)
        .put("messaging.client.id", clientId == null || clientId.isEmpty() ? this.clientId : clientId)
        .put("process.pid", pid);
    if (threadName != null && !threadName.isEmpty()) {
      b.put("thread.name", threadName);
    }
    if (queueName != null && !queueName.isEmpty()) {
      b.put("messaging.destination.name", queueName);
    }
    if (queueUrl != null && !queueUrl.isEmpty()) {
      b.put("aws.sqs.queue.url", queueUrl);
    }
    if (returned != null) {
      b.put("trailbeam.order.scope", returned.scope);
      // A tick of another scope (a SnapStart restore while the call was in flight) is never mixed in.
      if (issued != null && issued.scope.equals(returned.scope)) {
        b.put("trailbeam.order.issued", issued.value);
      }
      b.put("trailbeam.order.returned", returned.value);
    }
    b.putAll(attrs);
    SpanBuilder sb = tracer.spanBuilder(name)
        .setSpanKind(SpanKind.INTERNAL)
        .setStartTimestamp(Math.min(startNanos, endNanos), TimeUnit.NANOSECONDS)
        .setAllAttributes(b.build());
    if (parent != null) {
      sb.setParent(Context.root().with(Span.wrap(parent)));
    } else {
      sb.setNoParent();
    }
    sb.startSpan().end(endNanos, TimeUnit.NANOSECONDS);
  }

  /**
   * The wall clock in nanoseconds since the epoch (section 2). Here, not in the interceptor, so the Lambda wrapper
   * never loads a class that needs the AWS SDK: a function that only reads its SQS event may not have it.
   */
  static long nowNanos() {
    java.time.Instant now = java.time.Instant.now();
    return now.getEpochSecond() * 1_000_000_000L + now.getNano();
  }

  static String kind(String spanName) {
    switch (spanName) {
      case SPAN_SEND: return "sqs.send";
      case SPAN_RECEIVE: return "sqs.receive";
      case SPAN_PURGE: return "sqs.purge";
      case SPAN_OUTCOME: return "sqs.outcome";
      default: return "sqs.settle";
    }
  }

  static String operationType(String spanName) {
    switch (spanName) {
      case SPAN_SEND: return "send";
      case SPAN_RECEIVE: return "receive";
      case SPAN_PURGE: return "purge"; // a custom value: the semantic conventions define none for a purge
      case SPAN_OUTCOME: return "process";
      default: return "settle";
    }
  }

  /** The queue name: the last path segment of the queue URL (section 3). */
  static String queueName(String url) {
    String u = url;
    while (u.endsWith("/")) {
      u = u.substring(0, u.length() - 1);
    }
    return u.substring(u.lastIndexOf('/') + 1);
  }

  // -- configuration -----------------------------------------------------------------------------

  static ReceiptHasher hasher(String key) {
    if (key == null || key.isEmpty()) {
      warnOnce("default-key", "trailbeam: no receipt-hash key configured (TRAILBEAM_RECEIPT_HASH_KEY); using the "
          + "published default key, which gives no protection. Set the same key on every service that uses these queues.");
      key = DEFAULT_KEY;
    } else if (key.getBytes(StandardCharsets.UTF_8).length < MIN_KEY_BYTES) {
      warnOnce("short-key", "trailbeam: the receipt-hash key is shorter than " + MIN_KEY_BYTES + " bytes");
    }
    return new ReceiptHasher(key);
  }

  /**
   * {@code OTEL_RESOURCE_ATTRIBUTES}, then {@code OTEL_SERVICE_NAME} (default {@code unknown_service}, section 5),
   * and a random {@code service.instance.id} unless one is configured.
   */
  static Resource resource(Function<String, String> env) {
    AttributesBuilder b = Attributes.builder();
    String raw = env.apply("OTEL_RESOURCE_ATTRIBUTES");
    if (raw != null) {
      for (String pair : raw.split(",")) {
        int eq = pair.indexOf('=');
        if (eq <= 0) {
          continue;
        }
        String k = pair.substring(0, eq).trim();
        String v = decode(pair.substring(eq + 1).trim());
        if (!k.isEmpty() && !v.isEmpty()) {
          b.put(k, v);
        }
      }
    }
    Resource r = Resource.getDefault().merge(Resource.create(b.build()));
    String service = env.apply("OTEL_SERVICE_NAME");
    if (service != null && !service.isEmpty()) {
      r = r.merge(Resource.create(Attributes.of(AttributeKey.stringKey("service.name"), service)));
    }
    String current = r.getAttribute(AttributeKey.stringKey("service.name"));
    if (current == null || current.startsWith("unknown_service")) {
      r = r.merge(Resource.create(Attributes.of(AttributeKey.stringKey("service.name"), "unknown_service")));
    }
    String id = r.getAttribute(AttributeKey.stringKey("service.instance.id"));
    if (id == null || id.isEmpty()) {
      r = r.merge(Resource.create(Attributes.of(AttributeKey.stringKey("service.instance.id"),
          UUID.randomUUID().toString())));
    }
    return r;
  }

  private static String decode(String v) {
    try {
      return URLDecoder.decode(v.replace("+", "%2B"), StandardCharsets.UTF_8);
    } catch (IllegalArgumentException e) {
      return v;
    }
  }

  private static int positiveInt(String raw, int dflt) {
    try {
      int v = Integer.parseInt(raw == null ? "" : raw.trim());
      return v > 0 ? v : dflt;
    } catch (NumberFormatException e) {
      return dflt;
    }
  }

  private static String nonEmpty(String v, String dflt) {
    return v == null || v.trim().isEmpty() ? dflt : v.trim();
  }

  static void warnOnce(String tag, String message) {
    if (WARNED.add(tag)) {
      LOG.log(System.Logger.Level.WARNING, message);
    }
  }

  static void infoOnce(String tag, String message) {
    if (WARNED.add(tag)) {
      LOG.log(System.Logger.Level.INFO, message);
    }
  }

  private static String readVersion() {
    try (InputStream in = Trailbeam.class.getResourceAsStream("version.properties")) {
      if (in != null) {
        Properties p = new Properties();
        p.load(in);
        String v = p.getProperty("version");
        if (v != null && !v.isEmpty() && !v.startsWith("${")) {
          return v;
        }
      }
    } catch (Exception ignored) {
    }
    return "0.0.0-dev";
  }
}
