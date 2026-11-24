package dev.trailbeam.sqs;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSBatchResponse;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Wraps a Lambda handler with no change to its code. Set the function's handler to
 * {@code dev.trailbeam.sqs.TrailbeamSqsLambdaWrapper::handleRequest}, and name your own handler in the environment
 * variable {@code TRAILBEAM_LAMBDA_HANDLER}, in the form Lambda uses: {@code com.example.OrdersHandler} for a
 * {@code RequestHandler<SQSEvent, ...>}, or {@code com.example.OrdersHandler::handle} for a public method that
 * takes {@code (SQSEvent, Context)} or {@code (SQSEvent)}. That is the shape of the OpenTelemetry Java Lambda
 * wrappers (QV-312 report, section 3, point 5).
 *
 * <p>Your handler must return an {@link SQSBatchResponse}, a {@code Map} of the same shape, or nothing
 * ({@code null}, {@code void}). The wrapper hands it to Lambda as an {@code SQSBatchResponse}. It also reads
 * {@code TRAILBEAM_LAMBDA_REPORT_BATCH_ITEM_FAILURES}: set it to {@code false} when the event source mapping does
 * not report batch item failures, so Trailbeam ignores the list as Lambda does.
 */
public final class TrailbeamSqsLambdaWrapper implements RequestHandler<SQSEvent, SQSBatchResponse> {
  static final String HANDLER_VARIABLE = "TRAILBEAM_LAMBDA_HANDLER";

  private final LambdaRecorder recorder;
  private final LambdaRecorder.Handler target;
  private final boolean reportBatchItemFailures;

  /** Called by the Lambda runtime once per execution environment, at init: loads the handler named in the environment. */
  public TrailbeamSqsLambdaWrapper() {
    this(System::getenv);
  }

  TrailbeamSqsLambdaWrapper(Function<String, String> env) {
    start();
    this.recorder = new LambdaRecorder(env);
    this.target = load(env.apply(HANDLER_VARIABLE));
    this.reportBatchItemFailures = !"false".equalsIgnoreCase(trim(env.apply("TRAILBEAM_LAMBDA_REPORT_BATCH_ITEM_FAILURES")));
  }

  @Override
  public SQSBatchResponse handleRequest(SQSEvent event, Context context) {
    Object result;
    try {
      result = recorder.invoke(target, event, context, reportBatchItemFailures);
    } catch (Throwable t) {
      throw TrailbeamSqsLambdaWrapper.<RuntimeException>rethrow(t);
    }
    return toResponse(result);
  }

  /**
   * Starts the runtime at init, so a SnapStart snapshot is taken with it (and its restore hook) in place, and the
   * first invocation doesn't pay for it.
   */
  static void start() {
    try {
      Trailbeam.get();
    } catch (RuntimeException | LinkageError e) {
      Trailbeam.LOG.log(System.Logger.Level.DEBUG, "trailbeam: could not start", e);
    }
  }

  // -- the user's handler ------------------------------------------------------------------------

  /** {@code com.example.Handler} or {@code com.example.Handler::method}. Fails the init with a plain message. */
  static LambdaRecorder.Handler load(String spec) {
    String s = trim(spec);
    if (s == null) {
      throw new IllegalStateException("trailbeam: set " + HANDLER_VARIABLE + " to your handler, such as "
          + "com.example.OrdersHandler or com.example.OrdersHandler::handleRequest");
    }
    int sep = s.indexOf("::");
    String className = sep < 0 ? s : s.substring(0, sep);
    String methodName = sep < 0 ? null : trim(s.substring(sep + 2));
    Object instance;
    Class<?> type;
    try {
      ClassLoader cl = Thread.currentThread().getContextClassLoader();
      type = Class.forName(className, true, cl != null ? cl : TrailbeamSqsLambdaWrapper.class.getClassLoader());
      if (type == TrailbeamSqsLambdaWrapper.class) {
        throw new IllegalStateException("trailbeam: " + HANDLER_VARIABLE + " names the wrapper itself; set it to your own handler");
      }
      instance = Modifier.isStatic(methodModifiers(type, methodName)) ? null : type.getDeclaredConstructor().newInstance();
    } catch (ClassNotFoundException e) {
      throw new IllegalStateException("trailbeam: " + HANDLER_VARIABLE + " names class " + className + ", which is not on the classpath", e);
    } catch (InvocationTargetException e) {
      throw rethrow(e.getCause());
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException("trailbeam: cannot create " + className + " (" + HANDLER_VARIABLE
          + "): it needs a public constructor without arguments", e);
    }
    if ((methodName == null || "handleRequest".equals(methodName)) && instance instanceof RequestHandler) {
      @SuppressWarnings("unchecked")
      RequestHandler<Object, Object> h = (RequestHandler<Object, Object>) instance;
      return h::handleRequest;
    }
    Method m = method(type, methodName == null ? "handleRequest" : methodName);
    final Object target = instance;
    final boolean withContext = m.getParameterCount() == 2;
    return (event, context) -> {
      try {
        return withContext ? m.invoke(target, event, context) : m.invoke(target, event);
      } catch (InvocationTargetException e) {
        throw e.getCause();
      }
    };
  }

  private static int methodModifiers(Class<?> type, String methodName) {
    if (methodName == null) {
      return 0;
    }
    try {
      return method(type, methodName).getModifiers();
    } catch (IllegalStateException e) {
      return 0; // reported with the right message below
    }
  }

  /** The public method {@code name(SQSEvent, Context)}, else {@code name(SQSEvent)}. */
  private static Method method(Class<?> type, String name) {
    Method oneArg = null;
    for (Method m : type.getMethods()) {
      if (!m.getName().equals(name) || m.isBridge()) {
        continue;
      }
      Class<?>[] p = m.getParameterTypes();
      if (p.length == 2 && p[0].isAssignableFrom(SQSEvent.class) && p[1].isAssignableFrom(Context.class)
          && p[0] != Object.class) {
        return m;
      }
      if (p.length == 1 && p[0].isAssignableFrom(SQSEvent.class) && p[0] != Object.class) {
        oneArg = m;
      }
    }
    if (oneArg != null) {
      return oneArg;
    }
    throw new IllegalStateException("trailbeam: " + type.getName() + " has no public method " + name
        + "(SQSEvent, Context) or " + name + "(SQSEvent) (" + HANDLER_VARIABLE + ")");
  }

  // -- the response ------------------------------------------------------------------------------

  /** What the handler returned, as the SQSBatchResponse Lambda reads. */
  static SQSBatchResponse toResponse(Object result) {
    if (result == null || result instanceof SQSBatchResponse) {
      return (SQSBatchResponse) result;
    }
    if (result instanceof Map) {
      Object items = ((Map<?, ?>) result).get("batchItemFailures");
      if (items == null) {
        return new SQSBatchResponse(null);
      }
      List<SQSBatchResponse.BatchItemFailure> out = new ArrayList<>();
      if (items instanceof List) {
        for (Object item : (List<?>) items) {
          Object id = item instanceof Map ? ((Map<?, ?>) item).get("itemIdentifier") : null;
          out.add(new SQSBatchResponse.BatchItemFailure(id == null ? null : String.valueOf(id)));
        }
      } else {
        out.add(new SQSBatchResponse.BatchItemFailure(null)); // Lambda fails the batch, as Trailbeam records it
      }
      return new SQSBatchResponse(out);
    }
    Trailbeam.warnOnce("lambda-result", "trailbeam: the Lambda handler returned a " + result.getClass().getName()
        + ", not an SQSBatchResponse; the wrapper returns nothing to Lambda in its place");
    return null;
  }

  private static String trim(String s) {
    return s == null || s.trim().isEmpty() ? null : s.trim();
  }

  /** Throws {@code t} as it is, checked or not, so Lambda reports the handler's own error. */
  @SuppressWarnings("unchecked")
  static <T extends Throwable> T rethrow(Throwable t) throws T {
    throw (T) t;
  }
}
