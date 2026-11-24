package dev.trailbeam.sqs;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSBatchResponse;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;

/**
 * A base class for a Lambda function invoked by an SQS event source mapping. Extend it instead of implementing
 * {@code RequestHandler<SQSEvent, SQSBatchResponse>} yourself, and put your code in {@link #handleSqsEvent}:
 *
 * <pre>{@code
 * public class OrdersHandler extends TrailbeamSqsHandler {
 *   protected SQSBatchResponse handleSqsEvent(SQSEvent event, Context context) {
 *     ...
 *     return new SQSBatchResponse(failures); // or null when every record succeeded
 *   }
 * }
 * }</pre>
 *
 * <p>Each record gets a {@code trailbeam receive}, exported before your code runs, and a {@code trailbeam outcome}
 * after it returns or throws (contract sections 4.2 and 4.4). What you return, and what you throw, reach Lambda
 * unchanged. To wrap a handler without changing its code, use {@link TrailbeamSqsLambdaWrapper} instead.
 */
public abstract class TrailbeamSqsHandler implements RequestHandler<SQSEvent, SQSBatchResponse> {
  private final LambdaRecorder recorder = new LambdaRecorder(System::getenv);

  protected TrailbeamSqsHandler() {
    TrailbeamSqsLambdaWrapper.start();
  }

  /** Your handler. Return the records that failed, or null (or an empty list) when all succeeded. */
  protected abstract SQSBatchResponse handleSqsEvent(SQSEvent event, Context context);

  /**
   * Whether Lambda reads {@code batchItemFailures}: true when the event source mapping has
   * {@code ReportBatchItemFailures} in its {@code FunctionResponseTypes}. When it is off, Lambda ignores the list,
   * and so does Trailbeam. Override to return false for such a mapping.
   */
  protected boolean reportBatchItemFailures() {
    return true;
  }

  @Override
  public final SQSBatchResponse handleRequest(SQSEvent event, Context context) {
    try {
      return (SQSBatchResponse) recorder.invoke(this::handleSqsEvent, event, context, reportBatchItemFailures());
    } catch (Throwable t) {
      throw TrailbeamSqsLambdaWrapper.<RuntimeException>rethrow(t);
    }
  }
}
