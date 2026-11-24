package trailbeam.kit;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSBatchResponse;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The conformance kit's Lambda function (Java). It knows nothing of Trailbeam: the function's handler is
 * {@code dev.trailbeam.sqs.TrailbeamSqsLambdaWrapper}, which loads this class from {@code TRAILBEAM_LAMBDA_HANDLER}.
 *
 * <p>What it does with a batch is set by each record's body, {@code {"case": ...}} (SCENARIO-LAMBDA.md): it throws if
 * any record is {@code throw}, returns an invalid batchItemFailures if any is {@code invalid}, and otherwise lists
 * the {@code report} and {@code dlq} records as failures. On the dead-letter queue it succeeds. Its first act is
 * the KIT_HANDLER log line the poller reads.
 */
public class LambdaHandler implements RequestHandler<SQSEvent, SQSBatchResponse> {
  private static final Pattern CASE = Pattern.compile("\"case\"\\s*:\\s*\"([^\"]*)\"");

  @Override
  public SQSBatchResponse handleRequest(SQSEvent event, Context context) {
    Instant now = Instant.now();
    long started = now.getEpochSecond() * 1_000_000_000L + now.getNano();
    List<SQSEvent.SQSMessage> records = event.getRecords() == null ? Collections.emptyList() : event.getRecords();
    StringBuilder ids = new StringBuilder("[");
    for (SQSEvent.SQSMessage r : records) {
      ids.append(ids.length() > 1 ? "," : "").append(json(r.getMessageId()));
    }
    String stream = System.getenv("AWS_LAMBDA_LOG_STREAM_NAME");
    System.out.println("KIT_HANDLER {\"started_unix_nano\":\"" + started + "\",\"request_id\":" + json(context.getAwsRequestId())
        + ",\"function_name\":" + json(context.getFunctionName()) + ",\"log_stream\":" + (stream == null ? "null" : json(stream))
        + ",\"message_ids\":" + ids + "]}");
    System.out.flush();

    boolean dlq = !records.isEmpty();
    List<String> cases = new ArrayList<>();
    for (SQSEvent.SQSMessage r : records) {
      dlq &= r.getEventSourceArn() != null && r.getEventSourceArn().endsWith("-dlq");
      Matcher m = CASE.matcher(r.getBody() == null ? "" : r.getBody());
      cases.add(m.find() ? m.group(1) : "");
    }
    if (dlq) {
      return new SQSBatchResponse(Collections.emptyList());
    }
    if (cases.contains("throw")) {
      throw new RuntimeException("conformance kit: the handler threw (case throw)");
    }
    if (cases.contains("invalid")) {
      return new SQSBatchResponse(Collections.singletonList(new SQSBatchResponse.BatchItemFailure("not-a-record-of-this-batch")));
    }
    List<SQSBatchResponse.BatchItemFailure> failures = new ArrayList<>();
    for (int i = 0; i < records.size(); i++) {
      if (cases.get(i).equals("report") || cases.get(i).equals("dlq")) {
        failures.add(new SQSBatchResponse.BatchItemFailure(records.get(i).getMessageId()));
      }
    }
    if (failures.isEmpty() && cases.size() == 1 && cases.get(0).equals("success")) {
      return null; // no batchItemFailures at all
    }
    return new SQSBatchResponse(failures);
  }

  private static String json(String s) {
    if (s == null) {
      return "null";
    }
    StringBuilder b = new StringBuilder("\"");
    for (char c : s.toCharArray()) {
      if (c == '"' || c == '\\') {
        b.append('\\').append(c);
      } else if (c < 0x20) {
        b.append(String.format("\\u%04x", (int) c));
      } else {
        b.append(c);
      }
    }
    return b.append('"').toString();
  }
}
