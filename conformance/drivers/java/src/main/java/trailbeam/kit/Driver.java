package trailbeam.kit;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import dev.trailbeam.sqs.Trailbeam;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import software.amazon.awssdk.core.util.VersionInfo;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.BatchResultErrorEntry;
import software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityBatchRequestEntry;
import software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityBatchResponse;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequestEntry;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchResponse;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequestEntry;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchResponse;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchResultEntry;
import software.amazon.awssdk.services.sqs.model.SqsException;

/**
 * Conformance driver for the Java package: runs conformance/SCENARIO.md on the AWS SDK for Java v2 and writes
 * the ground truth to $KIT_TRUTH_FILE. Trailbeam is not mentioned until the flush at the end: the jar on the
 * classpath is the whole setup, as for users.
 */
public final class Driver {
  static final ObjectMapper JSON = new ObjectMapper();
  static Writer truth;
  static SqsClient sqs;

  static void write(ObjectNode row) throws IOException {
    truth.write(JSON.writeValueAsString(row) + "\n");
    truth.flush();
  }

  static ObjectNode row(String op, String queueUrl) {
    ObjectNode r = JSON.createObjectNode();
    r.put("op", op);
    r.put("queue_url", queueUrl);
    return r;
  }

  static String errorCode(SqsException e) {
    String c = e.awsErrorDetails() == null ? null : e.awsErrorDetails().errorCode();
    return c == null || c.isEmpty() ? e.getClass().getSimpleName() : c;
  }

  static String send(String q, int n) throws IOException {
    String body = "{\"n\": " + n + "}";
    String id = sqs.sendMessage(b -> b.queueUrl(q).messageBody(body)).messageId();
    ObjectNode r = row("SendMessage", q);
    r.put("ok", true);
    r.put("message_id", id);
    r.put("body", body);
    write(r);
    return id;
  }

  static List<Message> receive(String q, List<String> asked) throws IOException {
    ReceiveMessageRequest.Builder b = ReceiveMessageRequest.builder().queueUrl(q).maxNumberOfMessages(10).waitTimeSeconds(1);
    if (!asked.isEmpty()) {
      b.messageSystemAttributeNamesWithStrings(asked);
    }
    List<Message> msgs = sqs.receiveMessage(b.build()).messages();
    ObjectNode r = row("ReceiveMessage", q);
    r.put("ok", true);
    ArrayNode a = r.putArray("asked_system_attributes");
    asked.forEach(a::add);
    ArrayNode ms = r.putArray("messages");
    for (Message m : msgs) {
      ObjectNode o = ms.addObject();
      o.put("message_id", m.messageId());
      o.put("receipt_handle", m.receiptHandle());
      o.put("body", m.body());
      ObjectNode attrs = o.putObject("attributes");
      m.attributesAsStrings().forEach(attrs::put);
    }
    write(r);
    return msgs;
  }

  static Map<String, Message> receiveUntil(String q, List<String> want, List<String> asked) throws IOException {
    Map<String, Message> got = new LinkedHashMap<>();
    for (int i = 0; i < 30; i++) {
      for (Message m : receive(q, asked)) {
        got.put(m.messageId(), m);
      }
      if (got.keySet().containsAll(want)) {
        return got;
      }
    }
    throw new IllegalStateException("timed out waiting for " + want + " on " + q);
  }

  static void delete(String q, String handle) throws IOException {
    ObjectNode r = row("DeleteMessage", q);
    r.put("receipt_handle", handle);
    try {
      sqs.deleteMessage(b -> b.queueUrl(q).receiptHandle(handle));
      r.put("ok", true);
    } catch (SqsException e) {
      r.put("ok", false);
      r.put("error_code", errorCode(e));
    }
    write(r);
  }

  static void batchRows(String op, String q, List<String[]> entries, Integer vt, List<BatchResultErrorEntry> failedList)
      throws IOException {
    Map<String, String> failed = new HashMap<>();
    for (BatchResultErrorEntry f : failedList) {
      failed.put(f.id(), f.code());
    }
    ObjectNode r = row(op, q);
    r.put("ok", true);
    ArrayNode es = r.putArray("entries");
    for (String[] e : entries) {
      ObjectNode o = es.addObject();
      o.put("id", e[0]);
      o.put("receipt_handle", e[1]);
      o.put("ok", !failed.containsKey(e[0]));
      if (vt != null) {
        o.put("visibility_timeout", vt);
      }
      if (failed.containsKey(e[0])) {
        o.put("error_code", failed.get(e[0]));
      }
    }
    write(r);
  }

  public static void main(String[] args) throws Exception {
    String run = System.getenv("KIT_RUN");
    truth = new FileWriter(System.getenv("KIT_TRUTH_FILE"));
    sqs = SqsClient.builder()
        .endpointOverride(URI.create(System.getenv("KIT_SQS_ENDPOINT")))
        .region(Region.of(System.getenv("AWS_REGION")))
        .build();

    ObjectNode header = JSON.createObjectNode();
    header.put("truth", 1);
    header.put("driver", "java");
    header.put("language", "java");
    header.put("sdk", "aws-sdk-java-v2 " + VersionInfo.SDK_VERSION);
    header.put("package", "trailbeam-aws-sdk-v2 " + Trailbeam.version());
    header.put("java", System.getProperty("java.version"));
    String agent = System.getenv("JAVA_TOOL_OPTIONS");
    header.put("app_tracing", agent == null || agent.isEmpty() ? "none" : agent);
    write(header);

    // 1. Queues
    String dlq = sqs.createQueue(b -> b.queueName(run + "-dlq")).queueUrl();
    String dlqArn = sqs.getQueueAttributes(b -> b.queueUrl(dlq).attributeNames(QueueAttributeName.QUEUE_ARN))
        .attributes().get(QueueAttributeName.QUEUE_ARN);
    Map<QueueAttributeName, String> qa = new HashMap<>();
    qa.put(QueueAttributeName.VISIBILITY_TIMEOUT, "2");
    qa.put(QueueAttributeName.REDRIVE_POLICY,
        "{\"deadLetterTargetArn\":\"" + dlqArn + "\",\"maxReceiveCount\":\"2\"}");
    String q = sqs.createQueue(b -> b.queueName(run).attributes(qa)).queueUrl();
    String pq = sqs.createQueue(b -> b.queueName(run + "-purge")).queueUrl();

    // 2. Send and batch send
    Map<Integer, String> ids = new HashMap<>();
    ids.put(1, send(q, 1));
    Map<String, String> bodies = Map.of("e2", "{\"n\": 2}", "e3", "{\"n\": 3}");
    SendMessageBatchResponse sb = sqs.sendMessageBatch(b -> b.queueUrl(q).entries(
        SendMessageBatchRequestEntry.builder().id("e2").messageBody(bodies.get("e2")).build(),
        SendMessageBatchRequestEntry.builder().id("e3").messageBody(bodies.get("e3")).build()));
    ObjectNode br = row("SendMessageBatch", q);
    br.put("ok", true);
    ArrayNode be = br.putArray("entries");
    for (SendMessageBatchResultEntry s : sb.successful()) {
      ObjectNode o = be.addObject();
      o.put("id", s.id());
      o.put("ok", true);
      o.put("message_id", s.messageId());
      o.put("body", bodies.get(s.id()));
      ids.put(Integer.parseInt(s.id().substring(1)), s.messageId());
    }
    for (BatchResultErrorEntry f : sb.failed()) {
      ObjectNode o = be.addObject();
      o.put("id", f.id());
      o.put("ok", false);
      o.put("error_code", f.code());
    }
    write(br);

    // 3. Receive all three; these receives ask for one system attribute of their own
    Map<String, Message> got = receiveUntil(q, List.of(ids.get(1), ids.get(2), ids.get(3)), List.of("SenderId"));

    // 4. Delete n=1; batch delete n=2 and a bogus handle (one failed entry)
    delete(q, got.get(ids.get(1)).receiptHandle());
    List<String[]> de = new ArrayList<>();
    de.add(new String[] {"d2", got.get(ids.get(2)).receiptHandle()});
    de.add(new String[] {"bogus", "not-a-receipt-handle"});
    DeleteMessageBatchResponse dr = sqs.deleteMessageBatch(b -> b.queueUrl(q).entries(
        DeleteMessageBatchRequestEntry.builder().id(de.get(0)[0]).receiptHandle(de.get(0)[1]).build(),
        DeleteMessageBatchRequestEntry.builder().id(de.get(1)[0]).receiptHandle(de.get(1)[1]).build()));
    batchRows("DeleteMessageBatch", q, de, null, dr.failed());

    // 5. Visibility-timeout retry: n=3 comes back with receive count 2
    Thread.sleep(3000);
    receiveUntil(q, List.of(ids.get(3)), List.of());

    // 6. DLQ move: ElasticMQ moves n=3 on the next receive of the source queue
    Thread.sleep(3000);
    receive(q, List.of());
    Map<String, Message> d = receiveUntil(dlq, List.of(ids.get(3)), List.of());
    delete(dlq, d.get(ids.get(3)).receiptHandle());

    // 7. ChangeMessageVisibility(0), receive again, ChangeMessageVisibilityBatch, delete
    ids.put(4, send(q, 4));
    Message m = receiveUntil(q, List.of(ids.get(4)), List.of()).get(ids.get(4));
    String h = m.receiptHandle();
    sqs.changeMessageVisibility(b -> b.queueUrl(q).receiptHandle(h).visibilityTimeout(0));
    ObjectNode cv = row("ChangeMessageVisibility", q);
    cv.put("receipt_handle", h);
    cv.put("visibility_timeout", 0);
    cv.put("ok", true);
    write(cv);
    Message m2 = receiveUntil(q, List.of(ids.get(4)), List.of()).get(ids.get(4));
    List<String[]> ve = new ArrayList<>();
    ve.add(new String[] {"v4", m2.receiptHandle()});
    ChangeMessageVisibilityBatchResponse vr = sqs.changeMessageVisibilityBatch(b -> b.queueUrl(q).entries(
        ChangeMessageVisibilityBatchRequestEntry.builder().id("v4").receiptHandle(m2.receiptHandle())
            .visibilityTimeout(30).build()));
    batchRows("ChangeMessageVisibilityBatch", q, ve, 30, vr.failed());
    delete(q, m2.receiptHandle());

    // 8. A delete with an invalid handle: the call fails
    delete(q, "definitely-not-a-valid-receipt-handle");

    // 9. Purge
    sqs.purgeQueue(b -> b.queueUrl(pq));
    ObjectNode pr = row("PurgeQueue", pq);
    pr.put("ok", true);
    write(pr);

    // 10. Flush
    sqs.close();
    if (!Trailbeam.flush(10, TimeUnit.SECONDS)) {
      throw new IllegalStateException("trailbeam flush timed out");
    }
    Trailbeam.shutdown();
    truth.close();
    ObjectNode done = JSON.createObjectNode();
    done.put("driver", "java");
    done.put("run", run);
    System.out.println(JSON.writeValueAsString(done));
  }
}
