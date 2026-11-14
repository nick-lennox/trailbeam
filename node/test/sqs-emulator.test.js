'use strict';
// End to end against a local SQS emulator (ElasticMQ or LocalStack), with the stock
// @opentelemetry/instrumentation-aws-sdk installed by the "application". Skipped unless
// QV2_TEST_SQS_ENDPOINT is set, for example:
//
//     java -jar elasticmq-server.jar &          # listens on http://localhost:9324
//     QV2_TEST_SQS_ENDPOINT=http://localhost:9324 npm test
const test = require('node:test');
const assert = require('node:assert');

const ENDPOINT = process.env.QV2_TEST_SQS_ENDPOINT;

test('send, receive, change visibility, delete (single and batch) against the emulator', { skip: !ENDPOINT && 'QV2_TEST_SQS_ENDPOINT not set' }, async (t) => {
  // The application's own OpenTelemetry setup: a provider that samples nothing, and the stock
  // AWS SDK instrumentation. It must be registered before the SDK is loaded.
  const api = require('@opentelemetry/api');
  const { AsyncLocalStorageContextManager } = require('@opentelemetry/context-async-hooks');
  const { BasicTracerProvider, AlwaysOffSampler, InMemorySpanExporter, SimpleSpanProcessor } = require('@opentelemetry/sdk-trace-base');
  const { registerInstrumentations } = require('@opentelemetry/instrumentation');
  const { AwsInstrumentation } = require('@opentelemetry/instrumentation-aws-sdk');
  api.context.setGlobalContextManager(new AsyncLocalStorageContextManager().enable());
  const appExporter = new InMemorySpanExporter();
  const appProvider = new BasicTracerProvider({ sampler: new AlwaysOffSampler(), spanProcessors: [new SimpleSpanProcessor(appExporter)] });
  registerInstrumentations({ tracerProvider: appProvider, instrumentations: [new AwsInstrumentation()] });

  const S = require('@aws-sdk/client-sqs');
  const qv2 = require('../src/index');
  const { memoryExporter, byName } = require('./helpers');

  const KEY = 'emulator-test-key-0123456789';
  const { exporter, spanProcessor } = memoryExporter();
  qv2.install({ receiptHashKey: KEY, spanProcessor });
  t.after(() => qv2.uninstall());
  const sqs = qv2.instrument(new S.SQSClient({
    region: 'us-east-1', endpoint: ENDPOINT, credentials: { accessKeyId: 'x', secretAccessKey: 'x' },
  }));

  const name = `qv2-node-test-${process.pid}-${Date.now()}`;
  const { QueueUrl } = await sqs.send(new S.CreateQueueCommand({ QueueName: name }));
  t.after(() => sqs.send(new S.DeleteQueueCommand({ QueueUrl })));

  const sent = await sqs.send(new S.SendMessageCommand({ QueueUrl, MessageBody: 'one' }));
  const batch = await sqs.send(new S.SendMessageBatchCommand({
    QueueUrl, Entries: [{ Id: 'b1', MessageBody: 'two' }, { Id: 'b2', MessageBody: 'three' }],
  }));
  const sentIds = [sent.MessageId, ...batch.Successful.map((e) => e.MessageId)];

  const received = [];
  for (let i = 0; i < 10 && received.length < 3; i++) {
    const r = await sqs.send(new S.ReceiveMessageCommand({ QueueUrl, MaxNumberOfMessages: 10, WaitTimeSeconds: 1 }));
    received.push(...(r.Messages || []));
  }
  assert.deepStrictEqual(received.map((m) => m.MessageId).sort(), [...sentIds].sort());
  const [first, ...rest] = received;

  // Make the first message visible again, receive it a second time and delete it.
  await sqs.send(new S.ChangeMessageVisibilityCommand({ QueueUrl, ReceiptHandle: first.ReceiptHandle, VisibilityTimeout: 0 }));
  let again;
  for (let i = 0; i < 10 && !again; i++) {
    const r = await sqs.send(new S.ReceiveMessageCommand({ QueueUrl, WaitTimeSeconds: 1 }));
    again = (r.Messages || [])[0];
  }
  assert.strictEqual(again.MessageId, first.MessageId);
  await sqs.send(new S.DeleteMessageCommand({ QueueUrl, ReceiptHandle: again.ReceiptHandle }));
  await sqs.send(new S.DeleteMessageBatchCommand({
    QueueUrl,
    Entries: [...rest.map((m, i) => ({ Id: `d${i}`, ReceiptHandle: m.ReceiptHandle })), { Id: 'bogus', ReceiptHandle: 'not-a-receipt-handle' }],
  }));
  await assert.rejects(sqs.send(new S.DeleteMessageCommand({ QueueUrl, ReceiptHandle: 'not-a-receipt-handle' })));

  const spans = exporter.getFinishedSpans();
  const attr = (s, k) => s.attributes[k];

  const sends = byName(spans, 'qv2 send');
  assert.deepStrictEqual(sends.map((s) => attr(s, 'messaging.message.id')), sentIds);

  const receives = byName(spans, 'qv2 receive');
  assert.strictEqual(receives.length, 4);
  for (const s of receives) {
    assert.ok(Number.isInteger(attr(s, 'qv2.sent_timestamp_ms')));
    assert.strictEqual(attr(s, 'messaging.destination.name'), name);
    assert.strictEqual(attr(s, 'aws.sqs.queue.url'), QueueUrl);
  }
  const firstReceives = receives.filter((s) => attr(s, 'messaging.message.id') === first.MessageId);
  assert.deepStrictEqual(firstReceives.map((s) => attr(s, 'qv2.receive_count')), [1, 2]);

  const [cv] = byName(spans, 'qv2 change_visibility');
  assert.strictEqual(attr(cv, 'qv2.visibility_timeout'), 0);
  assert.strictEqual(attr(cv, 'qv2.result'), 'ok');
  assert.strictEqual(attr(cv, 'qv2.receipt_handle.hmac'), attr(firstReceives[0], 'qv2.receipt_handle.hmac'));

  // DeleteMessage, DeleteMessageBatch (3 entries), then the DeleteMessage that threw.
  const deletes = byName(spans, 'qv2 delete');
  assert.deepStrictEqual(deletes.map((s) => [attr(s, 'messaging.operation.name'), attr(s, 'qv2.batch_entry_id'), attr(s, 'qv2.result')]), [
    ['DeleteMessage', undefined, 'ok'],
    ['DeleteMessageBatch', 'd0', 'ok'],
    ['DeleteMessageBatch', 'd1', 'ok'],
    ['DeleteMessageBatch', 'bogus', 'failed'],
    ['DeleteMessage', undefined, 'failed'],
  ]);
  const [single, d0, d1, bogus, thrown] = deletes;
  assert.strictEqual(attr(single, 'messaging.message.id'), first.MessageId);
  assert.strictEqual(attr(single, 'qv2.receipt_handle.hmac'), attr(firstReceives[1], 'qv2.receipt_handle.hmac'),
    'the delete matches the latest receive');
  assert.deepStrictEqual([d0, d1].map((s) => attr(s, 'messaging.message.id')), rest.map((m) => m.MessageId));
  assert.strictEqual(attr(bogus, 'qv2.error_code'), 'ReceiptHandleIsInvalid');
  assert.strictEqual(attr(thrown, 'qv2.error_code'), 'ReceiptHandleIsInvalid');
  assert.ok(!('messaging.message.id' in bogus.attributes));

  // Every qv2 span is a child of the stock span for its call, although the application samples
  // nothing: qv2 has its own always-on provider.
  assert.strictEqual(appExporter.getFinishedSpans().length, 0);
  for (const s of spans) {
    assert.ok(s.parentSpanContext && s.parentSpanContext.spanId, `${s.name} has a parent`);
    assert.strictEqual(s.parentSpanContext.traceId, s.spanContext().traceId);
  }
  const batchSendParents = new Set(sends.slice(1).map((s) => s.parentSpanContext.spanId));
  assert.strictEqual(batchSendParents.size, 1, 'batch entries share the one stock span');
});
