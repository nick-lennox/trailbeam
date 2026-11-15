'use strict';
// End to end against a local SQS emulator (ElasticMQ or LocalStack), with the stock
// @opentelemetry/instrumentation-aws-sdk installed by the "application". Skipped unless
// TRAILBEAM_TEST_SQS_ENDPOINT is set, for example:
//
//     java -jar elasticmq-server.jar &          # listens on http://localhost:9324
//     TRAILBEAM_TEST_SQS_ENDPOINT=http://localhost:9324 npm test
const test = require('node:test');
const assert = require('node:assert');

const ENDPOINT = process.env.TRAILBEAM_TEST_SQS_ENDPOINT;

test('send, receive, change visibility, delete (single and batch) against the emulator', { skip: !ENDPOINT && 'TRAILBEAM_TEST_SQS_ENDPOINT not set' }, async (t) => {
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
  const trailbeam = require('../src/index');
  const { memoryExporter, byName } = require('./helpers');

  const KEY = 'emulator-test-key-0123456789';
  const { exporter, spanProcessor } = memoryExporter();
  trailbeam.install({ receiptHashKey: KEY, spanProcessor });
  t.after(() => trailbeam.uninstall());
  const sqs = trailbeam.instrument(new S.SQSClient({
    region: 'us-east-1', endpoint: ENDPOINT, credentials: { accessKeyId: 'x', secretAccessKey: 'x' },
  }));

  const name = `trailbeam-node-test-${process.pid}-${Date.now()}`;
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

  const sends = byName(spans, 'trailbeam send');
  assert.deepStrictEqual(sends.map((s) => attr(s, 'messaging.message.id')), sentIds);

  const receives = byName(spans, 'trailbeam receive');
  assert.strictEqual(receives.length, 4);
  for (const s of receives) {
    assert.ok(Number.isInteger(attr(s, 'trailbeam.sent_timestamp_ms')));
    assert.strictEqual(attr(s, 'messaging.destination.name'), name);
    assert.strictEqual(attr(s, 'aws.sqs.queue.url'), QueueUrl);
  }
  const firstReceives = receives.filter((s) => attr(s, 'messaging.message.id') === first.MessageId);
  assert.deepStrictEqual(firstReceives.map((s) => attr(s, 'trailbeam.receive_count')), [1, 2]);

  const [cv] = byName(spans, 'trailbeam change_visibility');
  assert.strictEqual(attr(cv, 'trailbeam.visibility_timeout'), 0);
  assert.strictEqual(attr(cv, 'trailbeam.result'), 'ok');
  assert.strictEqual(attr(cv, 'trailbeam.receipt_handle.hmac'), attr(firstReceives[0], 'trailbeam.receipt_handle.hmac'));

  // DeleteMessage, DeleteMessageBatch (3 entries), then the DeleteMessage that threw.
  const deletes = byName(spans, 'trailbeam delete');
  assert.deepStrictEqual(deletes.map((s) => [attr(s, 'messaging.operation.name'), attr(s, 'trailbeam.batch_entry_id'), attr(s, 'trailbeam.result')]), [
    ['DeleteMessage', undefined, 'ok'],
    ['DeleteMessageBatch', 'd0', 'ok'],
    ['DeleteMessageBatch', 'd1', 'ok'],
    ['DeleteMessageBatch', 'bogus', 'failed'],
    ['DeleteMessage', undefined, 'failed'],
  ]);
  const [single, d0, d1, bogus, thrown] = deletes;
  assert.strictEqual(attr(single, 'messaging.message.id'), first.MessageId);
  assert.strictEqual(attr(single, 'trailbeam.receipt_handle.hmac'), attr(firstReceives[1], 'trailbeam.receipt_handle.hmac'),
    'the delete matches the latest receive');
  assert.deepStrictEqual([d0, d1].map((s) => attr(s, 'messaging.message.id')), rest.map((m) => m.MessageId));
  assert.strictEqual(attr(bogus, 'trailbeam.error_code'), 'ReceiptHandleIsInvalid');
  assert.strictEqual(attr(thrown, 'trailbeam.error_code'), 'ReceiptHandleIsInvalid');
  assert.ok(!('messaging.message.id' in bogus.attributes));

  // Every Trailbeam span is a child of the stock span for its call, although the application samples
  // nothing: Trailbeam has its own always-on provider.
  assert.strictEqual(appExporter.getFinishedSpans().length, 0);
  for (const s of spans) {
    assert.ok(s.parentSpanContext && s.parentSpanContext.spanId, `${s.name} has a parent`);
    assert.strictEqual(s.parentSpanContext.traceId, s.spanContext().traceId);
  }
  const batchSendParents = new Set(sends.slice(1).map((s) => s.parentSpanContext.spanId));
  assert.strictEqual(batchSendParents.size, 1, 'batch entries share the one stock span');
});

test('dead-letter queue receive carries DeadLetterQueueSourceArn (schema 3.1)', { skip: !ENDPOINT && 'TRAILBEAM_TEST_SQS_ENDPOINT not set' }, async (t) => {
  // The move itself has no API call to observe. SQS reports it on the dead-letter receive: ElasticMQ
  // 1.6.15 sets DeadLetterQueueSourceArn there, with the same MessageId (checked for QV-31).
  const S = require('@aws-sdk/client-sqs');
  const trailbeam = require('../src/index');
  const { memoryExporter, byName } = require('./helpers');
  const { exporter, spanProcessor } = memoryExporter();
  trailbeam.install({ receiptHashKey: 'emulator-test-key-0123456789', spanProcessor });
  t.after(() => trailbeam.uninstall());
  const sqs = trailbeam.instrument(new S.SQSClient({
    region: 'us-east-1', endpoint: ENDPOINT, credentials: { accessKeyId: 'x', secretAccessKey: 'x' },
  }));
  const name = `trailbeam-node-dlq-${process.pid}-${Date.now()}`;
  const dlq = (await sqs.send(new S.CreateQueueCommand({ QueueName: `${name}-dlq` }))).QueueUrl;
  t.after(() => sqs.send(new S.DeleteQueueCommand({ QueueUrl: dlq })));
  const dlqArn = (await sqs.send(new S.GetQueueAttributesCommand({ QueueUrl: dlq, AttributeNames: ['QueueArn'] }))).Attributes.QueueArn;
  const src = (await sqs.send(new S.CreateQueueCommand({
    QueueName: name,
    Attributes: { RedrivePolicy: JSON.stringify({ deadLetterTargetArn: dlqArn, maxReceiveCount: '1' }) },
  }))).QueueUrl;
  t.after(() => sqs.send(new S.DeleteQueueCommand({ QueueUrl: src })));
  const srcArn = (await sqs.send(new S.GetQueueAttributesCommand({ QueueUrl: src, AttributeNames: ['QueueArn'] }))).Attributes.QueueArn;

  const { MessageId } = await sqs.send(new S.SendMessageCommand({ QueueUrl: src, MessageBody: 'poison' }));
  const first = await sqs.send(new S.ReceiveMessageCommand({ QueueUrl: src, VisibilityTimeout: 0, WaitTimeSeconds: 1 }));
  assert.strictEqual(first.Messages[0].MessageId, MessageId);
  // The next receive on the source queue moves it (receive count 1 has reached maxReceiveCount 1).
  let moved;
  for (let i = 0; i < 10 && !moved; i++) {
    await sqs.send(new S.ReceiveMessageCommand({ QueueUrl: src, WaitTimeSeconds: 0 }));
    moved = ((await sqs.send(new S.ReceiveMessageCommand({ QueueUrl: dlq, WaitTimeSeconds: 1 }))).Messages || [])[0];
  }
  assert.ok(moved, 'the message reached the dead-letter queue');
  assert.strictEqual(moved.MessageId, MessageId);
  assert.strictEqual(moved.Attributes.DeadLetterQueueSourceArn, srcArn, 'Trailbeam asked for it without the caller asking');

  const receives = byName(exporter.getFinishedSpans(), 'trailbeam receive');
  const onSrc = receives.filter((s) => s.attributes['messaging.destination.name'] === name);
  const onDlq = receives.filter((s) => s.attributes['messaging.destination.name'] === `${name}-dlq`);
  assert.ok(onSrc.length >= 1 && onDlq.length === 1);
  for (const s of onSrc) assert.ok(!('trailbeam.dead_letter_queue_source_arn' in s.attributes));
  assert.strictEqual(onDlq[0].attributes['trailbeam.dead_letter_queue_source_arn'], srcArn);
  assert.strictEqual(onDlq[0].attributes['messaging.message.id'], MessageId);
});
