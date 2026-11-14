'use strict';
// AWS SDK v3 hooks (contract sections 3 to 5 and 8), with canned SQS responses.
const test = require('node:test');
const assert = require('node:assert');
const api = require('@opentelemetry/api');
const { AsyncLocalStorageContextManager } = require('@opentelemetry/context-async-hooks');
const { BasicTracerProvider } = require('@opentelemetry/sdk-trace-base');
const S = require('@aws-sdk/client-sqs');

const qv2 = require('../src/index');
const { addReceiveAttributeNames, errorCode, plainCode, MIDDLEWARE_NAME } = require('../src/sqs');
const order = require('../src/order');
const { memoryExporter, stubClient, serviceError, byName } = require('./helpers');
const { version } = require('../package.json');

api.context.setGlobalContextManager(new AsyncLocalStorageContextManager().enable());

const KEY = 'unit-test-key-0123456789';
const URL = 'http://localhost:9324/000000000000/orders';
const INSTANCE_ID = 'test-instance-6f1c2a1e';
const RH = (n) => `AQEB-receipt-handle-${n}-${'x'.repeat(40)}`;

/** Fresh install + stub client per test. `responses` maps operation -> function(input). */
function setup(t, responses = {}) {
  process.env.OTEL_RESOURCE_ATTRIBUTES = `service.instance.id=${INSTANCE_ID}`;
  process.env.OTEL_SERVICE_NAME = 'orders-worker';
  const { exporter, spanProcessor } = memoryExporter();
  qv2.install({ receiptHashKey: KEY, spanProcessor });
  const { client, calls } = stubClient((op, input) => {
    const f = responses[op];
    if (!f) return {};
    return f(input);
  });
  qv2.instrument(client);
  t.after(async () => {
    delete process.env.OTEL_RESOURCE_ATTRIBUTES;
    delete process.env.OTEL_SERVICE_NAME;
    await qv2.uninstall();
  });
  return { client, calls, spans: () => exporter.getFinishedSpans() };
}

const message = (n, attrs) => ({
  MessageId: `mid-${n}`,
  ReceiptHandle: RH(n),
  Body: 'not read',
  Attributes: attrs === undefined
    ? { ApproximateReceiveCount: '2', SentTimestamp: '1763080000000', ApproximateFirstReceiveTimestamp: '1763080000500', AWSTraceHeader: 'Root=1-abc' }
    : attrs,
});

const COMMON = (operation, kind, opType) => ({
  'qv2.schema_version': 3,
  'qv2.order.scope': order.scope,
  'qv2.kind': kind,
  'qv2.source': 'client',
  'messaging.system.name': 'aws.sqs',
  'messaging.operation.type': opType,
  'messaging.operation.name': operation,
  'messaging.destination.name': 'orders',
  'aws.sqs.queue.url': URL,
  'messaging.client.id': INSTANCE_ID,
  'process.pid': process.pid,
});

/** The attributes without the call-order ticks, after checking that issued < returned (contract v3). */
function withoutTicks(attrs) {
  const { 'qv2.order.issued': issued, 'qv2.order.returned': returned, ...rest } = attrs;
  assert.ok(Number.isInteger(issued) && issued > 0 && issued < returned, `issued ${issued}, returned ${returned}`);
  return rest;
}

test('SendMessage: one qv2 send with the message ID', async (t) => {
  const { client, spans } = setup(t, { SendMessage: () => ({ MessageId: 'mid-1' }) });
  await client.send(new S.SendMessageCommand({ QueueUrl: URL, MessageBody: 'hello' }));
  const [s, ...rest] = spans();
  assert.strictEqual(rest.length, 0);
  assert.strictEqual(s.name, 'qv2 send');
  assert.deepStrictEqual(withoutTicks(s.attributes), { ...COMMON('SendMessage', 'sqs.send', 'send'), 'messaging.message.id': 'mid-1' });
});

test('every qv2 span: scope, kind, an interval, resource, no thread.name in Node.js', async (t) => {
  const { client, spans } = setup(t, { SendMessage: () => ({ MessageId: 'mid-1' }) });
  await client.send(new S.SendMessageCommand({ QueueUrl: URL, MessageBody: 'hello' }));
  const [s] = spans();
  assert.strictEqual(s.instrumentationScope.name, 'qv2');
  assert.strictEqual(s.instrumentationScope.version, version);
  assert.strictEqual(s.kind, api.SpanKind.INTERNAL);
  assert.ok(ms(s.startTime) <= ms(s.endTime));
  assert.ok(!('qv2.start_observed' in s.attributes));
  assert.strictEqual(s.resource.attributes['service.name'], 'orders-worker');
  assert.strictEqual(s.resource.attributes['service.instance.id'], INSTANCE_ID);
  assert.ok(!('thread.name' in s.attributes));
});

// -- interval (contract v2, section 2) -------------------------------------------------------

/** An OpenTelemetry HrTime as epoch milliseconds. */
const ms = ([sec, ns]) => sec * 1e3 + ns / 1e6;
const sleep = (n) => new Promise((resolve) => { setTimeout(resolve, n); });

test('interval: starts before the request is sent, ends after the response arrives', async (t) => {
  let sentAt;
  const { client, spans } = setup(t, {
    SendMessage: async () => { sentAt = Date.now(); await sleep(30); return { MessageId: 'mid-1' }; },
  });
  const before = Date.now();
  await client.send(new S.SendMessageCommand({ QueueUrl: URL, MessageBody: 'hello' }));
  const after = Date.now();
  const [s] = spans();
  const [start, end] = [ms(s.startTime), ms(s.endTime)];
  assert.ok(before <= start && start <= sentAt, `start ${start} not in [${before}, ${sentAt}]`);
  assert.ok(sentAt + 25 <= end && end <= after, `end ${end} not in [${sentAt + 25}, ${after}]`);
});

test('interval: every entry of a batch call gets the call\'s interval', async (t) => {
  const { client, spans } = setup(t, {
    ReceiveMessage: async () => { await sleep(10); return { Messages: [message(1), message(2)] }; },
    DeleteMessageBatch: async () => { await sleep(10); return { Successful: [{ Id: 'a' }], Failed: [{ Id: 'b', Code: 'X' }] }; },
  });
  await client.send(new S.ReceiveMessageCommand({ QueueUrl: URL }));
  await client.send(new S.DeleteMessageBatchCommand({ QueueUrl: URL, Entries: [
    { Id: 'a', ReceiptHandle: RH(1) }, { Id: 'b', ReceiptHandle: RH(2) }] }));
  for (const name of ['qv2 receive', 'qv2 delete']) {
    const got = byName(spans(), name);
    assert.strictEqual(got.length, 2);
    assert.deepStrictEqual(got[0].startTime, got[1].startTime);
    assert.deepStrictEqual(got[0].endTime, got[1].endTime);
    assert.ok(ms(got[0].endTime) - ms(got[0].startTime) >= 5, name);
  }
  // One caller's sequential calls do not overlap.
  assert.ok(ms(byName(spans(), 'qv2 receive')[0].endTime) <= ms(byName(spans(), 'qv2 delete')[0].startTime));
});

test('interval: a settle call that throws still ends when the error arrived', async (t) => {
  const { client, spans } = setup(t, {
    DeleteMessage: async () => { await sleep(20); throw serviceError('ReceiptHandleIsInvalid'); },
  });
  await assert.rejects(client.send(new S.DeleteMessageCommand({ QueueUrl: URL, ReceiptHandle: RH(1) })));
  const [s] = spans();
  assert.ok(ms(s.endTime) - ms(s.startTime) >= 15);
});

test('SqsHooks: spans take the start and end times they are given', () => {
  const { exporter, spanProcessor } = memoryExporter();
  const provider = new BasicTracerProvider({ spanProcessors: [spanProcessor] });
  const { Emitter } = require('../src/emitter');
  const { SqsHooks } = require('../src/sqs');
  const hooks = new SqsHooks(new Emitter(provider, { hash: () => 'h', keyId: 'k' }, 'client', 'c'));
  hooks.onSend('SendMessage', { QueueUrl: URL }, { MessageId: 'm' }, undefined, { startTime: 1763080000000, endTime: 1763080000250 });
  const [s] = exporter.getFinishedSpans();
  assert.deepStrictEqual([ms(s.startTime), ms(s.endTime)], [1763080000000, 1763080000250]);
  // No ticks given: no order attributes, never placeholders.
  for (const k of ['qv2.order.scope', 'qv2.order.issued', 'qv2.order.returned']) assert.ok(!(k in s.attributes), k);
});

// -- call order inside one process (contract v3) ---------------------------------------------

const ticks = (s) => [s.attributes['qv2.order.issued'], s.attributes['qv2.order.returned']];

test('order: a call that returned before the next was issued is ordered before it', async (t) => {
  const { client, spans } = setup(t, {
    SendMessage: () => ({ MessageId: 'mid-1' }),
    ReceiveMessage: () => ({ Messages: [message(1)] }),
  });
  await client.send(new S.SendMessageCommand({ QueueUrl: URL, MessageBody: 'hello' }));
  await client.send(new S.ReceiveMessageCommand({ QueueUrl: URL }));
  await client.send(new S.DeleteMessageCommand({ QueueUrl: URL, ReceiptHandle: RH(1) }));
  const [send, rcv, del] = ['qv2 send', 'qv2 receive', 'qv2 delete'].map((n) => byName(spans(), n)[0]);
  for (const s of [send, rcv, del]) {
    assert.ok(ticks(s)[0] < ticks(s)[1], s.name);
    assert.strictEqual(s.attributes['qv2.order.scope'], order.scope);
  }
  assert.ok(ticks(send)[1] < ticks(rcv)[0]);
  assert.ok(ticks(rcv)[1] < ticks(del)[0]);
});

test('order: concurrent calls are not ordered against each other', async (t) => {
  let release;
  const gate = new Promise((resolve) => { release = resolve; });
  let n = 0;
  const { client, spans } = setup(t, { SendMessage: async () => { n += 1; const id = `mid-${n}`; await gate; return { MessageId: id }; } });
  const a = client.send(new S.SendMessageCommand({ QueueUrl: URL, MessageBody: 'a' }));
  const b = client.send(new S.SendMessageCommand({ QueueUrl: URL, MessageBody: 'b' }));
  await sleep(10);
  release();
  await Promise.all([a, b]);
  const [x, y] = spans();
  assert.ok(ticks(x)[1] > ticks(y)[0] && ticks(y)[1] > ticks(x)[0]);
});

test('order: every entry of a batch, and every message of one receive, shares the call\'s ticks', async (t) => {
  const { client, spans } = setup(t, {
    SendMessageBatch: () => ({ Successful: [{ Id: 'a', MessageId: 'm-a' }, { Id: 'b', MessageId: 'm-b' }] }),
    ReceiveMessage: () => ({ Messages: [message(1), message(2)] }),
    DeleteMessageBatch: () => ({ Successful: [{ Id: 'a' }], Failed: [{ Id: 'b', Code: 'X' }] }),
  });
  await client.send(new S.SendMessageBatchCommand({ QueueUrl: URL, Entries: [{ Id: 'a', MessageBody: '1' }, { Id: 'b', MessageBody: '2' }] }));
  await client.send(new S.ReceiveMessageCommand({ QueueUrl: URL }));
  await client.send(new S.DeleteMessageBatchCommand({ QueueUrl: URL, Entries: [
    { Id: 'a', ReceiptHandle: RH(1) }, { Id: 'b', ReceiptHandle: RH(2) }] }));
  const pairs = [];
  for (const name of ['qv2 send', 'qv2 receive', 'qv2 delete']) {
    const got = byName(spans(), name);
    assert.strictEqual(got.length, 2);
    assert.deepStrictEqual(ticks(got[0]), ticks(got[1]), name);
    pairs.push(ticks(got[0]));
  }
  assert.ok(pairs[0][1] < pairs[1][0] && pairs[1][1] < pairs[2][0]);
});

test('order: a settle call that throws gets returned when the error arrived', async (t) => {
  const { client, spans } = setup(t, {
    DeleteMessageBatch: () => { throw serviceError('AWS.SimpleQueueService.TooManyEntriesInBatchRequest'); },
  });
  const before = order.tick();
  await assert.rejects(client.send(new S.DeleteMessageBatchCommand({ QueueUrl: URL, Entries: [
    { Id: 'a', ReceiptHandle: RH(1) }, { Id: 'b', ReceiptHandle: RH(2) }] })));
  const after = order.tick();
  const got = spans();
  assert.strictEqual(got.length, 2);
  assert.deepStrictEqual(ticks(got[0]), ticks(got[1]));
  const [issued, returned] = ticks(got[0]);
  assert.ok(before < issued && issued < returned && returned < after);
  assert.strictEqual(got[0].attributes['qv2.order.scope'], order.scope);
});

test('SendMessageBatch: one span per accepted entry; failed entries get none', async (t) => {
  const { client, spans } = setup(t, {
    SendMessageBatch: () => ({
      Successful: [{ Id: 'a', MessageId: 'mid-a' }, { Id: 'c', MessageId: 'mid-c' }],
      Failed: [{ Id: 'b', Code: 'InvalidParameterValue', SenderFault: true }],
    }),
  });
  await client.send(new S.SendMessageBatchCommand({
    QueueUrl: URL,
    Entries: [{ Id: 'a', MessageBody: '1' }, { Id: 'b', MessageBody: '2' }, { Id: 'c', MessageBody: '3' }],
  }));
  const got = spans().map((s) => [s.name, s.attributes['messaging.operation.name'], s.attributes['messaging.message.id'], s.attributes['qv2.batch_entry_id']]);
  assert.deepStrictEqual(got, [
    ['qv2 send', 'SendMessageBatch', 'mid-a', 'a'],
    ['qv2 send', 'SendMessageBatch', 'mid-c', 'c'],
  ]);
});

test('SendMessage that throws: no span, error reaches the caller', async (t) => {
  const { client, spans } = setup(t, { SendMessage: () => { throw serviceError('QueueDoesNotExist'); } });
  await assert.rejects(client.send(new S.SendMessageCommand({ QueueUrl: URL, MessageBody: 'x' })), { name: 'QueueDoesNotExist' });
  assert.strictEqual(spans().length, 0);
});

test('ReceiveMessage: one qv2 receive per message with the contract attributes', async (t) => {
  const { client, spans } = setup(t, { ReceiveMessage: () => ({ Messages: [message(1), message(2)] }) });
  const out = await client.send(new S.ReceiveMessageCommand({ QueueUrl: URL, MaxNumberOfMessages: 10 }));
  assert.strictEqual(out.Messages.length, 2, 'the caller still gets the response');
  const rs = spans();
  assert.strictEqual(rs.length, 2);
  assert.deepStrictEqual(withoutTicks(rs[0].attributes), {
    ...COMMON('ReceiveMessage', 'sqs.receive', 'receive'),
    'messaging.message.id': 'mid-1',
    'qv2.receipt_handle.hmac': qv2.hashReceiptHandle(KEY, RH(1)),
    'qv2.receipt_handle.key_id': qv2.keyId(KEY),
    'qv2.receive_count': 2,
    'qv2.sent_timestamp_ms': 1763080000000,
    'qv2.first_receive_timestamp_ms': 1763080000500,
    'qv2.aws_trace_header': 'Root=1-abc',
  });
  assert.strictEqual(rs[1].attributes['messaging.message.id'], 'mid-2');
});

test('ReceiveMessage: missing system attributes are omitted, not sent as empty or -1', async (t) => {
  const { client, spans } = setup(t, { ReceiveMessage: () => ({ Messages: [message(1, {})] }) });
  await client.send(new S.ReceiveMessageCommand({ QueueUrl: URL }));
  const a = spans()[0].attributes;
  for (const k of ['qv2.receive_count', 'qv2.sent_timestamp_ms', 'qv2.first_receive_timestamp_ms', 'qv2.aws_trace_header']) {
    assert.ok(!(k in a), k);
  }
  for (const v of Object.values(a)) assert.ok(v !== '' && v !== -1);
});

test('ReceiveMessage with no messages: no span', async (t) => {
  const { client, spans } = setup(t, { ReceiveMessage: () => ({}) });
  await client.send(new S.ReceiveMessageCommand({ QueueUrl: URL }));
  assert.strictEqual(spans().length, 0);
});

test('ReceiveMessage request asks for the system attributes, without touching the caller\'s input', async (t) => {
  const { client, calls } = setup(t, { ReceiveMessage: () => ({}) });
  const input = { QueueUrl: URL, MessageSystemAttributeNames: ['SenderId'], MessageAttributeNames: ['trace'] };
  await client.send(new S.ReceiveMessageCommand(input));
  assert.deepStrictEqual(calls[0].input.MessageSystemAttributeNames,
    ['SenderId', 'ApproximateReceiveCount', 'SentTimestamp', 'ApproximateFirstReceiveTimestamp', 'AWSTraceHeader']);
  assert.deepStrictEqual(calls[0].input.MessageAttributeNames, ['trace'], 'message attributes are left alone');
  assert.deepStrictEqual(input.MessageSystemAttributeNames, ['SenderId'], 'caller input not mutated');
});

test('addReceiveAttributeNames: All, legacy AttributeNames, nothing missing, no message attributes', () => {
  assert.strictEqual(addReceiveAttributeNames({ MessageSystemAttributeNames: ['All'] }), undefined);
  assert.strictEqual(addReceiveAttributeNames({ AttributeNames: ['All'] }), undefined);
  assert.deepStrictEqual(addReceiveAttributeNames({ AttributeNames: ['SentTimestamp'] }), {
    AttributeNames: ['SentTimestamp', 'ApproximateReceiveCount', 'ApproximateFirstReceiveTimestamp', 'AWSTraceHeader'],
  });
  const all4 = ['ApproximateReceiveCount', 'SentTimestamp', 'ApproximateFirstReceiveTimestamp', 'AWSTraceHeader'];
  assert.strictEqual(addReceiveAttributeNames({ MessageSystemAttributeNames: all4 }), undefined);
  const added = addReceiveAttributeNames({ QueueUrl: URL });
  assert.deepStrictEqual(added, { QueueUrl: URL, MessageSystemAttributeNames: all4 });
  assert.ok(!('MessageAttributeNames' in added));
});

test('DeleteMessage: emitted after the response, with qv2.result ok and the message ID from the map', async (t) => {
  const { client, spans } = setup(t, { ReceiveMessage: () => ({ Messages: [message(1)] }) });
  await client.send(new S.ReceiveMessageCommand({ QueueUrl: URL }));
  await client.send(new S.DeleteMessageCommand({ QueueUrl: URL, ReceiptHandle: RH(1) }));
  const [d] = byName(spans(), 'qv2 delete');
  assert.deepStrictEqual(withoutTicks(d.attributes), {
    ...COMMON('DeleteMessage', 'sqs.settle', 'settle'),
    'qv2.receipt_handle.hmac': qv2.hashReceiptHandle(KEY, RH(1)),
    'qv2.receipt_handle.key_id': qv2.keyId(KEY),
    'messaging.message.id': 'mid-1',
    'qv2.result': 'ok',
  });
  // A successful delete drops the handle from the map.
  await client.send(new S.DeleteMessageCommand({ QueueUrl: URL, ReceiptHandle: RH(1) }));
  assert.ok(!('messaging.message.id' in byName(spans(), 'qv2 delete')[1].attributes));
});

test('DeleteMessage of a handle this process never received: hash only', async (t) => {
  const { client, spans } = setup(t);
  await client.send(new S.DeleteMessageCommand({ QueueUrl: URL, ReceiptHandle: RH(9) }));
  const [d] = spans();
  assert.strictEqual(d.attributes['qv2.receipt_handle.hmac'], qv2.hashReceiptHandle(KEY, RH(9)));
  assert.ok(!('messaging.message.id' in d.attributes));
});

test('DeleteMessage that throws: a failed span with the SQS error code, and the error is rethrown', async (t) => {
  const { client, spans } = setup(t, {
    ReceiveMessage: () => ({ Messages: [message(1)] }),
    DeleteMessage: () => { throw serviceError('ReceiptHandleIsInvalid'); },
  });
  await client.send(new S.ReceiveMessageCommand({ QueueUrl: URL }));
  await assert.rejects(client.send(new S.DeleteMessageCommand({ QueueUrl: URL, ReceiptHandle: RH(1) })), { name: 'ReceiptHandleIsInvalid' });
  const [d] = byName(spans(), 'qv2 delete');
  assert.strictEqual(d.attributes['qv2.result'], 'failed');
  assert.strictEqual(d.attributes['qv2.error_code'], 'ReceiptHandleIsInvalid');
  assert.strictEqual(d.attributes['messaging.message.id'], 'mid-1', 'a failed delete keeps the map entry');
});

test('a non-SQS error (network, timeout) records the error name', async (t) => {
  class TimeoutError extends Error {}
  const { client, spans } = setup(t, { DeleteMessage: () => { throw new TimeoutError('socket timeout'); } });
  await assert.rejects(client.send(new S.DeleteMessageCommand({ QueueUrl: URL, ReceiptHandle: RH(1) })));
  assert.strictEqual(spans()[0].attributes['qv2.error_code'], 'Error');
  assert.strictEqual(errorCode(Object.assign(new Error('x'), { name: 'TimeoutError' })), 'TimeoutError');
  assert.strictEqual(errorCode(new TypeError('x')), 'TypeError');
  assert.strictEqual(errorCode('a string'), 'Error');
});

test('DeleteMessageBatch: one span per entry with its own result', async (t) => {
  const { client, spans } = setup(t, {
    ReceiveMessage: () => ({ Messages: [message(1), message(2)] }),
    DeleteMessageBatch: () => ({
      Successful: [{ Id: 'e1' }],
      Failed: [{ Id: 'e2', Code: 'ReceiptHandleIsInvalid', SenderFault: true }],
    }),
  });
  await client.send(new S.ReceiveMessageCommand({ QueueUrl: URL }));
  await client.send(new S.DeleteMessageBatchCommand({
    QueueUrl: URL,
    Entries: [{ Id: 'e1', ReceiptHandle: RH(1) }, { Id: 'e2', ReceiptHandle: RH(2) }, { Id: 'e3', ReceiptHandle: RH(3) }],
  }));
  const got = byName(spans(), 'qv2 delete').map((s) => {
    const a = s.attributes;
    return [a['messaging.operation.name'], a['qv2.batch_entry_id'], a['messaging.message.id'], a['qv2.result'], a['qv2.error_code'], a['qv2.receipt_handle.hmac']];
  });
  assert.deepStrictEqual(got, [
    ['DeleteMessageBatch', 'e1', 'mid-1', 'ok', undefined, qv2.hashReceiptHandle(KEY, RH(1))],
    ['DeleteMessageBatch', 'e2', 'mid-2', 'failed', 'ReceiptHandleIsInvalid', qv2.hashReceiptHandle(KEY, RH(2))],
    ['DeleteMessageBatch', 'e3', undefined, 'failed', 'MissingFromResponse', qv2.hashReceiptHandle(KEY, RH(3))],
  ]);
});

test('namespaced error codes (ElasticMQ 2.x) are recorded as the plain SQS code', async (t) => {
  const { client, spans } = setup(t, {
    DeleteMessageBatch: () => ({ Failed: [{ Id: 'e1', Code: 'com.amazonaws.sqs#ReceiptHandleIsInvalid', SenderFault: true }] }),
  });
  await client.send(new S.DeleteMessageBatchCommand({ QueueUrl: URL, Entries: [{ Id: 'e1', ReceiptHandle: RH(1) }] }));
  assert.strictEqual(spans()[0].attributes['qv2.error_code'], 'ReceiptHandleIsInvalid');
  assert.strictEqual(plainCode('ReceiptHandleIsInvalid'), 'ReceiptHandleIsInvalid');
  assert.strictEqual(plainCode('AWS.SimpleQueueService.TooManyEntriesInBatchRequest'), 'AWS.SimpleQueueService.TooManyEntriesInBatchRequest');
  assert.strictEqual(plainCode('ns#'), undefined);
  assert.strictEqual(errorCode({ Code: 'com.amazonaws.sqs#QueueDoesNotExist' }), 'QueueDoesNotExist');
});

test('DeleteMessageBatch that throws: one failed span per entry', async (t) => {
  const { client, spans } = setup(t, { DeleteMessageBatch: () => { throw serviceError('AWS.SimpleQueueService.TooManyEntriesInBatchRequest'); } });
  await assert.rejects(client.send(new S.DeleteMessageBatchCommand({
    QueueUrl: URL, Entries: [{ Id: 'e1', ReceiptHandle: RH(1) }, { Id: 'e2', ReceiptHandle: RH(2) }],
  })));
  const ds = spans();
  assert.strictEqual(ds.length, 2);
  for (const d of ds) {
    assert.strictEqual(d.attributes['qv2.result'], 'failed');
    assert.strictEqual(d.attributes['qv2.error_code'], 'AWS.SimpleQueueService.TooManyEntriesInBatchRequest');
  }
});

test('ChangeMessageVisibility: qv2.visibility_timeout, including 0', async (t) => {
  const { client, spans } = setup(t, { ReceiveMessage: () => ({ Messages: [message(1)] }) });
  await client.send(new S.ReceiveMessageCommand({ QueueUrl: URL }));
  await client.send(new S.ChangeMessageVisibilityCommand({ QueueUrl: URL, ReceiptHandle: RH(1), VisibilityTimeout: 0 }));
  const [c] = byName(spans(), 'qv2 change_visibility');
  assert.deepStrictEqual(withoutTicks(c.attributes), {
    ...COMMON('ChangeMessageVisibility', 'sqs.settle', 'settle'),
    'qv2.receipt_handle.hmac': qv2.hashReceiptHandle(KEY, RH(1)),
    'qv2.receipt_handle.key_id': qv2.keyId(KEY),
    'messaging.message.id': 'mid-1',
    'qv2.visibility_timeout': 0,
    'qv2.result': 'ok',
  });
  // Changing visibility keeps the handle in the map: the message can still be deleted with it.
  await client.send(new S.DeleteMessageCommand({ QueueUrl: URL, ReceiptHandle: RH(1) }));
  assert.strictEqual(byName(spans(), 'qv2 delete')[0].attributes['messaging.message.id'], 'mid-1');
});

test('ChangeMessageVisibilityBatch: per-entry timeout and result', async (t) => {
  const { client, spans } = setup(t, {
    ChangeMessageVisibilityBatch: () => ({ Successful: [{ Id: 'a' }], Failed: [{ Id: 'b', Code: 'MessageNotInflight', SenderFault: true }] }),
  });
  await client.send(new S.ChangeMessageVisibilityBatchCommand({
    QueueUrl: URL,
    Entries: [{ Id: 'a', ReceiptHandle: RH(1), VisibilityTimeout: 30 }, { Id: 'b', ReceiptHandle: RH(2), VisibilityTimeout: 0 }],
  }));
  const got = spans().map((s) => [s.name, s.attributes['messaging.operation.name'], s.attributes['qv2.batch_entry_id'],
    s.attributes['qv2.visibility_timeout'], s.attributes['qv2.result'], s.attributes['qv2.error_code']]);
  assert.deepStrictEqual(got, [
    ['qv2 change_visibility', 'ChangeMessageVisibilityBatch', 'a', 30, 'ok', undefined],
    ['qv2 change_visibility', 'ChangeMessageVisibilityBatch', 'b', 0, 'failed', 'MessageNotInflight'],
  ]);
});

test('receipt handles are never exported', async (t) => {
  const { client, spans } = setup(t, {
    ReceiveMessage: () => ({ Messages: [message(1)] }),
    DeleteMessageBatch: () => ({ Successful: [{ Id: 'e1' }] }),
  });
  await client.send(new S.ReceiveMessageCommand({ QueueUrl: URL }));
  await client.send(new S.ChangeMessageVisibilityCommand({ QueueUrl: URL, ReceiptHandle: RH(1), VisibilityTimeout: 5 }));
  await client.send(new S.DeleteMessageBatchCommand({ QueueUrl: URL, Entries: [{ Id: 'e1', ReceiptHandle: RH(1) }] }));
  const dump = JSON.stringify(spans().map((s) => ({ n: s.name, a: s.attributes, r: s.resource.attributes })));
  assert.ok(!dump.includes('AQEB-receipt-handle'));
  assert.ok(!dump.includes(KEY));
  assert.ok(!dump.includes('not read'), 'message bodies are not read');
});

test('qv2 spans are children of the active span (the stock SDK span when it is installed)', async (t) => {
  const { client, spans } = setup(t, { SendMessage: () => ({ MessageId: 'mid-1' }) });
  const appProvider = new BasicTracerProvider();
  const parent = appProvider.getTracer('app').startSpan('app span');
  await api.context.with(api.trace.setSpan(api.context.active(), parent), () =>
    client.send(new S.SendMessageCommand({ QueueUrl: URL, MessageBody: 'x' })));
  parent.end();
  const [s] = spans();
  assert.strictEqual(s.spanContext().traceId, parent.spanContext().traceId);
  assert.strictEqual(s.parentSpanContext.spanId, parent.spanContext().spanId);
});

test('other SQS commands produce no qv2 span', async (t) => {
  const { client, spans } = setup(t, { GetQueueUrl: () => ({ QueueUrl: URL }) });
  await client.send(new S.GetQueueUrlCommand({ QueueName: 'orders' }));
  await client.send(new S.PurgeQueueCommand({ QueueUrl: URL }));
  assert.strictEqual(spans().length, 0);
});

test('instrument is idempotent, and uninstall turns the hooks off', async (t) => {
  const { client, calls, spans } = setup(t, { SendMessage: () => ({ MessageId: 'mid-1' }) });
  qv2.instrument(client);
  assert.strictEqual(client.middlewareStack.identify().filter((n) => n.startsWith(MIDDLEWARE_NAME)).length, 1);
  await client.send(new S.SendMessageCommand({ QueueUrl: URL, MessageBody: 'x' }));
  assert.strictEqual(spans().length, 1);
  await qv2.uninstall(); // shutting the provider down also clears the in-memory exporter
  assert.strictEqual(spans().length, 0);
  await client.send(new S.SendMessageCommand({ QueueUrl: URL, MessageBody: 'x' }));
  assert.strictEqual(spans().length, 0);
  await client.send(new S.ReceiveMessageCommand({ QueueUrl: URL }));
  assert.ok(!('MessageSystemAttributeNames' in calls[calls.length - 1].input), 'request left alone after uninstall');
});

test('instrument rejects things that are not SDK v3 clients', () => {
  assert.throws(() => qv2.instrument({}), TypeError);
});

test('without OTEL_RESOURCE_ATTRIBUTES, a UUID v4 is both service.instance.id and messaging.client.id', async (t) => {
  const { exporter, spanProcessor } = memoryExporter();
  qv2.install({ receiptHashKey: KEY, spanProcessor });
  t.after(() => qv2.uninstall());
  const { client } = stubClient(() => ({ MessageId: 'mid-1' }));
  qv2.instrument(client);
  await client.send(new S.SendMessageCommand({ QueueUrl: URL, MessageBody: 'x' }));
  const [s] = exporter.getFinishedSpans();
  const id = s.attributes['messaging.client.id'];
  assert.match(id, /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/);
  assert.strictEqual(s.resource.attributes['service.instance.id'], id);
  assert.strictEqual(s.resource.attributes['service.name'], 'unknown_service');
});
