'use strict';
// Revision 3.2 (contract section 15): body size, tenant, FIFO, body capture with redaction, Lambda
// batch and error text. Shared vectors in contract/span-contract-vectors.json; Python checks the same.
const test = require('node:test');
const assert = require('node:assert');
const crypto = require('node:crypto');
const fs = require('node:fs');
const path = require('node:path');
const api = require('@opentelemetry/api');
const { AsyncLocalStorageContextManager } = require('@opentelemetry/context-async-hooks');
const { InMemorySpanExporter, SimpleSpanProcessor } = require('@opentelemetry/sdk-trace-base');
const S = require('@aws-sdk/client-sqs');

const trailbeam = require('../src/index');
const capture = require('../src/capture');
const { memoryExporter, stubClient } = require('./helpers');

api.context.setGlobalContextManager(new AsyncLocalStorageContextManager().enable());

const VECTORS = JSON.parse(fs.readFileSync(path.join(__dirname, '..', '..', 'contract', 'span-contract-vectors.json'), 'utf8')).capture;
const KEY = 'unit-test-key-0123456789';
const URL = 'http://localhost:9324/000000000000/orders';
const FIFO_URL = 'http://localhost:9324/000000000000/orders.fifo';
const md5 = (s) => crypto.createHash('md5').update(s, 'utf8').digest('hex');

// Options in the vectors use the Python names; map them to this package's.
const capOpts = (c) => ({
  bodyQueues: c.body_queues, errorQueues: c.error_queues, redactKeys: c.redact_keys,
  redactPatterns: c.redact_patterns, maxBodyBytes: c.max_body_bytes,
});
const tenantOpts = (t) => ({ messageAttribute: t.message_attribute, bodyPath: t.body_path });

// -- shared vectors --------------------------------------------------------------------------

test('redaction vectors', () => {
  assert.ok(VECTORS.redaction.length >= 10);
  for (const v of VECTORS.redaction) {
    const config = capture.resolveConfig({}, capOpts(v.capture));
    const [redacted, count, format] = capture.redactBody(v.body, config);
    assert.deepStrictEqual({ redacted, count, format }, { redacted: v.redacted, count: v.redacted_count, format: v.format }, v.name);
  }
});

test('tenant vectors', () => {
  for (const v of VECTORS.tenant) {
    const config = capture.resolveConfig(tenantOpts(v.tenant), {});
    const got = capture.tenantFrom(config, v.message_attributes, v.body, v.lambda_record);
    assert.deepStrictEqual(got === undefined ? null : got, v.expected, v.name);
  }
});

test('offloaded size vectors', () => {
  const config = capture.resolveConfig({}, {});
  for (const v of VECTORS.offloaded_size) {
    const got = capture.contentAttrs(config, 'orders', v.body, v.message_attributes, undefined, v.lambda_record);
    assert.strictEqual(got['trailbeam.body.offloaded_size'] ?? null, v.expected, v.name);
    assert.strictEqual(got['trailbeam.body.capture_status'], 'off', v.name);
  }
});

test('error message vectors', () => {
  for (const v of VECTORS.error_message) {
    const config = capture.resolveConfig({}, capOpts(v.capture));
    const got = capture.errorMessage(config, v.queue, new Error(v.message));
    assert.strictEqual(got === undefined ? null : got, v.expected, v.name);
  }
});

test('config: bad keys, unknown patterns and limits are refused; environment is read', (t) => {
  assert.throws(() => capture.resolveConfig({}, { redactKeys: ['a b'] }), /redact key/);
  assert.throws(() => capture.resolveConfig({}, { redactPatterns: ['nope'] }), /unknown redact pattern/);
  assert.throws(() => capture.resolveConfig({}, { maxBodyBytes: capture.HARD_MAX_BODY_BYTES + 1 }), /maxBodyBytes/);
  const saved = { ...process.env };
  t.after(() => { for (const k of Object.values(capture.ENV)) { if (saved[k] === undefined) delete process.env[k]; else process.env[k] = saved[k]; } });
  process.env[capture.ENV.BODY_QUEUES] = 'orders, payments';
  process.env[capture.ENV.TENANT_ATTRIBUTE] = 'tenant';
  process.env[capture.ENV.MAX_BODY_BYTES] = '100';
  const c = capture.resolveConfig();
  assert.ok(c.bodyOn('orders') && c.bodyOn('payments') && !c.bodyOn('other'));
  assert.strictEqual(c.tenantAttribute, 'tenant');
  assert.strictEqual(c.maxBodyBytes, 100);
  const off = capture.resolveConfig({}, {}); // explicit options replace the environment
  assert.ok(!off.bodyOn('orders'));
});

// -- client hooks ----------------------------------------------------------------------------

function setup(t, responses, options) {
  const { exporter, spanProcessor } = memoryExporter();
  trailbeam.install({ receiptHashKey: KEY, spanProcessor, ...options });
  const { client, calls } = stubClient((op, input) => (responses[op] ? responses[op](input) : {}));
  trailbeam.instrument(client);
  t.after(() => trailbeam.uninstall());
  return { client, calls, spans: (name) => exporter.getFinishedSpans().filter((s) => !name || s.name === name) };
}

const ON = { capture: { bodyQueues: ['orders', 'orders.fifo'] }, tenant: { messageAttribute: 'tenant', bodyPath: 'customer.id' } };

test('SendMessage: body captured and redacted, tenant from the attribute, MD5 checked', async (t) => {
  const body = '{"customer": {"id": "c-9"}, "password": "hunter2"}';
  const { client, spans } = setup(t, { SendMessage: () => ({ MessageId: 'm1', MD5OfMessageBody: md5(body) }) }, ON);
  await client.send(new S.SendMessageCommand({
    QueueUrl: URL, MessageBody: body, MessageAttributes: { tenant: { DataType: 'String', StringValue: 'acme' } },
  }));
  const a = spans('trailbeam send')[0].attributes;
  assert.strictEqual(a['trailbeam.body.capture_status'], 'captured');
  assert.strictEqual(a['trailbeam.body'], '{"customer": {"id": "c-9"}, "password": "[REDACTED]"}');
  assert.strictEqual(a['trailbeam.body.format'], 'json');
  assert.strictEqual(a['trailbeam.body.redacted_count'], 1);
  assert.strictEqual(a['messaging.message.body.size'], Buffer.byteLength(body));
  assert.strictEqual(a['trailbeam.tenant'], 'acme');
  assert.strictEqual(a['trailbeam.tenant.source'], 'message_attribute');
});

test('SendMessage: an MD5 that does not match drops the body', async (t) => {
  const { client, spans } = setup(t, { SendMessage: () => ({ MessageId: 'm1', MD5OfMessageBody: md5('other') }) }, ON);
  await client.send(new S.SendMessageCommand({ QueueUrl: URL, MessageBody: 'hello' }));
  const a = spans('trailbeam send')[0].attributes;
  assert.strictEqual(a['trailbeam.body.capture_status'], 'md5_mismatch');
  assert.ok(!('trailbeam.body' in a));
});

test('SendMessageBatch: each body matched to its entry by Id; FIFO group and sequence number', async (t) => {
  const { client, spans } = setup(t, {
    SendMessageBatch: () => ({
      Successful: [
        { Id: 'b', MessageId: 'mb', MD5OfMessageBody: md5('second'), SequenceNumber: '18849496460467696129' },
        { Id: 'a', MessageId: 'ma', MD5OfMessageBody: md5('first'), SequenceNumber: '18849496460467696128' },
      ],
      Failed: [{ Id: 'c', Code: 'InternalError', SenderFault: false }],
    }),
  }, ON);
  await client.send(new S.SendMessageBatchCommand({
    QueueUrl: FIFO_URL,
    Entries: [
      { Id: 'a', MessageBody: 'first', MessageGroupId: 'g1', MessageDeduplicationId: 'd1' },
      { Id: 'b', MessageBody: 'second', MessageGroupId: 'g2', MessageDeduplicationId: 'd2' },
      { Id: 'c', MessageBody: 'third', MessageGroupId: 'g3', MessageDeduplicationId: 'd3' },
    ],
  }));
  const got = spans('trailbeam send').map((s) => [s.attributes['messaging.message.id'], s.attributes['trailbeam.body'],
    s.attributes['trailbeam.message_group_id'], s.attributes['trailbeam.sequence_number']]);
  assert.deepStrictEqual(got, [
    ['mb', 'second', 'g2', '18849496460467696129'],
    ['ma', 'first', 'g1', '18849496460467696128'],
  ]);
});

test('ReceiveMessage: asks for the tenant attribute; body, tenant and FIFO fields from the reply', async (t) => {
  const { client, calls, spans } = setup(t, {
    ReceiveMessage: () => ({
      Messages: [{
        MessageId: 'm1', ReceiptHandle: 'rh-1', Body: 'mail ada@example.com', MD5OfBody: md5('mail ada@example.com'),
        Attributes: { ApproximateReceiveCount: '1', MessageGroupId: 'g1', SequenceNumber: '42' },
        MessageAttributes: { tenant: { DataType: 'String', StringValue: 'acme' } },
      }],
    }),
  }, ON);
  const input = { QueueUrl: FIFO_URL, MessageAttributeNames: ['trace'] };
  await client.send(new S.ReceiveMessageCommand(input));
  assert.deepStrictEqual(calls[0].input.MessageAttributeNames, ['trace', 'tenant', 'ExtendedPayloadSize', 'SQSLargePayloadSize', 'trailbeam.publish_id']);
  assert.deepStrictEqual(input.MessageAttributeNames, ['trace'], 'caller input not mutated');
  const a = spans('trailbeam receive')[0].attributes;
  assert.strictEqual(a['trailbeam.body'], 'mail [REDACTED]');
  assert.strictEqual(a['trailbeam.body.format'], 'text');
  assert.strictEqual(a['trailbeam.tenant'], 'acme');
  assert.strictEqual(a['trailbeam.message_group_id'], 'g1');
  assert.strictEqual(a['trailbeam.sequence_number'], '42');
});

test('S3 pointer: exported unredacted, offloaded size from the send request and the receive reply', async (t) => {
  const pointer = '["software.amazon.payloadoffloading.PayloadS3Pointer", {"s3BucketName": "payloads-123456789012", '
    + '"s3Key": "61fc25e3-8ae5-4569-9189-43c27e5ba06f"}]';
  const compact = pointer.replace(/", /g, '",').replace(/": /g, '":');
  const size = { ExtendedPayloadSize: { DataType: 'Number', StringValue: '300000' } };
  const { client, spans } = setup(t, {
    SendMessage: () => ({ MessageId: 'm1', MD5OfMessageBody: md5(pointer) }),
    ReceiveMessage: () => ({
      Messages: [{ MessageId: 'm1', ReceiptHandle: 'rh-1', Body: pointer, MD5OfBody: md5(pointer), MessageAttributes: size }],
    }),
  }, ON);
  await client.send(new S.SendMessageCommand({ QueueUrl: URL, MessageBody: pointer, MessageAttributes: size }));
  await client.send(new S.ReceiveMessageCommand({ QueueUrl: URL }));
  for (const name of ['trailbeam send', 'trailbeam receive']) {
    const a = spans(name)[0].attributes;
    assert.strictEqual(a['trailbeam.body.capture_status'], 'offloaded_to_s3', name);
    assert.strictEqual(a['trailbeam.body'], compact, name);
    assert.strictEqual(a['trailbeam.body.redacted_count'], 0, name);
    assert.strictEqual(a['trailbeam.body.offloaded_size'], 300000, name);
    assert.strictEqual(a['messaging.message.body.size'], Buffer.byteLength(pointer), name);
  }
});

test('ReceiveMessage: All or .* already asks for every message attribute', async (t) => {
  for (const names of [['All'], ['.*'], ['tenant', 'ExtendedPayloadSize', 'SQSLargePayloadSize', 'trailbeam.publish_id']]) {
    const { client, calls } = setup(t, {}, ON);
    await client.send(new S.ReceiveMessageCommand({ QueueUrl: URL, MessageAttributeNames: names }));
    assert.deepStrictEqual(calls[0].input.MessageAttributeNames, names);
    await trailbeam.uninstall();
  }
});

test('capture off for the queue: status off, no body; over the limit: no body; S3 pointer', async (t) => {
  const pointer = '["software.amazon.payloadoffloading.PayloadS3Pointer",{"s3BucketName":"b","s3Key":"k"}]';
  const { client, spans } = setup(t, {
    SendMessage: (input) => ({ MessageId: input.MessageBody.length > 100 ? 'big' : `m-${input.QueueUrl.length}-${input.MessageBody.length}` }),
  }, { capture: { bodyQueues: ['orders'], maxBodyBytes: 100 } });
  await client.send(new S.SendMessageCommand({ QueueUrl: 'http://localhost:9324/000000000000/other', MessageBody: 'x' }));
  await client.send(new S.SendMessageCommand({ QueueUrl: URL, MessageBody: 'y'.repeat(101) }));
  await client.send(new S.SendMessageCommand({ QueueUrl: URL, MessageBody: pointer }));
  const st = spans('trailbeam send').map((s) => [s.attributes['trailbeam.body.capture_status'], 'trailbeam.body' in s.attributes]);
  assert.deepStrictEqual(st, [['off', false], ['exceeds_capture_limit', false], ['offloaded_to_s3', true]]);
});

test('the hooks never make an SQS call of their own, with every option on', async (t) => {
  const { client, calls } = setup(t, {
    SendMessage: () => ({ MessageId: 'm1' }),
    ReceiveMessage: () => ({ Messages: [{ MessageId: 'm1', ReceiptHandle: 'rh', Body: '{"customer":{"id":"c"}}' }] }),
    DeleteMessage: () => ({}),
  }, { ...ON, capture: { ...ON.capture, errorQueues: ['*'] } });
  await client.send(new S.SendMessageCommand({ QueueUrl: URL, MessageBody: 'x' }));
  await client.send(new S.ReceiveMessageCommand({ QueueUrl: URL }));
  await client.send(new S.DeleteMessageCommand({ QueueUrl: URL, ReceiptHandle: 'rh' }));
  assert.deepStrictEqual(calls.map((c) => c.operation), ['SendMessage', 'ReceiveMessage', 'DeleteMessage']);
});

// -- Lambda wrapper --------------------------------------------------------------------------

const record = (id, body, extra = {}) => ({
  messageId: id, receiptHandle: `rh-${id}`, body, md5OfBody: md5(body),
  attributes: { ApproximateReceiveCount: '1', ...(extra.attributes || {}) },
  messageAttributes: extra.messageAttributes || {},
  eventSource: 'aws:sqs', eventSourceARN: 'arn:aws:sqs:us-east-2:123456789012:orders.fifo', awsRegion: 'us-east-2',
});

function wrapped(fn, options) {
  const exporter = new InMemorySpanExporter();
  const handler = trailbeam.wrapHandler(fn, { receiptHashKey: KEY, spanProcessor: new SimpleSpanProcessor(exporter), ...options });
  return { handler, spans: (name) => exporter.getFinishedSpans().filter((s) => s.name === name) };
}

const LAMBDA_EVENT = {
  Records: [
    record('r1', '{"customer": {"id": "c-1"}}', { attributes: { MessageGroupId: 'g', SequenceNumber: '1' } }),
    record('r2', 'two', { messageAttributes: { tenant: { stringValue: 'acme', dataType: 'String' } } }),
    record('r3', 'three'),
  ],
};
const ctx = { awsRequestId: 'req-1', functionName: 'fn' };

test('Lambda: batch size and position, body, tenant and FIFO fields on each receive', async () => {
  const { handler, spans } = wrapped(async () => undefined, ON);
  await handler(LAMBDA_EVENT, ctx);
  const got = spans('trailbeam receive').map((s) => {
    const a = s.attributes;
    return [a['trailbeam.batch.size'], a['trailbeam.batch.index'], a['trailbeam.body'], a['trailbeam.tenant'], a['trailbeam.message_group_id']];
  });
  assert.deepStrictEqual(got, [
    [3, 0, '{"customer": {"id": "c-1"}}', 'c-1', 'g'],
    [3, 1, 'two', 'acme', undefined],
    [3, 2, 'three', undefined, undefined],
  ]);
});

test('Lambda: reported failure count on every outcome; none for an invalid list', async () => {
  let { handler, spans } = wrapped(async () => ({ batchItemFailures: [{ itemIdentifier: 'r2' }] }));
  await handler(LAMBDA_EVENT, ctx);
  assert.deepStrictEqual(spans('trailbeam outcome').map((s) => [s.attributes['trailbeam.outcome'], s.attributes['trailbeam.batch.index'], s.attributes['trailbeam.batch.reported_failure_count']]),
    [['success', 0, 1], ['failed_reported', 1, 1], ['success', 2, 1]]);
  ({ handler, spans } = wrapped(async () => ({ batchItemFailures: [{ itemIdentifier: 'nope' }] })));
  await handler(LAMBDA_EVENT, ctx);
  for (const s of spans('trailbeam outcome')) assert.ok(!('trailbeam.batch.reported_failure_count' in s.attributes));
});

test('Lambda: the exception message, redacted, only for queues with error text on', async () => {
  const boom = async () => { throw new TypeError('no card for ada@example.com'); };
  let { handler, spans } = wrapped(boom, { capture: { errorQueues: ['orders.fifo'] } });
  await assert.rejects(handler(LAMBDA_EVENT, ctx), TypeError);
  for (const s of spans('trailbeam outcome')) {
    assert.strictEqual(s.attributes['trailbeam.error_type'], 'TypeError');
    assert.strictEqual(s.attributes['trailbeam.error_message'], 'no card for [REDACTED]');
    assert.ok(!('trailbeam.batch.reported_failure_count' in s.attributes));
  }
  ({ handler, spans } = wrapped(boom, { capture: {} }));
  await assert.rejects(handler(LAMBDA_EVENT, ctx), TypeError);
  for (const s of spans('trailbeam outcome')) assert.ok(!('trailbeam.error_message' in s.attributes));
});
