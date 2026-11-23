'use strict';
// Revision 3.4 (contract section 20): SNS fan-out, against the shared vectors in
// contract/span-contract-vectors.json ("sns"), with canned SNS and SQS responses. Python checks the same
// vectors, and runs SNS to SQS delivery end to end in moto (python/tests/test_sns.py).
const test = require('node:test');
const assert = require('node:assert');
const fs = require('node:fs');
const path = require('node:path');
const api = require('@opentelemetry/api');
const { AsyncLocalStorageContextManager } = require('@opentelemetry/context-async-hooks');
const N = require('@aws-sdk/client-sns');
const S = require('@aws-sdk/client-sqs');

const trailbeam = require('../src/index');
const capture = require('../src/capture');
const { memoryExporter, stubClient, byName } = require('./helpers');

api.context.setGlobalContextManager(new AsyncLocalStorageContextManager().enable());

const VECTORS = JSON.parse(fs.readFileSync(path.join(__dirname, '..', '..', 'contract', 'span-contract-vectors.json'), 'utf8')).sns;
const KEY = 'unit-test-key-0123456789';
const URL = 'http://localhost:9324/000000000000/orders';
const SNS_NAMES = ['trailbeam.sns.publish_id', 'trailbeam.sns.message_id', 'trailbeam.sns.topic_arn', 'trailbeam.sns.timestamp_ms'];
const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;
const clone = (x) => JSON.parse(JSON.stringify(x));

const snsOnly = (attrs) => Object.fromEntries(Object.entries(attrs).filter(([k]) => SNS_NAMES.includes(k)));

/** A vector entry with its {"repeat": c, "times": n} Message written out. */
function expand(entry) {
  const e = clone(entry);
  if (e.Message && typeof e.Message === 'object') e.Message = e.Message.repeat.repeat(e.Message.times);
  return e;
}

/** ReceiveMessage-form message attributes in a Lambda record's form. */
function lambdaAttributes(attrs) {
  const out = {};
  for (const [name, a] of Object.entries(attrs || {})) {
    const b = { dataType: a.DataType };
    if ('StringValue' in a) b.stringValue = a.StringValue;
    if ('BinaryValue' in a) b.binaryValue = a.BinaryValue;
    out[name] = b;
  }
  return out;
}

function setup(t, { sns, responses = {}, service = 'sqs' } = {}) {
  const { exporter, spanProcessor } = memoryExporter();
  trailbeam.install({ receiptHashKey: KEY, spanProcessor, sns });
  const { client, calls } = stubClient((op, input) => (responses[op] ? responses[op](input) : {}), { service });
  trailbeam.instrument(client);
  t.after(() => trailbeam.uninstall());
  return { client, calls, spans: () => exporter.getFinishedSpans() };
}

test('timestamp vectors', () => {
  for (const [value, expected] of VECTORS.timestamps) {
    assert.strictEqual(capture.snsTimestampMs(value), expected === null ? undefined : expected, value);
  }
});

for (const c of VECTORS.receive) {
  test(`receive vector (client hooks): ${c.name}`, async (t) => {
    const m = { MessageId: 'sqs-1', ReceiptHandle: 'rh-1', Body: c.body, Attributes: { ApproximateReceiveCount: '1' } };
    if (c.message_attributes) m.MessageAttributes = c.message_attributes;
    const { client, calls, spans } = setup(t, {
      sns: { envelopeQueues: c.envelope_on ? ['orders'] : [] },
      responses: { ReceiveMessage: () => ({ Messages: [m] }) },
    });
    await client.send(new S.ReceiveMessageCommand({ QueueUrl: URL }));
    assert.ok(calls[0].input.MessageAttributeNames.includes('trailbeam.publish_id'));
    const [s] = byName(spans(), 'trailbeam receive');
    assert.deepStrictEqual(snsOnly(s.attributes), c.attributes);
  });

  test(`receive vector (Lambda wrapper): ${c.name}`, async () => {
    const { exporter, spanProcessor } = memoryExporter();
    const handler = trailbeam.wrapHandler(async () => {}, {
      receiptHashKey: KEY, spanProcessor, sns: { envelopeQueues: c.envelope_on ? ['orders'] : [] },
    });
    await handler({ Records: [{
      messageId: 'sqs-1', receiptHandle: 'rh-1', body: c.body, attributes: { ApproximateReceiveCount: '1' },
      messageAttributes: lambdaAttributes(c.message_attributes), eventSource: 'aws:sqs',
      eventSourceARN: 'arn:aws:sqs:us-east-2:111122223333:orders',
    }] }, { awsRequestId: 'req-1', functionName: 'fn', getRemainingTimeInMillis: () => 30000 });
    const [s] = byName(exporter.getFinishedSpans(), 'trailbeam receive');
    assert.deepStrictEqual(snsOnly(s.attributes), c.attributes);
  });
}

test('body capture turns the envelope fields on', () => {
  const body = VECTORS.receive.find((c) => c.name.startsWith('envelope on')).body;
  const config = capture.resolveConfig({}, { bodyQueues: ['orders'] }, {});
  assert.ok(capture.snsReceiveAttrs(config, 'orders', body)['trailbeam.sns.message_id']);
  assert.deepStrictEqual(capture.snsReceiveAttrs(config, 'other', body), {});
  assert.ok(capture.snsReceiveAttrs(capture.resolveConfig({}, {}, { envelopeQueues: ['*'] }), 'x', body)['trailbeam.sns.message_id']);
});

for (const c of VECTORS.inject) {
  test(`inject vector: ${c.name}`, () => {
    const config = capture.resolveConfig({}, {}, { inject: c.inject });
    const entries = c.entries.map(expand);
    const before = clone(entries);
    const { entries: out, results } = capture.snsInject(config, entries);
    assert.deepStrictEqual(entries, before); // the caller's objects are not changed
    assert.strictEqual(out.length, entries.length);
    entries.forEach((e, i) => {
      const [o, r, want] = [out[i], results[i], c.results[i]];
      if (want === 'injected') {
        assert.strictEqual(r[0], 'trailbeam.sns.publish_id');
        assert.match(r[1], UUID_RE);
        assert.notStrictEqual(o, e);
        assert.deepStrictEqual(o.MessageAttributes['trailbeam.publish_id'], { DataType: 'String', StringValue: r[1] });
        assert.ok(Object.keys(o.MessageAttributes).length <= VECTORS.max_attributes);
        const { MessageAttributes: _a, ...rest } = o;
        const { MessageAttributes: _b, ...given } = e;
        assert.deepStrictEqual(rest, given);
        assert.ok(capture.snsEntryBytes(o) + 62 <= VECTORS.max_message_bytes);
      } else {
        assert.deepStrictEqual(r, ['trailbeam.sns.publish_id_skipped', want]);
        assert.strictEqual(o, e);
      }
    });
    const ids = results.filter((r) => r && r[0] === 'trailbeam.sns.publish_id').map((r) => r[1]);
    assert.strictEqual(new Set(ids).size, ids.length);
  });
}

for (const c of VECTORS.publish.cases) {
  test(`publish vector: ${c.name}`, async (t) => {
    const { client, calls, spans } = setup(t, {
      service: 'sns',
      sns: { inject: false },
      responses: {
        [c.operation]: () => {
          if (c.response.Error) throw Object.assign(new Error(c.response.Error.Code), { name: c.response.Error.Code });
          return clone(c.response);
        },
      },
    });
    const Command = c.operation === 'Publish' ? N.PublishCommand : N.PublishBatchCommand;
    const input = clone(c.request);
    await client.send(new Command(input)).catch((e) => assert.ok(c.response.Error, String(e)));
    assert.deepStrictEqual(input, c.request);
    assert.deepStrictEqual(calls[0].input, c.request); // injection is off: the request is unchanged
    const got = spans();
    assert.strictEqual(got.length, c.spans.length);
    got.forEach((s, i) => {
      const want = c.spans[i];
      assert.strictEqual(s.name, 'trailbeam publish');
      const a = s.attributes;
      assert.deepStrictEqual(Object.fromEntries(Object.keys(want).map((k) => [k, a[k]])), want);
      const extra = Object.keys(a).filter((k) => !(k in want) && !VECTORS.publish.present.includes(k));
      assert.deepStrictEqual(extra, []);
      for (const k of VECTORS.publish.present) assert.ok(k in a, k);
      for (const k of VECTORS.publish.absent) assert.ok(!(k in a), k);
      assert.ok(a['trailbeam.order.issued'] < a['trailbeam.order.returned']);
    });
  });
}

test('PublishBatch: the SDK sends the publish IDs the spans record; the caller\'s objects are not changed', async (t) => {
  const { client, calls, spans } = setup(t, {
    service: 'sns',
    responses: { PublishBatch: () => ({ Successful: [{ Id: 'b', MessageId: 'm-b' }, { Id: 'a', MessageId: 'm-a' }] }) },
  });
  const mine = { kind: { DataType: 'String', StringValue: 'order' } };
  const entries = [{ Id: 'a', Message: '1', MessageAttributes: mine }, { Id: 'b', Message: '2' }];
  await client.send(new N.PublishBatchCommand({ TopicArn: 'arn:aws:sns:us-east-1:123456789012:t', PublishBatchRequestEntries: entries }));
  assert.deepStrictEqual(mine, { kind: { DataType: 'String', StringValue: 'order' } });
  assert.deepStrictEqual(entries[1], { Id: 'b', Message: '2' });
  const sent = Object.fromEntries(calls[0].input.PublishBatchRequestEntries.map(
    (e) => [e.Id, e.MessageAttributes['trailbeam.publish_id'].StringValue]));
  assert.deepStrictEqual(Object.keys(calls[0].input.PublishBatchRequestEntries[0].MessageAttributes).sort(), ['kind', 'trailbeam.publish_id']);
  const got = Object.fromEntries(spans().map((s) => [s.attributes['trailbeam.batch_entry_id'], s.attributes['trailbeam.sns.publish_id']]));
  assert.deepStrictEqual(got, sent);
  for (const id of Object.values(got)) assert.match(id, UUID_RE);
});

test('Publish that throws: no span, and the error reaches the caller', async (t) => {
  const { client, spans } = setup(t, {
    service: 'sns',
    responses: { Publish: () => { throw Object.assign(new Error('boom'), { name: 'NotFound' }); } },
  });
  await assert.rejects(client.send(new N.PublishCommand({ TopicArn: 'arn:aws:sns:us-east-1:123456789012:t', Message: 'x' })), /boom/);
  assert.deepStrictEqual(spans(), []);
});

test('environment variables', (t) => {
  const saved = { ...process.env };
  t.after(() => {
    for (const k of ['TRAILBEAM_SNS_INJECT', 'TRAILBEAM_SNS_ENVELOPE_QUEUES']) {
      if (k in saved) process.env[k] = saved[k]; else delete process.env[k];
    }
  });
  process.env.TRAILBEAM_SNS_INJECT = 'off';
  process.env.TRAILBEAM_SNS_ENVELOPE_QUEUES = 'a, b';
  let c = capture.resolveConfig();
  assert.deepStrictEqual([c.snsInject, [...c.snsEnvelopeQueues]], [false, ['a', 'b']]);
  process.env.TRAILBEAM_SNS_INJECT = 'on';
  assert.strictEqual(capture.resolveConfig().snsInject, true);
  delete process.env.TRAILBEAM_SNS_INJECT;
  delete process.env.TRAILBEAM_SNS_ENVELOPE_QUEUES;
  c = capture.resolveConfig();
  assert.deepStrictEqual([c.snsInject, [...c.snsEnvelopeQueues]], [true, []]);
  // A group given explicitly replaces its environment variables as a whole.
  process.env.TRAILBEAM_SNS_INJECT = 'off';
  assert.strictEqual(capture.resolveConfig(undefined, undefined, {}).snsInject, true);
});
