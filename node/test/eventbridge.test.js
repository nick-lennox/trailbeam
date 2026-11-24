'use strict';
// Revision 3.6 (contract section 25): EventBridge PutEvents, against the shared vectors in
// contract/span-contract-vectors.json ("eventbridge"), with canned EventBridge responses. Python checks the
// same vectors.
const test = require('node:test');
const assert = require('node:assert');
const fs = require('node:fs');
const path = require('node:path');
const api = require('@opentelemetry/api');
const { AsyncLocalStorageContextManager } = require('@opentelemetry/context-async-hooks');
const E = require('@aws-sdk/client-eventbridge');

const trailbeam = require('../src/index');
const capture = require('../src/capture');
const { memoryExporter, stubClient } = require('./helpers');

api.context.setGlobalContextManager(new AsyncLocalStorageContextManager().enable());

const VECTORS = JSON.parse(fs.readFileSync(path.join(__dirname, '..', '..', 'contract', 'span-contract-vectors.json'), 'utf8')).eventbridge;
const KEY = 'unit-test-key-0123456789';
const clone = (x) => JSON.parse(JSON.stringify(x));

function setup(t, respond) {
  const { exporter, spanProcessor } = memoryExporter();
  trailbeam.install({ receiptHashKey: KEY, spanProcessor });
  const { client, calls } = stubClient(respond, { service: 'eventbridge' });
  trailbeam.instrument(client);
  t.after(() => trailbeam.uninstall());
  return { client, calls, spans: () => exporter.getFinishedSpans() };
}

const P = VECTORS.put_events;

for (const c of P.cases) {
  test(`put_events vector: ${c.name}`, async (t) => {
    const { client, calls, spans } = setup(t, (op) => {
      assert.strictEqual(op, 'PutEvents');
      if (c.error) throw Object.assign(new Error(c.error), { name: c.error });
      return clone(c.response);
    });
    const input = clone(c.request);
    const before = Date.now();
    if (c.error) {
      await assert.rejects(client.send(new E.PutEventsCommand(input)), (e) => e.name === c.error);
    } else {
      await client.send(new E.PutEventsCommand(input));
    }
    const after = Date.now();
    assert.deepStrictEqual(input, c.request); // the caller's object is not changed
    assert.deepStrictEqual(calls[0].input, c.request); // nor is what the SDK sends
    const got = spans();
    assert.strictEqual(got.length, c.spans.length);
    got.forEach((s, i) => {
      const want = c.spans[i];
      assert.strictEqual(s.name, 'trailbeam put_event');
      const a = s.attributes;
      assert.deepStrictEqual(Object.fromEntries(Object.keys(want).map((k) => [k, a[k]])), want);
      const extra = Object.keys(a).filter((k) => !(k in want) && !P.present.includes(k));
      assert.deepStrictEqual(extra, []);
      for (const k of P.present) assert.ok(k in a, k);
      for (const k of P.absent) assert.ok(!(k in a), k);
      assert.ok(a['trailbeam.order.issued'] < a['trailbeam.order.returned']);
      // Every entry of one call has the call's interval and ticks.
      assert.deepStrictEqual([s.startTime, s.endTime], [got[0].startTime, got[0].endTime]);
      assert.strictEqual(a['trailbeam.order.issued'], got[0].attributes['trailbeam.order.issued']);
      const ms = (hr) => hr[0] * 1000 + hr[1] / 1e6;
      assert.ok(ms(s.startTime) >= before && ms(s.endTime) <= after && ms(s.startTime) <= ms(s.endTime));
    });
  });
}

test('Detail, Resources, TraceHeader, Time, ErrorMessage and EndpointId are never recorded', async (t) => {
  const { client, spans } = setup(t, () => ({
    FailedEntryCount: 1,
    Entries: [{ EventId: 'e-1' }, { ErrorCode: 'InternalFailure', ErrorMessage: 'secret-message' }],
  }));
  const entry = {
    Source: 'a.b', DetailType: 'T', Detail: '{"secret-detail": 1}', Resources: ['arn:secret-resource'],
    TraceHeader: 'Root=secret-trace', Time: new Date('2026-11-24T10:00:00Z'),
  };
  await client.send(new E.PutEventsCommand({ Entries: [entry, { ...entry }], EndpointId: 'secret-endpoint.veo' }));
  const got = spans();
  assert.strictEqual(got.length, 2);
  const dump = JSON.stringify(got.map((s) => s.attributes));
  for (const secret of ['secret-detail', 'secret-resource', 'secret-trace', 'secret-message', 'secret-endpoint', '2026-11-24']) {
    assert.ok(!dump.includes(secret), secret);
  }
  assert.strictEqual(got[0].attributes['trailbeam.eventbridge.time_given'], true);
  assert.strictEqual(got[1].attributes['trailbeam.error_code'], 'InternalFailure');
});

test('entries that are not objects, and entries with no result entry, get no span', async (t) => {
  const { client, spans } = setup(t, () => ({ Entries: [{ EventId: 'e-0' }, { EventId: 'e-1' }] }));
  const input = { Entries: [{ Source: 'a.b', DetailType: 'T', Detail: '{}' }, null, { Source: 'a.b', DetailType: 'T', Detail: '{}' }] };
  await client.send(new E.PutEventsCommand(input)).catch(() => {});
  // The SDK may reject a null entry before sending; either way only entry 0 can have a result here.
  const idx = spans().map((s) => s.attributes['trailbeam.eventbridge.entry_index']);
  assert.ok(idx.every((i) => i === 0), String(idx));

  assert.deepStrictEqual(capture.putEventAttrs({ Entries: 'x' }, { Entries: [] }, undefined), []);
  assert.deepStrictEqual(capture.putEventAttrs({ Entries: [{}] }, {}, undefined), []);
  assert.deepStrictEqual(capture.putEventAttrs({ Entries: [{}, 7, {}] }, { Entries: [{ EventId: 'a' }, {}] }, undefined)
    .map((a) => a['trailbeam.eventbridge.entry_index']), [0]);
});

test('a call error with a protocol namespace is recorded without it', async (t) => {
  const { client, spans } = setup(t, () => {
    throw Object.assign(new Error('x'), { name: 'com.amazonaws.events#InternalException' });
  });
  await assert.rejects(client.send(new E.PutEventsCommand({ Entries: [{ Source: 'a.b', DetailType: 'T', Detail: '{}' }] })));
  assert.strictEqual(spans()[0].attributes['trailbeam.error_code'], 'InternalException');
});

test('a hook that fails does not reach the caller', async (t) => {
  const { client, spans } = setup(t, () => ({ Entries: [{ EventId: 'e-0' }] }));
  const orig = capture.putEventAttrs;
  capture.putEventAttrs = () => { throw new Error('hook bug'); };
  t.after(() => { capture.putEventAttrs = orig; });
  const out = await client.send(new E.PutEventsCommand({ Entries: [{ Source: 'a.b', DetailType: 'T', Detail: '{}' }] }));
  assert.deepStrictEqual(out.Entries, [{ EventId: 'e-0' }]);
  assert.deepStrictEqual(spans(), []);
});

test('PutEvents on a client that is not an EventBridgeClient gets no span', async (t) => {
  const { exporter, spanProcessor } = memoryExporter();
  trailbeam.install({ receiptHashKey: KEY, spanProcessor });
  t.after(() => trailbeam.uninstall());
  const { sqsMiddleware } = require('../src/sqs');
  const mw = sqsMiddleware(() => new (require('../src/sqs').SqsHooks)(
    { emit: () => assert.fail('no span expected') }));
  const next = async () => ({ output: { Entries: [{ EventId: 'e-0' }] } });
  await mw(next, { clientName: 'PinpointClient', commandName: 'PutEventsCommand' })({ input: { Entries: [{}] } });
  assert.deepStrictEqual(exporter.getFinishedSpans(), []);
});

test('event bus names', () => {
  assert.strictEqual(capture.eventBusName(undefined), 'default');
  assert.strictEqual(capture.eventBusName(''), 'default');
  assert.strictEqual(capture.eventBusName('orders'), 'orders');
  assert.strictEqual(capture.eventBusName('arn:aws:events:us-east-2:111122223333:event-bus/orders'), 'orders');
});
