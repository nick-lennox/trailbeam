'use strict';
// QV-515 (contract sections 21.3 and 23): a receive parses a body at most once for its SNS, EventBridge and S3
// readings, and not at all when the body is over the size or the object-and-array limit. Python checks the same
// (python/tests/test_envelope_limits.py).
const test = require('node:test');
const assert = require('node:assert');

const capture = require('../src/capture');

const MAX = capture.MAX_ENVELOPE_BYTES;
const CONTAINERS = capture.MAX_ENVELOPE_CONTAINERS;
const CONFIG = capture.resolveConfig({}, {}, {});
const EVENT = {
  version: '0', id: '6a7e8feb-b491-4cf7-a9f1-bf3703467718', 'detail-type': 'Order placed',
  source: 'com.example.orders', account: '111122223333', time: '2026-11-24T10:00:00Z', region: 'us-east-2',
};
const EXPECTED = {
  'trailbeam.eventbridge.event_id': EVENT.id, 'trailbeam.eventbridge.source': 'com.example.orders',
  'trailbeam.eventbridge.detail_type': 'Order placed', 'trailbeam.eventbridge.account': '111122223333',
  'trailbeam.eventbridge.region': 'us-east-2', 'trailbeam.eventbridge.time_ms': 1795514400000,
};
const bytes = (s) => Buffer.byteLength(s, 'utf8');
const event = (detail, extra = {}) => JSON.stringify({ ...EVENT, ...extra, detail });

/** An EventBridge event of exactly `size` UTF-8 bytes (`pad` is one character). */
function padded(size, pad = 'x') {
  const base = bytes(event({ pad: '' }));
  let body = event({ pad: pad.repeat(Math.floor((size - base) / bytes(pad))) });
  body = body.replace('"pad"', `"pad${'y'.repeat(size - bytes(body))}"`);
  assert.strictEqual(bytes(body), size);
  return body;
}

function spyParse(t) {
  const calls = [];
  const real = JSON.parse;
  JSON.parse = (s, ...rest) => { calls.push(s.length); return real(s, ...rest); };
  t.after(() => { JSON.parse = real; });
  return calls;
}

const receive = (body) => capture.receiveBodyAttrs(CONFIG, 'orders', body, undefined);

test('the crafted body of the issue is not parsed', (t) => {
  // About 1 MiB, both markers, arrays nested 500 deep: over the size limit, so never parsed.
  const unit = '['.repeat(500) + ']'.repeat(500);
  const body = `{"Type":"Notification","detail-type":1,"x":[${Array(Math.floor(1048576 / 1001)).fill(unit).join(',')}]}`;
  assert.ok(body.length > MAX);
  const calls = spyParse(t);
  assert.deepStrictEqual(receive(body), {});
  assert.strictEqual(capture.snsEnvelope(body), undefined);
  assert.deepStrictEqual(capture.originReceiveAttrs(CONFIG, 'orders', body), {});
  assert.deepStrictEqual(calls, []);
});

test('too many objects and arrays is not parsed', (t) => {
  const unit = '['.repeat(500) + ']'.repeat(500);
  const body = `{"Type":"Notification","detail-type":1,"x":[${Array(30).fill(unit).join(',')}]}`;
  assert.ok(body.length < MAX);
  const calls = spyParse(t);
  assert.deepStrictEqual(receive(body), {});
  assert.deepStrictEqual(calls, []);
});

test('a body with both markers is parsed once', (t) => {
  // Not an SNS envelope (no TopicArn), but an EventBridge event: one parse serves both readings.
  const body = event({ note: 'x' }, { Type: 'Notification' });
  const calls = spyParse(t);
  assert.deepStrictEqual(receive(body), EXPECTED);
  assert.strictEqual(calls.length, 1);
});

test('an SNS envelope is parsed once', (t) => {
  const body = JSON.stringify({
    Type: 'Notification', MessageId: '22b80b92-fdea-4c2c-8f9d-bdfb0c7bf324',
    TopicArn: 'arn:aws:sns:us-east-2:111122223333:orders', 'detail-type': 'x',
    Timestamp: '2026-11-24T10:00:00.000Z', Message: '{}',
  });
  const calls = spyParse(t);
  const out = receive(body);
  assert.strictEqual(out['trailbeam.sns.message_id'], '22b80b92-fdea-4c2c-8f9d-bdfb0c7bf324');
  assert.ok(!Object.keys(out).some((k) => k.startsWith('trailbeam.eventbridge.')));
  assert.strictEqual(calls.length, 1);
});

for (const pad of ['x', 'é', '😀']) {
  test(`the size limit is in UTF-8 bytes (${pad})`, () => {
    assert.deepStrictEqual(receive(padded(MAX, pad)), EXPECTED);
    assert.deepStrictEqual(receive(padded(MAX + 1, pad)), {});
    assert.deepStrictEqual(capture.originReceiveAttrs(CONFIG, 'orders', padded(MAX + 1, pad)), {});
  });
}

test('the object and array limit', () => {
  // The event object and its detail array are two; the rest are empty arrays inside detail.
  assert.deepStrictEqual(receive(event(Array(CONTAINERS - 2).fill([]))), EXPECTED);
  assert.deepStrictEqual(receive(event(Array(CONTAINERS - 1).fill([]))), {});
  assert.deepStrictEqual(receive(event(Array(CONTAINERS - 1).fill({}))), {});
});

test('brackets inside strings do not count', () => {
  assert.deepStrictEqual(receive(event({ text: '[{\\"'.repeat(CONTAINERS + 1) })), EXPECTED);
});

test('an unterminated string is scanned in linear time', () => {
  const body = `{"detail-type":"${'\\"'.repeat(MAX / 2 - 20)}`;
  const start = process.hrtime.bigint();
  assert.deepStrictEqual(receive(body), {});
  assert.ok(Number(process.hrtime.bigint() - start) / 1e6 < 1000);
});
