'use strict';
// The per-process call-order counter (contract schema version 3).
const test = require('node:test');
const assert = require('node:assert');
const { Worker } = require('node:worker_threads');

const order = require('../src/order');

test('tick is a strictly increasing integer', () => {
  const got = Array.from({ length: 1000 }, () => order.tick());
  assert.ok(got.every(Number.isInteger));
  for (let i = 1; i < got.length; i += 1) assert.ok(got[i] > got[i - 1]);
  assert.ok(got[0] >= 1);
});

test('scope is a UUID v4, the same for every require in this realm', () => {
  assert.match(order.scope, /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/);
  assert.strictEqual(require('../src/order').scope, order.scope);
  assert.strictEqual(require('../src/order').tick, order.tick);
});

test('a worker thread has its own scope and counter', async () => {
  const worker = new Worker(`
    const { parentPort } = require('node:worker_threads');
    const order = require(${JSON.stringify(require.resolve('../src/order'))});
    parentPort.postMessage({ scope: order.scope, tick: order.tick() });
  `, { eval: true });
  const got = await new Promise((resolve, reject) => { worker.once('message', resolve); worker.once('error', reject); });
  assert.notStrictEqual(got.scope, order.scope);
  assert.strictEqual(got.tick, 1);
});
