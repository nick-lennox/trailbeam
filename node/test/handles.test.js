'use strict';
// Receipt handle -> message ID map (contract section 8).
const test = require('node:test');
const assert = require('node:assert');
const { HandleMap, MAX_AGE_MS } = require('../src/handles');

test('put, get and discard', () => {
  const m = new HandleMap();
  m.put('rh1', 'm1');
  assert.strictEqual(m.get('rh1'), 'm1');
  assert.strictEqual(m.get('rh2'), undefined);
  m.discard('rh1');
  assert.strictEqual(m.get('rh1'), undefined);
  assert.strictEqual(m.size, 0);
});

test('bounded: the oldest entries are dropped first', () => {
  const m = new HandleMap({ maxEntries: 3 });
  for (let i = 0; i < 5; i++) m.put(`rh${i}`, `m${i}`);
  assert.strictEqual(m.size, 3);
  assert.strictEqual(m.get('rh0'), undefined);
  assert.strictEqual(m.get('rh1'), undefined);
  assert.strictEqual(m.get('rh4'), 'm4');
});

test('entries expire after 12 hours', () => {
  let now = 0;
  const m = new HandleMap({ clock: () => now });
  m.put('old', 'm-old');
  now = MAX_AGE_MS - 1;
  assert.strictEqual(m.get('old'), 'm-old');
  m.put('new', 'm-new');
  now = MAX_AGE_MS + 1;
  assert.strictEqual(m.get('old'), undefined);
  assert.strictEqual(m.get('new'), 'm-new');
  now = 3 * MAX_AGE_MS;
  m.put('newest', 'm-newest'); // put also sweeps expired entries from the front
  assert.strictEqual(m.size, 1);
});

test('putting a handle again refreshes its age', () => {
  let now = 0;
  const m = new HandleMap({ clock: () => now, maxEntries: 2 });
  m.put('a', 'm1');
  m.put('b', 'm2');
  m.put('a', 'm1');
  m.put('c', 'm3'); // evicts b, the oldest
  assert.strictEqual(m.get('a'), 'm1');
  assert.strictEqual(m.get('b'), undefined);
});
