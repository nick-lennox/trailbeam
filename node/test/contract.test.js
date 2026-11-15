'use strict';
// Receipt-handle hash (contract section 7) against the shared vectors, key resolution, helpers.
const test = require('node:test');
const assert = require('node:assert');
const fs = require('node:fs');
const path = require('node:path');
const util = require('node:util');

const contract = require('../src/contract');
const sdk = require('../src/index');

const VECTORS = JSON.parse(fs.readFileSync(path.join(__dirname, '..', '..', 'docs', 'span-contract-vectors.json'), 'utf8'));

test('hash functions match the shared test vectors (same as the Python package)', () => {
  assert.ok(VECTORS.vectors.length >= 3);
  for (const v of VECTORS.vectors) {
    assert.strictEqual(sdk.hashReceiptHandle(v.key, v.receipt_handle), v.hmac, v.name);
    assert.strictEqual(sdk.keyId(v.key), v.key_id, v.name);
    const h = new contract.ReceiptHasher(v.key);
    assert.strictEqual(h.hash(v.receipt_handle), v.hmac, v.name);
    assert.strictEqual(h.keyId, v.key_id, v.name);
  }
});

test('default key is the published one, with the ID the contract names', () => {
  assert.strictEqual(contract.DEFAULT_KEY, VECTORS.default_key);
  assert.strictEqual(sdk.keyId(contract.DEFAULT_KEY), 'b5e633f7');
});

test('ReceiptHasher never shows its key', () => {
  const key = 'secret-key-0123456789abcdef';
  const h = new contract.ReceiptHasher(key);
  assert.ok(!util.inspect(h).includes(key));
  assert.ok(!JSON.stringify(h).includes(key));
  assert.ok(!String(h).includes(key));
});

test('resolveHasher: explicit key, then environment, then default with one warning', (t) => {
  const warnings = [];
  contract._setWarn((m) => warnings.push(m));
  const saved = process.env.TRAILBEAM_RECEIPT_HASH_KEY;
  t.after(() => {
    if (saved === undefined) delete process.env.TRAILBEAM_RECEIPT_HASH_KEY; else process.env.TRAILBEAM_RECEIPT_HASH_KEY = saved;
    contract._setWarn(null);
  });

  process.env.TRAILBEAM_RECEIPT_HASH_KEY = 'env-key-0123456789abcdef';
  assert.strictEqual(contract.resolveHasher('explicit-key-0123456789').keyId, sdk.keyId('explicit-key-0123456789'));
  assert.strictEqual(contract.resolveHasher().keyId, sdk.keyId('env-key-0123456789abcdef'));
  assert.deepStrictEqual(warnings, []);

  delete process.env.TRAILBEAM_RECEIPT_HASH_KEY;
  assert.strictEqual(contract.resolveHasher().keyId, 'b5e633f7');
  assert.strictEqual(contract.resolveHasher().keyId, 'b5e633f7');
  assert.strictEqual(warnings.length, 1, 'one default-key warning per process');
  assert.match(warnings[0], /default key/);

  contract.resolveHasher('short');
  contract.resolveHasher('short');
  assert.strictEqual(warnings.length, 2, 'one short-key warning per process');
  assert.match(warnings[1], /shorter than 16 bytes/);
  assert.ok(!warnings.join(' ').includes('short-'), 'the key itself is not in the warning');
});

test('short-key check counts UTF-8 bytes, not characters', (t) => {
  const warnings = [];
  contract._setWarn((m) => warnings.push(m));
  t.after(() => contract._setWarn(null));
  contract.resolveHasher('ééééééééé'); // 9 characters, 18 bytes
  assert.deepStrictEqual(warnings, []);
});

test('queueNameFromUrl, put and intOrUndefined', () => {
  assert.strictEqual(contract.queueNameFromUrl('http://localhost:9324/000000000000/orders'), 'orders');
  assert.strictEqual(contract.queueNameFromUrl('https://sqs.us-east-1.amazonaws.com/1234/orders/'), 'orders');
  assert.strictEqual(contract.queueNameFromUrl(''), undefined);
  assert.strictEqual(contract.queueNameFromUrl(undefined), undefined);

  const a = {};
  contract.put(a, 'x', '');
  contract.put(a, 'y', undefined);
  contract.put(a, 'z', null);
  contract.put(a, 'n', NaN);
  contract.put(a, 'zero', 0);
  assert.deepStrictEqual(a, { zero: 0 });

  assert.strictEqual(contract.intOrUndefined('12'), 12);
  assert.strictEqual(contract.intOrUndefined('1794624825412'), 1794624825412);
  assert.strictEqual(contract.intOrUndefined(0), 0);
  assert.strictEqual(contract.intOrUndefined('0'), 0);
  assert.strictEqual(contract.intOrUndefined('abc'), undefined);
  assert.strictEqual(contract.intOrUndefined('1.5'), undefined);
  assert.strictEqual(contract.intOrUndefined(''), undefined);
  assert.strictEqual(contract.intOrUndefined(undefined), undefined);
});
