'use strict';
// Checks contract/span-contract-vectors.json with node:crypto (contract section 7).
const test = require('node:test');
const assert = require('node:assert');
const crypto = require('node:crypto');
const fs = require('node:fs');
const path = require('node:path');

const vectors = JSON.parse(fs.readFileSync(path.join(__dirname, '..', '..', 'contract', 'span-contract-vectors.json'), 'utf8'));
const hmacHex = (key, data) => crypto.createHmac('sha256', Buffer.from(key, 'utf8')).update(Buffer.from(data, 'utf8')).digest('hex');

test('vectors match HMAC-SHA256', () => {
  assert.ok(vectors.vectors.length > 0);
  for (const v of vectors.vectors) {
    assert.strictEqual(hmacHex(v.key, v.receipt_handle).slice(0, 32), v.hmac, v.name);
    assert.strictEqual(hmacHex(v.key, 'trailbeam key id').slice(0, 8), v.key_id, v.name);
  }
});
