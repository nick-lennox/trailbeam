'use strict';
const test = require('node:test');
const assert = require('node:assert');
const sdk = require('../src/index.js');

test('version is set', () => {
  assert.strictEqual(sdk.version, '0.3.0');
});
