'use strict';
// Names and values from docs/span-contract.md (schema version 3), and the receipt-handle hash.

const crypto = require('node:crypto');

const SCHEMA_VERSION = 3;
const SCOPE_NAME = 'qv2';

const DEFAULT_KEY = 'qv2-default-receipt-hash-key';
const KEY_ENV = 'QV2_RECEIPT_HASH_KEY';
const ENDPOINT_ENV = 'QV2_OTLP_TRACES_ENDPOINT';
const DEFAULT_ENDPOINT = 'http://localhost:4318/v1/traces';
const MIN_KEY_BYTES = 16;

// Span names (section 1). They never contain a variable part.
const SPAN_SEND = 'qv2 send';
const SPAN_RECEIVE = 'qv2 receive';
const SPAN_DELETE = 'qv2 delete';
const SPAN_CHANGE_VISIBILITY = 'qv2 change_visibility';
const SPAN_OUTCOME = 'qv2 outcome';

// [qv2.kind, messaging.operation.type] per span name.
const KINDS = {
  [SPAN_SEND]: ['sqs.send', 'send'],
  [SPAN_RECEIVE]: ['sqs.receive', 'receive'],
  [SPAN_DELETE]: ['sqs.settle', 'settle'],
  [SPAN_CHANGE_VISIBILITY]: ['sqs.settle', 'settle'],
  [SPAN_OUTCOME]: ['sqs.outcome', 'process'],
};

const OUTCOME_SUCCESS = 'success';
const OUTCOME_FAILED_REPORTED = 'failed_reported';
const OUTCOME_FAILED_INVALID_RESPONSE = 'failed_invalid_response';
const OUTCOME_FAILED_EXCEPTION = 'failed_exception';

function hmacHex(key, data) {
  return crypto.createHmac('sha256', Buffer.from(key, 'utf8')).update(Buffer.from(data, 'utf8')).digest('hex');
}

/** `qv2.receipt_handle.hmac` for a receipt handle (section 7). */
function hashReceiptHandle(key, receiptHandle) {
  return hmacHex(key, receiptHandle).slice(0, 32);
}

/** `qv2.receipt_handle.key_id` for a key (section 7). */
function keyId(key) {
  return hmacHex(key, 'qv2 key id').slice(0, 8);
}

/** Holds the key and hashes receipt handles. Never logs or exports the key. */
class ReceiptHasher {
  #key;

  constructor(key) {
    this.#key = Buffer.from(key, 'utf8');
    this.keyId = crypto.createHmac('sha256', this.#key).update('qv2 key id', 'utf8').digest('hex').slice(0, 8);
  }

  hash(receiptHandle) {
    return crypto.createHmac('sha256', this.#key).update(receiptHandle, 'utf8').digest('hex').slice(0, 32);
  }

  toJSON() { return { keyId: this.keyId }; }

  [Symbol.for('nodejs.util.inspect.custom')]() { return `ReceiptHasher { keyId: '${this.keyId}' }`; }
}

const warned = new Set();
let warn = (msg) => process.emitWarning(msg, { code: 'QV2' });

function warnOnce(tag, msg) {
  if (warned.has(tag)) return;
  warned.add(tag);
  warn(msg);
}

/** Explicit key, then $QV2_RECEIPT_HASH_KEY, then the published default key (with one warning). */
function resolveHasher(key) {
  if (key === undefined || key === null || key === '') key = process.env[KEY_ENV] || undefined;
  if (key === undefined) {
    warnOnce('default-key',
      'qv2: no receipt-hash key configured (QV2_RECEIPT_HASH_KEY); using the published default key, ' +
      'which gives no protection. Set the same key on every service that uses these queues.');
    key = DEFAULT_KEY;
  } else if (Buffer.byteLength(key, 'utf8') < MIN_KEY_BYTES) {
    warnOnce('short-key', `qv2: the receipt-hash key is shorter than ${MIN_KEY_BYTES} bytes`);
  }
  return new ReceiptHasher(key);
}

/** For tests: replace the warning function and forget which warnings were given. */
function _setWarn(fn) {
  warned.clear();
  if (fn) warn = fn;
}

function queueNameFromUrl(url) {
  if (typeof url !== 'string' || !url) return undefined;
  const parts = url.replace(/\/+$/, '').split('/');
  return parts[parts.length - 1] || undefined;
}

/** Set an attribute unless it has no value (section 3: absent values are omitted). */
function put(attrs, name, value) {
  if (value === undefined || value === null || value === '') return;
  if (typeof value === 'number' && !Number.isFinite(value)) return;
  attrs[name] = value;
}

/** Integer from an SQS attribute string, or undefined. */
function intOrUndefined(value) {
  if (value === undefined || value === null || value === '') return undefined;
  if (typeof value === 'number') return Number.isInteger(value) ? value : undefined;
  if (typeof value !== 'string' || !/^-?\d+$/.test(value.trim())) return undefined;
  const n = Number(value.trim());
  return Number.isSafeInteger(n) ? n : undefined;
}

module.exports = {
  SCHEMA_VERSION,
  SCOPE_NAME,
  DEFAULT_KEY,
  KEY_ENV,
  ENDPOINT_ENV,
  DEFAULT_ENDPOINT,
  MIN_KEY_BYTES,
  SPAN_SEND,
  SPAN_RECEIVE,
  SPAN_DELETE,
  SPAN_CHANGE_VISIBILITY,
  SPAN_OUTCOME,
  KINDS,
  OUTCOME_SUCCESS,
  OUTCOME_FAILED_REPORTED,
  OUTCOME_FAILED_INVALID_RESPONSE,
  OUTCOME_FAILED_EXCEPTION,
  hashReceiptHandle,
  keyId,
  ReceiptHasher,
  resolveHasher,
  queueNameFromUrl,
  put,
  intOrUndefined,
  _setWarn,
};
