'use strict';
// Receipt handle -> message ID map for settle spans (contract section 8).
// Node.js runs this on one thread, so every method is atomic with respect to other callers.

const MAX_AGE_MS = 12 * 60 * 60 * 1000; // the longest SQS visibility timeout
const DEFAULT_MAX_ENTRIES = 100000;

/** Bounded: oldest entries are dropped first, and entries expire after 12 hours. */
class HandleMap {
  constructor({ maxEntries = DEFAULT_MAX_ENTRIES, maxAgeMs = MAX_AGE_MS, clock = () => performance.now() } = {}) {
    this._d = new Map(); // insertion order is time order
    this._max = maxEntries;
    this._age = maxAgeMs;
    this._clock = clock;
  }

  put(receiptHandle, messageId) {
    const now = this._clock();
    this._d.delete(receiptHandle);
    this._d.set(receiptHandle, [messageId, now]);
    while (this._d.size > this._max) this._d.delete(this._d.keys().next().value);
    for (const [rh, [, t]] of this._d) {
      if (now - t <= this._age) break;
      this._d.delete(rh);
    }
  }

  get(receiptHandle) {
    const v = this._d.get(receiptHandle);
    if (v === undefined) return undefined;
    if (this._clock() - v[1] > this._age) {
      this._d.delete(receiptHandle);
      return undefined;
    }
    return v[0];
  }

  discard(receiptHandle) {
    this._d.delete(receiptHandle);
  }

  get size() {
    return this._d.size;
  }
}

module.exports = { HandleMap, MAX_AGE_MS, DEFAULT_MAX_ENTRIES };
