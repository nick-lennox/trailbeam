'use strict';
// Call order inside one process (span contract schema version 3).
//
// `scope` names this realm's counter; `tick()` returns a strictly increasing integer, starting at 1.
// Two ticks with the same scope were taken in this order, whatever the clock said. Every qv2 client
// hook and Lambda wrapper loaded in this realm shares this one counter. Each worker_thread (or other
// realm) loads its own copy of this module and so gets its own scope, as it should: the counter is
// not shared across realms. JavaScript runs one realm on one thread, so no lock is needed.

const crypto = require('node:crypto');

const scope = crypto.randomUUID();
let last = 0;

/** The next value of this realm's call-order counter (1, 2, 3, ...). */
function tick() {
  last += 1;
  return last;
}

module.exports = { scope, tick };
