'use strict';
// The conformance kit's Lambda function (Node.js), wrapped the way the README tells users to.
//
// What it does with a batch is set by each record's body, {"case": ...} (SCENARIO-LAMBDA.md): it throws if
// any record is `throw`, returns an invalid batchItemFailures if any is `invalid`, and otherwise lists the
// `report` and `dlq` records as failures. On the dead-letter queue it succeeds.
//
// Its first act is one log line the poller reads (the "KIT_HANDLER" line): when the handler started, and
// the request ID and function name the runtime gave it. The checker compares that start time with the
// moment the Collector received each receive span, to see that the wrapper flushed them first.
const trailbeam = require('trailbeam');

/**
 * The wall clock now, in nanoseconds, to well under a millisecond. Date.now() has whole milliseconds, and
 * performance.timeOrigin + performance.now() drifts away from the wall clock over a long-lived process (by
 * 2 ms within seconds, seen on the bench). So: note the monotonic clock now, wait for Date.now() to tick
 * to the next millisecond (at most 1 ms), and count back from that moment, which is known exactly.
 */
function nowUnixNano() {
  const t0 = process.hrtime.bigint();
  const ms = Date.now();
  let next = Date.now();
  while (next === ms) next = Date.now();
  const t1 = process.hrtime.bigint();
  return BigInt(next) * 1000000n - (t1 - t0);
}

exports.handler = trailbeam.wrapHandler(async (event, context) => {
  const started = nowUnixNano();
  const records = event.Records || [];
  console.log('KIT_HANDLER ' + JSON.stringify({
    started_unix_nano: String(started), request_id: context.awsRequestId,
    function_name: context.functionName, log_stream: process.env.AWS_LAMBDA_LOG_STREAM_NAME || null,
    message_ids: records.map((r) => r.messageId),
  }));
  if (records.length && records.every((r) => r.eventSourceARN.endsWith('-dlq'))) return { batchItemFailures: [] };
  const cases = records.map((r) => JSON.parse(r.body).case);
  if (cases.includes('throw')) throw new Error('conformance kit: the handler threw (case throw)');
  if (cases.includes('invalid')) return { batchItemFailures: [{ itemIdentifier: 'not-a-record-of-this-batch' }] };
  const failures = records.filter((r, i) => cases[i] === 'report' || cases[i] === 'dlq').map((r) => ({ itemIdentifier: r.messageId }));
  if (!failures.length && cases.length === 1 && cases[0] === 'success') return undefined; // no batchItemFailures at all
  return { batchItemFailures: failures };
});
