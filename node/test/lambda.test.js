'use strict';
// Lambda wrapper (contract 4.2, 4.4): one receive (flushed before the handler) and one outcome per record.
const test = require('node:test');
const assert = require('node:assert');
const fs = require('node:fs');
const path = require('node:path');
const api = require('@opentelemetry/api');
const { AsyncLocalStorageContextManager } = require('@opentelemetry/context-async-hooks');
const { BasicTracerProvider, BatchSpanProcessor, InMemorySpanExporter, SimpleSpanProcessor } = require('@opentelemetry/sdk-trace-base');

const qv2 = require('../src/index');
const { decideOutcomes, queueFromArn, sqsRecords, errorType } = require('../src/lambda');

api.context.setGlobalContextManager(new AsyncLocalStorageContextManager().enable());

// Recorded events shared with the Python wrapper's tests (see build_qv9_lambda_events.py there).
const FIXTURES = path.join(__dirname, '..', '..', 'python', 'tests', 'fixtures');
const QV9 = JSON.parse(fs.readFileSync(path.join(FIXTURES, 'qv9_lambda_events.json'), 'utf8'));
const DOCS = JSON.parse(fs.readFileSync(path.join(FIXTURES, 'aws_docs_sqs_event.json'), 'utf8')).event;

const KEY = 'example-key-0123456789abcdef'; // key ID 3b3edeb7, from docs/span-contract-vectors.json
const clone = (x) => JSON.parse(JSON.stringify(x));
const ctx = (awsRequestId = 'req-1', functionName = 'orders-fn') => ({ awsRequestId, functionName, getRemainingTimeInMillis: () => 30000 });

/** A wrapped handler. With `batch`, the production BatchSpanProcessor: spans only show up if the wrapper flushes. */
function wrapped(fn, { batch = true, ...options } = {}) {
  const exporter = new InMemorySpanExporter();
  const spanProcessor = batch
    ? new BatchSpanProcessor(exporter, { scheduledDelayMillis: 60000 })
    : new SimpleSpanProcessor(exporter);
  const handler = qv2.wrapHandler(fn, { receiptHashKey: KEY, spanProcessor, ...options });
  return { handler, exporter, spans: (name) => exporter.getFinishedSpans().filter((s) => !name || s.name === name) };
}

function withEnv(t, name, value) {
  const saved = process.env[name];
  if (value === undefined) delete process.env[name]; else process.env[name] = value;
  t.after(() => { if (saved === undefined) delete process.env[name]; else process.env[name] = saved; });
}

// -- every delivery gets a receive and an outcome ---------------------------------------------

class Killed extends Error {}

/** The handler behaviour recorded for each invocation. `killed` never returns, like a timeout. */
function behave(spec) {
  return async () => {
    if (spec.kind === 'raise') throw new ({ RuntimeError: class RuntimeError extends Error {} })[spec.error_type]('synthetic failure');
    if (spec.kind === 'killed') return new Promise(() => {});
    return { batchItemFailures: spec.batchItemFailures.map((itemIdentifier) => ({ itemIdentifier })) };
  };
}

test('recorded QV-9 events: every delivery gets a receive and an outcome', async (t) => {
  assert.strictEqual(QV9.invocations.length, 24);
  let deliveries = 0;
  for (const inv of QV9.invocations) {
    withEnv(t, 'AWS_LAMBDA_LOG_STREAM_NAME', inv.log_stream_name);
    const records = inv.event.Records;
    const seenBeforeHandler = [];
    const inner = behave(inv.behaviour);
    const { handler, exporter } = wrapped(async (event, context) => {
      seenBeforeHandler.push(...exporter.getFinishedSpans().map((s) => s.name));
      return inner(event, context);
    });
    const event = clone(inv.event);
    const c = ctx(inv.aws_request_id, inv.function_name);
    const run = handler(event, c);
    if (inv.behaviour.kind === 'killed') {
      // The runtime would kill the invocation here; give the wrapper every chance to (wrongly) emit an outcome.
      await new Promise((r) => setTimeout(r, 50));
    } else if (inv.behaviour.kind === 'raise') {
      await assert.rejects(run, { name: 'Error', message: 'synthetic failure' });
    } else {
      assert.deepStrictEqual(await run, await inner(event, c));
    }
    const finished = exporter.getFinishedSpans(); // before shutdown: the wrapper must have flushed
    await handler.qv2TracerProvider.shutdown();
    deliveries += records.length;
    assert.deepStrictEqual(event, inv.event, 'the wrapper does not modify the event');

    assert.deepStrictEqual(seenBeforeHandler, Array(records.length).fill('qv2 receive'), 'receives exported before the handler ran');
    const receives = new Map(finished.filter((s) => s.name === 'qv2 receive').map((s) => [s.attributes['messaging.message.id'], s]));
    const outcomes = new Map(finished.filter((s) => s.name === 'qv2 outcome').map((s) => [s.attributes['messaging.message.id'], s]));
    assert.strictEqual(finished.length, records.length + outcomes.size);
    assert.strictEqual(receives.size, records.length);
    for (const r of records) {
      const mid = r.messageId;
      const rcv = receives.get(mid).attributes;
      assert.strictEqual(rcv['qv2.receipt_handle.hmac'], qv2.hashReceiptHandle(KEY, r.receiptHandle));
      assert.strictEqual(rcv['qv2.receive_count'], Number(r.attributes.ApproximateReceiveCount));
      assert.strictEqual(rcv['faas.invocation_id'], inv.aws_request_id);
      assert.strictEqual(rcv['messaging.client.id'], inv.log_stream_name);
      const want = inv.expected[mid];
      if (want === null) {
        assert.ok(!outcomes.has(mid), 'killed: no outcome; the store infers it from the receive');
      } else {
        const out = outcomes.get(mid);
        assert.strictEqual(out.attributes['qv2.outcome'], want, `${inv.aws_request_id} ${mid}`);
        assert.strictEqual(out.attributes['faas.invocation_id'], inv.aws_request_id);
        if (want === 'failed_exception') assert.strictEqual(out.attributes['qv2.error_type'], 'RuntimeError');
        else assert.ok(!('qv2.error_type' in out.attributes));
        const tOut = out.startTime[0] * 1e9 + out.startTime[1];
        const tRcv = receives.get(mid).endTime[0] * 1e9 + receives.get(mid).endTime[1];
        assert.ok(tOut >= tRcv);
      }
    }
    if (inv.behaviour.kind === 'killed') assert.strictEqual(outcomes.size, 0);
    else assert.deepStrictEqual([...outcomes.keys()].sort(), [...receives.keys()].sort());
  }
  assert.strictEqual(deliveries, 34);
});

test('recorded QV-9 events cover every outcome', () => {
  const got = new Set(QV9.invocations.flatMap((inv) => Object.values(inv.expected)));
  assert.deepStrictEqual([...got].sort(), ['failed_exception', 'failed_invalid_response', 'failed_reported', null, 'success'].sort());
});

// -- span contents ----------------------------------------------------------------------------

test('receive and outcome attributes for the AWS documentation event', async (t) => {
  withEnv(t, 'AWS_LAMBDA_LOG_STREAM_NAME', '2026/11/13/[$LATEST]abc');
  const { handler, spans } = wrapped(async () => undefined, { batch: false });
  assert.strictEqual(await handler(clone(DOCS), ctx()), undefined);
  const rcv = spans('qv2 receive');
  const out = spans('qv2 outcome');
  assert.strictEqual(rcv.length, 2);
  assert.strictEqual(out.length, 2);
  assert.deepStrictEqual(rcv[0].attributes, {
    'qv2.schema_version': 1,
    'qv2.kind': 'sqs.receive',
    'qv2.source': 'lambda',
    'messaging.system.name': 'aws.sqs',
    'messaging.operation.type': 'receive',
    'messaging.operation.name': 'LambdaEvent',
    'messaging.client.id': '2026/11/13/[$LATEST]abc',
    'messaging.destination.name': 'my-queue',
    'aws.sqs.queue.url': 'https://sqs.us-east-2.amazonaws.com/123456789012/my-queue',
    'qv2.queue.arn': 'arn:aws:sqs:us-east-2:123456789012:my-queue',
    'process.pid': process.pid,
    'messaging.message.id': '059f36b4-87a3-44ab-83d2-661975830a7d',
    'qv2.receipt_handle.hmac': qv2.hashReceiptHandle(KEY, 'AQEBwJnKyrHigUMZj6rYigCgxlaS3SLy0a...'),
    'qv2.receipt_handle.key_id': '3b3edeb7',
    'qv2.receive_count': 1,
    'qv2.sent_timestamp_ms': 1545082649183,
    'qv2.first_receive_timestamp_ms': 1545082649185,
    'faas.name': 'orders-fn',
    'faas.invocation_id': 'req-1',
  });
  const o = out[0].attributes;
  assert.strictEqual(o['qv2.kind'], 'sqs.outcome');
  assert.strictEqual(o['messaging.operation.type'], 'process');
  assert.strictEqual(o['qv2.source'], 'lambda');
  assert.strictEqual(o['qv2.outcome'], 'success');
  assert.ok(!('qv2.error_type' in o));
  assert.deepStrictEqual([o['faas.name'], o['faas.invocation_id']], ['orders-fn', 'req-1']);
  for (const s of [...rcv, ...out]) {
    assert.strictEqual(s.instrumentationScope.name, 'qv2');
    assert.deepStrictEqual(s.startTime, s.endTime);
    assert.strictEqual(s.kind, api.SpanKind.INTERNAL);
    for (const v of Object.values(s.attributes)) {
      assert.ok(v !== '' && !String(v).includes('AQEB') && !String(v).includes('Test message'));
    }
  }
});

test('an exception records the class name only, and is rethrown', async () => {
  class SecretError extends TypeError {}
  const { handler, spans } = wrapped(async () => { throw new SecretError('contains customer data'); }, { batch: false });
  await assert.rejects(handler(clone(DOCS), ctx()), SecretError);
  const out = spans('qv2 outcome');
  assert.deepStrictEqual(out.map((s) => s.attributes['qv2.outcome']), ['failed_exception', 'failed_exception']);
  assert.deepStrictEqual(new Set(out.map((s) => s.attributes['qv2.error_type'])), new Set(['SecretError']));
  assert.ok(!out.some((s) => Object.values(s.attributes).some((v) => String(v).includes('customer'))));
});

test('a synchronous handler, and one that throws synchronously', async () => {
  let w = wrapped(() => ({ batchItemFailures: [{ itemIdentifier: DOCS.Records[1].messageId }] }), { batch: false });
  assert.deepStrictEqual(await w.handler(clone(DOCS), ctx()), { batchItemFailures: [{ itemIdentifier: DOCS.Records[1].messageId }] });
  assert.deepStrictEqual(w.spans('qv2 outcome').map((s) => s.attributes['qv2.outcome']), ['success', 'failed_reported']);

  w = wrapped(() => { throw new RangeError('x'); }, { batch: false });
  await assert.rejects(w.handler(clone(DOCS), ctx()), RangeError);
  assert.deepStrictEqual(w.spans('qv2 outcome').map((s) => s.attributes['qv2.error_type']), ['RangeError', 'RangeError']);
});

test('a callback-style handler', async () => {
  // eslint-disable-next-line no-unused-vars
  let w = wrapped((event, context, callback) => { setImmediate(() => callback(null, { batchItemFailures: [] })); }, { batch: false });
  assert.deepStrictEqual(await w.handler(clone(DOCS), ctx()), { batchItemFailures: [] });
  assert.deepStrictEqual(w.spans('qv2 outcome').map((s) => s.attributes['qv2.outcome']), ['success', 'success']);

  w = wrapped((event, context, callback) => callback(new SyntaxError('bad')), { batch: false });
  await assert.rejects(w.handler(clone(DOCS), ctx()), SyntaxError);
  assert.deepStrictEqual(w.spans('qv2 outcome').map((s) => s.attributes['qv2.outcome']), ['failed_exception', 'failed_exception']);
  assert.strictEqual(w.handler.length, 2, 'the wrapper itself is an async (event, context) handler');
});

test('spans are children of the active span at invocation time, or root spans', async () => {
  const { handler, exporter, spans } = wrapped(async () => undefined, { batch: false });
  const parent = new BasicTracerProvider().getTracer('app').startSpan('invocation');
  await api.context.with(api.trace.setSpan(api.context.active(), parent), () => handler(clone(DOCS), ctx()));
  for (const s of spans()) assert.strictEqual(s.parentSpanContext.spanId, parent.spanContext().spanId);
  exporter.reset();
  await handler(clone(DOCS), ctx());
  assert.ok(spans().length && spans().every((s) => !s.parentSpanContext));
});

test('non-SQS events pass through untouched, with no spans', async () => {
  const { handler, spans } = wrapped(async (e) => e, { batch: false });
  const sns = { Records: [{ eventSource: 'aws:sns' }] };
  assert.strictEqual(await handler(sns, ctx()), sns);
  assert.deepStrictEqual(await handler({ hello: 1 }, ctx()), { hello: 1 });
  assert.deepStrictEqual(await handler([1, 2], ctx()), [1, 2]);
  assert.deepStrictEqual(spans(), []);
});

test('without AWS_LAMBDA_LOG_STREAM_NAME, messaging.client.id falls back to service.instance.id', async (t) => {
  withEnv(t, 'AWS_LAMBDA_LOG_STREAM_NAME', undefined);
  const { handler, spans } = wrapped(async () => undefined, { batch: false });
  await handler(clone(DOCS), ctx());
  const [s] = spans();
  assert.strictEqual(s.attributes['messaging.client.id'], s.resource.attributes['service.instance.id']);
});

test('faas.name falls back to AWS_LAMBDA_FUNCTION_NAME; a missing request ID is omitted', async (t) => {
  withEnv(t, 'AWS_LAMBDA_FUNCTION_NAME', 'env-fn');
  const { handler, spans } = wrapped(async () => undefined, { batch: false });
  await handler(clone(DOCS), {});
  for (const s of spans()) {
    assert.strictEqual(s.attributes['faas.name'], 'env-fn');
    assert.ok(!('faas.invocation_id' in s.attributes));
  }
});

test('an exporter that never answers does not hold the invocation past the flush timeout', async () => {
  const stuck = { export() {}, shutdown: async () => {}, forceFlush: () => new Promise(() => {}) };
  const spanProcessor = { onStart() {}, onEnd() {}, shutdown: async () => {}, forceFlush: () => stuck.forceFlush() };
  const handler = qv2.wrapHandler(async () => 'done', { receiptHashKey: KEY, spanProcessor, flushTimeoutMs: 20 });
  const t0 = Date.now();
  assert.strictEqual(await handler(clone(DOCS), ctx()), 'done');
  assert.ok(Date.now() - t0 < 1000);
});

test('wrapHandler rejects a non-function', () => {
  assert.throws(() => qv2.wrapHandler('nope'), TypeError);
});

// -- outcome rules ------------------------------------------------------------------------------

const IDS = ['a', 'b', 'c'];
const CASES = [
  [undefined, ['success', 'success', 'success']],
  [null, ['success', 'success', 'success']],
  [{}, ['success', 'success', 'success']],
  ['ok', ['success', 'success', 'success']],
  [{ batchItemFailures: null }, ['success', 'success', 'success']],
  [{ batchItemFailures: [] }, ['success', 'success', 'success']],
  [{ batchItemFailures: [{ itemIdentifier: 'b' }] }, ['success', 'failed_reported', 'success']],
  [{ batchItemFailures: [{ itemIdentifier: 'a' }, { itemIdentifier: 'c' }] }, ['failed_reported', 'success', 'failed_reported']],
  [{ batchItemFailures: [{ itemIdentifier: 'b' }, { itemIdentifier: 'zzz' }] }, Array(3).fill('failed_invalid_response')],
  [{ batchItemFailures: [{ itemIdentifier: '' }] }, Array(3).fill('failed_invalid_response')],
  [{ batchItemFailures: [{ itemIdentifier: null }] }, Array(3).fill('failed_invalid_response')],
  [{ batchItemFailures: [{ itemId: 'a' }] }, Array(3).fill('failed_invalid_response')],
  [{ batchItemFailures: ['a'] }, Array(3).fill('failed_invalid_response')],
  [{ batchItemFailures: 'a' }, Array(3).fill('failed_invalid_response')],
];

test('decideOutcomes follows the partial batch response rules (same cases as the Python tests)', () => {
  for (const [result, expected] of CASES) {
    const got = decideOutcomes(IDS, result);
    assert.deepStrictEqual(IDS.map((i) => got[i]), expected, JSON.stringify(result));
  }
});

test('batchItemFailures are ignored when the mapping does not report them', async () => {
  const r = { batchItemFailures: [{ itemIdentifier: 'zzz' }] };
  assert.deepStrictEqual(new Set(Object.values(decideOutcomes(IDS, r, false))), new Set(['success']));
  const { handler, spans } = wrapped(async () => r, { batch: false, reportBatchItemFailures: false });
  await handler(clone(DOCS), ctx());
  assert.deepStrictEqual(spans('qv2 outcome').map((s) => s.attributes['qv2.outcome']), ['success', 'success']);
});

test('queueFromArn, sqsRecords and errorType', () => {
  assert.deepStrictEqual(queueFromArn('arn:aws:sqs:eu-west-1:111122223333:orders'),
    ['orders', 'https://sqs.eu-west-1.amazonaws.com/111122223333/orders']);
  assert.deepStrictEqual(queueFromArn('garbage'), ['garbage', '']);
  assert.deepStrictEqual(queueFromArn(undefined), ['', '']);
  assert.strictEqual(sqsRecords(DOCS).length, 2);
  assert.deepStrictEqual(sqsRecords({ Records: [{ eventSource: 'aws:sqs' }, null, { eventSource: 'aws:sqs', messageId: '' }] }), []);
  assert.strictEqual(errorType(new TypeError('x')), 'TypeError');
  assert.strictEqual(errorType('a string'), 'string');
  assert.strictEqual(errorType(Object.create(null)), 'Object');
  assert.strictEqual(errorType(new Killed()), 'Killed');
});
