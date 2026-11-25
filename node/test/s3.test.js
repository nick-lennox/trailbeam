'use strict';
// Revision 3.7 (contract section 27): S3 uploads, against the shared vectors in contract/span-contract-vectors.json
// ("s3"), with canned S3 responses. Python checks the same vectors.
const test = require('node:test');
const assert = require('node:assert');
const fs = require('node:fs');
const path = require('node:path');
const api = require('@opentelemetry/api');
const { AsyncLocalStorageContextManager } = require('@opentelemetry/context-async-hooks');
const S3 = require('@aws-sdk/client-s3');

const trailbeam = require('../src/index');
const s3 = require('../src/s3');
const capture = require('../src/capture');
const { memoryExporter } = require('./helpers');

api.context.setGlobalContextManager(new AsyncLocalStorageContextManager().enable());

const VECTORS = JSON.parse(fs.readFileSync(path.join(__dirname, '..', '..', 'contract', 'span-contract-vectors.json'), 'utf8')).s3;
const UP = VECTORS.uploads;
const KEY = 'unit-test-key-0123456789';
const clone = (x) => JSON.parse(JSON.stringify(x));

const COMMANDS = {
  PutObject: S3.PutObjectCommand,
  CompleteMultipartUpload: S3.CompleteMultipartUploadCommand,
  CopyObject: S3.CopyObjectCommand,
};

/** An S3Client that never touches the network: `respond(operation, input)` returns the output or throws. */
function stubS3(respond) {
  const client = new S3.S3Client({
    region: 'us-east-1',
    endpoint: 'http://localhost:9000',
    forcePathStyle: true,
    credentials: { accessKeyId: 'test', secretAccessKey: 'test' },
    maxAttempts: 1,
  });
  const calls = [];
  client.middlewareStack.add((next, context) => async (args) => {
    const operation = context.commandName.replace(/Command$/, '');
    calls.push({ operation, input: args.input });
    const output = await respond(operation, args.input);
    return { output: { $metadata: { httpStatusCode: 200 }, ...output }, response: {} };
  }, { step: 'initialize', priority: 'low', name: 'testS3StubResponder' });
  return { client, calls };
}

function setup(t, respond, bodyQueues = []) {
  const { exporter, spanProcessor } = memoryExporter();
  trailbeam.install({ receiptHashKey: KEY, spanProcessor, capture: { bodyQueues } });
  const { client, calls } = stubS3(respond);
  trailbeam.instrument(client);
  t.after(() => trailbeam.uninstall());
  return { client, calls, spans: () => exporter.getFinishedSpans() };
}

for (const c of UP.cases) {
  test(`s3 uploads vector: ${c.name}`, async (t) => {
    const { client, calls, spans } = setup(t, (op) => {
      assert.strictEqual(op, c.operation);
      if (c.error) throw Object.assign(new Error(c.error), { name: c.error });
      return clone(c.response);
    }, c.body_queues);
    const input = clone(c.request);
    const before = Date.now();
    const cmd = new COMMANDS[c.operation](input);
    if (c.error) await assert.rejects(client.send(cmd), (e) => e.name === c.error);
    else await client.send(cmd);
    const after = Date.now();
    assert.deepStrictEqual(input, c.request); // the caller's object is not changed
    assert.deepStrictEqual(calls[0].input, c.request);
    const got = spans().filter((s) => s.name.startsWith('trailbeam'));
    assert.strictEqual(got.length, c.spans.length);
    got.forEach((s, i) => {
      const want = c.spans[i];
      assert.strictEqual(s.name, 'trailbeam s3_upload');
      const a = s.attributes;
      assert.deepStrictEqual(Object.fromEntries(Object.keys(want).map((k) => [k, a[k]])), want);
      const extra = Object.keys(a).filter((k) => !(k in want) && !UP.present.includes(k));
      assert.deepStrictEqual(extra, []);
      for (const k of UP.present) assert.ok(k in a, k);
      for (const k of UP.absent) assert.ok(!(k in a), k);
      assert.ok(!JSON.stringify(a).includes('secret'));
      assert.ok(a['trailbeam.order.issued'] < a['trailbeam.order.returned']);
      const ms = (hr) => hr[0] * 1000 + hr[1] / 1e6;
      assert.ok(ms(s.startTime) >= before && ms(s.endTime) <= after && ms(s.startTime) <= ms(s.endTime));
    });
  });
}

test('the S3 span has the process client ID of the SQS spans', async (t) => {
  const { client, spans } = setup(t, () => ({ ETag: '"e"' }));
  await client.send(new S3.PutObjectCommand({ Bucket: 'b', Key: 'k', Body: 'x' }));
  const [s] = spans();
  // s3.js reads it from the provider's resource: this fails if a new @opentelemetry/sdk-trace-base moves it
  assert.strictEqual(s.attributes['messaging.client.id'], s.resource.attributes['service.instance.id']);
  assert.ok(s.attributes['messaging.client.id']);
});

test('other S3 operations have no span', async (t) => {
  const { client, spans } = setup(t, () => ({ ETag: '"e"' }), ['*']);
  await client.send(new S3.GetObjectCommand({ Bucket: 'b', Key: 'k' })).catch(() => {});
  await client.send(new S3.DeleteObjectCommand({ Bucket: 'b', Key: 'k' }));
  await client.send(new S3.UploadPartCommand({ Bucket: 'b', Key: 'k', UploadId: 'u', PartNumber: 1, Body: 'x' }));
  assert.deepStrictEqual(spans(), []);
});

test('PutObject on a client that is not an S3Client gets no span', async () => {
  const mw = s3.s3Middleware(() => ({ onUpload: () => assert.fail('no span expected') }));
  const next = async () => ({ output: { ETag: '"e"' } });
  await mw(next, { clientName: 'MediaStoreDataClient', commandName: 'PutObjectCommand' })({ input: { Bucket: 'b' } });
});

test('a call error with a protocol namespace is recorded without it', async (t) => {
  const { client, spans } = setup(t, () => {
    throw Object.assign(new Error('x'), { name: 'com.amazonaws.s3#AccessDenied' });
  });
  await assert.rejects(client.send(new S3.PutObjectCommand({ Bucket: 'b', Key: 'k', Body: 'x' })));
  assert.strictEqual(spans()[0].attributes['trailbeam.error_code'], 'AccessDenied');
});

test('a hook that fails does not reach the caller', async (t) => {
  const { client, spans } = setup(t, () => ({ ETag: '"e"' }), ['*']); // capture on: the key is redacted
  const orig = capture.redactText;
  capture.redactText = () => { throw new Error('hook bug'); };
  t.after(() => { capture.redactText = orig; });
  const out = await client.send(new S3.PutObjectCommand({ Bucket: 'b', Key: 'k', Body: 'x' }));
  assert.strictEqual(out.ETag, '"e"');
  assert.deepStrictEqual(spans(), []);
});

test('uninstall turns the S3 hooks off', async () => {
  const { exporter, spanProcessor } = memoryExporter();
  trailbeam.install({ receiptHashKey: KEY, spanProcessor });
  const { client } = stubS3(() => ({ ETag: '"e"' }));
  trailbeam.instrument(client);
  await trailbeam.uninstall();
  await client.send(new S3.PutObjectCommand({ Bucket: 'b', Key: 'k', Body: 'x' }));
  assert.deepStrictEqual(exporter.getFinishedSpans(), []);
});

test('a key longer than S3 allows is not recorded', () => {
  const on = new capture.Config({ bodyQueues: ['*'] });
  assert.ok(!('trailbeam.s3.object_key' in s3.uploadAttrs(on, 'PutObject', { Bucket: 'b', Key: 'é'.repeat(513) }, {})));
  assert.ok(s3.uploadAttrs(on, 'PutObject', { Bucket: 'b', Key: 'é'.repeat(512) }, {})['trailbeam.s3.object_key']);
});
