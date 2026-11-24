'use strict';
// Revision 3.5 (contract section 23): what a receive records from an EventBridge event or an S3 event
// notification body, against the shared vectors in contract/span-contract-vectors.json ("origin"). Python checks
// the same vectors (python/tests/test_origin.py).
const test = require('node:test');
const assert = require('node:assert');
const fs = require('node:fs');
const path = require('node:path');
const api = require('@opentelemetry/api');
const { AsyncLocalStorageContextManager } = require('@opentelemetry/context-async-hooks');
const S = require('@aws-sdk/client-sqs');

const trailbeam = require('../src/index');
const capture = require('../src/capture');
const { memoryExporter, stubClient, byName } = require('./helpers');

api.context.setGlobalContextManager(new AsyncLocalStorageContextManager().enable());

const ALL = JSON.parse(fs.readFileSync(path.join(__dirname, '..', '..', 'contract', 'span-contract-vectors.json'), 'utf8'));
const VECTORS = ALL.origin;
const KEY = 'unit-test-key-0123456789';
const URL = 'http://localhost:9324/000000000000/orders';

const originOnly = (attrs) => Object.fromEntries(Object.entries(attrs)
  .filter(([k]) => k.startsWith('trailbeam.eventbridge.') || k.startsWith('trailbeam.s3.')));
const captureFor = (c) => ({ bodyQueues: c.body_captured ? ['orders'] : [] });

for (const c of VECTORS.receive) {
  test(`origin vector: ${c.name}`, () => {
    const config = capture.resolveConfig({}, captureFor(c), {});
    assert.deepStrictEqual(capture.originReceiveAttrs(config, 'orders', c.body), c.attributes);
  });

  test(`origin vector (client hooks): ${c.name}`, async (t) => {
    const { exporter, spanProcessor } = memoryExporter();
    trailbeam.install({ receiptHashKey: KEY, spanProcessor, capture: captureFor(c) });
    t.after(() => trailbeam.uninstall());
    const m = { MessageId: 'sqs-1', ReceiptHandle: 'rh-1', Body: c.body, Attributes: { ApproximateReceiveCount: '1' } };
    const { client } = stubClient((op) => (op === 'ReceiveMessage' ? { Messages: [m] } : {}));
    trailbeam.instrument(client);
    await client.send(new S.ReceiveMessageCommand({ QueueUrl: URL }));
    const [s] = byName(exporter.getFinishedSpans(), 'trailbeam receive');
    assert.deepStrictEqual(originOnly(s.attributes), c.attributes);
  });

  test(`origin vector (Lambda wrapper): ${c.name}`, async () => {
    const { exporter, spanProcessor } = memoryExporter();
    const handler = trailbeam.wrapHandler(async () => {}, { receiptHashKey: KEY, spanProcessor, capture: captureFor(c) });
    await handler({ Records: [{
      messageId: 'sqs-1', receiptHandle: 'rh-1', body: c.body, attributes: { ApproximateReceiveCount: '1' },
      messageAttributes: {}, eventSource: 'aws:sqs', eventSourceARN: 'arn:aws:sqs:us-east-2:111122223333:orders',
    }] }, { awsRequestId: 'req-1', functionName: 'fn', getRemainingTimeInMillis: () => 30000 });
    const [s] = byName(exporter.getFinishedSpans(), 'trailbeam receive');
    assert.deepStrictEqual(originOnly(s.attributes), c.attributes);
  });
}

test('an SNS envelope wins over an EventBridge reading', async (t) => {
  const env = ALL.sns.receive.find((c) => c.name.startsWith('envelope:'));
  const doc = { ...JSON.parse(env.body), id: '6a7e8feb-b491-4cf7-a9f1-bf3703467718', source: 'aws.ec2', 'detail-type': 'x', account: '111122223333', detail: {} };
  const { exporter, spanProcessor } = memoryExporter();
  trailbeam.install({ receiptHashKey: KEY, spanProcessor });
  t.after(() => trailbeam.uninstall());
  const { client } = stubClient((op) => (op === 'ReceiveMessage'
    ? { Messages: [{ MessageId: 'sqs-1', ReceiptHandle: 'rh-1', Body: JSON.stringify(doc), Attributes: {} }] } : {}));
  trailbeam.instrument(client);
  await client.send(new S.ReceiveMessageCommand({ QueueUrl: URL }));
  const [s] = byName(exporter.getFinishedSpans(), 'trailbeam receive');
  assert.ok(s.attributes['trailbeam.sns.message_id']);
  assert.deepStrictEqual(originOnly(s.attributes), {});
});
