'use strict';
// Default export: OTLP/HTTP protobuf to $TRAILBEAM_OTLP_TRACES_ENDPOINT (contract section 2).
const test = require('node:test');
const assert = require('node:assert');
const http = require('node:http');
const S = require('@aws-sdk/client-sqs');

const trailbeam = require('../src/index');
const { stubClient } = require('./helpers');

test('spans are posted as OTLP/HTTP protobuf to TRAILBEAM_OTLP_TRACES_ENDPOINT', async (t) => {
  const requests = [];
  const server = http.createServer((req, res) => {
    const chunks = [];
    req.on('data', (c) => chunks.push(c));
    req.on('end', () => {
      requests.push({ url: req.url, type: req.headers['content-type'], body: Buffer.concat(chunks) });
      res.writeHead(200, { 'content-type': 'application/x-protobuf' });
      res.end();
    });
  });
  await new Promise((r) => server.listen(0, '127.0.0.1', r));
  t.after(() => server.close());
  process.env.TRAILBEAM_OTLP_TRACES_ENDPOINT = `http://127.0.0.1:${server.address().port}/v1/traces`;
  t.after(() => { delete process.env.TRAILBEAM_OTLP_TRACES_ENDPOINT; });

  trailbeam.install({ receiptHashKey: 'otlp-test-key-0123456789' });
  const { client } = stubClient(() => ({ MessageId: 'mid-otlp-1' }));
  trailbeam.instrument(client);
  await client.send(new S.SendMessageCommand({ QueueUrl: 'http://localhost:9324/000000000000/orders', MessageBody: 'x' }));
  await trailbeam.forceFlush();
  await trailbeam.uninstall();

  assert.strictEqual(requests.length, 1);
  assert.strictEqual(requests[0].url, '/v1/traces');
  assert.strictEqual(requests[0].type, 'application/x-protobuf');
  for (const s of ['trailbeam send', 'mid-otlp-1', 'trailbeam.schema_version', 'messaging.client.id']) {
    assert.ok(requests[0].body.includes(Buffer.from(s)), s);
  }
});
