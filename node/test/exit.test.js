'use strict';
// A short script that never calls uninstall() still delivers its spans (QV-156): the README's three-line
// snippet, run as a script that sends a message and ends, must reach the server.
const test = require('node:test');
const assert = require('node:assert');
const http = require('node:http');
const path = require('node:path');
const { spawn } = require('node:child_process');

const HELPERS = path.join(__dirname, 'helpers.js');
const INDEX = path.join(__dirname, '..', 'src', 'index.js');

/** An OTLP endpoint that records each export request body. */
async function otlpServer(t) {
  const bodies = [];
  const server = http.createServer((req, res) => {
    const chunks = [];
    req.on('data', (c) => chunks.push(c));
    req.on('end', () => {
      bodies.push(Buffer.concat(chunks));
      res.writeHead(200, { 'content-type': 'application/x-protobuf' });
      res.end();
    });
  });
  await new Promise((r) => server.listen(0, '127.0.0.1', r));
  t.after(() => server.close());
  return { bodies, url: `http://127.0.0.1:${server.address().port}/v1/traces` };
}

/** Run `script` in a new Node.js process; resolves with its exit code and stderr. */
function run(script, env) {
  return new Promise((resolve, reject) => {
    const child = spawn(process.execPath, ['-e', script], { env: { ...process.env, ...env } });
    let stderr = '';
    child.stderr.on('data', (c) => { stderr += c; });
    const timer = setTimeout(() => { child.kill(); reject(new Error(`script did not exit: ${stderr}`)); }, 20000);
    child.on('error', reject);
    child.on('close', (code) => { clearTimeout(timer); resolve({ code, stderr }); });
  });
}

// The README snippet, then a send. The stub client answers without a network, like a real SQS call would.
const SCRIPT = (ending) => `
const S = require('@aws-sdk/client-sqs');
const trailbeam = require(${JSON.stringify(INDEX)});
const { stubClient } = require(${JSON.stringify(HELPERS)});
const { client } = stubClient(() => ({ MessageId: 'mid-exit-1' }));
const sqs = trailbeam.instrument(client);
sqs.send(new S.SendMessageCommand({ QueueUrl: 'http://localhost:9324/000000000000/orders', MessageBody: 'x' }))
  .then((r) => { console.log(r.MessageId); ${ending} });
`;

test('a script that ends without uninstall() still exports its spans', async (t) => {
  const otlp = await otlpServer(t);
  const r = await run(SCRIPT(''), { TRAILBEAM_OTLP_TRACES_ENDPOINT: otlp.url, TRAILBEAM_RECEIPT_HASH_KEY: 'exit-test-key-0123456789' });
  assert.strictEqual(r.code, 0, r.stderr);
  assert.strictEqual(otlp.bodies.length, 1, r.stderr);
  assert.ok(otlp.bodies[0].includes(Buffer.from('mid-exit-1')));
  assert.doesNotMatch(r.stderr, /not exported/);
});

test('process.exit() with spans still queued prints a warning that names uninstall()', async (t) => {
  const otlp = await otlpServer(t);
  const r = await run(SCRIPT('process.exit(0);'), { TRAILBEAM_OTLP_TRACES_ENDPOINT: otlp.url, TRAILBEAM_RECEIPT_HASH_KEY: 'exit-test-key-0123456789' });
  assert.strictEqual(r.code, 0, r.stderr);
  assert.strictEqual(otlp.bodies.length, 0);
  assert.match(r.stderr, /trailbeam: 1 span was not exported because the process exited/);
  assert.match(r.stderr, /await trailbeam\.uninstall\(\)/);
});

test('process.exit() after uninstall() exports everything and prints no warning', async (t) => {
  const otlp = await otlpServer(t);
  const r = await run(SCRIPT('return trailbeam.uninstall().then(() => process.exit(0));'), { TRAILBEAM_OTLP_TRACES_ENDPOINT: otlp.url, TRAILBEAM_RECEIPT_HASH_KEY: 'exit-test-key-0123456789' });
  assert.strictEqual(r.code, 0, r.stderr);
  assert.strictEqual(otlp.bodies.length, 1);
  assert.doesNotMatch(r.stderr, /not exported/);
});
