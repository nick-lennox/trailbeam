'use strict';
// What happens to spans when the script ends or the Trailbeam server misbehaves (QV-169). The same cases
// as python/tests/test_failure_modes.py. In every one the application must not crash, its SQS calls must
// not be slowed, and its exit must not wait much longer than $TRAILBEAM_SHUTDOWN_TIMEOUT_MS; every span
// is either exported or counted and reported as a drop. A short script that ends without uninstall(), and
// process.exit() with spans queued, are in exit.test.js.
const test = require('node:test');
const assert = require('node:assert');
const http = require('node:http');
const path = require('node:path');
const { spawn } = require('node:child_process');
const S = require('@aws-sdk/client-sqs');

const trailbeam = require('../src/index');
const exp = require('../src/export');
const { stubClient } = require('./helpers');

const HELPERS = path.join(__dirname, 'helpers.js');
const INDEX = path.join(__dirname, '..', 'src', 'index.js');
const QUEUE_URL = 'http://localhost:9324/000000000000/orders';
const SHUTDOWN_MS = 500;
// Without the shutdown bound, the unreachable and slow cases keep the process open 8 to 10 s (the OTLP
// exporter's retries and request timeout).
const EXIT_BUDGET_MS = 4000;

/** A local OTLP endpoint. mode: 'ok' (200), '401', or 'slow' (never answers). */
async function otlpServer(t, mode = 'ok') {
  const s = { requests: 0 };
  const server = http.createServer((req, res) => {
    req.resume();
    req.on('end', () => {
      if (mode === 'slow') return; // the socket is closed by the client's timeout, or at the end of the test
      if (mode === '401') { res.writeHead(401); res.end(); return; }
      s.requests += 1;
      res.writeHead(200, { 'content-type': 'application/x-protobuf' });
      res.end();
    });
  });
  await new Promise((r) => server.listen(0, '127.0.0.1', r));
  s.url = `http://127.0.0.1:${server.address().port}/v1/traces`;
  s.stop = () => new Promise((r) => { server.close(r); server.closeAllConnections(); });
  t.after(() => s.stop());
  return s;
}

/** An endpoint with nothing listening: a port that was free a moment ago. */
async function refusedUrl() {
  const probe = http.createServer();
  await new Promise((r) => probe.listen(0, '127.0.0.1', r));
  const { port } = probe.address();
  await new Promise((r) => probe.close(r));
  return `http://127.0.0.1:${port}/v1/traces`;
}

// The README snippet, three sends, and the slowest send in ms on stdout.
const SCRIPT = `
const S = require('@aws-sdk/client-sqs');
const trailbeam = require(${JSON.stringify(INDEX)});
const { stubClient } = require(${JSON.stringify(HELPERS)});
const { client } = stubClient(() => ({ MessageId: 'mid-failure-modes' }));
const sqs = trailbeam.instrument(client);
(async () => {
  let slowest = 0;
  for (let i = 0; i < 3; i++) {
    const t = Date.now();
    await sqs.send(new S.SendMessageCommand({ QueueUrl: ${JSON.stringify(QUEUE_URL)}, MessageBody: 'x' }));
    slowest = Math.max(slowest, Date.now() - t);
  }
  console.log('slowest_send_ms=' + slowest);
})();
`;

function run(url) {
  return new Promise((resolve, reject) => {
    const env = { ...process.env, TRAILBEAM_OTLP_TRACES_ENDPOINT: url,
      TRAILBEAM_RECEIPT_HASH_KEY: 'failure-modes-key-0123456789', TRAILBEAM_SHUTDOWN_TIMEOUT_MS: String(SHUTDOWN_MS) };
    const t = Date.now();
    const child = spawn(process.execPath, ['-e', SCRIPT], { env });
    let stdout = '';
    let stderr = '';
    child.stdout.on('data', (c) => { stdout += c; });
    child.stderr.on('data', (c) => { stderr += c; });
    const timer = setTimeout(() => { child.kill(); reject(new Error(`script did not exit: ${stderr}`)); }, 30000);
    child.on('error', reject);
    child.on('close', (code) => {
      clearTimeout(timer);
      const m = /slowest_send_ms=(\d+)/.exec(stdout);
      resolve({ code, stderr, wall: Date.now() - t, slowestSend: m ? Number(m[1]) : undefined });
    });
  });
}

test('server unreachable from the start: the script runs and exits promptly, the spans are reported', async () => {
  const r = await run(await refusedUrl());
  assert.strictEqual(r.code, 0, r.stderr);
  assert.ok(r.slowestSend < 1000, `slowest send ${r.slowestSend} ms`);
  assert.ok(r.wall < EXIT_BUDGET_MS, `exit took ${r.wall} ms: ${r.stderr}`);
  assert.match(r.stderr, /trailbeam: dropped 3 spans/);
  assert.match(r.stderr, /unreachable/);
});

test('wrong token: the 401 is reported as failed exports that name OTEL_EXPORTER_OTLP_HEADERS', async (t) => {
  const otlp = await otlpServer(t, '401');
  const r = await run(otlp.url);
  assert.strictEqual(r.code, 0, r.stderr);
  assert.ok(r.wall < EXIT_BUDGET_MS, `exit took ${r.wall} ms: ${r.stderr}`);
  assert.match(r.stderr, /dropped 3 spans \(3 export failed\)/);
  assert.match(r.stderr, /OTEL_EXPORTER_OTLP_HEADERS/);
});

test('slow server: sends are not slowed, and the exit waits only for the shutdown timeout', async (t) => {
  const otlp = await otlpServer(t, 'slow');
  const r = await run(otlp.url);
  assert.strictEqual(r.code, 0, r.stderr);
  assert.ok(r.slowestSend < 1000, `slowest send ${r.slowestSend} ms`);
  assert.ok(r.wall < EXIT_BUDGET_MS, `exit took ${r.wall} ms: ${r.stderr}`);
  assert.match(r.stderr, /trailbeam: dropped 3 spans/);
});

test('server going away mid-run: later spans are drops, sends are not slowed, uninstall keeps to its timeout', async (t) => {
  const warnings = [];
  exp._setForTest({ warnFn: (m) => warnings.push(m) });
  t.after(() => exp._setForTest({}));
  const otlp = await otlpServer(t);
  process.env.TRAILBEAM_OTLP_TRACES_ENDPOINT = otlp.url;
  process.env.TRAILBEAM_SHUTDOWN_TIMEOUT_MS = String(SHUTDOWN_MS);
  t.after(() => { delete process.env.TRAILBEAM_OTLP_TRACES_ENDPOINT; delete process.env.TRAILBEAM_SHUTDOWN_TIMEOUT_MS; });

  trailbeam.install({ receiptHashKey: 'failure-modes-key-0123456789' });
  const { client } = stubClient(() => ({ MessageId: 'mid-going-away' }));
  trailbeam.instrument(client);
  await client.send(new S.SendMessageCommand({ QueueUrl: QUEUE_URL, MessageBody: 'before' }));
  await trailbeam.forceFlush();
  assert.strictEqual(trailbeam.exportStats().exported, 1);

  await otlp.stop();
  let slowest = 0;
  for (let i = 0; i < 3; i++) {
    const t0 = Date.now();
    await client.send(new S.SendMessageCommand({ QueueUrl: QUEUE_URL, MessageBody: 'after' }));
    slowest = Math.max(slowest, Date.now() - t0);
  }
  assert.ok(slowest < 1000, `slowest send ${slowest} ms`);
  const t0 = Date.now();
  await trailbeam.uninstall();
  assert.ok(Date.now() - t0 < SHUTDOWN_MS + 1000, `uninstall took ${Date.now() - t0} ms`);

  const s = trailbeam.exportStats();
  assert.strictEqual(s.exported, 1);
  assert.strictEqual(s.queued, 0);
  assert.strictEqual(s.dropped, 3);
  assert.ok(warnings.some((w) => /dropped \d+ spans/.test(w)));
});

test('shutdown keeps to its timeout with an export that never finishes, and counts what is left', async (t) => {
  exp._setForTest({ warnFn: () => {} });
  t.after(() => exp._setForTest({}));
  let late;
  const stuck = { export: (spans, cb) => { late = cb; }, shutdown: () => new Promise(() => {}) };
  const proc = new exp.CountingSpanProcessor(stuck, { shutdownTimeoutMillis: 200, exportTimeoutMillis: 60000 });
  const { BasicTracerProvider } = require('@opentelemetry/sdk-trace-base');
  const provider = new BasicTracerProvider({ spanProcessors: [proc] });
  provider.getTracer('t').startSpan('s').end();
  const t0 = Date.now();
  await proc.shutdown();
  assert.ok(Date.now() - t0 < 1500, `shutdown took ${Date.now() - t0} ms`);
  if (late) late({ code: 0 }); // a late success must not be counted as exported
  const s = exp.stats();
  assert.deepStrictEqual([s.exported, s.queued, s.droppedByReason.shutdown], [0, 0, 1]);
});

test('the OTLP exporter still has the export timeout that the shutdown bound lowers', () => {
  // If @opentelemetry/otlp-exporter-base renames this private field, the bound stops working: see the top
  // of src/export.js.
  const { OTLPTraceExporter } = require('@opentelemetry/exporter-trace-otlp-proto');
  const exporter = new OTLPTraceExporter({ url: 'http://127.0.0.1:9/v1/traces' });
  assert.strictEqual(typeof exporter._delegate._timeout, 'number');
  const proc = new exp.CountingSpanProcessor(exporter, { shutdownTimeoutMillis: 250 });
  proc._shortenExports();
  assert.strictEqual(exporter._delegate._timeout, 250);
  return proc.shutdown();
});

test('TRAILBEAM_SHUTDOWN_TIMEOUT_MS from the environment', (t) => {
  const warnings = [];
  exp._setForTest({ warnFn: (m) => warnings.push(m) });
  t.after(() => { exp._setForTest({}); delete process.env.TRAILBEAM_SHUTDOWN_TIMEOUT_MS; });
  process.env.TRAILBEAM_SHUTDOWN_TIMEOUT_MS = '750';
  assert.strictEqual(exp.configuredShutdownTimeoutMs(), 750);
  process.env.TRAILBEAM_SHUTDOWN_TIMEOUT_MS = 'soon';
  assert.strictEqual(exp.configuredShutdownTimeoutMs(), exp.DEFAULT_SHUTDOWN_TIMEOUT_MS);
  assert.ok(warnings.some((w) => /not a non-negative integer/.test(w)));
});
