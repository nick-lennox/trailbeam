'use strict';
// The default export path counts and reports every Trailbeam span it drops (QV-75). Each test forces drops
// and checks that the counter, the metric and the warnings agree with the spans the exporter never got.
const test = require('node:test');
const assert = require('node:assert');
const api = require('@opentelemetry/api');

const trailbeam = require('../src/index');
const exp = require('../src/export');
const { makeProvider } = require('../src/emitter');

const LINE = /trailbeam: dropped (\d+) spans.*?\((.*?)\); (\d+) dropped since start/;

// A stand-in global meter provider that records what the drop counter is given.
const recorded = [];
api.metrics.setGlobalMeterProvider({
  getMeter: () => ({ createCounter: (name) => ({ add: (n, attrs) => recorded.push({ name, n, ...attrs }) }) }),
});
const metricTotal = () => {
  const out = {};
  for (const r of recorded) if (r.name === exp.METRIC_NAME) out[r['error.type']] = (out[r['error.type']] || 0) + r.n;
  return out;
};

/** Records what it is given. Holds every export until `release()`; fails while `fail` is set. */
class Gate {
  constructor({ blocked = false } = {}) {
    this.received = 0;
    this.fail = false;
    this.pending = [];
    this.blocked = blocked;
  }
  export(spans, cb) {
    const run = () => {
      if (this.fail) return cb({ code: 1, error: new Error('down') });
      this.received += spans.length;
      cb({ code: 0 });
    };
    if (this.blocked) this.pending.push(run); else run();
  }
  release() { this.blocked = false; for (const run of this.pending.splice(0)) run(); }
  shutdown() { return Promise.resolve(); }
}

function setup(t, { env, reportIntervalMs = 60000 } = {}) {
  const warnings = [];
  exp._setForTest({ warnFn: (m) => warnings.push(m), reportIntervalMs });
  recorded.length = 0;
  if (env !== undefined) process.env[exp.QUEUE_SIZE_ENV] = env;
  t.after(() => { delete process.env[exp.QUEUE_SIZE_ENV]; exp._setForTest({ reportIntervalMs: 60000 }); });
  const lines = () => warnings.map((w) => LINE.exec(w)).filter(Boolean);
  return { warnings, lines };
}

function emit(provider, n) {
  const tracer = provider.getTracer('trailbeam');
  for (let i = 0; i < n; i++) tracer.startSpan('trailbeam send').end();
}

test('the default queue holds 16,384 spans, or TRAILBEAM_MAX_QUEUE_SIZE', (t) => {
  const { warnings } = setup(t);
  assert.strictEqual(exp.configuredMaxQueueSize(), 16384);
  process.env[exp.QUEUE_SIZE_ENV] = '100';
  assert.strictEqual(exp.configuredMaxQueueSize(), 100);
  process.env[exp.QUEUE_SIZE_ENV] = 'lots';
  assert.strictEqual(exp.configuredMaxQueueSize(), 16384);
  assert.match(warnings[0], /not a positive integer/);
});

test('queue-full drops are counted, warned and metered exactly', async (t) => {
  const { lines } = setup(t, { env: '200' });
  const exporter = new Gate({ blocked: true }); // the server is down: nothing leaves the queue
  const { provider } = makeProvider({ spanExporter: exporter });
  emit(provider, 1000);
  assert.strictEqual(trailbeam.exportStats().queued, 200);
  exporter.release();
  await provider.forceFlush();
  emit(provider, 50); // room again
  await provider.shutdown();

  const real = 1050 - exporter.received;
  const s = trailbeam.exportStats();
  assert.strictEqual(real, 800);
  assert.deepStrictEqual(s, { exported: 250, queued: 0, dropped: 800,
    droppedByReason: { queue_full: 800, export_failed: 0, shutdown: 0 } });
  const l = lines();
  assert.ok(l.length >= 1);
  assert.strictEqual(l.reduce((a, m) => a + Number(m[1]), 0), real, 'the warnings add up to the real count');
  assert.strictEqual(Number(l[l.length - 1][3]), real);
  assert.deepStrictEqual(metricTotal(), { queue_full: real });
});

test('failed exports are drops', async (t) => {
  const { lines } = setup(t);
  const exporter = new Gate();
  exporter.fail = true;
  const { provider } = makeProvider({ spanExporter: exporter });
  emit(provider, 1300);
  await provider.forceFlush().catch(() => {});
  exporter.fail = false;
  emit(provider, 7);
  await provider.shutdown();
  assert.strictEqual(exporter.received, 7);
  assert.deepStrictEqual(trailbeam.exportStats().droppedByReason, { queue_full: 0, export_failed: 1300, shutdown: 0 });
  assert.strictEqual(lines().reduce((a, m) => a + Number(m[1]), 0), 1300);
  assert.deepStrictEqual(metricTotal(), { export_failed: 1300 });
});

test('an export that never calls back is a failed export after the export timeout', async (t) => {
  setup(t);
  const exporter = { export() {}, shutdown: () => Promise.resolve() };
  const proc = new exp.CountingSpanProcessor(exporter, { exportTimeoutMillis: 50 });
  const { BasicTracerProvider } = require('@opentelemetry/sdk-trace-base');
  const provider = new BasicTracerProvider({ spanProcessors: [proc] });
  emit(provider, 3);
  await provider.forceFlush().catch(() => {});
  await new Promise((r) => setTimeout(r, 100));
  const s = trailbeam.exportStats();
  assert.strictEqual(s.droppedByReason.export_failed, 3);
  assert.strictEqual(s.queued, 0, 'the gate is open again');
  await provider.shutdown().catch(() => {});
});

test('spans ended after shutdown are drops', async (t) => {
  setup(t);
  const exporter = new Gate();
  const { provider } = makeProvider({ spanExporter: exporter });
  const tracer = provider.getTracer('trailbeam');
  const late = Array.from({ length: 4 }, () => tracer.startSpan('trailbeam send'));
  emit(provider, 20);
  await provider.shutdown();
  for (const s of late) s.end(); // SQS calls that finished after uninstall: lost
  assert.strictEqual(exporter.received, 20);
  assert.deepStrictEqual(trailbeam.exportStats().droppedByReason, { queue_full: 0, export_failed: 0, shutdown: 4 });
  assert.deepStrictEqual(metricTotal(), { shutdown: 4 });
});

test('one warning per interval, with the count since the last one', async (t) => {
  const { lines } = setup(t, { env: '10', reportIntervalMs: 200 });
  const exporter = new Gate({ blocked: true });
  const { provider } = makeProvider({ spanExporter: exporter });
  emit(provider, 10);
  emit(provider, 1); // first drop: reported at once
  assert.strictEqual(lines().length, 1);
  emit(provider, 499); // within the interval: one more warning, from the timer
  assert.strictEqual(lines().length, 1);
  await new Promise((r) => setTimeout(r, 400));
  assert.strictEqual(lines().length, 2);
  exporter.release();
  await provider.shutdown();
  assert.deepStrictEqual(lines().map((m) => Number(m[1])), [1, 499]);
  assert.strictEqual(trailbeam.exportStats().dropped, 500);
  assert.strictEqual(510 - exporter.received, 500);
});

// QV-157: an unreachable Trailbeam server must not fail the application. The SDK's processor rejects
// forceFlush and shutdown when a batch fails; Trailbeam reports those spans as drops and resolves.
test('flush and shutdown resolve when every export fails', async (t) => {
  const { lines } = setup(t);
  const exporter = new Gate();
  exporter.fail = true;
  let exporterShutdown = 0;
  exporter.shutdown = () => { exporterShutdown += 1; return Promise.resolve(); };
  const { provider } = makeProvider({ spanExporter: exporter });
  emit(provider, 3);
  await provider.forceFlush(); // must not reject
  emit(provider, 2);
  await provider.shutdown(); // must not reject
  assert.strictEqual(exporterShutdown, 1, 'the exporter is shut down even though the final flush failed');
  assert.deepStrictEqual(trailbeam.exportStats().droppedByReason, { queue_full: 0, export_failed: 5, shutdown: 0 });
  assert.strictEqual(lines().reduce((a, m) => a + Number(m[1]), 0), 5);
});
