'use strict';
// The export path of Trailbeam spans: a bounded batch queue that counts every span it loses (QV-75).
//
// The SDK's BatchSpanProcessor drops spans when its queue is full and reports it only through
// `api.diag`, which is off unless the application sets a diag logger. CountingSpanProcessor puts a gate in
// front of it and wraps its exporter, so every span Trailbeam accepts ends up exported, still queued, or
// dropped for one of three reasons:
//
// - queue_full: the queue already held `maxQueueSize` spans that were not exported yet;
// - export_failed: the export reported a failure, threw, or had not finished after the export timeout
//   (the SDK gives up on it then and does not retry a failed batch);
// - shutdown: ended after shutdown, or still queued when shutdown finished.
//
// The gate counts a span as queued from onEnd until its export finishes, which is never less than what
// the SDK's buffer holds, and the SDK's buffer has the same capacity, so the SDK itself never drops on a
// full queue. Only public SDK APIs are used.
//
// Drops are counted process-wide (`stats()`), added to the `trailbeam.sdk.spans.dropped` counter of the
// global OpenTelemetry meter provider, and reported with process.emitWarning (code
// TRAILBEAM_SPANS_DROPPED): at the first drop, then at most once per REPORT_INTERVAL_MS with the count since
// the last warning, and at shutdown.

const api = require('@opentelemetry/api');
const { BatchSpanProcessor } = require('@opentelemetry/sdk-trace-base');

const { version } = require('./version');
const { SCOPE_NAME } = require('./contract');

const QUEUE_SIZE_ENV = 'TRAILBEAM_MAX_QUEUE_SIZE';
// The SDK default is 2,048: two seconds at the 1,000 spans/s small-tier target. 16,384 holds about 16 s
// of that rate, at about 3.3 KB per span without bodies (QV-75 measurement), so about 54 MB when full.
const DEFAULT_MAX_QUEUE_SIZE = 16384;
const DEFAULT_MAX_EXPORT_BATCH_SIZE = 512; // the SDK default; OTEL_BSP_MAX_EXPORT_BATCH_SIZE still applies
const DEFAULT_EXPORT_TIMEOUT_MS = 30000; // the SDK default; OTEL_BSP_EXPORT_TIMEOUT still applies
// ExportResultCode of @opentelemetry/core, which is not a direct dependency.
const ExportResultCode = { SUCCESS: 0, FAILED: 1 };
const REASONS = ['queue_full', 'export_failed', 'shutdown'];
const METRIC_NAME = 'trailbeam.sdk.spans.dropped';
const WARNING_CODE = 'TRAILBEAM_SPANS_DROPPED';
const DOCS = '"High volume" in the Trailbeam collector/README.md';

const config = { reportIntervalMs: 60000 };
let warn = (msg) => process.emitWarning(msg, { code: WARNING_CODE });

const zero = () => Object.fromEntries(REASONS.map((r) => [r, 0]));
let totals;
function reset() {
  if (totals && totals.timer) clearTimeout(totals.timer);
  totals = { exported: 0, dropped: zero(), unreported: zero(), lastReport: undefined, timer: undefined };
}
reset();
const processors = new Set();
let meter = { provider: undefined, counter: undefined };

function envInt(name, fallback, onBad) {
  const raw = (process.env[name] || '').trim();
  if (!raw) return fallback;
  const n = Number(raw);
  if (Number.isInteger(n) && n > 0) return n;
  if (onBad) onBad(raw);
  return fallback;
}

/** $TRAILBEAM_MAX_QUEUE_SIZE if it is a positive integer, else the default. */
function configuredMaxQueueSize() {
  return envInt(QUEUE_SIZE_ENV, DEFAULT_MAX_QUEUE_SIZE,
    (raw) => warn(`trailbeam: ignoring ${QUEUE_SIZE_ENV}=${JSON.stringify(raw)}, which is not a positive integer`));
}

function metric(reason, n) {
  try {
    const provider = api.metrics.getMeterProvider();
    if (meter.provider !== provider) { // the global provider may be set after the first drop
      meter = {
        provider,
        counter: provider.getMeter(SCOPE_NAME, version).createCounter(METRIC_NAME, {
          unit: '{span}', description: 'Trailbeam spans this process dropped before export',
        }),
      };
    }
    meter.counter.add(n, { 'error.type': reason });
  } catch (e) {
    api.diag.debug('trailbeam: drop metric failed', e); // must not lose the span count or the warning
  }
}

function report(limit, final = false) {
  if (totals.timer) clearTimeout(totals.timer);
  totals.timer = undefined;
  const u = totals.unreported;
  const n = REASONS.reduce((a, r) => a + u[r], 0);
  if (n === 0) return;
  const since = totals.lastReport === undefined ? '' : ` in the last ${Math.round((Date.now() - totals.lastReport) / 1000)} s`;
  totals.unreported = zero();
  totals.lastReport = Date.now();
  const total = REASONS.reduce((a, r) => a + totals.dropped[r], 0);
  const parts = REASONS.filter((r) => u[r]).map((r) => `${u[r]} ${r.replace('_', ' ')}`).join(', ');
  warn(`trailbeam: dropped ${n} spans${since} (${parts}); ${total} dropped since start. These messages will ` +
    `have gaps in their Trailbeam timelines. The export queue holds ${limit} spans (${QUEUE_SIZE_ENV}). For high ` +
    `volume, export through an OpenTelemetry Collector with a persistent queue: ${DOCS}` +
    (final ? '' : `. The next count follows in ${Math.round(config.reportIntervalMs / 1000)} s if spans are still dropped.`));
}

function addDropped(reason, n, limit) {
  if (n <= 0) return;
  totals.dropped[reason] += n;
  totals.unreported[reason] += n;
  metric(reason, n);
  const now = Date.now();
  if (totals.lastReport === undefined || now - totals.lastReport >= config.reportIntervalMs) {
    report(limit);
  } else if (!totals.timer) {
    totals.timer = setTimeout(() => report(limit), config.reportIntervalMs - (now - totals.lastReport));
    if (typeof totals.timer.unref === 'function') totals.timer.unref();
  }
}

/**
 * Trailbeam spans of this process: `exported`, `queued` (accepted, export not finished yet), `dropped`
 * and `droppedByReason` (`queue_full`, `export_failed`, `shutdown`).
 */
function stats() {
  let queued = 0;
  for (const p of processors) queued += p.queued;
  const droppedByReason = { ...totals.dropped };
  return { exported: totals.exported, queued, dropped: REASONS.reduce((a, r) => a + droppedByReason[r], 0), droppedByReason };
}

/** A BatchSpanProcessor with a bounded queue whose every lost span is counted (see the top of this file). */
class CountingSpanProcessor {
  constructor(exporter, options = {}) {
    this.limit = options.maxQueueSize || configuredMaxQueueSize();
    this.queued = 0;
    this._shutdown = false;
    const exportTimeoutMillis = options.exportTimeoutMillis
      || envInt('OTEL_BSP_EXPORT_TIMEOUT', DEFAULT_EXPORT_TIMEOUT_MS);
    const maxExportBatchSize = Math.min(options.maxExportBatchSize
      || envInt('OTEL_BSP_MAX_EXPORT_BATCH_SIZE', DEFAULT_MAX_EXPORT_BATCH_SIZE), this.limit);
    const self = this;
    const counting = {
      export(spans, resultCallback) {
        let done = false;
        const finish = (result) => {
          if (done) return; // a late callback after the timeout below: already counted as failed
          done = true;
          clearTimeout(timer);
          self._exported(spans.length, result && result.code === ExportResultCode.SUCCESS);
          resultCallback(result);
        };
        const timer = setTimeout(() => finish({ code: ExportResultCode.FAILED, error: new Error('Timeout') }),
          exportTimeoutMillis);
        if (typeof timer.unref === 'function') timer.unref();
        try {
          exporter.export(spans, finish);
        } catch (error) {
          finish({ code: ExportResultCode.FAILED, error });
        }
      },
      shutdown: () => exporter.shutdown(),
      forceFlush: () => (exporter.forceFlush ? exporter.forceFlush() : Promise.resolve()),
    };
    this.batch = new BatchSpanProcessor(counting, {
      scheduledDelayMillis: options.scheduledDelayMillis,
      exportTimeoutMillis,
      maxExportBatchSize,
      maxQueueSize: this.limit,
    });
    processors.add(this);
  }

  onStart(span, parentContext) { this.batch.onStart(span, parentContext); }

  onEnd(span) {
    if ((span.spanContext().traceFlags & api.TraceFlags.SAMPLED) === 0) return; // as the SDK's processor
    if (this._shutdown) { addDropped('shutdown', 1, this.limit); return; }
    if (this.queued >= this.limit) { addDropped('queue_full', 1, this.limit); return; }
    this.queued += 1;
    this.batch.onEnd(span);
  }

  _exported(n, ok) {
    if (this._final) return; // finished after shutdown gave up on it: counted as a shutdown drop already
    this.queued -= n;
    if (ok) totals.exported += n;
    else addDropped('export_failed', n, this.limit);
  }

  forceFlush() { return this.batch.forceFlush(); }

  async shutdown() {
    if (this._shutdown) return;
    this._shutdown = true;
    try {
      await this.batch.shutdown();
    } finally {
      this._final = true;
      const left = this.queued;
      this.queued = 0;
      processors.delete(this);
      addDropped('shutdown', left, this.limit);
      report(this.limit, true);
    }
  }
}

/** For tests: replace the warning function (null keeps it), set the report interval, zero the counts. */
function _setForTest({ warnFn, reportIntervalMs } = {}) {
  if (warnFn) warn = warnFn;
  if (reportIntervalMs !== undefined) config.reportIntervalMs = reportIntervalMs;
  reset();
}

module.exports = {
  CountingSpanProcessor, stats, configuredMaxQueueSize, _setForTest,
  QUEUE_SIZE_ENV, DEFAULT_MAX_QUEUE_SIZE, METRIC_NAME, WARNING_CODE,
};
