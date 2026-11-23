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
// Shutdown (uninstall()) and the flush when the event loop empties wait at most
// $TRAILBEAM_SHUTDOWN_TIMEOUT_MS (default 2,000 ms), so an unreachable or slow Trailbeam server cannot hold
// the process open for the OTLP exporter's retries and 10 s request timeout (QV-169). The process stays
// alive while the exporter has a request or a retry timer pending, and the exporter has no way to cancel
// them, so before those last exports `shortenExports` lowers the timeout the exporter gives each export
// (its delegate's `_timeout`: a private field of @opentelemetry/otlp-exporter-base, which the ^0.223 range
// pins to patch releases; if it is not there, nothing changes). What is still queued when shutdown gives
// up is a `shutdown` drop.
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
const SHUTDOWN_TIMEOUT_ENV = 'TRAILBEAM_SHUTDOWN_TIMEOUT_MS';
const DEFAULT_SHUTDOWN_TIMEOUT_MS = 2000;
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
let testWarn = null; // set by _setForTest

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

/** $TRAILBEAM_SHUTDOWN_TIMEOUT_MS if it is a non-negative integer, else the default. */
function configuredShutdownTimeoutMs() {
  const raw = (process.env[SHUTDOWN_TIMEOUT_ENV] || '').trim();
  if (!raw) return DEFAULT_SHUTDOWN_TIMEOUT_MS;
  const n = Number(raw);
  if (Number.isInteger(n) && n >= 0) return n;
  warn(`trailbeam: ignoring ${SHUTDOWN_TIMEOUT_ENV}=${JSON.stringify(raw)}, which is not a non-negative integer`);
  return DEFAULT_SHUTDOWN_TIMEOUT_MS;
}

/** What to do about the drops in `reasons`, one sentence per reason that has any. */
function advice(reasons, limit) {
  const out = [];
  if (reasons.export_failed) {
    out.push('Export failed: the Trailbeam server was unreachable, too slow, or refused the spans (a 401 is a ' +
      'missing or wrong token in OTEL_EXPORTER_OTLP_HEADERS); set a diag logger to see the exporter\'s error');
  }
  if (reasons.queue_full) {
    out.push(`The export queue holds ${limit} spans (${QUEUE_SIZE_ENV}). For high volume, export through an ` +
      `OpenTelemetry Collector with a persistent queue: ${DOCS}`);
  }
  if (reasons.shutdown) {
    out.push(`Shutdown waits at most ${configuredShutdownTimeoutMs()} ms for the queue to drain ` +
      `(${SHUTDOWN_TIMEOUT_ENV}); if the Trailbeam server is unreachable or slow, the export does not finish ` +
      'in that time');
  }
  return out.join('. ');
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

// `out`: where the line goes; at process exit it must be written at once, which emitWarning does not do.
function report(limit, final = false, out = warn) {
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
  out(`trailbeam: dropped ${n} spans${since} (${parts}); ${total} dropped since start. These messages will ` +
    `have gaps in their Trailbeam timelines. ${advice(u, limit)}` +
    (final ? '' : `. The next count follows in ${Math.round(config.reportIntervalMs / 1000)} s if spans are still dropped.`));
}

// `final`: the caller reports them with report(limit, true) straight after.
function addDropped(reason, n, limit, final = false) {
  if (n <= 0) return;
  totals.dropped[reason] += n;
  totals.unreported[reason] += n;
  metric(reason, n);
  const now = Date.now();
  if (final) return;
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

/** At process exit: write the drops not reported yet, if any, to stderr at once. */
function reportPending() {
  let limit = DEFAULT_MAX_QUEUE_SIZE;
  for (const p of processors) limit = p.limit;
  report(limit, true, testWarn || ((m) => process.stderr.write(`${m}\n`)));
}

/** Before the last exports of the process: no export of any processor waits longer than its shutdown timeout. */
function shortenExports() {
  for (const p of processors) p._shortenExports();
}

/** A BatchSpanProcessor with a bounded queue whose every lost span is counted (see the top of this file). */
class CountingSpanProcessor {
  constructor(exporter, options = {}) {
    this.limit = options.maxQueueSize || configuredMaxQueueSize();
    this.queued = 0;
    this._shutdown = false;
    this._exporter = exporter;
    this.shutdownTimeoutMillis = options.shutdownTimeoutMillis !== undefined
      ? options.shutdownTimeoutMillis : configuredShutdownTimeoutMs();
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

  // The SDK's processor rejects forceFlush and shutdown when a batch fails to export (an unreachable
  // endpoint: ECONNREFUSED). Those spans are already counted as export_failed drops and warned about, so
  // the rejection is not passed on: an unreachable Trailbeam server must not fail the application (QV-157).
  async forceFlush() {
    try {
      await this.batch.forceFlush();
    } catch (e) {
      api.diag.debug('trailbeam: flush failed', e);
    }
  }

  // An OTLP exporter's own export timeout (and so its retries) is cut to the shutdown timeout, for the
  // exports that start from now on. See the top of this file.
  _shortenExports() {
    const delegate = this._exporter && this._exporter._delegate;
    if (delegate && typeof delegate._timeout === 'number') {
      delegate._timeout = Math.min(delegate._timeout, Math.max(this.shutdownTimeoutMillis, 1));
    }
  }

  // Waits at most shutdownTimeoutMillis; then counts what is left as shutdown drops. Never rejects.
  async shutdown() {
    if (this._shutdown) return;
    this._shutdown = true;
    this._shortenExports();
    let timer;
    const timedOut = new Promise((resolve) => {
      timer = setTimeout(() => resolve(true), this.shutdownTimeoutMillis);
      if (typeof timer.unref === 'function') timer.unref();
    });
    try {
      const done = this.batch.shutdown().then(() => false, async (e) => {
        api.diag.debug('trailbeam: shutdown failed', e); // see forceFlush
        // The SDK skips the exporter's shutdown when the final flush fails; release the exporter anyway.
        try {
          await this._exporter.shutdown();
        } catch (e2) {
          api.diag.debug('trailbeam: exporter shutdown failed', e2);
        }
        return false;
      });
      if (await Promise.race([done, timedOut])) {
        api.diag.debug(`trailbeam: shutdown gave up after ${this.shutdownTimeoutMillis} ms`);
      }
    } finally {
      clearTimeout(timer);
      this._final = true;
      const left = this.queued;
      this.queued = 0;
      processors.delete(this);
      addDropped('shutdown', left, this.limit, true);
      report(this.limit, true);
    }
  }
}

/** For tests: replace the warning function (null keeps it), set the report interval, zero the counts. */
function _setForTest({ warnFn, reportIntervalMs } = {}) {
  if (warnFn) { warn = warnFn; testWarn = warnFn; }
  if (reportIntervalMs !== undefined) config.reportIntervalMs = reportIntervalMs;
  reset();
}

module.exports = {
  CountingSpanProcessor, stats, configuredMaxQueueSize, configuredShutdownTimeoutMs, reportPending,
  shortenExports, _setForTest, QUEUE_SIZE_ENV, DEFAULT_MAX_QUEUE_SIZE, SHUTDOWN_TIMEOUT_ENV,
  DEFAULT_SHUTDOWN_TIMEOUT_MS, METRIC_NAME, WARNING_CODE,
};
