'use strict';
// Trailbeam wrapper for AWS Lambda handlers invoked by an SQS event source mapping (contract 4.2, 4.4).
//
//     const trailbeam = require('trailbeam');
//     exports.handler = trailbeam.wrapHandler(async (event, context) => { ... });
//
// For each SQS record the wrapper emits one `trailbeam receive` and flushes it before the handler runs,
// so a timeout or out-of-memory kill cannot lose it. After the handler returns or throws it emits
// one `trailbeam outcome` per record. A killed invocation has no outcome; Trailbeam infers it.

const api = require('@opentelemetry/api');

const {
  OUTCOME_FAILED_EXCEPTION,
  OUTCOME_FAILED_INVALID_RESPONSE,
  OUTCOME_FAILED_REPORTED,
  OUTCOME_SUCCESS,
  SPAN_OUTCOME,
  SPAN_RECEIVE,
  intOrUndefined,
  put,
  resolveHasher,
} = require('./contract');
const { Emitter, makeProvider, nowMs, SERVICE_INSTANCE_ID } = require('./emitter');
const order = require('./order');
const capture = require('./capture');

const OPERATION = 'LambdaEvent';
const DEFAULT_FLUSH_TIMEOUT_MS = 2000;

function debug(what, err) {
  api.diag.debug(`trailbeam: ${what} failed`, err);
}

/** `arn:aws:sqs:<region>:<account>:<name>` -> [name, https://sqs.<region>.amazonaws.com/<account>/<name>]. */
function queueFromArn(arn) {
  const parts = typeof arn === 'string' ? arn.split(':') : [];
  if (parts.length < 6 || !parts[5]) return [parts.length ? parts[parts.length - 1] : '', ''];
  const [, , , region, account] = parts;
  const name = parts.slice(5).join(':');
  return [name, `https://sqs.${region}.amazonaws.com/${account}/${name}`];
}

const isObject = (v) => v !== null && typeof v === 'object' && !Array.isArray(v);

/**
 * Outcome per record ID after the handler returned `result` (contract 4.4). Follows Lambda's
 * partial batch response rules: no `batchItemFailures`, null or empty means success; an entry whose
 * `itemIdentifier` is missing, empty or not in the batch fails the whole batch.
 */
function decideOutcomes(recordIds, result, reportBatchItemFailures = true) {
  const ids = new Set(recordIds);
  const failed = new Set();
  if (reportBatchItemFailures && isObject(result)) {
    const items = result.batchItemFailures;
    if (items !== undefined && items !== null) {
      let valid = Array.isArray(items);
      for (const item of valid ? items : []) {
        const ident = isObject(item) ? item.itemIdentifier : undefined;
        if (typeof ident === 'string' && ids.has(ident)) failed.add(ident);
        else valid = false;
      }
      if (!valid) return Object.fromEntries(recordIds.map((i) => [i, OUTCOME_FAILED_INVALID_RESPONSE]));
    }
  }
  return Object.fromEntries(recordIds.map((i) => [i, failed.has(i) ? OUTCOME_FAILED_REPORTED : OUTCOME_SUCCESS]));
}

/** 3.2: records listed in a valid `batchItemFailures` (0 if none); undefined if the list was invalid. */
function reportedFailureCount(outcomes) {
  const values = Object.values(outcomes);
  if (values.includes(OUTCOME_FAILED_INVALID_RESPONSE)) return undefined;
  return values.filter((v) => v === OUTCOME_FAILED_REPORTED).length;
}

/** The SQS records of a Lambda event, or [] if it is not an SQS event. */
function sqsRecords(event) {
  if (!isObject(event) || !Array.isArray(event.Records)) return [];
  return event.Records.filter((r) => isObject(r) && r.eventSource === 'aws:sqs'
    && typeof r.messageId === 'string' && r.messageId);
}

/** Class name of a thrown value, never its message (messages often contain message contents). */
function errorType(err) {
  if (err !== null && typeof err === 'object') return (err.constructor && err.constructor.name) || 'Object';
  return typeof err;
}

/** Emits the wrapper's spans. One per wrapped handler; holds the Trailbeam tracer provider. */
class LambdaRecorder {
  constructor(provider, emitter, { flushTimeoutMs, reportBatchItemFailures, config = new capture.Config() }) {
    this.config = config;
    this.provider = provider;
    this.emitter = emitter;
    this.defaultClientId = emitter.clientId;
    this.flushTimeoutMs = flushTimeoutMs;
    this.reportBatchItemFailures = reportBatchItemFailures;
  }

  /** Flush, but never hold the invocation longer than the flush timeout or fail it. */
  async flush() {
    let timer;
    try {
      await Promise.race([
        this.provider.forceFlush(),
        new Promise((resolve) => { timer = setTimeout(resolve, this.flushTimeoutMs); }),
      ]);
    } catch (e) {
      debug('flush', e);
    } finally {
      clearTimeout(timer);
    }
  }

  static faas(context) {
    const attrs = {};
    put(attrs, 'faas.name', (context && context.functionName) || process.env.AWS_LAMBDA_FUNCTION_NAME);
    put(attrs, 'faas.invocation_id', context && context.awsRequestId);
    return attrs;
  }

  async receives(records, context, parentContext) {
    // One Lambda execution environment = one log stream (contract section 5).
    this.emitter.clientId = process.env.AWS_LAMBDA_LOG_STREAM_NAME || this.defaultClientId;
    const faas = LambdaRecorder.faas(context);
    // Lambda's poller made the ReceiveMessage call, out of sight of the wrapper. All the wrapper
    // knows is that it happened no later than now (contract section 2): the span ends now, and its
    // start is marked as not observed rather than invented. Likewise the call-order tick (schema
    // version 3): one "returned" tick for the moment the event arrived, and no "issued" tick.
    const t = nowMs();
    const returned = order.tick();
    const size = records.length;
    for (const [index, r] of records.entries()) {
      const arn = r.eventSourceARN;
      const [name, url] = queueFromArn(arn);
      const a = isObject(r.attributes) ? r.attributes : {};
      const attrs = { 'messaging.message.id': r.messageId };
      if (typeof r.receiptHandle === 'string' && r.receiptHandle) Object.assign(attrs, this.emitter.hashAttrs(r.receiptHandle));
      put(attrs, 'trailbeam.receive_count', intOrUndefined(a.ApproximateReceiveCount));
      put(attrs, 'trailbeam.sent_timestamp_ms', intOrUndefined(a.SentTimestamp));
      put(attrs, 'trailbeam.first_receive_timestamp_ms', intOrUndefined(a.ApproximateFirstReceiveTimestamp));
      put(attrs, 'trailbeam.aws_trace_header', a.AWSTraceHeader);
      put(attrs, 'trailbeam.dead_letter_queue_source_arn', a.DeadLetterQueueSourceArn);
      put(attrs, 'trailbeam.queue.arn', arn);
      Object.assign(attrs, faas);
      attrs['trailbeam.batch.size'] = size;
      attrs['trailbeam.batch.index'] = index;
      Object.assign(attrs, capture.fifoAttrs(a.MessageGroupId, a.SequenceNumber));
      try {
        Object.assign(attrs, capture.contentAttrs(this.config, name, r.body, r.messageAttributes, r.md5OfBody, true));
      } catch (e) {
        debug('content attributes', e);
        attrs['trailbeam.body.capture_status'] = capture.STATUS_REDACTION_FAILED;
      }
      try { // 3.4 (section 21.3) and 3.5 (section 23): SNS, else EventBridge or S3 sent it; one parse (QV-515)
        Object.assign(attrs, capture.receiveBodyAttrs(this.config, name, r.body, r.messageAttributes, true));
      } catch (e) {
        debug('SNS receive attributes', e);
      }
      this.emitter.emit(SPAN_RECEIVE, OPERATION, name, url, attrs, { parentContext, endTime: t, startObserved: false, order: { returned } });
    }
    await this.flush();
  }

  async outcomes(records, context, outcomes, errType, parentContext, err, threw = false) {
    const faas = LambdaRecorder.faas(context);
    const size = records.length;
    const reported = threw ? undefined : reportedFailureCount(outcomes);
    // The outcome is decided now, after the handler returned or threw: one moment, one tick.
    const t = nowMs();
    const tick = order.tick();
    for (const [index, r] of records.entries()) {
      const [name, url] = queueFromArn(r.eventSourceARN);
      const outcome = outcomes[r.messageId];
      const attrs = { 'messaging.message.id': r.messageId, 'trailbeam.outcome': outcome };
      if (outcome === OUTCOME_FAILED_EXCEPTION) {
        put(attrs, 'trailbeam.error_type', errType);
        if (threw) put(attrs, 'trailbeam.error_message', capture.errorMessage(this.config, name, err));
      }
      Object.assign(attrs, faas);
      attrs['trailbeam.batch.size'] = size;
      attrs['trailbeam.batch.index'] = index;
      if (reported !== undefined) attrs['trailbeam.batch.reported_failure_count'] = reported;
      this.emitter.emit(SPAN_OUTCOME, OPERATION, name, url, attrs, { parentContext, startTime: t, endTime: t, order: { issued: tick, returned: tick } });
    }
    await this.flush();
  }

  async invoke(handler, thisArg, event, context, rest) {
    const records = sqsRecords(event);
    if (!records.length) return callHandler(handler, thisArg, event, context, rest);
    const parentContext = api.context.active();
    try {
      await this.receives(records, context, parentContext);
    } catch (e) {
      debug('receive spans', e);
    }
    const ids = records.map((r) => r.messageId);
    let result;
    try {
      result = await callHandler(handler, thisArg, event, context, rest);
    } catch (err) {
      await this.safeOutcomes(records, context, Object.fromEntries(ids.map((i) => [i, OUTCOME_FAILED_EXCEPTION])), errorType(err), parentContext, err, true);
      throw err;
    }
    await this.safeOutcomes(records, context, decideOutcomes(ids, result, this.reportBatchItemFailures), undefined, parentContext);
    return result;
  }

  async safeOutcomes(...args) {
    try {
      await this.outcomes(...args);
    } catch (e) {
      debug('outcome spans', e);
    }
  }
}

/**
 * Call an async (or sync) handler, or a callback-style `(event, context, callback)` handler, and
 * return a promise of its result.
 */
function callHandler(handler, thisArg, event, context, rest) {
  if (handler.length >= 3 && rest.length === 0) {
    return new Promise((resolve, reject) => {
      const callback = (err, value) => (err !== undefined && err !== null ? reject(err) : resolve(value));
      try {
        const ret = handler.call(thisArg, event, context, callback);
        if (ret && typeof ret.then === 'function') ret.then(resolve, reject);
      } catch (e) {
        reject(e);
      }
    });
  }
  try {
    return Promise.resolve(handler.call(thisArg, event, context, ...rest));
  } catch (e) {
    return Promise.reject(e);
  }
}

/**
 * Wrap a Lambda handler. Returns an async handler `(event, context) => Promise`.
 *
 * `reportBatchItemFailures` should match the event source mapping's `FunctionResponseTypes`: when
 * it is off, Lambda ignores `batchItemFailures`, and so does Trailbeam. Events without SQS records are
 * passed to the handler untouched. The wrapped handler has a `trailbeamTracerProvider` property.
 *
 * @param {Function} handler
 * @param {object} [options]
 * @param {string} [options.receiptHashKey] takes precedence over $TRAILBEAM_RECEIPT_HASH_KEY
 * @param {import('@opentelemetry/sdk-trace-base').SpanExporter} [options.spanExporter]
 * @param {import('@opentelemetry/sdk-trace-base').SpanProcessor} [options.spanProcessor]
 * @param {number} [options.flushTimeoutMs=2000] longest wait for each flush
 * @param {boolean} [options.reportBatchItemFailures=true]
 * @param {object} [options.tenant] as in `install` (contract section 15)
 * @param {object} [options.capture] as in `install`; with the record's queue in `errorQueues`, a thrown
 *   error's message is recorded, redacted
 */
function wrapHandler(handler, options = {}) {
  if (typeof handler !== 'function') throw new TypeError('trailbeam.wrapHandler expects a function');
  const { provider, resource } = makeProvider(options);
  const emitter = new Emitter(provider, resolveHasher(options.receiptHashKey), 'lambda', resource.attributes[SERVICE_INSTANCE_ID]);
  const recorder = new LambdaRecorder(provider, emitter, {
    flushTimeoutMs: options.flushTimeoutMs !== undefined ? options.flushTimeoutMs : DEFAULT_FLUSH_TIMEOUT_MS,
    reportBatchItemFailures: options.reportBatchItemFailures !== undefined ? options.reportBatchItemFailures : true,
    config: capture.resolveConfig(options.tenant, options.capture, options.sns),
  });
  // Two declared parameters, so the Lambda runtime treats the wrapper as an async handler.
  const wrapped = async function trailbeamWrappedHandler(event, context, ...rest) {
    return recorder.invoke(handler, this, event, context, rest);
  };
  Object.defineProperty(wrapped, 'name', { value: handler.name || 'trailbeamWrappedHandler' });
  wrapped.trailbeamTracerProvider = provider;
  return wrapped;
}

module.exports = { wrapHandler, decideOutcomes, reportedFailureCount, queueFromArn, sqsRecords, errorType, OPERATION };
