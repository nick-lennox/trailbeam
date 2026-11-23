'use strict';
// Trailbeam hooks for AWS SDK v3 SQS clients (contract sections 4.1 to 4.3).
//
// The hooks are a middleware on each SQSClient's middleware stack. When the application uses
// @opentelemetry/instrumentation-aws-sdk, that instrumentation runs the whole stack inside the
// context of its span for the call, so each Trailbeam span is a child of the stock span and shares its
// trace ID. Without it, Trailbeam spans are children of whatever span is active, or root spans.
//
//     const trailbeam = require('trailbeam');
//     const sqs = trailbeam.instrument(new SQSClient({}));   // installs with defaults on first use

const api = require('@opentelemetry/api');

const {
  SPAN_CHANGE_VISIBILITY,
  SPAN_DELETE,
  SPAN_RECEIVE,
  SPAN_SEND,
  intOrUndefined,
  put,
  queueNameFromUrl,
  resolveHasher,
} = require('./contract');
const { Emitter, makeProvider, nowMs, SERVICE_INSTANCE_ID } = require('./emitter');
const { HandleMap } = require('./handles');
const order = require('./order');
const capture = require('./capture');
const { stats: exportStats } = require('./export');

const MIDDLEWARE_NAME = 'trailbeamSqsMiddleware';

// System attributes the receive hook asks for (section 4.2).
const RECEIVE_SYSTEM_ATTRIBUTES = Object.freeze([
  'ApproximateReceiveCount',
  'SentTimestamp',
  'ApproximateFirstReceiveTimestamp',
  'AWSTraceHeader',
  'DeadLetterQueueSourceArn', // schema 3.1: SQS sets it only on a receive from a dead-letter queue
  'MessageGroupId', // 3.2: FIFO queues (and fair standard queues)
  'SequenceNumber', // 3.2: FIFO queues only
]);

// operation -> [span name, is batch]
const SETTLE_OPS = {
  DeleteMessage: [SPAN_DELETE, false],
  DeleteMessageBatch: [SPAN_DELETE, true],
  ChangeMessageVisibility: [SPAN_CHANGE_VISIBILITY, false],
  ChangeMessageVisibilityBatch: [SPAN_CHANGE_VISIBILITY, true],
};
const OPS = new Set(['SendMessage', 'SendMessageBatch', 'ReceiveMessage', ...Object.keys(SETTLE_OPS)]);

function debug(what, err) {
  api.diag.debug(`trailbeam: ${what} failed`, err);
}

/**
 * A copy of a ReceiveMessage input that asks for the system attributes Trailbeam needs, keeping
 * everything the caller asked for; or `undefined` if nothing needs adding. The caller's object is
 * never modified.
 */
function addReceiveAttributeNames(input) {
  const cur = Array.isArray(input.MessageSystemAttributeNames) ? input.MessageSystemAttributeNames : [];
  const legacy = Array.isArray(input.AttributeNames) ? input.AttributeNames : [];
  const asked = new Set([...cur, ...legacy]);
  if (asked.has('All')) return undefined;
  const missing = RECEIVE_SYSTEM_ATTRIBUTES.filter((n) => !asked.has(n));
  if (!missing.length) return undefined;
  // A caller using only the deprecated AttributeNames keeps using it; otherwise use the current name.
  if (legacy.length && !cur.length) return { ...input, AttributeNames: [...legacy, ...missing] };
  return { ...input, MessageSystemAttributeNames: [...cur, ...missing] };
}

/**
 * 3.2: a copy of a ReceiveMessage input that also asks for one message attribute (the configured
 * tenant attribute, or an Extended Client size attribute); or `undefined` if nothing needs adding. The caller's object is never modified.
 */
function addMessageAttributeName(input, name) {
  if (!name) return undefined;
  const asked = Array.isArray(input.MessageAttributeNames) ? input.MessageAttributeNames : [];
  if (asked.includes(name) || asked.includes('All') || asked.includes('.*')) return undefined;
  return { ...input, MessageAttributeNames: [...asked, name] };
}

/**
 * An SQS error code without a protocol namespace: some SQS implementations (ElasticMQ 2.x, for
 * one) send `com.amazonaws.sqs#ReceiptHandleIsInvalid` where SQS sends `ReceiptHandleIsInvalid`.
 */
function plainCode(code) {
  if (typeof code !== 'string' || !code) return undefined;
  return code.slice(code.lastIndexOf('#') + 1) || undefined;
}

/** The SQS error code of a thrown error, or the error's name if it has no code. */
function errorCode(err) {
  if (err && typeof err === 'object') {
    return plainCode(err.Code) || plainCode(err.code) || plainCode(err.name) || err.constructor?.name || 'Error';
  }
  return 'Error';
}

/**
 * `{startTime, endTime, order}` for one call's spans; the end defaults to now, the start to the end.
 * `issued` / `returned` are the call-order ticks (schema version 3); absent ones are omitted.
 */
function spanTimes({ startTime, endTime, issued, returned } = {}) {
  const end = endTime !== undefined ? endTime : nowMs();
  const t = { startTime: startTime !== undefined ? startTime : end, endTime: end };
  if (issued !== undefined || returned !== undefined) t.order = { issued, returned };
  return t;
}

/**
 * Turns SQS calls into Trailbeam spans. Hook methods never throw. `times` is `{startTime, endTime}` of the
 * SQS call in epoch ms (contract section 2), plus its call-order ticks `{issued, returned}` (schema
 * version 3); every span of one call, batch entries included, gets them.
 */
class SqsHooks {
  constructor(emitter, handles = new HandleMap(), config = new capture.Config()) {
    this.emitter = emitter;
    this.handles = handles;
    this.config = config;
  }

  contents(queueName, body, attributes, md5) {
    try {
      return capture.contentAttrs(this.config, queueName, body, attributes, md5);
    } catch (e) {
      debug('content attributes', e); // a capture bug must not lose the span
      return { 'trailbeam.body.capture_status': capture.STATUS_REDACTION_FAILED };
    }
  }

  /** 3.2 attributes of one sent message: the body from the request, checked against the response's MD5. */
  sent(queueName, request, response) {
    return {
      ...capture.fifoAttrs(request.MessageGroupId, response.SequenceNumber),
      ...this.contents(queueName, request.MessageBody, request.MessageAttributes, response.MD5OfMessageBody),
    };
  }

  onSend(operation, input, output, parentContext, times = {}) {
    const url = input.QueueUrl;
    const name = queueNameFromUrl(url);
    if (!output) return;
    const t = spanTimes(times);
    if (operation === 'SendMessage') {
      if (output.MessageId) {
        const attrs = { 'messaging.message.id': output.MessageId, ...this.sent(name, input, output) };
        this.emitter.emit(SPAN_SEND, operation, name, url, attrs, { parentContext, ...t });
      }
      return;
    }
    const requests = new Map((Array.isArray(input.Entries) ? input.Entries : []).filter(Boolean).map((e) => [e.Id, e]));
    for (const entry of output.Successful || []) {
      if (!entry || !entry.MessageId) continue;
      const attrs = { 'messaging.message.id': entry.MessageId };
      put(attrs, 'trailbeam.batch_entry_id', entry.Id);
      // The body comes from the request entry with the same Id.
      if (requests.has(entry.Id)) Object.assign(attrs, this.sent(name, requests.get(entry.Id), entry));
      this.emitter.emit(SPAN_SEND, operation, name, url, attrs, { parentContext, ...t });
    }
  }

  onReceive(input, output, parentContext, times = {}) {
    const url = input.QueueUrl;
    const name = queueNameFromUrl(url);
    const t = spanTimes(times);
    for (const m of (output && output.Messages) || []) {
      const rh = m && m.ReceiptHandle;
      const mid = m && m.MessageId;
      if (!rh || !mid) continue;
      this.handles.put(rh, mid);
      const a = m.Attributes || {};
      const attrs = { 'messaging.message.id': mid, ...this.emitter.hashAttrs(rh) };
      put(attrs, 'trailbeam.receive_count', intOrUndefined(a.ApproximateReceiveCount));
      put(attrs, 'trailbeam.sent_timestamp_ms', intOrUndefined(a.SentTimestamp));
      put(attrs, 'trailbeam.first_receive_timestamp_ms', intOrUndefined(a.ApproximateFirstReceiveTimestamp));
      put(attrs, 'trailbeam.aws_trace_header', a.AWSTraceHeader);
      put(attrs, 'trailbeam.dead_letter_queue_source_arn', a.DeadLetterQueueSourceArn);
      Object.assign(attrs, capture.fifoAttrs(a.MessageGroupId, a.SequenceNumber));
      Object.assign(attrs, this.contents(name, m.Body, m.MessageAttributes, m.MD5OfBody));
      this.emitter.emit(SPAN_RECEIVE, 'ReceiveMessage', name, url, attrs, { parentContext, ...t });
    }
  }

  /** `output` is the response, or undefined if the call threw `err`. */
  onSettle(operation, input, output, err, parentContext, times = {}) {
    const [spanName, batch] = SETTLE_OPS[operation];
    const url = input.QueueUrl;
    const name = queueNameFromUrl(url);
    const callError = output === undefined ? errorCode(err) : undefined;

    const entries = batch
      ? (Array.isArray(input.Entries) ? input.Entries : []).map((e) => [e && e.Id, e && e.ReceiptHandle, e && e.VisibilityTimeout])
      : [[undefined, input.ReceiptHandle, input.VisibilityTimeout]];

    let okIds = new Set();
    const failed = new Map();
    if (callError === undefined && batch) {
      okIds = new Set((output.Successful || []).map((e) => e && e.Id));
      for (const e of output.Failed || []) if (e) failed.set(e.Id, plainCode(e.Code) || 'Failed');
    }

    const t = spanTimes(times);
    for (const [entryId, rh, vt] of entries) {
      if (typeof rh !== 'string' || !rh) continue;
      let error;
      if (callError !== undefined) error = callError;
      else if (batch && !okIds.has(entryId)) error = failed.get(entryId) || 'MissingFromResponse';
      const attrs = this.emitter.hashAttrs(rh);
      put(attrs, 'messaging.message.id', this.handles.get(rh));
      if (batch) put(attrs, 'trailbeam.batch_entry_id', entryId);
      if (spanName === SPAN_CHANGE_VISIBILITY) put(attrs, 'trailbeam.visibility_timeout', intOrUndefined(vt));
      attrs['trailbeam.result'] = error === undefined ? 'ok' : 'failed';
      put(attrs, 'trailbeam.error_code', error);
      this.emitter.emit(spanName, operation, name, url, attrs, { parentContext, ...t });
      if (error === undefined && spanName === SPAN_DELETE) this.handles.discard(rh);
    }
  }
}

/** The AWS SDK v3 middleware. Looks up the hooks on every call, so `uninstall` turns it off. */
function sqsMiddleware(getHooks) {
  return (next, context) => async (args) => {
    const hooks = getHooks();
    const cmd = context && context.commandName;
    const operation = typeof cmd === 'string' ? cmd.replace(/Command$/, '') : undefined;
    if (!hooks || !OPS.has(operation) || !args || !args.input) return next(args);

    const input = args.input;
    if (operation === 'ReceiveMessage') {
      try {
        let changed = addReceiveAttributeNames(input) || input;
        changed = addMessageAttributeName(changed, hooks.config && hooks.config.tenantAttribute) || changed;
        for (const name of capture.OFFLOADED_SIZE_ATTRIBUTES) changed = addMessageAttributeName(changed, name) || changed;
        if (changed !== input) args = { ...args, input: changed };
      } catch (e) {
        debug('receive request hook', e);
      }
    }
    const parentContext = api.context.active();
    // Contract section 2: the span starts just before the request is sent. This middleware is in the
    // initialize step, ahead of serialization, signing and retries, so the start is a lower bound.
    // The call-order tick (schema version 3) is taken immediately before it.
    const issued = order.tick();
    const startTime = nowMs();
    let result;
    try {
      result = await next(args);
    } catch (err) {
      const times = { startTime, endTime: nowMs(), issued, returned: order.tick() };
      if (SETTLE_OPS[operation]) {
        try { hooks.onSettle(operation, input, undefined, err, parentContext, times); } catch (e) { debug('settle hook', e); }
      }
      throw err;
    }
    const times = { startTime, endTime: nowMs(), issued, returned: order.tick() };
    try {
      const output = result && result.output;
      if (operation === 'ReceiveMessage') hooks.onReceive(input, output, parentContext, times);
      else if (operation === 'SendMessage' || operation === 'SendMessageBatch') hooks.onSend(operation, input, output, parentContext, times);
      else hooks.onSettle(operation, input, output || {}, undefined, parentContext, times);
    } catch (e) {
      debug(`${operation} hook`, e);
    }
    return result;
  };
}

// -- install / instrument / uninstall ---------------------------------------------------------

let state = null;

/**
 * Create the Trailbeam client hooks for this process: a tracer provider of our own, the receipt-hash key
 * and the handle map. Returns the Trailbeam tracer provider. Calling it again replaces the previous
 * installation (and shuts its provider down).
 *
 * @param {object} [options]
 * @param {string} [options.receiptHashKey] takes precedence over $TRAILBEAM_RECEIPT_HASH_KEY
 * @param {import('@opentelemetry/sdk-trace-base').SpanExporter} [options.spanExporter]
 * @param {import('@opentelemetry/sdk-trace-base').SpanProcessor} [options.spanProcessor]
 * @param {{messageAttribute?: string, bodyPath?: string}} [options.tenant] contract section 15
 * @param {{bodyQueues?: string[], errorQueues?: string[], redactKeys?: string[], redactPatterns?: string[], maxBodyBytes?: number}} [options.capture]
 *   contract section 15; body capture is off unless a queue is listed in `bodyQueues`
 */
function install(options = {}) {
  const config = capture.resolveConfig(options.tenant, options.capture);
  const previous = state;
  const { provider, resource } = makeProvider(options);
  const clientId = resource.attributes[SERVICE_INSTANCE_ID];
  const emitter = new Emitter(provider, resolveHasher(options.receiptHashKey), 'client', clientId);
  state = { provider, hooks: new SqsHooks(emitter, new HandleMap(), config) };
  addExitHooks();
  if (previous) previous.provider.shutdown().catch((e) => debug('provider shutdown', e));
  return provider;
}

// A script that sends a message and ends must not lose its spans (QV-156). The batch processor's timer
// does not keep the process alive, so when the event loop empties ('beforeExit') we flush. A flush with
// nothing queued schedules no work, so Node.js does not fire 'beforeExit' again and the process exits.
// process.exit() and signals skip 'beforeExit': at 'exit' we can only say what was lost, in one line.
let exitHooksAdded = false;
function addExitHooks() {
  if (exitHooksAdded) return;
  exitHooksAdded = true;
  process.on('beforeExit', () => {
    if (state) state.provider.forceFlush().catch((e) => debug('flush at exit', e));
  });
  process.on('exit', () => {
    const { queued } = exportStats();
    if (queued === 0) return;
    process.stderr.write(`trailbeam: ${queued} span${queued === 1 ? ' was' : 's were'} not exported because the ` +
      'process exited; call `await trailbeam.uninstall()` before process.exit()\n');
  });
}

/**
 * Add the Trailbeam hooks to an AWS SDK v3 SQSClient. Installs with default options first if `install`
 * has not been called. Safe to call more than once on the same client. Returns the client.
 */
function instrument(client) {
  if (!client || !client.middlewareStack || typeof client.middlewareStack.add !== 'function') {
    throw new TypeError('trailbeam.instrument expects an AWS SDK v3 SQSClient');
  }
  if (!state) install();
  client.middlewareStack.add(sqsMiddleware(() => state && state.hooks), {
    step: 'initialize',
    priority: 'high',
    name: MIDDLEWARE_NAME,
    override: true,
  });
  return client;
}

/**
 * Turn the hooks off on every instrumented client, then flush and shut down the Trailbeam provider.
 * Never rejects: spans that could not be exported are reported as drops (TRAILBEAM_SPANS_DROPPED).
 */
async function uninstall() {
  const previous = state;
  state = null;
  if (previous) await previous.provider.shutdown().catch((e) => debug('provider shutdown', e));
}

/** Flush Trailbeam spans that are waiting to be exported. Never rejects, as `uninstall`. */
async function forceFlush() {
  if (state) await state.provider.forceFlush().catch((e) => debug('flush', e));
}

module.exports = {
  install,
  instrument,
  uninstall,
  forceFlush,
  SqsHooks,
  sqsMiddleware,
  addReceiveAttributeNames,
  addMessageAttributeName,
  errorCode,
  plainCode,
  RECEIVE_SYSTEM_ATTRIBUTES,
  MIDDLEWARE_NAME,
};
