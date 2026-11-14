'use strict';
// qv2 hooks for AWS SDK v3 SQS clients (contract sections 4.1 to 4.3).
//
// The hooks are a middleware on each SQSClient's middleware stack. When the application uses
// @opentelemetry/instrumentation-aws-sdk, that instrumentation runs the whole stack inside the
// context of its span for the call, so each qv2 span is a child of the stock span and shares its
// trace ID. Without it, qv2 spans are children of whatever span is active, or root spans.
//
//     const qv2 = require('@qv2/sdk');
//     const sqs = qv2.instrument(new SQSClient({}));   // installs with defaults on first use

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

const MIDDLEWARE_NAME = 'qv2SqsMiddleware';

// System attributes the receive hook asks for (section 4.2).
const RECEIVE_SYSTEM_ATTRIBUTES = Object.freeze([
  'ApproximateReceiveCount',
  'SentTimestamp',
  'ApproximateFirstReceiveTimestamp',
  'AWSTraceHeader',
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
  api.diag.debug(`qv2: ${what} failed`, err);
}

/**
 * A copy of a ReceiveMessage input that asks for the system attributes qv2 needs, keeping
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
 * Turns SQS calls into qv2 spans. Hook methods never throw. `times` is `{startTime, endTime}` of the
 * SQS call in epoch ms (contract section 2), plus its call-order ticks `{issued, returned}` (schema
 * version 3); every span of one call, batch entries included, gets them.
 */
class SqsHooks {
  constructor(emitter, handles = new HandleMap()) {
    this.emitter = emitter;
    this.handles = handles;
  }

  onSend(operation, input, output, parentContext, times = {}) {
    const url = input.QueueUrl;
    const name = queueNameFromUrl(url);
    if (!output) return;
    const t = spanTimes(times);
    if (operation === 'SendMessage') {
      if (output.MessageId) {
        this.emitter.emit(SPAN_SEND, operation, name, url, { 'messaging.message.id': output.MessageId },
          { parentContext, ...t });
      }
      return;
    }
    for (const entry of output.Successful || []) {
      if (!entry || !entry.MessageId) continue;
      const attrs = { 'messaging.message.id': entry.MessageId };
      put(attrs, 'qv2.batch_entry_id', entry.Id);
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
      put(attrs, 'qv2.receive_count', intOrUndefined(a.ApproximateReceiveCount));
      put(attrs, 'qv2.sent_timestamp_ms', intOrUndefined(a.SentTimestamp));
      put(attrs, 'qv2.first_receive_timestamp_ms', intOrUndefined(a.ApproximateFirstReceiveTimestamp));
      put(attrs, 'qv2.aws_trace_header', a.AWSTraceHeader);
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
      if (batch) put(attrs, 'qv2.batch_entry_id', entryId);
      if (spanName === SPAN_CHANGE_VISIBILITY) put(attrs, 'qv2.visibility_timeout', intOrUndefined(vt));
      attrs['qv2.result'] = error === undefined ? 'ok' : 'failed';
      put(attrs, 'qv2.error_code', error);
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
        const changed = addReceiveAttributeNames(input);
        if (changed) args = { ...args, input: changed };
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
 * Create the qv2 client hooks for this process: a tracer provider of our own, the receipt-hash key
 * and the handle map. Returns the qv2 tracer provider. Calling it again replaces the previous
 * installation (and shuts its provider down).
 *
 * @param {object} [options]
 * @param {string} [options.receiptHashKey] takes precedence over $QV2_RECEIPT_HASH_KEY
 * @param {import('@opentelemetry/sdk-trace-base').SpanExporter} [options.spanExporter]
 * @param {import('@opentelemetry/sdk-trace-base').SpanProcessor} [options.spanProcessor]
 */
function install(options = {}) {
  const previous = state;
  const { provider, resource } = makeProvider(options);
  const clientId = resource.attributes[SERVICE_INSTANCE_ID];
  const emitter = new Emitter(provider, resolveHasher(options.receiptHashKey), 'client', clientId);
  state = { provider, hooks: new SqsHooks(emitter) };
  if (previous) previous.provider.shutdown().catch((e) => debug('provider shutdown', e));
  return provider;
}

/**
 * Add the qv2 hooks to an AWS SDK v3 SQSClient. Installs with default options first if `install`
 * has not been called. Safe to call more than once on the same client. Returns the client.
 */
function instrument(client) {
  if (!client || !client.middlewareStack || typeof client.middlewareStack.add !== 'function') {
    throw new TypeError('qv2.instrument expects an AWS SDK v3 SQSClient');
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

/** Turn the hooks off on every instrumented client, then flush and shut down the qv2 provider. */
async function uninstall() {
  const previous = state;
  state = null;
  if (previous) await previous.provider.shutdown();
}

/** Flush qv2 spans that are waiting to be exported. */
async function forceFlush() {
  if (state) await state.provider.forceFlush();
}

module.exports = {
  install,
  instrument,
  uninstall,
  forceFlush,
  SqsHooks,
  sqsMiddleware,
  addReceiveAttributeNames,
  errorCode,
  plainCode,
  RECEIVE_SYSTEM_ATTRIBUTES,
  MIDDLEWARE_NAME,
};
