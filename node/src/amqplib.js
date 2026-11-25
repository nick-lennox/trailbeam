'use strict';
// Trailbeam hooks for amqplib, the RabbitMQ client (contract/span-contract-amqp.md, schema version 4).
//
//     const trailbeam = require('trailbeam');
//     trailbeam.amqplib.install();      // once per process, before opening connections
//
// The hooks sit on amqplib's channel classes, which its promise API (`require('amqplib')`) and its
// callback API (`require('amqplib/callback_api')`) both build on, with or without connection recovery:
//
// - `publish` and `sendToQueue`, and the broker's confirms on a confirm channel (a `trailbeam publish` per
//   publish), and its `basic.return` (`trailbeam return`);
// - deliveries to `consume` consumers and `get` replies (`trailbeam deliver`), emitted before the
//   application's callback runs;
// - `ack`, `ackAll`, `nack`, `nackAll` and `reject` (one span per delivery covered);
// - `cancel` and a cancel from the broker (`trailbeam cancel`);
// - the channel or its connection closing with deliveries not settled (`trailbeam channel_closed`).
//
// The same spans as the Python package's pika hooks (`trailbeam.pika`), span for span. The hooks make no
// broker calls of their own and never change a message, except that `setMessageId: true` (or
// TRAILBEAM_AMQP_SET_MESSAGE_ID=on) gives a publish without a `messageId` a random one (A6); the
// application's options object is copied, not changed. A consumer on the server-named queue `''` gets no
// spans: its queue name is not known to the hooks. amqplib 0.10 or later.
//
// Nothing here requires amqplib until `install` is called, so the package works without it.

const crypto = require('node:crypto');
const path = require('node:path');
const api = require('@opentelemetry/api');

const { makeProvider, nowMs, SERVICE_INSTANCE_ID } = require('./emitter');
const { SCOPE_NAME } = require('./contract');
const { version } = require('./version');
const order = require('./order');
const capture = require('./capture');

const AMQP_SCHEMA_VERSION = 4;
const RABBITMQ = 'rabbitmq';
const DEFAULT_EXCHANGE = 'amq.default'; // A5: the name the OpenTelemetry conventions use for ""
const ENV_SET_MESSAGE_ID = 'TRAILBEAM_AMQP_SET_MESSAGE_ID';

const SPAN_PUBLISH = 'trailbeam publish';
const SPAN_RETURN = 'trailbeam return';
const SPAN_DELIVER = 'trailbeam deliver';
const SPAN_ACK = 'trailbeam ack';
const SPAN_NACK = 'trailbeam nack';
const SPAN_REJECT = 'trailbeam reject';
const SPAN_CANCEL = 'trailbeam cancel';
const SPAN_CHANNEL_CLOSED = 'trailbeam channel_closed';

// span name -> [trailbeam.kind, messaging.operation.type], A4
const KINDS = {
  [SPAN_PUBLISH]: ['amqp.publish', 'send'],
  [SPAN_RETURN]: ['amqp.return', 'send'],
  [SPAN_DELIVER]: ['amqp.deliver', 'receive'],
  [SPAN_ACK]: ['amqp.ack', 'settle'],
  [SPAN_NACK]: ['amqp.nack', 'settle'],
  [SPAN_REJECT]: ['amqp.reject', 'settle'],
  [SPAN_CANCEL]: ['amqp.cancel', 'cancel'],
  [SPAN_CHANNEL_CLOSED]: ['amqp.channel_closed', 'settle'],
};
const SETTLE_SPANS = { 'basic.ack': SPAN_ACK, 'basic.nack': SPAN_NACK, 'basic.reject': SPAN_REJECT };

// AMQP 0-9-1 reply codes, for trailbeam.error_code of a publish whose channel closed before its confirm (A4.1)
const REPLY_CODE_NAMES = {
  311: 'CONTENT_TOO_LARGE', 312: 'NO_ROUTE', 313: 'NO_CONSUMERS', 320: 'CONNECTION_FORCED',
  402: 'INVALID_PATH', 403: 'ACCESS_REFUSED', 404: 'NOT_FOUND', 405: 'RESOURCE_LOCKED',
  406: 'PRECONDITION_FAILED', 501: 'FRAME_ERROR', 502: 'SYNTAX_ERROR', 503: 'COMMAND_INVALID',
  504: 'CHANNEL_ERROR', 505: 'UNEXPECTED_FRAME', 506: 'RESOURCE_ERROR', 530: 'NOT_ALLOWED',
  540: 'NOT_IMPLEMENTED', 541: 'INTERNAL_ERROR',
};

const MAX_DELIVERIES_PER_CHANNEL = 100000; // only reached with no prefetch limit and nothing settled
const MAX_PUBLISHES_PER_CHANNEL = 100000; // publishes waiting for a confirm
const MAX_CONSUMERS_PER_CHANNEL = 10000;
const MAX_PACKAGE_IDS = 10000;

const STATUS_NOT_TEXT = 'not_text'; // 4.0: a body with a content_encoding, or not valid UTF-8
const IDENTITY_ENCODINGS = new Set(['', 'identity']);

function debug(what, err) {
  api.diag.debug(`trailbeam: amqplib ${what} failed`, err);
}

// -- values (A5, A7, A8) ---------------------------------------------------------------------------

/** A5: the exchange as Trailbeam names it; the default exchange "" is amq.default. */
function exchangeName(exchange) {
  return typeof exchange === 'string' && exchange ? exchange : DEFAULT_EXCHANGE;
}

/** An AMQP integer value (A7: anything else is absent). amqplib decodes table integers to numbers. */
function amqpInt(v) {
  return typeof v === 'number' && Number.isSafeInteger(v) ? v : undefined;
}

const utf8 = new TextDecoder('utf-8', { fatal: true, ignoreBOM: true });

function decodeText(bytes) {
  try {
    return utf8.decode(bytes);
  } catch (e) {
    return undefined;
  }
}

// A3: the longest string the package copies from the broker or a header, in UTF-8 bytes: an AMQP short string's
// limit, so no real queue, exchange, routing key, consumer tag or message ID is longer. Longer values are dropped.
const MAX_TEXT_BYTES = 255;

/** A non-empty string of at most MAX_TEXT_BYTES bytes (A3); a Buffer of valid UTF-8 is read as one. */
function text(v) {
  if (v instanceof Uint8Array) v = v.length > MAX_TEXT_BYTES ? undefined : decodeText(v);
  if (typeof v !== 'string' || !v || v.length > MAX_TEXT_BYTES) return undefined; // a UTF-16 unit is >= 1 byte
  return Buffer.byteLength(v, 'utf8') <= MAX_TEXT_BYTES ? v : undefined;
}

/** An AMQP timestamp (POSIX seconds) in milliseconds. amqplib gives a number, or `{'!': 'timestamp', value}`
 * inside a header table; a Date is accepted too. */
function timestampMs(v) {
  if (v instanceof Date) {
    const t = v.getTime();
    return Number.isFinite(t) ? Math.floor(t / 1000) * 1000 : undefined;
  }
  if (v && typeof v === 'object' && v['!'] === 'timestamp') v = v.value;
  const n = amqpInt(v);
  return n !== undefined && n >= 0 ? n * 1000 : undefined;
}

const isTable = (v) => v !== null && typeof v === 'object' && !Array.isArray(v) && !(v instanceof Uint8Array);

/** A7: the headers whose `x-acquired-count` and `x-delivery-count` the broker set. RabbitMQ sets them only on a
 * redelivery or carries them over to a dead-letter queue (with `x-death`); on any other delivery they came from
 * the publisher, so they are ignored ({}). */
function counterHeaders(redelivered, headers) {
  const h = isTable(headers) ? headers : {};
  if (redelivered || Object.keys(deadLetterAttrs(h['x-death'], h)).length) return h;
  return {};
}

/** A7: [trailbeam.receive_count, trailbeam.receive_count.source], or [undefined, undefined] when the broker gave none. */
function receiveCount(redelivered, headers) {
  const h = counterHeaders(redelivered, headers);
  const acquired = amqpInt(h['x-acquired-count']);
  if (acquired !== undefined) return [acquired + 1, 'x-acquired-count'];
  const delivered = amqpInt(h['x-delivery-count']);
  if (delivered !== undefined) return [delivered + 1, 'x-delivery-count'];
  if (!redelivered) return [1, 'first_delivery'];
  return [undefined, undefined];
}

/** A8: the trailbeam.dead_letter.* attributes from the newest `x-death` entry; {} when it is not usable. */
function deadLetterAttrs(xDeath, headers) {
  if (!Array.isArray(xDeath) || !xDeath.length || !isTable(xDeath[0])) return {};
  const first = xDeath[0];
  const queue = text(first.queue);
  const reason = text(first.reason);
  if (queue === undefined || reason === undefined) return {};
  const out = { 'trailbeam.dead_letter.source_queue': queue, 'trailbeam.dead_letter.reason': reason };
  const count = amqpInt(first.count);
  if (count !== undefined) out['trailbeam.dead_letter.count'] = count;
  const t = timestampMs(first.time);
  if (t !== undefined) out['trailbeam.dead_letter.time_ms'] = t;
  let exchange = first.exchange;
  if (exchange instanceof Uint8Array) exchange = text(exchange);
  if (typeof exchange === 'string' && (exchange === '' || text(exchange) !== undefined)) {
    out['trailbeam.dead_letter.exchange'] = exchangeName(exchange);
  }
  const h = isTable(headers) ? headers : {};
  for (const [header, attr] of [['x-first-death-queue', 'trailbeam.dead_letter.first_queue'],
    ['x-first-death-reason', 'trailbeam.dead_letter.first_reason']]) {
    const v = text(h[header]);
    if (v !== undefined) out[attr] = v;
  }
  return out;
}

/** A11: the tenant from the header the tenant option names (a string or a number), else the body path. */
function amqpTenant(config, headers, bodyText) {
  let attributes;
  if (config.tenantAttribute && isTable(headers)) {
    let v = headers[config.tenantAttribute];
    if (v instanceof Uint8Array) v = decodeText(v);
    if (typeof v === 'number' && Number.isFinite(v)) v = JSON.stringify(v);
    if (typeof v === 'string') attributes = { [config.tenantAttribute]: { StringValue: v } };
  }
  // The SQS reader applies the same limits: the attribute first, then the body path.
  const t = capture.tenantFrom(config, attributes, bodyText);
  if (!t) return undefined;
  return { tenant: t.tenant, source: t.source === 'message_attribute' ? 'header' : t.source };
}

/**
 * A11 attributes for the message of a publish (`name`: the exchange) or a delivery (the queue): body size,
 * tenant and opt-in body capture. Text only, no MD5 check and no envelope.
 */
function amqpContentAttrs(config, name, body, headers, contentEncoding) {
  const out = {};
  if (typeof body === 'string') body = Buffer.from(body, 'utf8');
  if (!(body instanceof Uint8Array)) return out;
  out['messaging.message.body.size'] = body.byteLength;
  let bodyText;
  if (contentEncoding === undefined || contentEncoding === null
      || (typeof contentEncoding === 'string' && IDENTITY_ENCODINGS.has(contentEncoding.toLowerCase()))) {
    bodyText = decodeText(body);
  }
  const t = amqpTenant(config, headers, bodyText);
  if (t) {
    out['trailbeam.tenant'] = t.tenant;
    out['trailbeam.tenant.source'] = t.source;
  }
  if (!config.bodyOn(name)) {
    out['trailbeam.body.capture_status'] = capture.STATUS_OFF;
    return out;
  }
  if (bodyText === undefined) {
    out['trailbeam.body.capture_status'] = STATUS_NOT_TEXT;
    return out;
  }
  let redacted;
  let count;
  let format;
  try {
    [redacted, count, format] = capture.redactBody(bodyText, config);
  } catch (e) {
    out['trailbeam.body.capture_status'] = capture.STATUS_REDACTION_FAILED;
    return out;
  }
  if (Buffer.byteLength(redacted, 'utf8') > config.maxBodyBytes) {
    out['trailbeam.body.capture_status'] = capture.STATUS_EXCEEDS_LIMIT;
    return out;
  }
  out['trailbeam.body.capture_status'] = capture.STATUS_CAPTURED;
  out['trailbeam.body'] = redacted;
  out['trailbeam.body.format'] = format;
  out['trailbeam.body.redacted_count'] = count;
  return out;
}

/** trailbeam.error_code of a call that threw: the error's class name. */
function errorName(e) {
  if (e && typeof e === 'object') {
    if (typeof e.name === 'string' && e.name) return e.name;
    if (e.constructor && e.constructor.name) return e.constructor.name;
  }
  return 'Error';
}

function trimFirst(map, max) {
  while (map.size > max) map.delete(map.keys().next().value);
}

// -- per-channel state (A3, A6) ----------------------------------------------------------------------

/** What the package knows about one channel. `id` is the random UUID of A3, chosen when the channel is
 * first seen; channel numbers are reused, IDs are not. */
class ChannelState {
  constructor({ vhost = '/', host, port, number } = {}) {
    this.vhost = vhost || '/';
    this.host = host;
    this.port = port;
    this.number = number;
    this.id = crypto.randomUUID();
    this.confirm = false;
    this.seq = 0; // publish sequence number of the last publish in confirm mode
    this.publishes = new Map(); // seq -> { attrs, call }
    this.deliveries = new Map(); // delivery tag -> { queue, consumerTag, messageId }
    this.consumers = new Map(); // consumer tag -> { queue, noAck }
    this.cancels = new Map(); // consumer tag -> { attrs, call }
    this.closed = false;
    this.closeInfo = undefined; // [operation, initiator, reply code, reply text], once known
    this.clientClosing = false;
  }

  consumer(tag) {
    return typeof tag === 'string' ? this.consumers.get(tag) : undefined;
  }

  addConsumer(tag, queue, noAck) {
    this.consumers.delete(tag);
    this.consumers.set(tag, { queue, noAck: Boolean(noAck) });
    trimFirst(this.consumers, MAX_CONSUMERS_PER_CHANNEL);
  }

  record(tag, delivery) {
    this.deliveries.delete(tag);
    this.deliveries.set(tag, delivery);
    trimFirst(this.deliveries, MAX_DELIVERIES_PER_CHANNEL);
  }

  /** The recorded deliveries a settle with this tag covers (A4.4): tag 0 with `multiple` is all of them. */
  covered(tag, multiple) {
    if (!multiple) {
      const d = this.deliveries.get(tag);
      return d ? [[tag, d]] : [];
    }
    return [...this.deliveries].filter(([t]) => tag === 0 || t <= tag);
  }

  unsettledFor(consumerTag) {
    let n = 0;
    for (const d of this.deliveries.values()) if (d.consumerTag === consumerTag) n += 1;
    return n;
  }
}

// -- the spans ---------------------------------------------------------------------------------------

/** Builds and emits the RabbitMQ spans. Every public method is safe to call from a hook: a failure is
 * logged at debug level and never reaches the application. */
class AmqpHooks {
  constructor(tracer, clientId, config, setMessageId = false) {
    this.tracer = tracer;
    this.clientId = clientId;
    this.config = config || capture.resolveConfig();
    this.setMessageId = Boolean(setMessageId);
    this.packageIds = new Set();
  }

  /** Section 2: the tick just before the time, both just before the call. */
  static startCall() {
    const issued = order.tick();
    return { start: nowMs(), issued };
  }

  /** A6, design choice P2 option a: an ID the package sets on a publish without one (opt-in). */
  newMessageId() {
    const mid = crypto.randomUUID();
    this.packageIds.add(mid);
    if (this.packageIds.size > MAX_PACKAGE_IDS) this.packageIds.delete(this.packageIds.values().next().value);
    return mid;
  }

  idAttrs(messageId) {
    const mid = text(messageId);
    if (mid === undefined) return {};
    return {
      'messaging.message.id': mid,
      'trailbeam.amqp.message_id.source': this.packageIds.has(mid) ? 'package' : 'application',
    };
  }

  base(st, name, operation, destination, channel) {
    const [kind, opType] = KINDS[name];
    const a = {
      'trailbeam.schema_version': AMQP_SCHEMA_VERSION,
      'trailbeam.kind': kind,
      'trailbeam.source': 'client',
      'messaging.system.name': RABBITMQ,
      'messaging.operation.type': opType,
      'messaging.operation.name': operation,
      'messaging.destination.name': destination,
      'trailbeam.amqp.vhost': st.vhost || '/',
      'messaging.client.id': this.clientId,
      'process.pid': process.pid,
    };
    if (st.host) a['server.address'] = st.host;
    if (Number.isInteger(st.port)) a['server.port'] = st.port;
    if (channel) { // A3: on every span but publish and return
      a['trailbeam.amqp.channel.id'] = st.id;
      if (Number.isInteger(st.number)) a['trailbeam.amqp.channel.number'] = st.number;
    }
    return a;
  }

  emit(name, attrs, start, end, issued, returned, startObserved = true) {
    attrs['trailbeam.order.scope'] = order.scope;
    if (issued !== undefined && startObserved) attrs['trailbeam.order.issued'] = issued;
    attrs['trailbeam.order.returned'] = returned;
    if (!startObserved) {
      attrs['trailbeam.start_observed'] = false;
      start = end;
    }
    const attributes = {};
    for (const [k, v] of Object.entries(attrs)) if (v !== undefined && v !== null && v !== '') attributes[k] = v;
    const span = this.tracer.startSpan(name, { kind: api.SpanKind.INTERNAL, attributes, startTime: Math.min(start, end) },
      api.context.active());
    span.end(end);
  }

  /** Something the broker sent, or a close: zero-length at this moment, start not observed (A4). */
  event(name, attrs) {
    const end = nowMs();
    this.emit(name, attrs, end, end, undefined, order.tick(), false);
  }

  endCall(name, attrs, call, end = nowMs(), returned = order.tick()) {
    this.emit(name, attrs, call.start, end, call.issued, returned);
  }

  safely(what, fn) {
    try {
      return fn();
    } catch (e) { // never break the application's AMQP call
      debug(what, e);
      return undefined;
    }
  }

  // -- publish (A4.1) --------------------------------------------------------------------------------

  publishAttrs(st, exchange, routingKey, mandatory, messageId, deliveryMode, headers, body, contentEncoding) {
    const dest = exchangeName(exchange);
    const a = this.base(st, SPAN_PUBLISH, 'basic.publish', dest, false);
    a['rabbitmq.destination.routing_key'] = text(routingKey);
    Object.assign(a, this.idAttrs(messageId));
    a['trailbeam.amqp.mandatory'] = Boolean(mandatory);
    if (deliveryMode === 1 || deliveryMode === 2) a['trailbeam.amqp.delivery_mode'] = deliveryMode;
    Object.assign(a, amqpContentAttrs(this.config, dest, body, headers, contentEncoding));
    return a;
  }

  /** Times `send` (the library's publish) and emits or parks its span. `attrs` from `publishAttrs`;
   * undefined when building them failed, and then the publish goes ahead with no span. */
  publish(st, attrs, send) {
    if (!attrs) return send();
    const call = AmqpHooks.startCall();
    let seq;
    attrs['trailbeam.amqp.confirmed'] = st.confirm;
    if (st.confirm) {
      seq = st.seq + 1;
      st.seq = seq;
      st.publishes.set(seq, { attrs, call });
      trimFirst(st.publishes, MAX_PUBLISHES_PER_CHANNEL);
    }
    let result;
    try {
      result = send();
    } catch (e) {
      const end = nowMs();
      if (seq !== undefined && st.publishes.delete(seq) && st.seq === seq) {
        st.seq = seq - 1; // the library threw before the frame went out: the broker never numbered it
      }
      attrs['trailbeam.result'] = 'failed';
      attrs['trailbeam.error_code'] = errorName(e);
      this.safely('publish', () => this.endCall(SPAN_PUBLISH, attrs, call, end));
      throw e;
    }
    if (seq === undefined) { // no confirms: ends when the library returns from writing the frame
      attrs['trailbeam.result'] = 'ok';
      this.safely('publish', () => this.endCall(SPAN_PUBLISH, attrs, call));
    }
    return result;
  }

  /** The broker's basic.ack or basic.nack for publishes in confirm mode. */
  onConfirm(st, ack, tag, multiple) {
    this.safely('confirm', () => {
      tag = amqpInt(tag);
      if (tag === undefined) return;
      const end = nowMs();
      const returned = order.tick();
      const seqs = multiple ? [...st.publishes.keys()].filter((s) => tag === 0 || s <= tag)
        : (st.publishes.has(tag) ? [tag] : []);
      for (const s of seqs) {
        const p = st.publishes.get(s);
        st.publishes.delete(s);
        p.attrs['trailbeam.result'] = ack ? 'ok' : 'failed';
        if (!ack) p.attrs['trailbeam.error_code'] = 'nack';
        this.endCall(SPAN_PUBLISH, p.attrs, p.call, end, returned);
      }
    });
  }

  // -- return (A4.2) ---------------------------------------------------------------------------------

  onReturn(st, exchange, routingKey, replyCode, replyText, messageId) {
    this.safely('return', () => {
      const a = this.base(st, SPAN_RETURN, 'basic.return', exchangeName(exchange), false);
      a['rabbitmq.destination.routing_key'] = text(routingKey);
      Object.assign(a, this.idAttrs(messageId));
      a['trailbeam.amqp.reply_code'] = amqpInt(replyCode);
      a['trailbeam.amqp.reply_text'] = text(replyText);
      this.event(SPAN_RETURN, a);
    });
  }

  // -- deliver (A4.3) --------------------------------------------------------------------------------

  /** A push delivery, before the application's callback runs. `m`: amqplib's message. */
  onDeliver(st, m) {
    this.safely('deliver', () => {
      const f = m.fields || {};
      const consumer = st.consumer(f.consumerTag);
      if (!consumer || !consumer.queue) return; // a consumer the package didn't see start: its queue is not known
      const a = this.deliveryAttrs(st, 'basic.deliver', consumer.queue, consumer.noAck, m);
      if (a) this.event(SPAN_DELIVER, a);
    });
  }

  /** basic.get-ok: the pull delivery, the interval of the call (a basic.get-empty gives no span). */
  onGetOk(st, call, queue, noAck, m) {
    this.safely('get', () => {
      const end = nowMs();
      const returned = order.tick();
      const a = this.deliveryAttrs(st, 'basic.get', queue, noAck, m);
      if (a) this.endCall(SPAN_DELIVER, a, call, end, returned);
    });
  }

  deliveryAttrs(st, operation, queue, noAck, m) {
    const f = m.fields || {};
    const p = m.properties || {};
    const tag = amqpInt(f.deliveryTag);
    if (tag === undefined) return undefined;
    const a = this.base(st, SPAN_DELIVER, operation, queue, true);
    a['rabbitmq.message.delivery_tag'] = tag;
    if (operation === 'basic.deliver') a['trailbeam.amqp.consumer_tag'] = text(f.consumerTag);
    a['trailbeam.amqp.exchange'] = exchangeName(f.exchange);
    a['rabbitmq.destination.routing_key'] = text(f.routingKey);
    a['trailbeam.amqp.redelivered'] = Boolean(f.redelivered);
    const mid = text(p.messageId);
    a['messaging.message.id'] = mid;
    const h = isTable(p.headers) ? p.headers : {};
    const [count, source] = receiveCount(Boolean(f.redelivered), h);
    if (count !== undefined) {
      a['trailbeam.receive_count'] = count;
      a['trailbeam.receive_count.source'] = source;
    }
    const counters = counterHeaders(Boolean(f.redelivered), h);
    a['trailbeam.amqp.acquired_count'] = amqpInt(counters['x-acquired-count']);
    a['trailbeam.amqp.delivery_count'] = amqpInt(counters['x-delivery-count']);
    Object.assign(a, deadLetterAttrs(h['x-death'], h));
    a['trailbeam.amqp.timestamp_ms'] = timestampMs(p.timestamp);
    Object.assign(a, amqpContentAttrs(this.config, queue, m.content, h, p.contentEncoding));
    if (!noAck) { // with noAck the broker settled it on sending: nothing to settle or requeue
      st.record(tag, { queue, consumerTag: operation === 'basic.deliver' ? text(f.consumerTag) : undefined, messageId: mid });
    }
    return a;
  }

  // -- ack, nack, reject (A4.4) ----------------------------------------------------------------------

  /** Times `send` (the library's ack, nack or reject) and emits one span per recorded delivery it covers. */
  settle(st, operation, deliveryTag, multiple, requeue, send) {
    const name = SETTLE_SPANS[operation];
    const tag = amqpInt(deliveryTag);
    const covered = tag !== undefined ? st.covered(tag, Boolean(multiple)) : [];
    if (!covered.length) return send();
    const call = AmqpHooks.startCall();
    let result;
    try {
      result = send();
    } catch (e) {
      this.safely('settle', () => this.settled(st, name, operation, covered, multiple, requeue, call, errorName(e)));
      throw e;
    }
    this.safely('settle', () => this.settled(st, name, operation, covered, multiple, requeue, call, undefined));
    return result;
  }

  settled(st, name, operation, covered, multiple, requeue, call, error) {
    const end = nowMs();
    const returned = order.tick();
    for (const [tag, d] of covered) {
      if (error === undefined) st.deliveries.delete(tag);
      const a = this.base(st, name, operation, d.queue, true);
      a['messaging.message.id'] = d.messageId;
      a['rabbitmq.message.delivery_tag'] = tag;
      a['trailbeam.amqp.consumer_tag'] = d.consumerTag;
      if (operation !== 'basic.reject') a['trailbeam.amqp.multiple'] = Boolean(multiple);
      if (operation !== 'basic.ack') a['trailbeam.amqp.requeue'] = Boolean(requeue);
      a['trailbeam.result'] = error === undefined ? 'ok' : 'failed';
      a['trailbeam.error_code'] = error;
      this.endCall(name, a, call, end, returned);
    }
  }

  // -- consumers (A4.5) ------------------------------------------------------------------------------

  consumerStarted(st, consumerTag, queue, noAck) {
    if (typeof consumerTag === 'string' && consumerTag && typeof queue === 'string') st.addConsumer(consumerTag, queue, noAck);
  }

  cancelAttrs(st, consumerTag, by) {
    const consumer = st.consumer(consumerTag);
    if (!consumer || !consumer.queue) return undefined;
    const a = this.base(st, SPAN_CANCEL, 'basic.cancel', consumer.queue, true);
    a['trailbeam.amqp.consumer_tag'] = consumerTag;
    a['trailbeam.amqp.cancelled_by'] = by;
    a['trailbeam.amqp.unsettled_count'] = st.unsettledFor(consumerTag);
    return a;
  }

  /** A client basic.cancel: the span ends at its cancel-ok (`cancelOk`), or fails if `send` throws or the
   * channel closes first. */
  cancel(st, consumerTag, send) {
    const a = typeof consumerTag === 'string' ? this.safely('cancel', () => this.cancelAttrs(st, consumerTag, 'client')) : undefined;
    if (!a) return send();
    const call = AmqpHooks.startCall();
    st.cancels.set(consumerTag, { attrs: a, call });
    try {
      return send();
    } catch (e) {
      st.cancels.delete(consumerTag);
      a['trailbeam.result'] = 'failed';
      a['trailbeam.error_code'] = errorName(e);
      this.safely('cancel', () => this.endCall(SPAN_CANCEL, a, call));
      throw e;
    }
  }

  cancelOk(st, consumerTag) {
    this.safely('cancel', () => {
      const p = typeof consumerTag === 'string' ? st.cancels.get(consumerTag) : undefined;
      if (!p) return;
      st.cancels.delete(consumerTag);
      p.attrs['trailbeam.result'] = 'ok';
      this.endCall(SPAN_CANCEL, p.attrs, p.call);
    });
  }

  onBrokerCancel(st, consumerTag) {
    this.safely('cancel', () => {
      if (typeof consumerTag !== 'string') return;
      const a = this.cancelAttrs(st, consumerTag, 'broker');
      if (a) this.event(SPAN_CANCEL, a);
    });
  }

  // -- channel closed (A4.6) -------------------------------------------------------------------------

  /** The channel closed (`operation`: channel.close, connection.close or connection.lost). One span per
   * unsettled delivery; publishes still waiting for a confirm fail, as do client cancels waiting for cancel-ok. */
  onClose(st, operation, initiator, replyCode, replyText) {
    this.safely('close', () => {
      if (st.closed) return;
      st.closed = true;
      // Only a close the broker sent carries a reason worth showing; the client's own code is its choice.
      const code = initiator === 'broker' ? amqpInt(replyCode) : undefined;
      const reason = initiator === 'broker' ? text(replyText) : undefined;
      const error = code !== undefined ? (REPLY_CODE_NAMES[code] || 'channel_closed') : 'channel_closed';
      const end = nowMs();
      const returned = order.tick();
      for (const p of st.publishes.values()) {
        p.attrs['trailbeam.result'] = 'failed';
        p.attrs['trailbeam.error_code'] = error;
        this.endCall(SPAN_PUBLISH, p.attrs, p.call, end, returned);
      }
      st.publishes.clear();
      for (const p of st.cancels.values()) {
        p.attrs['trailbeam.result'] = 'failed';
        p.attrs['trailbeam.error_code'] = error;
        this.endCall(SPAN_CANCEL, p.attrs, p.call, end, returned);
      }
      st.cancels.clear();
      for (const [tag, d] of st.deliveries) {
        const a = this.base(st, SPAN_CHANNEL_CLOSED, operation, d.queue, true);
        a['messaging.message.id'] = d.messageId;
        a['rabbitmq.message.delivery_tag'] = tag;
        a['trailbeam.amqp.consumer_tag'] = d.consumerTag;
        a['trailbeam.amqp.close.initiator'] = initiator;
        a['trailbeam.amqp.close.reply_code'] = code;
        a['trailbeam.amqp.close.reply_text'] = reason;
        this.emit(SPAN_CHANNEL_CLOSED, a, end, end, undefined, returned, false);
      }
      st.deliveries.clear();
    });
  }
}

// -- the amqplib hooks -------------------------------------------------------------------------------

let hooks = null; // the AmqpHooks of the current installation
let state = null; // { provider, lib }

const ORIG = Symbol('trailbeam.amqplib.original');
const channelStates = new WeakMap(); // amqplib channel -> ChannelState
const connections = new WeakMap(); // amqplib Connection -> { vhost, closing }

function connectionInfo(conn) {
  let info = connections.get(conn);
  if (!info && conn && typeof conn === 'object') {
    info = { vhost: '/', closing: undefined };
    connections.set(conn, info);
  }
  return info || { vhost: '/', closing: undefined };
}

/** The channel's state, made when the hooks first see the channel. */
function stateOf(ch) {
  let st = channelStates.get(ch);
  if (st) return st;
  const conn = ch.connection;
  const info = connectionInfo(conn);
  // amqplib keeps no copy of the URL: the host is the one the socket was opened to (net.connect's own record of
  // it, not a reverse lookup), and the port the socket's.
  const stream = conn && conn.stream;
  const host = stream && (typeof stream._host === 'string' && stream._host ? stream._host : stream.remoteAddress);
  const port = stream && stream.remotePort;
  st = new ChannelState({ vhost: info.vhost, host: typeof host === 'string' ? host : undefined,
    port: Number.isInteger(port) ? port : undefined, number: ch.ch });
  channelStates.set(ch, st);
  return st;
}

/** [operation, initiator, reply code, reply text] of a channel that is closing (A4.6). */
function closeInfo(ch, st) {
  if (st.closeInfo) return st.closeInfo;
  const closing = connectionInfo(ch.connection).closing;
  if (st.clientClosing) return [closing && closing[1] === 'client' ? 'connection.close' : 'channel.close', 'client'];
  return closing || ['connection.lost', 'network'];
}

// amqplib's text for a connection.close from the broker (lib/format.js closeMessage)
const CONNECTION_CLOSED_RE = /^Connection closed: (\d+) \([^)]*\) with message "([\s\S]*)"$/;

function makePatches(lib) {
  const { defs } = lib;

  // lib/connection.js Connection
  const connection = {
    open: (orig) => function open(allFields, ...rest) {
      if (hooks) {
        try {
          const vhost = allFields && allFields.virtualHost;
          connectionInfo(this).vhost = typeof vhost === 'string' && vhost ? vhost : '/';
        } catch (e) {
          debug('open', e);
        }
      }
      return orig.call(this, allFields, ...rest);
    },
    // ChannelModel.close and CallbackModel.close, and closeWithError: the client closes the connection
    closeBecause: (orig) => function closeBecause(...args) {
      if (hooks) {
        const info = connectionInfo(this);
        if (!info.closing) info.closing = ['connection.close', 'client'];
      }
      return orig.apply(this, args);
    },
    toClosed: (orig) => function toClosed(capturedStack, maybeErr, ...rest) {
      if (hooks) {
        try {
          const info = connectionInfo(this);
          if (!info.closing) {
            const m = maybeErr && typeof maybeErr.code === 'number' && typeof maybeErr.message === 'string'
              ? CONNECTION_CLOSED_RE.exec(maybeErr.message) : null;
            if (m) info.closing = ['connection.close', 'broker', Number(m[1]), m[2]];
            else if (maybeErr) info.closing = ['connection.lost', 'network']; // socket error, end or heartbeat timeout
            else info.closing = ['connection.close', 'client'];
          }
        } catch (e) {
          debug('connection close', e);
        }
      }
      return orig.call(this, capturedStack, maybeErr, ...rest);
    },
  };

  // lib/channel.js Channel: the machinery both APIs share
  const channel = {
    allocate: (orig) => function allocate(...args) {
      const result = orig.apply(this, args);
      if (hooks) {
        try {
          stateOf(this).number = this.ch;
        } catch (e) {
          debug('channel open', e);
        }
      }
      return result;
    },
    _rpc: (orig) => function _rpc(method, fields, expect, cb, ...rest) {
      const h = hooks;
      if (!h || !fields) return orig.call(this, method, fields, expect, cb, ...rest);
      const st = stateOf(this);
      if (method === defs.ConfirmSelect) {
        h.safely('confirm.select', () => { st.confirm = true; });
      } else if (method === defs.BasicConsume) {
        const consumed = (err, f) => {
          if (err === null && f && f.fields) h.safely('consume', () => h.consumerStarted(st, f.fields.consumerTag, fields.queue, fields.noAck));
          return cb(err, f);
        };
        return orig.call(this, method, fields, expect, consumed, ...rest);
      } else if (method === defs.BasicCancel) {
        const tag = fields.consumerTag;
        const cancelled = (err, f) => {
          // on an error the channel is closing: the close fails the cancel (A4.6)
          if (err === null) h.cancelOk(st, tag);
          return cb(err, f);
        };
        return h.cancel(st, tag, () => orig.call(this, method, fields, expect, cancelled, ...rest));
      }
      return orig.call(this, method, fields, expect, cb, ...rest);
    },
    accept: (orig) => function accept(f, ...rest) {
      const h = hooks;
      if (h && f && f.id === defs.ChannelClose) {
        h.safely('close', () => {
          const st = stateOf(this);
          const fl = f.fields || {};
          st.closeInfo = ['channel.close', 'broker', fl.replyCode, fl.replyText];
        });
      }
      return orig.call(this, f, ...rest);
    },
    closeBecause: (orig) => function closeBecause(...args) {
      if (hooks) {
        try {
          stateOf(this).clientClosing = true;
        } catch (e) {
          debug('channel close', e);
        }
      }
      return orig.apply(this, args);
    },
    // Events go to the hooks before any listener: the application's, and amqplib's own, which run the
    // confirm callbacks and fail them when the channel closes.
    emit: (orig) => function emit(event, ...args) {
      const h = hooks;
      if (h) {
        if (event === 'ack' || event === 'nack') {
          const f = args[0] || {};
          h.onConfirm(stateOf(this), event === 'ack', f.deliveryTag, Boolean(f.multiple));
        } else if (event === 'return') {
          const m = args[0] || {};
          const fl = m.fields || {};
          h.onReturn(stateOf(this), fl.exchange, fl.routingKey, fl.replyCode, fl.replyText, (m.properties || {}).messageId);
        } else if (event === 'cancel') {
          h.onBrokerCancel(stateOf(this), (args[0] || {}).consumerTag);
        } else if (event === 'close') {
          const st = channelStates.get(this); // a channel the hooks never saw has nothing to report
          if (st) h.safely('close', () => h.onClose(st, ...closeInfo(this, st)));
        }
      }
      return orig.call(this, event, ...args);
    },
  };

  // lib/channel.js BaseChannel
  const baseChannel = {
    dispatchMessage: (orig) => function dispatchMessage(fields, message, ...rest) {
      const h = hooks;
      // amqplib dispatches a delivery to a consumer it knows (else it throws); a null message is a broker cancel
      if (h && message && fields && this.consumers && this.consumers.has(fields.consumerTag)) {
        h.onDeliver(stateOf(this), message);
      }
      return orig.call(this, fields, message, ...rest);
    },
  };

  // lib/channel_model.js and lib/callback_model.js Channel: the API methods (ConfirmChannel's publish calls these)
  const deliveryMode = (o) => {
    if (o.persistent !== undefined) return o.persistent ? 2 : 1;
    if (typeof o.deliveryMode === 'number') return o.deliveryMode;
    return o.deliveryMode ? 2 : undefined;
  };
  // nack and reject requeue unless told not to (amqplib's default); an ack has no requeue
  const requeueFlag = (v) => (v === undefined ? true : Boolean(v));
  const settle = (operation, tagOf, multipleOf, requeueOf) => (orig) => function settleMethod(...args) {
    const h = hooks;
    if (!h) return orig.apply(this, args);
    let tag;
    try {
      tag = tagOf(args);
    } catch (e) {
      return orig.apply(this, args); // amqplib reports the mistake itself
    }
    return h.settle(stateOf(this), operation, tag, multipleOf(args), requeueOf ? requeueFlag(requeueOf(args)) : undefined,
      () => orig.apply(this, args));
  };
  const tagOfMessage = (args) => args[0].fields.deliveryTag;
  const startGet = (h, ch, queue, options) => {
    const st = stateOf(ch);
    const noAck = Boolean(options && options.noAck);
    const call = AmqpHooks.startCall();
    return (m) => { if (m) h.onGetOk(st, call, queue, noAck, m); };
  };
  const model = (callbackApi) => ({
    publish: (orig) => function publish(exchange, routingKey, content, options, ...rest) {
      const h = hooks;
      if (!h) return orig.call(this, exchange, routingKey, content, options, ...rest);
      let opts = options;
      let attrs;
      try {
        if (h.setMessageId && !(options && options.messageId)) opts = { ...(options || {}), messageId: h.newMessageId() };
        const o = opts || {};
        attrs = h.publishAttrs(stateOf(this), exchange, routingKey, o.mandatory, o.messageId, deliveryMode(o), o.headers,
          content, o.contentEncoding);
      } catch (e) {
        debug('publish', e);
        opts = options;
        attrs = undefined;
      }
      return h.publish(stateOf(this), attrs, () => orig.call(this, exchange, routingKey, content, opts, ...rest));
    },
    // basic.get: the span is the interval of the call, to its get-ok (a get-empty or a failure gives none)
    get: callbackApi
      ? (orig) => function get(queue, options, cb0, ...rest) {
        const h = hooks;
        if (!h || typeof queue !== 'string' || !queue) return orig.call(this, queue, options, cb0, ...rest);
        const got = startGet(h, this, queue, options);
        const cb = (err, m) => {
          if (err === null) got(m);
          if (typeof cb0 !== 'function') return undefined;
          return err === null ? cb0(null, m) : cb0(err);
        };
        return orig.call(this, queue, options, cb, ...rest);
      }
      : (orig) => function get(queue, options, ...rest) {
        const h = hooks;
        if (!h || typeof queue !== 'string' || !queue) return orig.call(this, queue, options, ...rest);
        const got = startGet(h, this, queue, options);
        const p = orig.call(this, queue, options, ...rest);
        return p && typeof p.then === 'function' ? p.then((m) => { got(m); return m; }) : p;
      },
    ack: settle('basic.ack', tagOfMessage, (args) => Boolean(args[1])),
    ackAll: settle('basic.ack', () => 0, () => true),
    nack: settle('basic.nack', tagOfMessage, (args) => Boolean(args[1]), (args) => args[2]),
    nackAll: settle('basic.nack', () => 0, () => true, (args) => args[0]),
    reject: settle('basic.reject', tagOfMessage, () => false, (args) => args[1]),
  });

  return [
    [lib.Connection.prototype, connection],
    [lib.Channel.prototype, channel],
    [lib.BaseChannel.prototype, baseChannel],
    [lib.PromiseChannel.prototype, model(false)],
    [lib.CallbackChannel.prototype, model(true)],
  ];
}

/** amqplib's internal modules. Its package `exports` hide lib/, so they are loaded by path: the same module
 * instances amqplib itself uses. */
function loadAmqplib(dir) {
  if (!dir) {
    let main;
    try {
      main = require.resolve('amqplib');
    } catch (e) {
      main = require.resolve('amqplib', { paths: [process.cwd()] });
    }
    dir = path.dirname(main);
  }
  const lib = (name) => require(path.join(dir, 'lib', name));
  const channel = lib('channel.js');
  return {
    dir,
    defs: lib('defs.js'),
    Connection: lib('connection.js').Connection,
    Channel: channel.Channel,
    BaseChannel: channel.BaseChannel,
    PromiseChannel: lib('channel_model.js').Channel,
    CallbackChannel: lib('callback_model.js').Channel,
  };
}

function patch(lib) {
  for (const [proto, methods] of makePatches(lib)) {
    for (const [name, make] of Object.entries(methods)) {
      const current = proto[name];
      if (typeof current !== 'function' || current[ORIG]) continue;
      const own = Object.prototype.hasOwnProperty.call(proto, name);
      const wrapper = make(current);
      wrapper[ORIG] = { fn: current, own };
      Object.defineProperty(wrapper, 'name', { value: current.name || name });
      proto[name] = wrapper;
    }
  }
}

function unpatch(lib) {
  // Channels opened while installed only hold bound copies of methods the hooks don't replace, so after this
  // they behave as if the hooks had never been there.
  for (const [proto, methods] of makePatches(lib)) {
    for (const name of Object.keys(methods)) {
      const current = Object.prototype.hasOwnProperty.call(proto, name) ? proto[name] : undefined;
      const orig = current && current[ORIG];
      if (!orig) continue;
      if (orig.own) proto[name] = orig.fn;
      else delete proto[name]; // EventEmitter's emit, inherited
    }
  }
}

let exitHookAdded = false;
function addExitHook() {
  if (exitHookAdded) return;
  exitHookAdded = true;
  // A script that publishes and closes its connection must not lose its spans (as for the SQS hooks).
  process.on('beforeExit', () => {
    if (state) state.provider.forceFlush().catch((e) => debug('flush at exit', e));
  });
}

/**
 * Install the Trailbeam hooks into amqplib. Returns the Trailbeam tracer provider.
 *
 * `spanExporter` / `spanProcessor` replace the default OTLP/HTTP export to $TRAILBEAM_OTLP_TRACES_ENDPOINT.
 * `tenant` (`{ messageAttribute: header name, bodyPath }`) and `capture` are as for `install` (A11: the
 * tenant comes from a message header; the body lists name queues, and exchanges for publishes).
 * `setMessageId`: give a publish without a `messageId` a random UUID, marked
 * `trailbeam.amqp.message_id.source = package`; off unless true or $TRAILBEAM_AMQP_SET_MESSAGE_ID is on.
 * `amqplibDir`: the folder of the amqplib package to hook, when it is not the one `require('amqplib')`
 * finds from here or from the working directory. Calling `install` again replaces the previous installation.
 */
function install(options = {}) {
  const { tenant, capture: captureOptions, amqplibDir } = options;
  let { setMessageId } = options;
  const lib = loadAmqplib(amqplibDir);
  const config = capture.resolveConfig(tenant, captureOptions);
  if (setMessageId === undefined || setMessageId === null) {
    setMessageId = ['on', 'true', '1', 'yes'].includes((process.env[ENV_SET_MESSAGE_ID] || '').trim().toLowerCase());
  }
  uninstallSync();
  const { provider, resource } = makeProvider(options);
  const tracer = provider.getTracer(SCOPE_NAME, version);
  hooks = new AmqpHooks(tracer, String(resource.attributes[SERVICE_INSTANCE_ID]), config, setMessageId);
  patch(lib);
  state = { provider, lib };
  addExitHook();
  return provider;
}

function uninstallSync() {
  const previous = state;
  hooks = null;
  state = null;
  if (!previous) return undefined;
  unpatch(previous.lib);
  return previous.provider.shutdown().catch((e) => debug('provider shutdown', e));
}

/** Remove the hooks, then flush and shut down the Trailbeam tracer provider. Never rejects. */
async function uninstall() {
  await uninstallSync();
}

/** Flush spans waiting to be exported. Never rejects. */
async function forceFlush() {
  if (state) await state.provider.forceFlush().catch((e) => debug('flush', e));
}

module.exports = {
  install,
  uninstall,
  forceFlush,
  ENV_SET_MESSAGE_ID,
  // for tests and other AMQP 0-9-1 clients' hooks
  AmqpHooks,
  ChannelState,
  receiveCount,
  deadLetterAttrs,
  amqpContentAttrs,
  exchangeName,
  MAX_TEXT_BYTES,
};
