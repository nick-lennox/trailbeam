'use strict';
// The amqplib hooks against the contract's RabbitMQ vectors (contract/span-contract-amqp-vectors.json, A13),
// the same vectors the Python pika hooks run (python/tests/test_pika_vectors.py).
//
// Each `emit` case is played on amqplib's own channel classes over a connection to nowhere
// (test/amqplib-fake.js): the application's calls go through amqplib's methods, the broker's frames through
// the handler amqplib uses for them.

const test = require('node:test');
const assert = require('node:assert');
const fs = require('node:fs');
const path = require('node:path');
const { hrTimeToMilliseconds } = require('@opentelemetry/core');

const amqplibHooks = require('../src/amqplib');
const fake = require('./amqplib-fake');
const { memoryExporter } = require('./helpers');

const VECTORS = JSON.parse(fs.readFileSync(path.join(__dirname, '..', '..', 'contract', 'span-contract-amqp-vectors.json'), 'utf8'));
const PRESENT = VECTORS.emit_present;
const PRESENT_ON_CHANNEL_SPANS = VECTORS.emit_present_on_channel_spans;
const NOT_CHANNEL_SPANS = new Set(['trailbeam publish', 'trailbeam return']);

// trailbeam.error_code is the class name of what the library raised: the vectors name pika's, amqplib raises its own
const RAISED = { ChannelWrongStateError: 'IllegalOperationError' };

// A vector's {"bytes": hex} as amqplib decodes it (A3): toString('utf8'), each bad byte becoming U+FFFD
function wire(v) {
  if (v && typeof v === 'object' && !Array.isArray(v)) {
    const keys = Object.keys(v);
    if (keys.length === 1 && keys[0] === 'bytes') return Buffer.from(v.bytes, 'hex').toString('utf8');
    return Object.fromEntries(Object.entries(v).map(([k, x]) => [k, wire(x)]));
  }
  return Array.isArray(v) ? v.map(wire) : v;
}

function props(properties = {}, headers) {
  const p = {};
  if (properties.message_id) p.messageId = properties.message_id;
  if (properties.delivery_mode !== undefined) p.deliveryMode = properties.delivery_mode;
  if (properties.timestamp !== undefined) p.timestamp = properties.timestamp;
  if (headers !== undefined) p.headers = headers;
  return p;
}

async function record(ch, recorded) {
  const tags = new Set();
  for (const r of recorded) {
    if (!tags.has(r.consumer_tag) && !ch.consumers.has(r.consumer_tag)) {
      await fake.consume(ch, r.queue, r.consumer_tag);
      tags.add(r.consumer_tag);
    }
    fake.deliver(ch, r.consumer_tag, r.delivery_tag, { routingKey: r.queue, properties: props(r.message_id ? { message_id: r.message_id } : {}) });
  }
}

async function play(event, exporter) {
  event = wire(event);
  const ctx = VECTORS.context;
  const conn = new fake.FakeConnection({ host: ctx['server.address'], port: ctx['server.port'] });
  const confirmMode = event.call === 'basic.publish' && event.confirm_mode;
  const ch = await fake.openChannel(conn, { confirm: confirmMode });
  const c = event.consumer;
  if (c && !ch.consumers.has(c.consumer_tag)) await fake.consume(ch, c.queue, c.consumer_tag);
  await record(ch, event.recorded || []);
  exporter.reset();
  const broker = event.broker || {};
  const { call } = event;
  const raised = event.client && typeof event.client === 'object' ? event.client.raised : undefined;

  if (call === 'basic.publish') {
    if (raised) conn.failWrites = new fake.IllegalOperationError('Channel closed', '');
    const p = event.properties;
    const options = { mandatory: event.mandatory };
    if (p.message_id) options.messageId = p.message_id;
    if (p.delivery_mode !== undefined) options.deliveryMode = p.delivery_mode;
    try {
      ch.publish(event.exchange, event.routing_key, Buffer.from('{"n": 1}'), options, () => {});
    } catch (e) {
      // the vector's library raised; the span says so
    }
    if (broker['basic.return']) {
      const r = broker['basic.return'];
      fake.returned(ch, r.reply_code, r.reply_text, event.exchange, event.routing_key, props(p));
    }
    if (broker.confirm) fake.confirm(ch, broker.confirm === 'ack', 1);
    if (broker['channel.close']) fake.brokerClosesChannel(ch, broker['channel.close'].reply_code, broker['channel.close'].reply_text);
  } else if (event.method === 'basic.deliver') {
    await fake.consume(ch, event.queue, event.consumer_tag);
    exporter.reset();
    fake.deliver(ch, event.consumer_tag, event.delivery_tag, {
      redelivered: event.redelivered, exchange: event.exchange, routingKey: event.routing_key, properties: props(event.properties, event.headers),
    });
  } else if (event.method === 'basic.get') {
    if (event.empty) {
      assert.strictEqual(await fake.getEmpty(ch, event.queue), false);
    } else {
      await fake.getOk(ch, event.queue, event.delivery_tag, {
        redelivered: event.redelivered, exchange: event.exchange, routingKey: event.routing_key, properties: props(event.properties, event.headers),
      });
    }
  } else if (['basic.ack', 'basic.nack', 'basic.reject'].includes(call)) {
    if (raised) {
      assert.strictEqual(raised, 'ChannelWrongStateError'); // the channel went away under the application
      fake.channelGone(ch);
    }
    const m = { fields: { deliveryTag: event.delivery_tag } };
    try {
      if (call === 'basic.ack') ch.ack(m, event.multiple);
      else if (call === 'basic.nack') ch.nack(m, event.multiple, event.requeue);
      else ch.reject(m, event.requeue);
    } catch (e) {
      assert.ok(e instanceof fake.IllegalOperationError, `${e}`);
    }
  } else if (call === 'basic.cancel') {
    const cancelled = ch.cancel(c.consumer_tag);
    if (broker['basic.cancel-ok']) fake.reply(ch, fake.defs.BasicCancelOk, { consumerTag: c.consumer_tag });
    await cancelled;
  } else if (broker['basic.cancel']) {
    fake.brokerCancels(ch, broker['basic.cancel'].consumer_tag);
  } else if (broker['channel.close']) {
    fake.brokerClosesChannel(ch, broker['channel.close'].reply_code, broker['channel.close'].reply_text);
  } else if (event.network) {
    fake.connectionLost(conn);
  } else if (call === 'connection.close') {
    fake.clientClosesConnection(conn);
  } else if (call === 'channel.close') {
    await fake.clientClosesChannel(ch);
  } else {
    assert.fail(`vector event not played: ${JSON.stringify(event)}`);
  }
  return exporter.getFinishedSpans();
}

const same = (got, want) => got === want && typeof got === typeof want;

test.describe('the contract emit vectors (A13)', () => {
  let exporter;
  test.beforeEach(() => {
    const m = memoryExporter();
    exporter = m.exporter;
    amqplibHooks.install({ spanProcessor: m.spanProcessor });
  });
  test.afterEach(() => amqplibHooks.uninstall());

  for (const c of VECTORS.emit) {
    test(c.name, async () => {
      const spans = await play(c.event, exporter);
      assert.deepStrictEqual(spans.map((s) => s.name), c.spans.map((s) => s.span_name));
      const raised = c.event.client && typeof c.event.client === 'object' ? c.event.client.raised : undefined;
      spans.forEach((got, i) => {
        const want = c.spans[i];
        const attrs = got.attributes;
        for (let [k, v] of Object.entries(want.attributes)) {
          if (k === 'trailbeam.error_code' && raised && v === raised) v = RAISED[raised];
          assert.ok(k in attrs, `${got.name}: no ${k}`);
          assert.ok(same(attrs[k], v), `${got.name}: ${k} = ${JSON.stringify(attrs[k])}, not ${JSON.stringify(v)}`);
        }
        for (const k of [...PRESENT, ...(NOT_CHANNEL_SPANS.has(got.name) ? [] : PRESENT_ON_CHANNEL_SPANS)]) {
          assert.ok(k in attrs, `${got.name}: no ${k}`);
        }
        for (const k of want.absent || []) assert.ok(!(k in attrs), `${got.name}: ${k} = ${JSON.stringify(attrs[k])} should be absent`);
        const start = hrTimeToMilliseconds(got.startTime);
        const end = hrTimeToMilliseconds(got.endTime);
        if (attrs['trailbeam.start_observed'] === false) {
          assert.strictEqual(start, end);
          assert.ok(!('trailbeam.order.issued' in attrs));
        } else {
          assert.ok(attrs['trailbeam.order.issued'] < attrs['trailbeam.order.returned']);
          assert.ok(start <= end);
        }
      });
      // spans of one call share their interval and ticks
      const calls = new Set(spans.filter((s) => /(ack|nack|reject)$/.test(s.name))
        .map((s) => `${hrTimeToMilliseconds(s.startTime)} ${hrTimeToMilliseconds(s.endTime)} ${s.attributes['trailbeam.order.returned']}`));
      assert.ok(calls.size <= 1);
    });
  }
});

test.describe('receive count vectors (A7)', () => {
  for (const c of VECTORS.receive_count) {
    test(c.name, () => {
      const [count, source] = amqplibHooks.receiveCount(c.redelivered, c.headers);
      assert.deepStrictEqual([count ?? null, source ?? null], [c.receive_count, c.source]);
    });
  }
});

test.describe('dead-letter vectors (A8)', () => {
  for (const c of VECTORS.dead_letter) {
    test(c.name, () => {
      assert.deepStrictEqual(amqplibHooks.deadLetterAttrs(wire(c['x-death']), wire(c.headers)), c.attributes);
    });
  }
});
