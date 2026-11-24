'use strict';
// The amqplib hooks against a real RabbitMQ (contract/span-contract-amqp.md): the scenarios of the Python pika
// hooks' tests (python/tests/test_pika_rabbitmq.py), played with amqplib's promise API and, in one, its
// callback API.
//
// Needs a broker: TRAILBEAM_TEST_AMQP_URL, such as amqp://trailbeam:trailbeam@127.0.0.1:5672/%2F (CI runs rabbitmq:4).
// Skipped without one. Every test declares queues of its own, so tests can share one broker.
// With TRAILBEAM_TEST_SERVER_PYTHON (a Python with the trailbeam package), each test's spans also go through
// the server's ingest (A12) and the messages' timelines are read back from a Trailbeam store, as the Python
// tests do (test/amqp_server_check.py).

const test = require('node:test');
const assert = require('node:assert');
const crypto = require('node:crypto');
const path = require('node:path');
const { execFileSync } = require('node:child_process');
const { hrTimeToMilliseconds } = require('@opentelemetry/core');

const amqplibHooks = require('../src/amqplib');
const { memoryExporter } = require('./helpers');

const URL = process.env.TRAILBEAM_TEST_AMQP_URL;
const PYTHON = process.env.TRAILBEAM_TEST_SERVER_PYTHON;
const skip = URL ? false : 'no RabbitMQ: set TRAILBEAM_TEST_AMQP_URL';

const amqp = URL ? require('amqplib') : undefined;
const callbackApi = URL ? require('amqplib/callback_api') : undefined;

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function waitFor(cond, timeout = 10000) {
  const end = Date.now() + timeout;
  while (Date.now() < end) {
    if (cond()) return true;
    await sleep(20);
  }
  return cond();
}

const a = (span) => span.attributes;

/** The finished spans, optionally only those with these names. */
function spansOf(exporter, ...names) {
  return exporter.getFinishedSpans().filter((s) => !names.length || names.includes(s.name));
}

// -- the server's view (optional) -------------------------------------------------------------------

function otlpValue(v) {
  if (typeof v === 'boolean') return { boolValue: v };
  if (Number.isInteger(v)) return { intValue: String(v) };
  if (typeof v === 'number') return { doubleValue: v };
  return { stringValue: String(v) };
}

const nanos = (t) => String(BigInt(t[0]) * 1000000000n + BigInt(t[1]));

function otlp(spans) {
  return {
    resourceSpans: spans.map((s) => ({
      resource: { attributes: Object.entries(s.resource.attributes).map(([key, v]) => ({ key, value: otlpValue(v) })) },
      scopeSpans: [{
        scope: { name: 'trailbeam' },
        spans: [{
          traceId: s.spanContext().traceId,
          spanId: s.spanContext().spanId,
          name: s.name,
          startTimeUnixNano: nanos(s.startTime),
          endTimeUnixNano: nanos(s.endTime),
          attributes: Object.entries(s.attributes).map(([key, v]) => ({ key, value: otlpValue(v) })),
        }],
      }],
    })),
  };
}

/** Every span passes the server's ingest; returns each message's timeline (event types), or undefined
 * without TRAILBEAM_TEST_SERVER_PYTHON. */
function serverView(spans, ...messageIds) {
  if (!PYTHON) return undefined;
  const out = JSON.parse(execFileSync(PYTHON, [path.join(__dirname, 'amqp_server_check.py')], {
    input: JSON.stringify({ otlp: otlp(spans), message_ids: messageIds }),
    encoding: 'utf8',
  }));
  assert.deepStrictEqual(out.rejected, [], 'the server rejects spans');
  return out.timelines;
}

function assertTimeline(spans, messageId, expected) {
  const t = serverView(spans, messageId);
  if (t) assert.deepStrictEqual(t[messageId], expected);
}

// -- the broker ----------------------------------------------------------------------------------------

function namer() {
  const made = [];
  const run = crypto.randomUUID().slice(0, 8);
  const name = (base) => {
    const n = `tb-${base}-${run}`;
    made.push(n);
    return n;
  };
  name.cleanup = async () => {
    try {
      const c = await amqp.connect(URL);
      for (const n of made) {
        for (const what of ['deleteQueue', 'deleteExchange']) {
          const ch = await c.createChannel();
          ch.on('error', () => {});
          try {
            await ch[what](n);
            await ch.close();
          } catch (e) {
            // not there, or the channel closed
          }
        }
      }
      await c.close();
    } catch (e) {
      // the broker is gone: nothing to clean
    }
  };
  return name;
}

async function connect() {
  const c = await amqp.connect(URL);
  c.on('error', () => {});
  return c;
}

const quorum = (ch, name, args = {}) => ch.assertQueue(name, { durable: true, arguments: { 'x-queue-type': 'quorum', ...args } });

async function publish(ch, routingKey, messageId, { exchange = '', body = '{"order": 1}', ...options } = {}) {
  ch.publish(exchange, routingKey, Buffer.from(body), { messageId, persistent: true, ...options });
  if (typeof ch.waitForConfirms === 'function') await ch.waitForConfirms();
}

/** Consume until n deliveries arrived; returns { consumerTag, got }, not settled. */
async function take(ch, queue, n = 1, options = {}) {
  const got = [];
  const { consumerTag } = await ch.consume(queue, (m) => { if (m) got.push(m); }, options);
  assert.ok(await waitFor(() => got.length >= n), `${got.length} of ${n} deliveries from ${queue}`);
  return { consumerTag, got };
}

// -- the scenarios ---------------------------------------------------------------------------------

test.describe('amqplib hooks on RabbitMQ', { skip }, () => {
  let exporter;
  let names;
  test.beforeEach(() => {
    const m = memoryExporter();
    exporter = m.exporter;
    amqplibHooks.install({ spanProcessor: m.spanProcessor });
    names = namer();
  });
  test.afterEach(async () => {
    await amqplibHooks.uninstall();
    await names.cleanup();
  });

  test('publish, deliver, ack', async () => {
    const q = names('happy');
    const conn = await connect();
    const ch = await conn.createConfirmChannel();
    await quorum(ch, q);
    await publish(ch, q, 'm-happy');
    const { consumerTag, got } = await take(ch, q);
    ch.ack(got[0]);
    await ch.cancel(consumerTag);
    await conn.close();

    const [pub] = spansOf(exporter, 'trailbeam publish');
    assert.strictEqual(a(pub)['trailbeam.amqp.confirmed'], true);
    assert.strictEqual(a(pub)['trailbeam.result'], 'ok');
    assert.strictEqual(a(pub)['messaging.destination.name'], 'amq.default');
    assert.strictEqual(a(pub)['rabbitmq.destination.routing_key'], q);
    assert.strictEqual(a(pub)['messaging.message.body.size'], Buffer.byteLength('{"order": 1}'));
    assert.strictEqual(a(pub)['trailbeam.body.capture_status'], 'off');
    assert.strictEqual(a(pub)['trailbeam.amqp.delivery_mode'], 2);
    const delivers = spansOf(exporter, 'trailbeam deliver');
    assert.strictEqual(delivers.length, 1);
    const d = a(delivers[0]);
    assert.strictEqual(d['messaging.destination.name'], q);
    assert.strictEqual(d['messaging.operation.name'], 'basic.deliver');
    assert.strictEqual(d['trailbeam.receive_count'], 1);
    assert.strictEqual(d['trailbeam.receive_count.source'], 'first_delivery');
    assert.strictEqual(d['trailbeam.start_observed'], false);
    assert.strictEqual(hrTimeToMilliseconds(delivers[0].startTime), hrTimeToMilliseconds(delivers[0].endTime));
    assert.strictEqual(d['trailbeam.amqp.vhost'], '/');
    assert.ok(d['server.address']);
    assert.ok(Number.isInteger(d['server.port']));
    const [ack] = spansOf(exporter, 'trailbeam ack');
    assert.strictEqual(a(ack)['trailbeam.amqp.channel.id'], d['trailbeam.amqp.channel.id']);
    assert.strictEqual(a(ack)['rabbitmq.message.delivery_tag'], d['rabbitmq.message.delivery_tag']);
    assert.strictEqual(a(ack)['trailbeam.result'], 'ok');
    assert.strictEqual(a(ack)['messaging.message.id'], 'm-happy');
    // the publish's interval contains the broker's confirm, so it ends before the delivery
    assert.ok(a(pub)['trailbeam.order.returned'] < d['trailbeam.order.returned']);
    const [cancel] = spansOf(exporter, 'trailbeam cancel');
    assert.strictEqual(a(cancel)['trailbeam.amqp.cancelled_by'], 'client');
    assert.strictEqual(a(cancel)['trailbeam.result'], 'ok');
    assert.strictEqual(a(cancel)['trailbeam.amqp.unsettled_count'], 0);
    assert.deepStrictEqual(spansOf(exporter, 'trailbeam channel_closed'), []); // nothing was open at the close

    assertTimeline(spansOf(exporter), 'm-happy', ['sent', 'routed', 'received', 'acked']);
  });

  test('nack with requeue, then ack: the count comes from x-acquired-count', async () => {
    const q = names('requeue');
    const conn = await connect();
    const ch = await conn.createConfirmChannel();
    await quorum(ch, q);
    await publish(ch, q, 'm-requeue');
    const seen = [];
    await ch.consume(q, (m) => {
      if (!m) return;
      seen.push(m);
      if (seen.length === 1) ch.nack(m, false, true);
      else ch.ack(m);
    });
    assert.ok(await waitFor(() => seen.length >= 2));
    await conn.close();

    const [d1, d2] = spansOf(exporter, 'trailbeam deliver').map(a);
    assert.strictEqual(d1['trailbeam.receive_count'], 1);
    assert.strictEqual(d1['trailbeam.amqp.redelivered'], false);
    assert.strictEqual(d2['trailbeam.amqp.redelivered'], true);
    assert.strictEqual(d2['trailbeam.receive_count.source'], 'x-acquired-count');
    assert.strictEqual(d2['trailbeam.receive_count'], 2);
    assert.strictEqual(d2['trailbeam.amqp.acquired_count'], 1);
    const [nack] = spansOf(exporter, 'trailbeam nack').map(a);
    assert.strictEqual(nack['trailbeam.amqp.requeue'], true);
    assert.strictEqual(nack['trailbeam.amqp.multiple'], false);
    assertTimeline(spansOf(exporter), 'm-requeue', ['sent', 'routed', 'received', 'requeued', 'returned_to_queue', 'received', 'acked']);
  });

  test('reject to a dead-letter queue reports x-death', async () => {
    const q = names('orders');
    const dlq = names('orders-dlq');
    const dlx = names('dlx');
    const conn = await connect();
    const ch = await conn.createConfirmChannel();
    await ch.assertExchange(dlx, 'fanout');
    await quorum(ch, dlq);
    await ch.bindQueue(dlq, dlx, '');
    await quorum(ch, q, { 'x-dead-letter-exchange': dlx });
    await publish(ch, q, 'm-dead');
    const first = await take(ch, q);
    ch.reject(first.got[0], false);
    await ch.cancel(first.consumerTag);
    const second = await take(ch, dlq);
    ch.ack(second.got[0]);
    await conn.close();

    const [reject] = spansOf(exporter, 'trailbeam reject').map(a);
    assert.strictEqual(reject['trailbeam.amqp.requeue'], false);
    assert.ok(!('trailbeam.amqp.multiple' in reject));
    const d = spansOf(exporter, 'trailbeam deliver').map(a).find((x) => x['messaging.destination.name'] === dlq);
    assert.strictEqual(d['trailbeam.dead_letter.source_queue'], q);
    assert.strictEqual(d['trailbeam.dead_letter.reason'], 'rejected');
    assert.strictEqual(d['trailbeam.dead_letter.count'], 1);
    assert.strictEqual(d['trailbeam.dead_letter.exchange'], 'amq.default');
    assert.ok(Math.abs(d['trailbeam.dead_letter.time_ms'] - Date.now()) < 120000, `${d['trailbeam.dead_letter.time_ms']}`);
    assert.strictEqual(d['trailbeam.dead_letter.first_queue'], q);
    assert.strictEqual(d['trailbeam.amqp.exchange'], dlx);
    assertTimeline(spansOf(exporter), 'm-dead', ['sent', 'routed', 'received', 'rejected', 'moved_to_dead_letter_queue', 'received', 'acked']);
  });

  test('a mandatory publish no queue takes: a return, then an ok publish', async () => {
    const conn = await connect();
    const ch = await conn.createConfirmChannel();
    const returns = [];
    ch.on('return', (m) => returns.push(m));
    await publish(ch, `tb-nobody-${crypto.randomUUID()}`, 'm-unroutable', { mandatory: true });
    await conn.close();
    assert.strictEqual(returns.length, 1); // the application's listener still gets it
    const spans = spansOf(exporter, 'trailbeam return', 'trailbeam publish');
    assert.deepStrictEqual(spans.map((s) => s.name), ['trailbeam return', 'trailbeam publish']);
    const [ret, pub] = spans.map(a);
    assert.strictEqual(ret['trailbeam.amqp.reply_code'], 312);
    assert.strictEqual(ret['trailbeam.amqp.reply_text'], 'NO_ROUTE');
    assert.strictEqual(ret['messaging.message.id'], 'm-unroutable');
    assert.strictEqual(pub['trailbeam.result'], 'ok'); // A4.1: the broker acked the publish it returned
    assert.strictEqual(pub['trailbeam.amqp.mandatory'], true);
    serverView(spans);
  });

  test('a publish to a missing exchange fails with the reply-code name', async () => {
    const conn = await connect();
    const ch = await conn.createConfirmChannel();
    ch.on('error', () => {});
    const confirmed = new Promise((resolve) => {
      ch.publish(`tb-missing-${crypto.randomUUID()}`, 'k', Buffer.from('x'), { messageId: 'm-nope' }, resolve);
    });
    const err = await confirmed;
    assert.ok(err instanceof Error); // the application's callback still learns of it
    await conn.close();
    const [pub] = spansOf(exporter, 'trailbeam publish').map(a);
    assert.strictEqual(pub['trailbeam.result'], 'failed');
    assert.strictEqual(pub['trailbeam.error_code'], 'NOT_FOUND');
    serverView(spansOf(exporter));
  });

  test('a publish without confirms and without a message ID', async () => {
    const q = names('noid');
    const conn = await connect();
    const ch = await conn.createChannel();
    await quorum(ch, q);
    ch.sendToQueue(q, Buffer.from('plain'));
    const { got } = await take(ch, q);
    ch.ack(got[0]);
    await conn.close();
    const [pub] = spansOf(exporter, 'trailbeam publish').map(a);
    assert.strictEqual(pub['trailbeam.amqp.confirmed'], false);
    assert.strictEqual(pub['trailbeam.result'], 'ok');
    assert.ok(!('messaging.message.id' in pub));
    const [d] = spansOf(exporter, 'trailbeam deliver').map(a);
    assert.ok(!('messaging.message.id' in d));
    serverView(spansOf(exporter));
  });

  test('get is the interval of the call, and an empty get gives no span', async () => {
    const q = names('pull');
    const conn = await connect();
    const ch = await conn.createConfirmChannel();
    await quorum(ch, q);
    await publish(ch, q, 'm-pull');
    let m = false;
    const end = Date.now() + 10000;
    while (!m && Date.now() < end) m = await ch.get(q);
    assert.ok(m);
    ch.ack(m);
    assert.strictEqual(await ch.get(q), false);
    await conn.close();
    const gets = spansOf(exporter, 'trailbeam deliver').map(a);
    assert.strictEqual(gets.length, 1);
    const g = gets[0];
    assert.strictEqual(g['messaging.operation.name'], 'basic.get');
    assert.ok(!('trailbeam.start_observed' in g));
    assert.ok(g['trailbeam.order.issued'] < g['trailbeam.order.returned']);
    assert.ok(!('trailbeam.amqp.consumer_tag' in g));
    const [ack] = spansOf(exporter, 'trailbeam ack').map(a);
    assert.strictEqual(ack['rabbitmq.message.delivery_tag'], g['rabbitmq.message.delivery_tag']);
    serverView(spansOf(exporter));
  });

  test('ack with allUpTo covers every open delivery', async () => {
    const q = names('multi');
    const conn = await connect();
    const ch = await conn.createConfirmChannel();
    await quorum(ch, q);
    for (let i = 0; i < 3; i += 1) await publish(ch, q, `m-multi-${i}`);
    const { got } = await take(ch, q, 3);
    ch.ack(got[2], true);
    await conn.close();
    const acks = spansOf(exporter, 'trailbeam ack');
    assert.deepStrictEqual(acks.map((s) => a(s)['messaging.message.id']).sort(), ['m-multi-0', 'm-multi-1', 'm-multi-2']);
    assert.strictEqual(new Set(acks.map((s) => `${hrTimeToMilliseconds(s.startTime)} ${hrTimeToMilliseconds(s.endTime)}`)).size, 1);
    assert.ok(acks.every((s) => a(s)['trailbeam.amqp.multiple'] === true));
  });

  test('the client closes the connection with a delivery open', async () => {
    const q = names('open');
    const conn = await connect();
    const ch = await conn.createConfirmChannel();
    await quorum(ch, q);
    await publish(ch, q, 'm-open');
    await take(ch, q);
    await conn.close();
    const closed = spansOf(exporter, 'trailbeam channel_closed').map(a);
    assert.strictEqual(closed.length, 1);
    const [c] = closed;
    assert.strictEqual(c['messaging.operation.name'], 'connection.close');
    assert.strictEqual(c['trailbeam.amqp.close.initiator'], 'client');
    assert.strictEqual(c['messaging.message.id'], 'm-open');
    assert.ok(!('trailbeam.amqp.close.reply_code' in c));
    serverView(spansOf(exporter, 'trailbeam channel_closed'));
  });

  test('the client closes the channel with a delivery open', async () => {
    const q = names('chclose');
    const conn = await connect();
    const ch = await conn.createConfirmChannel();
    await quorum(ch, q);
    await publish(ch, q, 'm-chclose');
    await take(ch, q);
    await ch.close();
    await conn.close();
    const [c] = spansOf(exporter, 'trailbeam channel_closed').map(a);
    assert.strictEqual(c['messaging.operation.name'], 'channel.close');
    assert.strictEqual(c['trailbeam.amqp.close.initiator'], 'client');
  });

  test('the broker closes the channel after a settle it refused', async () => {
    // A4.4: an ack of an unknown tag (here one already acked) gets PRECONDITION_FAILED; the deliveries still open
    // on the channel are reported closed by the broker
    const q = names('refused');
    const conn = await connect();
    const ch = await conn.createConfirmChannel();
    const errors = [];
    ch.on('error', (e) => errors.push(e));
    let closed = false;
    ch.on('close', () => { closed = true; });
    await quorum(ch, q);
    await publish(ch, q, 'm-refused-1');
    await publish(ch, q, 'm-refused-2');
    const { got } = await take(ch, q, 2);
    ch.ack(got[0]);
    ch.ack(got[0]); // not recorded any more: no span; the broker closes the channel
    assert.ok(await waitFor(() => closed));
    await conn.close();
    assert.strictEqual(errors.length, 1);
    assert.strictEqual(spansOf(exporter, 'trailbeam ack').length, 1);
    const [c] = spansOf(exporter, 'trailbeam channel_closed').map(a);
    assert.strictEqual(c['messaging.message.id'], 'm-refused-2');
    assert.strictEqual(c['trailbeam.amqp.close.initiator'], 'broker');
    assert.strictEqual(c['messaging.operation.name'], 'channel.close');
    assert.strictEqual(c['trailbeam.amqp.close.reply_code'], 406);
    assert.ok(c['trailbeam.amqp.close.reply_text'].startsWith('PRECONDITION_FAILED - unknown delivery tag'), c['trailbeam.amqp.close.reply_text']);
    serverView(spansOf(exporter, 'trailbeam channel_closed'));
  });

  test('the connection is lost with a delivery open', async () => {
    const q = names('lost');
    const conn = await connect();
    let gone = false;
    conn.on('close', () => { gone = true; });
    const ch = await conn.createConfirmChannel();
    await quorum(ch, q);
    await publish(ch, q, 'm-lost');
    await take(ch, q);
    conn.connection.stream.destroy(Object.assign(new Error('read ECONNRESET'), { code: 'ECONNRESET' })); // the network goes away
    assert.ok(await waitFor(() => gone));
    const [c] = spansOf(exporter, 'trailbeam channel_closed').map(a);
    assert.strictEqual(c['messaging.operation.name'], 'connection.lost');
    assert.strictEqual(c['trailbeam.amqp.close.initiator'], 'network');
    assert.ok(!('trailbeam.amqp.close.reply_code' in c));
    // and the broker requeued it: the next delivery counts it
    const conn2 = await connect();
    const ch2 = await conn2.createChannel();
    const { got } = await take(ch2, q);
    ch2.ack(got[0]);
    await conn2.close();
    const d2 = a(spansOf(exporter, 'trailbeam deliver').at(-1));
    assert.strictEqual(d2['trailbeam.amqp.redelivered'], true);
    assert.strictEqual(d2['trailbeam.receive_count'], 2);
    const t = serverView(spansOf(exporter), 'm-lost');
    if (t) {
      assert.strictEqual(t['m-lost'].filter((e) => e === 'received').length, 2, t['m-lost']);
      assert.ok(t['m-lost'].includes('channel_closed'), t['m-lost']);
    }
  });

  test('the broker cancels the consumer when its queue is deleted', async () => {
    const q = names('gone');
    const conn = await connect();
    const ch = await conn.createConfirmChannel();
    await quorum(ch, q);
    await publish(ch, q, 'm-gone');
    const { consumerTag } = await take(ch, q);
    const other = await connect();
    const och = await other.createChannel();
    await och.deleteQueue(q);
    await other.close();
    assert.ok(await waitFor(() => spansOf(exporter, 'trailbeam cancel').length > 0));
    await conn.close();
    const [c] = spansOf(exporter, 'trailbeam cancel').map(a);
    assert.strictEqual(c['trailbeam.amqp.cancelled_by'], 'broker');
    assert.strictEqual(c['trailbeam.amqp.consumer_tag'], consumerTag);
    assert.strictEqual(c['trailbeam.amqp.unsettled_count'], 1);
    assert.strictEqual(c['trailbeam.start_observed'], false);
    assert.ok(!('trailbeam.result' in c));
    serverView(spansOf(exporter, 'trailbeam cancel'));
  });

  test('a fanout delivers a copy to each queue', async () => {
    const fan = names('fan');
    const qa = names('fan-a');
    const qb = names('fan-b');
    const conn = await connect();
    const ch = await conn.createConfirmChannel();
    await ch.assertExchange(fan, 'fanout');
    for (const q of [qa, qb]) {
      await quorum(ch, q);
      await ch.bindQueue(q, fan, '');
    }
    await publish(ch, '', 'm-fan', { exchange: fan });
    for (const q of [qa, qb]) {
      const { consumerTag, got } = await take(ch, q);
      ch.ack(got[0]);
      await ch.cancel(consumerTag);
    }
    await conn.close();
    const [pub] = spansOf(exporter, 'trailbeam publish').map(a);
    assert.strictEqual(pub['messaging.destination.name'], fan);
    assert.ok(!('rabbitmq.destination.routing_key' in pub));
    assert.deepStrictEqual(spansOf(exporter, 'trailbeam deliver').map((s) => a(s)['messaging.destination.name']).sort(), [qa, qb].sort());
    const t = serverView(spansOf(exporter), 'm-fan');
    if (t) {
      assert.strictEqual(t['m-fan'].filter((e) => e === 'received').length, 2, t['m-fan']);
      assert.strictEqual(t['m-fan'].filter((e) => e === 'acked').length, 2, t['m-fan']);
    }
  });

  test('a classic queue redelivery has no count', async () => {
    const q = names('classic');
    const conn = await connect();
    const ch = await conn.createConfirmChannel();
    await ch.assertQueue(q, { durable: true, arguments: { 'x-queue-type': 'classic' } });
    await publish(ch, q, 'm-classic');
    const seen = [];
    await ch.consume(q, (m) => {
      if (!m) return;
      seen.push(m);
      if (seen.length === 1) ch.nack(m);
      else ch.ack(m);
    });
    assert.ok(await waitFor(() => seen.length >= 2));
    await conn.close();
    const [d1, d2] = spansOf(exporter, 'trailbeam deliver').map(a);
    assert.strictEqual(d1['trailbeam.receive_count'], 1);
    assert.ok(!('trailbeam.receive_count' in d2));
    assert.strictEqual(d2['trailbeam.amqp.redelivered'], true);
    serverView(spansOf(exporter));
  });

  test('noAck deliveries are not left open', async () => {
    const q = names('autoack');
    const conn = await connect();
    const ch = await conn.createConfirmChannel();
    await quorum(ch, q);
    await publish(ch, q, 'm-auto');
    await take(ch, q, 1, { noAck: true });
    await conn.close();
    assert.strictEqual(spansOf(exporter, 'trailbeam deliver').length, 1);
    assert.deepStrictEqual(spansOf(exporter, 'trailbeam channel_closed'), []);
  });

  test('the callback API is followed too', async () => {
    const q = names('callback');
    const conn = await new Promise((resolve, reject) => callbackApi.connect(URL, (err, c) => (err ? reject(err) : resolve(c))));
    conn.on('error', () => {});
    const ch = await new Promise((resolve, reject) => conn.createConfirmChannel((err, c) => (err ? reject(err) : resolve(c))));
    await new Promise((resolve, reject) => ch.assertQueue(q, { durable: true, arguments: { 'x-queue-type': 'quorum' } }, (err) => (err ? reject(err) : resolve())));
    await new Promise((resolve, reject) => ch.sendToQueue(q, Buffer.from('{"n": 1}'), { messageId: 'm-callback' }, (err) => (err ? reject(err) : resolve())));
    const got = [];
    ch.consume(q, (m) => { if (m) got.push(m); });
    assert.ok(await waitFor(() => got.length === 1));
    ch.ack(got[0]);
    await new Promise((resolve) => conn.close(resolve));
    assert.deepStrictEqual(spansOf(exporter).map((s) => s.name), ['trailbeam publish', 'trailbeam deliver', 'trailbeam ack']);
    assertTimeline(spansOf(exporter), 'm-callback', ['sent', 'routed', 'received', 'acked']);
  });
});

test.describe('amqplib hooks on RabbitMQ: options', { skip }, () => {
  let names;
  test.beforeEach(() => { names = namer(); });
  test.afterEach(async () => {
    await amqplibHooks.uninstall();
    await names.cleanup();
  });

  test('the tenant header and body capture', async () => {
    const m = memoryExporter();
    const queue = names('tenant');
    amqplibHooks.install({ spanProcessor: m.spanProcessor, tenant: { messageAttribute: 'tenant' }, capture: { bodyQueues: [queue] } });
    const conn = await connect();
    const ch = await conn.createConfirmChannel();
    await quorum(ch, queue);
    await publish(ch, queue, 'm-tenant', { headers: { tenant: 'acme' }, body: '{"card": "4111 1111 1111 1111", "password": "hunter2"}' });
    const { got } = await take(ch, queue);
    ch.ack(got[0]);
    await conn.close();
    const [pub] = m.exporter.getFinishedSpans().filter((s) => s.name === 'trailbeam publish').map(a);
    assert.strictEqual(pub['trailbeam.tenant'], 'acme');
    assert.strictEqual(pub['trailbeam.tenant.source'], 'header');
    assert.strictEqual(pub['trailbeam.body.capture_status'], 'off'); // the list names queues; a publish is to an exchange
    const [d] = m.exporter.getFinishedSpans().filter((s) => s.name === 'trailbeam deliver').map(a);
    assert.strictEqual(d['trailbeam.tenant'], 'acme');
    assert.strictEqual(d['trailbeam.body.capture_status'], 'captured');
    assert.ok(!d['trailbeam.body'].includes('hunter2') && !d['trailbeam.body'].includes('4111'), d['trailbeam.body']);
    assert.ok(d['trailbeam.body.redacted_count'] >= 2);
    serverView(m.exporter.getFinishedSpans());
  });

  test('the setMessageId option', async () => {
    const m = memoryExporter();
    amqplibHooks.install({ spanProcessor: m.spanProcessor, setMessageId: true });
    const q = names('setid');
    const conn = await connect();
    const ch = await conn.createConfirmChannel();
    await quorum(ch, q);
    const options = {};
    ch.sendToQueue(q, Buffer.from('x'), options);
    await ch.waitForConfirms();
    assert.deepStrictEqual(options, {});
    const { got } = await take(ch, q);
    ch.ack(got[0]);
    await conn.close();
    const [pub] = m.exporter.getFinishedSpans().filter((s) => s.name === 'trailbeam publish').map(a);
    assert.strictEqual(pub['trailbeam.amqp.message_id.source'], 'package');
    assert.strictEqual(got[0].properties.messageId, pub['messaging.message.id']); // the broker got the ID
    const [d] = m.exporter.getFinishedSpans().filter((s) => s.name === 'trailbeam deliver').map(a);
    assert.strictEqual(d['messaging.message.id'], pub['messaging.message.id']);
  });

  test('uninstall stops the spans', async () => {
    const ended = [];
    // a processor of its own: an in-memory exporter forgets its spans when uninstall shuts it down
    amqplibHooks.install({ spanProcessor: { onStart() {}, onEnd: (s) => ended.push(s.name), forceFlush: async () => {}, shutdown: async () => {} } });
    const q = names('off');
    const conn = await connect();
    const ch = await conn.createConfirmChannel();
    await quorum(ch, q);
    await amqplibHooks.uninstall();
    await publish(ch, q, 'm-off');
    const { got } = await take(ch, q);
    ch.ack(got[0]);
    await conn.close();
    assert.deepStrictEqual(ended, []);
  });
});
