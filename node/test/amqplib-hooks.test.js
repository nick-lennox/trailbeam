'use strict';
// The amqplib hooks on amqplib's own classes over a connection to nowhere (test/amqplib-fake.js): what the
// contract's vectors don't cover. The Python counterpart is python/tests/test_pika_hooks.py.

const test = require('node:test');
const assert = require('node:assert');
const path = require('node:path');

const amqplibHooks = require('../src/amqplib');
const fake = require('./amqplib-fake');
const { memoryExporter } = require('./helpers');

const channelModel = require(path.join(fake.dir, 'lib', 'channel_model.js'));
const channelLib = require(path.join(fake.dir, 'lib', 'channel.js'));

const names = (exporter) => exporter.getFinishedSpans().map((s) => s.name);
const attrsOf = (exporter) => exporter.getFinishedSpans().map((s) => s.attributes);

function installed(options = {}) {
  const m = memoryExporter();
  amqplibHooks.install({ spanProcessor: m.spanProcessor, ...options });
  return m.exporter;
}

test.describe('amqplib hooks', () => {
  let exporter;
  test.beforeEach(() => { exporter = installed(); });
  test.afterEach(() => amqplibHooks.uninstall());

  test('a publish the library fails gets a failed span and the error reaches the application', async () => {
    const conn = new fake.FakeConnection();
    const ch = await fake.openChannel(conn);
    conn.failWrites = new fake.IllegalOperationError('Connection closing', '');
    assert.throws(() => ch.publish('', 'orders', Buffer.from('x'), { messageId: 'm-1' }), fake.IllegalOperationError);
    const [span] = attrsOf(exporter);
    assert.strictEqual(span['trailbeam.result'], 'failed');
    assert.strictEqual(span['trailbeam.error_code'], 'IllegalOperationError');
  });

  test('in confirm mode a failed write does not shift the sequence numbers', async () => {
    const conn = new fake.FakeConnection();
    const ch = await fake.openChannel(conn, { confirm: true });
    conn.failWrites = new fake.IllegalOperationError('Channel closed', '');
    assert.throws(() => ch.sendToQueue('orders', Buffer.from('x'), { messageId: 'm-lost' }));
    conn.failWrites = undefined;
    ch.sendToQueue('orders', Buffer.from('x'), { messageId: 'm-sent' });
    fake.confirm(ch, true, 1); // the broker numbered only the publish it got
    const byId = Object.fromEntries(attrsOf(exporter).map((a) => [a['messaging.message.id'], a]));
    assert.strictEqual(byId['m-lost']['trailbeam.result'], 'failed');
    assert.strictEqual(byId['m-sent']['trailbeam.result'], 'ok');
  });

  test("a multiple confirm ends every publish up to its tag, and the application's callbacks still run", async () => {
    const ch = await fake.openChannel(new fake.FakeConnection(), { confirm: true });
    const results = [];
    for (let i = 0; i < 3; i += 1) ch.publish('', 'orders', Buffer.from('x'), { messageId: `m-${i}` }, (err) => results.push([i, err === null]));
    fake.confirm(ch, true, 2, true);
    assert.deepStrictEqual(attrsOf(exporter).map((a) => a['messaging.message.id']).sort(), ['m-0', 'm-1']);
    fake.confirm(ch, false, 3);
    const last = attrsOf(exporter).at(-1);
    assert.strictEqual(last['messaging.message.id'], 'm-2');
    assert.strictEqual(last['trailbeam.error_code'], 'nack');
    assert.deepStrictEqual(results, [[0, true], [1, true], [2, false]]);
  });

  test('waitForConfirms still resolves, and rejects when the channel closes first', async () => {
    const ch = await fake.openChannel(new fake.FakeConnection(), { confirm: true });
    ch.sendToQueue('orders', Buffer.from('x'));
    const waited = ch.waitForConfirms();
    fake.confirm(ch, true, 1);
    await waited;
    ch.sendToQueue('orders', Buffer.from('x'));
    const lost = ch.waitForConfirms();
    fake.brokerClosesChannel(ch, 404, "NOT_FOUND - no exchange 'nope' in vhost '/'");
    await assert.rejects(lost, /channel closed/);
    assert.deepStrictEqual(attrsOf(exporter).map((a) => a['trailbeam.error_code'] ?? 'ok'), ['ok', 'NOT_FOUND']);
  });

  test("wrong arguments get amqplib's own error, and no span", async () => {
    const ch = await fake.openChannel(new fake.FakeConnection());
    assert.throws(() => ch.ack(undefined), TypeError);
    assert.throws(() => ch.reject({}), TypeError);
    assert.deepStrictEqual(names(exporter), []);
  });

  test('deliveries of an unknown consumer or a server-named queue get no span', async () => {
    const ch = await fake.openChannel(new fake.FakeConnection());
    const got = await fake.consume(ch, '', 'ctag-anon');
    fake.deliver(ch, 'ctag-anon', 1);
    assert.strictEqual(got.length, 1);
    assert.deepStrictEqual(names(exporter), []);
  });

  test('noAck deliveries are not left open', async () => {
    const ch = await fake.openChannel(new fake.FakeConnection());
    await fake.consume(ch, 'orders', 'ctag-1', { noAck: true });
    fake.deliver(ch, 'ctag-1', 1);
    await fake.clientClosesChannel(ch);
    assert.deepStrictEqual(names(exporter), ['trailbeam deliver']);
  });

  test('ackAll and nackAll cover every open delivery', async () => {
    const ch = await fake.openChannel(new fake.FakeConnection());
    await fake.consume(ch, 'orders', 'ctag-1');
    for (const tag of [1, 2]) fake.deliver(ch, 'ctag-1', tag);
    exporter.reset();
    ch.nackAll(false);
    assert.deepStrictEqual(attrsOf(exporter).map((a) => [a['rabbitmq.message.delivery_tag'], a['trailbeam.amqp.requeue'], a['trailbeam.amqp.multiple']]),
      [[1, false, true], [2, false, true]]);
    fake.deliver(ch, 'ctag-1', 3);
    exporter.reset();
    ch.ackAll();
    assert.deepStrictEqual(attrsOf(exporter).map((a) => a['rabbitmq.message.delivery_tag']), [3]);
    exporter.reset();
    ch.ackAll(); // nothing open
    assert.deepStrictEqual(names(exporter), []);
  });

  test('nack and reject requeue unless told not to', async () => {
    const ch = await fake.openChannel(new fake.FakeConnection());
    const got = await fake.consume(ch, 'orders', 'ctag-1');
    for (const tag of [1, 2]) fake.deliver(ch, 'ctag-1', tag);
    exporter.reset();
    ch.nack(got[0]);
    ch.reject(got[1]);
    assert.deepStrictEqual(attrsOf(exporter).map((a) => a['trailbeam.amqp.requeue']), [true, true]);
  });

  test('the delivery record is bounded: the oldest go first', () => {
    const st = new amqplibHooks.ChannelState();
    for (let tag = 1; tag <= 100001; tag += 1) st.record(tag, { queue: 'orders' });
    assert.strictEqual(st.deliveries.size, 100000);
    assert.strictEqual(st.deliveries.keys().next().value, 2);
  });

  test('two gets in flight each get their own span', async () => {
    const ch = await fake.openChannel(new fake.FakeConnection());
    const first = ch.get('orders');
    const second = ch.get('orders'); // amqplib queues it until the first is answered
    fake.reply(ch, fake.defs.BasicGetOk, { deliveryTag: 1, redelivered: false, exchange: '', routingKey: 'orders', messageCount: 1 });
    ch.accept({ id: fake.defs.BasicProperties, channel: ch.ch, fields: { messageId: 'm-1' }, size: 0 });
    fake.reply(ch, fake.defs.BasicGetOk, { deliveryTag: 2, redelivered: false, exchange: '', routingKey: 'orders', messageCount: 0 });
    ch.accept({ id: fake.defs.BasicProperties, channel: ch.ch, fields: { messageId: 'm-2' }, size: 0 });
    const [a, b] = await Promise.all([first, second]);
    assert.deepStrictEqual([a.properties.messageId, b.properties.messageId], ['m-1', 'm-2']);
    const spans = attrsOf(exporter);
    assert.deepStrictEqual(spans.map((s) => [s['messaging.operation.name'], s['messaging.message.id']]), [['basic.get', 'm-1'], ['basic.get', 'm-2']]);
    assert.ok(spans[0]['trailbeam.order.issued'] < spans[1]['trailbeam.order.issued']);
  });

  test('a client cancel waiting for its cancel-ok fails when the channel closes', async () => {
    const ch = await fake.openChannel(new fake.FakeConnection());
    await fake.consume(ch, 'orders', 'ctag-1');
    const cancelled = ch.cancel('ctag-1');
    fake.brokerClosesChannel(ch, 406, 'PRECONDITION_FAILED - unknown delivery tag 9');
    await assert.rejects(cancelled);
    const [span] = attrsOf(exporter);
    assert.strictEqual(span['trailbeam.kind'], 'amqp.cancel');
    assert.strictEqual(span['trailbeam.result'], 'failed');
    assert.strictEqual(span['trailbeam.error_code'], 'PRECONDITION_FAILED');
  });

  test('the broker closing the connection is told apart from a lost one', async () => {
    const conn = new fake.FakeConnection();
    const ch = await fake.openChannel(conn);
    await fake.consume(ch, 'orders', 'ctag-1');
    fake.deliver(ch, 'ctag-1', 1);
    exporter.reset();
    // what amqplib's channel 0 does with the broker's connection.close
    const e = new Error('Connection closed: 320 (CONNECTION-FORCED) with message "CONNECTION_FORCED - broker forced connection closure with reason \'shutdown\'"');
    e.code = 320;
    conn.toClosed('stack', e);
    const [span] = attrsOf(exporter);
    assert.strictEqual(span['messaging.operation.name'], 'connection.close');
    assert.strictEqual(span['trailbeam.amqp.close.initiator'], 'broker');
    assert.strictEqual(span['trailbeam.amqp.close.reply_code'], 320);
    assert.strictEqual(span['trailbeam.amqp.close.reply_text'], "CONNECTION_FORCED - broker forced connection closure with reason 'shutdown'");
  });

  test('the callback API is followed too', async () => {
    const conn = new fake.FakeConnection();
    const ch = await fake.openChannel(conn, { api: 'callback', confirm: true });
    const confirmed = [];
    ch.sendToQueue('orders', Buffer.from('x'), { messageId: 'm-1' }, (err) => confirmed.push(err));
    fake.confirm(ch, true, 1);
    const got = [];
    ch.consume('orders', (m) => got.push(m), { consumerTag: 'ctag-1' }, () => {});
    fake.reply(ch, fake.defs.BasicConsumeOk, { consumerTag: 'ctag-1' });
    fake.deliver(ch, 'ctag-1', 1, { properties: { messageId: 'm-1' } });
    ch.ack(got[0]);
    const pulled = new Promise((resolve, reject) => ch.get('orders', {}, (err, m) => (err ? reject(err) : resolve(m))));
    fake.reply(ch, fake.defs.BasicGetOk, { deliveryTag: 2, redelivered: false, exchange: '', routingKey: 'orders', messageCount: 0 });
    ch.accept({ id: fake.defs.BasicProperties, channel: ch.ch, fields: { messageId: 'm-2' }, size: 0 });
    assert.strictEqual((await pulled).properties.messageId, 'm-2');
    ch.get('orders', {}); // no callback: still a span, and the delivery is recorded
    fake.reply(ch, fake.defs.BasicGetOk, { deliveryTag: 3, redelivered: false, exchange: '', routingKey: 'orders', messageCount: 0 });
    ch.accept({ id: fake.defs.BasicProperties, channel: ch.ch, fields: { messageId: 'm-3' }, size: 0 });
    const cancelled = new Promise((resolve) => ch.cancel('ctag-1', resolve));
    fake.reply(ch, fake.defs.BasicCancelOk, { consumerTag: 'ctag-1' });
    await cancelled;
    fake.clientClosesConnection(conn);
    assert.deepStrictEqual(confirmed, [null]);
    assert.deepStrictEqual(names(exporter), ['trailbeam publish', 'trailbeam deliver', 'trailbeam ack', 'trailbeam deliver',
      'trailbeam deliver', 'trailbeam cancel', 'trailbeam channel_closed', 'trailbeam channel_closed']);
    const closed = attrsOf(exporter).slice(-2);
    assert.deepStrictEqual(closed.map((a) => [a['rabbitmq.message.delivery_tag'], a['messaging.operation.name']]),
      [[2, 'connection.close'], [3, 'connection.close']]);
  });

  test('body capture and the tenant header', async () => {
    await amqplibHooks.uninstall();
    exporter = installed({ tenant: { messageAttribute: 'x-tenant' }, capture: { bodyQueues: ['orders'] } });
    const ch = await fake.openChannel(new fake.FakeConnection());
    await fake.consume(ch, 'orders', 'ctag-1');
    fake.deliver(ch, 'ctag-1', 1, { properties: { headers: { 'x-tenant': 'acme' } }, body: Buffer.from('{"card": "4111111111111111", "n": 1}') });
    fake.deliver(ch, 'ctag-1', 2, { properties: { headers: { 'x-tenant': 7 }, contentEncoding: 'gzip' }, body: Buffer.from([0x1f, 0x8b]) });
    fake.deliver(ch, 'ctag-1', 3, { body: Buffer.from([0xff, 0xfe]) });
    ch.publish('orders-x', 'k', Buffer.from('{"n": 1}'), { headers: { 'x-tenant': 'acme' } });
    const [a, b, c, p] = attrsOf(exporter);
    assert.strictEqual(a['trailbeam.tenant'], 'acme');
    assert.strictEqual(a['trailbeam.tenant.source'], 'header');
    assert.strictEqual(a['trailbeam.body.capture_status'], 'captured');
    assert.strictEqual(a['trailbeam.body.format'], 'json');
    assert.ok(!a['trailbeam.body'].includes('4111111111111111'));
    assert.strictEqual(a['messaging.message.body.size'], 36);
    assert.strictEqual(b['trailbeam.tenant'], '7');
    assert.strictEqual(b['trailbeam.body.capture_status'], 'not_text');
    assert.strictEqual(c['trailbeam.body.capture_status'], 'not_text');
    assert.strictEqual(p['trailbeam.body.capture_status'], 'off'); // the body list names the exchange for a publish
    assert.strictEqual(p['trailbeam.tenant'], 'acme');
  });
});

test('a return of a package-set ID is marked package', async () => {
  const exporter = installed({ setMessageId: true });
  let spans;
  try {
    const conn = new fake.FakeConnection();
    const ch = await fake.openChannel(conn);
    const options = { mandatory: true };
    ch.publish('', 'nobody', Buffer.from('x'), options);
    assert.deepStrictEqual(options, { mandatory: true }); // the application's options are not changed
    const sent = conn.sent.at(-1).props;
    fake.returned(ch, 312, 'NO_ROUTE', '', 'nobody', { messageId: sent.messageId });
    spans = attrsOf(exporter); // before uninstall, whose shutdown empties the exporter
  } finally {
    await amqplibHooks.uninstall();
  }
  const [pub, ret] = spans;
  assert.ok(pub['messaging.message.id']);
  assert.strictEqual(ret['messaging.message.id'], pub['messaging.message.id']);
  assert.strictEqual(ret['trailbeam.amqp.message_id.source'], 'package');
});

test('setMessageId from the environment', async (t) => {
  process.env.TRAILBEAM_AMQP_SET_MESSAGE_ID = 'on';
  t.after(() => { delete process.env.TRAILBEAM_AMQP_SET_MESSAGE_ID; });
  const exporter = installed();
  let spans;
  try {
    const ch = await fake.openChannel(new fake.FakeConnection());
    ch.publish('', 'orders', Buffer.from('x'), { messageId: 'mine' });
    ch.publish('', 'orders', Buffer.from('x'));
    spans = attrsOf(exporter);
  } finally {
    await amqplibHooks.uninstall();
  }
  const [a, b] = spans;
  assert.strictEqual(a['messaging.message.id'], 'mine');
  assert.strictEqual(a['trailbeam.amqp.message_id.source'], 'application');
  assert.strictEqual(b['trailbeam.amqp.message_id.source'], 'package');
});

test('a channel opened while installed passes through after uninstall, and amqplib is as it was', async () => {
  const before = {
    emit: Object.prototype.hasOwnProperty.call(channelLib.Channel.prototype, 'emit'),
    publish: channelModel.Channel.prototype.publish,
  };
  assert.strictEqual(before.emit, false);
  const exporter = installed();
  const ch = await fake.openChannel(new fake.FakeConnection());
  await fake.consume(ch, 'orders', 'ctag-1');
  assert.notStrictEqual(channelModel.Channel.prototype.publish, before.publish);
  await amqplibHooks.uninstall();
  exporter.reset();
  fake.deliver(ch, 'ctag-1', 1);
  fake.brokerCancels(ch, 'ctag-1');
  ch.publish('', 'orders', Buffer.from('x'));
  assert.deepStrictEqual(names(exporter), []);
  assert.strictEqual(Object.prototype.hasOwnProperty.call(channelLib.Channel.prototype, 'emit'), false);
  assert.strictEqual(channelModel.Channel.prototype.publish, before.publish);
});

test('a failing tracer never breaks the call', async () => {
  const failing = {
    onStart() { throw new Error('tracer down'); },
    onEnd() {},
    forceFlush: async () => {},
    shutdown: async () => {},
  };
  amqplibHooks.install({ spanProcessor: failing });
  try {
    const ch = await fake.openChannel(new fake.FakeConnection(), { confirm: true });
    const got = await fake.consume(ch, 'orders', 'ctag-1');
    fake.deliver(ch, 'ctag-1', 1, { properties: { messageId: 'm-1' } });
    ch.ack(got[0]);
    ch.publish('', 'orders', Buffer.from('x'));
    fake.confirm(ch, true, 1);
    fake.brokerClosesChannel(ch, 406, 'PRECONDITION_FAILED');
    assert.strictEqual(got.length, 1);
  } finally {
    await amqplibHooks.uninstall();
  }
});

test('install twice patches once', async () => {
  installed();
  const exporter = installed();
  try {
    const ch = await fake.openChannel(new fake.FakeConnection());
    ch.publish('', 'orders', Buffer.from('x'));
    assert.strictEqual(exporter.getFinishedSpans().length, 1);
  } finally {
    await amqplibHooks.uninstall();
  }
});

test('the package loads without amqplib until install is called', () => {
  const index = require('../src/index.js');
  assert.strictEqual(typeof index.amqplib.install, 'function');
  assert.strictEqual(index.amqplib, amqplibHooks);
});
