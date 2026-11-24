'use strict';
// A broker that isn't there, for the amqplib hooks' tests: amqplib's own Connection and channel classes over a
// stream that goes nowhere. The application's calls go through amqplib's methods; the broker's frames go to
// `channel.accept`, as amqplib's connection hands them over, so every hook sees what it would on a real broker.
// The same hooks against a real RabbitMQ are test/amqplib-rabbitmq.test.js.

const path = require('node:path');
const { PassThrough } = require('node:stream');

const dir = path.dirname(require.resolve('amqplib'));
const lib = (name) => require(path.join(dir, 'lib', name));
const defs = lib('defs.js');
const { Connection } = lib('connection.js');
const promiseApi = lib('channel_model.js');
const callbackApi = lib('callback_model.js');
const { IllegalOperationError } = lib('error.js');

/** A Connection whose frames are recorded instead of written. `failWrites`: an error a publish throws. */
class FakeConnection extends Connection {
  constructor({ host = 'localhost', port = 5672 } = {}) {
    const stream = new PassThrough();
    super(stream);
    this.stream._host = host; // what net.connect records
    this.stream.remotePort = port;
    this.channelMax = 0xffff;
    this.sent = [];
    this.failWrites = undefined;
    this.on('error', () => {});
  }

  sendMethod(channel, method, fields) {
    this.sent.push({ channel, method, fields });
  }

  sendMessage(channel, method, fields, Properties, props, content) {
    if (this.failWrites) throw this.failWrites;
    this.sent.push({ channel, method, fields, props, content });
    return true;
  }
}

/** A frame from the broker. */
function reply(ch, id, fields = {}) {
  ch.accept({ id, channel: ch.ch, fields });
}

function message(ch, method, fields, properties = {}, body = Buffer.from('{"n": 1}')) {
  reply(ch, method, fields);
  ch.accept({ id: defs.BasicProperties, channel: ch.ch, fields: properties, size: body.length });
  if (body.length) ch.accept({ channel: ch.ch, content: body });
}

/** An open channel of the promise API (`confirm`: a confirm channel, after confirm.select-ok). */
async function openChannel(conn, { confirm = false, api = 'promise' } = {}) {
  const models = api === 'promise' ? promiseApi : callbackApi;
  const ch = new (confirm ? models.ConfirmChannel : models.Channel)(conn);
  ch.on('error', () => {});
  if (api === 'promise') {
    const opened = ch.open();
    await new Promise(setImmediate); // open() sends channel.open after a tick
    reply(ch, defs.ChannelOpenOk);
    await opened;
    if (confirm) await selectConfirms(ch);
  } else {
    await new Promise((resolve, reject) => {
      ch.open((err) => (err ? reject(err) : resolve()));
      reply(ch, defs.ChannelOpenOk);
    });
    if (confirm) {
      await new Promise((resolve, reject) => {
        ch.rpc(defs.ConfirmSelect, { nowait: false }, defs.ConfirmSelectOk, (err) => (err ? reject(err) : resolve()));
        reply(ch, defs.ConfirmSelectOk);
      });
    }
  }
  return ch;
}

async function selectConfirms(ch) {
  const selected = ch.rpc(defs.ConfirmSelect, { nowait: false }, defs.ConfirmSelectOk);
  reply(ch, defs.ConfirmSelectOk);
  await selected;
}

/** consume(queue) as the application calls it, and the broker's consume-ok. Returns the messages it gets. */
async function consume(ch, queue, consumerTag, options = {}) {
  const got = [];
  const started = ch.consume(queue, (m) => got.push(m), { consumerTag, ...options });
  reply(ch, defs.BasicConsumeOk, { consumerTag });
  await started;
  return got;
}

function deliver(ch, consumerTag, deliveryTag, { redelivered = false, exchange = '', routingKey = '', properties = {}, body } = {}) {
  message(ch, defs.BasicDeliver, { consumerTag, deliveryTag, redelivered, exchange, routingKey }, properties, body);
}

async function getOk(ch, queue, deliveryTag, { redelivered = false, exchange = '', routingKey = '', properties = {}, body, options } = {}) {
  const got = ch.get(queue, options);
  message(ch, defs.BasicGetOk, { deliveryTag, redelivered, exchange, routingKey, messageCount: 0 }, properties, body);
  return got;
}

async function getEmpty(ch, queue) {
  const got = ch.get(queue);
  reply(ch, defs.BasicGetEmpty, { clusterId: '' });
  return got;
}

function confirm(ch, ack, deliveryTag, multiple = false) {
  reply(ch, ack ? defs.BasicAck : defs.BasicNack, { deliveryTag, multiple, requeue: false });
}

function returned(ch, replyCode, replyText, exchange, routingKey, properties = {}) {
  message(ch, defs.BasicReturn, { replyCode, replyText, exchange, routingKey }, properties);
}

function brokerClosesChannel(ch, replyCode, replyText) {
  reply(ch, defs.ChannelClose, { replyCode, replyText, classId: 0, methodId: 0 });
}

function brokerCancels(ch, consumerTag) {
  reply(ch, defs.BasicCancel, { consumerTag, nowait: true });
}

async function clientClosesChannel(ch) {
  const closed = ch.close();
  ch.accept({ id: defs.ChannelCloseOk, channel: ch.ch, fields: {} });
  await closed;
}

/** The application closes the connection (ChannelModel.close) and the broker's close-ok arrives. */
function clientClosesConnection(conn) {
  conn.closeBecause('Cheers, thanks', defs.constants.REPLY_SUCCESS);
  conn.accept({ id: defs.ConnectionCloseOk, channel: 0, fields: {} });
}

/** The socket fails, as amqplib's connection reports it. */
function connectionLost(conn) {
  conn.onSocketError(new Error('read ECONNRESET'));
}

/** The library raises on the next frame the channel sends, as on a channel that has gone away. */
function channelGone(ch) {
  const raise = () => { throw new IllegalOperationError('Channel closed', ''); };
  ch.sendImmediately = raise;
  ch.sendOrEnqueue = raise;
  ch.sendMessage = raise;
}

module.exports = {
  dir,
  defs,
  FakeConnection,
  IllegalOperationError,
  openChannel,
  selectConfirms,
  consume,
  deliver,
  getOk,
  getEmpty,
  confirm,
  returned,
  brokerClosesChannel,
  brokerCancels,
  clientClosesChannel,
  clientClosesConnection,
  connectionLost,
  channelGone,
  reply,
};
