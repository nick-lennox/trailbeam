// Send three orders to the `orders` queue, print a Trailbeam link for each, and exit.
const { SendMessageCommand } = require('@aws-sdk/client-sqs');
const { sqs, queueUrl } = require('./sqs');

const ui = process.env.TRAILBEAM_UI || 'http://127.0.0.1:4318';

// `simulate` tells the worker what to do with the order, so each timeline shows something different.
const orders = [
  { order_id: 'A-1001', simulate: 'ok' }, //          received and deleted
  { order_id: 'A-1002', simulate: 'fail_once' }, //   fails once, comes back after the visibility timeout, then succeeds
  { order_id: 'A-1003', simulate: 'always_fail' }, // fails 3 times, then SQS moves it to orders-dlq
];

(async () => {
  const url = await queueUrl('orders');
  for (const order of orders) {
    const { MessageId } = await sqs.send(new SendMessageCommand({ QueueUrl: url, MessageBody: JSON.stringify(order) }));
    console.log(`sent ${order.order_id} (${order.simulate}): ${ui}/message?id=${encodeURIComponent(MessageId)}`);
  }
  // trailbeam exports its spans when the script ends.
})();
