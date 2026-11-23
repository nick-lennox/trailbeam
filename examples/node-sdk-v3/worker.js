// Process orders from the `orders` queue. A failed order is not deleted, so SQS delivers it again after
// the visibility timeout (5 s here), and moves it to `orders-dlq` after 3 receives.
const { ReceiveMessageCommand, DeleteMessageCommand } = require('@aws-sdk/client-sqs');
const { sqs, queueUrl } = require('./sqs');

function processOrder(order, receiveCount) {
  if (order.simulate === 'always_fail' || (order.simulate === 'fail_once' && receiveCount === 1)) {
    throw new Error(`payment service unavailable for ${order.order_id}`);
  }
  console.log(`processed ${order.order_id} on receive ${receiveCount}`);
}

(async () => {
  const url = await queueUrl('orders');
  for (;;) {
    const { Messages = [] } = await sqs.send(new ReceiveMessageCommand({
      QueueUrl: url, MaxNumberOfMessages: 10, WaitTimeSeconds: 10,
      MessageSystemAttributeNames: ['ApproximateReceiveCount'],
    }));
    for (const msg of Messages) {
      try {
        processOrder(JSON.parse(msg.Body), Number(msg.Attributes.ApproximateReceiveCount));
      } catch (e) {
        console.log(`failed, leaving it for a retry: ${e.message}`);
        continue;
      }
      await sqs.send(new DeleteMessageCommand({ QueueUrl: url, ReceiptHandle: msg.ReceiptHandle }));
    }
  }
})();
