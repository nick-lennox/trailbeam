// Read the dead-letter queue `orders-dlq`: log each dead letter, then delete it.
//
// Receiving here is what lets Trailbeam show the move to the DLQ: SQS reports it with the
// DeadLetterQueueSourceArn attribute on this receive, and the trailbeam hooks record it.
const { ReceiveMessageCommand, DeleteMessageCommand } = require('@aws-sdk/client-sqs');
const { sqs, queueUrl } = require('./sqs');

(async () => {
  const url = await queueUrl('orders-dlq');
  for (;;) {
    const { Messages = [] } = await sqs.send(new ReceiveMessageCommand({
      QueueUrl: url, MaxNumberOfMessages: 10, WaitTimeSeconds: 10,
    }));
    for (const msg of Messages) {
      console.log(`dead letter ${msg.MessageId}: ${msg.Body}`);
      await sqs.send(new DeleteMessageCommand({ QueueUrl: url, ReceiptHandle: msg.ReceiptHandle }));
    }
  }
})();
