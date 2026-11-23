// A Lambda function with two SQS triggers: `orders` and its dead-letter queue `orders-dlq`.
//
// An order that fails is listed in `batchItemFailures` (ReportBatchItemFailures on the trigger), so only
// that record comes back after the visibility timeout (5 s here); after 3 receives SQS moves it to
// `orders-dlq`, and the DLQ trigger logs it.
const trailbeam = require('trailbeam');

function processOrder(order, receiveCount) {
  if (order.simulate === 'always_fail' || (order.simulate === 'fail_once' && receiveCount === 1)) {
    throw new Error(`payment service unavailable for ${order.order_id}`);
  }
  console.log(`processed ${order.order_id} on receive ${receiveCount}`);
}

exports.handler = trailbeam.wrapHandler(async (event) => {
  const batchItemFailures = [];
  for (const record of event.Records) {
    if (record.eventSourceARN.endsWith(':orders-dlq')) {
      // Receiving it is what lets Trailbeam show the move to the DLQ (SQS reports it on this receive).
      console.log(`dead letter ${record.messageId}: ${record.body}`);
      continue;
    }
    try {
      processOrder(JSON.parse(record.body), Number(record.attributes.ApproximateReceiveCount));
    } catch (e) {
      console.log(`failed, leaving it for a retry: ${e.message}`);
      batchItemFailures.push({ itemIdentifier: record.messageId });
    }
  }
  return { batchItemFailures };
});
