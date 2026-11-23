// The instrumented SQS client every script here uses.
const { SQSClient, GetQueueUrlCommand } = require('@aws-sdk/client-sqs');
const trailbeam = require('trailbeam');

// endpoint: the emulator here; leave it out in AWS.
const sqs = trailbeam.instrument(new SQSClient({ endpoint: process.env.SQS_ENDPOINT }));

async function queueUrl(name) {
  return (await sqs.send(new GetQueueUrlCommand({ QueueName: name }))).QueueUrl;
}

module.exports = { sqs, queueUrl };
