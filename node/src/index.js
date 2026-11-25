'use strict';
// trailbeam: the spans in contract/span-contract.md for AWS SDK v3 SQS clients and AWS Lambda.

const { version } = require('./version');
const { hashReceiptHandle, keyId, SCHEMA_VERSION } = require('./contract');
const sqs = require('./sqs');
const { wrapHandler } = require('./lambda');
const { stats: exportStats } = require('./export');
// RabbitMQ (contract/span-contract-amqp.md): requires amqplib only when trailbeam.amqplib.install() is called
const amqplib = require('./amqplib');
// S3 uploads (contract section 27, revision 3.7): PutObject, CompleteMultipartUpload and CopyObject on an S3Client
const s3 = require('./s3');

/** sqs.js's install, with the S3 hooks on the same provider and options. */
function install(options = {}) {
  const provider = sqs.install(options);
  s3.attach(provider, options);
  return provider;
}

/** sqs.js's instrument, plus the S3 middleware, which acts only on an S3Client. */
function instrument(client) {
  if (!s3.attached()) install();
  sqs.instrument(client);
  return s3.instrument(client);
}

async function uninstall() {
  s3.detach();
  return sqs.uninstall();
}

module.exports = {
  version,
  SCHEMA_VERSION,
  install,
  instrument,
  uninstall,
  forceFlush: sqs.forceFlush,
  wrapHandler,
  exportStats,
  hashReceiptHandle,
  keyId,
  amqplib,
};
