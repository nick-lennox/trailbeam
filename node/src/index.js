'use strict';
// trailbeam: the spans in contract/span-contract.md for AWS SDK v3 SQS clients and AWS Lambda.

const { version } = require('./version');
const { hashReceiptHandle, keyId, SCHEMA_VERSION } = require('./contract');
const { install, instrument, uninstall, forceFlush } = require('./sqs');
const { wrapHandler } = require('./lambda');
const { stats: exportStats } = require('./export');
// RabbitMQ (contract/span-contract-amqp.md): requires amqplib only when trailbeam.amqplib.install() is called
const amqplib = require('./amqplib');

module.exports = {
  version,
  SCHEMA_VERSION,
  install,
  instrument,
  uninstall,
  forceFlush,
  wrapHandler,
  exportStats,
  hashReceiptHandle,
  keyId,
  amqplib,
};
