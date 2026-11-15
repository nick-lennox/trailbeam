'use strict';
// @trailbeam/sdk: the spans in docs/span-contract.md for AWS SDK v3 SQS clients and AWS Lambda.

const { version } = require('./version');
const { hashReceiptHandle, keyId, SCHEMA_VERSION } = require('./contract');
const { install, instrument, uninstall, forceFlush } = require('./sqs');
const { wrapHandler } = require('./lambda');

module.exports = {
  version,
  SCHEMA_VERSION,
  install,
  instrument,
  uninstall,
  forceFlush,
  wrapHandler,
  hashReceiptHandle,
  keyId,
};
