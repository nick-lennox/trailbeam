'use strict';
// Shared test helpers: an in-memory span exporter and an SQSClient whose responses are canned.

const { InMemorySpanExporter, SimpleSpanProcessor } = require('@opentelemetry/sdk-trace-base');

function memoryExporter() {
  const exporter = new InMemorySpanExporter();
  return { exporter, spanProcessor: new SimpleSpanProcessor(exporter) };
}

/**
 * An SQSClient that never touches the network. `respond(operation, input)` returns the command
 * output or throws. Requests are serialized as usual first; `calls` records each operation with
 * the input the SDK actually sent. `service: 'sns'` makes an SNSClient instead.
 */
function stubClient(respond, { service = 'sqs' } = {}) {
  const Client = service === 'sns' ? require('@aws-sdk/client-sns').SNSClient : require('@aws-sdk/client-sqs').SQSClient;
  const client = new Client({
    region: 'us-east-1',
    endpoint: 'http://localhost:9324',
    credentials: { accessKeyId: 'test', secretAccessKey: 'test' },
    maxAttempts: 1,
    md5: false, // canned responses carry no body checksums
  });
  const calls = [];
  client.middlewareStack.add((next, context) => async (args) => {
    const operation = context.commandName.replace(/Command$/, '');
    calls.push({ operation, input: args.input });
    const output = await respond(operation, args.input);
    return { output: { $metadata: { httpStatusCode: 200 }, ...output }, response: {} };
  }, { step: 'build', priority: 'low', name: 'testStubResponder' });
  return { client, calls };
}

/** An SDK-style service error, as the AWS SDK v3 throws it. */
function serviceError(name, message = name) {
  const { SQSServiceException } = require('@aws-sdk/client-sqs');
  return new SQSServiceException({ name, $fault: 'client', $metadata: { httpStatusCode: 400 }, message });
}

const byName = (spans, name) => spans.filter((s) => s.name === name);

module.exports = { memoryExporter, stubClient, serviceError, byName };
