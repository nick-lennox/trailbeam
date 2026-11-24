'use strict';
// Section 21.2 with @opentelemetry/instrumentation-aws-sdk and a W3C propagator: the stock instrumentation
// adds traceparent to a Publish before the middleware stack runs, so the Trailbeam hook counts it and the
// message never carries more than 10 attributes. Own file: the instrumentation patches the SDK globally.
const test = require('node:test');
const assert = require('node:assert');
const api = require('@opentelemetry/api');
const { AsyncLocalStorageContextManager } = require('@opentelemetry/context-async-hooks');
const { W3CTraceContextPropagator } = require('@opentelemetry/core');
const { BasicTracerProvider, InMemorySpanExporter, SimpleSpanProcessor } = require('@opentelemetry/sdk-trace-base');
const { registerInstrumentations } = require('@opentelemetry/instrumentation');
const { AwsInstrumentation } = require('@opentelemetry/instrumentation-aws-sdk');

api.context.setGlobalContextManager(new AsyncLocalStorageContextManager().enable());
// Its fields are traceparent and tracestate; the stock instrumentation injects only if both would fit.
api.propagation.setGlobalPropagator(new W3CTraceContextPropagator());
const appProvider = new BasicTracerProvider({ spanProcessors: [new SimpleSpanProcessor(new InMemorySpanExporter())] });
registerInstrumentations({ tracerProvider: appProvider, instrumentations: [new AwsInstrumentation()] });

const N = require('@aws-sdk/client-sns');
const trailbeam = require('../src/index');
const { memoryExporter, stubClient } = require('./helpers');

test('traceparent and trailbeam.publish_id never pass 10 attributes', async (t) => {
  const { exporter, spanProcessor } = memoryExporter();
  trailbeam.install({ receiptHashKey: 'unit-test-key-0123456789', spanProcessor });
  t.after(() => trailbeam.uninstall());
  const { client, calls } = stubClient(() => ({ MessageId: 'm' }), { service: 'sns' });
  trailbeam.instrument(client);
  const sent = {};
  for (let n = 7; n <= 10; n++) {
    const MessageAttributes = Object.fromEntries([...Array(n).keys()].map((i) => [`a${i}`, { DataType: 'String', StringValue: 'v' }]));
    await client.send(new N.PublishCommand({ TopicArn: 'arn:aws:sns:us-east-1:123456789012:t', Message: `n=${n}`, MessageAttributes }));
    sent[n] = Object.keys(calls[calls.length - 1].input.MessageAttributes);
  }
  for (const names of Object.values(sent)) assert.ok(names.length <= 10, names.join(','));
  // The stock instrumentation ran first: Trailbeam counted its traceparent.
  assert.ok(sent[7].includes('traceparent') && sent[7].includes('trailbeam.publish_id')); // 7 + 1 + 1
  assert.ok(sent[8].includes('traceparent') && sent[8].includes('trailbeam.publish_id')); // 8 + 1 + 1 = 10
  assert.ok(!sent[9].includes('traceparent') && sent[9].includes('trailbeam.publish_id')); // 9 + 2 fields > 10; 9 + 1
  assert.ok(!sent[10].includes('traceparent') && !sent[10].includes('trailbeam.publish_id'));
  const skipped = exporter.getFinishedSpans().map((s) => s.attributes['trailbeam.sns.publish_id_skipped']);
  assert.deepStrictEqual(skipped, [undefined, undefined, undefined, 'attribute_limit']);
});
