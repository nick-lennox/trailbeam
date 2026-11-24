'use strict';
// Conformance driver for the Node.js package (@aws-sdk/client-sqs v3 + trailbeam). Runs SCENARIO.md and
// writes the ground truth to $KIT_TRUTH_FILE. The kit sets every other variable (README.md).
//
// KIT_APP_TRACING=otel: the application has its own tracing, as a real service would: a global provider
// (sampler from OTEL_TRACES_SAMPLER / _ARG) exporting over OTLP, and the stock AWS SDK instrumentation,
// registered before the SDK is loaded.
const fs = require('fs');

const RUN = process.env.KIT_RUN;
const truth = fs.openSync(process.env.KIT_TRUTH_FILE, 'w');
const write = (row) => fs.writeSync(truth, JSON.stringify(row) + '\n');

let appProvider = null;
if (process.env.KIT_APP_TRACING === 'otel') {
  const api = require('@opentelemetry/api');
  const { AsyncLocalStorageContextManager } = require('@opentelemetry/context-async-hooks');
  const base = require('@opentelemetry/sdk-trace-base');
  const { OTLPTraceExporter } = require('@opentelemetry/exporter-trace-otlp-proto');
  const { resourceFromAttributes } = require('@opentelemetry/resources');
  const { registerInstrumentations } = require('@opentelemetry/instrumentation');
  const { AwsInstrumentation } = require('@opentelemetry/instrumentation-aws-sdk');
  const ratio = process.env.OTEL_TRACES_SAMPLER === 'traceidratio' ? Number(process.env.OTEL_TRACES_SAMPLER_ARG || '1') : 1;
  api.context.setGlobalContextManager(new AsyncLocalStorageContextManager().enable());
  appProvider = new base.BasicTracerProvider({
    sampler: new base.ParentBasedSampler({ root: new base.TraceIdRatioBasedSampler(ratio) }),
    resource: resourceFromAttributes({ 'service.name': process.env.OTEL_SERVICE_NAME }),
    spanProcessors: [new base.BatchSpanProcessor(new OTLPTraceExporter())],
  });
  api.trace.setGlobalTracerProvider(appProvider);
  registerInstrumentations({ tracerProvider: appProvider, instrumentations: [new AwsInstrumentation()] });
}

const S = require('@aws-sdk/client-sqs');
const trailbeam = require(process.env.KIT_NODE_PACKAGE || 'trailbeam');

trailbeam.install();
const sqs = trailbeam.instrument(new S.SQSClient({
  endpoint: process.env.KIT_SQS_ENDPOINT, region: 'elasticmq', credentials: { accessKeyId: 'x', secretAccessKey: 'x' },
}));
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const errorCode = (e) => e.Code || (e.Error && e.Error.Code) || e.name;

async function send(q, n) {
  const body = JSON.stringify({ n });
  const r = await sqs.send(new S.SendMessageCommand({ QueueUrl: q, MessageBody: body }));
  write({ op: 'SendMessage', queue_url: q, ok: true, message_id: r.MessageId, body });
  return r.MessageId;
}

async function receive(q, asked) {
  const params = { QueueUrl: q, MaxNumberOfMessages: 10, WaitTimeSeconds: 1 };
  if (asked) params.MessageSystemAttributeNames = [...asked];
  const r = await sqs.send(new S.ReceiveMessageCommand(params));
  const msgs = r.Messages || [];
  write({
    op: 'ReceiveMessage', queue_url: q, ok: true, asked_system_attributes: asked || [],
    messages: msgs.map((m) => ({ message_id: m.MessageId, receipt_handle: m.ReceiptHandle, body: m.Body, attributes: m.Attributes || {} })),
  });
  return msgs;
}

async function receiveUntil(q, want, asked) {
  const got = {};
  for (let i = 0; i < 30; i++) {
    for (const m of await receive(q, asked)) got[m.MessageId] = m;
    if (want.every((id) => id in got)) return got;
  }
  throw new Error(`timed out waiting for ${want} on ${q}`);
}

async function del(q, handle) {
  const row = { op: 'DeleteMessage', queue_url: q, receipt_handle: handle };
  try {
    await sqs.send(new S.DeleteMessageCommand({ QueueUrl: q, ReceiptHandle: handle }));
    row.ok = true;
  } catch (e) {
    row.ok = false;
    row.error_code = errorCode(e);
  }
  write(row);
}

async function batch(op, q, Entries, Command) {
  const r = await sqs.send(new Command({ QueueUrl: q, Entries }));
  const failed = Object.fromEntries((r.Failed || []).map((f) => [f.Id, f.Code]));
  write({
    op, queue_url: q, ok: true,
    entries: Entries.map((e) => {
      const row = { id: e.Id, receipt_handle: e.ReceiptHandle, ok: !(e.Id in failed) };
      if (e.VisibilityTimeout !== undefined) row.visibility_timeout = e.VisibilityTimeout;
      if (e.Id in failed) row.error_code = failed[e.Id];
      return row;
    }),
  });
}

async function main() {
  // 1. Queues
  const dlq = (await sqs.send(new S.CreateQueueCommand({ QueueName: `${RUN}-dlq` }))).QueueUrl;
  const dlqArn = (await sqs.send(new S.GetQueueAttributesCommand({ QueueUrl: dlq, AttributeNames: ['QueueArn'] }))).Attributes.QueueArn;
  const q = (await sqs.send(new S.CreateQueueCommand({
    QueueName: RUN,
    Attributes: { VisibilityTimeout: '2', RedrivePolicy: JSON.stringify({ deadLetterTargetArn: dlqArn, maxReceiveCount: '2' }) },
  }))).QueueUrl;
  const pq = (await sqs.send(new S.CreateQueueCommand({ QueueName: `${RUN}-purge` }))).QueueUrl;

  // 2. Send and batch send
  const ids = { 1: await send(q, 1) };
  const bodies = { 2: JSON.stringify({ n: 2 }), 3: JSON.stringify({ n: 3 }) };
  const r = await sqs.send(new S.SendMessageBatchCommand({
    QueueUrl: q, Entries: [2, 3].map((n) => ({ Id: `e${n}`, MessageBody: bodies[n] })),
  }));
  write({
    op: 'SendMessageBatch', queue_url: q, ok: true,
    entries: [
      ...(r.Successful || []).map((s) => ({ id: s.Id, ok: true, message_id: s.MessageId, body: bodies[s.Id.slice(1)] })),
      ...(r.Failed || []).map((f) => ({ id: f.Id, ok: false, error_code: f.Code })),
    ],
  });
  for (const s of r.Successful || []) ids[s.Id.slice(1)] = s.MessageId;

  // 3. Receive all three; the first receives ask for one system attribute of their own
  const got = await receiveUntil(q, [ids[1], ids[2], ids[3]], ['SenderId']);

  // 4. Delete n=1; batch delete n=2 and a bogus handle (one failed entry)
  await del(q, got[ids[1]].ReceiptHandle);
  await batch('DeleteMessageBatch', q, [
    { Id: 'd2', ReceiptHandle: got[ids[2]].ReceiptHandle },
    { Id: 'bogus', ReceiptHandle: 'not-a-receipt-handle' },
  ], S.DeleteMessageBatchCommand);

  // 5. Visibility-timeout retry: n=3 comes back with receive count 2
  await sleep(3000);
  await receiveUntil(q, [ids[3]]);

  // 6. DLQ move: ElasticMQ moves n=3 on the next receive of the source queue
  await sleep(3000);
  await receive(q);
  const d = await receiveUntil(dlq, [ids[3]]);
  await del(dlq, d[ids[3]].ReceiptHandle);

  // 7. ChangeMessageVisibility(0), receive again, ChangeMessageVisibilityBatch, delete
  ids[4] = await send(q, 4);
  let m = (await receiveUntil(q, [ids[4]]))[ids[4]];
  await sqs.send(new S.ChangeMessageVisibilityCommand({ QueueUrl: q, ReceiptHandle: m.ReceiptHandle, VisibilityTimeout: 0 }));
  write({ op: 'ChangeMessageVisibility', queue_url: q, receipt_handle: m.ReceiptHandle, visibility_timeout: 0, ok: true });
  m = (await receiveUntil(q, [ids[4]]))[ids[4]];
  await batch('ChangeMessageVisibilityBatch', q, [{ Id: 'v4', ReceiptHandle: m.ReceiptHandle, VisibilityTimeout: 30 }],
    S.ChangeMessageVisibilityBatchCommand);
  await del(q, m.ReceiptHandle);

  // 8. A delete with an invalid handle: the call fails
  await del(q, 'definitely-not-a-valid-receipt-handle');

  // 9. Purge
  await sqs.send(new S.PurgeQueueCommand({ QueueUrl: pq }));
  write({ op: 'PurgeQueue', queue_url: pq, ok: true });

  // 10. Flush
  await trailbeam.uninstall();
  if (appProvider) await appProvider.shutdown();
  fs.closeSync(truth);
  console.log(JSON.stringify({ driver: 'node', run: RUN, ids }));
}

const pkg = (() => { try { return require(require('path').join(process.env.KIT_NODE_PACKAGE || 'trailbeam', 'package.json')); } catch (e) { return {}; } })();
write({
  truth: 1, driver: 'node', language: 'javascript', node: process.version,
  sdk: `@aws-sdk/client-sqs ${(() => { try { return require('@aws-sdk/client-sqs/package.json').version; } catch (e) { return '?'; } })()}`,
  package: `trailbeam ${pkg.version || '?'}`, app_tracing: process.env.KIT_APP_TRACING || 'none',
});
main().catch((e) => { console.error(e); process.exit(1); });
