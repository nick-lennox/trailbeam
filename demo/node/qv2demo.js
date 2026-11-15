'use strict';
// qv2 demo driver for Node.js: a producer, a consumer and a deleter on an SQS emulator, with @qv2/sdk.
//
//   node qv2demo.js produce --queue orders --count 200 --mix check
//   node qv2demo.js consume --queue orders --consumers 4 --deleter http://deleter:8080
//   node qv2demo.js deleter --queue orders --port 8080
//
// The message kinds and the --truth files are the same as in ../python/qv2demo.py; see there. The truth reads
// the spans' clock, Date.now() in whole milliseconds (../../node/src/emitter.js nowMs), so its lines say
// same_clock_as_spans and the checker bracket-checks Node.js events too (QV-40; demo/check/check.py
// BRACKET_BOUND_NS).
// "--consumers 4" runs four concurrent receive loops in one process (Node's concurrency model).

const crypto = require('node:crypto');
const fs = require('node:fs');
const http = require('node:http');
const os = require('node:os');
const path = require('node:path');
const { parseArgs } = require('node:util');
const {
  SQSClient, CreateQueueCommand, GetQueueAttributesCommand, GetQueueUrlCommand, SendMessageCommand,
  ReceiveMessageCommand, DeleteMessageCommand, ChangeMessageVisibilityCommand,
} = require('@aws-sdk/client-sqs');
const qv2 = require('@qv2/sdk');

// Unset: the local emulator. Set but empty: the AWS SDK's default endpoint, that is real SQS (demo/check.sh --aws).
const ENDPOINT = process.env.QV2_DEMO_SQS_ENDPOINT ?? 'http://localhost:9324';
const REGION = process.env.AWS_REGION || 'us-east-1';
let stopping = false;
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function kindOf(i, mix) {
  if (mix === 'check-dlq' && i % 20 === 10) return 'poison_fast'; // QV-33: the check covers the DLQ move
  if (mix === 'check' || mix === 'check-dlq') return i % 7 === 3 ? 'retry_once' : i % 7 === 5 ? 'xproc' : 'ok';
  if (i % 25 === 12) return 'poison';
  return { 3: 'retry_once', 5: 'xproc', 7: 'slow' }[i % 10] || 'ok';
}

const handleHash = (rh) => crypto.createHash('sha256').update(rh).digest('hex').slice(0, 16);
// Truth times read the system clock on every call, as the spans do (../../node/src/emitter.js nowMs) and as
// Python's time.time_ns() does, so every process on the host reads the same clock. (Anchoring hrtime to
// Date.now() once per process, as this did before QV-38, leaves each process on its own clock once the
// system clock steps or slews: up to ~70 ms apart in QV-35.) The resolution is 1 ms, rounded down.
const nowNs = () => String(BigInt(Date.now()) * 1000000n);

// The driver asks for SQS's facts itself rather than relying on @qv2/sdk to add them, so the truth doesn't
// depend on the code under test. A receive's truth line records them (QV-38), null if SQS left one out.
const RECEIVE_ATTRIBUTES = ['ApproximateReceiveCount', 'SentTimestamp', 'ApproximateFirstReceiveTimestamp'];
const optInt = (v) => (v === undefined ? null : parseInt(v, 10));
const sqsFacts = (m) => ({
  rc: parseInt(m.Attributes.ApproximateReceiveCount, 10),
  sent_ms: optInt(m.Attributes.SentTimestamp),
  first_receive_ms: optInt(m.Attributes.ApproximateFirstReceiveTimestamp),
});

function truthWriter(dir, role) {
  if (!dir) return () => {};
  fs.mkdirSync(dir, { recursive: true });
  const fd = fs.openSync(path.join(dir, `node-${role}-${os.hostname()}-${process.pid}.jsonl`), 'a');
  return (rec) => fs.writeSync(fd, JSON.stringify({ ...rec, pid: process.pid, host: os.hostname(), same_clock_as_spans: true }) + '\n');
}

async function ensureQueue(sqs, name, visibilityTimeout, maxReceives) {
  for (let attempt = 0; ; attempt++) {
    try {
      const dlqUrl = (await sqs.send(new CreateQueueCommand({ QueueName: `${name}-dlq` }))).QueueUrl;
      const dlqArn = (await sqs.send(new GetQueueAttributesCommand({ QueueUrl: dlqUrl, AttributeNames: ['QueueArn'] }))).Attributes.QueueArn;
      return (await sqs.send(new CreateQueueCommand({
        QueueName: name,
        Attributes: {
          VisibilityTimeout: String(visibilityTimeout),
          RedrivePolicy: JSON.stringify({ deadLetterTargetArn: dlqArn, maxReceiveCount: String(maxReceives) }),
        },
      }))).QueueUrl;
    } catch (e) {
      if (attempt >= 59) throw e;
      console.error(`waiting for SQS at ${ENDPOINT}: ${e.message}`);
      await sleep(1000);
    }
  }
}

function client(role) {
  process.env.OTEL_SERVICE_NAME = process.env.OTEL_SERVICE_NAME || `qv2-demo-node-${role}`;
  qv2.install();
  return qv2.instrument(new SQSClient({ endpoint: ENDPOINT || undefined, region: REGION }));
}

async function produce(a) {
  const sqs = client('producer');
  const url = await ensureQueue(sqs, a.queue, a.visibilityTimeout, a.maxReceives);
  const truth = truthWriter(a.truth, 'producer');
  let i = 0;
  while (!stopping && (a.count === 0 || i < a.count)) {
    const kind = kindOf(i, a.mix);
    const t = nowNs();
    const r = await sqs.send(new SendMessageCommand({ QueueUrl: url, MessageBody: JSON.stringify({ kind, n: i }) }));
    truth({ op: 'send', id: r.MessageId, kind, n: i, t_ns: t, result: 'ok', t_start_ns: t, t_end_ns: nowNs() });
    i++;
    if (a.interval) await sleep(a.interval * 1000);
  }
  console.log(`sent ${i} messages to ${a.queue}`);
}

async function consume(a) {
  const sqs = client('consumer');
  const url = await ensureQueue(sqs, a.queue, a.visibilityTimeout, a.maxReceives);
  const truth = truthWriter(a.truth, 'consumer');

  const settle = async (op, Command, m, loop, extra = {}) => {
    const t = nowNs();
    let result = 'ok';
    try {
      await sqs.send(new Command({ QueueUrl: url, ReceiptHandle: m.ReceiptHandle, ...extra }));
    } catch (e) {
      result = 'failed';
      console.error(`${op} ${m.MessageId} failed: ${e.name}`);
    }
    truth({ op, id: m.MessageId, rh: handleHash(m.ReceiptHandle), t_ns: t, result, thread: `consumer-${loop}`, t_start_ns: t, t_end_ns: nowNs() });
  };

  const handle = async (m, loop) => {
    const { kind } = JSON.parse(m.Body);
    const rc = parseInt(m.Attributes.ApproximateReceiveCount, 10);
    if (kind === 'retry_once' && rc === 1) {
      await settle('change_visibility', ChangeMessageVisibilityCommand, m, loop, { VisibilityTimeout: 0 });
    } else if (kind === 'xproc') {
      truth({ op: 'handoff', id: m.MessageId, rh: handleHash(m.ReceiptHandle), t_ns: nowNs(), thread: `consumer-${loop}` });
      const res = await fetch(`${a.deleter}/delete`, {
        method: 'POST', body: JSON.stringify({ receipt_handle: m.ReceiptHandle, message_id: m.MessageId }),
      });
      if (res.status !== 202) throw new Error(`deleter answered ${res.status}`);
    } else if (kind === 'slow' && rc === 1) {
      await sleep((a.visibilityTimeout + 3) * 1000);
      await settle('delete', DeleteMessageCommand, m, loop);
    } else if (kind === 'poison') {
      console.error(`poison message ${m.MessageId} failed (receive ${rc})`);
    } else if (kind === 'poison_fast') {
      await settle('change_visibility', ChangeMessageVisibilityCommand, m, loop, { VisibilityTimeout: 0 });
    } else {
      await settle('delete', DeleteMessageCommand, m, loop);
    }
  };

  const loop = async (j) => {
    while (!stopping) {
      let r;
      const t0 = nowNs();
      try {
        r = await sqs.send(new ReceiveMessageCommand({
          QueueUrl: url, MaxNumberOfMessages: 10, WaitTimeSeconds: a.wait, MessageSystemAttributeNames: RECEIVE_ATTRIBUTES,
        }));
      } catch (e) {
        console.error(`receive failed: ${e.message}`);
        await sleep(1000);
        continue;
      }
      const t = nowNs();
      for (const m of r.Messages || []) {
        truth({ op: 'receive', id: m.MessageId, rh: handleHash(m.ReceiptHandle), ...sqsFacts(m), t_ns: t, result: 'ok', thread: `consumer-${j}`, queue: a.queue, t_start_ns: t0, t_end_ns: t });
      }
      for (const m of r.Messages || []) {
        try { await handle(m, j); } catch (e) { console.error(`handling ${m.MessageId} failed: ${e.message}`); }
      }
    }
  };

  // Reads the dead-letter queue and deletes what it finds. It does not ask for DeadLetterQueueSourceArn:
  // @qv2/sdk does, and the store shows the move SQS reports with it (contract 3.1).
  const dlqLoop = async () => {
    const dlq = `${a.queue}-dlq`;
    const dlqUrl = (await sqs.send(new GetQueueUrlCommand({ QueueName: dlq }))).QueueUrl;
    while (!stopping) {
      let r;
      const t0 = nowNs();
      try {
        r = await sqs.send(new ReceiveMessageCommand({
          QueueUrl: dlqUrl, MaxNumberOfMessages: 10, WaitTimeSeconds: a.wait, MessageSystemAttributeNames: RECEIVE_ATTRIBUTES,
        }));
      } catch (e) {
        console.error(`dead-letter receive failed: ${e.message}`);
        await sleep(1000);
        continue;
      }
      const t = nowNs();
      for (const m of r.Messages || []) {
        truth({ op: 'receive', id: m.MessageId, rh: handleHash(m.ReceiptHandle), ...sqsFacts(m), t_ns: t, result: 'ok', thread: 'dlq', queue: dlq, t_start_ns: t0, t_end_ns: t });
      }
      for (const m of r.Messages || []) {
        console.log(`dead-letter queue: ${m.MessageId}`);
        const td = nowNs();
        let result = 'ok';
        try {
          await sqs.send(new DeleteMessageCommand({ QueueUrl: dlqUrl, ReceiptHandle: m.ReceiptHandle }));
        } catch (e) {
          result = 'failed';
          console.error(`dead-letter delete ${m.MessageId} failed: ${e.name}`);
        }
        truth({ op: 'delete', id: m.MessageId, rh: handleHash(m.ReceiptHandle), t_ns: td, result, thread: 'dlq', queue: dlq, t_start_ns: td, t_end_ns: nowNs() });
      }
    }
  };

  console.log(`consuming ${a.queue} with ${a.consumers} concurrent consumers`);
  const loops = Array.from({ length: a.consumers }, (_, j) => loop(j));
  if (a.dlq) loops.push(dlqLoop());
  await Promise.all(loops);
}

async function deleter(a) {
  const sqs = client('deleter');
  const url = await ensureQueue(sqs, a.queue, a.visibilityTimeout, a.maxReceives);
  const truth = truthWriter(a.truth, 'deleter');
  const work = [];
  const server = http.createServer((req, res) => {
    let body = '';
    req.on('data', (c) => { body += c; });
    req.on('end', () => {
      try { const b = JSON.parse(body); work.push([b.receipt_handle, b.message_id]); res.writeHead(202); } catch { res.writeHead(400); }
      res.end();
    });
  });
  await new Promise((r) => server.listen(a.port, '0.0.0.0', r));
  console.log(`deleter for ${a.queue} on port ${a.port}`);
  while (!stopping) {
    const item = work.shift();
    if (!item) { await sleep(20); continue; }
    const [rh, mid] = item;
    await sleep(50);
    const t = nowNs();
    let result = 'ok';
    try {
      await sqs.send(new DeleteMessageCommand({ QueueUrl: url, ReceiptHandle: rh }));
    } catch (e) {
      result = 'failed';
      console.error(`delete ${mid} failed: ${e.name}`);
    }
    truth({ op: 'delete', id: mid, rh: handleHash(rh), t_ns: t, result, by: 'deleter', thread: 'deleter', t_start_ns: t, t_end_ns: nowNs() });
  }
  server.close();
}

async function main() {
  const role = process.argv[2];
  const { values: v } = parseArgs({
    args: process.argv.slice(3),
    options: {
      queue: { type: 'string', default: 'orders' },
      truth: { type: 'string' },
      'visibility-timeout': { type: 'string', default: '30' },
      'max-receives': { type: 'string', default: '3' },
      count: { type: 'string', default: '0' },
      interval: { type: 'string', default: '0' },
      mix: { type: 'string', default: 'demo' },
      consumers: { type: 'string', default: '4' },
      wait: { type: 'string', default: '1' },
      deleter: { type: 'string', default: 'http://deleter:8080' },
      dlq: { type: 'boolean', default: false },
      port: { type: 'string', default: '8080' },
    },
  });
  const a = {
    queue: v.queue, truth: v.truth, visibilityTimeout: +v['visibility-timeout'], maxReceives: +v['max-receives'],
    count: +v.count, interval: +v.interval, mix: v.mix, consumers: +v.consumers, wait: +v.wait, deleter: v.deleter,
    dlq: v.dlq, port: +v.port,
  };
  const run = { produce, consume, deleter }[role];
  if (!run || !['demo', 'check', 'check-dlq'].includes(a.mix)) {
    console.error('usage: node qv2demo.js produce|consume|deleter [--queue Q] [--truth DIR] ...');
    process.exit(2);
  }
  for (const sig of ['SIGTERM', 'SIGINT']) process.on(sig, () => { stopping = true; });
  try {
    await run(a);
  } finally {
    await qv2.uninstall(); // flushes the qv2 spans
  }
}

main().catch((e) => { console.error(e); process.exitCode = 1; });
