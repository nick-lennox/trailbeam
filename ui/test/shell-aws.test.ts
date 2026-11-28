/*
  QV-589 part 4: the shell's pages with AWS reads. The Overview's data takes each queue's QueueLive and triggers from
  /api/queues/{queue}/now when the server read it, and keeps the spans' figures with the server's reason when it
  didn't. The queue page's data takes /series' `aws`: with reads off it stays null (the page keeps "What Trailbeam
  can't see here"); read, it has the settings, CloudWatch, triggers and a verdict from SQS and CloudWatch. And the
  Settings page's AWS section (aws/settings.ts, AwsSettingsView). The AWS answers are the parity fixture's
  (contract/fixtures/aws-figures-parity.json), as the server sends them. Run: npm test
*/
import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { NOW_BATCH, anyAwsRead, overviewNowPath, overviewNowPaths, selfHostedFlowsData, type QueueNowAnswer } from "../src/overview/flows-data.ts";
import { fromSelfHosted, PAGE_WINDOW, type Flow, type SeriesResponse } from "../src/queue/selfhosted.ts";
import { emptyAws, liveOf, type AwsTrigger, type QueueAws } from "../src/model/aws.ts";
import type { InQueueView } from "../src/model/in-queue.ts";
import { QueuePage } from "../src/queue/QueuePage.tsx";
import type { QueueHrefs } from "../src/queue/types.ts";
import { changeOf, changed, formOf, queueList, statusLine, type AwsReadSettings, type AwsReadsStatus } from "../src/aws/settings.ts";
import { AwsSettingsView } from "../src/aws/AwsSettingsView.tsx";

const F = JSON.parse(readFileSync(new URL("../../contract/fixtures/aws-figures-parity.json", import.meta.url), "utf8"));
const OFF = "AWS reads are off on this server, so the queue's settings and depth are not read. An admin turns them on in Settings, under AWS.";
const NOT_NAMED = "This queue is not in the AWS settings' list of queues, so its settings and depth are not read. An admin adds it in Settings, under AWS.";

/** The fixture's read queue, with its CloudWatch window under `id` and the given triggers. */
function readAws(id: string, triggers: AwsTrigger[] | null = null): QueueAws {
  const a: QueueAws = structuredClone(F.aws["read, with SQS's counts"]);
  a.series = { [id]: a.series.recent };
  a.triggers = triggers;
  return a;
}
const trigger = (functionName: string): AwsTrigger => ({
  uuid: `u-${functionName}`, functionName, functionArn: `arn:aws:lambda:eu-west-1:123456789012:function:${functionName}`, state: "Enabled",
  batchSize: 10, batchingWindowS: 0, reportBatchItemFailures: true, maxConcurrency: null, filters: 0,
filterPatterns: [],
});
const NOW: number = F.aws["read, with SQS's counts"].readAt;

/* ── the Overview ────────────────────────────────────────────────────── */

const flow = (queue: string, o: Partial<Flow> = {}): Flow => ({
  key: queue, queue, name: queue, url: null, account: null, region: null, lastSeen: NOW - 1000,
  sent: 120, received: 130, deleted: 110, visibilityChanges: 4,
  producers: [{ name: "checkout", n: 120, lambda: false }], consumers: [{ name: "worker", n: 130, lambda: false }],
  avgBytes: 512, dlq: null, ...o,
});
const view = (o: Partial<InQueueView> = {}): InQueueView => ({
  nowMs: NOW, config: { visibilityTimeoutS: 30, retentionS: null, maxReceiveCount: 3, dlq: "orders-dlq", arn: null, visibilityKnown: true },
  held: [], capped: false, instrumented: { visible: 0, inFlight: 0 }, totals: null, uninstrumented: null, notSeenOnDlq: [], seenOnDlq: 0,
  deleted: [], producers: [], ...o,
});
const W = { fromMs: NOW - 3_600_000, toMs: NOW, minutes: 60 };
const overview = (now: Record<string, QueueNowAnswer | null>, flows = [flow("orders")]) =>
  selfHostedFlowsData({ range: "1h", window: W, flows, causes: null, now, causeHref: (id) => `/stuck?cause=${id}` });
const windows = [{ id: "recent", fromMs: W.fromMs, toMs: W.toMs, periodS: 60 }];

test("the Overview asks /now for CloudWatch over its whole minutes", () => {
  assert.equal(overviewNowPath("a/b", 60), "/api/queues/a%2Fb/now?minutes=60");
  assert.equal(overviewNowPath("orders", 14.6), "/api/queues/orders/now?minutes=15");
  assert.equal(overviewNowPath("orders", 0.2), "/api/queues/orders/now?minutes=1");
});

test("the Overview asks for its queues together, the same requests for every viewer of them (QV-743)", () => {
  assert.deepEqual(overviewNowPaths(["orders", "a/b", "orders"], 14.6), ["/api/queues/now?keys=a%2Fb,orders&minutes=15"]);
  assert.deepEqual(overviewNowPaths([], 60), []);
  const keys = Array.from({ length: NOW_BATCH + 1 }, (_, i) => `q${String(i).padStart(3, "0")}`);
  const paths = overviewNowPaths([...keys].reverse(), 60);
  assert.equal(paths.length, 2);
  assert.deepEqual(paths, overviewNowPaths(keys, 60));
  assert.ok(paths[1].startsWith(`/api/queues/now?keys=${keys[NOW_BATCH]}&`));
});

test("a queue the server read: its QueueLive, triggers and a verdict from SQS and CloudWatch", () => {
  const aws = readAws("recent", [trigger("worker")]);
  const v = view();
  const live = liveOf(aws, v, windows, NOW);
  const d = overview({ orders: { ...v, aws, live } });
  const info = d.info.orders;
  assert.equal(info.live, live);
  assert.equal(info.healthBasis, "aws");
  assert.equal(info.live.visible, 12);
  assert.deepEqual(info.triggers?.map((t) => t.functionName), ["worker"]);
  // the triggers are the page's own shape: no ARN or UUID
  assert.equal("functionArn" in info.triggers![0], false);
  // "In ‹queue› now" without the AWS parts
  assert.equal(info.inQueue && "aws" in info.inQueue, false);
  assert.equal(anyAwsRead({ orders: { ...v, aws, live } }), true);
});

test("a queue not read: the spans' figures, health from spans, and the server's reason", () => {
  const aws = emptyAws("no_account", NOT_NAMED);
  const live = liveOf(aws, view(), [], NOW);
  const d = overview({ orders: { ...view(), aws, live } });
  const info = d.info.orders;
  assert.equal(info.live.status, "no_account");
  assert.equal(info.live.detail, NOT_NAMED);
  // the spans' settings are kept
  assert.equal(info.live.maxReceiveCount, 3);
  assert.equal(info.live.visibilityTimeoutS, 30);
  assert.equal(info.healthBasis, "spans");
  assert.equal(info.triggers, null);
  assert.equal(anyAwsRead({ orders: { ...view(), aws, live } }), false);

  // reads off: not_configured with the server's sentence
  const off = emptyAws("not_configured", OFF);
  const o = overview({ orders: { ...view(), aws: off, live: liveOf(off, view(), [], NOW) } }).info.orders;
  assert.deepEqual([o.live.status, o.live.detail, o.healthBasis], ["not_configured", OFF, "spans"]);

  // a server before QV-589 part 3 sends neither: as before
  const old = overview({ orders: view() }).info.orders;
  assert.deepEqual([old.live.status, old.live.detail, old.healthBasis], ["not_configured", null, "spans"]);
  assert.equal(anyAwsRead({ orders: view(), other: null }), false);
});

/* ── the queue page ──────────────────────────────────────────────────── */

const MIN = 60_000;
const series = (aws?: QueueAws): SeriesResponse => ({
  queue: { key: "orders", name: "orders", account: null, region: null, fifo: false },
  from_ms: NOW - 3_600_000, to_ms: NOW, step_s: 60, max_receive_count: 3,
  series: Array.from({ length: 60 }, (_, i) => ({
    t_ms: NOW - 3_600_000 + i * MIN, sends: 60, receives: 60, re_receives: 0, handled: 60, failed: 0, dlq_seen: 0, dlq_inferred: 0,
    no_send_receives: 0, p95_queue_ms: 400, p95_handler_ms: 1200, queue_samples: 60, handler_samples: 60,
  })),
  versions: [],
  ...(aws ? { aws } : {}),
});
const pageFlow = flow("orders", { consumers: [{ name: "orders-worker", n: 3600, lambda: false }] });
const href: QueueHrefs = {
  queues: "/overview", queue: (n) => `/queue?name=${n}`, stuck: (q) => `/stuck?queue=${q}`, range: (p) => `/queue?name=orders&range=${p}`,
  messages: () => null,
};
const render = (data: ReturnType<typeof fromSelfHosted>, awsWhy?: (a: NonNullable<typeof data.aws>) => string) =>
  renderToStaticMarkup(createElement(QueuePage, { name: "orders", data, href, sel: null, onSelect: () => {}, split: false, onSplit: () => {}, awsWhy }));

test("reads off: aws stays null and the page keeps what Trailbeam can't see", () => {
  for (const aws of [undefined, emptyAws("not_configured", OFF)]) {
    const d = fromSelfHosted({ series: series(aws), flow: pageFlow, stuck: { count: 0, more: false }, endsNow: true, awsOn: false });
    assert.equal(d.aws, null);
    assert.equal(d.health?.basis, "spans");
    assert.match(render(d), /What Trailbeam can’t see here/);
  }
});

test("reads on, the queue not read: the server's reason where the hosted page names Connections", () => {
  const d = fromSelfHosted({ series: series(emptyAws("no_account", NOT_NAMED)), flow: pageFlow, stuck: null, endsNow: true, awsOn: true });
  assert.equal(d.aws?.status, "no_account");
  assert.equal(d.health?.basis, "spans");
  const html = render(d, (a) => `WHY: ${a.detail}`);
  assert.match(html, /From CloudWatch/);
  assert.ok(html.includes("WHY: This queue is not in the AWS settings"));
  assert.doesNotMatch(html, /No connected AWS account/);
  assert.doesNotMatch(html, /What Trailbeam can’t see here/);
  // without awsWhy, the hosted sentence
  assert.match(render(d), /No connected AWS account names this queue/);
  // on but without boto3: not_configured, still said
  const nb = fromSelfHosted({ series: series(emptyAws("not_configured", "no boto3")), flow: pageFlow, stuck: null, endsNow: true, awsOn: true });
  assert.equal(nb.aws?.detail, "no boto3");
});

test("a queue read: its settings, CloudWatch, triggers on their rows and a verdict from SQS and CloudWatch", () => {
  const aws = readAws(PAGE_WINDOW, [trigger("orders-worker"), trigger("silent-fn")]);
  const d = fromSelfHosted({ series: series(aws), flow: pageFlow, stuck: { count: 0, more: false }, endsNow: true, awsOn: true });
  assert.equal(d.aws?.status, "ok");
  assert.equal(d.aws?.settings?.maxReceiveCount, 5);
  assert.equal(d.aws?.now?.visible, 12);
  assert.ok(d.aws?.series?.visible);
  assert.equal(d.health?.basis, "aws");
  // the fixture's DLQ grew over the hour
  assert.deepEqual(d.health, { tone: "critical", why: "DLQ growing", basis: "aws" });
  assert.equal(d.queue.account, "123456789012");
  assert.equal(d.queue.region, "eu-west-1");
  const worker = d.consumers.find((c) => c.name === "orders-worker")!;
  assert.deepEqual([worker.kind, worker.trigger?.functionName], ["lambda", "orders-worker"]);
  assert.deepEqual(d.silentTriggers?.map((t) => t.functionName), ["silent-fn"]);
  const html = render(d);
  assert.match(html, /Read from SQS at/);
  assert.doesNotMatch(html, /What Trailbeam can’t see here/);
});

/* ── the Settings page's AWS section ─────────────────────────────────── */

const settings = (o: Partial<AwsReadSettings> = {}): AwsReadSettings =>
  ({ enabled: false, queues: [], region: null, regions: [], accountId: null, updatedAt: null, updatedBy: null, ...o });
const status = (o: Partial<AwsReadsStatus> = {}): AwsReadsStatus =>
  ({ on: false, queues: 0, boto3: null, credentials: null, region: null, problem: null, ...o });

test("the form: one name per line, tidied, and only a real change saves", () => {
  assert.deepEqual(queueList(" orders \n\npayments-*, orders\n"), ["orders", "payments-*"]);
  const s = settings({ enabled: true, queues: ["orders"], region: "eu-west-1" });
  const f = formOf(s);
  assert.deepEqual(f, { enabled: true, queues: "orders", redrive: "", region: "eu-west-1", regions: "", accountId: "" });
  assert.equal(changed(f, s), false);
  assert.equal(changed({ ...f, queues: "orders\n" }, s), false);
  assert.equal(changed({ ...f, queues: "orders\nbilling" }, s), true);
  assert.deepEqual(changeOf({ enabled: false, queues: "a\nb", redrive: " a-dlq \n", region: " ", regions: "us-east-1, ap-south-1,\nus-east-1", accountId: " 123456789012 " }),
    { enabled: false, queues: ["a", "b"], redrive: ["a-dlq"], region: null, regions: ["us-east-1", "ap-south-1"], accountId: "123456789012" });
  // the other regions, as saved, shown with commas (QV-740)
  assert.equal(formOf(settings({ regions: ["us-east-1", "ap-south-1"] })).regions, "us-east-1, ap-south-1");
  // QV-590: a server from before redrive sends no redrive list; the form reads it as empty
  assert.equal(formOf(settings()).redrive, "");
  assert.equal(changed({ ...f, redrive: "orders-dlq" }, s), true);
});

test("what the screen says about the reads", () => {
  assert.deepEqual(statusLine(status(), settings()), { tone: "idle", text: "Off. This server makes no AWS calls." });
  assert.equal(statusLine(status(), settings({ enabled: true })).text, "Not reading: AWS reads are switched on, but no queue is named.");
  assert.deepEqual(statusLine(status({ on: true, queues: 2, problem: "No AWS credentials were found." }), settings({ enabled: true, queues: ["a", "b"] })),
    { tone: "warning", text: "On for 2 names or patterns, but nothing can be read. No AWS credentials were found." });
  assert.deepEqual(statusLine(status({ on: true, queues: 1, boto3: true, credentials: "iam-role", region: "eu-west-1" }), settings({ enabled: true, queues: ["a"] })),
    { tone: "good", text: "On for 1 name or pattern. Credentials from the instance's role. Region eu-west-1 for queues whose spans don't name one. Redrive is off." });
  assert.equal(statusLine(status({ on: true, queues: 1, credentials: "sso-ish" }), settings()).text, "On for 1 name or pattern. Credentials from sso-ish. Redrive is off.");
  assert.equal(statusLine(status({ on: true, queues: 1 }), settings({ redrive: ["*-dlq"] })).text, "On for 1 name or pattern. Redrive is on for 1 dead-letter queue name or pattern.");
});

test("the AWS section: off by default, the three read-only permissions, and the app's copy rules", () => {
  const html = renderToStaticMarkup(createElement(AwsSettingsView, { answer: { settings: settings(), status: status() }, onSave: async () => ({ settings: settings(), status: status() }) }));
  assert.match(html, /Not reading from AWS/);
  assert.match(html, /Off\. This server makes no AWS calls\./);
  for (const a of ["sqs:GetQueueAttributes", "cloudwatch:GetMetricData", "lambda:ListEventSourceMappings"]) assert.ok(html.includes(a), a);
  assert.doesNotMatch(html, /—|→|title="/);
  const on = renderToStaticMarkup(createElement(AwsSettingsView, {
    answer: { settings: settings({ enabled: true, queues: ["orders"], updatedAt: NOW, updatedBy: "Ana" }), status: status({ on: true, queues: 1, problem: "No AWS credentials were found." }) },
    onSave: async () => { throw new Error("no"); },
  }));
  assert.match(on, /Can’t read from AWS|Can&#x27;t read from AWS/);
  assert.match(on, /by Ana/);
  assert.match(on, />orders<\/textarea>/);
  // QV-590: the dead-letter queues to redrive, empty by default, and what redrive needs besides the reads
  assert.match(html, /Dead-letter queues to redrive/);
  assert.match(html, /Leave it empty to keep redrive off/);
  for (const a of ["sqs:StartMessageMoveTask", "sqs:ListMessageMoveTasks", "sqs:CancelMessageMoveTask", "sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:SendMessage"]) assert.ok(html.includes(a), a);
});
