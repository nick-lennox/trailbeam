/*
  RabbitMQ in the shared views (QV-308): the message page of every RabbitMQ message Trailbeam returns for the
  contract's fixture (fixtures/amqp-timelines.json, from make-amqp-fixture.py) is in RabbitMQ's words, with its
  system badge, its ID as the application set it, and one row per queue copy; an SQS message's page has none of it.
  Run: npm test
*/
import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { createElement as h } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { MessagePage, StatePill, SystemBadge, TimelineView } from "../src/design/index.ts";
import { ORDER_SOURCE, answerFor, isRabbitMq, shownId, splitKey, systemOf, type MessageHrefs, type Timeline } from "../src/index.ts";
import { ORDER_SOURCE_RABBITMQ } from "../src/timeline/words.ts";
import { eventName, eventsOfTimeline, kindName, queryLanguage, type Ev, type SpanMeta } from "../src/model/index.ts";
import { cellText, filledColumns } from "../src/explorer/columns.ts";
import { EventExplorer } from "../src/explorer/index.ts";
import { overviewData } from "../src/model/overview.ts";
import { dlqNodeCount, OverviewFlows, RABBITMQ_NO_FIGURES, type OverviewCopy, type OverviewFlowsData, type OverviewFlowsHrefs } from "../src/now/OverviewFlows.tsx";
import type { Flow } from "../src/queue/selfhosted.ts";
import { FirstRun } from "../src/design/index.ts";
import { lookupEmpty, stuckEmpty } from "../src/messages/empty.tsx";
import { MESSAGE_COLUMNS, messageLevel, type MessageRow } from "../src/messages/columns.tsx";
import { rabbitKeyOf } from "../shell/message.tsx";

const load = (f: string) => JSON.parse(readFileSync(new URL(`./fixtures/${f}`, import.meta.url), "utf8")) as { timelines: Timeline[]; items?: Record<string, unknown>[] };
const amqp = load("amqp-timelines.json");
const sqs = ["timelines.json", "sns-timelines.json", "put-event-timelines.json", "origin-timelines.json", "purge-timelines.json", "contract-3.2-timelines.json"]
  .flatMap((f) => load(f).timelines);
const byId = (id: string) => amqp.timelines.find((t) => t.message_id === `~rabbitmq~${id}`)!;
const text = (html: string) => html.replace(/<[^>]+>/g, " ").replace(/&#x27;/g, "'").replace(/&quot;/g, '"').replace(/&amp;/g, "&").replace(/\s+/g, " ");
const href: MessageHrefs = { messages: "/m", queue: (q) => `/m?queue=${q}`, message: (id) => `/msg/${id}`, tab: () => "/msg/x" };
const page = (t: Timeline) => renderToStaticMarkup(h(MessagePage, { t, href, tab: "timeline", openSteps: t.events.map((e) => e.id) }));
const view = (t: Timeline) => renderToStaticMarkup(h(TimelineView, { t, answer: answerFor(t), openSteps: t.events.map((e) => e.id) }));

// as the server's own test of these pages (python/tests/server/test_amqp_timeline.py), plus the SQS step names
const SQS_WORDS = /SQS|visibility|ReceiveMessage|DeleteMessage|maxReceiveCount|receipt|Received \(|Received by|\bDeleted\b|No delete|handled\)/;

test("the key names the system: ~rabbitmq~<ID> is RabbitMQ, anything else is SQS", () => {
  assert.deepEqual(splitKey("~rabbitmq~order-17"), ["rabbitmq", "order-17"]);
  assert.deepEqual(splitKey("~aws.sqs~~odd"), ["aws.sqs", "~odd"]);
  assert.deepEqual(splitKey("4f1c2c1e-0b7e-4c43-9f3e-6c8d1a2b3c4d"), ["aws.sqs", "4f1c2c1e-0b7e-4c43-9f3e-6c8d1a2b3c4d"]);
  assert.deepEqual(splitKey("~kafka~x"), ["aws.sqs", "~kafka~x"], "a system Trailbeam doesn't read is not a system mark");
  assert.equal(shownId("~rabbitmq~m-fan"), "m-fan");
  assert.equal(systemOf(undefined), "aws.sqs");
  assert.ok(amqp.timelines.length >= 11 && amqp.timelines.every(isRabbitMq));
  assert.ok(!sqs.some(isRabbitMq));
});

test("every RabbitMQ message's page is in RabbitMQ's words, never SQS's, and keeps the design system's rules", () => {
  for (const t of amqp.timelines) {
    for (const html of [page(t), view(t)]) {
      const words = text(html);
      assert.doesNotMatch(words, SQS_WORDS, `${t.message_id}: ${words.match(SQS_WORDS)?.[0]}`);
      assert.doesNotMatch(html, / title="|<title>/, `${t.message_id}: hover notes are the Tooltip, never title`);
      assert.doesNotMatch(html, /[→←↗↑×]/, `${t.message_id}: no arrows`);
      assert.doesNotMatch(html, /uppercase|tracking-/, `${t.message_id}: no tracked capitals`);
      assert.doesNotMatch(words, / · |—| – /, `${t.message_id}: no a · b fragments, no em dashes`);
      assert.doesNotMatch(words, /~rabbitmq~/, `${t.message_id}: the ID as the application set it`);
      assert.match(html, /data-system="rabbitmq"[^>]*>RabbitMQ</, `${t.message_id}: the system badge`);
    }
  }
});

test("the steps are named as RabbitMQ names them: published, routed, delivered, acked, requeued, refused, returned", () => {
  const steps = (id: string) => text(view(byId(id)));
  assert.match(steps("m-happy"), /Published to the default exchange .*Routed to 1 queue .*Delivered on orders \(receive count 1\) .*Acked \(basic\.ack\)/);
  assert.match(steps("m-requeue"), /Requeued with basic\.nack .*Returned to the queue: requeued by the consumer/);
  assert.match(steps("m-crash"), /Channel closed before an ack .*Closed by a lost connection .*Returned to the queue: its channel closed before an ack/);
  assert.match(steps("m-crash"), /Dead-lettered to orders\.dlq .*x-death header .*Reason delivery_limit/);
  assert.match(steps("m-reject"), /Refused with basic\.(reject|nack), not requeued/);
  assert.match(steps("m-noroute"), /Returned to the publisher \(basic\.return\) .*Reply code 312/);
  assert.match(steps("m-timeout"), /Returned to the queue: the broker cancelled the consumer/);
  assert.match(steps("m-retry"), /Published again to/);
  assert.match(text(page(byId("m-happy"))), /Delivered to orders-worker/);
  // the receive count's hover note is RabbitMQ's, not "SQS's receive count"
  assert.match(ORDER_SOURCE_RABBITMQ.receive_count.long, /x-delivery-count, x-acquired-count or redelivered/);
  assert.match(ORDER_SOURCE.receive_count.long, /SQS/);
});

test("the headline and the state pill say what happened in RabbitMQ's terms", () => {
  assert.equal(answerFor(byId("m-happy")), "Delivered once by orders-worker and acked.");
  assert.equal(answerFor(byId("m-requeue")), "Returned to the queue after delivery 1 (requeued by the consumer), delivered again by orders-worker and acked.");
  assert.match(answerFor(byId("m-reject")), /^Dead-lettered to orders\.dlq after 1 delivery on orders\.$/);
  assert.match(answerFor(byId("m-noroute")), /no queue took it: the broker returned it to the publisher with basic\.return\.$/);
  assert.match(answerFor(byId("m-stuck")), /^Delivered 1 time by orders-worker\. No ack seen yet\.$/);
  assert.match(answerFor(byId("m-retry")), /^Published 2 times under one message ID/);
  const pill = (t: Timeline) => text(renderToStaticMarkup(h(StatePill, { m: t })));
  assert.match(pill(byId("m-happy")), /Done/);
  assert.match(pill(byId("m-stuck")), /No ack seen/);
  assert.match(pill(byId("m-noroute")), /Not delivered/);
  assert.match(pill(byId("m-reject")), /In the DLQ/);
});

test("a fanout to two queues is one message with two rows, one per queue copy, and not a duplicate", () => {
  const t = byId("m-fan");
  assert.equal(t.copies?.length, 2);
  const html = page(t);
  const rows = [...html.matchAll(/<tr[^>]*data-copy="([^"]+)"/g)].map((m) => m[1]);
  assert.deepEqual(rows, ["fan.a", "fan.b"]);
  assert.match(text(html), /2 queue copies Queue Deliveries Ended fan\.a 1 Acked fan\.b 1 Acked/);
  assert.match(answerFor(t), /^Routed to 2 queues, fan\.a and fan\.b\. Each queue's copy was delivered by orders-worker and acked\.$/);
  // a copy still on its queue says so; a copy dead-lettered says where it went
  assert.match(text(page(byId("m-stuck"))), /One queue copy .* orders 1 Not settled yet/);
  assert.match(text(page(byId("m-reject"))), /orders 1 Moved to a dead-letter queue orders\.dlq 1 Acked/);
});

test("an SQS message's page has no system badge, no queue copies and no RabbitMQ words", () => {
  assert.equal(renderToStaticMarkup(h(SystemBadge, { system: "aws.sqs" })), "");
  assert.match(renderToStaticMarkup(h(SystemBadge, { system: "aws.sqs", always: true })), />Amazon SQS</);
  for (const t of sqs) {
    const html = page(t);
    assert.doesNotMatch(html, /data-system=|data-copies=/, t.message_id);
    assert.doesNotMatch(text(html), /RabbitMQ|basic\.|Acked|Delivered on|queue cop/, t.message_id);
  }
});

/* ── the Event explorer ─────────────────────────────────────────────── */

type Input = { timeline: Record<string, unknown>; urls: Record<string, string>; metas: SpanMeta[]; body: { value: unknown; traceId: string; spanId: string } | null };
const parity = JSON.parse(readFileSync(new URL("./fixtures/amqp-timeline-events-parity.json", import.meta.url), "utf8")) as {
  fromMs: number; toMs: number; inputs: Input[]; answer: { events: Ev[]; messages: number } };
const plain = <T,>(x: T): T => JSON.parse(JSON.stringify(x));

test("eventsOfTimeline makes the server's events of the same RabbitMQ timelines, newest first (timeline_events.py)", () => {
  const found: { e: Ev; i: number }[] = [];
  for (const m of parity.inputs)
    for (const e of eventsOfTimeline(m.timeline, { urls: m.urls, metas: m.metas, body: m.body ?? undefined }))
      if (e.ms >= parity.fromMs && e.ms <= parity.toMs) found.push({ e, i: found.length });
  assert.deepEqual(plain(found.sort((a, b) => b.e.ms - a.e.ms || b.i - a.i).map((x) => x.e)), parity.answer.events);
  assert.equal(parity.answer.messages, 11);
});

test("a RabbitMQ event is named by its AMQP method and carries its system; SQS's names never show", () => {
  const evs = parity.answer.events;
  assert.ok(evs.every((e) => e.system === "rabbitmq"));
  assert.deepEqual([...new Set(evs.map(eventName))].sort(),
    ["Dead-lettered", "Returned to the queue", "basic.ack", "basic.deliver", "basic.nack", "basic.publish", "basic.reject", "basic.return", "connection.lost"]);
  for (const e of evs) assert.doesNotMatch(`${eventName(e)} ${e.detail}`, /SQS|SendMessage|ReceiveMessage|DeleteMessage|visibility|receipt/i, e.key);
  assert.ok(evs.every((e) => e.attrs?.["message.ApproximateReceiveCount"] === undefined), "SQS's attribute is SQS's only");
  // the fanout: one publish to exchange fan, then a delivery and an ack on each queue's copy
  const fan = evs.filter((e) => e.messageId === "~rabbitmq~m-fan").reverse();
  assert.deepEqual(fan.map((e) => `${eventName(e)} ${e.queue}`), ["basic.publish fan", "basic.deliver fan.a", "basic.ack fan.a", "basic.deliver fan.b", "basic.ack fan.b"]);
  const noroute = evs.find((e) => e.call === "basic.return")!;
  assert.equal(noroute.status, "error");
  assert.match(noroute.detail, /^Unroutable: no queue took it, so the broker returned it to the publisher \(312 NO_ROUTE\)$/);
  assert.match(evs.find((e) => e.event === "dead_letter" && e.messageId.endsWith("m-reject"))!.detail, /x-death header/);
});

test("an event kind is named for the systems in view: SQS's, RabbitMQ's, or both", () => {
  assert.equal(kindName("delete", new Set([undefined])), "Delete");
  assert.equal(kindName("delete", new Set(["rabbitmq"])), "Ack");
  assert.equal(kindName("delete", new Set(["rabbitmq", undefined])), "Delete or ack");
  assert.equal(kindName("dead_letter", new Set(["rabbitmq", undefined])), "Moved to dead-letter queue");
  assert.equal(kindName("dead_letter", new Set(["rabbitmq"])), "Dead-lettered");
  assert.equal(eventName({ event: "visibility_expired", system: "rabbitmq" }), "Returned to the queue");
  assert.equal(eventName({ event: "visibility_expired" }), "Visibility timeout expired");
});

test("the System filter and column are offered once a RabbitMQ event is in range, never on SQS's alone", () => {
  const rabbit = parity.answer.events;
  const sqsEvents = rabbit.map(({ system: _s, ...e }) => e as Ev);
  assert.ok(filledColumns(["system"], rabbit).has("system"));
  assert.ok(!filledColumns(["system"], sqsEvents).has("system"));
  assert.equal(cellText("system", rabbit[0]), "RabbitMQ");
  assert.equal(cellText("system", sqsEvents[0]), "Amazon SQS");
  const lang = queryLanguage();
  assert.equal(rabbit.filter((e) => lang.matches(e, lang.parse("system:rabbitmq"))).length, rabbit.length);
  assert.equal(sqsEvents.filter((e) => lang.matches(e, lang.parse("system:rabbitmq"))).length, 0);
});

test("the Event explorer lists RabbitMQ events with their badge, methods and kind names, and keeps the copy rules", () => {
  const now = parity.toMs;
  const html = renderToStaticMarkup(h(EventExplorer, {
    query: "", setQuery: () => {}, time: { from: parity.fromMs, to: now + 1, label: "the fixture's window" }, page: true,
    data: { events: parity.answer.events, now, live: true, capped: false }, messageHref: (id: string) => `/message?id=${id}`,
  }));
  const words = text(html);
  assert.match(html, /data-system="rabbitmq"[^>]*>RabbitMQ</, "the row's badge");
  assert.match(words, /basic\.publish/);
  assert.match(words, /\bm-fan\b/, "the ID as the application set it");
  assert.doesNotMatch(words, /~rabbitm/);
  assert.match(words, /System/, "the System filter");
  assert.match(words, / Ack /, "an event kind in RabbitMQ's words, as the filter lists it");
  assert.doesNotMatch(words, /ReceiveMessage|DeleteMessage|SendMessage|Visibility timeout expired/);
  assert.doesNotMatch(html, / title="|[→←↗↑×]|uppercase|tracking-/);
  assert.doesNotMatch(words, / · |—| – /);
});

/* ── the Overview ───────────────────────────────────────────────────── */

test("the Overview marks a RabbitMQ queue with its badge and words, and draws an SQS queue as before", () => {
  const flows = (amqp as unknown as { flows: Flow[] }).flows;
  const d = overviewData("local", "1h", 60, flows, {}, parity.toMs) as OverviewFlowsData;
  assert.ok(d.queues.length >= 5 && d.queues.every((q) => q.system === "rabbitmq"));
  const copy: OverviewCopy = { noHealth: (n) => `${n} queues`, spansOnly: "", noFigures: () => "Trailbeam makes no AWS calls." };
  const hrefs: OverviewFlowsHrefs = { range: (id) => `/?range=${id}`, queue: ({ name }) => `/queues/${name}`, issues: (q) => `/issues?queue=${q}`, cause: () => "/issues",
    events: (q) => `/events?q=${encodeURIComponent(q)}`, deadLettered: () => "/dlq", message: (id) => `/messages/${id}` };
  const html = renderToStaticMarkup(h(OverviewFlows, { data: d, copy, hrefs }));
  const words = text(html);
  assert.match(html, /data-system="rabbitmq"[^>]*>RabbitMQ</);
  assert.ok(words.includes(RABBITMQ_NO_FIGURES));
  assert.doesNotMatch(words, /SQS|AWS calls|Visibility timeout|maxReceiveCount|Standard/);
  assert.doesNotMatch(html, / title="|[→←↗↑×]|uppercase|tracking-/);
  assert.doesNotMatch(words, / · |—| – /);
  // the DLQ node's note, for a RabbitMQ queue: the x-death header, not SQS
  assert.equal(dlqNodeCount({ real: true, queue: "orders", dlq: "orders.dlq", depth: null, received: 2, moved: 1, range: "the past hour", rabbit: true }).note,
    "Deliveries on orders.dlq in the past hour to your services whose x-death header names orders. 1 message moved to it in the past hour.");
  // an SQS flow has no system
  const sqsFlow = { ...flows[0], key: "https://sqs.us-east-1.amazonaws.com/111122223333/orders", url: "https://sqs.us-east-1.amazonaws.com/111122223333/orders" };
  assert.equal(overviewData("local", "1h", 60, [sqsFlow], {}, parity.toMs).queues[0].system, undefined);
});

/* ── first run and empty states ─────────────────────────────────────── */

test("the first-run screen sets up RabbitMQ: pika or amqplib, a publish with a message_id, in RabbitMQ's words", () => {
  const props = { endpoint: "http://127.0.0.1:4318/v1/traces", auth: "off" as const, status: { kind: "waiting", checkedAgoS: 1, hidden: false } as never, version: "0.5.0", onRuntime: () => {} };
  for (const runtime of ["python", "node"] as const) {
    const html = renderToStaticMarkup(h(FirstRun, { ...props, runtime, system: "rabbitmq" }));
    const words = text(html);
    assert.match(words, /Send your first RabbitMQ message to Trailbeam/);
    assert.match(words, /Publish one message/);
    assert.match(words, runtime === "python" ? /trailbeam\[pika\].*pika 1\.3 or later.*trailbeam\.pika\.install\(\).*message_id= ?"first-message"/ : /npm install trailbeam amqplib.*amqplib 0\.10\.6 or later.*trailbeam\.amqplib\.install\(\).*messageId: ?"first-message"/);
    assert.doesNotMatch(words, /SQS call|boto3|client-sqs|SendMessage|Queue URL|queue URL/);
    assert.match(html, /role="tab" aria-selected="true"[^>]*>RabbitMQ</);
    assert.doesNotMatch(html, / title="|[→←↗↑×]|uppercase|tracking-/);
    assert.doesNotMatch(words, / · |—| – /);
  }
  // SQS is the default, and its setup is SQS's
  const sqsWords = text(renderToStaticMarkup(h(FirstRun, { ...props, runtime: "python" })));
  assert.match(sqsWords, /Send your first SQS message to Trailbeam .*Make one SQS call/);
  assert.match(sqsWords, /Amazon SQS RabbitMQ/, "the switch");
});

test("a RabbitMQ message's bare ID opens it under its key; the empty states name RabbitMQ's terms", () => {
  assert.equal(rabbitKeyOf("m-fan"), "~rabbitmq~m-fan");
  assert.equal(rabbitKeyOf("~rabbitmq~m-fan"), null);
  assert.equal(rabbitKeyOf(""), null);
  const words = (e: { title: unknown; detail?: unknown }) => text(renderToStaticMarkup(h("div", null, e.title as never, e.detail as never)));
  assert.match(words(lookupEmpty("m-x", false)), /message_id property its publisher set/);
  assert.match(words(stuckEmpty(300, false)), /or on RabbitMQ an ack/);
});

test("the Messages list shows a RabbitMQ message's ID as set, with its badge and RabbitMQ's state", () => {
  const items = (amqp.items ?? []) as unknown as MessageRow[];
  const col = (id: string) => MESSAGE_COLUMNS.find((c) => c.id === id)!;
  const fan = items.find((m) => m.message_id === "~rabbitmq~m-fan")!;
  const cell = renderToStaticMarkup(h("div", null, col("message_id").cell(fan)));
  assert.match(cell, />m-fan<.*data-system="rabbitmq"[^>]*>RabbitMQ</);
  assert.equal(col("message_id").text(fan), "m-fan");
  assert.equal(col("state").text(fan), "Done");
  assert.equal(col("state").text(items.find((m) => m.message_id === "~rabbitmq~m-stuck")!), "No ack seen");
  assert.equal(messageLevel(items.find((m) => m.message_id === "~rabbitmq~m-loop")!).word, "Delivered more than once");
});
