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
