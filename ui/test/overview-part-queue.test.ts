/*
  QV-726: the messages under a selected producer, consumer or dead-letter queue on the Overview are read for that
  queue alone (GET /api/timeline-events?queue=<its key>), so a busier queue can't take its places in the 500 read,
  and a capped read with no row says it was capped instead of "No message". Run: npm test
*/
import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import type { Flow } from "../src/queue/selfhosted.ts";
import { partEmptyText, partQueueKeys } from "../shell/overview.tsx";

const flow = (o: Partial<Flow> & Pick<Flow, "key" | "name">): Flow => ({
  queue: o.name, url: null, account: "123456789012", region: "eu-west-1", lastSeen: 0, sent: 0, received: 0, deleted: 0,
  visibilityChanges: 0, producers: [], consumers: [], avgBytes: null, dlq: null, ...o,
});

const ORDERS = flow({ key: "123456789012/eu-west-1/orders", name: "orders", dlq: { name: "orders-dlq", received: 2 } });
const DLQ = flow({ key: "123456789012/eu-west-1/orders-dlq", name: "orders-dlq" });
const ELSEWHERE_DLQ = flow({ key: "210987654321/eu-west-1/orders-dlq", name: "orders-dlq", account: "210987654321" });

test("a producer or consumer is read on its queue's key alone", () => {
  assert.deepEqual(partQueueKeys([ORDERS, DLQ], ORDERS, "producer"), [ORDERS.key]);
  assert.deepEqual(partQueueKeys([ORDERS, DLQ], ORDERS, "consumer"), [ORDERS.key]);
});

test("a dead-letter queue is read with the dead-letter queue's key too, the one in the same account and region", () => {
  assert.deepEqual(partQueueKeys([ORDERS, ELSEWHERE_DLQ, DLQ], ORDERS, "dlq"), [ORDERS.key, DLQ.key]);
  assert.deepEqual(partQueueKeys([ORDERS, ELSEWHERE_DLQ], ORDERS, "dlq"), [ORDERS.key]);
  assert.deepEqual(partQueueKeys([flow({ key: "q", name: "q" })], flow({ key: "q", name: "q" }), "dlq"), ["q"]);
});

test("no row: a capped read says it was capped, an uncapped one that nothing is in the range", () => {
  assert.equal(partEmptyText("producer", false, "the last hour"), "No message it sent here has an event in the last hour.");
  assert.equal(partEmptyText("producer", true, "the last hour"), "No message it sent here is among the 500 most recently active on this queue in the last hour.");
  assert.equal(partEmptyText("consumer", true, "the last hour"), "No message it received from here is among the 500 most recently active on this queue in the last hour.");
  assert.equal(partEmptyText("dlq", false, "the last day"), "No message moved to the dead-letter queue has an event in the last day.");
});

test("the panel asks the server for its queue's messages", () => {
  const src = readFileSync(new URL("../shell/overview.tsx", import.meta.url), "utf8");
  assert.match(src, /\/api\/timeline-events\?\$\{qs\(\{ \.\.\.timelineEventsQuery\([^)]*\), queue: keys\.join\(","\)/);
  assert.match(src, /keys=\{partQueueKeys\(flows, f, at\.part\)\}/);
});
