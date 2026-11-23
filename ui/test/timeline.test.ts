/*
  The shared timeline, rendered to HTML on the server, for timelines Trailbeam really returned
  (fixtures/timelines.json, from make-fixtures.py). Every step must appear, in the API's order, and the
  one-sentence summary must say what happened. Run: npm test
*/
import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { TimelineView, Node, SourceTag } from "../src/timeline/Timeline.tsx";
import { answerFor } from "../src/summary.ts";
import type { Timeline, TimelineEvent } from "../src/timeline/types.ts";

const { timelines } = JSON.parse(readFileSync(new URL("./fixtures/timelines.json", import.meta.url), "utf8")) as { timelines: Timeline[] };
const render = (t: Timeline) => renderToStaticMarkup(createElement(TimelineView, { t, answer: answerFor(t) }));
const text = (html: string) => html.replace(/<[^>]+>/g, " ").replace(/&#x27;/g, "'").replace(/\s+/g, " ");

test("every step of every recorded timeline is drawn, in the API's order", () => {
  assert.ok(timelines.length >= 3);
  for (const t of timelines) {
    const html = text(render(t));
    let at = -1;
    for (const e of t.events) {
      const i = html.indexOf(` ${e.id} `, at + 1);
      assert.ok(i > at, `${t.message_id}: step ${e.id} (${e.type}) is drawn after the step before it`);
      at = i;
    }
    assert.ok(!/undefined|NaN/.test(html), `${t.message_id}: no undefined or NaN in the page`);
  }
});

test("an inferred step is marked inferred, and a message without a group still shows", () => {
  const t = timelines.find((x) => x.events.some((e) => e.inferred))!;
  const html = render(t);
  assert.match(html, /inferred/);
  const lone = t.events.find((e) => e.group === null);
  if (lone) assert.ok(text(html).includes(` ${lone.id} `), "a step placed between groups is drawn");
});

test("inferred steps are drawn in neutral Haze with dashes, never the failure red (QV-211)", () => {
  const inferred = { id: "e3", inferred: true, type: "visibility_timeout_expired", group: null, time: null,
    window: { from: "2026-01-01T00:00:01Z", to: "2026-01-01T00:00:31Z" }, reason: "", queue: "orders", after: [] } as unknown as TimelineEvent;
  const node = renderToStaticMarkup(createElement(Node, { e: inferred }));
  assert.match(node, /stroke="var\(--color-sky-ink-3\)"/);
  assert.match(node, /stroke-dasharray/);
  assert.doesNotMatch(node, /lost/);
  const tag = renderToStaticMarkup(createElement(SourceTag, { kind: "inferred", children: "inferred" }));
  assert.match(tag, /border-dashed border-sky-ink-3/);
  assert.doesNotMatch(tag, /lost/);
  for (const t of timelines.filter((x) => x.events.some((e) => e.inferred))) {
    const html = render(t);
    assert.doesNotMatch(html, /border-dashed border-lost|text-lost[^"]*">\s*inferred/, `${t.message_id}: no red on inferred marks`);
  }
});

test("failures stay red: a failed delete, a dead-letter move, a failed handler (QV-211)", () => {
  const base = { group: 1, time: "2026-01-01T00:00:01Z", start: null, end: "2026-01-01T00:00:01Z", start_observed: false,
    queue: "orders", consumer: { service_name: "orders-worker" }, after: [] };
  const red = (e: object) => renderToStaticMarkup(createElement(Node, { e: e as TimelineEvent })).includes("var(--color-lost)");
  assert.ok(red({ ...base, id: "e1", inferred: false, type: "deleted", delivery: 1, result: "failed", status: "failed" }), "failed delete");
  assert.ok(red({ ...base, id: "e2", inferred: false, type: "lambda_outcome", outcome: "failed_exception" }), "failed handler");
  assert.ok(red({ id: "e3", inferred: false, type: "moved_to_dead_letter_queue", group: null, time: null, window: { from: null, to: null },
    reason: "", queue: "orders", after: [], source: "sqs_dlq_source" }), "DLQ move");
  assert.ok(!red({ ...base, id: "e4", inferred: false, type: "deleted", delivery: 1, result: "ok", status: "final" }), "a good delete is not red");
});

test("the summary says what happened", () => {
  const by = Object.fromEntries(timelines.map((t) => [t.deliveries, answerFor(t)]));
  assert.equal(by[0], "Sent to trailbeam-smoke and not received yet.");
});

const twice = (has: (t: Timeline) => boolean) => timelines.find((t) => t.deliveries === 2 && has(t))!;
const released = twice((t) => t.events.some((e) => e.type === "visibility_changed"));
const expired = twice((t) => !t.events.some((e) => e.type === "visibility_changed"));

test("the summary says a message released with ChangeMessageVisibility 0 was received again", () => {
  assert.equal(answerFor(released), "Released with ChangeMessageVisibility after receive 1, received again by trailbeam-smoke-worker and deleted.");
  assert.ok(text(render(released)).includes("Released with ChangeMessageVisibility after receive 1"), "the headline is on the page");
});

test("the summary says the visibility timeout expired when no visibility change was seen", () => {
  assert.equal(answerFor(expired), "The visibility timeout expired during receive 1; received again by trailbeam-smoke-worker and deleted.");
});

test("other repeat-receive headlines are unchanged", () => {
  // A change to a non-zero timeout is not a release; without an inferred expiry there is no reason to give.
  const extended: Timeline = {
    ...expired,
    events: expired.events.flatMap((e) =>
      e.type === "visibility_timeout_expired" ? [] : e.type === "received" && e.delivery === 1
        ? [e, { ...released.events.find((x) => x.type === "visibility_changed")!, visibility_timeout_s: 30 } as Timeline["events"][number]]
        : [e]),
  };
  assert.equal(answerFor(extended), "Received 2 times by trailbeam-smoke-worker, then deleted.");
  // Three receives keep the count sentence.
  assert.equal(answerFor({ ...expired, deliveries: 3 }), "Received 3 times by trailbeam-smoke-worker, then deleted.");
});

test("between steps there is only the rail, and the record keeps the order evidence (QV-212)", () => {
  const t = timelines.find((x) => x.events.some((e) => e.type === "visibility_changed" && e.delivery === 1))!;
  const html = render(t);
  const page = text(html);
  assert.ok(!/placed after|↓/.test(page), "no connector text between steps");
  assert.ok(!/visibility change of \d/.test(page), "no clipped 'visibility change of 1'");
  assert.match(html, /data-connector=/, "the rail between steps is still drawn");
  const vc = t.events.find((e) => e.type === "visibility_changed")!;
  if (t.events.some((e) => e.after.some((a) => a.event === vc.id))) {
    assert.ok(page.includes(`After ${vc.id} (the visibility change on delivery 1)`), "the record names the visibility change in full");
  }
});

test("the default-key notice is drawn once in the card, with the command as code and a link to why", () => {
  const t: Timeline = { ...timelines[0], notices: [{ code: "default_key", message:
    "These spans use the package's built-in receipt-hash key, which anyone can read in the source. Generate one key with `openssl rand -hex 32` and set it as `TRAILBEAM_RECEIPT_HASH_KEY` on every service that uses these queues." }] };
  const html = render(t);
  assert.equal(html.split("built-in receipt-hash key").length - 1, 1, "one copy of the notice");
  assert.match(html, /<code[^>]*>openssl rand -hex 32<\/code>/);
  assert.match(html, /<code[^>]*>TRAILBEAM_RECEIPT_HASH_KEY<\/code>/);
  assert.ok(!html.includes("`"), "no literal backticks");
  assert.match(html, /href="https:\/\/github.com\/nick-lennox\/trailbeam#add-it-to-your-services"[^>]*>Why the key matters</);
});

// QV-247: the summary and the state say no more than the timeline shows.
const two = timelines.find((t) => t.deliveries === 2)!;
const received = two.events.filter((e) => e.type === "received");

test("a dead-lettered message counts its receives on the source queue, not the receive from the DLQ", () => {
  const dlqReceive = { ...received[1], id: "dlq", queue: "orders-dlq", delivery: 3 } as TimelineEvent;
  const moved = { ...two, source_queue: two.source_queue, current_queue: "orders-dlq", in_dead_letter_queue: true, dead_letter_queue_move: "observed" as const,
    deliveries: 3, events: [...two.events, dlqReceive] };
  assert.equal(answerFor(moved), `Moved to the dead-letter queue orders-dlq after 2 observed receives on ${two.source_queue}.`);
  const none = { ...moved, events: [dlqReceive], deliveries: 1 };
  assert.equal(answerFor(none), `Moved from ${two.source_queue} to the dead-letter queue orders-dlq. No receive on ${two.source_queue} was observed.`);
});

test("maxReceiveCount reached: the DLQ move is marked inferred, and the last receive may still delete it", () => {
  const t = { ...two, state: "not_deleted" as const, dead_letter_queue_move: "not_seen_on_dlq_yet" as const };
  const a = answerFor(t);
  assert.equal(a, "SQS's receive count reached its queue's maxReceiveCount. Unless the last receive deletes it, SQS moves it to the dead-letter queue (inferred); not seen there yet.");
  assert.doesNotMatch(a, /more than/);
});

test("no delete yet is not drawn as lost; being handled is marked inferred", () => {
  const open = { ...two, state: "not_deleted" as const, in_dead_letter_queue: false, dead_letter_queue_move: null };
  assert.match(answerFor(open), /^Received 2 times by .+\. No delete seen yet\.$/);
  const html = render(open);
  assert.ok(html.includes('<span class="text-ember">No delete seen</span>') && !html.includes("Not deleted"), "amber, not the red of a lost message");
  const busy = { ...open, state: "processing" as const };
  assert.match(answerFor(busy), /^Probably being handled by .+ now \(inferred\): the Lambda invocation has no outcome yet and started less than the Lambda timeout ago\.$/);
  assert.ok(render(busy).includes('<span class="text-ember">Being handled (inferred)</span>'));
  const dead = { ...open, in_dead_letter_queue: true };
  assert.ok(render(dead).includes('<span class="text-lost">No delete seen, in the DLQ</span>'), "a message in the DLQ stays red");
});

test("a receive from a dead-letter queue is titled as one, not as a retry (QV-244)", () => {
  // contract-3.2-timelines.json is what Trailbeam's server returns for the Node.js package's recorded spans
  const all = (JSON.parse(readFileSync(new URL("./fixtures/contract-3.2-timelines.json", import.meta.url), "utf8")) as { timelines: Timeline[] }).timelines;
  const t = all.find((x) => x.message_id === "m-pay-2")!;
  const dlq = t.events.find((e) => e.type === "received" && e.queue === "payments-dlq")! as TimelineEvent & { dlq_source_arn?: string };
  assert.equal(dlq.dlq_source_arn, "arn:aws:sqs:us-east-1:123456789012:payments", "the server puts the ARN on the receive");
  const html = text(renderToStaticMarkup(createElement(TimelineView, { t, answer: answerFor(t), openSteps: [dlq.id] })));
  assert.ok(html.includes("Picked up from the dead-letter queue"));
  assert.ok(!html.includes(`Picked up, attempt ${(dlq as { receive_count: number }).receive_count}`));
  assert.ok(html.includes("DeadLetterQueueSourceArn"), "the detail row shows the ARN");
});
