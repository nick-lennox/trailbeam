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

test("a FIFO send SQS deduplicated says so, with the reason, and is not another plain send (QV-246)", () => {
  const base = { inferred: false, group: 0, start: null, start_observed: false, queue: "py-fifo.fifo",
    consumer: { service_name: "orders-api" }, after: [], process_order: null };
  const reason = "SQS returned the same MessageId: this send was deduplicated and enqueued nothing";
  const t = {
    message_id: "m-fifo", state: "not_deleted", source_queue: "py-fifo.fifo", current_queue: "py-fifo.fifo",
    in_dead_letter_queue: false, dead_letter_queue_move: null, send_observed: true, deliveries: 0, successful_deliveries: 0,
    first_seen: "2026-01-01T00:00:01Z", first_seen_clock: "host", last_seen: "2026-01-01T00:00:02Z", concurrent_groups: 0,
    groups: [{ group: 0, concurrent: false, label: null, observed_events: 1, inferred_events: 0, ordered_inside: 0, start: null, end: "2026-01-01T00:00:01Z" },
             { group: 1, concurrent: false, label: null, observed_events: 1, inferred_events: 0, ordered_inside: 0, start: null, end: "2026-01-01T00:00:02Z" }],
    events: [
      { ...base, id: "e1", type: "sent", time: "2026-01-01T00:00:01Z", end: "2026-01-01T00:00:01Z" },
      { ...base, id: "e2", group: 1, type: "sent", time: "2026-01-01T00:00:02Z", end: "2026-01-01T00:00:02Z", deduplicated: true, reason },
    ],
    order: { edges: [], edges_by_source: {}, conflicts: [] }, notices: [],
  } as unknown as Timeline;
  const html = text(render(t));
  assert.ok(html.includes("Sent to py-fifo.fifo"), html);
  assert.ok(html.includes("Sent again to py-fifo.fifo, deduplicated by SQS"));
  assert.ok(html.includes(reason));
  assert.ok(html.includes("deduplicated · enqueued nothing") && !html.includes("e2 Sent to"), "the second send is not drawn as a plain send");
});
