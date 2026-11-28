/*
  Redrive's words and views (QV-590), shared by both products. The words' tests moved here from the hosted app's
  test/redrive.test.ts with the words; the gate and the AWS calls are each host's (the self-hosted server's in
  python/tests/server/test_redrive.py). The views: the button off with its reason, the confirmation, the log, and
  the app's copy rules. Run: npm test
*/
import { test } from "node:test";
import assert from "node:assert/strict";
import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { counted, freshWarning, moveWords, nextCheck, redrivenNotice } from "../src/redrive/words.ts";
import { Redrive, RedriveLog, RedriveSelect, cancellable, entryState } from "../src/redrive/Redrive.tsx";
import type { RedriveActions, RedriveEntry } from "../src/redrive/types.ts";

test("the confirmation warns when a message reached the dead-letter queue in the last minute", () => {
  assert.match(freshWarning("orders-dlq", 100_000, 105_000)!, /reached orders-dlq 5 s ago.*wait a minute/);
  assert.equal(freshWarning("orders-dlq", 100_000, 160_000), null);
  assert.equal(freshWarning("orders-dlq", null, 1), null);
});

test("a move SQS calls COMPLETED is believed only once the dead-letter queue is seen empty a minute later", () => {
  const row = (o: Partial<Parameters<typeof moveWords>[0]> = {}) => ({ status: "COMPLETED", moved: 2, toMove: 2, failure: null, statusAt: 10_000, left: null, leftAt: null, ...o });
  // when to read what
  assert.equal(nextCheck(row({ status: "RUNNING" }), 0), "task");
  assert.equal(nextCheck(row(), 30_000), "wait");
  assert.equal(nextCheck(row(), 70_000), "depth");
  assert.equal(nextCheck(row({ status: "CANCELLED" }), 70_000), "depth");
  assert.equal(nextCheck(row({ left: 0, leftAt: 70_000 }), 80_000), "done");
  assert.equal(nextCheck(row(), 10_000 + 16 * 60_000), "done");
  // ToMove is the move's size, not what is left
  assert.equal(counted({ moved: 1, toMove: 4 }), "1 of 4 moved");
  assert.equal(counted({ moved: 0, toMove: 0 }), "SQS counted 0 messages to move");
  // the words
  assert.equal(moveWords(row(), "orders-dlq", 30_000).state, "checking");
  assert.match(moveWords(row(), "orders-dlq", 30_000).detail!, /reads orders-dlq's depth a minute later/);
  assert.equal(moveWords(row(), "orders-dlq", 10_000 + 16 * 60_000).state, "unchecked");
  assert.deepEqual(moveWords(row({ left: 0, leftAt: 70_000 }), "orders-dlq", 80_000), { state: "emptied", word: "Done", detail: "orders-dlq was empty a minute after the move finished." });
  // measured: "0 of 4" with all 4 back on the source queue
  assert.match(moveWords(row({ moved: 0, toMove: 4, left: 0, leftAt: 70_000 }), "orders-dlq", 80_000).detail!, /SQS counted 0 of 4 moved; its counts are approximate/);
  // measured: "0 of 0" started 5 s after 2 messages arrived, both still there
  const zero = moveWords(row({ moved: 0, toMove: 0, left: 2, leftAt: 70_000 }), "orders-dlq", 80_000);
  assert.equal(zero.state, "left");
  assert.match(zero.detail!, /finished \(SQS counted 0 messages to move\), but a minute later it still held 2 messages\..*Redrive again/);
  // measured: messages sent straight to a dead-letter queue have no source; SQS's code is explained, not shown raw
  const nosrc = moveWords(row({ status: "FAILED", moved: 0, toMove: 3, failure: "CouldNotDetermineMessageSource" }), "orders-dlq", 80_000);
  assert.equal(nosrc.state, "failed");
  assert.equal(nosrc.word, "Failed, 0 of 3 moved");
  assert.match(nosrc.detail!, /^SQS did not move these messages to orders-dlq itself, so they have no source queue to go back to\..*still in orders-dlq\. \(SQS: CouldNotDetermineMessageSource\)$/);
  assert.equal(moveWords(row({ status: "FAILED", failure: "SomethingElse" }), "orders-dlq", 80_000).detail, "SQS stopped the move: SomethingElse.");
  // measured: cancel is a request, not a stop
  const asked = moveWords(row({ status: "CANCELLING", moved: 1, failure: "Cancel requested by ana@example.com." }), "orders-dlq", 80_000);
  assert.deepEqual([asked.state, asked.word], ["moving", "Cancel requested"]);
  assert.match(asked.detail!, /^Cancel requested by ana@example\.com\. SQS can keep moving messages after a cancel/);
  assert.equal(nextCheck(row({ status: "CANCELLING" }), 80_000), "task");
  const ended = moveWords(row({ status: "CANCELLED", moved: 4, toMove: 4, failure: "Cancel requested by ana@example.com.", left: 0, leftAt: 70_000 }), "orders-dlq", 80_000);
  assert.equal(ended.word, "Cancelled, 4 of 4 moved");
  assert.match(ended.detail!, /orders-dlq was empty a minute later, so every message moved before the cancel took hold/);
});

test("a redriven message's timeline says the whole queue moved and the message has a new ID, without guessing which", () => {
  const dlq = { name: "orders-dlq", account: "111111111111", region: "us-east-1" };
  const row = (o: Partial<Parameters<typeof redrivenNotice>[0][number]> = {}) => ({ dlqName: "orders-dlq", accountId: "111111111111", region: "us-east-1",
    messageIds: [] as string[], requestedBy: "u1", requestedByEmail: "ana@example.com", requestedAt: 2_000, outcome: "started" as const, status: "COMPLETED", moved: 2, toMove: 2,
    failure: null, statusAt: 3_000, left: 0, leftAt: 63_000, ...o });
  const m = { messageId: "m1", dlq, lastSeen: 1_000 };
  const now = 70_000;
  // not picked, but in the queue when the move started, and the queue was empty a minute after: it moved too
  const done = redrivenNotice([row()], m, now);
  assert.match(done!.message, /moved every message in orders-dlq back to its source queue, this one included/);
  assert.match(done!.message, /new message ID/);
  assert.match(done!.message, /ana@example\.com/);
  // COMPLETED alone is not believed
  assert.match(redrivenNotice([row({ left: null, leftAt: null })], m, 30_000)!.message, /SQS reports the move finished \(2 of 2 moved\).*may be back on its source queue under a new message ID, or still here/);
  assert.match(redrivenNotice([row({ left: 2 })], m, now)!.message, /still held 2 messages.*or still here/);
  // arrived after the move started and not picked: not moved
  assert.equal(redrivenNotice([row()], { ...m, lastSeen: 3_000 }, now), null);
  // picked: moved, whatever the clocks say
  assert.ok(redrivenNotice([row({ messageIds: ["m1"] })], { ...m, lastSeen: 3_000 }, now));
  // refused or failed to start, another queue, another account: nothing moved
  assert.equal(redrivenNotice([row({ outcome: "refused" }), row({ outcome: "failed" }), row({ dlqName: "billing-dlq" }), row({ accountId: "222222222222" })], m, now), null);
  assert.match(redrivenNotice([row({ status: "RUNNING", moved: 1 })], m, now)!.message, /is moving every message .*\(1 of 2 moved\)/);
  assert.match(redrivenNotice([row({ status: "CANCELLED", moved: 1 })], m, now)!.message, /stopped \(cancelled\).*may be back on its source queue/);
  assert.match(redrivenNotice([row({ status: "FAILED", moved: 0, toMove: 3, failure: "CouldNotDetermineMessageSource" })], m, now)!.message, /failed \(0 of 3 moved\)\. SQS did not move these messages to orders-dlq itself/);
  assert.match(redrivenNotice([row({ status: "CANCELLING" })], m, now)!.message, /was asked to cancel\. SQS can keep moving/);
  // the latest redrive wins
  assert.match(redrivenNotice([row({ status: "CANCELLED", requestedAt: 1_500 }), row({ requestedAt: 2_500 })], m, now)!.message, /moved every message/);
});


/* ── the views ───────────────────────────────────────────────────────── */

const actions: RedriveActions = {
  preview: async () => ({ ok: true, preview: { visible: 3, inFlight: 0, running: null } }),
  start: async () => ({ ok: true }),
  cancel: async () => ({ ok: true }),
};
const dlq = { name: "orders-dlq", account: "123456789012", region: "eu-west-1" };
const html = (el: Parameters<typeof renderToStaticMarkup>[0]) => renderToStaticMarkup(el);
/* the app's copy rules: no em dash, no arrow, no native title, no "a · b" fragment */
const rules = (s: string) => assert.doesNotMatch(s, /—|→|title="|·/);

test("the button is off with the gate's reason and the docs link until the gate opens", () => {
  const off = html(createElement(Redrive, { label: "Redrive whole DLQ", actions, docsHref: "/docs/redrive", onChanged: () => {},
    targets: [{ dlq, messageIds: ["m1"], gate: { ok: false, reason: "Redrive is not turned on for orders-dlq." } }] }));
  assert.match(off, /disabled=""/);
  assert.match(off, /Redrive is not turned on for orders-dlq\./);
  assert.match(off, /href="\/docs\/redrive"[^>]*>How redrive works</);
  rules(off);
  const on = html(createElement(Redrive, { label: "Redrive whole DLQ", actions, onChanged: () => {},
    targets: [{ dlq, messageIds: ["m1"], gate: { ok: true } }] }));
  assert.doesNotMatch(on, /disabled=""/);
  assert.match(on, /Moves every message in the dead-letter queue, not one\. Moved messages get new message IDs\./);
  rules(on);
  // nothing picked on Stuck
  assert.match(html(createElement(Redrive, { label: "x", actions, onChanged: () => {}, targets: [] })), /Select a dead-letter queue to redrive\./);
});

test("the dead-letter queues to pick from: a box for each, off where the gate is shut, the place in mono", () => {
  const groups = [
    { key: "a", name: "orders-dlq", account: "123456789012", region: "eu-west-1", messageIds: ["m1", "m2"], sources: ["orders"], newest: 1, gate: { ok: true as const } },
    { key: "b", name: "billing-dlq", account: null, region: null, messageIds: ["m3"], sources: ["billing"], newest: 1, gate: { ok: false as const, reason: "Redrive is not turned on for billing-dlq." } },
  ];
  const out = html(createElement(RedriveSelect, { groups, nowMs: 10, actions, onChanged: () => {} }));
  assert.match(out, /aria-label="Select orders-dlq"/);
  assert.match(out, /<input type="checkbox" disabled="" aria-label="Select billing-dlq"|aria-label="Select billing-dlq"[^>]*disabled=""|disabled=""[^>]*aria-label="Select billing-dlq"/);
  assert.match(out, /<span class="font-mono">123456789012<\/span> in <span class="font-mono">eu-west-1<\/span>/);
  assert.match(out, /Redrive is not turned on for billing-dlq\./);
  assert.match(out, /Redrive whole DLQ/);
  // the list itself is the shared DeadLetterQueues (its own markup); what redrive adds keeps the rules
  const added = out.slice(out.indexOf('<span class="ml-auto">'));
  assert.doesNotMatch(added, /—|→|title="|·/);
});

const entry = (o: Partial<RedriveEntry> = {}): RedriveEntry => ({
  id: "e1", dlqName: "orders-dlq", dlqArn: "arn:aws:sqs:eu-west-1:123456789012:orders-dlq", accountId: "123456789012", region: "eu-west-1",
  messageIds: ["m1"], requestedBy: "Ana", requestedByEmail: "ana@example.com", requestedAt: 1_000, outcome: "started", detail: null,
  taskHandle: "h-1", status: "RUNNING", moved: 1, toMove: 3, failure: null, statusAt: 1_000, left: null, leftAt: null, ...o,
});

test("the log: each entry's state in words, Cancel only on a running move and for those who may", () => {
  const now = 120_000;
  assert.deepEqual(entryState(entry(), now), { tone: "warning", word: "Moving, 1 of 3 moved", detail: null });
  assert.deepEqual(entryState(entry({ outcome: "refused", status: null, taskHandle: null, detail: "A redrive of orders-dlq is already running: 0 of 3 moved." }), now),
    { tone: "idle", word: "Not started", detail: "A redrive of orders-dlq is already running: 0 of 3 moved." });
  assert.equal(entryState(entry({ outcome: "failed", detail: "AWS denied sqs:StartMessageMoveTask on orders-dlq." }), now).word, "Could not start");
  assert.equal(entryState(entry({ status: "COMPLETED", moved: 3, left: 0, leftAt: 70_000 }), now).word, "Done");
  assert.equal(cancellable(entry()), true);
  assert.equal(cancellable(entry({ status: "CANCELLING" })), false);
  assert.equal(cancellable(entry({ taskHandle: null })), false);

  const out = html(createElement(RedriveLog, { entries: [entry(), entry({ id: "e2", status: "COMPLETED", taskHandle: null, moved: 3, left: 0, leftAt: 70_000 })], now, canCancel: true, actions, onChanged: () => {} }));
  assert.equal(out.match(/>Cancel</g)?.length, 1);
  assert.match(out, /ana@example\.com/);
  assert.match(out, /Moving, 1 of 3 moved/);
  rules(out);
  assert.doesNotMatch(html(createElement(RedriveLog, { entries: [entry()], now, canCancel: false, actions })), />Cancel</);
  assert.match(html(createElement(RedriveLog, { entries: [], now, canCancel: true, actions })), /No redrive has been asked for yet\./);
});

test("the words keep the copy rules too", () => {
  const words = [
    moveWords({ status: "COMPLETED", moved: 0, toMove: 0, failure: null, statusAt: 0, left: 2, leftAt: 60_000 }, "orders-dlq", 70_000).detail!,
    moveWords({ status: "FAILED", moved: 0, toMove: 3, failure: "CouldNotDetermineMessageSource", statusAt: 0, left: null, leftAt: null }, "orders-dlq", 70_000).detail!,
    freshWarning("orders-dlq", 0, 5_000)!,
    redrivenNotice([{ ...entry(), status: "COMPLETED", left: 0, leftAt: 63_000, statusAt: 2_000 }], { messageId: "m1", dlq, lastSeen: 0 }, 70_000)!.message,
  ];
  for (const w of words) rules(w);
  assert.equal(counted({ moved: null, toMove: 3 }), null);
});
