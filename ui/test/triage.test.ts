/*
  The triage rules (src/model/triage.ts, arrivals.ts; QV-593) on the cases in ../contract/fixtures/triage-parity.json,
  which the server's trailbeam.server.triage writes and runs (python/tests/server/test_triage.py), and which the
  hosted app runs on its own copies of the rules (its test/triage-parity.test.ts, from a copy of the file until the
  @trailbeam/ui it vendors ships it): the ignore and regression rules, which row a cause shows, the filters and the
  note checks are the same in both products. Then the words, which only this side has.
  Run: npm test
*/
import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import {
  OPEN, filterOf, inFilter, newSince, noteProblem, rowFor, sameCause, statusLine, statusTag, messageLine, regressionText,
  activityText, controlAria, noteParts, hiddenText, triageOf, type Arrival, type Filter, type People, type Triage, type TriageRow,
} from "../src/model/index.ts";

const F = JSON.parse(readFileSync(new URL("../../contract/fixtures/triage-parity.json", import.meta.url), "utf8"));

// JSON has no NaN: the fixture's null seq or ms is NaN (no span seq, no evidence time)
const arrivals = (a: { mid: string; seq: number | null; ms: number | null; version: string | null }[]): Arrival[] =>
  a.map((x) => ({ ...x, seq: x.seq ?? NaN, ms: x.ms ?? NaN }));
// and the result as JSON.stringify writes it, as the server's answers are
const json = (v: unknown) => JSON.parse(JSON.stringify(v));

test("triageOf: the state a stored row is in now, as the server's", () => {
  for (const c of F.triageOf) {
    const g = c.group ? { id: c.group.id, arrivals: arrivals(c.group.arrivals) } : null;
    assert.deepEqual(json(triageOf(c.row, g, c.now)), c.expected, c.name);
  }
});

test("newSince: the failures after a resolve or ignore", () => {
  for (const c of F.newSince) {
    const got = newSince(arrivals(c.arrivals), c.sinceSeq, c.atMs, c.except ? new Set(c.except) : undefined);
    assert.deepEqual(got.map((a) => a.mid), c.expected, c.name);
  }
});

test("rowFor and sameCause: which row a cause shows across the merge and split of error texts", () => {
  for (const c of F.rowFor) assert.equal(rowFor(c.rows, c.key)?.causeKey ?? null, c.expected, c.name);
  for (const c of F.sameCause) assert.equal(sameCause(c.r, c.g), c.expected, `${c.r} ${c.g}`);
});

test("filters", () => {
  for (const c of F.inFilter) assert.equal(inFilter(c.triage, c.filter as Filter, c.me, new Set(c.members)), c.expected, `${c.name} ${c.filter}`);
  for (const c of F.filterOf) assert.equal(filterOf(c.raw), c.expected, String(c.raw));
});

test("noteProblem", () => {
  for (const c of F.noteProblem) assert.equal(noteProblem(c.body), c.expected, c.name);
});

// ── the words ────────────────────────────────────────────────────────────
const NOW = Date.UTC(2025, 10, 24, 16, 0);
const P: People = { me: "me", names: { me: "Kim", ana: "Ana", sam: "Sam", gone: "Lee" }, members: ["me", "ana", "sam"] };
const t = (o: Partial<Triage>): Triage => ({ ...OPEN, ...o });

test("the status words", () => {
  assert.equal(statusLine(t({}), P, NOW), "Open. Nobody assigned.");
  assert.equal(statusLine(t({ status: "resolved", statusBy: "ana", statusAt: NOW - 2 * 3_600_000, assignee: "sam" }), P, NOW), "Resolved by Ana 2 h ago. Assigned to Sam.");
  assert.equal(statusLine(t({ status: "ignored", statusBy: "me", more: 10, seen: 3 }), P, NOW), "Ignored by you until 10 more messages: 3 so far. Nobody assigned.");
  assert.equal(statusTag(t({ status: "open", regressed: true }), NOW)?.word, "Regressed");
  assert.equal(statusTag(t({ status: "ignored", more: 100 }), NOW)?.word, "Ignored for 100 more");
  assert.equal(statusTag(t({}), NOW), null);
  assert.equal(regressionText(t({ regressed: true, regressedAt: NOW, regressedCount: 4, statusBy: "ana" }), P, NOW).replace(/\d\d:\d\d/, "HH:MM"),
    "Regressed HH:MM: 4 new failures after Ana resolved it.");
  assert.equal(messageLine(t({ status: "resolved", statusBy: "me", assignee: "gone", notes: 1 }), P, NOW), "Resolved by you, Lee (left) is on it, 1 note");
  assert.equal(controlAria("billing timed out", t({ assignee: "me" }), P, NOW), "billing timed out: assigned to you. Triage.");
  assert.equal(hiddenText(2, 1), "2 resolved and 1 ignored causes aren't shown.");
  assert.equal(hiddenText(0, 0), "");
});

test("the thread's words", () => {
  const e = (kind: string, actor: string | null, data: Record<string, unknown> = {}) =>
    ({ id: "e", kind, actor, createdAt: NOW, body: null, messageId: null, data, editedAt: null, deletedAt: null, deletedBy: null }) as Parameters<typeof activityText>[0];
  assert.equal(activityText(e("assigned", "sam", { assignee: "sam" }), P), "Sam assigned this to themself.");
  assert.equal(activityText(e("assigned", "me", { assignee: "me" }), P), "You assigned this to yourself.");
  assert.equal(activityText(e("ignored", "ana", { forS: 86_400 }), P), "Ana ignored this for 24 h.");
  assert.equal(activityText(e("ignore_ended", null, { why: "count", more: 10, by: "ana" }), P), "Back: 10 more messages since Ana ignored it.");
  assert.equal(activityText(e("regressed", null, { count: 2, by: "ana", version: "1.4.4" }), P), "Regressed: 2 new failures after Ana resolved it, seen in 1.4.4.");
  assert.deepEqual(noteParts("see https://x.test/a. and 0a1b2c3d-0000-4000-8000-00000000abcd"),
    [{ text: "see " }, { url: "https://x.test/a" }, { text: ". and " }, { mid: "0a1b2c3d-0000-4000-8000-00000000abcd" }]);
});

test("a row the server sent is a TriageRow", () => {
  const r: TriageRow = F.triageOf.find((c: { row: TriageRow | null }) => c.row).row;
  assert.equal(typeof r.updatedAt, "number");
});
