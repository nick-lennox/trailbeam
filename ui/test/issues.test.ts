/*
  QV-583 (One UI 5): the Issues pages' view model (@trailbeam/ui/model issues.ts and stuck-age.ts), moved here from the
  hosted app so both products' Issues pages are made by the same functions. Both backends' groups and messages for the
  same spans give the same issues: test/viewmodel.test.ts. Run: npm test
*/
import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import {
  AGE_CHOICES, CHECKS, EVIDENCE, STAGE_OF, STUCK_AFTER_S, ageLabel, ageParam, ageText, issueOf, issuePageKind, issuesOfPage, severity, stuckAge, withAge,
  type IssueGroup, type IssueItem,
} from "../src/model/index.ts";
import type { CausePage } from "../src/causes/types.ts";

const T0 = Date.UTC(2026, 10, 27, 12, 0);
const NOW = T0 + 3_600_000;

test("every cause kind has a stage, evidence and checks", () => {
  for (const k of Object.keys(STAGE_OF) as (keyof typeof STAGE_OF)[]) {
    assert.ok(EVIDENCE[k], k);
    assert.ok(CHECKS[k]?.length, k);
  }
  assert.deepEqual(Object.keys(EVIDENCE).sort(), Object.keys(STAGE_OF).sort());
});

const group = (o: Partial<IssueGroup> = {}): IssueGroup => ({
  id: "c1", kind: "exc", label: "TimeoutError", why: null, inferred: false, queue: "orders", consumer: "billing-fn", tenants: [], versions: [],
  counts: { dead_lettered: 1, stuck: 1, filtered: 0 }, count: 2, firstMs: T0, lastMs: T0 + 9 * 60_000, times: [T0 + 60_000, T0 + 9 * 60_000], sampleMid: "A", ...o,
});
const item = (id: string, lastMin: number, dlq: boolean): IssueItem => ({
  message_id: id, cause: "c1", current_queue: dlq ? "orders-dlq" : "orders", deliveries: 3, state: "not_deleted", in_dead_letter_queue: dlq,
  first_seen: new Date(T0).toISOString(), last_seen: new Date(T0 + lastMin * 60_000).toISOString(), consumers: [{ service_name: "billing-fn" }],
});

test("issueOf: each message's lifespan is its own, whatever order the messages were read in (QV-621)", () => {
  const issue = issueOf(group(), [item("A", 1, true), item("B", 9, false)], NOW);
  // messages are newest first, so B comes before A; B must stay open and A in the DLQ
  assert.deepEqual(issue.messages.map((m) => [m.id, m.real?.in_dead_letter_queue]), [["B", false], ["A", true]]);
  assert.deepEqual(issue.lifespans.map((l) => [l.seen, l.state]).sort(), [[T0 + 60_000, "dlq"], [T0 + 9 * 60_000, "open"]]);
  assert.equal(issue.lifespans.find((l) => l.state === "open")!.to, NOW);
  assert.equal(issue.dlq, "orders-dlq");
  assert.equal(issue.stage, "handler");
  assert.equal(severity(issue), "critical");
});

test("issuesOfPage: GET /api/causes's groups, each with its own listed messages, worst first", () => {
  const page = {
    groups: [group({ id: "c2", label: "Not deleted", kind: "not_deleted", counts: { dead_lettered: 0, stuck: 1, filtered: 0 }, count: 1 }), group()],
    messages: [item("A", 1, true), item("B", 9, false), { ...item("C", 5, false), cause: "c2" }],
  } as unknown as CausePage;
  const issues = issuesOfPage(page, NOW);
  assert.deepEqual(issues.map((i) => [i.id, i.messages.map((m) => m.id)]), [["c1", ["B", "A"]], ["c2", ["C"]]]);
  assert.equal(issues[1].evidence, EVIDENCE.not_deleted);
});

test("the recorded open-source answer reads as issues with the cause IDs triage and alerts use", () => {
  const f = JSON.parse(readFileSync(new URL("./fixtures/viewmodel-parity.json", import.meta.url), "utf8"));
  const a = f.answers.python;
  const issues = issuesOfPage({ groups: a.causes, messages: a.cause_messages }, f.nowMs);
  assert.deepEqual(issues.map((i) => i.id).sort(), a.causes.map((g: IssueGroup) => g.id).sort());
  for (const i of issues) assert.equal(i.messages.length, i.count, i.id);
});

test("issuePageKind: a link to a cause whose messages finished is a page, not a 404 (QV-619)", () => {
  assert.equal(issuePageKind({ found: true, triaged: false, sample: false }), "issue");
  assert.equal(issuePageKind({ found: false, triaged: true, sample: false }), "finished");
  assert.equal(issuePageKind({ found: false, triaged: false, sample: false }), "none");
  assert.equal(issuePageKind({ found: false, triaged: false, sample: true }), "not_found");
});

test("a link's age: seconds, or a whole number of minutes, hours or days; none is 5 minutes", () => {
  assert.deepEqual(stuckAge(undefined), { s: 300, bad: false });
  assert.deepEqual(stuckAge(""), { s: 300, bad: false });
  assert.equal(STUCK_AFTER_S, 300);
  for (const [raw, s] of [["90", 90], ["90s", 90], ["15m", 900], ["15min", 900], ["2h", 7200], ["1d", 86400], [" 1H ", 3600], ["0", 0]] as const) {
    assert.deepEqual(stuckAge(raw), { s, bad: false }, raw);
  }
  for (const raw of ["soon", "-1", "1.5h", "5 weeks", "99999999999", "20000d"]) assert.deepEqual(stuckAge(raw), { s: 300, bad: true }, raw);
});

test("an age goes back into a link in its largest whole unit, and reads back the same", () => {
  assert.deepEqual([ageParam(60), ageParam(300), ageParam(3600), ageParam(86400), ageParam(90), ageParam(0)], ["1m", "5m", "1h", "1d", "90s", "0s"]);
  for (const s of [...AGE_CHOICES, 90, 0, 7 * 86400]) assert.equal(stuckAge(ageParam(s)).s, s);
  assert.deepEqual([ageText(300), ageText(60), ageText(3600), ageText(86400 * 2), ageText(90)], ["5 minutes", "1 minute", "1 hour", "2 days", "90 seconds"]);
  assert.deepEqual(AGE_CHOICES.map(ageLabel), ["1 min", "5 min", "15 min", "1 h", "6 h", "1 day"]);
  assert.equal(withAge("/issues?cause=abc&range=4h", 3600), "/issues?cause=abc&range=4h&age=1h");
  assert.equal(withAge("/issues?age=1h&cause=abc", 300), "/issues?cause=abc");
});
