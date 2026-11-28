/*
  The AWS figures (src/model/aws.ts; QV-589) on the cases in ../contract/fixtures/aws-figures-parity.json, which the
  server's trailbeam.server.aws_figures writes and runs (python/tests/server/test_aws_figures.py): the settings from
  GetQueueAttributes, the CloudWatch period, the queue names a pattern names, the figures now, a queue's figures and
  the queue page's AWS part are the same in both products. The hosted app reads the same file as
  @trailbeam/ui/fixtures/aws-figures-parity.json.
  Run: npm test
*/
import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { liveOf, nameMatches, nowFigures, parseQueueArn, periodFor, queueAwsView, settingsFrom, type InQueueView } from "../src/model/index.ts";

const F = JSON.parse(readFileSync(new URL("../../contract/fixtures/aws-figures-parity.json", import.meta.url), "utf8"));
// the result as JSON.stringify writes it, as the server's answers are
const json = (v: unknown) => JSON.parse(JSON.stringify(v));

test("settingsFrom: the attributes Trailbeam uses, as the server reads them", () => {
  for (const c of F.settingsFrom) assert.deepEqual(json(settingsFrom(c.attributes)), c.expected, c.name);
});

test("parseQueueArn", () => {
  for (const c of F.parseQueueArn) assert.deepEqual(parseQueueArn(c.arn), c.expected, String(c.arn));
});

test("periodFor: the CloudWatch period for a window", () => {
  for (const c of F.periodFor) assert.equal(periodFor(c.fromMs, c.toMs, c.nowMs, c.maxPoints ?? undefined), c.expected, c.name);
});

test("nameMatches: the queues a pattern names", () => {
  for (const c of F.nameMatches) assert.equal(nameMatches(c.pattern, c.name), c.expected, `${c.pattern} ${c.name}`);
});

test("nowFigures: the figures now", () => {
  for (const c of F.nowFigures) assert.deepEqual(json(nowFigures(c.aws ? F.aws[c.aws] : null, c.windowId)), c.expected, `${c.aws} ${c.windowId}`);
});

test("liveOf: a queue's figures", () => {
  for (const c of F.liveOf) {
    const view = c.view ? (F.views[c.view] as InQueueView) : null;
    assert.deepEqual(json(liveOf(F.aws[c.aws], view, c.windows, c.now, c.spans ? F.spans[c.spans] : null)), c.expected, c.name);
  }
});

test("queueAwsView: the queue page's AWS part", () => {
  for (const c of F.queueAwsView) assert.deepEqual(json(queueAwsView(F.aws[c.aws], c.windowId)), c.expected, c.aws);
});

test("the cases cover every status and a FIFO queue", () => {
  const statuses = new Set(Object.values(F.aws as Record<string, { status: string }>).map((a) => a.status));
  assert.deepEqual([...statuses].sort(), ["failed", "no_account", "not_configured", "ok"]);
  assert.ok(F.settingsFrom.some((c: { expected: { fifo: boolean } }) => c.expected.fifo));
});
