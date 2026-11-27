/*
  What the shared view-model functions make of one backend's answers in fixtures/viewmodel-parity.json (QV-578):
  the Event explorer's events of every timeline, the Overview of the flows, the cause rows of the groups, and the
  Issues pages' issues of the groups and their messages (QV-583).
  test/viewmodel.test.ts, test/make-viewmodel-expected.ts and the hosted app's test/viewmodel-parity.test.ts use it.
*/
import { eventsOfTimeline, issuesOf, overviewData, toCause, type CauseRowGroup, type IssueGroup, type IssueItem } from "../src/model/index.ts";
import type { Flow } from "../src/queue/selfhosted.ts";
import { TREND_MS } from "../src/causes/text.ts";

export type Answers = { timelines: Record<string, unknown>[]; flows: Flow[]; causes: (CauseRowGroup & IssueGroup)[]; cause_messages: IssueItem[] };
export type ViewModels = ReturnType<typeof viewModels>;

/* JSON, as a page receives it: no undefined fields */
const plain = <T,>(x: T): T => JSON.parse(JSON.stringify(x));

export function viewModels(a: Answers, nowMs: number, fromMs: number) {
  const places = Object.fromEntries(a.flows.map((f) => [f.name, { account: f.account, region: f.region }]));
  const events = a.timelines.flatMap((t) => eventsOfTimeline(t, { places })).sort((x, y) => y.ms - x.ms || x.key.localeCompare(y.key));
  const minutes = Math.round((nowMs - fromMs) / 60_000);
  const overview = overviewData("w", "2h", minutes, a.flows, {}, nowMs, {}, { events });
  const causes = a.causes.map((g) => toCause(g, g.queue, nowMs - TREND_MS, nowMs));
  const issues = issuesOf({ groups: a.causes, items: a.cause_messages }, nowMs);
  return plain({ events, overview, causes, issues });
}
