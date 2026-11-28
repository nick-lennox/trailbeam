/*
  What the shared view-model functions make of one backend's answers in fixtures/viewmodel-parity.json (QV-578):
  the Event explorer's events of every timeline, the Overview of the flows, the cause rows of the groups, and the
  Issues pages' issues of the groups and their messages (QV-583), and "In ‹queue› now" of each queue's view (QV-579).
  test/viewmodel.test.ts, test/make-viewmodel-expected.ts and the hosted app's test/viewmodel-parity.test.ts use it.
*/
import { eventsOfTimeline, heldAt, issuesOf, overviewData, toCause, type CauseRowGroup, type InQueueView, type IssueGroup, type IssueItem } from "../src/model/index.ts";
import type { Flow } from "../src/queue/selfhosted.ts";
import { TREND_MS } from "../src/causes/text.ts";

/* one queue's "In ‹queue› now" at nowMs: GET /api/queues/{queue}/now, or the hosted loadInQueue with no AWS reads */
export type NowAnswer = { queue: string; nowMs: number; maxReceiveCount: number | null; view: InQueueView };
export type Answers = { timelines: Record<string, unknown>[]; flows: Flow[]; causes: (CauseRowGroup & IssueGroup)[]; cause_messages: IssueItem[];
  queue_now: NowAnswer[] };
/* the page counts held messages down between loads; the answers are checked 10 s into that (a timeout counted down)
   and 30 s into it (SQS's default timeout run out) too */
export const NOW_TICKS_MS = [10_000, 30_000];

/* "In ‹queue› now" as QueueNowLive draws it: the view (the Python server also names the queue's place, which the view
   doesn't read), and each held message 10 s and 30 s later on the page's clock (heldAt) */
export function nowViews(a: NowAnswer[]) {
  return plain(a.map(({ queue, nowMs, view }) => {
    const { queue: _place, ...v } = view as InQueueView & { queue?: unknown };
    const max = v.config.maxReceiveCount ?? null;
    return { queue, nowMs, view: v, ticked: NOW_TICKS_MS.map((t) => v.held.map((m) => heldAt(m, nowMs + t, max, !v.config.visibilityKnown))) };
  }));
}
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
  return plain({ events, overview, causes, issues, now: nowViews(a.queue_now) });
}
