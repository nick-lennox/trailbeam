/*
  Issues (One UI 5, QV-583): messages that failed or did not finish, grouped by cause. One issue is one cause group
  (GET /api/causes on open source, the hosted app's lib/app/causes.ts): the same groups, with the same IDs, as the
  Overview's cause rows, the message page's cause line, triage and cause alerts, so they all agree. This file draws a
  group the way the Issues pages show it (issueOf); it does not classify anything itself. Moved here from the hosted
  app's lib/app/issue-causes.ts, so both products' Issues pages are made by the same functions:
  fixtures/viewmodel-parity.json holds both backends' groups and messages for the same spans (test/viewmodel.test.ts).

  Pure: no React, no server.
*/
import type { CauseGroup, CauseKind } from "../causes/types.ts";

export type { CauseKind };

export type Step = { label: string; tone: "plain" | "warn" | "bad"; inferred: boolean };

/* Where in a message's life an issue stops it. Every issue sits at one of these four points,
   in the order a message meets them. */
export type Stage = "queue" | "receive" | "handler" | "delete";
export const STAGES: { id: Stage; label: string; means: string }[] = [
  { id: "queue", label: "In queue", means: "Sent, never received" },
  { id: "receive", label: "Receive", means: "Dropped, or received by something that does not report" },
  { id: "handler", label: "Handler", means: "The consumer failed while holding the message" },
  { id: "delete", label: "Delete", means: "Not deleted before the visibility timeout" },
];
export const STAGE_OF: Record<CauseKind, Stage> = {
  never_received: "queue", fifo_blocked: "queue", filter: "receive",
  fn_timeout: "handler", fn_oom: "handler", exc: "handler", invalid_batch_response: "handler", reported_failure: "handler", no_outcome: "handler", in_flight: "handler",
  delete_failed: "delete", not_deleted: "delete",
};

/* One thing an issue's messages can share, and how they split over its values. `base` is the
   share of all messages on the queue with that value, 0 to 1, when it is known: a value far
   above its base is a lead. */
export type Breakdown = { label: string; mono?: boolean; rows: { value: string; n: number; base?: number }[] };
/* One message's time in trouble: from when it was first seen to when it was last seen, and how it stands.
   `seen` is when it was last seen. `to` is now for a message that has not finished, so the two differ. */
export type Lifespan = { from: number; to: number; seen: number; state: "dlq" | "open" | "done" };
/* How long into a receive the failure comes, against the queue's visibility timeout when known. */
export type Timing = { label: string; atS: number; visibilityTimeoutS: number | null };

/* One message in an issue. `real` and `outcome` are what the page needs to draw its state:
   Trailbeam's own state for a workspace's message, the sample outcome for a sample one. */
export type IssueMessage = {
  id: string;
  queue: string;
  receives: number;
  firstSeen: number;
  lastSeen: number;
  consumers: string[];
  tenant?: string;
  real?: IssueState;
  outcome?: "deleted" | "in_flight" | "retrying" | "dead_lettered" | "stuck" | "filtered";
};

/* A message's own state, as the state pill draws it (design StateOf). */
export type IssueState = { state: string; in_dead_letter_queue: boolean; deliveries: number; purge?: { status: "purged" | "possibly_purged"; start: string } | null };

export type Issue = {
  id: string;
  kind: CauseKind;
  title: string;
  /* what the cause rests on, in a sentence or two */
  evidence: string;
  inferred: boolean;
  stage: Stage;
  queue: string;
  producers: string[];
  consumers: string[];
  /* the dead-letter queue its messages were moved to, when SQS reported a move */
  dlq: string | null;
  /* what each service runs on, by name, when known: picks its icon */
  platforms?: Record<string, string>;
  tenants?: string;
  /* the highest receive count among its messages, and the queue's settings when known */
  maxReceives: number;
  maxReceiveCount: number | null;
  visibilityTimeoutS: number | null;
  /* the queue's MessageRetentionPeriod, when known. SQS deletes a message that many days after it
     was first sent, wherever it is by then, a dead-letter queue included. */
  retentionDays: number | null;
  lifespans: Lifespan[];
  breakdowns: Breakdown[];
  timing: Timing | null;
  /* messages with this cause, and how they stand now */
  count: number;
  inDlq: number;
  notFinished: number;
  handledTwice: number;
  firstSeen: number;
  lastSeen: number;
  /* messages per interval, by when each was last seen, oldest first */
  trend: number[];
  trendLabel: string;
  /* the time the whole trend covers, ending now */
  trendSpanMs: number;
  messages: IssueMessage[];
  /* the steps most of its messages took, and how many took exactly those */
  shape: { steps: Step[]; count: number; example?: string } | null;
};

export type Severity = "critical" | "serious" | "warning";
export const severity = (i: Pick<Issue, "inDlq" | "notFinished">): Severity => (i.inDlq > 0 ? "critical" : i.notFinished > 0 ? "serious" : "warning");
const RANK: Record<Severity, number> = { critical: 0, serious: 1, warning: 2 };
/* Worst first, then the one seen most recently. */
type Ranked = Pick<Issue, "inDlq" | "notFinished" | "lastSeen" | "count">;
export const byUrgency = (a: Ranked, b: Ranked) => RANK[severity(a)] - RANK[severity(b)] || b.lastSeen - a.lastSeen || b.count - a.count;

export const EVIDENCE: Record<CauseKind, string> = {
  filter: "A Lambda event filter removed the message from the queue without invoking the function. Nothing in your code saw it. CloudTrail recorded the delete.",
  fn_timeout: "Lambda's log says the invocation reached the function timeout. Lambda does not delete a record from a failed invocation, so SQS delivers it again when its visibility timeout expires.",
  fn_oom: "Lambda's log says the invocation ran out of memory. Lambda does not delete a record from a failed invocation, so SQS delivers it again when its visibility timeout expires.",
  exc: "The handler raised an exception while it held the message, and no delete followed. SQS delivers it again when its visibility timeout expires.",
  invalid_batch_response: "The function returned a batchItemFailures ID that is not in the batch, so Lambda treated the whole batch as failed.",
  reported_failure: "The function listed the message in batchItemFailures, so Lambda did not delete it.",
  no_outcome: "A Lambda receive with no outcome reported inside the function timeout. The invocation most likely timed out or ran out of memory. Nothing recorded how it ended, so this is inferred.",
  delete_failed: "SQS rejected the DeleteMessage call, so the message stayed in the queue. The usual reason is a receipt handle that is no longer valid because the visibility timeout expired first.",
  not_deleted: "A receive was recorded and no accepted delete followed before the visibility timeout ran out, so SQS makes the message visible again. Inferred from the receive and the timeout.",
  in_flight: "A receive was recorded over 5 minutes ago and no delete has followed yet. The consumer may still be working, may have stopped, or may have returned without deleting the message.",
  fifo_blocked: "SQS delivers a FIFO message group's messages in order, and an earlier message of the same group has not finished, so this one waits behind it. Inferred.",
  never_received: "A send was recorded and no instrumented consumer has reported a receive since. Either nothing is consuming the queue, or its consumer does not have the Trailbeam package.",
};

/* What to look at first for each kind of cause: how SQS and Lambda behave, not advice about your code. */
export const CHECKS: Record<CauseKind, string[]> = {
  filter: ["The event source mapping's filter criteria. A message that matches no filter is deleted from the queue without the function running."],
  fn_timeout: ["The function timeout against the slowest call the handler makes. An invocation that reaches the timeout reports nothing.", "The visibility timeout against the function timeout. AWS recommends a visibility timeout of at least six times the function timeout for a Lambda trigger."],
  fn_oom: ["The function's memory setting against what one batch needs.", "Whether the batch size makes one invocation hold more than it can."],
  exc: ["Whether the error depends on the message (a missing field, one tenant) or on the moment (a dependency that was down).", "Whether the function reports batch item failures. Without ReportBatchItemFailures, one failed record sends its whole batch back to the queue."],
  invalid_batch_response: ["The itemIdentifier values the function returns. Each must be the messageId of a record in the batch it was given."],
  reported_failure: ["Why the handler listed the message as failed. The message is delivered again until it succeeds or reaches maxReceiveCount."],
  no_outcome: ["The function timeout against the slowest call the handler makes. An invocation that reaches the timeout reports nothing.", "The visibility timeout against the function timeout. AWS recommends a visibility timeout of at least six times the function timeout for a Lambda trigger.", "Memory. An invocation that runs out of memory also ends without an outcome."],
  delete_failed: ["How long the consumer holds a message against the visibility timeout. A receipt handle stops working once the message has been delivered again.", "Whether two workers share receipt handles, so that one deletes with a handle the other received."],
  not_deleted: ["Processing time against the visibility timeout. If work can run longer, extend the timeout with ChangeMessageVisibility while it runs.", "Whether the consumer deletes after handling. A handler that returns without calling DeleteMessage looks the same from here."],
  in_flight: ["Whether the consumer is still running. A worker stopped mid-message leaves it in flight until the visibility timeout expires.", "Whether the consumer deletes after handling."],
  fifo_blocked: ["The earlier message in the same message group. Until it is deleted or moved to a dead-letter queue, SQS holds back the rest of its group."],
  never_received: ["Whether anything consumes this queue, and whether that consumer has the Trailbeam package.", "For a Lambda trigger, whether the event source mapping is enabled."],
};

/* What issueOf reads of a cause group: the fields both products' groups have. */
export type IssueGroup = Pick<CauseGroup, "id" | "kind" | "label" | "why" | "inferred" | "queue" | "consumer" | "tenants" | "versions" | "count" | "counts"
  | "firstMs" | "lastMs" | "times" | "sampleMid">;
/* What issueOf reads of one of a group's messages: a message summary with its group's ID (open source's CauseMessage,
   the hosted app's UnfinishedItem). */
export type IssueItem = {
  message_id: string;
  cause: string;
  state: string;
  current_queue?: string | null;
  in_dead_letter_queue?: boolean;
  deliveries?: number | null;
  first_seen?: string | null;
  last_seen?: string | null;
  consumers?: { service_name?: string | null }[];
  purge?: IssueState["purge"];
};
type Item = IssueItem;
type ItemConsumer = { service_name?: string | null };

export const TREND_BUCKETS = 12;

/* how many of `times` fall in each of n equal parts of [fromMs, toMs] */
function trend(times: number[], fromMs: number, toMs: number, n: number): number[] {
  const out = Array.from({ length: n }, () => 0);
  const span = Math.max(1, toMs - fromMs);
  for (const t of times) if (t >= fromMs && t <= toMs) out[Math.min(n - 1, Math.floor(((t - fromMs) / span) * n))]++;
  return out;
}
export const TREND_HOURS = 24;

function tally(label: string, values: string[], mono = false): Breakdown {
  const n = new Map<string, number>();
  for (const v of values) n.set(v, (n.get(v) ?? 0) + 1);
  return { label, mono, rows: [...n.entries()].map(([value, k]) => ({ value, n: k })).sort((a, b) => b.n - a.n || a.value.localeCompare(b.value)).slice(0, 6) };
}
const stateWord = (m: Item) =>
  m.in_dead_letter_queue ? "In the DLQ" : m.state === "deleted" ? "Deleted" : m.state === "deleted_inferred" ? "Deleted by Lambda" : Number(m.deliveries) === 0 ? "Not received" : "Not deleted";
const consumersOf = (m: Item) => [...new Set(((m.consumers as ItemConsumer[] | undefined) ?? []).flatMap((c) => c.service_name ?? []))];

/** A cause group as an issue, with the messages of it that were read (u.items whose cause is this group). */
export function issueOf(g: IssueGroup, items: Item[], nowMs: number): Issue {
  const messages: IssueMessage[] = items.map((m) => ({
    id: m.message_id, queue: String(m.current_queue ?? g.queue), receives: Number(m.deliveries ?? 0),
    firstSeen: Date.parse(String(m.first_seen)), lastSeen: Date.parse(String(m.last_seen)), consumers: consumersOf(m),
    real: { state: m.state, in_dead_letter_queue: !!m.in_dead_letter_queue, deliveries: Number(m.deliveries ?? 0), purge: m.purge },
  })).sort((a, b) => b.lastSeen - a.lastSeen);
  const done = g.kind === "filter";
  const lifespans = messages.map((m): Lifespan => {
    const dlq = m.real?.in_dead_letter_queue === true;
    const open = !done && !dlq;
    return { from: m.firstSeen, to: open ? nowMs : m.lastSeen, seen: m.lastSeen, state: dlq ? "dlq" : open ? "open" : "done" };
  }).filter((l) => Number.isFinite(l.from) && Number.isFinite(l.seen)).sort((a, b) => a.from - b.from);
  const span = TREND_HOURS * 3600_000;
  return {
    id: g.id, kind: g.kind, title: g.label, evidence: g.why ?? EVIDENCE[g.kind], inferred: g.inferred, stage: STAGE_OF[g.kind],
    queue: g.queue, producers: [], consumers: g.consumer ? [g.consumer] : [...new Set(messages.flatMap((m) => m.consumers))].sort(),
    dlq: items.find((m) => m.in_dead_letter_queue)?.current_queue ?? null,
    tenants: g.tenants.length ? (g.tenants.length === 1 ? g.tenants[0] : `${g.tenants[0]} and ${g.tenants.length - 1} more`) : undefined,
    maxReceives: Math.max(0, ...messages.map((m) => m.receives)), maxReceiveCount: null, visibilityTimeoutS: null, retentionDays: null,
    lifespans,
    breakdowns: [
      tally("Receive count", messages.map((m) => String(m.receives)), true),
      tally("Consumer version", g.versions.flatMap((v) => Array.from({ length: v.count }, () => v.version)), true),
      tally("State now", items.map(stateWord)),
    ].filter((b) => b.rows.length > 0),
    timing: null,
    count: g.count, inDlq: g.counts.dead_lettered, notFinished: g.counts.stuck, handledTwice: 0,
    firstSeen: g.firstMs, lastSeen: g.lastMs,
    trend: trend(g.times, nowMs - span, nowMs, TREND_BUCKETS), trendLabel: `last ${TREND_HOURS} hours`, trendSpanMs: span,
    messages,
    shape: g.sampleMid ? { steps: [], count: 0, example: g.sampleMid } : null,
  };
}

/** Every cause group read as an issue, worst first. */
export function issuesOf(u: { groups: IssueGroup[]; items: Item[] }, nowMs: number): Issue[] {
  const byCause = new Map<string, Item[]>();
  for (const it of u.items) (byCause.get(it.cause) ?? byCause.set(it.cause, []).get(it.cause)!).push(it);
  return u.groups.map((g) => issueOf(g, byCause.get(g.id) ?? [], nowMs)).sort(byUrgency);
}

export function ago(ms: number, now: number): string {
  const s = Math.max(0, Math.round((now - ms) / 1000));
  if (s < 60) return "just now";
  if (s < 5400) return `${Math.round(s / 60)} min ago`;
  if (s < 172800) return `${Math.round(s / 3600)} h ago`;
  return `${Math.round(s / 86400)} days ago`;
}
export const utc = (ms: number) => new Date(ms).toISOString().replace("T", " ").slice(0, 19);

/* What an issue page shows for an ID (QV-619). Alert, assignment and old Needs attention links all go to
   /issues/<id>, so a cause whose messages have all finished still gets a page: its triage and notes when it
   has them, or a plain "nothing unfinished has this cause now". Only a sample ID that isn't a sample issue is
   not found: sample issues are fixed, so there is no old link to keep. */
export function issuePageKind(o: { found: boolean; triaged: boolean; sample: boolean }): "issue" | "finished" | "none" | "not_found" {
  if (o.found) return "issue";
  if (o.sample) return "not_found";
  return o.triaged ? "finished" : "none";
}

/** GET /api/causes's answer as issues: its groups, each with the messages of it the page listed (`messages`, up to
    the page's limit). Open source's Issues pages read this; the hosted app's read issuesOf over its own groups. */
export const issuesOfPage = (page: { groups: IssueGroup[]; messages: Item[] }, nowMs: number): Issue[] => issuesOf({ groups: page.groups, items: page.messages }, nowMs);
