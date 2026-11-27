/*
  A queue's figures as the queue card, the Overview and the queue-level dashboard measures read them: its settings
  (GetQueueAttributes), its depth and age now and over time (CloudWatch), and what the spans say about the messages
  in it. Plain data: a host that reads AWS fills it in, and the verdict comes from it here. Pure.

  These figures are per queue. CloudWatch's SQS metrics have only the QueueName dimension, so a filter on
  anything but the queue (a tenant, a producer) can never narrow them.
*/
import type { Point, Tone } from "../queue/types.ts";

export type { Point, Tone };
/** How an AWS read went: read, no account names the queue, the host reads no AWS, or the read failed. */
export type AwsReadStatus = "ok" | "no_account" | "not_configured" | "failed";

export type LiveWindow = {
  periodS: number; fromMs: number; toMs: number; visible: Point[]; inFlight: Point[]; oldestS: Point[]; sent: Point[]; deleted: Point[]; dlq: Point[];
  /* worked out from the spans (span-history.ts), on the day window only and only when asked for (BuildOptions.spanHistory):
     Stuck in flight, and the share (%) of messages in flight that are on their last allowed receive */
  stuck?: Point[];
  nearDlq?: Point[];
};

export type QueueLive = {
  status: AwsReadStatus;
  detail: string | null;
  readAt: number | null;
  visibilityTimeoutS: number | null;
  retentionS: number | null;
  /* DelaySeconds; absent on figures made before it was carried */
  delayS?: number | null;
  maxReceiveCount: number | null;
  dlq: string | null;
  fifo: boolean | null;
  /* now: SQS's own counts, CloudWatch's latest point for age and the dead-letter queue */
  visible: number | null;
  inFlight: number | null;
  oldestS: number | null;
  dlqDepth: number | null;
  /* when the CloudWatch figures above are from: the end of the period of the point they came from. They
     trail the clock by a minute or two, so the card prints "as of hh:mm" by them */
  oldestAsOf: number | null;
  dlqAsOf: number | null;
  /* CloudWatch was read and returned no point at all for the figure over the window: it has not published
     this queue (or its dead-letter queue) yet. It starts a few minutes after a queue is created or first
     used, and never fills in the minutes before (QV-60), so the card says so rather than "No data" */
  oldestUnpublished: boolean;
  dlqUnpublished: boolean;
  /* windows of CloudWatch history, finest first */
  windows: LiveWindow[];
  /* from the spans: instrumented messages received and not settled for longer than the Stuck page's limit,
     and those in flight on their last allowed receive */
  stuck: number | null;
  nearDlq: { count: number; inFlight: number } | null;
};

/* How much of the queue's recent history the drain rate is averaged over. */
const DRAIN_POINTS = 5;

/** Seconds until the backlog is gone at the recent net rate (deleted minus sent), Infinity when it is not shrinking. */
export function drainS(visible: number | null, sent: Point[], deleted: Point[], periodS: number): number | null {
  if (visible === null) return null;
  if (visible <= 0) return 0;
  const tail = (p: Point[]) => p.slice(-DRAIN_POINTS).reduce((a, [, v]) => a + v, 0);
  const n = Math.max(1, Math.min(DRAIN_POINTS, Math.max(sent.length, deleted.length)));
  const netPerS = (tail(deleted) - tail(sent)) / (n * periodS);
  return netPerS > 0 ? visible / netPerS : Infinity;
}

export const retentionPct = (oldestS: number | null, retentionS: number | null) => (oldestS === null || !retentionS ? null : (100 * oldestS) / retentionS);
export const visibilityPct = (p95Ms: number | null, vtS: number | null) => (p95Ms === null || !vtS ? null : (100 * p95Ms) / (vtS * 1000));

/** How much the dead-letter queue's depth changed since sinceMs: its latest point minus the last point at or before then. */
export function dlqChange(w: LiveWindow, sinceMs: number): number | null {
  if (w.dlq.length < 2) return null;
  const start = [...w.dlq].reverse().find(([t]) => t <= sinceMs) ?? w.dlq[0];
  return w.dlq[w.dlq.length - 1][1] - start[1];
}

/** The finest window that reaches back to fromMs, or the one that reaches furthest. */
export function windowFor(q: QueueLive, fromMs: number): LiveWindow | null {
  if (!q.windows.length) return null;
  return q.windows.find((w) => w.fromMs <= fromMs) ?? [...q.windows].sort((a, b) => a.fromMs - b.fromMs)[0];
}

/**
  Points in n equal buckets over [from, to). A gauge takes the highest value in a bucket, a count the sum.
  CloudWatch writes no point for a minute with nothing to say (and none at all for a queue inactive for six
  hours), so an empty bucket keeps the gauge's last value, and a count is 0.
*/
export function bucketize(points: Point[], from: number, to: number, n: number, kind: "gauge" | "count"): number[] {
  const span = (to - from) / n;
  const out: (number | null)[] = Array.from({ length: n }, () => null);
  let before = 0;
  for (const [t, v] of points) {
    if (t < from) { before = v; continue; }
    if (t >= to) continue;
    const i = Math.min(n - 1, Math.floor((t - from) / span));
    out[i] = kind === "count" ? (out[i] ?? 0) + v : Math.max(out[i] ?? -Infinity, v);
  }
  if (kind === "count") return out.map((v) => v ?? 0);
  let prev = before;
  return out.map((v) => (v === null ? prev : (prev = v)));
}

/**
  The queue's health from its own figures. Null when AWS was not read: no figure, no verdict.
    critical  the oldest message has used 90% of the retention period (it is about to be lost), or the
              dead-letter queue grew in the past hour
    warning   the oldest message is over 5 minutes old, processing takes 80% of the visibility timeout,
              something is in the dead-letter queue, a message is stuck or on its last allowed receive, or
              the backlog is not shrinking
*/
export function healthOf(q: QueueLive | undefined, p95Ms: number | null = null): Tone | null {
  return health(q, p95Ms)?.tone ?? null;
}

/** The verdict with its reason, in a word or two, for the queue card's pill. */
export function health(q: QueueLive | undefined, p95Ms: number | null = null): { tone: Tone; why: string } | null {
  if (!q || q.status !== "ok") return null;
  const w = q.windows[0];
  const ret = retentionPct(q.oldestS, q.retentionS);
  if (ret !== null && ret >= 90) return { tone: "critical", why: "Near retention" };
  if (w && (dlqChange(w, (q.readAt ?? w.toMs) - 3_600_000) ?? 0) > 0) return { tone: "critical", why: "DLQ growing" };
  const vis = visibilityPct(p95Ms, q.visibilityTimeoutS);
  const drain = w ? drainS(q.visible, w.sent, w.deleted, w.periodS) : null;
  if (drain === Infinity && (q.visible ?? 0) > 0) return { tone: "warning", why: "Backlog growing" };
  if ((q.oldestS ?? 0) > 300) return { tone: "warning", why: "Old messages" };
  if (vis !== null && vis >= 80) return { tone: "warning", why: "Near timeout" };
  if ((q.dlqDepth ?? 0) > 0) return { tone: "warning", why: "In the DLQ" };
  if ((q.nearDlq?.count ?? 0) > 0) return { tone: "warning", why: "Near the DLQ" };
  if ((q.stuck ?? 0) > 0) return { tone: "warning", why: "Stuck messages" };
  return { tone: "good", why: "Healthy" };
}
