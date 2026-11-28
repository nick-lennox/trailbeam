/*
  What AWS says about a queue, and the figures worked out from it (QV-589). Both backends read the same three calls,
  each allowed by the read-only policy and nothing else:

    sqs:GetQueueAttributes           a queue's settings, its redrive policy and SQS's own counts
    cloudwatch:GetMetricData         depth, in flight, age of the oldest message, sends, deletes and the
                                     dead-letter queue's depth, with history
    lambda:ListEventSourceMappings   the Lambda triggers on a queue and their batch settings

  ReceiveMessage is never called: it would change the visibility of the messages it returned.

  The reads themselves are each backend's own: the hosted app's lib/app/aws-reads.ts and queue-aws.ts with the AWS
  SDK, and the Python server's trailbeam.server.aws_reads with boto3. What they return is the plain data here, and
  everything worked out from it is here once: the settings from the attributes map (settingsFrom), the CloudWatch
  period for a window (periodFor), the figures now (nowFigures), a queue's figures for the Overview, the dashboards and
  the alert check (liveOf), and the queue page's AWS part (queueAwsView). The Python server's
  trailbeam.server.aws_figures is a port of these, and ../../contract/fixtures/aws-figures-parity.json holds the cases
  both must give the same answer on (test/aws.test.ts here, python/tests/server/test_aws_figures.py there). Pure.
*/
import type { InQueueView } from "./in-queue.ts";
import type { LiveWindow, QueueLive } from "./queue-live.ts";
import type { AwsReadStatus } from "./queue-live.ts";
import type { Point, QueueAwsView, Trigger as PageTrigger } from "../queue/types.ts";

/* ── GetQueueAttributes ─────────────────────────────────────────────── */

export type AwsQueueSettings = {
  arn: string | null;
  fifo: boolean;
  visibilityTimeoutS: number | null;
  retentionS: number | null;
  delayS: number | null;
  /* from RedrivePolicy; all null when the queue has none */
  maxReceiveCount: number | null;
  dlqArn: string | null;
  dlqName: string | null;
  /* SQS's own approximate counts at the time of the call: ApproximateNumberOfMessages(NotVisible/Delayed) */
  visible: number | null;
  inFlight: number | null;
  delayed: number | null;
};

const int = (v: unknown) => {
  if (v === undefined || v === null || v === "") return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
};

/** A queue's name, account and region from its ARN, arn:aws:sqs:<region>:<account>:<name>. */
export function parseQueueArn(arn: string | null | undefined): { name: string; account: string | null; region: string | null } | null {
  const m = /^arn:[^:]+:sqs:([^:]*):([^:]*):(.+)$/.exec(arn ?? "");
  return m ? { name: m[3], account: m[2] || null, region: m[1] || null } : null;
}

/** The parts of GetQueueAttributes' Attributes map that Trailbeam uses. */
export function settingsFrom(a: Record<string, string | undefined>): AwsQueueSettings {
  let maxReceiveCount: number | null = null, dlqArn: string | null = null;
  if (a.RedrivePolicy) {
    try {
      const p = JSON.parse(a.RedrivePolicy) as { maxReceiveCount?: unknown; deadLetterTargetArn?: unknown };
      maxReceiveCount = int(p?.maxReceiveCount); // a number or a string, depending on how it was set
      dlqArn = typeof p?.deadLetterTargetArn === "string" ? p.deadLetterTargetArn : null;
    } catch { /* a policy that is not JSON: treated as none */ }
  }
  return {
    arn: a.QueueArn ?? null,
    fifo: a.FifoQueue === "true",
    visibilityTimeoutS: int(a.VisibilityTimeout),
    retentionS: int(a.MessageRetentionPeriod),
    delayS: int(a.DelaySeconds),
    maxReceiveCount, dlqArn, dlqName: dlqArn ? parseQueueArn(dlqArn)?.name ?? null : null,
    visible: int(a.ApproximateNumberOfMessages),
    inFlight: int(a.ApproximateNumberOfMessagesNotVisible),
    delayed: int(a.ApproximateNumberOfMessagesDelayed),
  };
}

/* ── CloudWatch GetMetricData ───────────────────────────────────────── */

export type MetricName = "visible" | "inFlight" | "oldestS" | "sent" | "deleted";
export type AwsQueueSeries = Record<MetricName, Point[]>;

/* The SQS metrics read, and the statistic for each. Depth and age are gauges, so the highest value in a
   period; sends and deletes are counts, so their sum.
   https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-available-cloudwatch-metrics.html */
export const METRICS: { key: MetricName; name: string; stat: "Maximum" | "Sum" }[] = [
  { key: "visible", name: "ApproximateNumberOfMessagesVisible", stat: "Maximum" },
  { key: "inFlight", name: "ApproximateNumberOfMessagesNotVisible", stat: "Maximum" },
  { key: "oldestS", name: "ApproximateAgeOfOldestMessage", stat: "Maximum" },
  { key: "sent", name: "NumberOfMessagesSent", stat: "Sum" },
  { key: "deleted", name: "NumberOfMessagesDeleted", stat: "Sum" },
];

export const emptySeries = (): AwsQueueSeries => ({ visible: [], inFlight: [], oldestS: [], sent: [], deleted: [] });

/* CloudWatch keeps 1-minute points for 15 days, 5-minute points for 63 days and 1-hour points for 455
   days, and a period must be a multiple of 60 s. The shortest period that both exists that far back and
   keeps a request under maxPoints per metric.
   https://docs.aws.amazon.com/AmazonCloudWatch/latest/APIReference/API_GetMetricData.html */
export function periodFor(fromMs: number, toMs: number, nowMs: number, maxPoints = 1440): number {
  const age = nowMs - fromMs;
  const floor = age > 63 * 86_400_000 ? 3600 : age > 15 * 86_400_000 ? 300 : 60;
  const need = Math.ceil((toMs - fromMs) / 1000 / maxPoints / 60) * 60;
  return Math.max(floor, need);
}

/* ── Lambda ListEventSourceMappings ─────────────────────────────────── */

export type AwsTrigger = PageTrigger & {
  uuid: string;
  functionArn: string | null;
  /* the event filter patterns themselves, as the mapping's FilterCriteria has them */
  filterPatterns: string[];
};

/* ── what a backend's reads return for one queue ─────────────────────── */

/* a window of CloudWatch history asked for: an ID the figures name it by, its edges and its period */
export type AwsWindow = { id: string; fromMs: number; toMs: number; periodS: number };
export type WindowSeries = { periodS: number; fromMs: number; toMs: number; queue: AwsQueueSeries; dlq: Point[] };

export type QueueAws = {
  status: AwsReadStatus;
  /* why there is nothing, or what went wrong, in a sentence */
  detail: string | null;
  accountId: string | null;
  region: string | null;
  readAt: number | null;
  settings: AwsQueueSettings | null;
  series: Record<string, WindowSeries>;
  triggers: AwsTrigger[] | null;
  /* reads that failed while others worked, e.g. lambda:ListEventSourceMappings denied */
  errors: string[];
  /* the dead-letter queue's depth from its own GetQueueAttributes, and when it was read */
  dlqNow?: { visible: number; at: number } | null;
};

export const emptyAws = (status: AwsReadStatus, detail: string | null, a?: { accountId: string; region: string } | null): QueueAws =>
  ({ status, detail, accountId: a?.accountId ?? null, region: a?.region ?? null, readAt: null, settings: null, series: {}, triggers: null, errors: [] });

/** A queue name a list of names names: the exact name, or a pattern with * in it. */
export function nameMatches(pattern: string, name: string): boolean {
  const re = new RegExp(`^${pattern.split("*").map((s) => s.replace(/[.+?^${}()|[\]\\]/g, "\\$&")).join(".*")}$`);
  return re.test(name);
}

/* ── figures from a queue's reads ────────────────────────────────────── */

const lastOf = (p: Point[] | undefined) => (p && p.length ? p[p.length - 1] : null);

/**
  The queue's figures now. Visible and in flight are SQS's own counts from GetQueueAttributes, which are
  current; CloudWatch's latest minute is used when they are missing. The age of the oldest message and
  the dead-letter queue's depth exist only in CloudWatch, so they are as of its latest point: oldestAt and
  dlqAt are when that point's period starts. CloudWatch's newest point is for the minute that ended about
  30 s before the read, so these trail the true figures by a minute or two (QV-60 measured an oldest age
  94 to 145 s below the true one), and the card says when they are from.
*/
export function nowFigures(a: QueueAws | undefined | null, windowId: string) {
  const s = a?.series[windowId];
  const v = lastOf(s?.queue.visible), f = lastOf(s?.queue.inFlight), o = lastOf(s?.queue.oldestS), d = lastOf(s?.dlq);
  return {
    visible: a?.settings?.visible ?? v?.[1] ?? null,
    inFlight: a?.settings?.inFlight ?? f?.[1] ?? null,
    delayed: a?.settings?.delayed ?? null,
    oldestS: o ? o[1] : null,
    oldestAt: o ? o[0] : null,
    dlqDepth: a?.settings?.dlqName ? (a.dlqNow ? a.dlqNow.visible : d ? d[1] : null) : null,
    /* when the depth is from: the DLQ's own read, or the start of CloudWatch's latest period */
    dlqAt: a?.settings?.dlqName ? (a.dlqNow ? a.dlqNow.at : d ? d[0] : null) : null,
    dlqFromSqs: !!(a?.settings?.dlqName && a.dlqNow),
    /* how the dead-letter queue's depth changed over the window: last point minus first */
    dlqChange: a?.settings?.dlqName && s && s.dlq.length > 1 ? s.dlq[s.dlq.length - 1][1] - s.dlq[0][1] : null,
  };
}

/* the Stuck page's limit: a message received and not settled for longer is stuck */
export const STUCK_AFTER_MS = 300_000;
/* the window the span-derived history goes on: a day at one minute */
export const SPAN_WINDOW = "day";

/* worked out from the spans over the day window: Stuck in flight, and the share on its last allowed receive */
export type SpanHistory = { stuck: Point[]; nearDlq: Point[] | null };

/** A queue's figures (QueueLive) from its AWS reads, "In ‹queue› now" and the span history. */
export function liveOf(aws: QueueAws, view: InQueueView | null, windows: AwsWindow[], now: number, spans: SpanHistory | null = null): QueueLive {
  const s = aws.settings;
  const first = windows[0]?.id;
  const f = nowFigures(aws, first ?? "");
  const series = aws.status === "ok" ? aws.series[first ?? ""] : undefined;
  const periodMs = (series?.periodS ?? 60) * 1000;
  const near = view && s?.maxReceiveCount ? view.held.filter((m) => m.state === "in_flight" && m.receiveCount >= s.maxReceiveCount!).length : null;
  return {
    status: aws.status, detail: aws.detail ?? (aws.errors.length ? aws.errors.join(" ") : null), readAt: aws.readAt,
    visibilityTimeoutS: s?.visibilityTimeoutS ?? null, retentionS: s?.retentionS ?? null, delayS: s?.delayS ?? null, maxReceiveCount: s?.maxReceiveCount ?? null,
    dlq: s?.dlqName ?? null, fifo: s ? s.fifo : null,
    visible: f.visible, inFlight: f.inFlight, oldestS: f.oldestS, dlqDepth: f.dlqDepth,
    oldestAsOf: f.oldestAt === null ? null : f.oldestAt + periodMs, dlqAsOf: f.dlqAt === null ? null : f.dlqFromSqs ? f.dlqAt : f.dlqAt + periodMs,
    oldestUnpublished: !!series && !series.queue.oldestS.length, dlqUnpublished: !!series && !!s?.dlqName && !series.dlq.length && !f.dlqFromSqs,
    windows: windows.flatMap((w): LiveWindow[] => {
      const x = aws.series[w.id];
      if (!x) return [];
      const fromSpans = spans && w.id === SPAN_WINDOW ? { stuck: spans.stuck, ...(spans.nearDlq ? { nearDlq: spans.nearDlq } : {}) } : {};
      return [{ periodS: x.periodS, fromMs: x.fromMs, toMs: x.toMs, ...x.queue, dlq: x.dlq, ...fromSpans }];
    }).sort((a, b) => a.periodS - b.periodS),
    stuck: view ? view.held.filter((m) => m.sentMs < now - STUCK_AFTER_MS).length : null,
    nearDlq: near === null || !view ? null : { count: near, inFlight: view.instrumented.inFlight },
  };
}

/** A trigger as the queue page draws it. */
export const pageTrigger = (t: AwsTrigger): PageTrigger => ({
  functionName: t.functionName, state: t.state, batchSize: t.batchSize, batchingWindowS: t.batchingWindowS,
  reportBatchItemFailures: t.reportBatchItemFailures, maxConcurrency: t.maxConcurrency, filters: t.filters,
});

/** The queue page's AWS part (QueuePageData.aws) from a queue's reads over the page's window. */
export function queueAwsView(a: QueueAws, windowId: string): QueueAwsView {
  const w = a.series[windowId];
  const f = nowFigures(a, windowId);
  return {
    status: a.status, detail: a.detail, readAt: a.readAt,
    settings: a.settings ? { fifo: a.settings.fifo, visibilityTimeoutS: a.settings.visibilityTimeoutS, retentionS: a.settings.retentionS, delayS: a.settings.delayS,
      maxReceiveCount: a.settings.maxReceiveCount, dlqName: a.settings.dlqName, visible: a.settings.visible, inFlight: a.settings.inFlight } : null,
    series: w ? { visible: w.queue.visible, inFlight: w.queue.inFlight, oldestS: w.queue.oldestS, sent: w.queue.sent, dlq: w.dlq } : null,
    now: a.status === "ok" ? { visible: f.visible, inFlight: f.inFlight, oldestS: f.oldestS, oldestAt: f.oldestAt, dlqDepth: f.dlqDepth, dlqAt: f.dlqAt, dlqChange: f.dlqChange } : null,
    errors: a.errors,
  };
}
