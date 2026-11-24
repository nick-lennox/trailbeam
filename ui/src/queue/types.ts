/*
  What the queue page takes (QV-360 design §9): one QueuePageData, the same shape in both products. `aws` is null
  in self-hosted Trailbeam, which makes no AWS calls; in the hosted app it says whether AWS was read and what it
  answered. The view checks what it was given, never which product it runs in.

  QueueSettings, Trigger and the CloudWatch points are the hosted app's own types (lib/app/aws-reads.ts), cut to
  the fields the page reads, so the app passes its objects as they are.
*/

export type Tone = "good" | "warning" | "critical";
/** [period start ms, value] */
export type Point = [number, number];

export type VersionBucket = { version: string | null; handled: number; p95QueueMs: number | null; p95HandlerMs: number | null; queueSamples?: number; handlerSamples?: number };

/** One step of the span charts: only steps with at least one event are listed (a missing step is a gap, not a zero). */
export type Bucket = {
  t: number;
  sends: number;
  receives: number;
  /** of receives, those with a receive count of 2 or more */
  reReceives: number;
  /** successful deletes plus Lambda successes */
  handled: number;
  failed: number;
  /** DLQ moves seen: receives on the DLQ that carried DeadLetterQueueSourceArn for this queue */
  dlqSeen: number;
  /** DLQ moves worked out: a receive count at maxReceiveCount with no DLQ receive yet */
  dlqInferred: number;
  /** send to first receive */
  p95QueueMs: number | null;
  /** receive to delete or processing error */
  p95HandlerMs: number | null;
  /** receives with no recorded send, left out of p95QueueMs */
  noSendReceives: number;
  byVersion?: VersionBucket[];
};

export type QueueSettings = {
  fifo?: boolean;
  visibilityTimeoutS: number | null;
  retentionS: number | null;
  delayS: number | null;
  maxReceiveCount: number | null;
  dlqName: string | null;
  visible?: number | null;
  inFlight?: number | null;
};

export type Trigger = {
  functionName: string;
  state: string | null;
  batchSize: number | null;
  batchingWindowS: number | null;
  reportBatchItemFailures: boolean;
  maxConcurrency: number | null;
  filters: number;
};

export type QueueSender = { service: string; sends: number; versions: string[]; lastMs: number | null };
export type QueueConsumer = {
  name: string;
  kind: "lambda" | "service";
  receives: number;
  /** null: this product doesn't count it per consumer */
  handled: number | null;
  failed: number | null;
  p95HandlerMs: number | null;
  versions: string[];
  lastMs: number | null;
  /** the Lambda trigger on this queue (hosted, AWS read) */
  trigger?: Trigger;
};

/** A row of "Why messages here didn't finish": the hosted cause rows (Needs attention), for this queue. */
export type CauseRow = {
  id: string;
  outcome: "dead_lettered" | "stuck" | "filtered";
  signature: string;
  consumer: string | null;
  messages: number;
  lastMs: number;
  versions?: string[];
  href: string;
};
/** Self-hosted Trailbeam before shared cause rows: how many are stuck, and how many moved to the DLQ. */
export type CauseCounts = { stuck: number; stuckMore?: boolean; dlq: number };

export type AwsStatus = "ok" | "no_account" | "not_configured" | "failed";
export type AwsSeries = { visible: Point[]; inFlight: Point[]; oldestS: Point[]; sent: Point[]; dlq: Point[] };
export type AwsNow = {
  visible: number | null;
  inFlight: number | null;
  oldestS: number | null;
  oldestAt: number | null;
  dlqDepth: number | null;
  dlqAt: number | null;
  dlqChange: number | null;
};

export type QueueAwsView = {
  status: AwsStatus;
  /** why there is nothing, or what AWS answered, in a sentence */
  detail: string | null;
  readAt: number | null;
  settings: QueueSettings | null;
  series: AwsSeries | null;
  /** the hosted nowFigures() */
  now?: AwsNow | null;
  /** reads that failed while others worked, e.g. "cloudwatch:GetMetricData failed with AccessDenied" */
  errors: string[];
};

export type QueueHealth = { tone: Tone; why: string; basis: "aws" | "spans" };

export type QueuePageData = {
  queue: { name: string; account: string | null; region: string | null; fifo: boolean; key: string; url?: string | null };
  range: { fromMs: number; toMs: number; stepS: number; endsNow: boolean };
  health: QueueHealth | null;
  /** why there is no verdict, when health is null (hosted with span health off); a default sentence otherwise */
  noHealth?: string | null;
  /** the server's own settings, self-hosted; null in the hosted app */
  configured: { maxReceiveCount: number | null; isDefault: boolean } | null;
  /** the DLQ, from a DLQ receive with DeadLetterQueueSourceArn */
  dlqSeen: { name: string } | null;
  series: Bucket[];
  /** deploy markers: a service's new service.version, at its first span in the range */
  versions: { service: string; version: string; firstMs: number }[];
  senders: QueueSender[];
  consumers: QueueConsumer[];
  /** triggers on this queue whose function sent no span in the range (hosted, AWS read) */
  silentTriggers?: Trigger[];
  causes: CauseRow[] | CauseCounts;
  /** stuck messages on this queue now, for the Now row */
  stuck?: number | null;
  aws: QueueAwsView | null;
  /** the hosted sample workspace */
  sample?: boolean;
};
