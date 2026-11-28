/*
  Self-hosted Trailbeam's answers as the queue page's data: GET /api/queues/{queue}/series (QV-363), one flow of
  GET /api/flows (who sends and receives, and the DLQ seen on a move), and the queue's stuck count from
  /api/stuck. AWS reads are off by default (QV-589): then `aws` is null, the page keeps "What Trailbeam can't see
  here", and the health verdict is from spans. With reads on, /series carries what AWS says about the queue over the
  page's range (`aws`, the server's "page" window): a queue it read has its settings, CloudWatch, Lambda triggers and
  health from SQS and CloudWatch, as in the hosted app; one it didn't has the server's reason.
*/
import { spanFiguresOf, spanHealth } from "./model.ts";
import { liveOf, pageTrigger, queueAwsView, type QueueAws } from "../model/aws.ts";
import { health as awsHealth } from "../model/queue-live.ts";
import type { Bucket, QueuePageData, Trigger } from "./types.ts";

/** The window /series reads AWS over (queue_data.PAGE_WINDOW). */
export const PAGE_WINDOW = "page";

/** One bucket as /api/queues/{queue}/series returns it. */
export type ApiBucket = {
  t_ms: number; sends: number; receives: number; re_receives: number; handled: number; failed: number;
  dlq_seen: number; dlq_inferred: number; no_send_receives: number;
  p95_queue_ms: number | null; p95_handler_ms: number | null; queue_samples?: number; handler_samples?: number;
  by_version?: { version: string | null; handled: number; p95_queue_ms: number | null; p95_handler_ms: number | null; queue_samples: number; handler_samples: number }[];
};
export type SeriesResponse = {
  queue: { key: string; name: string; account: string | null; region: string | null; fifo: boolean };
  from_ms: number; to_ms: number; step_s: number;
  max_receive_count: number | null;
  series: ApiBucket[];
  versions: { service: string; version: string; first_ms: number }[];
  /** what AWS says about the queue over the range; a server before QV-589 part 3 sends none */
  aws?: QueueAws;
};
/** One flow of /api/flows: the hosted app's Flow (lib/app/flows.ts), field for field. */
export type Flow = {
  key: string; queue: string; name: string; url: string | null; account: string | null; region: string | null; lastSeen: number;
  sent: number; received: number; deleted: number; visibilityChanges: number;
  producers: { name: string; n: number; lambda: boolean }[];
  consumers: { name: string; n: number; lambda: boolean }[];
  avgBytes: number | null;
  dlq: { name: string; received: number } | null;
};
export type FlowsResponse = { from_ms: number; to_ms: number; flows: Flow[] };

export const bucketOf = (b: ApiBucket): Bucket => ({
  t: b.t_ms, sends: b.sends, receives: b.receives, reReceives: b.re_receives, handled: b.handled, failed: b.failed,
  dlqSeen: b.dlq_seen, dlqInferred: b.dlq_inferred, p95QueueMs: b.p95_queue_ms, p95HandlerMs: b.p95_handler_ms, noSendReceives: b.no_send_receives,
  ...(b.by_version && {
    byVersion: b.by_version.map((v) => ({ version: v.version, handled: v.handled, p95QueueMs: v.p95_queue_ms, p95HandlerMs: v.p95_handler_ms, queueSamples: v.queue_samples, handlerSamples: v.handler_samples })),
  }),
});

/** The flow for a series' queue: by key, else by name when one flow has it. */
export function flowFor(flows: Flow[], s: SeriesResponse["queue"]): Flow | null {
  const byKey = flows.find((f) => f.key === s.key);
  if (byKey) return byKey;
  const named = flows.filter((f) => f.name === s.name);
  return named.length === 1 ? named[0] : null;
}

/**
 * The page's data from self-hosted Trailbeam's answers. `stuck`: the queue's stuck messages now (with `more` when
 * the list had another page), or null when it couldn't be read. `endsNow`: the range ends at the page's now.
 * `awsOn`: the server has AWS reads on (/api/health config.aws.on). With them off, `aws` is null even when /series
 * sent its not_configured answer, so the page keeps its note on what Trailbeam can't see.
 */
export function fromSelfHosted(o: { series: SeriesResponse; flow: Flow | null; stuck: { count: number; more: boolean } | null; endsNow: boolean; awsOn?: boolean }): QueuePageData {
  const s = o.series;
  const series = s.series.map(bucketOf);
  const f = o.flow;
  const stuck = o.stuck?.count ?? 0;
  const dlq = series.reduce((n, b) => n + b.dlqSeen + b.dlqInferred, 0);
  const a = s.aws && (s.aws.status !== "not_configured" || o.awsOn) ? s.aws : null;
  const read = a?.status === "ok";
  // the queue's figures over the page's window, for the verdict from SQS and CloudWatch (no range-wide p95 here, so no "Near timeout")
  const live = read ? liveOf(a!, null, [{ id: PAGE_WINDOW, fromMs: s.from_ms, toMs: s.to_ms, periodS: a!.series[PAGE_WINDOW]?.periodS ?? s.step_s }], s.to_ms) : null;
  const fromAws = live ? awsHealth({ ...live, stuck }) : null;
  // the Lambda triggers on this queue: each on its function's row, or a row of its own when the function sent no span
  const consumers = (f?.consumers ?? []).map((c): QueuePageData["consumers"][number] => ({
    name: c.name, kind: c.lambda ? "lambda" : "service", receives: c.n, handled: null, failed: null, p95HandlerMs: null, versions: [], lastMs: null,
  }));
  const silent: Trigger[] = [];
  for (const t of read ? a!.triggers ?? [] : []) {
    const c = consumers.find((x) => x.name === t.functionName);
    if (c) { c.trigger = pageTrigger(t); c.kind = "lambda"; } else silent.push(pageTrigger(t));
  }
  return {
    queue: { ...s.queue, account: s.queue.account ?? a?.accountId ?? null, region: s.queue.region ?? a?.region ?? null, fifo: a?.settings?.fifo ?? s.queue.fifo, url: f?.url ?? null },
    range: { fromMs: s.from_ms, toMs: s.to_ms, stepS: s.step_s, endsNow: o.endsNow },
    health: fromAws ? { ...fromAws, basis: "aws" } : spanHealth(spanFiguresOf(series, s.to_ms, { stuck, failingNow: false, retrying: false })),
    configured: { maxReceiveCount: s.max_receive_count, isDefault: false },
    dlqSeen: f?.dlq ? { name: f.dlq.name } : null,
    series,
    versions: s.versions.map((v) => ({ service: v.service, version: v.version, firstMs: v.first_ms })),
    senders: (f?.producers ?? []).map((p) => ({ service: p.name, sends: p.n, versions: [], lastMs: null })),
    consumers,
    ...(silent.length ? { silentTriggers: silent } : {}),
    causes: { stuck, stuckMore: o.stuck?.more ?? false, dlq },
    stuck: o.stuck?.count ?? null,
    aws: a && queueAwsView(a, PAGE_WINDOW),
  };
}
