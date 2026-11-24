/*
  Self-hosted Trailbeam's answers as the queue page's data: GET /api/queues/{queue}/series (QV-363), one flow of
  GET /api/flows (who sends and receives, and the DLQ seen on a move), and the queue's stuck count from
  /api/stuck. Self-hosted Trailbeam makes no AWS calls, so `aws` is null and the health verdict is from spans.
*/
import { spanFiguresOf, spanHealth } from "./model.ts";
import type { Bucket, QueuePageData } from "./types.ts";

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
 */
export function fromSelfHosted(o: { series: SeriesResponse; flow: Flow | null; stuck: { count: number; more: boolean } | null; endsNow: boolean }): QueuePageData {
  const s = o.series;
  const series = s.series.map(bucketOf);
  const f = o.flow;
  const stuck = o.stuck?.count ?? 0;
  const dlq = series.reduce((n, b) => n + b.dlqSeen + b.dlqInferred, 0);
  return {
    queue: { ...s.queue, url: f?.url ?? null },
    range: { fromMs: s.from_ms, toMs: s.to_ms, stepS: s.step_s, endsNow: o.endsNow },
    health: spanHealth(spanFiguresOf(series, s.to_ms, { stuck, failingNow: false, retrying: false })),
    configured: { maxReceiveCount: s.max_receive_count, isDefault: false },
    dlqSeen: f?.dlq ? { name: f.dlq.name } : null,
    series,
    versions: s.versions.map((v) => ({ service: v.service, version: v.version, firstMs: v.first_ms })),
    senders: (f?.producers ?? []).map((p) => ({ service: p.name, sends: p.n, versions: [], lastMs: null })),
    consumers: (f?.consumers ?? []).map((c) => ({
      name: c.name, kind: c.lambda ? "lambda" : "service", receives: c.n, handled: null, failed: null, p95HandlerMs: null, versions: [], lastMs: null,
    })),
    causes: { stuck, stuckMore: o.stuck?.more ?? false, dlq },
    stuck: o.stuck?.count ?? null,
    aws: null,
  };
}
