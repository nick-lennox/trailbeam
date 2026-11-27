import type { Flow } from "../queue/selfhosted.ts";
import { FAILING_NOW, spanHealth } from "../queue/model.ts";
import { dlqChange, health as healthWhy, type QueueLive, type Tone } from "./queue-live.ts";
import type { Cause } from "./causes.ts";
import type { Ev } from "./events.ts";

/** A queue as the Overview draws it (the hosted sample topology's Queue). */
export type Queue = {
  name: string;
  type: "Standard" | "FIFO";
  consumer: string;
  sentPerMin: number;
  p95WaitMs: number;
  stuck: number;
  dlq: number;
  health: "good" | "warning" | "critical";
  spark: number[];
};
/** Sends from a producer to a queue, or receives from a queue by a consumer, per minute. */
export type Edge = { from: string; to: string; perMin: number };

/** The 95th percentile of xs, nearest rank; 0 with none. The dashboards' measure too. */
export function p95Of(xs: number[]): number {
  if (!xs.length) return 0;
  const s = [...xs].sort((a, b) => a - b);
  return s[Math.min(s.length - 1, Math.ceil(0.95 * s.length) - 1)];
}

/** What "In ‹queue› now" reads of a queue's messages (the hosted app's InQueueView, which has more). */
export type InQueueLike = {
  held: { receiveCount: number; state: "visible" | "in_flight"; leftS: number; sentMs: number }[];
  notSeenOnDlq: { timedOutMs: number }[];
  seenOnDlq: number;
};
/** What a host read about one queue, by flow key: its AWS triggers (T), its figures, and its messages now (V). */
export type QueueData<T = unknown, V extends InQueueLike = InQueueLike> = { aws: { triggers: T[] | null }; live: QueueLive; inQueue: V | null };

/*
  What the Overview draws for recorded data. It has the same shapes as the sample topology, built from the
  spans the services reported (GET /api/flows, or the hosted app's flows.ts: the same Flow). What spans can't say (queue depth, queue
  settings, why a message did not finish) is left out, and the page writes
  "No data" in its place.

  Each consumer's processing figures come from the range's events (eventsOfTimeline), the same
  events the dashboards chart "Processing time, p95" and "Processing errors" from: its deletes
  and processing errors. A consumer with neither in the range has none, and the page says "No data".
*/
export type OverviewData<T = unknown, V extends InQueueLike = InQueueLike> = {
  workspace: string;
  range: string;
  queues: Queue[];
  sends: Edge[];
  receives: Edge[];
  /* runtime: the key of the icon drawn for it (Lambda, ECS, ...), or ""; runsOn: what its spans' resource says it runs on */
  consumers: { name: string; runtime: string; queue: string; runsOn?: string }[];
  /* queue label to consumer name to its processing figures in the range (consumerFigures) */
  consumerStats: Record<string, Record<string, ConsumerFigures>>;
  /* true when the range had more messages than were read for consumerStats: they are of the most recently active ones */
  consumerStatsCapped: boolean;
  /* per producer, what it runs on, when its spans say: "Lambda" when its sends carried faas.name.
     The spans Trailbeam stores say nothing of other platforms, so other producers are not listed. */
  producerPlatform: Record<string, string>;
  /* per queue, the average body size of its sends in the range, when the sends reported one */
  avgBytes: Record<string, number>;
  /* queue name to its dead-letter queue, when a receive on that queue named this one as its source.
     A queue's name is its label: the name, with its account or region when another queue has the same name. */
  dlq: Record<string, { name: string; received: number }>;
  /* where each queue lives, read from its URL */
  places: Record<string, { account: string | null; region: string | null }>;
  /* what AWS and the spans say about each queue, by its label */
  info: Record<string, QueueInfo<T, V>>;
  /* per Lambda function, from its own log: invocations in the range that timed out or ran out of memory.
     Only functions whose logs Trailbeam read are in it. */
  functionEnds: Record<string, { timeouts: number; oom: number }>;
  /* why messages didn't finish, one row per cause (toCause), worst outcome first; queue is the flow's label */
  causes: Cause[];
  /* more messages were unfinished than were read for causes: they are of the most recently active ones */
  causesCapped: boolean;
  now: number;
};

/*
  One consumer's processing in the range. processed: its deletes that succeeded and its processing errors,
  failed: the processing errors of those. p95Ms: the 95th percentile of their processing times, over the
  events that have one (null when none does). A DeleteMessage call that failed says nothing about the
  handler, so it is in neither count, though its time is in p95Ms, as on the dashboards.
*/
export type ConsumerFigures = { p95Ms: number | null; processed: number; failed: number; failPct: number };

/* Events to the flow they belong to: the queue's name, and its account and region where both sides know them. */
function flowOfEvent(flows: Flow[], e: Ev): Flow | undefined {
  const same = flows.filter((f) => f.name === e.queue);
  if (same.length <= 1) return same[0];
  const account = e.attrs?.["queue.account"], region = e.attrs?.["queue.region"];
  return same.find((f) => (!account || f.account === account) && (!region || f.region === region));
}

/** Each consumer's processing figures, by queue label then consumer, from the events in [sinceMs, nowMs]. */
export function consumerFigures(events: Ev[], flows: Flow[], sinceMs: number, nowMs: number): Record<string, Record<string, ConsumerFigures>> {
  const acc = new Map<string, { label: string; consumer: string; times: number[]; processed: number; failed: number }>();
  for (const e of events) {
    if ((e.event !== "delete" && e.event !== "processing_error") || !e.consumer || e.ms < sinceMs || e.ms > nowMs) continue;
    const f = flowOfEvent(flows, e);
    if (!f) continue;
    const k = `${f.queue}\u0000${e.consumer}`;
    const a = acc.get(k) ?? { label: f.queue, consumer: e.consumer, times: [], processed: 0, failed: 0 };
    if (e.durationMs !== undefined && Number.isFinite(e.durationMs)) a.times.push(e.durationMs);
    if (e.event === "processing_error") { a.processed++; a.failed++; } else if (e.status === "ok") a.processed++;
    acc.set(k, a);
  }
  const out: Record<string, Record<string, ConsumerFigures>> = {};
  for (const a of acc.values()) {
    (out[a.label] ??= {})[a.consumer] = {
      p95Ms: a.times.length ? Math.round(p95Of(a.times)) : null, processed: a.processed, failed: a.failed,
      failPct: a.processed ? Math.round((1000 * a.failed) / a.processed) / 10 : 0,
    };
  }
  return out;
}

/* Each flow's p95 processing time over [sinceMs, nowMs], all consumers together, for health()'s "Near timeout": the
   same events and measure as consumerFigures and the dashboards' queue health (measures.ts p95Processing). */
export function queueP95(events: Ev[], flows: Flow[], sinceMs: number, nowMs: number): Map<Flow, number> {
  const times = new Map<Flow, number[]>();
  for (const e of events) {
    if ((e.event !== "delete" && e.event !== "processing_error") || e.durationMs === undefined || !Number.isFinite(e.durationMs) || e.ms < sinceMs || e.ms > nowMs) continue;
    const f = flowOfEvent(flows, e);
    if (f) (times.get(f) ?? times.set(f, []).get(f)!).push(e.durationMs);
  }
  return new Map([...times].map(([f, xs]) => [f, p95Of(xs)]));
}

/* What overviewData reads besides the flows: the range's events, for the consumers' processing figures,
   and the services, for what each consumer runs on. */
export type OverviewExtras = { events?: Ev[]; eventsCapped?: boolean; services?: { name: string; runsOn: string | null; platform: string | null }[];
  /* the unfinished messages' causes, by flow label, with what span-only health reads from each */
  causes?: { row: Cause; lastMs: number; retrying: number }[]; causesCapped?: boolean;
  /* give a health verdict from spans alone where AWS wasn't read (spanHealth) */
  spanHealth?: boolean };

/* At most this many messages of a queue are sent to the page for "In ‹queue› now"; the rest are counted. */
export const HELD_LIMIT = 200;

export type QueueInfo<T = unknown, V extends InQueueLike = InQueueLike> = {
  live: QueueLive;
  health: Tone | null;
  /* the reason for it, in a word or two */
  why: string | null;
  /* what the verdict rests on: AWS's figures, or the spans alone (spanHealth) */
  healthBasis: "aws" | "spans" | null;
  triggers: T[] | null;
  inQueue: V | null;
  /* messages in the queue that were worked out, before HELD_LIMIT */
  heldTotal: number;
  /* moves to the dead-letter queue in the range: worked out from receive counts (moved), and receives
     SQS made on the dead-letter queue that name this queue (dlqReceives) */
  moved: number;
  dlqReceives: number;
  /* the dead-letter queue's depth change over the range, from CloudWatch */
  dlqChange: number | null;
};

/* the order "In ‹queue› now" lists messages in: closest to the dead-letter queue first */
const closest = (a: InQueueLike["held"][number], b: InQueueLike["held"][number]) =>
  b.receiveCount - a.receiveCount || (a.state === b.state ? (a.state === "in_flight" ? a.leftS - b.leftS : a.sentMs - b.sentMs) : a.state === "in_flight" ? -1 : 1);

export function overviewData<T = unknown, V extends InQueueLike = InQueueLike>(workspace: string, range: string, minutes: number, flows: Flow[], queueData: Record<string, QueueData<T, V>> = {}, now = Date.now(),
  functionEnds: OverviewData["functionEnds"] = {}, extras: OverviewExtras = {}): OverviewData<T, V> {
  const perMin = (n: number) => Math.round((n / minutes) * 10) / 10;
  const since = now - minutes * 60_000;
  const data: OverviewData<T, V> = { workspace, range, queues: [], sends: [], receives: [], consumers: [], producerPlatform: {}, avgBytes: {}, dlq: {}, places: {}, info: {}, functionEnds, now,
    consumerStats: consumerFigures(extras.events ?? [], flows, since, now), consumerStatsCapped: !!extras.eventsCapped,
    causes: (extras.causes ?? []).map((c) => c.row), causesCapped: !!extras.causesCapped };
  const services = new Map((extras.services ?? []).map((s) => [s.name, s]));
  const p95 = queueP95(extras.events ?? [], flows, since, now);
  for (const f of flows) {
    const qd = queueData[f.key];
    let health: Tone | null = null;
    if (qd) {
      const view = qd.inQueue;
      const moved = view ? view.notSeenOnDlq.filter((m) => m.timedOutMs >= since).length : 0;
      const mine = (extras.causes ?? []).filter((c) => c.row.queue === f.queue);
      const aws = healthWhy(qd.live, p95.get(f) ?? null);
      const h = aws ?? (extras.spanHealth ? spanHealth({
        seenOnDlq: view?.seenOnDlq ?? 0, moved, stuck: qd.live.stuck ?? 0,
        failingNow: mine.some((c) => c.row.count >= FAILING_NOW.messages && now - c.lastMs <= FAILING_NOW.withinMs),
        retrying: mine.some((c) => c.retrying > 0), active: f.sent > 0 || f.consumers.some((c) => c.n > 0),
      }) : null);
      health = h?.tone ?? null;
      const w = qd.live.windows[0];
      data.info[f.queue] = {
        live: qd.live, health, why: h?.why ?? null, healthBasis: aws ? "aws" : h ? "spans" : null, triggers: qd.aws.triggers,
        inQueue: view ? { ...view, held: [...view.held].sort(closest).slice(0, HELD_LIMIT) } : null,
        heldTotal: view?.held.length ?? 0,
        moved,
        dlqReceives: f.dlq?.received ?? 0,
        dlqChange: w ? dlqChange(w, since) : null,
      };
    }
    const fifo = qd?.live.fifo ?? f.queue.endsWith(".fifo");
    data.queues.push({ name: f.queue, type: fifo ? "FIFO" : "Standard", consumer: f.consumers[0]?.name ?? "", sentPerMin: perMin(f.sent), p95WaitMs: 0,
      stuck: qd?.live.stuck ?? 0, dlq: qd?.live.dlqDepth ?? 0, health: health ?? "good", spark: [] });
    for (const p of f.producers) {
      data.sends.push({ from: p.name, to: f.queue, perMin: perMin(p.n) });
      if (p.lambda) data.producerPlatform[p.name] = "Lambda";
    }
    if (f.avgBytes !== null) data.avgBytes[f.queue] = f.avgBytes;
    // every consumer, each with its own receives
    for (const c of f.consumers) {
      const svc = services.get(c.name);
      data.consumers.push({ name: c.name, runtime: c.lambda ? "Lambda" : svc?.platform ?? "", queue: f.queue, ...(svc?.runsOn ? { runsOn: svc.runsOn } : {}) });
      data.receives.push({ from: f.queue, to: c.name, perMin: perMin(c.n) });
    }
    if (f.dlq) data.dlq[f.queue] = f.dlq;
    data.places[f.queue] = { account: f.account, region: f.region };
  }
  return data;
}
