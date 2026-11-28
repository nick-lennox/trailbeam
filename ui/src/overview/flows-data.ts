/*
  Self-hosted Trailbeam's data for the shared Overview (OverviewFlows, @trailbeam/ui/design), from its own API: GET /api/flows
  for the range, GET /api/causes for why messages didn't finish in it, and GET /api/queues/{queue}/now for each queue's
  messages now, with what AWS says about it (QV-589). AWS reads are off by default: then every queue's figures are the
  spans' alone, its QueueLive says no AWS read was made ("not_configured"), and its health is spanHealth's verdict. A
  queue the server read (aws.status "ok") has the server's QueueLive (depth, age, settings) and Lambda triggers, and its
  health from them, as in the hosted app. A queue it didn't read keeps the spans' figures, with the server's reason in
  `detail`. The URL is the Event explorer's range=<preset> or from=&to=, and flow=. Pure.
*/
import { overviewData, type OverviewData } from "../model/overview.ts";
import type { QueueLive } from "../model/queue-live.ts";
import { pageTrigger, type QueueAws } from "../model/aws.ts";
import type { InQueueView } from "../model/in-queue.ts";
import { toCause } from "../model/causes.ts";
import type { OverviewRangeId } from "../model/format.ts";
import { follow, pinned, rangeFromParams, withRange, type TimeRange } from "../model/timerange.ts";
import { QUEUE_PRESETS, type QueueRange } from "../queue/model.ts";
import type { CauseGroup, CausePage } from "../causes/types.ts";
import type { Flow } from "../queue/selfhosted.ts";
import type { Trigger } from "../queue/types.ts";

/**
 * GET /api/queues/{queue}/now: the queue it read and "In ‹queue› now", with what AWS says about it (`aws`) and its
 * figures for the Overview's card (`live`). A server before QV-589 part 3 sends neither.
 */
export type QueueNowResponse = InQueueView & {
  queue: { key: string; name: string; account: string | null; region: string | null; fifo: boolean };
  aws?: QueueAws; live?: QueueLive;
};
/** "In ‹queue› now" with the queue's AWS parts, as selfHostedFlowsData takes it. */
export type QueueNowAnswer = InQueueView & { aws?: QueueAws; live?: QueueLive };

/** The /now path for a queue on the Overview, asking for CloudWatch over its past `minutes` (whole, at least one). */
export const overviewNowPath = (key: string, minutes: number) =>
  `/api/queues/${encodeURIComponent(key)}/now?minutes=${Math.max(1, Math.round(minutes))}`;

/**
 * GET /api/queues/now?keys=: /now for several queues at once (QV-743), by the key asked. The server reads their AWS
 * figures together, one CloudWatch call per region, and keeps that read a minute for every viewer.
 */
export type QueuesNowResponse = { minutes: number | null; queues: Record<string, QueueNowResponse>; errors: Record<string, string> };

/** The most queues the Overview asks for in one request: the server takes 100 (queue_data.NOW_BATCH_MAX), and this keeps the URL short. */
export const NOW_BATCH = 50;

/**
 * The Overview's requests for its queues' "In ‹queue› now" and AWS figures over the past `minutes`: the keys sorted and
 * in groups of NOW_BATCH, so every viewer of the same queues makes the same requests, and the server's kept read answers them.
 */
export function overviewNowPaths(keys: string[], minutes: number): string[] {
  const sorted = [...new Set(keys)].sort();
  const out: string[] = [];
  for (let i = 0; i < sorted.length; i += NOW_BATCH) {
    out.push(`/api/queues/now?keys=${sorted.slice(i, i + NOW_BATCH).map(encodeURIComponent).join(",")}&minutes=${Math.max(1, Math.round(minutes))}`);
  }
  return out;
}

/** Whether any queue on the page was read from AWS: the page then says where its depth and age come from. */
export const anyAwsRead = (now: Record<string, QueueNowAnswer | null>) => Object.values(now).some((v) => v?.aws?.status === "ok");

export type FlowsUrl = { time: TimeRange; flow: string | undefined };

/**
 * The page's state from its search parameters, at nowMs: the Event explorer's range=<preset> or from=&to= (rangeFromParams),
 * so a 7d, 4h or exact range opens as it was. The page before QV-579 wrote a fixed range as from_ms=&to_ms=, and its links
 * still open on it. A range that can't be read is the default, the past hour.
 */
export function readFlowsUrl(p: URLSearchParams, nowMs: number): FlowsUrl {
  const fromMs = p.get("from_ms"), toMs = p.get("to_ms");
  const get = (k: string) => (k === "from" && !p.get("from") ? fromMs : k === "to" && !p.get("to") ? toMs : p.get(k));
  return { time: rangeFromParams(get, nowMs), flow: p.get("flow") || undefined };
}
/** The URL for a state, with the default range (the past hour) left out. */
export function flowsHref(u: FlowsUrl): string {
  const href = withRange("/overview", u.time);
  if (!u.flow) return href;
  return `${href}${href.includes("?") ? "&" : "?"}${new URLSearchParams({ flow: u.flow })}`;
}
/** The same page later: a range that ends now pinned to the times it covers now, for Copy link. */
export const pinnedFlowsHref = (u: FlowsUrl, nowMs: number) => flowsHref({ ...u, time: pinned(follow(u.time, nowMs)) });
/** A stable key for a range, so the page asks again when it changes. */
export const timeKey = (t: TimeRange) => (t.preset ? t.preset : `${t.from}-${t.to}`);

/**
 * The window a range asks for at nowMs (a range that ends now moves up to nowMs, a fixed one stays), clipped to what
 * the server keeps (keptMs, null when forever or unknown). `clipped`: its start was before the oldest kept span.
 */
export function flowsWindow(t: TimeRange, nowMs: number, keptMs: number | null): { fromMs: number; toMs: number; minutes: number; clipped: boolean } {
  const r = follow(t, nowMs);
  const kept = keptMs === null ? null : nowMs - keptMs;
  const clipped = kept !== null && r.from < kept;
  const fromMs = clipped ? Math.min(kept, r.to) : r.from;
  return { fromMs, toMs: r.to, minutes: Math.max(0, r.to - fromMs) / 60_000, clipped };
}

/** The Overview range nearest a time range (data.range, for the view's sample-sized wording): 15m, 1h or 24h. */
export function overviewRangeOf(t: TimeRange): OverviewRangeId {
  const minutes = (t.to - t.from) / 60_000;
  return minutes <= 15 ? "15m" : minutes <= 60 ? "1h" : "24h";
}
/** A queue's page at the Overview's range: the same preset when the queue page has it, else the window's exact times. */
export function queueRangeOf(t: TimeRange, w: { fromMs: number; toMs: number }): QueueRange {
  const p = QUEUE_PRESETS.find(([k]) => k === t.preset);
  return p ? { kind: "preset", preset: p[0] } : { kind: "fixed", fromMs: Math.floor(w.fromMs), toMs: Math.ceil(w.toMs) };
}

/** The flow a cause group is of: by queue key, else by name when one flow has it. */
export function flowOfGroup(flows: Flow[], g: Pick<CauseGroup, "queue" | "queueKey">): Flow | undefined {
  const byKey = flows.find((f) => f.key === g.queueKey);
  if (byKey) return byKey;
  const named = flows.filter((f) => f.name === g.queue);
  return named.length === 1 ? named[0] : undefined;
}

/**
 * A queue's figures when no AWS read was made: its settings from what "In ‹queue› now" read of the spans and the
 * server's configuration (maxReceiveCount, the visibility timeout a receive reported), its dead-letter queue from
 * a receive that named it, and its stuck messages from the causes. Depth, age and CloudWatch history are unknown.
 */
export function spanOnlyLive(o: { view: InQueueView | null; fifo: boolean; dlq: string | null; stuck: number }): QueueLive {
  const c = o.view?.config;
  return {
    status: "not_configured", detail: null, readAt: null,
    visibilityTimeoutS: c?.visibilityKnown ? c.visibilityTimeoutS : null, retentionS: null, maxReceiveCount: c?.maxReceiveCount ?? null,
    dlq: c?.dlq ?? o.dlq, fifo: o.fifo,
    visible: null, inFlight: null, oldestS: null, dlqDepth: null, oldestAsOf: null, dlqAsOf: null, oldestUnpublished: false, dlqUnpublished: false,
    windows: [], stuck: o.stuck, nearDlq: null,
  };
}

export type SelfHostedFlowsInput = {
  range: OverviewRangeId | string;
  window: { fromMs: number; toMs: number; minutes: number };
  flows: Flow[];
  /** GET /api/causes for the window, or null when it couldn't be read */
  causes: CausePage | null;
  /** each flow's GET /api/queues/{queue}/now, by flow key, or null when it couldn't be read */
  now: Record<string, QueueNowAnswer | null>;
  /** a cause group's page on this server */
  causeHref: (id: string) => string;
};

/**
 * The shared Overview's data on this server. A queue AWS was read for has the server's QueueLive and its Lambda
 * triggers, so its health is from SQS and CloudWatch; any other has the spans' figures and health from spans alone,
 * with the server's reason for not reading it.
 */
export function selfHostedFlowsData(i: SelfHostedFlowsInput): OverviewData<Trigger, InQueueView> {
  const { flows, window: w } = i;
  const groups = (i.causes?.groups ?? []).map((g) => ({ g, f: flowOfGroup(flows, g) })).filter((x): x is { g: CauseGroup; f: Flow } => !!x.f);
  const causes = groups.map(({ g, f }) => ({ row: toCause(g, f.queue, w.fromMs, w.toMs, i.causeHref), lastMs: g.lastMs, retrying: g.retrying }));
  const queueData = Object.fromEntries(flows.map((f) => {
    const got = i.now[f.key] ?? null;
    const view = got && inQueueOf(got);
    const stuck = groups.filter((x) => x.f === f).reduce((a, x) => a + (x.g.counts.stuck ?? 0), 0);
    const aws = got?.aws, live = got?.live;
    if (aws?.status === "ok" && live) return [f.key, { aws: { triggers: aws.triggers && aws.triggers.map(pageTrigger) }, live, inQueue: view }];
    const spans = spanOnlyLive({ view, fifo: f.name.endsWith(".fifo"), dlq: f.dlq?.name ?? null, stuck });
    // not read: the spans' figures, with the server's reason (reads off, not named, or what AWS answered)
    return [f.key, { aws: { triggers: null }, live: aws ? { ...spans, status: aws.status, detail: live?.detail ?? aws.detail } : spans, inQueue: view }];
  }));
  return overviewData<Trigger, InQueueView>("this server", i.range, w.minutes, flows, queueData, w.toMs, {},
    { causes, causesCapped: !!i.causes?.capped, spanHealth: true });
}

/** "In ‹queue› now" without the AWS parts /now sends with it. */
function inQueueOf(r: QueueNowAnswer): InQueueView {
  const { aws: _aws, live: _live, ...view } = r;
  return view;
}
