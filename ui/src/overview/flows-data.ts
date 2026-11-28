/*
  Self-hosted Trailbeam's data for the shared Overview (OverviewFlows, @trailbeam/ui/design), from its own API: GET /api/flows
  for the range, GET /api/causes for why messages didn't finish in it, and GET /api/queues/{queue}/now for each queue's
  messages now. Trailbeam makes no AWS calls, so every queue's figures are the spans' alone: its QueueLive says no
  AWS read was made ("not_configured"), and its health is spanHealth's verdict. The URL is the Event explorer's range=<preset> or from=&to=, and flow=. Pure.
*/
import { overviewData, type OverviewData } from "../model/overview.ts";
import type { QueueLive } from "../model/queue-live.ts";
import type { InQueueView } from "../model/in-queue.ts";
import { toCause } from "../model/causes.ts";
import type { OverviewRangeId } from "../model/format.ts";
import { follow, pinned, rangeFromParams, withRange, type TimeRange } from "../model/timerange.ts";
import { QUEUE_PRESETS, type QueueRange } from "../queue/model.ts";
import type { CauseGroup, CausePage } from "../causes/types.ts";
import type { Flow } from "../queue/selfhosted.ts";
import type { Trigger } from "../queue/types.ts";

/** GET /api/queues/{queue}/now: the queue it read, and "In ‹queue› now" with no SQS totals. */
export type QueueNowResponse = InQueueView & { queue: { key: string; name: string; account: string | null; region: string | null; fifo: boolean } };

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
  now: Record<string, InQueueView | null>;
  /** a cause group's page on this server */
  causeHref: (id: string) => string;
};

/** The shared Overview's data on this server: the spans' figures, with health from spans alone. */
export function selfHostedFlowsData(i: SelfHostedFlowsInput): OverviewData<Trigger, InQueueView> {
  const { flows, window: w } = i;
  const groups = (i.causes?.groups ?? []).map((g) => ({ g, f: flowOfGroup(flows, g) })).filter((x): x is { g: CauseGroup; f: Flow } => !!x.f);
  const causes = groups.map(({ g, f }) => ({ row: toCause(g, f.queue, w.fromMs, w.toMs, i.causeHref), lastMs: g.lastMs, retrying: g.retrying }));
  const queueData = Object.fromEntries(flows.map((f) => {
    const view = i.now[f.key] ?? null;
    const stuck = groups.filter((x) => x.f === f).reduce((a, x) => a + (x.g.counts.stuck ?? 0), 0);
    return [f.key, { aws: { triggers: null }, live: spanOnlyLive({ view, fifo: f.name.endsWith(".fifo"), dlq: f.dlq?.name ?? null, stuck }), inQueue: view }];
  }));
  return overviewData<Trigger, InQueueView>("this server", i.range, w.minutes, flows, queueData, w.toMs, {},
    { causes, causesCapped: !!i.causes?.capped, spanHealth: true });
}
