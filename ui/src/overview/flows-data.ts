/*
  Self-hosted Trailbeam's data for the shared Overview (OverviewFlows, @trailbeam/ui/design), from its own API: GET /api/flows
  for the range, GET /api/causes for why messages didn't finish in it, and GET /api/queues/{queue}/now for each queue's
  messages now. Trailbeam makes no AWS calls, so every queue's figures are the spans' alone: its QueueLive says no
  AWS read was made ("not_configured"), and its health is spanHealth's verdict. The URL is range=15m|1h|24h and flow=. Pure.
*/
import { overviewData, type OverviewData } from "../model/overview.ts";
import type { QueueLive } from "../model/queue-live.ts";
import type { InQueueView } from "../model/in-queue.ts";
import { toCause } from "../model/causes.ts";
import { OVERVIEW_RANGES, type OverviewRangeId } from "../model/format.ts";
import type { CauseGroup, CausePage } from "../causes/types.ts";
import type { Flow } from "../queue/selfhosted.ts";
import type { Trigger } from "../queue/types.ts";

/** GET /api/queues/{queue}/now: the queue it read, and "In ‹queue› now" with no SQS totals. */
export type QueueNowResponse = InQueueView & { queue: { key: string; name: string; account: string | null; region: string | null; fifo: boolean } };

export type FlowsUrl = { range: OverviewRangeId; flow: string | undefined };
const DEFAULT_RANGE: OverviewRangeId = "1h";

/** The page's state from its search parameters. A range this page doesn't offer (an older link's 7d) is the default. */
export function readFlowsUrl(p: URLSearchParams): FlowsUrl {
  const r = p.get("range");
  return { range: OVERVIEW_RANGES.find((x) => x.id === r)?.id ?? DEFAULT_RANGE, flow: p.get("flow") || undefined };
}
/** The URL for a state, with the default range left out. */
export function flowsHref(u: FlowsUrl): string {
  const p = new URLSearchParams();
  if (u.range !== DEFAULT_RANGE) p.set("range", u.range);
  if (u.flow) p.set("flow", u.flow);
  return p.size ? `/overview?${p}` : "/overview";
}
/** The queue page's preset for an Overview range (its presets are queue/model.ts QUEUE_PRESETS). */
export const QUEUE_PRESET_OF: Record<OverviewRangeId, "15m" | "1h" | "1d"> = { "15m": "15m", "1h": "1h", "24h": "1d" };

/**
 * The window a range asks for at nowMs, clipped to what the server keeps (keptMs, null when forever or unknown).
 * `clipped`: its start was before the oldest kept span.
 */
export function flowsWindow(range: OverviewRangeId, nowMs: number, keptMs: number | null): { fromMs: number; toMs: number; minutes: number; clipped: boolean } {
  const minutes = OVERVIEW_RANGES.find((r) => r.id === range)!.minutes;
  const asked = nowMs - minutes * 60_000;
  const kept = keptMs === null ? null : nowMs - keptMs;
  const clipped = kept !== null && asked < kept;
  const fromMs = clipped ? kept : asked;
  return { fromMs, toMs: nowMs, minutes: (nowMs - fromMs) / 60_000, clipped };
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
  range: OverviewRangeId;
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
