/*
  The Overview's derived parts (QV-392 design §3, §9): pure functions over the flows of GET /api/flows (the hosted
  app's Flow, field for field), so both hosts get the same notes, counts and row order from the same flows.
*/
import type { Flow } from "../queue/selfhosted.ts";

export type { Flow };
export type FlowService = Flow["producers"][number];

/* ── idle queues (§3.5) ────────────────────────────────────────────────── */

/** Nothing happened on the queue in the range: no send, receive, delete or visibility change. */
export const isIdle = (f: Flow) => f.sent === 0 && f.received === 0 && f.deleted === 0 && f.visibilityChanges === 0;

/** The rows with activity, in order, and the idle ones, most recently seen first. */
export function splitIdle(flows: Flow[]): { active: Flow[]; idle: Flow[] } {
  const active: Flow[] = [], idle: Flow[] = [];
  for (const f of flows) (isIdle(f) ? idle : active).push(f);
  idle.sort((a, b) => b.lastSeen - a.lastSeen);
  return { active, idle };
}

/* ── the summary sentence (§3.6) ──────────────────────────────────────── */

export type OverviewSummary = { queues: number; senders: number; receivers: number };

/** Queues with activity, and the distinct service names that sent and received on them (unknown_service once). */
export function summaryOf(flows: Flow[]): OverviewSummary {
  const active = flows.filter((f) => !isIdle(f));
  const names = (side: "producers" | "consumers") => new Set(active.flatMap((f) => f[side].filter((s) => s.n > 0).map((s) => s.name))).size;
  return { queues: active.length, senders: names("producers"), receivers: names("consumers") };
}

const plural = (n: number, one: string, many = `${one}s`) => `${n.toLocaleString("en-US")} ${n === 1 ? one : many}`;

/**
 * "6 queues had activity in the last hour. 4 services sent, 5 received." `during` is the range as a phrase with its
 * preposition ("in the last hour", "between 13:05 and 14:05 UTC"), the same phrase the idle toggle uses.
 */
export function summaryText(s: OverviewSummary, during: string): string {
  if (s.queues === 0) return `No queue had activity ${during}.`;
  const head = `${plural(s.queues, "queue")} had activity ${during}.`;
  if (s.senders === 0 && s.receivers === 0) return head;
  return `${head} ${plural(s.senders, "service")} sent, ${s.receivers.toLocaleString("en-US")} received.`;
}

/** "Show 3 queues with no activity in the last hour" */
export const idleToggleText = (n: number, during: string) => `Show ${plural(n, "queue")} with no activity ${during}`;

/* ── notes (§3.3): recorded facts, not health ─────────────────────────── */

export const NO_RECEIVE = "No receive reported";
export const NO_SEND = "No send reported";
export const NOTE_HINT = {
  [NO_RECEIVE]: "Your services sent to this queue in the range, and none reported a receive. Its consumer may be stopped, or may not run Trailbeam.",
  [NO_SEND]: "Something sent to this queue without Trailbeam: another service, the console, or SQS moving messages to a dead-letter queue.",
} as const;
export type Note = keyof typeof NOTE_HINT;

/** The note that stands in for a missing side of "Senders → Receivers". Idle rows get none (they show "last seen"). */
export function notesOf(f: Flow): { senders: Note | null; receivers: Note | null } {
  if (isIdle(f)) return { senders: null, receivers: null };
  const producers = f.producers.some((p) => p.n > 0), consumers = f.consumers.some((c) => c.n > 0);
  return {
    senders: !producers || (f.sent === 0 && f.received > 0) ? NO_SEND : null,
    receivers: !consumers || (f.received === 0 && f.sent > 0) ? NO_RECEIVE : null,
  };
}

/* ── service lists (§3.2) ─────────────────────────────────────────────── */

/** At most `max` names in the API's order (most first), then "+N"; every name for the hover. */
export function servicesText(list: FlowService[], { max = 2, side }: { max?: number; side: "producers" | "consumers" }) {
  const names = list.filter((s) => s.n > 0).map((s) => (side === "consumers" && s.lambda ? `${s.name} (Lambda)` : s.name));
  return { shown: names.slice(0, max).join(", "), more: Math.max(0, names.length - max), all: names.join(", ") };
}

/* ── deletes for Lambda consumers (§3.4) ──────────────────────────────── */

export const LAMBDA_DELETES_HINT = "Lambda deletes a batch's messages itself after the function returns, and no span records it. The queue page counts Lambda's successes as handled.";

/** "all": every consumer is a Lambda event source mapping, so no span counts the deletes. "mixed": some are. */
export function lambdaDeletes(f: Flow): "all" | "mixed" | "none" {
  const cs = f.consumers.filter((c) => c.n > 0);
  if (!cs.length) return "none";
  const n = cs.filter((c) => c.lambda).length;
  return n === 0 ? "none" : n === cs.length ? "all" : "mixed";
}

/* ── the order freeze (§3.2) ──────────────────────────────────────────── */

/**
 * The rows in the order they had at the last load or range change (`frozen`: their keys, null for a fresh order).
 * A fresh order is the host's sort when it gives one, else the API's (sends plus receives, most first). Rows that
 * have gone are dropped and new ones go at the bottom, so a refresh never reorders rows under the pointer.
 */
export function orderFlows(flows: Flow[], frozen: string[] | null, sort?: (a: Flow, b: Flow) => number): Flow[] {
  const fresh = sort ? [...flows].sort(sort) : flows;
  if (!frozen) return fresh;
  const byKey = new Map(flows.map((f) => [f.key, f]));
  const kept = frozen.map((k) => byKey.get(k)).filter((f): f is Flow => !!f);
  const seen = new Set(frozen);
  return [...kept, ...fresh.filter((f) => !seen.has(f.key))];
}

/* ── the open row (§2) ────────────────────────────────────────────────── */

/**
 * The open row's key from `flow=`: absent opens the first row; empty opens none; a key opens its row; an old
 * hosted link's label (`?flow=orders`) is tried as a label; anything else opens none (not an error).
 */
export function openKey(flows: Flow[], param: string | null | undefined): string | null {
  if (param == null) return flows[0]?.key ?? null;
  if (param === "") return null;
  return (flows.find((f) => f.key === param) ?? flows.find((f) => f.queue === param) ?? flows.find((f) => f.name === param))?.key ?? null;
}

/* ── the filter box (§8) ──────────────────────────────────────────────── */

/** More rows than this and the filter box shows; more than PAGE and the rest wait behind "Show N more". */
export const FILTER_FROM = 12;
export const PAGE = 50;

/** Rows whose queue or any service name contains `q`, ignoring case. */
export function filterFlows(flows: Flow[], q: string): Flow[] {
  const t = q.trim().toLowerCase();
  if (!t) return flows;
  return flows.filter((f) => [f.queue, f.name, ...f.producers.map((p) => p.name), ...f.consumers.map((c) => c.name)].some((s) => s.toLowerCase().includes(t)));
}

/* ── figures ──────────────────────────────────────────────────────────── */

export const fmtNum = (n: number) => n.toLocaleString("en-US");

/** A count over the range as a rate per minute. */
export const perMinute = (count: number, fromMs: number, toMs: number) => (toMs > fromMs ? (count * 60_000) / (toMs - fromMs) : 0);

/** "68.7", "4,120", "0.2": one decimal under 100, whole numbers above. */
export function fmtRate(n: number): string {
  if (n === 0) return "0";
  if (n < 100) return (Math.round(n * 10) / 10).toLocaleString("en-US", { maximumFractionDigits: 1 });
  return fmtNum(Math.round(n));
}

export function fmtBytes(n: number): string {
  if (n < 1024) return `${Math.round(n)} B`;
  if (n < 1024 ** 2) return `${(n / 1024).toFixed(1)} KB`;
  if (n < 1024 ** 3) return `${(n / 1024 ** 2).toFixed(1)} MB`;
  return `${(n / 1024 ** 3).toFixed(2)} GB`;
}

const MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
/** "Nov 21, 09:14" in UTC, for an idle row's "last seen". */
export function fmtSeen(ms: number): string {
  const d = new Date(ms);
  return `${MONTHS[d.getUTCMonth()]} ${d.getUTCDate()}, ${d.toISOString().slice(11, 16)}`;
}

export const isFifo = (f: Flow) => f.name.endsWith(".fifo");

/** "orders: checkout-api sends 68.7 a minute; orders-worker receives 73.1 a minute; dead-letter queue orders-dlq, 3 received." */
export function drawingLabel(f: Flow, fromMs: number, toMs: number): string {
  const rate = (n: number) => fmtRate(perMinute(n, fromMs, toMs));
  const parts = [
    ...f.producers.filter((p) => p.n > 0).map((p) => `${p.name} sends ${rate(p.n)} a minute`),
    ...f.consumers.filter((c) => c.n > 0).map((c) => `${c.name} receives ${rate(c.n)} a minute`),
  ];
  if (!f.producers.some((p) => p.n > 0)) parts.unshift(NO_SEND.toLowerCase());
  if (!f.consumers.some((c) => c.n > 0)) parts.push(NO_RECEIVE.toLowerCase());
  if (f.dlq) parts.push(`dead-letter queue ${f.dlq.name}, ${fmtNum(f.dlq.received)} received`);
  return `${f.queue}: ${parts.join("; ")}.`;
}
