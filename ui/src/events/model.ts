/*
  What the Event explorer shows of a step, and how it reads GET /api/events: the asks for a window and a query, the
  chart's bars, the words for each step, and the CSV export. No React here, so the tests run it directly.
*/
import { KIND_LABELS, type ParsedQuery } from "./query.ts";
import { STEP_KINDS, type EventHistogram, type EventStep, type StepKind } from "./types.ts";

/** The most steps one ask returns (the API's limit maximum), and the most an export collects. */
export const PAGE_LIMIT = 100;
export const EXPORT_PAGE = 1000;
export const EXPORT_MAX = 10_000;

/** A step's own key in the order: its time, trace and span (unique in a store). */
export const stepKey = (s: EventStep) => `${s.time_unix_nano}:${s.trace_id}:${s.span_id}`;

/** The step's end time in ms since the epoch. */
export const stepMs = (s: EventStep) => Number(BigInt(s.time_unix_nano) / 1_000_000n);

const OUTCOMES: Record<string, string> = {
  success: "Succeeded",
  failed_exception: "Raised an exception",
  failed_reported: "Reported as a batch item failure",
  failed_invalid_response: "Returned an invalid batch response",
};

/** Whether a step says something failed: a call SQS refused, or a Lambda outcome that wasn't a success. */
export const stepFailed = (s: EventStep) => s.result === "failed" || (s.kind === "outcome" && !!s.outcome && s.outcome !== "success");

/** What a step's Result column says: how a call or invocation went, in words. Empty when the span says nothing. */
export function stepResult(s: EventStep): string {
  if (s.kind === "outcome") {
    const what = s.outcome ? OUTCOMES[s.outcome] ?? s.outcome : "";
    return s.error_type ? `${what}: ${s.error_type}` : what;
  }
  if (s.result === "failed") return s.error_code ? `Failed: ${s.error_code}` : "Failed";
  if (s.kind === "change_visibility" && s.visibility_timeout !== null) return `Timeout set to ${s.visibility_timeout} s`;
  if (s.result === "ok") return "Accepted";
  return "";
}

export const kindLabel = (k: StepKind) => KIND_LABELS[k];

/** "2026-11-16 18:05:37.716": a step's time in UTC, to the millisecond. */
export const fmtStepTime = (s: EventStep) => s.time.replace("T", " ").replace(/(\.\d{3})\d*Z$/, "$1").replace(/Z$/, "");

/** The service, with its version when the span carried one: "orders-worker 1.4.2". */
export const serviceText = (s: EventStep) => (s.service_name ? (s.service_version ? `${s.service_name} ${s.service_version}` : s.service_name) : "");

/** The window an ask covers, in ms, both ends included (the API's from_ms/to_ms). */
export type Window = { from_ms: number; to_ms: number };

/** The query string for one ask: the window, the query's filters, the page size and the cursor. */
export function eventsQuery(w: Window, parsed: ParsedQuery, limit = PAGE_LIMIT, cursor?: string | null): Record<string, string> {
  const q: Record<string, string> = { from_ms: String(Math.floor(w.from_ms)), to_ms: String(Math.floor(w.to_ms)) };
  for (const [k, v] of Object.entries(parsed.params)) if (v) q[k] = v;
  q.limit = String(limit);
  if (cursor) q.cursor = cursor;
  return q;
}

export type Bar = { start: number; end: number; total: number; counts: Record<StepKind, number> };

/** The chart's bars: one per bucket, each the sum of the kinds the query keeps (the API already left the others out). */
export function barsOf(h: EventHistogram): Bar[] {
  return h.buckets.map((b) => {
    const counts = Object.fromEntries(STEP_KINDS.map((k) => [k, b[k] ?? 0])) as Record<StepKind, number>;
    return { start: b.start_ms, end: b.start_ms + h.bucket_ms, total: STEP_KINDS.reduce((a, k) => a + counts[k], 0), counts };
  });
}

const PER: [number, string][] = [[60_000, "minute"], [300_000, "5 minutes"], [900_000, "15 minutes"], [3_600_000, "hour"], [10_800_000, "3 hours"], [21_600_000, "6 hours"], [86_400_000, "day"]];
/** "minute", "hour", "3 days": the width of a bar, for "Steps per …". */
export function perLabel(ms: number): string {
  const named = PER.find(([w]) => w === ms);
  if (named) return named[1];
  return ms % 86_400_000 === 0 ? `${ms / 86_400_000} days` : `${Math.round(ms / 60_000)} minutes`;
}

/** A picked span of bars as an ask's window: from the first bar's start to just before the last one's end, inside the range. */
export function pickedWindow(range: Window, a: Bar, b: Bar): Window {
  const lo = Math.min(a.start, b.start);
  const hi = Math.max(a.end, b.end);
  return { from_ms: Math.max(range.from_ms, lo), to_ms: Math.min(range.to_ms, hi - 1) };
}

/** The export's columns, in order. */
export const EXPORT_HEADERS = ["Time (UTC)", "Step", "Queue", "Service", "Service version", "Message ID", "Receive count", "Result", "Trace ID", "Span ID"];

/** One step as an export row. Values are written in full. */
export const exportRow = (s: EventStep): string[] => [
  fmtStepTime(s), kindLabel(s.kind), s.queue, s.service_name ?? "", s.service_version ?? "", s.message_id ?? "",
  s.receive_count === null ? "" : String(s.receive_count), stepResult(s), s.trace_id, s.span_id,
];

/** RFC 4180 CSV, with a cell that a spreadsheet would read as a formula made plain text. */
export function toCsv(headers: string[], rows: string[][]): string {
  const cell = (v: string) => {
    const safe = /^[=+\-@\t\r]/.test(v) ? `'${v}` : v;
    return /[",\r\n]/.test(safe) ? `"${safe.replace(/"/g, '""')}"` : safe;
  };
  return [headers, ...rows].map((r) => r.map(cell).join(",")).join("\r\n") + "\r\n";
}

/** Every step a query matches in a window, page after page, up to `max`. `more`: there were more than were collected. */
export async function collectSteps(load: (q: Record<string, string>) => Promise<{ events: EventStep[]; next_cursor: string | null }>,
  w: Window, parsed: ParsedQuery, max = EXPORT_MAX): Promise<{ steps: EventStep[]; more: boolean }> {
  const steps: EventStep[] = [];
  let cursor: string | null = null;
  do {
    const page: { events: EventStep[]; next_cursor: string | null } = await load(eventsQuery(w, parsed, Math.min(EXPORT_PAGE, max - steps.length), cursor));
    steps.push(...page.events);
    cursor = page.next_cursor;
  } while (cursor && steps.length < max);
  return { steps, more: !!cursor };
}
