/*
  What the first-run screen says, worked out from GET /api/health (contract/openapi.yaml, Health). Pure, so the
  host polls and this decides. Design: QV-146, "First-run screen and empty states".
*/

/** The parts of /api/health the first-run screen reads. Without a session, health has only status and version. */
export type Health = {
  status?: string;
  version?: string;
  ingest?: { requests?: number; inserted?: number; ignored?: number; rejected?: number };
  data?: { last_row?: number | null };
  config?: { auth?: "token" | "off"; store_bodies?: string[]; retention?: { default?: string; passes?: number; last_pass?: { deleted_spans?: number } | null } | null };
};

/** The newest message, for "First message received". */
export type FirstMessage = { message_id: string; source_queue: string; consumer: string | null };

export type FirstRunStatus =
  | { kind: "waiting"; checkedAgoS: number | null; hidden?: boolean }
  /** two minutes on the page and no OTLP request at all */
  | { kind: "no-spans" }
  /** spans came, none from the Trailbeam package */
  | { kind: "ignored"; ignored: number }
  /** spans came that don't match the span contract */
  | { kind: "rejected"; rejected: number }
  /** /api/health failed or timed out */
  | { kind: "unreachable" }
  | { kind: "received"; first: FirstMessage };

/** How long the screen waits before it says nothing has arrived: a person pasting three blocks needs about a minute. */
export const NO_SPANS_AFTER_MS = 120_000;

/** Whether the store holds no span at all, so `/` shows the first-run screen. Unknown (no session, store down) is not empty. */
export const storeIsEmpty = (h: Health | null | undefined) => h?.data?.last_row === 0;

/** Whether spans have been stored: the screen fetches the newest message and goes live. */
export const storeHasSpans = (h: Health | null | undefined) => typeof h?.data?.last_row === "number" && h.data.last_row > 0;

/**
 * The waiting line's status for a health answer, while the store is still empty. The reasons come from the ingest
 * counters, which count since the server started, so they say only what the server saw.
 */
export function waitingStatus(h: Health, waitedMs: number, checkedAgoS: number | null = 0): FirstRunStatus {
  const i = h.ingest ?? {};
  const inserted = i.inserted ?? 0;
  if ((i.rejected ?? 0) > 0 && inserted === 0) return { kind: "rejected", rejected: i.rejected! };
  if ((i.ignored ?? 0) > 0 && inserted === 0) return { kind: "ignored", ignored: i.ignored! };
  if (waitedMs >= NO_SPANS_AFTER_MS && (i.requests ?? 0) === 0) return { kind: "no-spans" };
  return { kind: "waiting", checkedAgoS };
}

const UNITS: Record<string, [string, string]> = { s: ["second", "seconds"], m: ["minute", "minutes"], h: ["hour", "hours"], d: ["day", "days"] };

/**
 * When retention has emptied the store, how long it keeps spans in words ("7 days"); null when it hasn't, or spans
 * are kept forever. Health gives the period as "7d", "12h", "30m" or "forever".
 *
 * A pass count alone can't tell: the server runs a pass when it starts, so a fresh install has passes 1 and nothing
 * deleted. Emptied means the store is empty and the last pass deleted spans, or spans were stored since the server
 * started (and are gone). After a restart, or once a later pass finds nothing, that's unknown and the screen shows
 * the full setup, which is still right, only more than that person needs.
 */
export function retainedFor(h: Health): string | null {
  const r = h.config?.retention;
  if (!r || !storeIsEmpty(h)) return null;
  if (!((r.last_pass?.deleted_spans ?? 0) > 0 || (h.ingest?.inserted ?? 0) > 0)) return null;
  const m = /^(\d+)([smhd])$/.exec(r.default ?? "");
  if (!m) return null;
  const n = Number(m[1]);
  return `${n} ${UNITS[m[2]][n === 1 ? 0 : 1]}`;
}

/** Whether the endpoint is the package's default, so a service on this machine needs no endpoint setting. */
export const isDefaultEndpoint = (endpoint: string) => /^http:\/\/(localhost|127\.0\.0\.1):4318\/v1\/traces$/.test(endpoint);

/** What the live region says: once per change of state, never on the poll counter. */
export function announcement(s: FirstRunStatus): string {
  switch (s.kind) {
    case "waiting": return s.hidden ? "Waiting for your first message. Paused while this tab is hidden." : "Waiting for your first message.";
    case "no-spans": return "Waiting for your first message. No spans have arrived in 2 minutes.";
    case "ignored": return `Waiting for your first message. Trailbeam received ${s.ignored} spans, but none came from the Trailbeam package.`;
    case "rejected": return `Waiting for your first message. Trailbeam rejected ${s.rejected} spans that don't match its span contract.`;
    case "unreachable": return "Can't reach Trailbeam's API. Trying again every 10 s.";
    case "received": return `First message received: ${s.first.message_id}. Open its timeline.`;
  }
}
