/*
  Triage on a cause (the hosted app's QV-393 design; open source in QV-593): who has it, and whether it is open,
  resolved or ignored. Pure and safe in the browser: the types, triageOf() (the state a stored row is in now),
  rowFor() (which stored row a cause shows across the merge and split of error texts), the list's filters, and the
  words the row, the cause panel and the message page use. Each backend keeps its own store (the hosted app's
  lib/app/triage.ts, the server's trailbeam.server.triage_store) and calls these.

  A cause is worked out at read time and is never stored, so triage is kept apart from it, keyed by the cause's key,
  and joined when a page is read. Assignee and status are separate: assigning doesn't change the status, and
  changing the status keeps the assignee.

  Regression and "ignore until N more messages" count the cause's messages whose deciding span reached Trailbeam
  after the resolve or ignore (arrivals.ts).

  fixtures/triage-parity.json: cases both engines must agree on (the server's trailbeam.server.triage runs them
  too, and the hosted app's test reads the same file).
*/
import { newSince, seqMs, type Arrival } from "./arrivals.ts";

export type TriageStatus = "open" | "resolved" | "ignored";
export type ResolveMode = "now" | "next_version";

/** One stored triage row. Times are ms on the server's clock; user IDs are the backend's own. */
export type TriageRow = {
  causeKey: string; causeId: string; label: string; queue: string;
  status: TriageStatus;
  resolveMode: ResolveMode | null; resolveVersions: string[];
  /** arrival watermark (seq units, ms × 1000) of the last resolve or ignore, for stage 2's "failures after it" */
  sinceSeq: number | null;
  ignoreUntil: number | null; ignoreMore: number | null; ignoreSeen: number | null;
  regressedAt: number | null; regressedVersion: string | null; regressedCount: number | null;
  assignee: string | null; assignedBy: string | null; assignedAt: number | null;
  /** who last set the status; null with a status_at: Trailbeam (an ignore that ended by itself) */
  statusBy: string | null; statusAt: number | null;
  noteCount: number; heldRule: number;
  createdAt: number; updatedAt: number;
};

/** The ignore lengths the menu offers (design §3.3), in seconds. null: until someone takes it off. */
export const IGNORE_FOR_S = [3600, 86_400, 7 * 86_400] as const;
/** "Ignore until N more messages" (design §3.3) */
export const IGNORE_MORE = [10, 100] as const;
export const NOTE_MAX = 2000;
/* the character count shows from here (design §5) */
export const NOTE_COUNT_FROM = 1800;
export const MAX_NOTES = 500;
export const MAX_TRIAGE_ROWS = 5000;
/* how long after an action its Undo still works on the server; the toast shows it for 8 s */
export const UNDO_MS = 60_000;
export const TOAST_MS = 8000;

/** What a page shows of a cause's triage now. */
export type Triage = {
  status: TriageStatus;
  mode: ResolveMode | null;
  versions: string[];
  /** ignored until (ms); null when ignored with no end, or not ignored */
  until: number | null;
  /** ignored until this many more messages, and how many have come so far; null otherwise */
  more: number | null; seen: number | null;
  /**
    set when the stored row is ignored but its time is up or its count is reached: the read that sees it writes the
    ending once. `by`: who had ignored it.
  */
  ended: { why: "time" | "count"; at: number; more?: number; by?: string | null } | null;
  regressed: boolean;
  /** when it regressed (ms, server clock: the first new failure's arrival), on what version, and how many new failures since the resolve */
  regressedAt: number | null; regressedVersion: string | null; regressedCount: number | null;
  /** the regression this read found, not stored yet: the read writes it once (settle) */
  regression: { at: number; version: string | null; count: number } | null;
  assignee: string | null;
  statusBy: string | null; statusAt: number | null;
  notes: number;
  /** the cause has no unfinished messages now; only its triage row is left */
  gone: boolean;
};

export const OPEN: Triage = { status: "open", mode: null, versions: [], until: null, more: null, seen: null, ended: null, regressed: false,
  regressedAt: null, regressedVersion: null, regressedCount: null, regression: null, assignee: null, statusBy: null, statusAt: null, notes: 0, gone: false };

/** The failures after the row's resolve or ignore (arrivals.ts): seq past since_seq, not a late export, and not on a version the resolve excused. */
export function failuresAfter(row: TriageRow, arrivals: readonly Arrival[] | undefined): Arrival[] {
  if (row.sinceSeq === null || !arrivals?.length) return [];
  const except = row.resolveMode === "next_version" && row.resolveVersions.length ? new Set(row.resolveVersions) : undefined;
  return newSince(arrivals, row.sinceSeq, row.statusAt ?? seqMs(row.sinceSeq), row.status === "ignored" ? undefined : except);
}

/**
  The state a cause is in now (design §8.4): the stored row, with an ignore whose time is up or whose count is reached
  read as open, and a resolved cause with a failure after the resolve read as Open · Regressed. The page never shows a
  stale state, even when nothing has written the change yet. With no row, the cause is open and nobody has it.
  `group` is the cause as read now (its arrivals count the failures since), or null when it has no unfinished messages.
*/
export function triageOf(row: TriageRow | null, group: { id: string; arrivals?: readonly Arrival[] } | null, nowMs: number): Triage {
  if (!row) return { ...OPEN, gone: !group };
  const base: Triage = {
    status: row.status, mode: row.status === "resolved" ? row.resolveMode ?? "now" : null, versions: row.status === "resolved" ? row.resolveVersions : [],
    until: row.status === "ignored" ? row.ignoreUntil : null,
    more: row.status === "ignored" ? row.ignoreMore : null, seen: row.status === "ignored" && row.ignoreMore !== null ? row.ignoreSeen ?? 0 : null,
    ended: null, regressed: row.regressedAt !== null,
    regressedAt: row.regressedAt, regressedVersion: row.regressedVersion, regressedCount: row.regressedCount, regression: null,
    assignee: row.assignee, statusBy: row.statusBy, statusAt: row.statusAt, notes: row.noteCount, gone: !group,
  };
  const fresh = failuresAfter(row, group?.arrivals);
  if (row.status === "ignored") {
    if (row.ignoreUntil !== null && row.ignoreUntil <= nowMs) {
      return { ...base, status: "open", until: null, more: null, seen: null, ended: { why: "time", at: row.ignoreUntil, by: row.statusBy }, statusBy: null, statusAt: row.ignoreUntil };
    }
    if (row.ignoreMore !== null) {
      // messages, not deliveries: one arrival per message. Never fewer than a read already counted, as messages that finish leave the cause
      const seen = Math.max(row.ignoreSeen ?? 0, fresh.length);
      if (seen >= row.ignoreMore) {
        const at = fresh.length >= row.ignoreMore ? seqMs(fresh[row.ignoreMore - 1].seq) : nowMs;
        return { ...base, status: "open", until: null, more: null, seen: null, ended: { why: "count", at, more: row.ignoreMore, by: row.statusBy }, statusBy: null, statusAt: at };
      }
      return { ...base, seen };
    }
    return base;
  }
  if (row.status === "resolved" && row.regressedAt === null && fresh.length) {
    const first = fresh[0];
    const regression = { at: Math.max(seqMs(first.seq), row.statusAt ?? 0), version: first.version, count: fresh.length };
    return { ...base, status: "open", mode: null, versions: [], regressed: true, regressedAt: regression.at, regressedVersion: regression.version, regressedCount: regression.count, regression };
  }
  // regressed: the count of new failures keeps growing until the next status change
  if (row.regressedAt !== null && fresh.length > (row.regressedCount ?? 0)) return { ...base, regressedCount: fresh.length };
  return base;
}

/* ── which row a cause shows ──────────────────────────────────────────── */

/** Whether a group with key g is the cause with key r, across the merge of error texts ("…|exc:<class>:*"). */
export function sameCause(r: string, g: string): boolean {
  if (r === g) return true;
  if (g.endsWith(":*") && g.includes("|exc:") && r.startsWith(g.slice(0, -1))) return true;
  if (r.endsWith(":*") && r.includes("|exc:") && g.startsWith(r.slice(0, -1))) return true;
  return false;
}

/** The key of a merged cause: all of one exception class's error texts, "…|exc:<class>:*". */
export const isMerged = (key: string) => key.endsWith(":*") && key.includes("|exc:");
/* the latest change first, then by key: the same pick on every read and every database */
const newest = (a: TriageRow, b: TriageRow) => b.updatedAt - a.updatedAt || (a.causeKey < b.causeKey ? -1 : a.causeKey > b.causeKey ? 1 : 0);

/**
  The row the cause with this key shows. While more than a few error texts of one (queue, consumer, exception class)
  are unfinished they merge into one cause, "exc:<class>:*", and split back when messages finish:
  - a cause with a row under its own key shows that row, except that a split cause whose class's merged row was
    changed later shows the merged row (resolving the merged cause resolved every text in it);
  - a split cause with no row of its own shows its class's merged row;
  - a merged cause with no row of its own shows the most recently changed row of its texts.
  Where more than one row could be picked: the latest updatedAt first, then the key.
*/
export function rowFor(rows: readonly TriageRow[], key: string): TriageRow | null {
  const own = rows.find((r) => r.causeKey === key) ?? null;
  if (isMerged(key)) return own ?? rows.filter((r) => !isMerged(r.causeKey) && sameCause(r.causeKey, key)).sort(newest)[0] ?? null;
  const merged = rows.filter((r) => isMerged(r.causeKey) && sameCause(r.causeKey, key)).sort(newest)[0];
  if (!merged || !own) return own ?? merged ?? null;
  return merged.updatedAt > own.updatedAt ? merged : own;
}

/** Whether the default list (Open) shows the cause. */
export const isOpen = (t: Pick<Triage, "status">) => t.status === "open";

/* ── the list's filters (design §4.4), kept in the link as ?status= ───── */

export const FILTERS = ["open", "mine", "unassigned", "resolved", "ignored", "all"] as const;
export type Filter = (typeof FILTERS)[number];
export const FILTER_LABEL: Record<Filter, string> = { open: "Open", mine: "Assigned to me", unassigned: "Unassigned", resolved: "Resolved", ignored: "Ignored", all: "All" };

/** ?status= as a filter; anything else is the default, Open. */
export const filterOf = (raw: string | undefined | null): Filter => (FILTERS as readonly string[]).includes(raw ?? "") ? (raw as Filter) : "open";

/**
  Whether a cause is in a filter. "Assigned to me" and "Unassigned" are of the open causes. An assignee who has left
  the workspace counts as nobody (`members`: the workspace's user IDs now).
*/
export function inFilter(t: Triage, f: Filter, me: string, members: Set<string>): boolean {
  const who = t.assignee && members.has(t.assignee) ? t.assignee : null;
  switch (f) {
    case "all": return true;
    case "resolved": return t.status === "resolved";
    case "ignored": return t.status === "ignored";
    case "open": return t.status === "open";
    case "mine": return t.status === "open" && who === me;
    case "unassigned": return t.status === "open" && who === null;
  }
}

/** The link with ?status= set, or taken out for the default. */
export function withStatus(href: string, f: Filter): string {
  const [path, query = ""] = href.split("?");
  const q = new URLSearchParams(query);
  if (f === "open") q.delete("status"); else q.set("status", f);
  const s = q.toString();
  return s ? `${path}?${s}` : path;
}

/** "2 resolved and 1 ignored cause aren't shown." */
export function hiddenText(resolved: number, ignored: number): string {
  const parts = [resolved ? `${resolved} resolved` : "", ignored ? `${ignored} ignored` : ""].filter(Boolean);
  if (!parts.length) return "";
  const n = resolved + ignored;
  return `${parts.join(" and ")} ${n === 1 ? "cause isn't" : "causes aren't"} shown.`;
}

/* ── words (design §3, §4) ────────────────────────────────────────────── */

export type People = { me: string; names: Record<string, string>; members: string[] };

/** "You", "Sam", or "Sam (left)" when they are no longer in the workspace. */
export function personName(id: string | null, p: People): string {
  if (!id) return "Trailbeam";
  if (id === p.me) return "You";
  const name = p.names[id] ?? "Someone";
  return p.members.includes(id) ? name : `${name} (left)`;
}
/* the same, inside a sentence */
const who = (id: string | null, p: People) => (id === p.me ? "you" : personName(id, p));

const p2 = (n: number) => String(n).padStart(2, "0");
/** "18:00" in the viewer's time zone; "Nov 24, 18:00" when it isn't today. */
export function atClock(ms: number, nowMs: number): string {
  const d = new Date(ms), n = new Date(nowMs);
  const hm = `${p2(d.getHours())}:${p2(d.getMinutes())}`;
  return d.toDateString() === n.toDateString() ? hm : `${d.toLocaleDateString("en-US", { month: "short", day: "numeric" })}, ${hm}`;
}
/** "just now", "4 min ago", "2 h ago" under a day, then the date (design §4.3). */
export function whenAgo(ms: number, nowMs: number): string {
  const s = Math.max(0, (nowMs - ms) / 1000);
  if (s < 60) return "just now";
  if (s < 3600) return `${Math.floor(s / 60)} min ago`;
  if (s < 86_400) return `${Math.max(1, Math.round(s / 3600))} h ago`;
  if (s < 2 * 86_400) return "yesterday";
  if (s < 30 * 86_400) return `${Math.floor(s / 86_400)} days ago`;
  return new Date(ms).toLocaleDateString("en-US", { month: "short", day: "numeric", year: "numeric" });
}
/** "in 3 h", "in 20 min" */
function inTime(ms: number, nowMs: number): string {
  const s = Math.max(0, (ms - nowMs) / 1000);
  if (s < 3600) return `in ${Math.max(1, Math.round(s / 60))} min`;
  if (s < 86_400) return `in ${Math.round(s / 3600)} h`;
  return `in ${Math.round(s / 86_400)} days`;
}
export const ignoreText = (s: number) => (s === 3600 ? "1 h" : s === 86_400 ? "24 h" : `${s / 86_400} days`);
export const notesText = (n: number) => (n ? `${n} ${n === 1 ? "note" : "notes"}` : "");

/** The status tag, when there is one: Resolved, Resolved · next version, Ignored until 18:00, Ignored. */
export function statusTag(t: Triage, nowMs: number): { word: string; tone: "ok" | "muted" | "critical" } | null {
  if (t.regressed && t.status === "open") return { word: "Regressed", tone: "critical" };
  if (t.status === "resolved") return { word: t.mode === "next_version" ? "Resolved · next version" : "Resolved", tone: "ok" };
  if (t.status === "ignored") return { word: t.until !== null ? `Ignored until ${atClock(t.until, nowMs)}` : t.more !== null ? `Ignored for ${t.more} more` : "Ignored", tone: "muted" };
  return null;
}

const failures = (n: number) => `${n} new ${n === 1 ? "failure" : "failures"}`;
/**
  The row's regression line (design §3.2, §4.1): "Regressed 14:02: 4 new failures, seen in 1.4.4." With no version on
  the failures: "Regressed 14:02: 4 new failures after Ana resolved it." Empty when it hasn't regressed.
*/
export function regressionText(t: Triage, p: People, nowMs: number): string {
  if (!t.regressed || t.status !== "open" || t.regressedAt === null) return "";
  const n = Math.max(1, t.regressedCount ?? 1);
  return `Regressed ${atClock(t.regressedAt, nowMs)}: ${failures(n)}${t.regressedVersion ? `, seen in ${t.regressedVersion}` : ` after ${who(t.statusBy, p)} resolved it`}.`;
}

/** What the row control says: the assignee, or Assign. */
export function controlWord(t: Triage, p: People): string {
  return t.assignee ? personName(t.assignee, p) : "Assign";
}

/** The row control's full label: "billing-sync timed out on Stripe call: assigned to Sam, resolved. Triage." */
export function controlAria(label: string, t: Triage, p: People, nowMs: number): string {
  const parts = [t.assignee ? `assigned to ${who(t.assignee, p)}` : "nobody assigned"];
  const tag = statusTag(t, nowMs);
  if (tag) parts.push(tag.word.toLowerCase());
  if (t.notes) parts.push(notesText(t.notes));
  return `${label}: ${parts.join(", ")}. Triage.`;
}

/**
  The cause panel's status line (design §4.3): the status, the assignee, then the last thing that changed it.
  "Open · Nobody assigned." "Resolved by Ana 2 h ago · Assigned to Sam." "Ignored by Ana until 18:00 (in 3 h)."
*/
export function statusLine(t: Triage, p: People, nowMs: number): string {
  const by = who(t.statusBy, p);
  const assigned = t.assignee ? `Assigned to ${who(t.assignee, p)}` : "Nobody assigned";
  if (t.status === "resolved") {
    const when = t.statusAt !== null ? ` ${whenAgo(t.statusAt, nowMs)}` : "";
    const how = t.mode === "next_version" && t.versions.length ? `, in a new version: failures on ${t.versions.join(", ")} won't count against it` : "";
    return `Resolved by ${by}${when}${how} · ${assigned}.`;
  }
  if (t.status === "ignored") {
    const how = t.until !== null ? `until ${atClock(t.until, nowMs)} (${inTime(t.until, nowMs)})`
      : t.more !== null ? `until ${t.more} more messages: ${t.seen ?? 0} so far`
      : `${t.statusAt !== null ? `${whenAgo(t.statusAt, nowMs)}, ` : ""}until someone takes it off`;
    return `Ignored by ${by} ${how} · ${assigned}.`;
  }
  if (t.ended) return `Open · ${assigned}. Its ignore ended ${whenAgo(t.ended.at, nowMs)}${t.ended.why === "count" ? `, after ${t.ended.more} more messages` : ""}.`;
  if (t.regressed && t.regressedAt !== null) {
    const resolved = t.statusBy !== null || t.statusAt !== null ? `Resolved by ${by}${t.statusAt !== null ? ` ${whenAgo(t.statusAt, nowMs)}` : ""}, regressed` : "Regressed";
    return `Open · ${assigned} · ${resolved} ${atClock(t.regressedAt, nowMs)}${t.regressedVersion ? ` in ${t.regressedVersion}` : ""}.`;
  }
  return `Open · ${assigned}.`;
}

/**
  The message page's cause line additions (design §4.5): "Resolved by Ana · Sam is on it · 3 notes". Open is left
  out, so an open, unassigned cause with no notes adds nothing.
*/
export function messageLine(t: Triage, p: People, nowMs: number): string {
  const parts: string[] = [];
  if (t.regressed && t.status === "open") parts.push("Regressed");
  if (t.status === "resolved") parts.push(`Resolved by ${t.statusBy === p.me ? "you" : personName(t.statusBy, p)}`);
  if (t.status === "ignored") parts.push(t.until !== null ? `Ignored until ${atClock(t.until, nowMs)}` : t.more !== null ? `Ignored for ${t.more} more` : "Ignored");
  if (t.assignee) parts.push(t.assignee === p.me ? "You're on it" : `${personName(t.assignee, p)} is on it`);
  if (t.notes) parts.push(notesText(t.notes));
  return parts.join(" · ");
}

/* ── the thread (design §4.3, §5) ─────────────────────────────────────── */

export type ThreadKind = "note" | "assigned" | "unassigned" | "resolved" | "reopened" | "ignored" | "unignored" | "ignore_ended" | "regressed";
/** One line of a cause's thread, as the browser gets it. */
export type ThreadItem = {
  id: string; kind: ThreadKind; actor: string | null; createdAt: number;
  /** a note's text; null for activity and for a deleted note */
  body: string | null; messageId: string | null;
  data: Record<string, unknown>;
  editedAt: number | null; deletedAt: number | null; deletedBy: string | null;
};

/** An activity line, in the third person: "Sam assigned this to themself." */
export function activityText(e: ThreadItem, p: People): string {
  const actor = personName(e.actor, p);
  const d = e.data;
  const target = typeof d.assignee === "string" ? d.assignee : null;
  switch (e.kind) {
    case "assigned": return target === e.actor ? `${actor} assigned this to ${e.actor === p.me ? "yourself" : "themself"}.` : `${actor} assigned this to ${who(target, p)}.`;
    case "unassigned": return `${actor} unassigned ${target ? who(target, p) : "it"}.`;
    case "resolved": {
      const vs = Array.isArray(d.versions) ? (d.versions as string[]) : [];
      return d.mode === "next_version" ? `${actor} resolved this, in a new version${vs.length ? ` (not ${vs.join(", ")})` : ""}.` : `${actor} resolved this.`;
    }
    case "reopened": return `${actor} reopened this.`;
    case "ignored": {
      const forS = typeof d.forS === "number" ? d.forS : null;
      if (typeof d.more === "number") return `${actor} ignored this until ${d.more} more messages.`;
      return forS ? `${actor} ignored this for ${ignoreText(forS)}.` : `${actor} ignored this until someone takes it off.`;
    }
    case "unignored": return `${actor} stopped ignoring this.`;
    case "ignore_ended": {
      const by = typeof d.by === "string" ? who(d.by, p) : null;
      if (d.why === "count" && typeof d.more === "number") return `Back: ${d.more} more messages${by ? ` since ${by} ignored it` : ""}.`;
      return "Back: the ignore's time is up.";
    }
    case "regressed": {
      const n = typeof d.count === "number" ? d.count : 1;
      const by = typeof d.by === "string" ? ` after ${who(d.by, p)} resolved it` : " after it was resolved";
      return `Regressed: ${failures(n)}${by}${typeof d.version === "string" ? `, seen in ${d.version}` : ""}.`;
    }
    case "note": return e.deletedAt !== null ? `Note deleted by ${who(e.deletedBy, p)}.` : "";
  }
}

/** A note's text in parts: plain text, links, and full message IDs (which link to the message page). */
export type NotePart = { text: string } | { url: string } | { mid: string };
const URL_OR_ID = /(https?:\/\/[^\s<>"']+[^\s<>"'.,;:!?)\]])|(\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b)/gi;
export function noteParts(body: string): NotePart[] {
  const out: NotePart[] = [];
  let last = 0;
  for (const m of body.matchAll(URL_OR_ID)) {
    if (m.index > last) out.push({ text: body.slice(last, m.index) });
    out.push(m[1] ? { url: m[1] } : { mid: m[2] });
    last = m.index + m[0].length;
  }
  if (last < body.length) out.push({ text: body.slice(last) });
  return out;
}

/*
  QV-557: characters a note can't hold. C0 and C1 control characters other than tab and newline (NUL, which
  Postgres refuses outright; BEL; ESC, as in terminal colour codes) and the bidi embeddings, overrides and
  isolates, which can make a note read differently from what it says, a file name or URL shown reversed.
  QV-701: and lone surrogates, half of a UTF-16 pair (a truncated emoji), which node-postgres would store as U+FFFD
  and the self-hosted server can't store at all. With the u flag, the class matches only an unpaired surrogate.
*/
const NOTE_BAD = /[\u0000-\u0008\u000b-\u001f\u007f-\u009f\u202a-\u202e\u2066-\u2069\ud800-\udfff]/u;
/** What is left of a note that shows nothing: spaces, format characters such as zero-width spaces, and blank fillers. */
const NOTE_INVISIBLE = /[\s\p{Cf}\p{Zs}\u034f\u115f\u1160\u17b4\u17b5\u180e\u2800\u3164\ufe00-\ufe0f\uffa0]/gu;

/** True when a note would show nothing: empty, or only spaces and invisible characters. */
export const noteBlank = (body: string) => !body.replace(NOTE_INVISIBLE, "");

/** What is wrong with a note's text, or null. */
export function noteProblem(body: string): string | null {
  if (noteBlank(body)) return "Write the note first.";
  if (NOTE_BAD.test(body)) return "The note has hidden characters that can't be saved (control characters or text-direction marks), often from pasted text. Retype the pasted part and try again.";
  if (body.length > NOTE_MAX) return "Notes can be up to 2,000 characters.";
  return null;
}

/** "9f3c2a71…a6c9" */
export const shortId = (id: string) => (id.length > 14 ? `${id.slice(0, 8)}…${id.slice(-4)}` : id);
