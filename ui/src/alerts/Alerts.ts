/*
  The alert views' words and rules (QV-592), shared by self-hosted Trailbeam and the hosted app: what a rule's control
  says, the quiet periods in words, how the rules list is ordered, what the page says about the last check, and how
  self-hosted Trailbeam's /api/alerts answers become the views' data (types.ts). The parts that draw them are
  AlertViews.tsx's. Pure: no React, so the main entry and its tests can use it.
  The words follow the app's copy rules: no "a · b" fragments, no arrows, no ellipses, and SQS's own terms.
*/
import type { ChannelKind, ChannelRow, DashboardRuleView, RuleView, RulesView } from "./types.ts";

/** Muted with no end (the hosted app's rules.ts FOREVER, the server's alerts.FOREVER). */
export const FOREVER = 8_640_000_000_000_000;
export const SNOOZES_S = [3600, 4 * 3600, 24 * 3600] as const;
/** The control's word for each kind of channel. */
export const KIND: Record<ChannelKind, string> = { webhook: "Webhook", slack: "Slack", email: "Email" };

/** A quiet period in words: "30 min", "6 h", "24 h", "7 days". */
export const quietText = (s: number) =>
  s % 86_400 === 0 ? (s === 86_400 ? "24 h" : `${s / 86_400} days`) : s % 3600 === 0 ? `${s / 3600} h` : `${s / 60} min`;

/** How often checks run, in words: "once a day, at 13:00 UTC", "every 5 minutes". */
export function checkEvery(min: number) {
  if (min === 1440) return "once a day, at 13:00 UTC";
  if (min === 1) return "every minute";
  return min % 60 === 0 ? `every ${min / 60 === 1 ? "hour" : `${min / 60} hours`}` : `every ${min} minutes`;
}

/** A time of day in the viewer's clock. The server renders UTC, so the two may differ until the page hydrates. */
export const hm = (ms: number) => new Date(ms).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });

/** "Held while this cause is ignored (by Ana, until 18:00). It sends again when the cause stops being ignored." */
export function heldText(h: NonNullable<RuleView["held"]>) {
  const how = [h.by ? `by ${h.by}` : "", h.until !== null ? `until ${hm(h.until)}` : h.more !== null ? `until ${h.more} more messages` : ""].filter(Boolean).join(", ");
  return `Held while this cause is ignored${how ? ` (${how})` : ""}. It sends again when the cause stops being ignored.`;
}
/** The rules list's word for a held rule: "Held until 18:00", or "Held while ignored". */
export const heldWord = (h: NonNullable<RuleView["held"]>) => (h.until !== null ? `Held until ${hm(h.until)}` : "Held while ignored");

export type RuleState = { word: string; icon: "bell" | "ring" | "off"; tone: string; firing: boolean; aria: string };

/** What a rule's control says, as words and an icon, and its full aria-label. Never colour alone. */
export function ruleState(r: RuleView | null, channels: { id: string; name: string }[]): RuleState {
  if (!r) return { word: "Alert me", icon: "bell", tone: "text-ink-3 hover:text-ink", firing: false, aria: "Alert me about this cause" };
  const firing = r.firingSince !== null;
  const names = r.channels.map((id) => channels.find((c) => c.id === id)?.name).filter((n): n is string => !!n);
  const to = names.length ? `${names[0]}${names.length > 1 ? ` and ${names.length - 1} more` : ""}` : "no channel";
  if (r.mutedUntil !== null) {
    const word = firing ? "Muted, firing" : r.forever ? "Muted" : `Snoozed until ${hm(r.mutedUntil)}`;
    return { word, icon: "off", tone: "text-ink-3", firing, aria: `Alert rule for this cause: ${word.toLowerCase()}${r.mutedBy ? `, by ${r.mutedBy}` : ""}. Edit.` };
  }
  if (r.held) return { word: "Held while ignored", icon: "off", tone: "text-ink-3", firing, aria: `Alert rule for this cause: ${heldText(r.held).replace(/^H/, "h")} Edit.` };
  if (firing) return { word: `Firing since ${hm(r.firingSince!)}`, icon: "ring", tone: "text-critical-text", firing, aria: `Alert rule for this cause: firing since ${hm(r.firingSince!)}, sent to ${names.join(", ") || "no channel"}. Edit.` };
  return { word: `Alerting ${to}`, icon: "ring", tone: "text-ink-2", firing, aria: `Alert rule for this cause: on, sending to ${names.join(", ") || "no channel"}. Edit.` };
}

/** The rules list's order: firing first, then on, then snoozed or held (nothing is sent for now), then muted. */
export const ruleRank = (r: RuleView) => (r.firingSince !== null ? 0 : r.mutedUntil === null ? (r.held ? 2 : 1) : r.forever ? 3 : 2);
export const sortRules = (rules: RuleView[]) => [...rules].sort((a, b) => ruleRank(a) - ruleRank(b));

/** When a rule sends, in words: "Every check with new messages", "New messages arrive after none for 6 h". */
export const whenText = (r: Pick<RuleView, "mode" | "quietS">, quiet: RulesView["quiet"]) =>
  r.mode === "every" ? "Every check with new messages" : `New messages arrive after none for ${quiet.find((q) => q.s === r.quietS)?.text ?? quietText(r.quietS)}`;

/** A channel's Alert levels setting in words. */
export const toneText = (t: ChannelRow["minTone"]) => (t === "critical" ? "critical only" : "warning and critical");

/** A dashboard rule's tone at the last check, as a word. */
export const dashboardToneText = (t: string) => (t === "good" ? "OK" : t);

/** What a channel's form shows for each kind. */
export const PLACEHOLDER: Record<ChannelKind, string> = {
  webhook: "https://example.com/hooks/trailbeam", slack: "https://hooks.slack.com/services/T000/B000/XXXX", email: "oncall@company.com, lead@company.com",
};
/** The Name field's example for each kind. */
export const NAME_PLACEHOLDER: Record<ChannelKind, string> = { slack: "#oncall", webhook: "Incident bot", email: "On-call team" };
export const HINT: Record<ChannelKind, string> = {
  webhook: "A JSON POST, signed with a secret shown once you add it.",
  slack: "A Slack incoming webhook. Slack's app settings give you one per channel.",
  email: "Up to 10 addresses, separated by commas.",
};

/** A channel's last delivery time: "27 Nov, 13:05" in the viewer's clock. */
export const deliveredWhen = (ms: number) => new Date(ms).toLocaleString("en-GB", { day: "numeric", month: "short", hour: "2-digit", minute: "2-digit" });

export type CheckStatus = { text: string; late: boolean };

const ago = (ms: number, now: number) => {
  const m = Math.round((now - ms) / 60_000);
  return m < 1 ? "just now" : m < 60 ? `${m} min ago` : m < 1440 ? `${Math.round(m / 60)} h ago` : `${Math.round(m / 1440)} d ago`;
};
const MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
const utc = (ms: number, date: boolean) => {
  const d = new Date(ms);
  const t = `${String(d.getUTCHours()).padStart(2, "0")}:${String(d.getUTCMinutes()).padStart(2, "0")}`;
  return date ? `${d.getUTCDate()} ${MONTHS[d.getUTCMonth()]} ${t}` : t;
};

/**
  What a page says about the last check (the hosted app's schedule.ts checkStatus): when it ran, in UTC, how often
  checks run, and, when the last one is older than two intervals, that checks are late and alerts may not be going
  out. `addChannel`: where the page sends someone to add a channel, as a sentence.
*/
export function checkStatus(o: { checkedAt: number | null; now: number; hasChannel: boolean; everyMin: number; addChannel?: string }): CheckStatus {
  const every = checkEvery(o.everyMin);
  if (!o.hasChannel) return { text: `Not checked: Trailbeam checks rules only when there is an alert channel. ${o.addChannel ?? "Add one in Settings."}`, late: false };
  if (o.checkedAt === null) return { text: `Not checked yet. Checks run ${every}.`, late: false };
  const sameDay = new Date(o.checkedAt).toISOString().slice(0, 10) === new Date(o.now).toISOString().slice(0, 10);
  const at = `Last checked ${utc(o.checkedAt, !sameDay)} UTC, ${ago(o.checkedAt, o.now)}.`;
  const late = o.now - o.checkedAt > 2 * o.everyMin * 60_000;
  return { text: late ? `${at} Checks should run ${every}, so they are late and alerts may not be going out.` : `${at} Checks run ${every}.`, late };
}

/* ── self-hosted Trailbeam's /api/alerts answers (server/src/trailbeam/server/alerts_api.py) ─────────────────── */

/** A rule as GET /api/alerts/rules answers it: the stored rule, whether it is muted now, and its state at the last check. */
export type ApiRule = {
  id: string; causeId: string; label: string; queue: string; channels: string[]; mode: "quiet" | "every"; quietS: number;
  mutedUntil: number | null; mutedBy: string | null; createdBy: string; muted: boolean;
  state: { tone: string; since: number } | null;
};
export type ApiRules = { rules: ApiRule[]; cadence: string; quietChoices: number[]; defaultQuiet: number; everyMin?: number };
export type ApiChannel = {
  id: string; kind: ChannelKind; name: string; shown: string; minTone: "warning" | "critical";
  lastAt: number | null; lastOk: boolean | null; lastDetail: string | null;
};
export type ApiState = {
  alerts: { dashboardId: string; dashboardName?: string; widgetId: string; title?: string; tone: string; value?: string; limit?: string; since: number }[];
  checkedAt: number | null; hasChannel: boolean; everyMin?: number;
};

/**
  The rules view from the server's answers. `name`: a person's name from what the server stores as who made or muted
  something (a person's ID, or how the request was let in), "" when it can't be named.
*/
export function rulesViewOf(r: ApiRules, channels: ApiChannel[], state: ApiState | null, name: (by: string) => string = () => ""): RulesView {
  return {
    rules: r.rules.map((x): RuleView => ({
      id: x.id, causeId: x.causeId, label: x.label, queue: x.queue, channels: x.channels, mode: x.mode, quietS: x.quietS,
      mutedUntil: x.muted ? x.mutedUntil : null, forever: x.muted && x.mutedUntil === FOREVER, mutedBy: x.muted && x.mutedBy ? name(x.mutedBy) || null : null,
      createdBy: name(x.createdBy), firingSince: x.state?.tone === "critical" ? x.state.since : null, checked: !!x.state,
      // the server's rules answer doesn't say whether triage holds a rule yet
      held: null,
    })),
    channels: channels.map((c) => ({ id: c.id, name: c.name, kind: c.kind, minTone: c.minTone })),
    cadence: r.cadence, quiet: r.quietChoices.map((s) => ({ s, text: quietText(s) })), defaultQuiet: r.defaultQuiet,
    dashboards: (state?.alerts ?? []).filter((s) => s.dashboardId !== "rule").map((s): DashboardRuleView => ({
      dashboardId: s.dashboardId, dashboardName: s.dashboardName ?? "", title: s.title ?? s.widgetId, tone: s.tone, value: s.value ?? "", limit: s.limit ?? "", since: s.since,
    })),
  };
}

/** The channels' rows from GET /api/alerts/channels. `secrets`: a webhook's signing secret by channel, as POST answered it once. */
export const channelRowsOf = (channels: ApiChannel[], secrets: Record<string, string> = {}): ChannelRow[] =>
  channels.map((c) => ({ id: c.id, kind: c.kind, name: c.name, shown: c.shown, minTone: c.minTone, secret: secrets[c.id] ?? null, lastAt: c.lastAt, lastOk: c.lastOk, lastDetail: c.lastDetail }));
