/*
  What the Issues pages work out before they draw, moved from the hosted app's pages (QV-583) so both products
  write the same: a list row from an issue, the queue kept in a link, the lines Copy summary copies, an issue's
  figures, and what the example message says about the rest. Pure.
*/
import { utc, type Issue, type Lifespan } from "../model/issues.ts";
import type { IssueStatus } from "./parts.tsx";

/* One row of the Issues list: an issue's figures, its triage status and when each of its messages was last seen. */
export type Row = Omit<Issue, "messages" | "breakdowns" | "shape" | "evidence" | "lifespans"> & {
  /* the issue's triage status (open when the host has no triage); regressed is a resolved or ignored issue that failed again */
  status: IssueStatus;
  regressed: boolean;
  /* each of its messages: when it was last seen, and how it stands now */
  points: { at: number; state: Lifespan["state"] }[];
  tenantNames: string[];
};

/* the list needs each issue's figures and when each of its messages was last seen, not the messages */
export function issueRow({ messages: _m, breakdowns, shape: _s, evidence: _e, lifespans, ...row }: Issue, t: { status: IssueStatus; regressed: boolean } = OPEN_ROW): Row {
  return {
    ...row, status: t.status, regressed: t.regressed,
    points: lifespans.map((l) => ({ at: l.seen, state: l.state })),
    tenantNames: breakdowns.filter((b) => b.label === "Tenant").flatMap((b) => b.rows.map((r) => r.value)),
  };
}
const OPEN_ROW = { status: "open" as const, regressed: false };

/* ?queue= on a link: set, or taken off when `q` is null. Any #fragment stays at the end. */
export function withQueue(href: string, q: string | null): string {
  const hash = href.indexOf("#");
  const frag = hash < 0 ? "" : href.slice(hash);
  const [path, query = ""] = (hash < 0 ? href : href.slice(0, hash)).split("?");
  const p = new URLSearchParams(query);
  if (q) p.set("queue", q); else p.delete("queue");
  const out = p.toString();
  return `${out ? `${path}?${out}` : path}${frag}`;
}

/* A message ID short enough for a table: the first 8 and last 4 characters. */
export const shortId = (id: string) => (id.length > 14 ? `${id.slice(0, 8)}…${id.slice(-4)}` : id);

/* A span of time as a person says it: 13 days 23 h, 96 s. */
export function span(ms: number): string {
  const s = Math.max(0, Math.round(ms / 1000));
  if (s < 120) return `${s} s`;
  if (s < 7200) return `${Math.round(s / 60)} min`;
  if (s < 172800) return `${Math.round(s / 3600)} h`;
  const hours = Math.round(s / 3600), d = Math.floor(hours / 24), h = hours % 24;
  return h ? `${d} days ${h} h` : `${d} days`;
}

/* The lines Copy summary copies, before the page's link: what an incident channel or a ticket needs. */
export function issueSummary(issue: Pick<Issue, "title" | "queue" | "consumers" | "count" | "inDlq" | "notFinished" | "firstSeen" | "lastSeen">): string[] {
  return [
    issue.title,
    `Queue ${issue.queue}${issue.consumers.length ? `, consumer ${issue.consumers.join(", ")}` : ""}`,
    `${issue.count} ${issue.count === 1 ? "message" : "messages"}: ${issue.inDlq} in a dead-letter queue, ${issue.notFinished} not finished`,
    `First seen ${utc(issue.firstSeen)} UTC, last seen ${utc(issue.lastSeen)} UTC`,
  ];
}

/* One figure on an issue's page: its label, the figure (null is "No data"), a note under it, and its colour class. */
export type Figure = { label: string; value: string | null; note?: string; tone?: "critical" | "serious" };

/* The figures someone on call reads first. */
export function issueFigures(issue: Issue, now: number): Figure[] {
  const atMax = issue.maxReceiveCount !== null && issue.maxReceives >= issue.maxReceiveCount;
  const toDlq = issue.lifespans.filter((l) => l.state === "dlq").map((l) => l.to - l.from).sort((a, b) => a - b);
  const expires = issue.retentionDays === null ? null : issue.firstSeen + issue.retentionDays * 86400_000;
  return [
    { label: "Messages", value: String(issue.count), note: `over the ${issue.trendLabel}` },
    { label: "In a dead-letter queue", value: String(issue.inDlq), note: issue.dlq ?? undefined, tone: issue.inDlq ? "critical" : undefined },
    { label: "Not finished", value: String(issue.notFinished), note: "no accepted delete yet", tone: issue.notFinished ? "serious" : undefined },
    {
      label: "Highest receive count",
      value: issue.maxReceiveCount !== null ? `${issue.maxReceives} of ${issue.maxReceiveCount}` : String(issue.maxReceives),
      note: issue.maxReceiveCount !== null ? "of maxReceiveCount" : "maxReceiveCount not known",
      tone: atMax ? "critical" : undefined,
    },
    { label: "Time to the dead-letter queue", value: toDlq.length ? span(toDlq[Math.floor(toDlq.length / 2)]) : null, note: toDlq.length ? "median" : "none dead-lettered" },
    {
      label: "Retention deletes the oldest in",
      value: expires === null ? null : span(expires - now),
      note: issue.retentionDays === null ? "retention period not known" : `retention ${issue.retentionDays} ${issue.retentionDays === 1 ? "day" : "days"}`,
      tone: expires !== null && expires - now < 86400_000 ? "critical" : undefined,
    },
  ];
}

/* What the example message says about the issue's other messages. `drawn` is whether the page shows its
   timeline; without it, the steps are listed on their own. A count of 0 means the steps were not compared, so the
   example is the newest message. */
export function sharedBy(issue: Pick<Issue, "shape" | "count">, drawn: boolean): string {
  const s = issue.shape;
  if (!s) return "";
  const text = s.count === 0 ? "The newest message with this cause."
    : s.count === issue.count
      ? issue.count === 1 ? "The only message in this issue." : `All ${issue.count} messages took the same steps as this one.`
      : `${s.count} of ${issue.count} messages took the same steps as this one. The rest differ, most often in how many times they were received.`;
  return drawn ? text : text.replace(" as this one", "").replace("took the same steps", "took these steps, in this order");
}
