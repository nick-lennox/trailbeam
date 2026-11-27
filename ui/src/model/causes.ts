/*
  A cause group as one row of the Overview's and the queue page's cause lists (the hosted app's Cause, which its
  sample rows use too). toCause makes the row from a group, so a group from GET /api/causes and one from the hosted
  app's causes.ts give the same row: fixtures/viewmodel-parity.json. Pure.
*/
import { dlqSplit, tenantsText, trendOf, versionsText, whenFirst, whenLast } from "../causes/text.ts";
import type { CauseGroup, CauseOutcome } from "../causes/types.ts";

export { dlqSplit, tenantsText, trendOf, versionsText, whenFirst, whenLast };
export type { CauseOutcome };

export type Cause = {
  id: string; // an example message, when there is one
  href?: string;
  outcome: CauseOutcome;
  title: string;
  queue: string;
  count: number;
  tenants: string;
  first: string;
  last: string;
  trend: number[];
  /* where in the last hour it began, 0 to 1 (sample rows only) */
  start: number;
  /* on recorded data: what the cause rests on (spans, function logs, CloudTrail or inferred), and "31 in the DLQ, 4 retrying" */
  basis?: string;
  split?: string;
  /* on recorded data: the consumer versions it was seen on, "seen in 1.4.2, 1.4.3" (QV-362) */
  versions?: string;
};

/** What toCause reads of a group: the fields both products' groups have. */
export type CauseRowGroup = Pick<CauseGroup, "id" | "sampleMid" | "outcome" | "label" | "count" | "tenants" | "firstMs" | "lastMs" | "times" | "basis" | "counts" | "dlqInferred" | "versions">;

/** A group as a cause row, for queue label `queue`, with its trend over [fromMs, nowMs]. `href`: the cause's page, by default /issues/<id>. */
export function toCause(g: CauseRowGroup, queue: string, fromMs: number, nowMs: number, href: (id: string) => string = (id) => `/issues/${id}`): Cause {
  return { id: g.sampleMid, href: href(g.id), outcome: g.outcome, title: g.label, queue, count: g.count, tenants: tenantsText(g.tenants),
    first: whenFirst(g.firstMs, nowMs), last: whenLast(g.lastMs, nowMs), trend: trendOf(g.times, fromMs, nowMs), start: 0, basis: g.basis, split: dlqSplit(g),
    versions: versionsText(g.versions) };
}

/** Where a cause row links: its own href, else its example message. */
export const causeHref = (c: Pick<Cause, "href" | "id">) => c.href ?? `/messages/${c.id}`;

export const OUTCOME_LABEL: Record<CauseOutcome, string> = {
  dead_lettered: "Dead-lettered",
  stuck: "Stuck in flight",
  filtered: "Removed by an event filter",
};
export const OUTCOME_FILL: Record<CauseOutcome, string> = {
  dead_lettered: "var(--color-critical)",
  stuck: "var(--color-serious)",
  filtered: "var(--color-warning)",
};
export const OUTCOME_TEXT: Record<CauseOutcome, string> = {
  dead_lettered: "text-critical-text",
  stuck: "text-serious-text",
  filtered: "text-warning-text",
};
