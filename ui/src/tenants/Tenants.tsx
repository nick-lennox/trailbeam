/*
  The Tenants page's words and the tenant search box, shared by self-hosted Trailbeam and the hosted app: the number
  formats, the footnote, the empty state's text and the settings it shows. The page itself (the table, the range
  control and the page chrome) is TenantsPage.tsx, a client module reached through @trailbeam/ui/design, because it
  draws with the design system's Nav, Card and Tooltip; this module stays importable from a server component.
*/
import { useEffect, useId, useState } from "react";
import type { TenantRow } from "./types.ts";

export const fmtNum = (n: number) => n.toLocaleString("en-US");
/** A p95 in ms, as the table shows it. */
export function fmtWait(ms: number): string {
  if (ms < 1000) return `${Math.round(ms)} ms`;
  if (ms < 60_000) return `${(ms / 1000).toFixed(ms < 10_000 ? 1 : 0)} s`;
  return `${(ms / 60_000).toFixed(1)} min`;
}
/** A share as a whole percentage; "<1%" for a tenant that sent something but rounds to 0. */
export const fmtShare = (r: Pick<TenantRow, "share" | "sent">) => (r.share < 0.01 && r.sent > 0 ? "<1%" : `${Math.round(r.share * 100)}%`);

/** The p95 cell's hover note: how many first receives it is over. */
export const p95Note = (n: number) => (n ? `Over ${fmtNum(n)} first ${n === 1 ? "receive" : "receives"}.` : "No first receive observed.");

export const TENANTS_TITLE = "Tenants";
export const TENANTS_SUBTITLE = "Which of your customers are sending the traffic, and who is waiting longest.";

/** The sentence under the table: what the numbers are over. Hosts add where the tenant field is set. */
export function tenantsFootnote(s: { total_sent: number; untagged_sent: number; /** ignored: always false (QV-247) */ capped?: boolean }, range: string): string {
  const untagged = s.untagged_sent ? `, ${fmtNum(s.untagged_sent)} of them without a tenant` : "";
  return `${range}, from every message in the range. Share is of all ${fmtNum(s.total_sent)} observed sends${untagged}. ${P95_NOTE} `
    + "Dead-lettered counts messages SQS was seen moving to a dead-letter queue.";
}

/** What p95 in queue is over, and how close it is: shared with the server-rendered page. */
export const P95_NOTE = "p95 in queue is from the send to the first receive, over first receives only. "
  + "It compares two clocks: the sender's host and the consumer's, or SQS's SentTimestamp when the send wasn't observed "
  + "(those first receives count too, so a tenant can have more first receives than sends). "
  + "Clock skew shows up in it, and a time below 0 counts as 0. It is estimated, within 1% of the exact p95.";

/** The Tenants page with no tenant in its range (QV-217): what a tenant is, and how to set one. */
export const TENANTS_README = "https://github.com/nick-lennox/trailbeam#tenants";
export const TENANT_ENV = "export TRAILBEAM_TENANT_ATTRIBUTE=tenant        # a message attribute\n"
  + "export TRAILBEAM_TENANT_BODY_PATH=customer.id   # or a path in a JSON body";
export const TENANT_IS = "A tenant is your own customer or account ID, read from each message.";

/** The empty state's text before the code window. `range` is a range label such as "Last 24 hours". */
export function noTenantsText(totalSent: number, range: string): string {
  const when = `in the ${range.replace(/^Last /, "last ")}`;
  const seen = totalSent === 0 ? `No messages were sent ${when}.`
    : `${fmtNum(totalSent)} ${totalSent === 1 ? "message was" : "messages were"} sent ${when}, none with one.`;
  return `${TENANT_IS} ${seen} To split traffic by customer, set one of these in every service that sends or receives:`;
}

export type TenantSearchProps = {
  /** tenant names for the text so far (GET /api/tenants/search?q=) */
  suggest: (text: string) => Promise<string[]>;
  /** where to go for a tenant */
  href: (tenant: string) => string;
  defaultValue?: string;
  placeholder?: string;
  className?: string;
};

/** A search box with suggestions; Enter or a suggestion goes to href(tenant). Works as a plain form without scripts. */
export function TenantSearch({ suggest, href, defaultValue = "", placeholder = "Tenant (customer ID)", className = "" }: TenantSearchProps) {
  const id = useId();
  const [text, setText] = useState(defaultValue);
  const [options, setOptions] = useState<string[]>([]);
  useEffect(() => {
    const t = text.trim();
    if (!t) { setOptions([]); return; }
    let live = true;
    const timer = setTimeout(() => suggest(t).then((x) => live && setOptions(x), () => live && setOptions([])), 150);
    return () => { live = false; clearTimeout(timer); };
  }, [text, suggest]);
  return (
    <form role="search" className={className} onSubmit={(e) => { e.preventDefault(); const t = text.trim(); if (t) location.assign(href(t)); }}>
      <label className="sr-only" htmlFor={`${id}-q`}>Tenant</label>
      <input id={`${id}-q`} name="tenant" list={`${id}-list`} value={text} onChange={(e) => setText(e.target.value)} placeholder={placeholder} autoComplete="off"
        className="w-full rounded-full border border-line bg-surface px-4 py-1.5 font-mono text-caption text-ink placeholder:text-ink-3" />
      <datalist id={`${id}-list`}>{options.map((o) => <option key={o} value={o} />)}</datalist>
    </form>
  );
}
