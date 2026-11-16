/*
  The Tenants table and the tenant search box, shared by self-hosted Trailbeam and the hosted app. They take
  the API's data as props (types.ts) and know nothing about where they run: links come from href(), and the
  page around them (range picker, footnote wording, where "set the tenant field" points) is the host's.
  They are colored with role tokens (theme.css), not the sky, so they take the host page's theme.
*/
import { useEffect, useId, useState, type ComponentType, type ReactNode } from "react";
import type { TenantRow } from "./types.ts";

type LinkProps = { href: string; className?: string; children: ReactNode };
const A = ({ href, className, children }: LinkProps) => <a href={href} className={className}>{children}</a>;

const fmtNum = (n: number) => n.toLocaleString("en-US");
/** A p95 in ms, as the table shows it. */
export function fmtWait(ms: number): string {
  if (ms < 1000) return `${Math.round(ms)} ms`;
  if (ms < 60_000) return `${(ms / 1000).toFixed(ms < 10_000 ? 1 : 0)} s`;
  return `${(ms / 60_000).toFixed(1)} min`;
}
/** A share as a whole percentage; "<1%" for a tenant that sent something but rounds to 0. */
export const fmtShare = (r: Pick<TenantRow, "share" | "sent">) => (r.share < 0.01 && r.sent > 0 ? "<1%" : `${Math.round(r.share * 100)}%`);

export type TenantsTableProps = {
  rows: TenantRow[];
  /** where a tenant's name links to: its messages */
  href: (tenant: string) => string;
  /** a link component, e.g. next/link; plain <a> by default */
  Link?: ComponentType<LinkProps>;
  /** a p95 above this is highlighted (ms, default 1000) */
  slowMs?: number;
};

/** One row per tenant, in the API's order. */
export function TenantsTable({ rows, href, Link = A, slowMs = 1000 }: TenantsTableProps) {
  const max = Math.max(1, ...rows.map((t) => t.sent));
  return (
    <table className="w-full text-[15px]">
      <thead>
        <tr className="border-b border-line text-left font-mono text-caption text-ink-3">
          <th className="px-5 py-2 font-medium">Tenant</th>
          <th className="px-3 py-2 font-medium">Messages sent</th>
          <th className="px-3 py-2 text-right font-medium">Share</th>
          <th className="px-3 py-2 text-right font-medium">p95 in queue</th>
          <th className="px-5 py-2 text-right font-medium">Dead-lettered</th>
        </tr>
      </thead>
      <tbody>
        {rows.map((t) => (
          <tr key={t.tenant} className="border-b border-line/60 last:border-0">
            <td className="px-5 py-3 text-ink"><Link href={href(t.tenant)} className="hover:text-brand">{t.tenant}</Link></td>
            <td className="px-3 py-3">
              <div className="flex items-center gap-3">
                <div className="h-1.5 w-40 rounded-full bg-line/70 md:w-56" aria-hidden>
                  <div className="h-full rounded-full bg-series-1" style={{ width: `${(t.sent / max) * 100}%` }} />
                </div>
                <span className="font-mono text-caption tabular-nums text-ink-2">{fmtNum(t.sent)}</span>
              </div>
            </td>
            <td className="px-3 py-3 text-right font-mono text-caption tabular-nums text-ink-2">{fmtShare(t)}</td>
            <td className={`px-3 py-3 text-right font-mono text-caption tabular-nums ${t.p95_in_queue_ms !== null && t.p95_in_queue_ms > slowMs ? "text-warning-text" : "text-ink-2"}`}
              title={t.first_deliveries ? `over ${fmtNum(t.first_deliveries)} first ${t.first_deliveries === 1 ? "delivery" : "deliveries"}` : "No first delivery observed"}>
              {t.p95_in_queue_ms === null ? <span className="text-ink-3">-</span> : fmtWait(t.p95_in_queue_ms)}
            </td>
            <td className={`px-5 py-3 text-right font-mono text-caption tabular-nums ${t.dead_lettered ? "text-critical-text" : "text-ink-3"}`}>{fmtNum(t.dead_lettered)}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

/** The sentence under the table: what the numbers are over. Hosts add where the tenant field is set. */
export function tenantsFootnote(s: { total_sent: number; untagged_sent: number; capped: boolean }, range: string): string {
  const untagged = s.untagged_sent ? `, ${fmtNum(s.untagged_sent)} of them without a tenant` : "";
  return `${range}, from ${s.capped ? "the most recent 2,000 messages" : "every message"} in the range. Share is of all ${fmtNum(s.total_sent)} observed sends${untagged}. `
    + "p95 in queue is from the send to the first receive, over first deliveries only. Dead-lettered counts messages SQS was seen moving to a dead-letter queue.";
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
export function TenantSearch({ suggest, href, defaultValue = "", placeholder = "Tenant", className = "" }: TenantSearchProps) {
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
