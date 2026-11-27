"use client";

/*
  The Tenants page on the design system (row 7 of the one-UI plan), shared by self-hosted Trailbeam and the hosted app:
  TenantsPage is the page chrome (PageHeader, with the host's search in its top bar and the page's one range control),
  TenantsTable the table, TenantsCard the table in its card and TenantsFoot the footnote. They take the API's data as
  props (types.ts) and know nothing about where they run: links go through the host's Nav (UiLink), and what only the
  host knows (where the tenant field is set, the footnote's last sentence, the empty state) comes in as children.
  Colored with role tokens, so they take the theme they are drawn in. The words are Tenants.tsx's.
  A client module, reached through @trailbeam/ui/design, never the main entry (test/server-import.test.ts).
*/
import type { ComponentType, ReactNode } from "react";
import { PageHeader } from "../design/TopBar.tsx";
import { UiLink, type LinkProps } from "../design/nav.tsx";
import { Card } from "../design/Card.tsx";
import { Tooltip } from "../design/Tooltip.tsx";
import { TENANTS_SUBTITLE, TENANTS_TITLE, fmtNum, fmtShare, fmtWait, p95Note } from "./Tenants.tsx";
import type { TenantRow } from "./types.ts";

export type TenantsTableProps = {
  rows: TenantRow[];
  /**
   * Where a tenant's name links to, its messages: a prefix the encoded name is appended to
   * ("/?tenant=", so a server component can pass it), or a function of the name.
   */
  href: string | ((tenant: string) => string);
  /** a link component; the host's Nav link (UiLink) by default */
  Link?: ComponentType<LinkProps>;
  /** a p95 above this is highlighted (ms, default 1000) */
  slowMs?: number;
};

/* the design system's table cells, as the hosted tables draw them: sans labels, mono values only */
const TH = "border-b border-line px-3 py-2 text-left text-xs font-medium whitespace-nowrap text-ink-3";
const TD = "border-b border-line px-3 py-2.5";
const NUM = "font-mono text-caption tabular-nums";

/** One row per tenant, in the API's order. */
export function TenantsTable({ rows, href, Link = UiLink, slowMs = 1000 }: TenantsTableProps) {
  const max = Math.max(1, ...rows.map((t) => t.sent));
  const to = typeof href === "string" ? (t: string) => `${href}${encodeURIComponent(t)}` : href;
  return (
    <table className="w-full text-sm">
      <thead>
        <tr>
          <th scope="col" className={`${TH} pl-5`}>Tenant</th>
          <th scope="col" className={TH}>Messages sent</th>
          <th scope="col" className={`${TH} text-right`}>Share</th>
          <th scope="col" className={`${TH} text-right`}>p95 in queue</th>
          <th scope="col" className={`${TH} pr-5 text-right`}>Dead-lettered</th>
        </tr>
      </thead>
      <tbody>
        {rows.map((t) => (
          <tr key={t.tenant} className="group [&:last-child>td]:border-b-0">
            <td className={`${TD} pl-5 group-hover:bg-sunken`}>
              <Link href={to(t.tenant)} className="font-mono text-caption text-ink hover:text-brand hover:underline">{t.tenant}</Link>
            </td>
            <td className={`${TD} group-hover:bg-sunken`}>
              <div className="flex items-center gap-3">
                <div className="h-1.5 w-40 rounded-full bg-line md:w-56" aria-hidden>
                  <div className="h-full rounded-full bg-series-1" style={{ width: `${(t.sent / max) * 100}%` }} />
                </div>
                <span className={`${NUM} text-ink-2`}>{fmtNum(t.sent)}</span>
              </div>
            </td>
            <td className={`${TD} ${NUM} text-right text-ink-2 group-hover:bg-sunken`}>{fmtShare(t)}</td>
            <td className={`${TD} text-right group-hover:bg-sunken`}>
              <Tooltip content={p95Note(t.first_deliveries)}
                className={`${NUM} ${t.p95_in_queue_ms !== null && t.p95_in_queue_ms > slowMs ? "text-warning-text" : "text-ink-2"}`}>
                {t.p95_in_queue_ms === null ? <span className="font-sans text-ink-3">none</span> : fmtWait(t.p95_in_queue_ms)}
              </Tooltip>
            </td>
            <td className={`${TD} pr-5 ${NUM} text-right group-hover:bg-sunken ${t.dead_lettered ? "text-critical-text" : "text-ink-3"}`}>{fmtNum(t.dead_lettered)}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

export type TenantRange = { key: string; label: string; href: string };

/** The page's one range control: links, so a range is in the URL and a link to the page keeps it. */
export function TenantRanges({ ranges, current }: { ranges: TenantRange[]; current: string }) {
  return (
    <nav aria-label="Time range" className="flex rounded-lg border border-line bg-surface p-0.5 text-sm">
      {ranges.map((r) => (
        <UiLink key={r.key} href={r.href} aria-current={r.key === current ? "page" : undefined}
          className={`rounded-md px-2.5 py-1 ${r.key === current ? "bg-sunken font-medium text-ink" : "text-ink-2 hover:text-ink"}`}>{r.label}</UiLink>
      ))}
    </nav>
  );
}

export type TenantsPageProps = {
  /** the ranges the page offers, and the key of the one shown */
  ranges: TenantRange[];
  range: string;
  /** the host's message search, drawn in the top bar */
  search?: ReactNode;
  /** the top bar's freshness mark (PageHeader's) */
  fresh?: boolean;
  children?: ReactNode;
};

/** The page around the table: the design system's PageHeader with the range control, and the page's padding. */
export function TenantsPage({ ranges, range, search, fresh, children }: TenantsPageProps) {
  return (
    <>
      <PageHeader title={TENANTS_TITLE} subtitle={TENANTS_SUBTITLE} search={search} fresh={fresh}
        actions={ranges.length > 0 ? <TenantRanges ranges={ranges} current={range} /> : undefined} />
      <div className="px-8 pb-12">{children}</div>
    </>
  );
}

/** The table in its card, scrolling sideways on a narrow screen. */
export function TenantsCard(props: TenantsTableProps) {
  return <Card className="overflow-x-auto"><TenantsTable {...props} /></Card>;
}

/** The sentence under the table, in the page's footnote style. */
export function TenantsFoot({ children }: { children: ReactNode }) {
  return <p className="mt-3 max-w-[80ch] text-xs leading-relaxed text-ink-3">{children}</p>;
}

