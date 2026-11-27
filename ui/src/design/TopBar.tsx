import type { ReactNode } from "react";

/*
  The bar at the top of an app page, and the heading block under it. A page that needs no heading block
  of its own passes its title and controls to TopBar. `search` is the host's message search (the hosted
  app's MessageSearch); `plain` leaves out the search and the freshness mark, for pages that are not about
  queue data. The freshness mark is off by default, because nothing measures it for you: pass `fresh` the
  real time since the data was read (`fresh={<time>12s ago</time>}` reads "Updated 12s ago"), or
  `fresh` alone (true) for the sample pages' fixed "Updated 4s ago", which only they can claim.
*/
export type Fresh = ReactNode | true;

function FreshMark({ fresh }: { fresh: Fresh }) {
  if (fresh === false || fresh == null || fresh === "") return null;
  return (
    <span className="inline-flex items-center gap-1.5 text-xs text-ink-2" data-fresh="">
      <span className="live-dot size-1.5 rounded-full bg-good" /> Updated {fresh === true ? "4s ago" : fresh}
    </span>
  );
}

export function TopBar({ title, actions, pad = "px-8", plain = false, search, fresh = false }: { title?: string; actions?: ReactNode; pad?: string; plain?: boolean; search?: ReactNode; fresh?: Fresh }) {
  return (
    <div className={`sticky top-0 z-20 flex h-14 items-center gap-4 border-b border-line bg-canvas/85 ${pad} backdrop-blur`}>
      {title && <h1 className="shrink-0 font-serif text-[15px] font-medium text-ink">{title}</h1>}
      {!plain && search}
      <span className="ml-auto flex shrink-0 items-center gap-4">
        {actions}
        {!plain && <FreshMark fresh={fresh} />}
      </span>
    </div>
  );
}

export function PageHeader({ title, subtitle, actions, plain = false, search, fresh }: { title: string; subtitle?: string; actions?: ReactNode; plain?: boolean; search?: ReactNode; fresh?: Fresh }) {
  return (
    <>
      <TopBar plain={plain} search={search} fresh={fresh} />
      <div className="flex flex-wrap items-end gap-4 px-8 pt-8 pb-6">
        <div>
          <h1 className="font-serif text-title-sm text-ink">{title}</h1>
          {subtitle && <p className="mt-1 text-sm text-ink-2">{subtitle}</p>}
        </div>
        {actions && <div className="ml-auto flex items-center gap-2">{actions}</div>}
      </div>
    </>
  );
}
