"use client";

import type { ElementType, ReactNode } from "react";
import { Layers, Moon, Sun } from "lucide-react";
import { Logo } from "./Logo.tsx";
import { Tooltip } from "./Tooltip.tsx";
import { useAppTheme } from "./theme.tsx";
import { useNav } from "./nav.tsx";

/*
  The app's sidebar frame, shared by both products: the wordmark, the host's page links in groups, the
  queues, and the foot with the theme toggle. What is in it comes from the host. Each host builds its own
  items (the hosted app's live under /app/<workspace>, the open-source shell's at the root), and its own
  extras go in slots: `top` under the wordmark (the hosted workspace switcher), an item's `action` and
  `below` (the hosted Dashboards list), `foot` above the last row (Docs), and `account` in the last row
  beside the theme toggle (the signed-in person and Sign out).
*/
export type SidebarItem = {
  href: string;
  label: string;
  icon: ElementType;
  /** A count on the item (the open issues); none when absent or 0. */
  badge?: number | null;
  /** Whether this is the page shown. Without it, the item is on when the path is its href or under it. */
  on?: boolean;
  /** A control at the item's right end, drawn over it. */
  action?: ReactNode;
  /** What opens under the item. */
  below?: ReactNode;
};
export type SidebarQueue = { name: string; href: string; health?: string | null };

export function Sidebar({
  home = "/", groups, queues, top, foot, account,
}: {
  /** Where the wordmark goes. */
  home?: string;
  /** The page links, in groups with a rule between them. */
  groups: SidebarItem[][];
  /** The queues, each linking to its page; leave it out for no Queues section. */
  queues?: SidebarQueue[];
  top?: ReactNode;
  foot?: ReactNode;
  account?: ReactNode;
}) {
  const { theme, setTheme } = useAppTheme();
  const { Link, path } = useNav();
  const isOn = (item: SidebarItem) => item.on ?? (path === item.href || (item.href !== "/" && path.startsWith(`${item.href}/`)));
  const toggle = theme === "night" ? "Switch to the light theme" : "Switch to the dark theme";
  return (
    <aside className="sticky top-0 flex h-screen w-60 shrink-0 flex-col border-r border-line bg-sunken/60">
      <div className="flex h-14 items-center px-4">
        <Link href={home}><Logo tone={theme === "night" ? "sky" : "day"} /></Link>
      </div>
      {top}
      <nav className="mt-4 grid gap-0.5 px-2 text-sm">
        {groups.map((group, i) => [
          i > 0 && <div key={`rule-${i}`} className="mx-2.5 my-2 h-px bg-line" />,
          ...group.map((item) => (
            item.action || item.below ? (
              <div key={item.href} className="relative grid gap-0.5">
                <SidebarLink item={item} on={isOn(item)} />
                {item.action}
                {item.below}
              </div>
            ) : <SidebarLink key={item.href} item={item} on={isOn(item)} />
          )),
        ])}
      </nav>
      {queues && (
        <>
          <div className="mt-6 px-5 text-xs font-medium text-ink-3">Queues</div>
          {queues.length === 0 ? (
            <p className="mt-2 px-5 text-xs leading-relaxed text-ink-3">None yet. A queue appears when a service reports a call on it.</p>
          ) : (
            <div className="mt-2 grid min-h-0 gap-0.5 overflow-y-auto px-2 text-caption">
              {queues.map((q) => (
                <Link key={q.name} href={q.href} className="flex items-center gap-2 rounded-md px-2.5 py-1 text-ink-2 hover:bg-surface/70 hover:text-ink">
                  <Layers size={13} className="shrink-0 text-ink-3" />
                  <span className="truncate">{q.name}</span>
                  {q.health && (
                    <span
                      className={`ml-auto size-1.5 shrink-0 rounded-full ${q.health === "good" ? "bg-good" : q.health === "warning" ? "bg-warning" : "bg-critical"}`}
                      aria-label={q.health}
                    />
                  )}
                </Link>
              ))}
            </div>
          )}
        </>
      )}
      <div className="mt-auto grid gap-2 p-3">
        {foot}
        <div className="flex items-center gap-2 rounded-lg px-2.5 py-1.5">
          {account}
          <Tooltip content={toggle} className={`${account ? "" : "ml-auto "}inline-flex`}>
            <button
              type="button"
              onClick={() => setTheme(theme === "night" ? "day" : "night")}
              aria-label={toggle}
              className="rounded-md p-1.5 text-ink-3 hover:bg-surface/70 hover:text-ink"
            >
              {theme === "night" ? <Sun size={15} /> : <Moon size={15} />}
            </button>
          </Tooltip>
        </div>
      </div>
    </aside>
  );
}

function SidebarLink({ item: { href, label, icon: Icon, badge }, on }: { item: SidebarItem; on: boolean }) {
  const { Link } = useNav();
  return (
    <Link
      href={href}
      aria-current={on ? "page" : undefined}
      className={`flex items-center gap-2.5 rounded-md px-2.5 py-1.5 ${
        on ? "bg-surface font-medium text-ink shadow-sm ring-1 ring-line" : "text-ink-2 hover:bg-surface/70 hover:text-ink"
      }`}
    >
      <Icon size={16} strokeWidth={on ? 2.25 : 1.9} className={on ? "text-accent" : ""} />
      {label}
      {!!badge && <span className="ml-auto rounded-full bg-serious-soft px-1.5 font-mono text-micro font-medium text-serious-text">{badge}</span>}
    </Link>
  );
}
