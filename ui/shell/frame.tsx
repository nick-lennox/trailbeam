/*
  The open-source shell's app frame (row 0c of the one-UI plan): the shared Sidebar (@trailbeam/ui/design) with
  this server's pages as its items, inside the design system's theme. Dashboards is one only while the server
  answers /api/dashboards (TRAILBEAM_DASHBOARDS on), Alerts only while it answers /api/alerts (TRAILBEAM_ALERT_CHECK on). Each page moves into it from the header
  frame (chrome.tsx Frame) when it moves onto the design system, in rows 1 to 10; until then the pages keep
  the header, so nothing they show changes. The items are the header's: People only for admins, Tokens for
  anyone signed in, and Settings for admins (anyone before accounts exist). The account slot is the header's too: the name, a link to the Account page, and Sign out when
  it ends anything.
  ShellSearch is the top bar's search on this server: a message ID opens its timeline, tenant:<name> (or a
  suggested tenant) lists that tenant's messages, and suggestions come from /api/tenants/search.
*/
import { useCallback, useEffect, useRef, useState, type ReactNode } from "react";
import { Bell, CircleAlert, Copy, KeyRound, LayoutDashboard, ListFilter, Search, Settings, Users, UserCog, Workflow } from "lucide-react";
import { AppThemeProvider, MessageSearch, Sidebar, Tooltip, UiLink, tenantText, type SidebarItem, type SidebarQueue } from "../src/design/index.ts";
import { messageHref, suggestTenants, tenantHref, useMe, useTenantSeen, type Me } from "./chrome.tsx";

/**
 * dashboards: the server answers /api/dashboards (TRAILBEAM_DASHBOARDS is on), so Dashboards is an item. alerts: it
 * answers /api/alerts (TRAILBEAM_ALERT_CHECK is on), so Alerts is one.
 */
export function shellItems(me: Me | null, path: string, dashboards = false, alerts = false): SidebarItem[][] {
  const pages: SidebarItem[] = [
    // a queue's page is under the Overview, which lists the queues
    { href: "/overview", label: "Overview", icon: Workflow, on: path === "/overview" || path === "/queue" },
    { href: "/events", label: "Event explorer", icon: ListFilter },
    ...(dashboards ? [{ href: "/dashboards", label: "Dashboards", icon: LayoutDashboard }] : []),
    { href: "/", label: "Messages", icon: Search, on: path === "/" || path === "/message" },
    { href: "/duplicates", label: "Duplicates", icon: Copy },
    { href: "/stuck", label: "Stuck", icon: CircleAlert },
    { href: "/tenants", label: "Tenants", icon: Users },
    ...(alerts ? [{ href: "/alerts", label: "Alerts", icon: Bell }] : []),
  ];
  const manage: SidebarItem[] = [
    ...(me?.user?.role === "admin" ? [{ href: "/people", label: "People", icon: UserCog }] : []),
    ...(me?.user ? [{ href: "/tokens", label: "Tokens", icon: KeyRound }] : []),
    // as /api/settings/aws decides: anyone before accounts exist, else an admin
    ...(me && (me.auth !== "accounts" || me.user?.role === "admin") ? [{ href: "/settings", label: "Settings", icon: Settings }] : []),
  ];
  return manage.length ? [pages, manage] : [pages];
}

const asked = new Map<string, Promise<boolean>>();
/** Whether this server answers an API path (a feature it has on): asked once per page load, without the sign-in redirect a failed read makes. */
const isOn = (path: string) => {
  let p = asked.get(path);
  if (!p) asked.set(path, p = fetch(path, { headers: { Accept: "application/json" } }).then((r) => r.ok, () => false));
  return p;
};
function useOn(path: string): boolean {
  const [on, setOn] = useState(false);
  useEffect(() => {
    let live = true;
    isOn(path).then((v) => { if (live) setOn(v); });
    return () => { live = false; };
  }, [path]);
  return on;
}

export const shellQueueHref = (name: string) => `/queue?name=${encodeURIComponent(name)}`;

/** Who is signed in, and Sign out: nothing on a server without a token or accounts (chrome.tsx Account). */
const signedIn = (me: Me | null): me is Me => !!me && (me.via === "session" || me.via === "shared_cookie" || me.via === "proxy");

export function ShellAccount({ me }: { me: Me | null }) {
  if (!signedIn(me)) return null;
  const signOut = me.sign_out ?? me.via !== "proxy";
  return (
    <>
      {me.user && (
        <Tooltip content={`${me.user.email}, ${me.user.role}`} className="min-w-0 truncate text-sm text-ink">
          <UiLink href="/account" className="hover:underline">{me.user.name}</UiLink>
        </Tooltip>
      )}
      {signOut ? <a href="/logout" className="ml-auto rounded-md px-1.5 text-sm text-ink-3 hover:bg-surface/70 hover:text-ink">Sign out</a> : <span className="ml-auto" />}
    </>
  );
}

export function ShellFrame({ path, queues, children }: { path: string; queues?: { name: string; health?: string | null }[]; children: ReactNode }) {
  const me = useMe();
  // on the Dashboards or Alerts page itself the item shows at once
  const dashboards = useOn("/api/dashboards") || path === "/dashboards";
  const alerts = useOn("/api/alerts/rules") || path === "/alerts";
  const list: SidebarQueue[] | undefined = queues?.map((q) => ({ ...q, href: shellQueueHref(q.name) }));
  return (
    <AppThemeProvider className="flex min-h-screen bg-canvas font-sans text-ink">
      <Sidebar home="/overview" groups={shellItems(me, path, dashboards, alerts)} queues={list} account={signedIn(me) ? <ShellAccount me={me} /> : undefined} />
      <main className="flex min-h-screen min-w-0 flex-1 flex-col">{children}</main>
    </AppThemeProvider>
  );
}

/**
 * Where the top bar's text goes on this server: tenant:<name>, or a name the suggestions offered, to that tenant's
 * messages; anything else is a message ID, to its timeline. Empty text stays put.
 */
export function shellSearchHref(text: string, suggested: readonly string[] = []): string | null {
  const q = text.trim();
  if (!q) return null;
  if (/^tenant:/i.test(q)) return tenantText(q) ? tenantHref(tenantText(q)) : null;
  return suggested.includes(q) ? tenantHref(q) : messageHref(q);
}

/** The top bar's search. Without scripts the form opens /message?id=<text>, as the header's message box did. */
export function ShellSearch() {
  const tenantSeen = useTenantSeen();
  const last = useRef<string[]>([]);
  const suggest = useCallback((text: string, signal: AbortSignal) => suggestTenants(text, signal).then((t) => (last.current = t)), []);
  return (
    <MessageSearch action="/message" name="id" suggest={tenantSeen ? suggest : undefined}
      go={(q) => shellSearchHref(q, last.current)}
      placeholder={tenantSeen ? "Find a message by its ID or a tenant" : "Find a message by its ID"} />
  );
}
