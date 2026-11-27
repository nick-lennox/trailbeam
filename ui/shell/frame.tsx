/*
  The open-source shell's app frame (row 0c of the one-UI plan): the shared Sidebar (@trailbeam/ui/design) with
  this server's pages as its items, inside the design system's theme. Each page moves into it from the header
  frame (chrome.tsx Frame) when it moves onto the design system, in rows 1 to 10; until then the pages keep
  the header, so nothing they show changes. The items are the header's: People only for admins, Tokens for
  anyone signed in. The account slot is the header's too: the name, and Sign out when it ends anything.
  ShellSearch is the top bar's search on this server: a message ID opens its timeline, tenant:<name> (or a
  suggested tenant) lists that tenant's messages, and suggestions come from /api/tenants/search.
*/
import { useCallback, useRef, type ReactNode } from "react";
import { CircleAlert, Copy, KeyRound, ListFilter, Search, Users, UserCog, Workflow } from "lucide-react";
import { AppThemeProvider, MessageSearch, Sidebar, Tooltip, tenantText, type SidebarItem, type SidebarQueue } from "../src/design/index.ts";
import { messageHref, suggestTenants, tenantHref, useMe, useTenantSeen, type Me } from "./chrome.tsx";

export function shellItems(me: Me | null, path: string): SidebarItem[][] {
  const pages: SidebarItem[] = [
    // a queue's page is under the Overview, which lists the queues
    { href: "/overview", label: "Overview", icon: Workflow, on: path === "/overview" || path === "/queue" },
    { href: "/events", label: "Event explorer", icon: ListFilter },
    { href: "/", label: "Messages", icon: Search, on: path === "/" || path === "/message" },
    { href: "/duplicates", label: "Duplicates", icon: Copy },
    { href: "/stuck", label: "Stuck", icon: CircleAlert },
    { href: "/tenants", label: "Tenants", icon: Users },
  ];
  const manage: SidebarItem[] = [
    ...(me?.user?.role === "admin" ? [{ href: "/people", label: "People", icon: UserCog }] : []),
    ...(me?.user ? [{ href: "/tokens", label: "Tokens", icon: KeyRound }] : []),
  ];
  return manage.length ? [pages, manage] : [pages];
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
          <a href="/account" className="hover:underline">{me.user.name}</a>
        </Tooltip>
      )}
      {signOut ? <a href="/logout" className="ml-auto rounded-md px-1.5 text-sm text-ink-3 hover:bg-surface/70 hover:text-ink">Sign out</a> : <span className="ml-auto" />}
    </>
  );
}

export function ShellFrame({ path, queues, children }: { path: string; queues?: { name: string; health?: string | null }[]; children: ReactNode }) {
  const me = useMe();
  const list: SidebarQueue[] | undefined = queues?.map((q) => ({ ...q, href: shellQueueHref(q.name) }));
  return (
    <AppThemeProvider className="flex min-h-screen bg-canvas font-sans text-ink">
      <Sidebar home="/overview" groups={shellItems(me, path)} queues={list} account={signedIn(me) ? <ShellAccount me={me} /> : undefined} />
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
      placeholder={tenantSeen ? "Find a message by ID, or a tenant" : "Find a message by ID"} />
  );
}
