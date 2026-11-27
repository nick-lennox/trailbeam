/*
  The open-source shell's app frame (row 0c of the one-UI plan): the shared Sidebar (@trailbeam/ui/design) with
  this server's pages as its items, inside the design system's theme. Each page moves into it from the header
  frame (chrome.tsx Frame) when it moves onto the design system, in rows 1 to 10; until then the pages keep
  the header, so nothing they show changes. The items are the header's: People only for admins, Tokens for
  anyone signed in. The account slot is the header's too: the name, and Sign out when it ends anything.
*/
import type { ReactNode } from "react";
import { CircleAlert, Copy, KeyRound, ListFilter, Search, Users, UserCog, Workflow } from "lucide-react";
import { AppThemeProvider, Sidebar, Tooltip, type SidebarItem, type SidebarQueue } from "../src/design/index.ts";
import { useMe, type Me } from "./chrome.tsx";

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
        <Tooltip content={`${me.user.email}, ${me.user.role}`} className="min-w-0 truncate text-sm text-ink">{me.user.name}</Tooltip>
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
