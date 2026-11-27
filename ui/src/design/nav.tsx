"use client";

import {
  createContext, useCallback, useContext, useEffect, useMemo, useState,
  type AnchorHTMLAttributes, type ComponentType, type MouseEvent, type ReactNode,
} from "react";

/*
  The router seam (row 0c of the one-UI plan). A view never imports a router: it reads the page's path and
  search parameters, and moves to another page, through the Nav the host supplies once, around the page.
  The hosted app's Nav wraps Next.js (next/link, usePathname, useSearchParams, useRouter); the open-source
  shell's is HistoryNav below, on the browser's history. Without a host, Nav reads window.location and
  navigates with full page loads, so a view still works on its own (and renders on a server).
*/
export type LinkProps = AnchorHTMLAttributes<HTMLAnchorElement> & { href: string; children?: ReactNode };
export type LinkComponent = ComponentType<LinkProps>;
/** `scroll: false` keeps the scroll position, for a control that only changes the page's search parameters. */
export type NavOptions = { scroll?: boolean };

export type Nav = {
  /** The host's link component: next/link in the hosted app, a history link in the open-source shell. */
  Link: LinkComponent;
  /** The page's path, without the search: "/app/acme/events". */
  path: string;
  /**
   * The host's hook for the page's search parameters, called through useSearch() by the views that read them.
   * A hook and not a value, so only those views subscribe: in Next.js, reading them makes a page render on the
   * client, which the root layout must not do to every page.
   */
  useSearch: () => URLSearchParams;
  push: (href: string, options?: NavOptions) => void;
  replace: (href: string, options?: NavOptions) => void;
  /** Reads the page's data again: Next.js re-renders the server components; the shell redraws the page. */
  refresh: () => void;
};

const PlainLink: LinkComponent = ({ children, ...rest }) => <a {...rest}>{children}</a>;
const here = () => (typeof window === "undefined" ? null : window.location);

/** The Nav without a host: window.location, and full page loads. */
export const plainNav: Nav = {
  Link: PlainLink,
  get path() { return here()?.pathname ?? "/"; },
  useSearch: () => new URLSearchParams(here()?.search ?? ""),
  push: (href) => here()?.assign(href),
  replace: (href) => here()?.replace(href),
  refresh: () => here()?.reload(),
};

const Ctx = createContext<Nav>(plainNav);

/**
 * Supplies the host's Nav to every view inside it. `Link` alone is enough for a host that only needs its
 * links to go through its router (what NavProvider took before the seam); the rest falls back to plainNav.
 */
export function NavProvider({ nav, Link, children }: { nav?: Partial<Nav>; Link?: LinkComponent; children?: ReactNode }) {
  const value = useMemo<Nav>(() => {
    const n = nav ?? {};
    return {
      Link: Link ?? n.Link ?? PlainLink,
      path: n.path ?? plainNav.path,
      useSearch: n.useSearch ?? plainNav.useSearch,
      push: n.push ?? plainNav.push,
      replace: n.replace ?? plainNav.replace,
      refresh: n.refresh ?? plainNav.refresh,
    };
  }, [nav, Link]);
  return <Ctx.Provider value={value}>{children}</Ctx.Provider>;
}

/** The host's Nav. */
export const useNav = () => useContext(Ctx);
/** The host's link component, or a plain <a> when there is no NavProvider. */
export const useLink = () => useContext(Ctx).Link;
/** The page's path. */
export const usePath = () => useContext(Ctx).path;
/** The page's search parameters. Read only: build a new URLSearchParams to change them, then push. */
export const useSearch = () => useContext(Ctx).useSearch();

/** A link through the host's link component. */
export function UiLink(props: LinkProps) {
  const L = useLink();
  return <L {...props} />;
}

/** Whether a click on a link is the plain kind a router should take over (not a new tab, a download or another site). */
export function isPlainClick(e: MouseEvent<HTMLAnchorElement>, href: string): boolean {
  if (e.defaultPrevented || e.button !== 0 || e.metaKey || e.ctrlKey || e.shiftKey || e.altKey) return false;
  const a = e.currentTarget;
  if ((a.target && a.target !== "_self") || a.hasAttribute("download")) return false;
  try {
    return new URL(href, window.location.href).origin === window.location.origin;
  } catch {
    return false;
  }
}

/**
 * The open-source shell's Nav: pages change with history.pushState, and Back and Forward redraw the page,
 * so the static shell moves between pages without a full load. `version` changes on every move and every
 * refresh: the shell keys its page on it, so a page reads its data again.
 */
export function HistoryNav({ children }: { children: (version: number) => ReactNode }) {
  const read = () => ({ path: window.location.pathname, query: window.location.search });
  const [loc, setLoc] = useState(read);
  const [version, setVersion] = useState(0);
  useEffect(() => {
    const back = () => { setLoc(read()); setVersion((v) => v + 1); };
    window.addEventListener("popstate", back);
    return () => window.removeEventListener("popstate", back);
  }, []);
  const go = useCallback((how: "pushState" | "replaceState", href: string, options?: NavOptions) => {
    const url = new URL(href, window.location.href);
    if (url.origin !== window.location.origin) return window.location.assign(url.href);
    window.history[how](null, "", url.pathname + url.search + url.hash);
    setLoc(read());
    setVersion((v) => v + 1);
    if (options?.scroll !== false) window.scrollTo(0, 0);
  }, []);
  const push = useCallback((href: string, o?: NavOptions) => go("pushState", href, o), [go]);
  const Link = useMemo<LinkComponent>(() => function HistoryLink({ href, onClick, children, ...rest }: LinkProps) {
    return (
      <a {...rest} href={href} onClick={(e) => {
        onClick?.(e);
        if (!isPlainClick(e, href)) return;
        e.preventDefault();
        push(href);
      }}>{children}</a>
    );
  }, [push]);
  const nav = useMemo<Nav>(() => ({
    Link,
    path: loc.path,
    useSearch: () => new URLSearchParams(loc.query),
    push,
    replace: (href, o) => go("replaceState", href, o),
    refresh: () => setVersion((v) => v + 1),
  }), [Link, loc, push, go]);
  return <Ctx.Provider value={nav}>{children(version)}</Ctx.Provider>;
}
