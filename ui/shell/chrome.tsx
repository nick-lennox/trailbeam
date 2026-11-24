/* What every shell page shares: the header, the page frame, and reading Trailbeam's API on the same origin. */
import { useEffect, useState, type ReactNode } from "react";
import { TenantSearch, type Health } from "../src/index.ts";

/** A tenant's messages: the Messages list, filtered (contract/tenants.md, "Tenant filter"). */
export const tenantHref = (t: string) => `/?tenant=${encodeURIComponent(t)}`;
export const messageHref = (id: string) => `/message?id=${encodeURIComponent(id)}`;

/** Suggestions from the tenants named in the Tenants page's range: the one shown, else its default (a day). */
const suggest = (text: string) => {
  const since = location.pathname === "/tenants" ? new URLSearchParams(location.search).get("since_s") : null;
  const range = since ? `&since_s=${encodeURIComponent(since)}` : "";
  return fetch(`/api/tenants/search?q=${encodeURIComponent(text)}${range}`).then((r) => (r.ok ? r.json() : { tenants: [] })).then((d: { tenants: string[] }) => d.tenants);
};

export class ApiError extends Error {
  constructor(public status: number, message: string, public reason?: string) { super(message); }
}

/** GET a JSON API path; an ApiError says what went wrong in a sentence the page can show. */
export async function getJSON<T>(path: string, timeoutMs = 15_000): Promise<T> {
  const ctl = new AbortController();
  const timer = setTimeout(() => ctl.abort(), timeoutMs);
  try {
    const r = await fetch(path, { signal: ctl.signal, headers: { Accept: "application/json" } });
    if (r.status === 401) {
      // signed out (the session ended, or the person was removed): to the sign-in page, and back here after
      location.assign(signInHref());
      throw new ApiError(401, "Signed out: taking you to the sign-in page.");
    }
    if (!r.ok) {
      let msg = (await r.text()).slice(0, 200);
      try { msg = (JSON.parse(msg) as { error?: string }).error ?? msg; } catch { /* not JSON */ }
      throw new ApiError(r.status, `Trailbeam answered ${r.status}: ${msg}`);
    }
    return (await r.json()) as T;
  } catch (e) {
    if (e instanceof ApiError) throw e;
    throw new ApiError(0, "Couldn't reach Trailbeam's API.");
  } finally {
    clearTimeout(timer);
  }
}

/**
 * POST, PATCH or DELETE a people-and-tokens route (contract/openapi.yaml) with a JSON body, and read the JSON answer.
 * The page is on the server's own origin, so the browser's Sec-Fetch-Site (same-origin) passes the CSRF check.
 * An ApiError carries the server's sentence, and its `reason` (last_admin, email_taken, ...) when it gave one.
 */
export async function sendJSON<T>(method: "POST" | "PATCH" | "DELETE", path: string, body?: unknown, timeoutMs = 15_000): Promise<T> {
  const ctl = new AbortController();
  const timer = setTimeout(() => ctl.abort(), timeoutMs);
  try {
    const r = await fetch(path, {
      method, signal: ctl.signal, credentials: "same-origin",
      headers: { Accept: "application/json", ...(body !== undefined && { "Content-Type": "application/json" }) },
      ...(body !== undefined && { body: JSON.stringify(body) }),
    });
    if (r.status === 401) {
      location.assign(signInHref());
      throw new ApiError(401, "Signed out: taking you to the sign-in page.");
    }
    const text = await r.text();
    let data: { error?: string; reason?: string } = {};
    try { data = JSON.parse(text) as typeof data; } catch { /* not JSON */ }
    if (!r.ok) throw new ApiError(r.status, data.error ? capital(data.error) + "." : `Trailbeam answered ${r.status}.`, data.reason);
    return data as T;
  } catch (e) {
    if (e instanceof ApiError) throw e;
    throw new ApiError(0, "Couldn't reach Trailbeam's API.");
  } finally {
    clearTimeout(timer);
  }
}
/** The server's sentence as the page shows it: a capital first letter (not on an email it starts with), no final stop. */
const capital = (s: string) => (/^\S+@/.test(s) ? s : s.charAt(0).toUpperCase() + s.slice(1)).replace(/\.$/, "");

export const getHealth = (timeoutMs?: number) => getJSON<Health>("/api/health", timeoutMs);

export const signInHref = () => `/login?next=${encodeURIComponent(location.pathname + location.search)}`;

/** GET /api/me (contract/openapi.yaml, Me): how this page was let in, and as whom. */
export type Me = {
  auth: "off" | "token" | "accounts";
  via: "none" | "shared_token" | "shared_cookie" | "session" | "personal_token";
  user: { id: number; email: string; name: string; role: "admin" | "member" } | null;
};

let mePromise: Promise<Me | null> | null = null;
/** Asked once per page; null when it can't be read (an older server without /api/me, or the store is down). */
export const getMe = () => (mePromise ??= getJSON<Me>("/api/me", 10_000).catch(() => null));

export function useMe(): Me | null {
  const [me, setMe] = useState<Me | null>(null);
  useEffect(() => {
    let live = true;
    getMe().then((m) => { if (live) setMe(m); });
    return () => { live = false; };
  }, []);
  return me;
}

/** Who may read the bodies this server keeps, for the Body tab. */
export function bodyAccess(me: Me | null): string {
  if (me?.auth === "accounts") return "Only people with an account on this Trailbeam (admins and members) can read the bodies it keeps.";
  if (me?.auth === "token") return "Anyone with this Trailbeam's access token can read the bodies it keeps.";
  return "No logins: anyone who can reach this Trailbeam can read the bodies it keeps.";
}

/** The header's right end: who is signed in, and Sign out. Nothing on a server without a token or accounts. */
export function Account({ me }: { me: Me | null }) {
  if (!me || (me.via !== "session" && me.via !== "shared_cookie")) return null;
  return (
    <div className="flex items-center gap-3 text-sm text-sky-ink-2">
      {me.user && (
        <span title={`${me.user.email} (${me.user.role})`} className="max-w-48 truncate">
          {me.user.name}{me.user.role === "admin" && <span className="ml-1 text-caption text-sky-ink-3">admin</span>}
        </span>
      )}
      <a href="/logout" className="hover:text-sky-ink">Sign out</a>
    </div>
  );
}

const NAV = [["/", "Messages"], ["/queues", "Queues"], ["/duplicates", "Duplicates"], ["/stuck", "Stuck"], ["/tenants", "Tenants"]] as const;
/** Only admins manage people (the page is admin-only on the server too). */
const ADMIN_NAV = [["/people", "People"]] as const;
/** Tokens belong to a person: anyone signed in has their own personal API tokens (admins manage ingest tokens there too). */
const PERSON_NAV = [["/tokens", "Tokens"]] as const;

/* The header's tenant box shows only once a tenant has been seen in the last 7 days (QV-217): on an install
   without tenants it is a box that can find nothing. Remembered for the tab, so pages don't flicker. */
const SEEN_KEY = "trailbeam.tenantSeen";
const remembered = () => { try { return sessionStorage.getItem(SEEN_KEY) === "1"; } catch { return false; } };
const remember = () => { try { sessionStorage.setItem(SEEN_KEY, "1"); } catch { /* storage off: ask again next page */ } };
function useTenantSeen(): boolean {
  const [seen, setSeen] = useState(remembered);
  useEffect(() => {
    if (seen) return;
    let live = true;
    getJSON<{ rows: unknown[] }>("/api/tenants?since_s=604800").then((d) => {
      if (!live || d.rows.length === 0) return;
      remember();
      setSeen(true);
    }, () => {});
    return () => { live = false; };
  }, [seen]);
  return seen;
}

export function Header({ id = "" }: { id?: string }) {
  const tenantSeen = useTenantSeen();
  const me = useMe();
  return (
    <header className="flex flex-wrap items-center gap-4 border-b border-sky-line py-4">
      <a href="/" className="text-[1.25rem] text-sky-ink">Trailbeam</a>
      <nav className="flex gap-4 text-sm text-sky-ink-2">
        {[...NAV, ...(me?.user?.role === "admin" ? ADMIN_NAV : []), ...(me?.user ? PERSON_NAV : [])].map(([href, label]) => {
          // a queue's page is under Queues
          const here = location.pathname === href || (href === "/queues" && location.pathname === "/queue");
          return (
            <a key={href} href={href} aria-current={here ? "page" : undefined}
              className={here ? "text-sky-ink" : "hover:text-sky-ink"}>{label}</a>
          );
        })}
      </nav>
      <div className="ml-auto flex min-w-0 flex-1 basis-80 justify-end gap-2">
        {tenantSeen && <TenantSearch suggest={suggest} href={tenantHref} className="w-52 shrink-0" />}
        <form action="/message" className="flex min-w-0 flex-1 justify-end">
          <label className="sr-only" htmlFor="id">Message ID</label>
          <input id="id" name="id" defaultValue={id} placeholder="Message ID"
            className="w-full max-w-sm rounded-full border border-sky-line bg-sky-2 px-4 py-1.5 font-mono text-caption text-sky-ink placeholder:text-sky-ink-3" />
        </form>
      </div>
      <Account me={me} />
    </header>
  );
}

export function Frame({ id, children }: { id?: string; children: ReactNode }) {
  return (
    <div className="mx-auto max-w-[1100px] px-4 pb-16 md:px-8">
      <Header id={id} />
      <main className="mt-6">{children}</main>
    </div>
  );
}

/** The sentence under a list or a view: where the same data is as JSON. */
export const Json = ({ href }: { href: string }) => (
  <>The same data as JSON: <a className="font-mono text-caption text-dawn hover:underline" href={href}>{href.split("?")[0]}</a></>
);

/**
 * "Copy link": puts this origin plus ``link()`` on the clipboard. ``link`` is asked on the click, so a list can pin
 * the window of the rows on screen. Without a clipboard (a plain-http host other than localhost), the link is
 * shown in a field, selected, to copy by hand.
 */
export function CopyLink({ link, label = "Copy link" }: { link: () => string; label?: string }) {
  const [state, setState] = useState<{ kind: "idle" } | { kind: "copied" } | { kind: "select"; url: string }>({ kind: "idle" });
  useEffect(() => {
    if (state.kind !== "copied") return;
    const t = setTimeout(() => setState({ kind: "idle" }), 2000);
    return () => clearTimeout(t);
  }, [state]);
  const copy = () => {
    const url = `${location.origin}${link()}`;
    const clip = typeof navigator !== "undefined" ? navigator.clipboard : undefined;
    if (!clip?.writeText) return setState({ kind: "select", url });
    clip.writeText(url).then(() => setState({ kind: "copied" }), () => setState({ kind: "select", url }));
  };
  return (
    <span className="inline-flex items-center gap-2">
      <button type="button" onClick={copy}
        className="rounded-full border border-sky-line px-3 py-0.5 text-sm text-sky-ink-2 hover:border-sky-ink hover:text-sky-ink">
        {state.kind === "copied" ? "Copied" : label}
      </button>
      {state.kind === "select" && (
        <input readOnly value={state.url} aria-label="Link to copy" autoFocus onFocus={(e) => e.currentTarget.select()}
          className="w-72 rounded-full border border-sky-line bg-sky-2 px-3 py-0.5 font-mono text-caption text-sky-ink" />
      )}
      <span className="sr-only" aria-live="polite">{state.kind === "copied" ? "Link copied to the clipboard." : state.kind === "select" ? "Couldn't use the clipboard: the link is selected, press Ctrl+C or Cmd+C to copy." : ""}</span>
    </span>
  );
}
