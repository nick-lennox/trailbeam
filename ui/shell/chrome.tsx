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
  constructor(public status: number, message: string) { super(message); }
}

/** GET a JSON API path; an ApiError says what went wrong in a sentence the page can show. */
export async function getJSON<T>(path: string, timeoutMs = 15_000): Promise<T> {
  const ctl = new AbortController();
  const timer = setTimeout(() => ctl.abort(), timeoutMs);
  try {
    const r = await fetch(path, { signal: ctl.signal, headers: { Accept: "application/json" } });
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

export const getHealth = (timeoutMs?: number) => getJSON<Health>("/api/health", timeoutMs);

const NAV = [["/", "Messages"], ["/duplicates", "Duplicates"], ["/stuck", "Stuck"], ["/tenants", "Tenants"]] as const;

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
  return (
    <header className="flex flex-wrap items-center gap-4 border-b border-sky-line py-4">
      <a href="/" className="text-[1.25rem] text-sky-ink">Trailbeam</a>
      <nav className="flex gap-4 text-sm text-sky-ink-2">
        {NAV.map(([href, label]) => (
          <a key={href} href={href} aria-current={location.pathname === href ? "page" : undefined}
            className={location.pathname === href ? "text-sky-ink" : "hover:text-sky-ink"}>{label}</a>
        ))}
      </nav>
      <div className="ml-auto flex min-w-0 flex-1 basis-80 justify-end gap-2">
        {tenantSeen && <TenantSearch suggest={suggest} href={tenantHref} className="w-52 shrink-0" />}
        <form action="/message" className="flex min-w-0 flex-1 justify-end">
          <label className="sr-only" htmlFor="id">Message ID</label>
          <input id="id" name="id" defaultValue={id} placeholder="Message ID"
            className="w-full max-w-sm rounded-full border border-sky-line bg-sky-2 px-4 py-1.5 font-mono text-caption text-sky-ink placeholder:text-sky-ink-3" />
        </form>
      </div>
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
