/*
  The Tenants page's time range, the same in both products (QV-711): the time picker's ranges and names
  (model/timerange.ts), in the URL as every page with a range has it: range=<preset> for one that ends now, or
  from=&to= for a fixed one. The page opens on a day. Links from before QV-711 still open what they named: the
  open-source page's since_s=3600|86400|604800 and the hosted page's range=24h. Pure.
*/
import { rangeFromParams, rangeParams, type TimeRange } from "../model/timerange.ts";

/** The range the Tenants page opens on when its URL names none: a day, as it always has. */
export const DEFAULT_TENANTS_RANGE = "1d";

/** The older links' since_s values, as the time picker's ids. */
const SINCE: Record<string, string> = { "3600": "1h", "86400": "1d", "604800": "7d" };

/** The range a Tenants link names at `now`, or a day. */
export function tenantsTime(get: (k: string) => string | null | undefined, now: number): TimeRange {
  return rangeFromParams((k) => (k === "range" ? tenantsRangeId(get) : get(k)), now, DEFAULT_TENANTS_RANGE);
}

function tenantsRangeId(get: (k: string) => string | null | undefined): string | null {
  const r = get("range");
  if (r) return r === "24h" ? "1d" : r;
  return SINCE[get("since_s") ?? ""] ?? null;
}

/** The query a link from before QV-711 should have instead (since_s=, or range=24h), or null when it is already right.
    Hosts redirect to it (the hosted page) or replace the address with it (the open-source page). */
export function tenantsUpgrade(search: string): string | null {
  const p = new URLSearchParams(search);
  const since = p.get("since_s");
  if (since === null && p.get("range") !== "24h") return null;
  const id = p.get("range") === "24h" ? "1d" : SINCE[since ?? ""] ?? null;
  p.delete("since_s");
  p.delete("range");
  if (id && id !== DEFAULT_TENANTS_RANGE) p.set("range", id);
  const out = p.toString();
  return out ? `?${out}` : "";
}

/** The Tenants page's link for a range: range=<preset>, or from=&to= for a fixed range; the default range is left out. */
export function tenantsHref(path: string, t: TimeRange): string {
  const [base, query = ""] = path.split("?");
  const p = new URLSearchParams(query);
  for (const k of ["range", "from", "to", "since_s"]) p.delete(k);
  for (const [k, v] of rangeParams(t, true)) if (!(k === "range" && v === DEFAULT_TENANTS_RANGE)) p.set(k, v);
  const out = p.toString();
  return out ? `${base}?${out}` : base;
}
