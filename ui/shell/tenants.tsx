/*
  The Tenants page (/tenants, row 7 of the one-UI plan): the shared TenantsPage in the shell's frame (frame.tsx), from
  GET /api/tenants. The URL is the page's state, as on every page with a time range (QV-711):
    range=1h|4h|1d|...   a range that ends now (a day when the URL names none), chosen with the time picker
    from, to             a fixed range (ISO 8601 in UTC, or Unix milliseconds)
  A link from before QV-711 (since_s=3600|86400|604800, or the hosted page's range=24h) opens the same range, and the
  address is rewritten to range=. The top bar's search is ShellSearch, on /api/tenants/search for the same range.
*/
import { useEffect, useState } from "react";
import {
  EmptyState, TENANT_ENV, TENANT_IS, TENANTS_README, noTenantsText, storeIsEmpty, tenantsEmpty, tenantsFootnote, tenantsHref,
  tenantsTime, type TenantStats,
} from "../src/index.ts";
import { Card, CodeWindow, TenantsCard, TenantsFoot, TenantsPage as TenantsView } from "../src/design/index.ts";
import { TimePicker } from "../src/explorer/index.ts";
import type { TimeRange } from "../src/model/index.ts";
import { getHealth, getJSON, tenantHref } from "./chrome.tsx";
import { ShellFrame, ShellSearch } from "./frame.tsx";

/** GET /api/tenants' window for [from, to): from_ms and to_ms, both included. */
export const tenantsQuery = (t: Pick<TimeRange, "from" | "to">) => `from_ms=${Math.floor(t.from)}&to_ms=${Math.ceil(t.to) - 1}`;

/** The page's range at `now` from its query, reading the links from before QV-711 too. */
export const tenantsRange = (search: string, now: number) => {
  const p = new URLSearchParams(search);
  return tenantsTime((k) => p.get(k), now);
};

type State = { kind: "loading" } | { kind: "error"; message: string } | { kind: "ok"; s: TenantStats; storeEmpty: boolean };

const A = "text-brand hover:underline";

export function TenantsPage() {
  const [time, setTime] = useState<TimeRange>(() => tenantsRange(location.search, Date.now()));
  const [state, setState] = useState<State>({ kind: "loading" });
  const [copied, setCopied] = useState("");
  // the URL follows the chosen range; a link from before QV-711 is rewritten to range= at once, so a copied address is the new one
  const link = tenantsHref("/tenants", time);
  useEffect(() => {
    if (`${location.pathname}${location.search}` !== link) history.replaceState(history.state, "", link);
  }, [link]);
  useEffect(() => {
    document.title = "Tenants, Trailbeam";
    let live = true;
    setState({ kind: "loading" });
    getJSON<TenantStats>(`/api/tenants?${tenantsQuery(time)}`)
      .then(async (s) => {
        const storeEmpty = s.rows.length === 0 && s.total_sent === 0 ? await getHealth(5000).then(storeIsEmpty, () => false) : false;
        if (live) setState({ kind: "ok", s, storeEmpty });
      })
      .catch((e: Error) => { if (live) setState({ kind: "error", message: e.message }); });
    return () => { live = false; };
  }, [time.from, time.to]);
  const readme = <a className={A} href={TENANTS_README}>Tenants in the README</a>;
  const json = `/api/tenants?${tenantsQuery(time)}`;

  return (
    <ShellFrame path="/tenants">
      <TenantsView control={<TimePicker time={time} setTime={setTime} now={Date.now()} />} search={<ShellSearch />} fresh={false}>
        {state.kind === "loading" && <p className="text-sm text-ink-3">Loading tenants</p>}
        {state.kind === "error" && <p className="text-sm text-ink-2" role="alert">{state.message}</p>}
        {state.kind === "ok" && state.storeEmpty && <EmptyState {...tenantsEmpty()} />}
        {state.kind === "ok" && !state.storeEmpty && state.s.rows.length === 0 && (
          <Card className="p-6">
            <p className="font-medium text-ink">No tenants yet</p>
            <p className="mt-1 max-w-[72ch] text-sm leading-relaxed text-ink-2">{noTenantsText(state.s.total_sent, time.label)}</p>
            <CodeWindow caption="shell" code={TENANT_ENV} copyName="Copy the tenant settings" onCopied={setCopied} />
            <p className="sr-only" aria-live="polite">{copied}</p>
            <p className="mt-3 text-sm">{readme}</p>
          </Card>
        )}
        {state.kind === "ok" && state.s.rows.length > 0 && (
          <>
            <TenantsCard rows={state.s.rows} href={tenantHref} />
            <TenantsFoot>
              {tenantsFootnote(state.s, time.label)} {TENANT_IS} How to set one: {readme}. The same data as JSON:{" "}
              <a className={`font-mono ${A}`} href={json}>/api/tenants</a>
            </TenantsFoot>
          </>
        )}
      </TenantsView>
    </ShellFrame>
  );
}
