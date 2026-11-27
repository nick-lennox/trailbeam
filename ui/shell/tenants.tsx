/*
  The Tenants page (/tenants, row 7 of the one-UI plan): the shared TenantsPage in the shell's frame (frame.tsx), from
  GET /api/tenants. The URL is the page's state: since_s=3600|86400|604800 is a window ending now (a day when the URL
  names none). The top bar's search is ShellSearch, on /api/tenants/search.
*/
import { useEffect, useState } from "react";
import {
  CodeWindow, EmptyState, TENANT_ENV, TENANT_IS, TENANTS_README, noTenantsText, storeIsEmpty, tenantsEmpty, tenantsFootnote,
  type TenantStats,
} from "../src/index.ts";
import { Card, TenantsCard, TenantsFoot, TenantsPage as TenantsView, type TenantRange } from "../src/design/index.ts";
import { getHealth, getJSON, tenantHref } from "./chrome.tsx";
import { ShellFrame, ShellSearch } from "./frame.tsx";

export const RANGES = [["3600", "Last hour"], ["86400", "Last 24 hours"], ["604800", "Last 7 days"]] as const;
const RANGE_LINKS: TenantRange[] = RANGES.map(([key, label]) => ({ key, label, href: `/tenants?since_s=${key}` }));

/** The range the URL asks for, or the last 24 hours. */
export const tenantsRange = (search: string) => {
  const asked = new URLSearchParams(search).get("since_s");
  return RANGES.find((r) => r[0] === asked) ?? RANGES[1];
};

type State = { kind: "loading" } | { kind: "error"; message: string } | { kind: "ok"; s: TenantStats; storeEmpty: boolean };

const A = "text-brand hover:underline";

export function TenantsPage() {
  const range = tenantsRange(location.search);
  const [state, setState] = useState<State>({ kind: "loading" });
  const [copied, setCopied] = useState("");
  useEffect(() => {
    document.title = "Tenants · Trailbeam";
    getJSON<TenantStats>(`/api/tenants?since_s=${range[0]}`)
      .then(async (s) => {
        const storeEmpty = s.rows.length === 0 && s.total_sent === 0 ? await getHealth(5000).then(storeIsEmpty, () => false) : false;
        setState({ kind: "ok", s, storeEmpty });
      })
      .catch((e: Error) => setState({ kind: "error", message: e.message }));
  }, [range[0]]);
  const readme = <a className={A} href={TENANTS_README}>Tenants in the README</a>;
  const json = `/api/tenants?since_s=${range[0]}`;

  return (
    <ShellFrame path="/tenants">
      <TenantsView ranges={RANGE_LINKS} range={range[0]} search={<ShellSearch />} fresh={false}>
        {state.kind === "loading" && <p className="text-sm text-ink-3">Loading tenants</p>}
        {state.kind === "error" && <p className="text-sm text-ink-2" role="alert">{state.message}</p>}
        {state.kind === "ok" && state.storeEmpty && <EmptyState {...tenantsEmpty()} />}
        {state.kind === "ok" && !state.storeEmpty && state.s.rows.length === 0 && (
          <Card className="p-6">
            <p className="font-medium text-ink">No tenants yet</p>
            <p className="mt-1 max-w-[72ch] text-sm leading-relaxed text-ink-2">{noTenantsText(state.s.total_sent, range[1])}</p>
            <CodeWindow caption="shell" code={TENANT_ENV} copyName="Copy the tenant settings" onCopied={setCopied} />
            <p className="sr-only" aria-live="polite">{copied}</p>
            <p className="mt-3 text-sm">{readme}</p>
          </Card>
        )}
        {state.kind === "ok" && state.s.rows.length > 0 && (
          <>
            <TenantsCard rows={state.s.rows} href={tenantHref} />
            <TenantsFoot>
              {tenantsFootnote(state.s, range[1])} {TENANT_IS} How to set one: {readme}. The same data as JSON:{" "}
              <a className={`font-mono ${A}`} href={json}>/api/tenants</a>
            </TenantsFoot>
          </>
        )}
      </TenantsView>
    </ShellFrame>
  );
}
