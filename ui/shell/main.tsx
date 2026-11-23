/*
  Self-hosted Trailbeam's pages drawn with the shared views (../src): Messages at / (the first-run screen while
  no span is stored: home.tsx), Duplicates and Stuck (lists.tsx), the message page at /message?id=<message ID>
  (the timeline, and with &tab=body the Body tab) and the Tenants page at /tenants. They read Trailbeam's own
  API on the same origin. Without the build, or with scripts off, the server's HTML pages answer instead.
*/
import { StrictMode, useEffect, useState } from "react";
import { createRoot } from "react-dom/client";
import {
  BodyPanel, CodeWindow, EmptyState, TENANT_ENV, TENANT_IS, TENANTS_README, TenantsTable, TimelineView, answerFor, lookupEmpty, noTenantsText,
  storeIsEmpty, tenantsEmpty, tenantsFootnote,
  type EmptyStateProps, type MessageBody, type TenantStats, type Timeline,
} from "../src/index.ts";
import { ApiError, Header, getHealth, getJSON, tenantHref } from "./chrome.tsx";
import { HomePage } from "./home.tsx";
import { DuplicatesPage, StuckPage } from "./lists.tsx";
import "./styles.css";

type State = { kind: "loading" } | { kind: "missing" } | { kind: "error"; message: string } | { kind: "not-found"; empty: EmptyStateProps } | { kind: "ok"; t: Timeline };
type BodyState = { kind: "loading" } | { kind: "error"; message: string } | { kind: "ok"; v: MessageBody };

/* Self-hosted Trailbeam has no logins: the only gates are TRAILBEAM_STORE_BODIES and who can reach it. */
const ACCESS = "No logins: anyone who can reach this Trailbeam can read the bodies it keeps.";
const HINTS = {
  not_opted_in: <>To keep this queue&rsquo;s bodies from now on, add it to <code className="font-mono text-caption">TRAILBEAM_STORE_BODIES</code> on the server.</>,
};

const RANGES = [["3600", "Last hour"], ["86400", "Last 24 hours"], ["604800", "Last 7 days"]] as const;
type TenantsState = { kind: "loading" } | { kind: "error"; message: string } | { kind: "ok"; s: TenantStats; storeEmpty: boolean };

function TenantsPage() {
  const asked = new URLSearchParams(location.search).get("since_s");
  const range = RANGES.find((r) => r[0] === asked) ?? RANGES[1];
  const [state, setState] = useState<TenantsState>({ kind: "loading" });
  useEffect(() => {
    document.title = "Tenants · Trailbeam";
    getJSON<TenantStats>(`/api/tenants?since_s=${range[0]}`)
      .then(async (s) => {
        const storeEmpty = s.rows.length === 0 && s.total_sent === 0 ? await getHealth(5000).then(storeIsEmpty, () => false) : false;
        setState({ kind: "ok", s, storeEmpty });
      })
      .catch((e: Error) => setState({ kind: "error", message: e.message }));
  }, [range[0]]);
  const [copied, setCopied] = useState("");
  const readme = <a className="text-dawn hover:underline" href={TENANTS_README}>Tenants in the README</a>;
  return (
    <div className="mx-auto max-w-[1100px] px-4 pb-16 md:px-8">
      <Header />
      <main className="mt-6">
        <div className="flex flex-wrap items-end justify-between gap-4">
          <div>
            <h1 className="text-[1.75rem] text-sky-ink">Tenants</h1>
            <p className="mt-1 text-sky-ink-2">Which of your customers are sending the traffic, and who is waiting longest.</p>
          </div>
          <nav className="flex rounded-full border border-sky-line bg-sky-2 p-0.5 text-sm">
            {RANGES.map(([k, label]) => (
              <a key={k} href={`/tenants?since_s=${k}`} aria-current={k === range[0] ? "page" : undefined}
                className={`rounded-full px-3 py-1 ${k === range[0] ? "bg-sky-line text-sky-ink" : "text-sky-ink-2 hover:text-sky-ink"}`}>{label}</a>
            ))}
          </nav>
        </div>
        <div className="mt-5">
          {state.kind === "loading" && <p className="text-sky-ink-3">Loading tenants</p>}
          {state.kind === "error" && <p className="text-sky-ink-2" role="alert">{state.message}</p>}
          {state.kind === "ok" && state.storeEmpty && <EmptyState {...tenantsEmpty()} />}
          {state.kind === "ok" && !state.storeEmpty && state.s.rows.length === 0 && (
            <div className="rounded-[16px] border border-sky-line bg-sky-2/55 px-5 py-4 text-[15px] leading-[1.5] text-sky-ink-2">
              <p className="text-sky-ink">No tenants yet</p>
              <p className="mt-1 max-w-[72ch]">{noTenantsText(state.s.total_sent, range[1])}</p>
              <CodeWindow caption="shell" code={TENANT_ENV} copyName="Copy the tenant settings" onCopied={setCopied} />
              <p className="sr-only" aria-live="polite">{copied}</p>
              <p className="mt-3">{readme}</p>
            </div>
          )}
          {state.kind === "ok" && state.s.rows.length > 0 && (
            <>
              <div className="overflow-x-auto rounded-[16px] border border-sky-line bg-sky-2/55">
                <TenantsTable rows={state.s.rows} href={tenantHref} />
              </div>
              <p className="mt-3 max-w-[80ch] text-sm text-sky-ink-3">
                {tenantsFootnote(state.s, range[1])} {TENANT_IS} How to set one: {readme}. The same data as JSON: <a className="font-mono text-caption text-dawn hover:underline" href={`/api/tenants?since_s=${range[0]}`}>/api/tenants</a>
              </p>
            </>
          )}
        </div>
      </main>
    </div>
  );
}

function MessagePage() {
  const params = new URLSearchParams(location.search);
  const id = params.get("id")?.trim() ?? "";
  const tab = params.get("tab") === "body" ? "body" : "timeline";
  const [state, setState] = useState<State>(id ? { kind: "loading" } : { kind: "missing" });
  const [body, setBody] = useState<BodyState>({ kind: "loading" });

  useEffect(() => {
    if (!id) return;
    document.title = `Message ${id} · Trailbeam`;
    getJSON<Timeline>(`/api/messages/${encodeURIComponent(id)}`)
      .then((t) => setState({ kind: "ok", t }))
      .catch(async (e: Error) => {
        if (!(e instanceof ApiError && e.status === 404)) return setState({ kind: "error", message: e.message });
        const empty = await getHealth(5000).then(storeIsEmpty, () => false);
        setState({ kind: "not-found", empty: lookupEmpty(id, empty) });
      });
  }, [id]);

  useEffect(() => {
    if (!id || tab !== "body") return;
    fetch(`/api/messages/${encodeURIComponent(id)}/body`)
      .then(async (r) => (r.ok ? setBody({ kind: "ok", v: (await r.json()) as MessageBody }) : setBody({ kind: "error", message: `Trailbeam answered ${r.status}.` })))
      .catch(() => setBody({ kind: "error", message: "Couldn't reach Trailbeam's API." }));
  }, [id, tab]);

  const here = `/message?id=${encodeURIComponent(id)}`;

  return (
    <div className="mx-auto max-w-[1100px] px-4 pb-16 md:px-8">
      <Header id={id} />

      <main className="mt-6">
        {state.kind === "loading" && <p className="text-sky-ink-3">Loading the timeline</p>}
        {state.kind === "missing" && <p className="text-sky-ink-2">Enter a message ID to see its timeline.</p>}
        {state.kind === "error" && <p className="text-sky-ink-2" role="alert">{state.message}</p>}
        {state.kind === "not-found" && <EmptyState {...state.empty} />}
        {state.kind === "ok" && (
          <>
            {state.t.notices.length > 0 && (
              <ul className="mb-4 grid gap-2">
                {state.t.notices.map((n) => (
                  <li key={n.code} className="rounded-[12px] border border-sky-line bg-sky-2 px-4 py-3 text-[15px] leading-[1.5] text-sky-ink-2">{n.message}</li>
                ))}
              </ul>
            )}
            <nav className="mb-4 flex gap-1 border-b border-sky-line text-sm" aria-label="Message views">
              {([["timeline", "Timeline", here], ["body", "Body", `${here}&tab=body`]] as const).map(([k, label, href]) => (
                <a key={k} href={href} aria-current={tab === k ? "page" : undefined}
                  className={`-mb-px border-b-2 px-3 py-2 ${tab === k ? "border-sky-ink text-sky-ink" : "border-transparent text-sky-ink-2 hover:text-sky-ink"}`}>{label}</a>
              ))}
            </nav>
            {tab === "timeline" ? (
              <>
                <div className="overflow-hidden rounded-[16px] border border-sky-line bg-sky-2/55">
                  <TimelineView t={state.t} answer={answerFor(state.t)} />
                </div>
                <p className="mt-3 text-sm text-sky-ink-3">
                  Times are in UTC. Steps are in an order only where something observed proves it. Dashed steps are inferred.
                  The same data as JSON: <a className="font-mono text-caption text-dawn hover:underline" href={`/api/messages/${encodeURIComponent(id)}`}>/api/messages/{id}</a>
                </p>
              </>
            ) : (
              <>
                {body.kind === "loading" && <p className="text-sky-ink-3">Loading the body</p>}
                {body.kind === "error" && <p className="text-sky-ink-2" role="alert">{body.message}</p>}
                {body.kind === "ok" && (
                  <div className="overflow-hidden rounded-[16px] border border-sky-line bg-sky-2/55">
                    <BodyPanel view={body.v} access={ACCESS} hints={HINTS} />
                  </div>
                )}
                <p className="mt-3 text-sm text-sky-ink-3">
                  The same data as JSON: <a className="font-mono text-caption text-dawn hover:underline" href={`/api/messages/${encodeURIComponent(id)}/body`}>/api/messages/{id}/body</a>
                </p>
              </>
            )}
          </>
        )}
      </main>
    </div>
  );
}

const PAGES: Record<string, () => React.JSX.Element> = { "/": HomePage, "/duplicates": DuplicatesPage, "/stuck": StuckPage, "/tenants": TenantsPage };
const App = PAGES[location.pathname] ?? MessagePage;
createRoot(document.getElementById("root")!).render(<StrictMode><App /></StrictMode>);
