/*
  Self-hosted Trailbeam's pages drawn with the shared views (../src): the message page at /message?id=<message ID>
  (the timeline, and with &tab=body the Body tab) and the Tenants page at /tenants. They read Trailbeam's own
  API on the same origin. The other pages are still Trailbeam's server-rendered HTML for now; these link back
  to them.
*/
import { StrictMode, useEffect, useState } from "react";
import { createRoot } from "react-dom/client";
import {
  BodyPanel, TenantSearch, TenantsTable, TimelineView, answerFor, tenantsFootnote, type MessageBody, type TenantStats, type Timeline,
} from "../src/index.ts";
import "./styles.css";

type State = { kind: "loading" } | { kind: "missing" } | { kind: "error"; message: string } | { kind: "ok"; t: Timeline };
type BodyState = { kind: "loading" } | { kind: "error"; message: string } | { kind: "ok"; v: MessageBody };

/* Self-hosted Trailbeam has no logins: the only gates are TRAILBEAM_STORE_BODIES and who can reach it. */
const ACCESS = "No logins: anyone who can reach this Trailbeam can read the bodies it keeps.";
const HINTS = {
  not_opted_in: <>To keep this queue&rsquo;s bodies from now on, add it to <code className="font-mono text-caption">TRAILBEAM_STORE_BODIES</code> on the server.</>,
};

/** A tenant's messages: the server-rendered list, filtered (contract/tenants.md, "Tenant filter"). */
const tenantHref = (t: string) => `/?tenant=${encodeURIComponent(t)}`;
const suggest = (text: string) =>
  fetch(`/api/tenants/search?q=${encodeURIComponent(text)}`).then((r) => (r.ok ? r.json() : { tenants: [] })).then((d: { tenants: string[] }) => d.tenants);

function Header({ id = "" }: { id?: string }) {
  return (
    <header className="flex flex-wrap items-center gap-4 border-b border-sky-line py-4">
      <a href="/" className="text-[1.25rem] text-sky-ink">Trailbeam</a>
      <nav className="flex gap-4 text-sm text-sky-ink-2">
        <a href="/" className="hover:text-sky-ink">Messages</a>
        <a href="/duplicates" className="hover:text-sky-ink">Duplicates</a>
        <a href="/stuck" className="hover:text-sky-ink">Stuck</a>
        <a href="/tenants" className="hover:text-sky-ink">Tenants</a>
      </nav>
      <div className="ml-auto flex min-w-0 flex-1 basis-80 justify-end gap-2">
        <TenantSearch suggest={suggest} href={tenantHref} className="w-40 shrink-0" />
        <form action="/message" className="flex min-w-0 flex-1 justify-end">
          <label className="sr-only" htmlFor="id">Message ID</label>
          <input id="id" name="id" defaultValue={id} placeholder="Message ID"
            className="w-full max-w-sm rounded-full border border-sky-line bg-sky-2 px-4 py-1.5 font-mono text-caption text-sky-ink placeholder:text-sky-ink-3" />
        </form>
      </div>
    </header>
  );
}

const RANGES = [["3600", "Last hour"], ["86400", "Last 24 hours"], ["604800", "Last 7 days"]] as const;
type TenantsState = { kind: "loading" } | { kind: "error"; message: string } | { kind: "ok"; s: TenantStats };

function TenantsPage() {
  const asked = new URLSearchParams(location.search).get("since_s");
  const range = RANGES.find((r) => r[0] === asked) ?? RANGES[1];
  const [state, setState] = useState<TenantsState>({ kind: "loading" });
  useEffect(() => {
    document.title = "Tenants";
    fetch(`/api/tenants?since_s=${range[0]}`)
      .then(async (r) => (r.ok ? setState({ kind: "ok", s: (await r.json()) as TenantStats }) : setState({ kind: "error", message: `Trailbeam answered ${r.status}: ${(await r.text()).slice(0, 200)}` })))
      .catch(() => setState({ kind: "error", message: "Couldn't reach Trailbeam's API." }));
  }, [range[0]]);
  const setup = (
    <>A tenant comes from a Trailbeam package at contract revision 3.2 with a tenant field set: <code className="font-mono text-caption">TRAILBEAM_TENANT_ATTRIBUTE</code> (a
      message attribute) or <code className="font-mono text-caption">TRAILBEAM_TENANT_BODY_PATH</code> (a dotted path in a JSON body), or the package's <code className="font-mono text-caption">tenant</code> option,
      in each service that sends or receives. See the span contract, section 15.1.</>
  );
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
          {state.kind === "ok" && state.s.rows.length === 0 && (
            <div className="rounded-[16px] border border-sky-line bg-sky-2/55 px-5 py-4 text-[15px] leading-[1.5] text-sky-ink-2">
              <p className="text-sky-ink">No tenants in this range</p>
              <p className="mt-1 max-w-[72ch]">{state.s.total_sent > 0 ? `${state.s.total_sent.toLocaleString("en-US")} sends were seen, none with a tenant. ` : ""}{setup}</p>
            </div>
          )}
          {state.kind === "ok" && state.s.rows.length > 0 && (
            <>
              <div className="overflow-x-auto rounded-[16px] border border-sky-line bg-sky-2/55">
                <TenantsTable rows={state.s.rows} href={tenantHref} />
              </div>
              <p className="mt-3 max-w-[80ch] text-sm text-sky-ink-3">
                {tenantsFootnote(state.s, range[1])} {setup} The same data as JSON: <a className="font-mono text-caption text-dawn hover:underline" href={`/api/tenants?since_s=${range[0]}`}>/api/tenants</a>
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
    document.title = `Message ${id}`;
    fetch(`/api/messages/${encodeURIComponent(id)}`)
      .then(async (r) => {
        if (r.status === 404) return setState({ kind: "error", message: `No message with the ID ${id} has reached Trailbeam.` });
        if (!r.ok) return setState({ kind: "error", message: `Trailbeam answered ${r.status}: ${(await r.text()).slice(0, 200)}` });
        setState({ kind: "ok", t: (await r.json()) as Timeline });
      })
      .catch(() => setState({ kind: "error", message: "Couldn't reach Trailbeam's API." }));
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

const App = location.pathname === "/tenants" ? TenantsPage : MessagePage;
createRoot(document.getElementById("root")!).render(<StrictMode><App /></StrictMode>);
