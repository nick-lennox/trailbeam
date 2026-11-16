/*
  Self-hosted Trailbeam's message page, served at /message?id=<message ID>: the timeline, and with
  &tab=body the Body tab. It reads both from Trailbeam's own API on the same origin and draws them with
  the shared views (../src). The other pages are still Trailbeam's server-rendered HTML for now; this one
  links back to them.
*/
import { StrictMode, useEffect, useState } from "react";
import { createRoot } from "react-dom/client";
import { BodyPanel, TimelineView, answerFor, type MessageBody, type Timeline } from "../src/index.ts";
import "./styles.css";

type State = { kind: "loading" } | { kind: "missing" } | { kind: "error"; message: string } | { kind: "ok"; t: Timeline };
type BodyState = { kind: "loading" } | { kind: "error"; message: string } | { kind: "ok"; v: MessageBody };

/* Self-hosted Trailbeam has no logins: the only gates are TRAILBEAM_STORE_BODIES and who can reach it. */
const ACCESS = "No logins: anyone who can reach this Trailbeam can read the bodies it keeps.";
const HINTS = {
  not_opted_in: <>To keep this queue&rsquo;s bodies from now on, add it to <code className="font-mono text-caption">TRAILBEAM_STORE_BODIES</code> on the server.</>,
};

function App() {
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
      <header className="flex flex-wrap items-center gap-4 border-b border-sky-line py-4">
        <a href="/" className="text-[1.25rem] text-sky-ink">Trailbeam</a>
        <nav className="flex gap-4 text-sm text-sky-ink-2">
          <a href="/" className="hover:text-sky-ink">Messages</a>
          <a href="/duplicates" className="hover:text-sky-ink">Duplicates</a>
          <a href="/stuck" className="hover:text-sky-ink">Stuck</a>
        </nav>
        <form action="/message" className="ml-auto flex min-w-0 flex-1 basis-64 justify-end">
          <label className="sr-only" htmlFor="id">Message ID</label>
          <input id="id" name="id" defaultValue={id} placeholder="Message ID"
            className="w-full max-w-sm rounded-full border border-sky-line bg-sky-2 px-4 py-1.5 font-mono text-caption text-sky-ink placeholder:text-sky-ink-3" />
        </form>
      </header>

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

createRoot(document.getElementById("root")!).render(<StrictMode><App /></StrictMode>);
