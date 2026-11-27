/*
  Self-hosted Trailbeam's pages drawn with the shared views (../src): Messages at / (the first-run screen while
  no span is stored: home.tsx), Duplicates (lists.tsx), Stuck grouped by cause (stuck.tsx), the message page
  at /message?id=<message ID> (the timeline, and with &tab=body the Body tab), the Tenants page at /tenants
  (tenants.tsx, in the shared frame),
  the Overview at /overview (overview.tsx; /queues redirects there), a queue's page at /queue?name=<queue>
  (queue.tsx), the Event explorer at /events (events.tsx), for admins the People page at /people (people.tsx, in the
  shared frame), for anyone signed in the Tokens page at /tokens (tokens.tsx, in the shared frame) and the Account page at /account (account.tsx, in the shared frame). They read Trailbeam's own
  API on the same origin, and move between pages through the shared Nav on the browser's history. Without the
  build, or with scripts off, the server's HTML pages answer instead.
*/
import { StrictMode, useEffect, useState } from "react";
import { createRoot } from "react-dom/client";
import {
  BodyPanel, EmptyState, NoticeText, TimelineView, answerFor, lookupEmpty, storeIsEmpty,
  type EmptyStateProps, type MessageBody, type Timeline,
} from "../src/index.ts";
import { HistoryNav, usePath } from "../src/design/index.ts";
import { ApiError, CopyLink, Header, bodyAccess, getHealth, getJSON, messageHref, useMe } from "./chrome.tsx";
import { HomePage } from "./home.tsx";
import { EventsPage } from "./events.tsx";
import { DuplicatesPage } from "./lists.tsx";
import { OverviewPage, QueuesRedirect } from "./overview.tsx";
import { AccountPage } from "./account.tsx";
import { PeoplePage } from "./people.tsx";
import { QueueShellPage } from "./queue.tsx";
import { StuckPage } from "./stuck.tsx";
import { TenantsPage } from "./tenants.tsx";
import { TokensPage } from "./tokens.tsx";
import "./styles.css";

type State = { kind: "loading" } | { kind: "missing" } | { kind: "error"; message: string } | { kind: "not-found"; empty: EmptyStateProps } | { kind: "ok"; t: Timeline };
type BodyState = { kind: "loading" } | { kind: "error"; message: string } | { kind: "ok"; v: MessageBody };

const HINTS = {
  not_opted_in: <>To keep this queue&rsquo;s bodies from now on, add it to <code className="font-mono text-caption">TRAILBEAM_STORE_BODIES</code> on the server.</>,
};

function MessagePage() {
  const params = new URLSearchParams(location.search);
  const id = params.get("id")?.trim() ?? "";
  const tab = params.get("tab") === "body" ? "body" : "timeline";
  const [state, setState] = useState<State>(id ? { kind: "loading" } : { kind: "missing" });
  const [body, setBody] = useState<BodyState>({ kind: "loading" });
  const me = useMe();

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
            {/* The timeline card draws its own notices; only the Body tab needs them here. */}
            {tab === "body" && state.t.notices.length > 0 && (
              <ul className="mb-4 grid gap-2">
                {state.t.notices.map((n) => (
                  <li key={n.code} className="rounded-[12px] border border-sky-line bg-sky-2 px-4 py-3 text-[15px] leading-[1.5] text-sky-ink-2"><NoticeText n={n} /></li>
                ))}
              </ul>
            )}
            <nav className="mb-4 flex gap-1 border-b border-sky-line text-sm" aria-label="Message views">
              {([["timeline", "Timeline", here], ["body", "Body", `${here}&tab=body`]] as const).map(([k, label, href]) => (
                <a key={k} href={href} aria-current={tab === k ? "page" : undefined}
                  className={`-mb-px border-b-2 px-3 py-2 ${tab === k ? "border-sky-ink text-sky-ink" : "border-transparent text-sky-ink-2 hover:text-sky-ink"}`}>{label}</a>
              ))}
              {/* a message ID names the same timeline later: the link is this page's, with its tab */}
              <span className="ml-auto self-center"><CopyLink link={() => (tab === "body" ? `${here}&tab=body` : here)} /></span>
            </nav>
            {tab === "timeline" ? (
              <>
                <div className="overflow-hidden rounded-[16px] border border-sky-line bg-sky-2/55">
                  <TimelineView t={state.t} answer={answerFor(state.t)} messageHref={messageHref} />
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
                    <BodyPanel view={body.v} access={bodyAccess(me)} hints={HINTS} />
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

const PAGES: Record<string, () => React.JSX.Element> = { "/": HomePage, "/duplicates": DuplicatesPage, "/stuck": StuckPage, "/tenants": TenantsPage, "/queue": QueueShellPage, "/queues": QueuesRedirect, "/overview": OverviewPage, "/events": EventsPage, "/people": PeoplePage, "/tokens": TokensPage, "/account": AccountPage };
/* The shell's Nav is the browser's history (HistoryNav): a view that moves with the Nav's push changes the page
   without a full load. The page is keyed on the Nav's version, so it reads location and its data again. */
function App() {
  const path = usePath();
  const Page = PAGES[path] ?? MessagePage;
  return <Page />;
}
createRoot(document.getElementById("root")!).render(<StrictMode><HistoryNav>{(v) => <App key={v} />}</HistoryNav></StrictMode>);
