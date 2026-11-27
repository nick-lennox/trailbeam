/*
  The message page at /message?id=<message ID> (and &tab=body for the Body tab), drawn with the shared MessagePage
  (@trailbeam/ui/design) in the shell's frame (frame.tsx), with ShellSearch in its top bar. It reads
  /api/messages/{id} and, on the Body tab, /api/messages/{id}/body on the same origin. Anyone who can reach the
  page can read the body (bodyAccess says who that is on this server), so the Body tab is always there.
  The slots the hosted app fills (redrive, the cause and its triage note) stay empty until their feature issues
  make them shared views.
*/
import { useEffect, useState } from "react";
import {
  BodyPanel, EmptyState, lookupEmpty, storeIsEmpty,
  type EmptyStateProps, type MessageBody, type MessageHrefs, type MessageTab, type Timeline,
} from "../src/index.ts";
import { MessageFrame, MessagePage } from "../src/design/index.ts";
import { ApiError, bodyAccess, getHealth, getJSON, messageHref, tenantHref, useMe } from "./chrome.tsx";
import { ShellFrame, ShellSearch } from "./frame.tsx";

type State = { kind: "loading" } | { kind: "missing" } | { kind: "error"; message: string } | { kind: "not-found"; empty: EmptyStateProps } | { kind: "ok"; t: Timeline };
type BodyState = { kind: "loading" } | { kind: "error"; message: string } | { kind: "ok"; v: MessageBody };

const HINTS = {
  not_opted_in: <>To keep this queue&rsquo;s bodies from now on, add it to <code className="font-mono text-caption">TRAILBEAM_STORE_BODIES</code> on the server.</>,
};

/** The shell's links for the message page: the Messages list is /, a queue's messages are /?queue=<name>. */
export function shellMessageHrefs(id: string): MessageHrefs {
  const here = messageHref(id);
  return {
    messages: "/",
    queue: (name) => `/?queue=${encodeURIComponent(name)}`,
    tenant: tenantHref,
    message: messageHref,
    tab: (k: MessageTab) => (k === "body" ? `${here}&tab=body` : here),
  };
}

const jsonLink = (path: string) => <a className="font-mono text-caption text-brand hover:underline" href={path}>{path}</a>;

export function MessageShellPage() {
  const params = new URLSearchParams(location.search);
  const id = params.get("id")?.trim() ?? "";
  const tab: MessageTab = params.get("tab") === "body" ? "body" : "timeline";
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

  const href = shellMessageHrefs(id);

  if (state.kind !== "ok") {
    return (
      <ShellFrame path="/message">
        <MessageFrame href={href} id={id || undefined} search={<ShellSearch />}>
          {state.kind === "loading" && <p className="text-sm text-ink-3">Loading the timeline</p>}
          {state.kind === "missing" && <p className="text-sm text-ink-2">Enter a message ID to see its timeline.</p>}
          {state.kind === "error" && <p className="text-sm text-ink-2" role="alert">{state.message}</p>}
          {state.kind === "not-found" && <EmptyState {...state.empty} />}
        </MessageFrame>
      </ShellFrame>
    );
  }

  const api = `/api/messages/${encodeURIComponent(id)}`;
  const bodyView = (
    <>
      {body.kind === "loading" && <p className="px-5 py-5 text-sm text-ink-3 md:px-7">Loading the body</p>}
      {body.kind === "error" && <p className="px-5 py-5 text-sm text-ink-2 md:px-7" role="alert">{body.message}</p>}
      {body.kind === "ok" && <BodyPanel view={body.v} access={bodyAccess(me)} hints={HINTS} />}
    </>
  );
  return (
    <ShellFrame path="/message">
      <MessagePage
        t={state.t}
        href={href}
        tab={tab}
        body={bodyView}
        tenant={body.kind === "ok" && body.v.kind === "body" ? body.v.tenant : null}
        search={<ShellSearch />}
        footer={tab === "body"
          ? <>The same data as JSON: {jsonLink(`${api}/body`)}</>
          : <>Times are in UTC. Steps are in an order only where something observed proves it. Dashed steps are inferred. The same data as JSON: {jsonLink(api)}</>}
      />
    </ShellFrame>
  );
}
