/*
  `/`: the Messages list, or, while the store holds no span and the URL has no filter, the first-run screen
  (design QV-146). The screen polls /api/health every 2 s while the tab is visible (10 s after a failure) and, when
  the first span is stored, shows that message and renders the list below it, without a reload.
*/
import { useEffect, useRef, useState } from "react";
import {
  FirstRun, RUNTIME_KEY, retainedFor, storeHasSpans, storeIsEmpty, waitingStatus,
  type FirstMessage, type FirstRunStatus, type Health, type MessageSummary, type Runtime,
} from "../src/index.ts";
import { Frame, getHealth, getJSON } from "./chrome.tsx";
import { MessagesList } from "./lists.tsx";

const POLL_MS = 2000;
const BACKOFF_MS = 10_000;
const FILTERS = ["queue", "tenant", "body", "cursor"];

function savedRuntime(): Runtime {
  try { return localStorage.getItem(RUNTIME_KEY) === "node" ? "node" : "python"; } catch { return "python"; }
}

async function newestMessage(): Promise<FirstMessage | null> {
  const d = await getJSON<{ messages: MessageSummary[] }>("/api/messages?limit=1");
  const m = d.messages[0];
  return m ? { message_id: m.message_id, source_queue: m.source_queue, consumer: m.consumers[0]?.service_name ?? null } : null;
}

/** The waiting line's status, from polling health; "received" once the first span is stored. */
function useFirstRun(initial: Health): FirstRunStatus {
  const since = useRef(Date.now());
  const [status, setStatus] = useState<FirstRunStatus>(() => waitingStatus(initial, 0));
  const [checkedAt, setCheckedAt] = useState(Date.now());
  const [now, setNow] = useState(Date.now());
  const [hidden, setHidden] = useState(() => document.visibilityState === "hidden");
  const received = status.kind === "received";

  useEffect(() => {
    if (received) return;
    let timer: ReturnType<typeof setTimeout> | undefined;
    let stopped = false;
    const schedule = (ms: number) => { clearTimeout(timer); if (!stopped && document.visibilityState === "visible") timer = setTimeout(poll, ms); };
    async function poll() {
      try {
        const h = await getHealth(5000);
        if (stopped) return;
        if (storeHasSpans(h)) {
          const first = await newestMessage().catch(() => null);
          if (stopped) return;
          if (first) { stopped = true; setStatus({ kind: "received", first }); return; }
          // stored, but the list has nothing yet (written between the two reads): look again shortly
        } else {
          setStatus(waitingStatus(h, Date.now() - since.current));
        }
        setCheckedAt(Date.now());
        schedule(POLL_MS);
      } catch {
        if (stopped) return;
        setStatus({ kind: "unreachable" });
        schedule(BACKOFF_MS);
      }
    }
    const onVisibility = () => {
      const isHidden = document.visibilityState === "hidden";
      setHidden(isHidden);
      if (isHidden) clearTimeout(timer); else poll(); // poll at once on coming back
    };
    document.addEventListener("visibilitychange", onVisibility);
    schedule(POLL_MS);
    return () => { stopped = true; clearTimeout(timer); document.removeEventListener("visibilitychange", onVisibility); };
  }, [received]);

  useEffect(() => {
    if (received || hidden) return;
    const t = setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(t);
  }, [received, hidden]);

  if (status.kind !== "waiting") return status;
  return hidden ? { kind: "waiting", checkedAgoS: null, hidden: true } : { kind: "waiting", checkedAgoS: Math.max(0, Math.floor((now - checkedAt) / 1000)) };
}

function FirstRunPage({ health }: { health: Health }) {
  const status = useFirstRun(health);
  const [runtime, setRuntime] = useState<Runtime>(savedRuntime);
  const pick = (r: Runtime) => { setRuntime(r); try { localStorage.setItem(RUNTIME_KEY, r); } catch { /* private mode */ } };
  const received = status.kind === "received";
  useEffect(() => { document.title = received ? "Trailbeam" : "Get started · Trailbeam"; }, [received]);
  return (
    <>
      <FirstRun endpoint={`${location.origin}/v1/traces`} auth={health.config?.auth === "token" ? "token" : "off"} status={status}
        version={health.version ?? "unknown"} runtime={runtime} onRuntime={pick} retainedFor={retainedFor(health)} />
      {received && <div className="mt-10"><MessagesList params={{}} heading="h2" /></div>}
    </>
  );
}

type HomeState = { kind: "loading" } | { kind: "first-run"; health: Health } | { kind: "list" };

export function HomePage() {
  const params = Object.fromEntries(new URLSearchParams(location.search));
  const filtered = FILTERS.some((k) => params[k]?.trim());
  const [state, setState] = useState<HomeState>(filtered ? { kind: "list" } : { kind: "loading" });
  useEffect(() => {
    if (filtered) { document.title = "Trailbeam"; return; }
    // health can't tell (no session, store down): show the list, which says what went wrong
    getHealth(5000).then((h) => setState(storeIsEmpty(h) ? { kind: "first-run", health: h } : { kind: "list" }), () => setState({ kind: "list" }));
  }, [filtered]);
  useEffect(() => { if (state.kind === "list") document.title = "Trailbeam"; }, [state.kind]);
  return (
    <Frame>
      {state.kind === "loading" && <p className="text-sky-ink-3">Loading messages</p>}
      {state.kind === "first-run" && <FirstRunPage health={state.health} />}
      {state.kind === "list" && <MessagesList params={params} />}
    </Frame>
  );
}
