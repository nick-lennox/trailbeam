/*
  Redrive in the shell (QV-590): the shared Redrive views (../src/redrive) on this server's /api/redrive
  (redrive_api.py). Available only when AWS is on and an admin has named the dead-letter queues to redrive in
  Settings, under AWS: until then GET /api/redrive answers on: false, and the pages show what they showed before.
*/
import { useCallback, useEffect, useState } from "react";
import type { DlqPlace, RedriveActions, RedriveGateAnswer, RedrivePreview, RedriveView } from "../src/index.ts";
import { getJSON, sendJSON } from "./chrome.tsx";

const fail = (e: unknown) => ({ ok: false as const, error: e instanceof Error ? e.message : "Couldn't reach Trailbeam's API." });

/** The views' actions on this server. Each answers with the server's sentence when it refuses. */
export const SHELL_REDRIVE: RedriveActions = {
  preview: (dlq) => sendJSON<{ preview: RedrivePreview }>("POST", "/api/redrive/preview", { dlq })
    .then((r) => ({ ok: true as const, preview: r.preview }), fail),
  start: (dlq, messageIds) => sendJSON("POST", "/api/redrive/start", { dlq, messageIds: messageIds.slice(0, 200) })
    .then(() => ({ ok: true as const }), fail),
  cancel: (id) => sendJSON("POST", "/api/redrive/cancel", { id }).then(() => ({ ok: true as const }), fail),
};

/** Whether each dead-letter queue may be redriven, with the place the server used (from a message's spans when sent). */
export const redriveGates = (dlqs: (DlqPlace | { name: string; messageId: string })[]) =>
  sendJSON<{ gates: RedriveGateAnswer[] }>("POST", "/api/redrive/gates", { dlqs }).then((r) => r.gates);

/** GET /api/redrive: null while loading, and when it can't be read (an older server, or the store is down). */
export function useRedrive(): { view: RedriveView | null; reload: () => void } {
  const [view, setView] = useState<RedriveView | null>(null);
  const [n, setN] = useState(0);
  useEffect(() => {
    let live = true;
    getJSON<RedriveView>("/api/redrive").then((v) => live && setView(v), () => live && setView(null));
    return () => { live = false; };
  }, [n]);
  return { view, reload: useCallback(() => setN((x) => x + 1), []) };
}
