/*
  Redrive's shapes (QV-590), the same from either backend: the self-hosted server's /api/redrive (redrive_api.py,
  contract/openapi.yaml RedriveEntry, RedrivePreview, RedriveGate) and the hosted app's server actions.
*/
import type { DlqPlace, LoggedRedrive } from "./words.ts";

/** A move task, as sqs:ListMessageMoveTasks gives it. */
export type RedriveMoveTask = { handle: string | null; status: string; moved: number | null; toMove: number | null; failure: string | null; startedAt: number | null; destination: string | null };

/** What a redrive would move, for the confirmation: the dead-letter queue's depth now, and a move already running. */
export type RedrivePreview = { visible: number | null; inFlight: number | null; running: RedriveMoveTask | null };

/** One entry of the redrive log. */
export type RedriveEntry = LoggedRedrive & { id: string; dlqArn: string; detail: string | null; taskHandle: string | null };

/** Whether this person may redrive a dead-letter queue now, and if not why, in one sentence. */
export type RedriveGate = { ok: true } | { ok: false; reason: string; code?: string };

/** A gate as the self-hosted server answers it (POST /api/redrive/gates): with the place it used. */
export type RedriveGateAnswer = RedriveGate & { dlq: DlqPlace };

/** GET /api/redrive on the self-hosted server. */
export type RedriveView = { on: boolean; reason: string | null; canRedrive: boolean; log: RedriveEntry[] };

/* newest: the latest time Trailbeam saw a message in this dead-letter queue, to warn when the queue has just been added to. */
export type RedriveTarget = { dlq: DlqPlace; messageIds: string[]; newest?: number | null; gate: RedriveGate };

export type RedriveDone = { ok: true } | { ok: false; error: string };

/**
  What the views ask the host to do. Each resolves with a sentence on failure rather than throwing: the host
  checks the gate again on its server, as the button being on in a browser proves nothing.
*/
export type RedriveActions = {
  preview: (dlq: DlqPlace) => Promise<{ ok: true; preview: RedrivePreview } | { ok: false; error: string }>;
  start: (dlq: DlqPlace, messageIds: string[]) => Promise<RedriveDone>;
  cancel: (entryId: string) => Promise<RedriveDone>;
};
