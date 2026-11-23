/*
  @trailbeam/ui: Trailbeam's views (the timeline, tenants, message lists, the first-run screen), shared by self-hosted Trailbeam (built into the shell in ../shell)
  and the hosted app. A view takes the API's data as props and knows nothing about where it runs.
  Plan: the hosted app's docs/plans/shared-ui.md.
*/
export { TimelineView, Node, SourceTag, ORDER_SOURCE, BLIND_SPOTS, clock } from "./timeline/Timeline.tsx";
export { answerFor, consumersOf } from "./summary.ts";
export { ErrorCauses } from "./timeline/ErrorCauses.tsx";
export { errorCauses, type ErrorCause } from "./errors.ts";
export type * from "./timeline/types.ts";
export { TenantsTable, TenantSearch, tenantsFootnote, fmtWait, fmtShare } from "./tenants/Tenants.tsx";
export type * from "./tenants/types.ts";
export { BodyPanel, type BodyPanelProps } from "./body/BodyPanel.tsx";
export { bodyView, NO_BODY_TEXT, noBodyNotes, type BodySpan } from "./body/rules.ts";
export {
  bodyHit, bodyMatches, bodyPath, bodyValues, isBodyField, numHit, parseBodyQuery, parsedBody, textHit, NUM, RANGE, type BodyTerm,
} from "./body/search.ts";
export type * from "./body/types.ts";
export { FirstRun, highlight, environment, EXAMPLE, INSTALL, RUNTIMES, RUNTIME_KEY, type FirstRunProps, type Runtime } from "./firstrun/FirstRun.tsx";
export {
  announcement, isDefaultEndpoint, retainedFor, storeHasSpans, storeIsEmpty, waitingStatus, NO_SPANS_AFTER_MS,
  type FirstMessage, type FirstRunStatus, type Health,
} from "./firstrun/status.ts";
export {
  MessagesTable, EmptyState, SET_UP, STATE_LABELS, DUPLICATE_REASONS, fmtAge, fmtTime, rowFacts, shortId,
  type Column, type EmptyStateProps, type MessageSummary, type MessagesTableProps,
} from "./messages/Messages.tsx";
export { duplicatesEmpty, lookupEmpty, messagesEmpty, stuckEmpty, tenantsEmpty, without } from "./messages/empty.tsx";
