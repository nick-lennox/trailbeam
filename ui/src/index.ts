/*
  @trailbeam/ui: Trailbeam's views (the timeline, tenants, message lists, Needs attention, the first-run screen, people), shared by self-hosted Trailbeam (built into the shell in ../shell)
  and the hosted app. A view takes the API's data as props and knows nothing about where it runs.
  Plan: the hosted app's docs/plans/shared-ui.md.
*/
export { TimelineView, NoticeText, Node, SourceTag, ORDER_SOURCE, BLIND_SPOTS, clock, traceUrl, purgeNote } from "./timeline/Timeline.tsx";
export { answerFor, consumersOf } from "./summary.ts";
export { ErrorCauses } from "./timeline/ErrorCauses.tsx";
export { errorCauses, type ErrorCause } from "./errors.ts";
export type * from "./timeline/types.ts";
export { TenantsTable, TenantSearch, tenantsFootnote, noTenantsText, P95_NOTE, fmtWait, fmtShare, TENANT_ENV, TENANT_IS, TENANTS_README } from "./tenants/Tenants.tsx";
export type * from "./tenants/types.ts";
export {
  AddPerson, OneTimeLink, PeopleTable, LAST_ADMIN, ROLES, ROLE_TEXT, STATUS_HINT, STATUS_TEXT, fmtWhen, isLastAdmin, linkButton, linkKind, linkSentence,
  removeSentence, removeSsoNote, signInAdmins, type AddPersonProps, type OneTimeLinkProps, type PeopleTableProps,
} from "./people/People.tsx";
export type * from "./people/types.ts";
export { BodyPanel, type BodyPanelProps } from "./body/BodyPanel.tsx";
export { bodyView, NO_BODY_TEXT, noBodyNotes, type BodySpan } from "./body/rules.ts";
export {
  badBodyTerm, bodyHit, bodyMatches, bodyPath, bodyValues, isBodyField, numHit, parseBodyQuery, parsedBody, textHit, NUM, RANGE, type BodyTerm,
} from "./body/search.ts";
export type * from "./body/types.ts";
export { FirstRun, CodeWindow, highlight, environment, EXAMPLE, INSTALL, RUNTIMES, RUNTIME_KEY, type FirstRunProps, type Runtime } from "./firstrun/FirstRun.tsx";
export {
  announcement, isDefaultEndpoint, retainedFor, storeHasSpans, storeIsEmpty, waitingStatus, NO_SPANS_AFTER_MS,
  type FirstMessage, type FirstRunStatus, type Health,
} from "./firstrun/status.ts";
export {
  MessagesTable, EmptyState, SET_UP, STATE_LABELS, RECEIVES_TITLE, DUPLICATE_REASONS, duplicateWhy, fmtAge, fmtThreshold, fmtTime, rowFacts, shortId,
  stateText, POSSIBLY_PURGED_TITLE,
  type Column, type EmptyStateProps, type MessageSummary, type MessagesTableProps,
} from "./messages/Messages.tsx";
export {
  RANGE_PARAMS, RANGE_PRESETS, REFRESH_S, apiQuery, formQuery, fromInputUtc, pinnedQuery, rangeText, rangeWindow, readRange, refreshOn, toInputUtc,
  type RangePreset, type TimeRange,
} from "./messages/range.ts";
export { CauseList, CauseMessagesTable, DeadLetterQueues, OutcomeTag, TrendBars,
  type CauseListProps, type CauseMessagesTableProps, type DeadLetterQueuesProps } from "./causes/NeedsAttention.tsx";
export { causesNote, dlqSplit, tenantsText, trendOf, versionsText, whenFirst, whenLast, OUTCOME_LABELS, TREND_BARS, TREND_MS } from "./causes/text.ts";
export type * from "./causes/types.ts";
export { duplicatesEmpty, lookupEmpty, messagesEmpty, stuckEmpty, tenantsEmpty, without } from "./messages/empty.tsx";
