/*
  @trailbeam/ui: Trailbeam's views (the timeline, tenants, message lists, Needs attention, the first-run screen, the queue page, the Event explorer), shared by self-hosted Trailbeam (built into the shell in ../shell)
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
export { QueuePage, type QueuePageProps, type QueueHrefs, type MessagesView } from "./queue/QueuePage.tsx";
export { Chart, Legend, Tag, type ChartProps, type ChartLine, type LegendItem } from "./queue/Chart.tsx";
export {
  QUEUE_PRESETS, DEFAULT_QUEUE_RANGE, STEPS_S, FAILING_NOW, SPAN_HEALTH_NOTE, NO_VERDICT, chartSummary, collapseMarkers, consoleUrl, coverageText, fmtMs, fmtRate,
  grid, onGrid, pageStep, perMin, presetLabel, presetMs, queueQuery, queueWindow, readQueueUrl, spanFiguresOf, spanHealth, totalsIn, utc, windowText,
  type QueuePreset, type QueueRange, type QueueUrl, type SpanFigures,
} from "./queue/model.ts";
export { bucketOf, flowFor, fromSelfHosted, type ApiBucket, type Flow, type FlowsResponse, type SeriesResponse } from "./queue/selfhosted.ts";
export type * from "./queue/types.ts";
export { Overview, type OverviewProps, type OverviewSlots, type OverviewState } from "./overview/Overview.tsx";
export { FlowDrawing, Wires, type FlowDrawingProps, type OverviewHrefs, type QueueRef } from "./overview/FlowDrawing.tsx";
export {
  drawingLabel, filterFlows, fmtBytes, fmtSeen, idleToggleText, isFifo, isIdle, lambdaDeletes, notesOf, openKey, orderFlows, perMinute,
  servicesText, splitIdle, summaryOf, summaryText, FILTER_FROM, LAMBDA_DELETES_HINT, NO_RECEIVE, NO_SEND, NOTE_HINT, PAGE,
  type FlowService, type Note, type OverviewSummary,
} from "./overview/derive.ts";
export { overviewHref, overviewQuery, overviewWindow, pinnedOverview, readOverviewUrl, selfHostedHrefs, widerPresets, type OverviewUrl } from "./overview/selfhosted.ts";
export { EventExplorer, type EventExplorerProps, type EventExport, type ExportOption } from "./events/EventExplorer.tsx";
export { StepHistogram, niceMax, type HistogramProps } from "./events/Histogram.tsx";
export {
  EXPORT_HEADERS, EXPORT_MAX, EXPORT_PAGE, PAGE_LIMIT, barsOf, collectSteps, eventsQuery, exportRow, fmtStepTime, kindLabel, perLabel, pickedWindow,
  serviceText, stepFailed, stepKey, stepMs, stepResult, toCsv, type Bar, type Window,
} from "./events/model.ts";
export { KIND_LABELS, QUERY_FIELDS, kindsOf, onlyKind, parseQuery, setField, type ParsedQuery, type QueryField, type Term } from "./events/query.ts";
export { STEP_KINDS, type EventHistogram, type EventPage, type EventStep, type KindCounts, type StepKind } from "./events/types.ts";
