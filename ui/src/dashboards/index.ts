/*
  @trailbeam/ui/dashboards: dashboards, shared by both products. types.ts is a dashboard's definition (variables,
  sections and widgets on a 12-column grid) and the starters; measures.ts works out what each widget draws from the
  events and queue figures (EventData), the same way self-hosted Trailbeam's server does
  (server/src/trailbeam/server/measures.py, both run on contract/fixtures/measures-parity.json). A host makes one
  engine with dashboardMeasures(), over its query language and, on the hosted app, its sample figures.
*/
export * from "./types.ts";
export {
  ASKS, MAX_DEF_BYTES, QUEUE_MEASURES, TONE_WORD, dashboardMeasures, effectiveFilter, fmt, fmtLimit, noDepth, p95Of, queuesFor, toneOf,
  whyNone, type Alert, type DashboardMeasures, type EventData, type Measure, type QueueMeasureId, type SampleFigures, type Series,
  type Tone, type Unit,
} from "./measures.ts";
