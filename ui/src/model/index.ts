/*
  @trailbeam/ui/model: the pure view-model functions both products' pages are built from (One UI row 0d, QV-578).
  No React, no server, no sample data: a host passes its data (and, on the hosted app, its sample data) in.

    events.ts      the Event explorer's events, and eventsOfTimeline: one message's events from its timeline
    causes.ts      a cause group as a row (toCause), with its trend and when it was seen
    issues.ts      a cause group as an issue (issueOf), for the Issues pages, and what to check for each kind
    stuck-age.ts   how old an undeleted message must be to count as not finished, in links and in words
    overview.ts    what the Overview draws (overviewData) and each consumer's figures (consumerFigures)
    queue-live.ts  a queue's figures, and its health from them
    in-queue.ts    "In ‹queue› now": the messages in a queue now, from either backend
    timerange.ts   one configurable time range per page: presets, typed ranges, ranges in links
    query.ts       the Event explorer's query language
    format.ts      durations, counts, sizes, platform names and the Overview's ranges

  fixtures/viewmodel-parity.json: recorded answers from both backends for the same spans, and what these functions
  make of them, which must be the same (test/viewmodel.test.ts here, and the hosted app's own test).
*/
export * from "./events.ts";
export * from "./causes.ts";
export * from "./issues.ts";
export * from "./stuck-age.ts";
export * from "./overview.ts";
export * from "./queue-live.ts";
export * from "./in-queue.ts";
export * from "./timerange.ts";
export * from "./query.ts";
export * from "./format.ts";
