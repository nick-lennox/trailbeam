/*
  @trailbeam/ui: Trailbeam's views (the timeline, tenants), shared by self-hosted Trailbeam (built into the shell in ../shell)
  and the hosted app. A view takes the API's data as props and knows nothing about where it runs.
  Plan: the hosted app's docs/plans/shared-ui.md.
*/
export { TimelineView, Node, SourceTag, ORDER_SOURCE, BLIND_SPOTS, clock } from "./timeline/Timeline.tsx";
export { answerFor, consumersOf } from "./summary.ts";
export type * from "./timeline/types.ts";
export { TenantsTable, TenantSearch, tenantsFootnote, fmtWait, fmtShare } from "./tenants/Tenants.tsx";
export type * from "./tenants/types.ts";
