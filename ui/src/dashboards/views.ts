/*
  @trailbeam/ui/dashboards/views: the dashboard pages, shared by both products (QV-591), moved from the hosted app's
  components/dashboards. DashboardsPage is the list (with what is alerting), DashboardView one dashboard on its snap
  grid. Each reads what it needs from the product through DashboardsEnvProvider (env.tsx). A client entry: the
  engine and the definition's types are in the pure @trailbeam/ui/dashboards.
*/
export { DashboardsEnvProvider, useDashboardsEnv, type DashboardActions, type DashboardsEnv, type Refused, type SampleFlow } from "./env.tsx";
export { DashboardsPage, type ServerAlerts } from "./DashboardsPage.tsx";
export { DashboardView } from "./DashboardView.tsx";
export { Mini } from "./Mini.tsx";
