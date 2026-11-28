/*
  Self-hosted Trailbeam's pages drawn with the shared views (../src): Messages at / (messages.tsx, the shared MessagesView; the first-run screen while
  no span is stored: home.tsx), Duplicates (messages.tsx), Stuck grouped by cause (stuck.tsx), the message page
  at /message?id=<message ID> (message.tsx, in the shared frame: the timeline, and with &tab=body the Body tab), the Tenants page at /tenants
  (tenants.tsx, in the shared frame),
  the Overview at /overview (overview.tsx; /queues redirects there), a queue's page at /queue?name=<queue>
  (queue.tsx), the Event explorer at /events (events.tsx), Dashboards at /dashboards (dashboards.tsx, while the server has them on), for admins the People page at /people (people.tsx, in the
  shared frame), for anyone signed in the Tokens page at /tokens (tokens.tsx, in the shared frame) and the Account page at /account (account.tsx, in the shared frame), and the Alerts page at /alerts (alerts.tsx, in the shared frame: alert rules and channels, while the server has the alert check on). They read Trailbeam's own
  API on the same origin, and move between pages through the shared Nav on the browser's history. Without the
  build, or with scripts off, the server's HTML pages answer instead.
*/
import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { HistoryNav, usePath } from "../src/design/index.ts";
import { HomePage } from "./home.tsx";
import { DashboardsShellPage } from "./dashboards.tsx";
import { EventsPage } from "./events.tsx";
import { MessageShellPage } from "./message.tsx";
import { DuplicatesPage } from "./messages.tsx";
import { OverviewPage, QueuesRedirect } from "./overview.tsx";
import { AccountPage } from "./account.tsx";
import { AlertsPage } from "./alerts.tsx";
import { PeoplePage } from "./people.tsx";
import { QueueShellPage } from "./queue.tsx";
import { StuckPage } from "./stuck.tsx";
import { TenantsPage } from "./tenants.tsx";
import { TokensPage } from "./tokens.tsx";
import "./styles.css";

const PAGES: Record<string, () => React.JSX.Element> = { "/": HomePage, "/duplicates": DuplicatesPage, "/stuck": StuckPage, "/tenants": TenantsPage, "/queue": QueueShellPage, "/queues": QueuesRedirect, "/overview": OverviewPage, "/events": EventsPage, "/dashboards": DashboardsShellPage, "/people": PeoplePage, "/tokens": TokensPage, "/account": AccountPage, "/alerts": AlertsPage };
/* The shell's Nav is the browser's history (HistoryNav): a view that moves with the Nav's push changes the page
   without a full load. The page is keyed on the Nav's version, so it reads location and its data again. */
function App() {
  const path = usePath();
  const Page = PAGES[path] ?? MessageShellPage;
  return <Page />;
}
createRoot(document.getElementById("root")!).render(<StrictMode><HistoryNav>{(v) => <App key={v} />}</HistoryNav></StrictMode>);
