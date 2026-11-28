/*
  A dashboard is data. Its definition is JSON: variables, groups and widgets on a
  12-column grid. The editor's form is a view of a widget's definition, and the
  same JSON is what is stored, exported and pasted between dashboards.
*/

export const COLS = 12;
export type Layout = { x: number; y: number; w: number; h: number };

export const WIDGET_TYPES = ["timeseries", "heatmap", "distribution", "number", "gauge", "toplist", "queue_table", "status_grid", "event_list", "queue_flow", "note"] as const;
export type WidgetType = (typeof WIDGET_TYPES)[number];

/* A widget alerts when its value crosses a limit. "below" flips the comparison. */
export type Rule = { warn?: number; crit?: number; below?: boolean };
export type Query = { measure: string; group_by?: string };

export type Widget = {
  id: string;
  type: WidgetType;
  title: string;
  /* the Event explorer's query language; $name stands for a dashboard variable */
  filter?: string;
  queries?: Query[];
  /* event_list: field names, as in the Event explorer. queue_table: measure ids. */
  columns?: string[];
  /* distribution: the field to count by */
  field?: string;
  style?: "line" | "stacked" | "bars";
  marker?: { value: number; label: string };
  rule?: Rule;
  /* false pins the widget: it ignores the dashboard's variables */
  follow?: boolean;
  /* a preset id such as "7d": the widget keeps its own time range */
  time?: string;
  /* queue_table: draw a small trend beside the first column */
  trend?: boolean;
  text?: string;
  section?: string;
  layout: Layout;
};

export type Variable = { name: string; field: string; value: string };
/* A section is a titled band of widgets with its own grid. Widgets with no section sit above the first one. */
export const SECTION_COLORS = ["blue", "orange", "green", "yellow", "pink", "gray"] as const;
export type SectionColor = (typeof SECTION_COLORS)[number];
export type Section = {
  id: string;
  title: string;
  /* a line under the title, for what the section is about */
  description?: string;
  /* the header: its size (medium when absent), weight and alignment */
  size?: "sm" | "md" | "lg" | "xl";
  weight?: "regular" | "bold";
  align?: "left" | "center";
  /* colors: a palette name or a hex value such as "#1b2a6b". header and body are backgrounds. */
  header?: string;
  /* the header's text: "light", "dark" or a hex value; chosen to contrast when absent */
  text?: string;
  body?: string;
  /* an earlier name for header, still read */
  color?: SectionColor;
  /* false hides the rule under a header that has no background */
  divider?: boolean;
  border?: boolean;
  collapsed?: boolean;
};
export type DashboardDef = { variables: Variable[]; sections: Section[]; widgets: Widget[] };

export type Dashboard = {
  id: string;
  name: string;
  def: DashboardDef;
  starred: boolean;
  /** the display name of whoever locked it, for showing; it can be changed, so never compare it */
  lockedBy: string | null;
  /** the user ID of whoever locked it; null for locks made before this was recorded */
  lockedById: string | null;
  createdBy: string;
  updatedBy: string;
  updatedAt: number;
};

export const uid = () => Math.random().toString(36).slice(2, 10);

export const TYPE_INFO: Record<WidgetType, { label: string; hint: string; set: string; w: number; h: number }> = {
  timeseries: { label: "Time series", hint: "A measure over time", set: "Graphs", w: 6, h: 4 },
  heatmap: { label: "Heatmap", hint: "How a timing is spread, over time", set: "Graphs", w: 6, h: 4 },
  distribution: { label: "Distribution", hint: "A histogram, such as receive count", set: "Graphs", w: 4, h: 4 },
  number: { label: "Number", hint: "One value with an alert rule", set: "Numbers", w: 3, h: 2 },
  gauge: { label: "Headroom", hint: "How close queues are to their limits", set: "Numbers", w: 4, h: 2 },
  toplist: { label: "Top list", hint: "The highest or lowest few", set: "Rankings and tables", w: 4, h: 4 },
  queue_table: { label: "Queue table", hint: "A row per queue, your columns", set: "Rankings and tables", w: 8, h: 3 },
  status_grid: { label: "Queue status grid", hint: "Every queue as one cell", set: "Queues and events", w: 4, h: 3 },
  event_list: { label: "Event list", hint: "The Event explorer's list, your columns", set: "Queues and events", w: 8, h: 4 },
  queue_flow: { label: "Queue flow", hint: "Producers, queue, consumer and DLQ", set: "Queues and events", w: 8, h: 4 },
  note: { label: "Note", hint: "Text and links", set: "Layout", w: 4, h: 3 },
};
export const SETS = ["Graphs", "Numbers", "Rankings and tables", "Queues and events", "Layout"];

export const EVENT_COLUMNS = ["date", "event", "queue", "message_id", "receive_count", "body_size", "processing_time", "time_in_queue", "detail"];
export const TABLE_COLUMNS = ["messages_visible", "messages_in_flight", "oldest_age", "dlq_depth", "time_to_drain"];

/* What a new widget of each type starts as. */
export function blank(type: WidgetType, layout: Layout): Widget {
  const t = TYPE_INFO[type];
  const base = { id: uid(), type, title: t.label, layout, filter: "" };
  switch (type) {
    case "timeseries": return { ...base, title: "Messages visible by queue", queries: [{ measure: "messages_visible", group_by: "queue" }], style: "line" };
    case "heatmap": return { ...base, title: "Processing time", queries: [{ measure: "processing_time" }] };
    case "distribution": return { ...base, title: "Receive count", field: "receive_count" };
    case "number": return { ...base, title: "DLQ depth", queries: [{ measure: "dlq_depth" }], rule: { crit: 0 } };
    case "gauge": return { ...base, title: "About to fail", queries: [{ measure: "retention_headroom" }, { measure: "visibility_headroom" }, { measure: "near_dlq" }] };
    case "toplist": return { ...base, title: "Oldest message, top queues", queries: [{ measure: "oldest_age", group_by: "queue" }] };
    case "queue_table": return { ...base, title: "Queues", columns: TABLE_COLUMNS };
    case "status_grid": return { ...base, title: "Queue status" };
    case "event_list": return { ...base, title: "Failed events", filter: "status:error", columns: EVENT_COLUMNS };
    case "queue_flow": return { ...base, title: "Queue flow" };
    case "note": return { ...base, title: "Note", text: "Write what this dashboard is for, and link the runbook." };
  }
}

/* ── starters ─────────────────────────────────────────────────────── */

const W = (type: WidgetType, x: number, y: number, w: number, h: number, over: Partial<Widget> = {}): Widget => ({ ...blank(type, { x, y, w, h }), ...over, id: uid() });

const S = (title: string): Section => ({ id: uid(), title });

export const STARTERS: { id: string; name: string; hint: string; make: () => DashboardDef }[] = [
  {
    id: "queue-health",
    name: "Queue health",
    hint: "Every queue at once.",
    make: () => {
      const queues = S("Queues"), backlog = S("Backlog"), failing = S("Failing");
      return {
        variables: [],
        sections: [queues, backlog, failing],
        widgets: [
          W("status_grid", 0, 0, 6, 2),
          W("number", 6, 0, 2, 2, { title: "Messages visible", queries: [{ measure: "messages_visible" }], rule: undefined }),
          W("number", 8, 0, 2, 2, { title: "Age of oldest message", queries: [{ measure: "oldest_age" }], rule: { warn: 120, crit: 900 } }),
          W("number", 10, 0, 2, 2, { title: "DLQ depth", queries: [{ measure: "dlq_depth" }], rule: { crit: 0 } }),
          W("queue_table", 0, 0, 12, 3, { section: queues.id, trend: true }),
          W("timeseries", 0, 0, 8, 4, { section: backlog.id, title: "Messages visible by queue", queries: [{ measure: "messages_visible", group_by: "queue" }] }),
          W("toplist", 8, 0, 4, 2, { section: backlog.id, title: "Oldest message", queries: [{ measure: "oldest_age", group_by: "queue" }] }),
          W("gauge", 8, 2, 4, 2, { section: backlog.id }),
          W("timeseries", 0, 0, 6, 3, { section: failing.id, title: "Events per minute", queries: [{ measure: "events" }], style: "bars" }),
          W("toplist", 6, 0, 3, 3, { section: failing.id, title: "DLQ depth", queries: [{ measure: "dlq_depth", group_by: "queue" }] }),
          W("toplist", 9, 0, 3, 3, { section: failing.id, title: "Processing errors by consumer", queries: [{ measure: "processing_errors", group_by: "consumer" }] }),
          W("event_list", 0, 3, 12, 4, { section: failing.id }),
        ],
      };
    },
  },
  {
    id: "one-queue",
    name: "One queue in depth",
    hint: "Everything about the queue you pick.",
    make: () => {
      const through = S("Throughput"), failing = S("Failing");
      return {
        variables: [{ name: "queue", field: "queue", value: "email-dispatch" }],
        sections: [through, failing],
        widgets: [
          W("number", 0, 0, 3, 2, { title: "Messages visible", queries: [{ measure: "messages_visible" }], rule: { warn: 1000, crit: 10000 } }),
          W("number", 3, 0, 3, 2, { title: "Age of oldest message", queries: [{ measure: "oldest_age" }], rule: { warn: 120, crit: 900 } }),
          W("number", 6, 0, 3, 2, { title: "Time to drain", queries: [{ measure: "time_to_drain" }], rule: { warn: 600 } }),
          W("number", 9, 0, 3, 2, { title: "DLQ depth", queries: [{ measure: "dlq_depth" }], rule: { crit: 0 } }),
          W("timeseries", 0, 2, 7, 4, { title: "Messages in the queue", queries: [{ measure: "messages_visible" }, { measure: "messages_in_flight" }], style: "stacked" }),
          W("queue_flow", 7, 2, 5, 4),
          W("timeseries", 0, 0, 8, 4, { section: through.id, title: "Sent, received and deleted per minute", queries: [{ measure: "sent" }, { measure: "received" }, { measure: "deleted" }] }),
          W("heatmap", 8, 0, 4, 4, { section: through.id }),
          W("timeseries", 0, 0, 5, 3, { section: failing.id, title: "Events per minute", queries: [{ measure: "events" }], style: "bars" }),
          W("distribution", 5, 0, 3, 3, { section: failing.id }),
          W("gauge", 8, 0, 4, 3, { section: failing.id }),
          W("event_list", 0, 3, 12, 4, { section: failing.id }),
        ],
      };
    },
  },
  {
    id: "dead-letter",
    name: "Dead-letter queues",
    hint: "What is failing, and the events behind it.",
    make: () => {
      const evs = S("Events");
      return {
        variables: [],
        sections: [evs],
        widgets: [
          W("number", 0, 0, 3, 3, { title: "DLQ depth", queries: [{ measure: "dlq_depth" }], rule: { crit: 0 } }),
          W("toplist", 3, 0, 3, 3, { title: "DLQ depth by queue", queries: [{ measure: "dlq_depth", group_by: "queue" }] }),
          W("timeseries", 6, 0, 6, 3, { title: "Messages dead-lettered", queries: [{ measure: "dead_lettered" }], style: "bars" }),
          W("distribution", 0, 0, 4, 4, { section: evs.id }),
          W("event_list", 4, 0, 8, 4, { section: evs.id, title: "Failed events" }),
        ],
      };
    },
  },
];
