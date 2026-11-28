/*
  @trailbeam/ui/explorer: the Event explorer and its parts, shared by self-hosted Trailbeam and the hosted app. The
  host hands it the range's events (Ev, built from each message's timeline by eventsOfTimeline in @trailbeam/ui/model)
  and keeps the query and the time range. Other pages with a time range use its TimePicker; the dashboards use its
  columns, cells and query bar, and the Messages list (MessagesView) its query bar, results bar, column list and rows. Its classes come from @trailbeam/ui/design.css (the rows' level bar and the chart's
  bars are in tokens.css).
*/
export { EventExplorer, type EventExplorerProps, type ExplorerData } from "./EventExplorer.tsx";
export { EventChart, type Seg } from "./EventChart.tsx";
export { EventDetail } from "./EventDetail.tsx";
export { TimePicker } from "./TimePicker.tsx";
export {
  ColumnChoices, ColumnList, ColumnsButton, ColumnsPanel, ExportMenu, LevelMark, QueryInput, eventCell, type ColumnGroup, type QueryBarLanguage,
} from "./parts.tsx";
// the Messages list, built from these parts (row 4 of the one-UI plan)
export { MessagesView, type MessagesViewProps } from "../messages/MessagesView.tsx";
export { DEFAULT_MESSAGE_COLUMNS, MESSAGE_COLUMNS, messageLevel, type MessageColumn, type MessageRow } from "../messages/columns.tsx";
export {
  LIST_STATES, listQuery, messageQueryLanguage, queryOfParams, traceIdOf, type ListField, type ListQuery, type MessageQueryLanguage,
} from "../messages/listQuery.ts";
export {
  DEFAULT_COLUMNS, LEVEL_COLOR, RECORDED_LANGUAGE, SPECIAL, TD, TH, cellText, colLabel, columnChoices, filledColumns, level, show, type Level,
} from "./columns.ts";
export { download, toCsv, toPdf, type Table } from "./export.ts";
export { bucketLabel, cutShort, stepChart, stepChartQuery, type StepChart, type StepChartDoc } from "./stepChart.ts";
