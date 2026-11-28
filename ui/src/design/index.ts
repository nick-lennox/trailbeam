/*
  @trailbeam/ui/design: the hosted app's design system, shared by the hosted app and the open-source shell.
  The components here use the tokens in design.css (import "@trailbeam/ui/design.css"). Links, the page's path
  and search parameters, and moving to another page go through the host's Nav (NavProvider); the theme is
  AppThemeProvider's, night by default.
*/
export {
  HistoryNav, NavProvider, UiLink, isPlainClick, plainNav, useLink, useNav, usePath, useSearch,
  type LinkComponent, type LinkProps, type Nav, type NavOptions,
} from "./nav.tsx";
export { Sidebar, type SidebarItem, type SidebarQueue } from "./Sidebar.tsx";
export { AppThemeProvider, useAppTheme, type AppTheme } from "./theme.tsx";
export { Tooltip } from "./Tooltip.tsx";
export { Badge, type BadgeTone } from "./Badge.tsx";
export { Button, type ButtonProps, type ButtonVariant } from "./Button.tsx";
export { Card, IconTile } from "./Card.tsx";
export { Container, Eyebrow, Section, SectionHeader } from "./Layout.tsx";
export { NightWindow, WindowDots } from "./NightWindow.tsx";
export { StatCard } from "./Stat.tsx";
export { Sparkline } from "./Sparkline.tsx";
export { Logo, Mark, DOME, DOT, NAME, W, H, LOCKUP_W } from "./Logo.tsx";
export { PageHeader, TopBar, type Fresh } from "./TopBar.tsx";
export { MessageSearch, tenantText, type MessageSearchProps } from "./MessageSearch.tsx";
export {
  TenantRanges, TenantsCard, TenantsFoot, TenantsPage, TenantsTable,
  type TenantRange, type TenantsPageProps, type TenantsTableProps,
} from "../tenants/TenantsPage.tsx";
export { QueuePage, type QueuePageProps } from "../queue/QueuePage.tsx";
export {
  Node, NoticeText, Notices, Reached, TimelineView, SourceTag as StepSourceTag, type TimelineViewProps,
} from "../timeline/Timeline.tsx";
export { MessageFrame, MessagePage, type MessageHrefs, type MessagePageProps, type MessageTab } from "../message/MessagePage.tsx";
export { CopyText } from "./CopyText.tsx";
export {
  Caution, Field, FormError, INPUT, OneTimeBox, Problem, SELECT, SelectField, SettingsSection, StateMark, type StateTone,
} from "./Settings.tsx";
export {
  AddPerson, OneTimeLink, PeopleTable, type AddPersonProps, type OneTimeLinkProps, type PeopleTableProps,
} from "../people/PeopleViews.tsx";
export {
  CreateToken, OneTimeToken, TokensTable, type CreateTokenProps, type OneTimeTokenProps, type TokensTableProps,
} from "../tokens/TokensViews.tsx";
export { CodeWindow, FirstRun, highlight, type FirstRunProps } from "../firstrun/FirstRun.tsx";
export { AccountCard, type AccountCardProps, type AccountPasskeys } from "../account/AccountCard.tsx";
export {
  HealthPill, OutcomePill, SourceTag, StatePill, SOURCE, POSSIBLY_PURGED_TITLE, purgeClock, purgeText, stateLook,
  type Outcome, type PurgeOf, type StateOf, type StepSource,
} from "./pills.tsx";
export { Menu, MenuItem } from "./Menu.tsx";
export { AWS_FAMILY, PlatformIcon, ServiceIcon } from "./icons.tsx";
// the Overview's views (QV-579): the flows page, "In ‹queue› now", the queue lane, a message's sequence, the messages behind a part of the flow map, and "nothing yet"
export { Watching } from "../now/Watching.tsx";
export { QueueLane, type LaneMessage } from "../now/QueueLane.tsx";
export {
  MessageSequence, Rows as SequenceRows, barsOf, sequenceOf, hash as sequenceHash, recorded as recordedBy, ORD as RECEIVE_ORDINALS, PLATFORM_ICON, SQS_ICON,
  S as SequenceWord, type Fact, type MessageSequenceProps, type Participant, type QueueFacts, type Sequence, type SequenceBar, type SequencePart,
  type SequenceStep, type SequenceTrigger,
} from "../now/MessageSequence.tsx";
export { PartMessages, type PartMessage, type PartMessagesProps } from "../now/PartMessages.tsx";
export { QueueNowLive, type QueueNowHrefs, type QueueNowLiveProps } from "../now/QueueNowLive.tsx";
export {
  OverviewFlows, type OverviewCopy, type OverviewFlowsData, type OverviewFlowsHrefs, type OverviewFlowsProps, type OverviewFlowsSlots,
  type OverviewSample, type OverviewTrigger, type PartMessagesAt,
} from "../now/OverviewFlows.tsx";
