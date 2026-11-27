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
export { PageHeader, TopBar } from "./TopBar.tsx";
export { MessageSearch, tenantText, type MessageSearchProps } from "./MessageSearch.tsx";
export {
  TenantRanges, TenantsCard, TenantsFoot, TenantsPage, TenantsTable,
  type TenantRange, type TenantsPageProps, type TenantsTableProps,
} from "../tenants/TenantsPage.tsx";
export {
  HealthPill, OutcomePill, SourceTag, StatePill, SOURCE, POSSIBLY_PURGED_TITLE, purgeClock, purgeText, stateLook,
  type Outcome, type PurgeOf, type StateOf, type StepSource,
} from "./pills.tsx";
