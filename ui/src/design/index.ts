/*
  @trailbeam/ui/design: the hosted app's design system, shared by the hosted app and the open-source shell.
  The components here use the tokens in design.css (import "@trailbeam/ui/design.css"). Links go through the
  host's link component (NavProvider); the theme is AppThemeProvider's, night by default.
*/
export { NavProvider, UiLink, useLink, type LinkComponent, type LinkProps } from "./nav.tsx";
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
export {
  HealthPill, OutcomePill, SourceTag, StatePill, SOURCE, POSSIBLY_PURGED_TITLE, purgeClock, purgeText, stateLook,
  type Outcome, type PurgeOf, type StateOf, type StepSource,
} from "./pills.tsx";
