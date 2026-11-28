/*
  What the shared alert views draw (QV-592): "Alert me" rules on a cause, the channels alerts go to, and the
  dashboard rules beside them. The hosted app builds these from its tables (lib/alerts/server.ts ruleViews); self-hosted
  Trailbeam from /api/alerts (Alerts.ts rulesViewOf, channelRowsOf). Times are ms since the epoch.
*/
export type ChannelKind = "webhook" | "slack" | "email";
export type MinTone = "warning" | "critical";
export type RuleMode = "quiet" | "every";

export type RuleView = {
  id: string; causeId: string; label: string; queue: string; channels: string[]; mode: RuleMode; quietS: number;
  /** muted or snoozed until (ms), when it is now; forever: muted until someone unmutes it */
  mutedUntil: number | null; forever: boolean; mutedBy: string | null;
  /** who made it, as a name; empty when the host can't name them */
  createdBy: string;
  /** firing since (ms), at the last check; null: not firing */
  firingSince: number | null;
  /** a check has looked at it since it was made */
  checked: boolean;
  /** held while its cause is ignored in triage: who ignored it, until when (ms) or how many more messages; null: not held */
  held: { by: string | null; until: number | null; more: number | null } | null;
};

export type ChannelPick = { id: string; name: string; kind: ChannelKind; minTone: MinTone };

export type DashboardRuleView = { dashboardId: string; dashboardName: string; title: string; tone: string; value: string; limit: string; since: number };

export type RulesView = {
  rules: RuleView[]; channels: ChannelPick[];
  /** "Trailbeam checks once a day, at 13:00 UTC, so an alert can arrive up to a day after it happens." */
  cadence: string;
  quiet: { s: number; text: string }[]; defaultQuiet: number;
  dashboards: DashboardRuleView[];
};

export type ChannelRow = {
  id: string; kind: ChannelKind; name: string;
  /** the target as it may be shown: a Slack URL up to its team, a webhook up to its host, the addresses */
  shown: string; minTone: MinTone;
  /** a webhook's signing secret, when the host shows it (the hosted app to owners and admins; self-hosted once, as it is added) */
  secret: string | null;
  lastAt: number | null; lastOk: boolean | null; lastDetail: string | null;
};

/** What a change answers: the host's server action, or the self-hosted server's API through a fetch. */
export type AlertResult = { ok: true } | { ok: false; error: string };
export type RuleForm = { channels: string[]; mode: RuleMode; quietS: number };
export type ChannelForm = { kind: ChannelKind; name: string; target: string; minTone: MinTone };

/** The changes a rule's form sends. `forS`: null mutes until unmuted, 0 unmutes, else snoozes for that many seconds. */
export type RuleActions = {
  add: (causeId: string, form: RuleForm) => Promise<AlertResult>;
  edit: (ruleId: string, form: RuleForm) => Promise<AlertResult>;
  mute: (ruleId: string, forS: number | null) => Promise<AlertResult>;
  remove: (ruleId: string) => Promise<AlertResult>;
};

export type ChannelActions = {
  add: (form: ChannelForm) => Promise<AlertResult>;
  test: (id: string) => Promise<AlertResult>;
  remove: (id: string) => Promise<AlertResult>;
};

/** Where the views link: the alert channels (to add one), a cause's page, the page that lists causes, a dashboard. */
export type AlertHrefs = {
  channels: string;
  cause: (causeId: string) => string;
  causes: { href: string; label: string };
  dashboard?: (dashboardId: string) => string;
};
