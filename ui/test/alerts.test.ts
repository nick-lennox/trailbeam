/*
  The shared alert views (src/alerts), rendered to HTML on the server: what a rule's control says, the rules list's
  order, the channels list for an admin and for everyone else, how self-hosted Trailbeam's /api/alerts answers become
  the views' data, and the app's copy rules on every word they draw. Run: npm test
*/
import { test } from "node:test";
import assert from "node:assert/strict";
import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { AlertChannels, AlertMe, AlertRules } from "../src/alerts/AlertViews.tsx";
import {
  FOREVER, channelRowsOf, checkEvery, checkStatus, quietText, ruleState, rulesViewOf, sortRules, whenText,
  type ApiChannel, type ApiRules, type ApiState,
} from "../src/alerts/Alerts.ts";
import type { AlertHrefs, ChannelActions, ChannelRow, RuleActions, RuleView, RulesView } from "../src/alerts/types.ts";

const ok = async () => ({ ok: true as const });
const ruleActions: RuleActions = { add: ok, edit: ok, mute: ok, remove: ok };
const channelActions: ChannelActions = { add: ok, test: ok, remove: ok };
const hrefs: AlertHrefs = { channels: "/alerts#channels", cause: (id) => `/stuck?cause=${id}`, causes: { href: "/stuck", label: "Stuck" }, dashboard: (id) => `/dashboards/${id}` };

const rule = (id: string, over: Partial<RuleView> = {}): RuleView => ({
  id, causeId: `c${id}`, label: `Cause ${id}`, queue: "orders", channels: ["ch1"], mode: "quiet", quietS: 21_600,
  mutedUntil: null, forever: false, mutedBy: null, createdBy: "Ana", firingSince: null, checked: true, held: null, ...over,
});
const view = (rules: RuleView[], over: Partial<RulesView> = {}): RulesView => ({
  rules, channels: [{ id: "ch1", name: "#oncall", kind: "slack", minTone: "warning" }, { id: "ch2", name: "Pager", kind: "webhook", minTone: "critical" }],
  cadence: "Trailbeam checks every 5 minutes, so an alert can arrive up to 5 minutes after it happens.",
  quiet: [1800, 21_600].map((s) => ({ s, text: quietText(s) })), defaultQuiet: 21_600, dashboards: [], ...over,
});
const channel = (id: string, over: Partial<ChannelRow> = {}): ChannelRow => ({
  id, kind: "slack", name: `#${id}`, shown: "https://hooks.slack.com/services/T000", minTone: "warning", secret: null, lastAt: null, lastOk: null, lastDetail: null, ...over,
});

/** The app's copy rules (QV-477): no "a · b" fragments, no arrows, no ellipses, no em dashes, no native title. */
function copyRules(html: string) {
  assert.doesNotMatch(html, /·|→|←|…|—/, "no a · b fragments, arrows, ellipses or em dashes");
  assert.doesNotMatch(html, /\stitle="/, "hover notes are the app's Tooltip, never title");
  assert.doesNotMatch(html, /tracking-|uppercase/, "no tracked or spaced capitals");
}

test("quiet periods and check cadence in words", () => {
  assert.deepEqual([1800, 3600, 21_600, 86_400, 604_800].map(quietText), ["30 min", "1 h", "6 h", "24 h", "7 days"]);
  assert.equal(checkEvery(1440), "once a day, at 13:00 UTC");
  assert.equal(checkEvery(1), "every minute");
  assert.equal(checkEvery(60), "every hour");
  assert.equal(checkEvery(120), "every 2 hours");
  assert.equal(checkEvery(5), "every 5 minutes");
});

test("a rule's control says its state in words, never colour alone", () => {
  const ch = view([]).channels;
  assert.equal(ruleState(null, ch).word, "Alert me");
  assert.equal(ruleState(rule("1"), ch).word, "Alerting #oncall");
  assert.equal(ruleState(rule("1", { channels: ["ch1", "ch2"] }), ch).word, "Alerting #oncall and 1 more");
  assert.match(ruleState(rule("1", { channels: [] }), ch).aria, /sending to no channel/);
  const firing = ruleState(rule("1", { firingSince: 1_795_000_000_000 }), ch);
  assert.ok(firing.firing && firing.icon === "ring" && /^Firing since /.test(firing.word));
  assert.equal(ruleState(rule("1", { mutedUntil: FOREVER, forever: true }), ch).word, "Muted");
  assert.equal(ruleState(rule("1", { mutedUntil: FOREVER, forever: true, firingSince: 1 }), ch).word, "Muted, firing");
  assert.match(ruleState(rule("1", { mutedUntil: 1_795_000_000_000 }), ch).word, /^Snoozed until /);
  assert.equal(ruleState(rule("1", { held: { by: "Ben", until: null, more: 5 } }), ch).word, "Held while ignored");
  for (const r of [null, rule("1"), rule("2", { firingSince: 1 }), rule("3", { mutedUntil: FOREVER, forever: true, firingSince: 1 }), rule("4", { held: { by: null, until: null, more: null } })]) {
    const s = ruleState(r, ch);
    copyRules(`${s.word} ${s.aria}`);
  }
});

test("the rules list: firing first, then on, snoozed or held, muted", () => {
  const rules = [
    rule("muted", { mutedUntil: FOREVER, forever: true }), rule("snoozed", { mutedUntil: 9e12 }), rule("on"),
    rule("held", { held: { by: null, until: null, more: 3 } }), rule("firing", { firingSince: 1 }),
  ];
  assert.deepEqual(sortRules(rules).map((r) => r.id), ["firing", "on", "snoozed", "held", "muted"]);
  assert.equal(whenText(rule("x", { mode: "every", quietS: 0 }), view([]).quiet), "Every check with new messages");
  assert.equal(whenText(rule("x"), view([]).quiet), "Back after 6 h quiet");
  assert.equal(whenText(rule("x", { quietS: 604_800 }), view([]).quiet), "Back after 7 days quiet", "a period no longer offered still reads");
});

test("the rules list draws each rule, its channels, and the dashboard rules read-only", () => {
  const v = view([rule("1"), rule("2", { channels: ["gone"], createdBy: "", checked: false, firingSince: 1 })], {
    dashboards: [{ dashboardId: "d1", dashboardName: "Orders", title: "Dead letters", tone: "critical", value: "3", limit: "0", since: 1 }],
  });
  const html = renderToStaticMarkup(createElement(AlertRules, { view: v, canManage: true, actions: ruleActions, hrefs }));
  copyRules(html);
  assert.ok(html.indexOf('data-rule="2"') < html.indexOf('data-rule="1"'), "the firing rule first");
  assert.match(html, /href="\/stuck\?cause=c1"/);
  assert.match(html, /Back after 6 h quiet, sent to #oncall\. Made by Ana\./);
  assert.match(html, /no channel: its channels were removed/);
  assert.match(html, /Not checked yet\./);
  assert.doesNotMatch(html, /Made by \./, "no name, no made by");
  assert.match(html, /Dead letters.*on the dashboard “Orders”/);
  assert.match(html, /href="\/dashboards\/d1"/);
});

test("no rules says where to make one", () => {
  const html = renderToStaticMarkup(createElement(AlertRules, { view: view([]), canManage: true, actions: ruleActions, hrefs }));
  assert.match(html, /No alert rules yet\. Make one from <a[^>]*href="\/stuck"[^>]*>Stuck<\/a>, on a cause\./);
  copyRules(html);
});

test("Alert me on sample data is off and says why in the app's Tooltip", () => {
  const html = renderToStaticMarkup(createElement(AlertMe, { cause: { id: "c", label: "x", queue: "q" }, rule: null, view: view([]), canManage: true, actions: ruleActions, hrefs, sample: true }));
  assert.match(html, /<button[^>]*disabled=""/);
  copyRules(html);
  const live = renderToStaticMarkup(createElement(AlertMe, { cause: { id: "c", label: "x", queue: "q" }, rule: rule("1", { firingSince: 1 }), view: view([]), canManage: true, actions: ruleActions, hrefs }));
  assert.match(live, /aria-label="Alert rule for this cause: firing since [^"]+, sent to #oncall\. Edit\."/);
  assert.match(live, /aria-haspopup="dialog"/);
  copyRules(live);
});

test("channels: an admin can test and remove them and add one; everyone else is told who can", () => {
  const rows = [
    channel("a"),
    channel("b", { kind: "webhook", name: "Pager", minTone: "critical", shown: "https://example.com", lastAt: 1_795_000_000_000, lastOk: false, lastDetail: "The endpoint answered 500.", secret: "whsec_abc" }),
  ];
  const admin = renderToStaticMarkup(createElement(AlertChannels, { channels: rows, canManage: true, actions: channelActions, secretNote: "Shown once: copy it now." }));
  copyRules(admin);
  assert.equal([...admin.matchAll(/>Send test</g)].length, 2);
  assert.equal([...admin.matchAll(/>Remove</g)].length, 2);
  assert.match(admin, /Slack, warning and critical/);
  assert.match(admin, /Webhook, critical only/);
  assert.match(admin, /Nothing sent yet/);
  assert.match(admin, /Failed /);
  assert.match(admin, /The endpoint answered 500\./);
  assert.match(admin, /whsec_abc/);
  assert.match(admin, /Shown once: copy it now\./);
  assert.match(admin, />Add channel</);
  assert.match(admin, /font-mono[^"]*">https:\/\/hooks\.slack\.com/, "a target is a value, in mono");

  const member = renderToStaticMarkup(createElement(AlertChannels, { channels: rows.map((c) => ({ ...c, secret: null })), canManage: false, actions: channelActions, managers: "an owner or admin" }));
  assert.doesNotMatch(member, />Send test<|>Remove<|>Add channel</);
  assert.match(member, /Only an owner or admin can add or change alert channels\./);
  copyRules(member);

  const none = renderToStaticMarkup(createElement(AlertChannels, { channels: [], canManage: true, actions: channelActions }));
  assert.match(none, /No channels yet\./);
  copyRules(none);
});

test("the last check: when, how often, and late after two intervals", () => {
  const now = Date.UTC(2026, 10, 27, 14, 0);
  assert.match(checkStatus({ checkedAt: null, now, hasChannel: false, everyMin: 5, addChannel: "Add one below." }).text, /only when there is an alert channel\. Add one below\.$/);
  assert.equal(checkStatus({ checkedAt: null, now, hasChannel: true, everyMin: 5 }).text, "Not checked yet. Checks run every 5 minutes.");
  const fresh = checkStatus({ checkedAt: now - 3 * 60_000, now, hasChannel: true, everyMin: 5 });
  assert.deepEqual(fresh, { text: "Last checked 13:57 UTC, 3 min ago. Checks run every 5 minutes.", late: false });
  const late = checkStatus({ checkedAt: now - 11 * 60_000, now, hasChannel: true, everyMin: 5 });
  assert.ok(late.late && /so they are late and alerts may not be going out\.$/.test(late.text));
  assert.match(checkStatus({ checkedAt: Date.UTC(2026, 10, 25, 13, 0), now, hasChannel: true, everyMin: 1440 }).text, /^Last checked 25 Nov 13:00 UTC, 2 d ago\./);
});

test("self-hosted Trailbeam's /api/alerts answers become the views' data", () => {
  const api: ApiRules = {
    cadence: "Trailbeam checks every 5 minutes, so an alert can arrive up to 5 minutes after it happens.", quietChoices: [1800, 3600], defaultQuiet: 3600, everyMin: 5,
    rules: [
      { id: "r1", causeId: "c1", label: "Timeout", queue: "orders", channels: ["ch1"], mode: "quiet", quietS: 1800, mutedUntil: null, mutedBy: null, createdBy: "7", muted: false, state: { tone: "critical", since: 42 } },
      { id: "r2", causeId: "c2", label: "Bad JSON", queue: "", channels: [], mode: "every", quietS: 0, mutedUntil: FOREVER, mutedBy: "7", createdBy: "token", muted: true, state: { tone: "good", since: 1 } },
      // a snooze that ended: the server says it isn't muted now
      { id: "r3", causeId: "c3", label: "Old", queue: "q", channels: ["ch1"], mode: "quiet", quietS: 3600, mutedUntil: 5, mutedBy: "7", createdBy: "7", muted: false, state: null },
    ],
  };
  const channels: ApiChannel[] = [{ id: "ch1", kind: "email", name: "Ops", shown: "ops@example.com", minTone: "warning", lastAt: null, lastOk: null, lastDetail: null }];
  const state: ApiState = {
    checkedAt: 100, hasChannel: true, everyMin: 5,
    alerts: [
      { dashboardId: "rule", widgetId: "r1", tone: "critical", since: 42 },
      { dashboardId: "d9", dashboardName: "Payments", widgetId: "w1", title: "Stuck payments", tone: "warning", value: "4", limit: "3", since: 7 },
    ],
  };
  const v = rulesViewOf(api, channels, state, (by) => (by === "7" ? "Ana" : ""));
  assert.deepEqual(v.rules.map((r) => [r.id, r.firingSince, r.checked, r.mutedUntil, r.forever, r.mutedBy, r.createdBy]), [
    ["r1", 42, true, null, false, null, "Ana"],
    ["r2", null, true, FOREVER, true, "Ana", ""],
    ["r3", null, false, null, false, null, "Ana"],
  ]);
  assert.deepEqual(v.quiet, [{ s: 1800, text: "30 min" }, { s: 3600, text: "1 h" }]);
  assert.deepEqual(v.channels, [{ id: "ch1", name: "Ops", kind: "email", minTone: "warning" }]);
  assert.deepEqual(v.dashboards, [{ dashboardId: "d9", dashboardName: "Payments", title: "Stuck payments", tone: "warning", value: "4", limit: "3", since: 7 }]);
  assert.deepEqual(rulesViewOf(api, channels, null).dashboards, []);
  assert.deepEqual(channelRowsOf(channels, { ch1: "whsec_x" }).map((c) => [c.id, c.secret, c.shown]), [["ch1", "whsec_x", "ops@example.com"]]);
});
