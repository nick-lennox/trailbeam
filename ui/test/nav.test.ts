/*
  The router seam (row 0c of the one-UI plan): views read the path and search parameters and move between pages
  through the host's Nav, never a router of their own; the Sidebar is a frame whose items and extras come from the
  host. The open-source shell's items are the header's, and it routes through the browser's history.
  Run: npm test
*/
import { test } from "node:test";
import assert from "node:assert/strict";
import { readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { createElement as h } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { Workflow, Search } from "lucide-react";
import { NavProvider, Sidebar, UiLink, plainNav, useNav, useSearch, type LinkProps, type Nav } from "../src/design/index.ts";
import { shellItems } from "../shell/frame.tsx";

const html = (el: ReturnType<typeof h>) => renderToStaticMarkup(el);
const SRC = new URL("../src/", import.meta.url).pathname;
const SHELL = new URL("../shell/", import.meta.url).pathname;
const files = (dir: string): string[] =>
  readdirSync(dir, { withFileTypes: true }).flatMap((d) => (d.isDirectory() ? files(join(dir, d.name)) : /\.tsx?$/.test(d.name) ? [join(dir, d.name)] : []));

function Probe() {
  const nav = useNav();
  return h("span", null, `${nav.path}?${useSearch().toString()}`);
}

test("without a host, the Nav is a plain <a> and the root path, so a view renders on a server", () => {
  assert.equal(html(h(Probe)), "<span>/?</span>");
  assert.equal(html(h(UiLink, { href: "/x", className: "c" }, "x")), '<a href="/x" class="c">x</a>');
  assert.equal(plainNav.path, "/");
});

test("the host's Nav reaches every view: its link, path, search parameters and push", () => {
  const pushed: string[] = [];
  const HostLink = ({ children, ...rest }: LinkProps) => h("a", { ...rest, "data-host": "" }, children);
  const nav: Partial<Nav> = { Link: HostLink, path: "/events", useSearch: () => new URLSearchParams("q=a"), push: (href) => pushed.push(href) };
  function Pusher() { const { push } = useNav(); push("/next"); return null; }
  const out = html(h(NavProvider, { nav }, h(Probe), h(UiLink, { href: "/y" }, "y"), h(Pusher)));
  assert.equal(out, '<span>/events?q=a</span><a href="/y" data-host="">y</a>');
  assert.deepEqual(pushed, ["/next"]);
  // Link alone still works, as NavProvider took before the seam
  assert.match(html(h(NavProvider, { Link: HostLink }, h(UiLink, { href: "/z" }, "z"))), /data-host/);
});

const items = [[
  { href: "/overview", label: "Overview", icon: Workflow },
  { href: "/", label: "Messages", icon: Search, badge: 3, action: h("button", { "data-action": "" }), below: h("div", { "data-below": "" }) },
]];

test("the Sidebar draws the host's items, marks the page shown, and puts the host's extras in its slots", () => {
  const out = html(h(NavProvider, { nav: { path: "/overview" } }, h(Sidebar, {
    groups: [...items, [{ href: "/people", label: "People", icon: Search }]],
    queues: [{ name: "orders", href: "/queue?name=orders", health: "warning" }],
    top: h("div", { "data-top": "" }), foot: h("a", { "data-foot": "" }), account: h("span", { "data-account": "" }),
  })));
  assert.match(out, /<a aria-current="page"[^>]*href="\/overview"|<a href="\/overview" aria-current="page"/);
  assert.equal((out.match(/aria-current="page"/g) ?? []).length, 1, "the root item is not on for every page");
  for (const slot of ["data-top", "data-foot", "data-account", "data-action", "data-below"]) assert.match(out, new RegExp(slot));
  assert.match(out, />3<\/span>/);
  assert.match(out, /mx-2\.5 my-2 h-px bg-line/, "a rule between the groups");
  assert.match(out, /href="\/queue\?name=orders".*aria-label="warning"/);
  assert.match(out, /aria-label="Switch to the light theme"/);
  assert.doesNotMatch(out, /\stitle=/);
});

test("without queues the Sidebar has no Queues section, and with none yet it says so", () => {
  assert.doesNotMatch(html(h(Sidebar, { groups: items })), />Queues</);
  assert.match(html(h(Sidebar, { groups: items, queues: [] })), /None yet\. A queue appears when a service reports a call on it\./);
});

test("the shell's items are its header's: People for admins, Tokens for anyone signed in, Settings for admins", () => {
  const labels = (g: ReturnType<typeof shellItems>) => g.map((x) => x.map((i) => i.label));
  assert.deepEqual(labels(shellItems(null, "/")), [["Overview", "Event explorer", "Messages", "Duplicates", "Stuck", "Tenants"]]);
  const admin = { auth: "accounts", via: "session", user: { id: 1, email: "a@b.c", name: "A", role: "admin" } } as const;
  assert.deepEqual(labels(shellItems(admin, "/"))[1], ["People", "Tokens", "Settings"]);
  assert.deepEqual(labels(shellItems({ ...admin, user: { ...admin.user, role: "member" } }, "/"))[1], ["Tokens"]);
  // before accounts exist anyone may change the settings, as /api/settings/aws decides
  assert.deepEqual(labels(shellItems({ auth: "token", via: "shared_token", user: null }, "/"))[1], ["Settings"]);
  // a queue's page is under the Overview; a message's under Messages
  assert.equal(shellItems(null, "/queue")[0][0].on, true);
  assert.equal(shellItems(null, "/message")[0][2].on, true);
  // Dashboards only on a server that has them on, after the Event explorer
  assert.deepEqual(labels(shellItems(null, "/dashboards", true))[0].slice(0, 3), ["Overview", "Event explorer", "Dashboards"]);
  assert.deepEqual(labels(shellItems(null, "/alerts", false, true))[0].at(-1), "Alerts", "Alerts only while the server has the alert check on");
});

test("no view imports a router: the package and the shell route through the Nav", () => {
  const offenders = [...files(SRC), ...files(SHELL)].filter((f) => /from "(next\/|react-router|@tanstack\/react-router)/.test(readFileSync(f, "utf8")));
  assert.deepEqual(offenders, []);
});

test("the shell routes through the browser's history", () => {
  assert.match(readFileSync(join(SHELL, "main.tsx"), "utf8"), /<HistoryNav>\{\(v\) => <App key=\{v\} \/>\}<\/HistoryNav>/);
});
