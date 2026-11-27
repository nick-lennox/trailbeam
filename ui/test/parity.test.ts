/*
  The design parts look the same in the open-source shell and in the hosted app, in both themes (QV-656).
  Builds two stylesheets with Tailwind: the shell's (shell/styles.css), and the hosted app's (tailwindcss,
  design.css, the font variables next/font sets and its body's font-sans). Then renders the parts inside
  data-app-theme="night" and "day" and works out each element's font, size, colours and shadow from each
  stylesheet with a small cascade (the selectors Tailwind and these files emit: classes, tags, attributes,
  :root, descendant and child combinators; anything with a pseudo-class like :hover or :has is left out, the
  same in both). The two must agree element by element.
  Run: npm test
*/
import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { createElement as h, type ReactElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { compile } from "@tailwindcss/node";
import postcss, { type ChildNode, type Container } from "postcss";
import { Badge, Button, Card, Eyebrow, OutcomePill, PageHeader, SourceTag, StatCard, StatePill, type BadgeTone, type ButtonVariant, type Outcome, type StepSource } from "../src/design/index.ts";

const UI = new URL("../", import.meta.url).pathname;
const SHELL_CSS = readFileSync(`${UI}shell/styles.css`, "utf8");
// the hosted app's globals.css, as far as these parts go, and the variables next/font puts on <html>
const HOSTED_CSS = `@import "tailwindcss";
@import "./src/design/design.css";
:root { --font-newsreader: "Newsreader"; --font-instrument: "Instrument Sans"; --font-plex-mono: "IBM Plex Mono"; }`;

const PROPS = ["font-family", "font-size", "line-height", "font-weight", "color", "background-color", "border-color", "box-shadow"] as const;
const INHERITED = new Set(["font-family", "font-size", "line-height", "font-weight", "color"]);

// ── a small DOM from static markup ──────────────────────────────────────────
type El = { tag: string; attrs: Record<string, string>; classes: Set<string>; parent: El | null; children: El[]; text: string };
function parse(markup: string): El {
  const root: El = { tag: "#root", attrs: {}, classes: new Set(), parent: null, children: [], text: "" };
  let cur = root;
  for (const m of markup.matchAll(/<(\/?)([a-zA-Z][\w-]*)((?:\s+[\w:-]+(?:="[^"]*")?)*)\s*(\/?)>|([^<]+)/g)) {
    if (m[5] !== undefined) { cur.text += m[5]; continue; }
    if (m[1]) { cur = cur.parent ?? root; continue; }
    const attrs: Record<string, string> = {};
    for (const a of (m[3] ?? "").matchAll(/([\w:-]+)(?:="([^"]*)")?/g)) attrs[a[1]] = (a[2] ?? "").replace(/&quot;/g, '"').replace(/&amp;/g, "&");
    const el: El = { tag: m[2].toLowerCase(), attrs, classes: new Set((attrs.class ?? "").split(/\s+/).filter(Boolean)), parent: cur, children: [], text: "" };
    cur.children.push(el);
    if (!m[4] && !["path", "circle", "line", "rect", "polyline", "polygon", "br", "input", "img"].includes(el.tag)) cur = el;
  }
  return root;
}

// ── a cascade for the selectors these stylesheets use ───────────────────────
type Rule = { selector: string; decls: [string, string][]; layer: number; order: number };
const LAYERS = ["theme", "base", "components", "utilities"];

type Sheet = { rules: Rule[]; nonInherited: Set<string>; initial: Map<string, string> };
function rulesOf(css: string): Sheet {
  const rules: Rule[] = [], nonInherited = new Set<string>(), initial = new Map<string, string>();
  let order = 0;
  const walk = (node: Container, layer: number, selector: string | null) => {
    const decls: [string, string][] = [];
    node.each((child: ChildNode) => {
      if (child.type === "decl") {
        // `background: <one colour>` (the Tooltip's note) is its background-color
        const prop = child.prop === "background" && /^(var\([^()]*(\([^()]*\))?[^()]*\)|#[0-9a-f]+|[a-z]+)$/i.test(child.value.trim()) ? "background-color" : child.prop;
        decls.push([prop, child.value]);
      }
      else if (child.type === "rule") walk(child, layer, child.selector);
      else if (child.type === "atrule") {
        // Tailwind's properties layer is the fallback for a browser without @property; @property itself is read below
        if (child.name === "layer" && child.params.trim() === "properties") return;
        if (child.name === "layer" && child.nodes) walk(child, LAYERS.indexOf(child.params.trim()), selector);
        else if (child.name === "supports" && child.nodes) {
          // a rule's own @supports (Tailwind's color-mix with a token): a browser running the app supports it
          if (selector) child.each((d) => { if (d.type === "decl") decls.push([d.prop, d.value]); });
          else walk(child, layer, null);
        } else if (child.name === "property") {
          child.each((d) => {
            if (d.type === "decl" && d.prop === "inherits" && d.value.trim() === "false") nonInherited.add(child.params.trim());
            if (d.type === "decl" && d.prop === "initial-value") initial.set(child.params.trim(), d.value.trim());
          });
        }
        // @media (hover, reduced motion), @keyframes and @font-face don't change these properties at rest
      }
    });
    if (selector && decls.length) rules.push({ selector, decls, layer: layer < 0 ? LAYERS.length : layer, order: order++ });
  };
  walk(postcss.parse(css), LAYERS.length, null);
  return { rules, nonInherited, initial };
}

const unescape = (s: string) => s.replace(/\\(.)/g, "$1");
// one compound selector (no combinators): null when it uses something this cascade doesn't follow
function compound(s: string): ((el: El) => boolean) | null {
  const parts = s.match(/(\.(?:\\.|[\w-])+|\[[^\]]+\]|:[\w-]+(?:\([^)]*\))?|::[\w-]+|\*|[a-zA-Z][\w-]*)/g) ?? [];
  if (parts.join("") !== s) return null;
  const tests: ((el: El) => boolean)[] = [];
  for (const p of parts) {
    if (p === "*") continue;
    if (p.startsWith(".")) { const c = unescape(p.slice(1)); tests.push((el) => el.classes.has(c)); }
    else if (p.startsWith("[")) {
      const m = p.match(/^\[([\w-]+)(?:="([^"]*)")?\]$/);
      if (!m) return null;
      tests.push((el) => m[1] in el.attrs && (m[2] === undefined || el.attrs[m[1]] === m[2]));
    } else if (p === ":root") tests.push((el) => el.tag === "html");
    else if (p.startsWith(":")) return null;
    else { const t = p.toLowerCase(); tests.push((el) => el.tag === t); }
  }
  return (el) => tests.every((t) => t(el));
}
function specificity(s: string): number {
  const ids = (s.match(/#/g) ?? []).length, cls = (s.match(/\.(?:\\.|[\w-])+|\[[^\]]+\]|:(?!:)[\w-]+/g) ?? []).length;
  const tags = (s.replace(/\.(?:\\.|[\w-])+|\[[^\]]+\]|:[\w-]+/g, " ").match(/[a-zA-Z][\w-]*/g) ?? []).length;
  return ids * 1e6 + cls * 1e3 + tags;
}
function matches(selector: string, el: El): number | null {
  let best: number | null = null;
  for (const one of selector.split(/,(?![^(]*\))/).map((x) => x.trim())) {
    const steps = one.replace(/\s*>\s*/g, " > ").split(/\s+/);
    const fns = steps.map((st) => (st === ">" ? ">" : compound(st)));
    if (fns.includes(null)) continue;
    const at = (i: number, e: El | null): boolean => {
      if (!e || e.tag === "#root") return false;
      if (!(fns[i] as (el: El) => boolean)(e)) return false;
      if (i === 0) return true;
      if (fns[i - 1] === ">") return at(i - 2, e.parent);
      for (let a = e.parent; a && a.tag !== "#root"; a = a.parent) if (at(i - 1, a)) return true;
      return false;
    };
    if (at(fns.length - 1, el)) best = Math.max(best ?? -1, specificity(one));
  }
  return best;
}

type Computed = Map<string, string>;
function cascade(css: Sheet, el: El, parent: Computed | null): Computed {
  const won = new Map<string, { key: [number, number, number]; value: string }>();
  for (const r of css.rules) {
    const sp = matches(r.selector, el);
    if (sp === null) continue;
    const key: [number, number, number] = [r.layer, sp, r.order];
    for (const [prop, value] of r.decls) {
      const prev = won.get(prop);
      if (!prev || key[0] > prev.key[0] || (key[0] === prev.key[0] && (key[1] > prev.key[1] || (key[1] === prev.key[1] && key[2] >= prev.key[2])))) won.set(prop, { key, value });
    }
  }
  const out: Computed = new Map();
  if (parent) for (const [k, v] of parent) if ((k.startsWith("--") && !css.nonInherited.has(k)) || INHERITED.has(k)) out.set(k, v);
  for (const [k, v] of css.initial) if (css.nonInherited.has(k) || !parent) out.set(k, v);
  // the element's own custom properties, each resolved where it is set (as the browser does); `initial` leaves one
  // unset, so a var() that reads it takes its fallback
  const own = new Map([...won].filter(([p]) => p.startsWith("--")).map(([p, { value }]) => [p, value.trim()]));
  for (const [p, v] of own) if (v === "initial") { own.delete(p); out.delete(p); }
  const resolving = new Set<string>();
  const lookup = (name: string): string | undefined => {
    if (!own.has(name)) return out.get(name);
    if (resolving.has(name)) return "<cycle>";
    resolving.add(name);
    const v = sub(own.get(name)!);
    resolving.delete(name);
    own.delete(name);
    out.set(name, v);
    return v;
  };
  const sub = (v: string): string => {
    let s = v;
    for (;;) {
      const i = s.lastIndexOf("var(");
      if (i < 0) return s.trim();
      let d = 0, j = i + 4;
      for (; j < s.length; j++) { if (s[j] === "(") d++; else if (s[j] === ")") { if (d === 0) break; d--; } }
      const inner = s.slice(i + 4, j), comma = inner.indexOf(",");
      const name = (comma < 0 ? inner : inner.slice(0, comma)).trim();
      const fallback = comma < 0 ? undefined : inner.slice(comma + 1);
      const got = lookup(name) ?? (fallback !== undefined ? sub(fallback) : "<unset>");
      s = s.slice(0, i) + got + s.slice(j + 1);
    }
  };
  for (const name of [...own.keys()]) lookup(name);
  for (const p of PROPS) {
    const w = won.get(p)?.value;
    if (w !== undefined) out.set(p, w === "inherit" ? parent?.get(p) ?? "<initial>" : w === "currentColor" || w === "currentcolor" ? "currentcolor" : sub(w));
    else if (!INHERITED.has(p)) out.set(p, p === "border-color" ? "currentcolor" : p === "box-shadow" ? "none" : "transparent");
  }
  if (out.get("border-color") === "currentcolor") out.set("border-color", out.get("color") ?? "<initial>");
  return out;
}

async function stylesheet(input: string, candidates: string[]) {
  const c = await compile(input, { base: UI + (input === SHELL_CSS ? "shell/" : ""), onDependency() {} });
  return rulesOf(c.build(candidates));
}

// each element of the part (not inside an icon's svg), with what the two stylesheets give it
function computeAll(css: Awaited<ReturnType<typeof stylesheet>>, markup: string, bodyClass: string) {
  const doc = parse(`<html><body class="${bodyClass}">${markup}</body></html>`);
  const rows: { path: string; style: Record<string, string> }[] = [];
  const visit = (el: El, parent: Computed | null, path: string, inPart: boolean) => {
    const c = cascade(css, el, parent);
    const here = `${path}>${el.tag}${el.classes.size ? "." + [...el.classes].slice(0, 3).join(".") : ""}`;
    if (inPart) rows.push({ path: here, style: Object.fromEntries(PROPS.map((p) => [p, c.get(p) ?? "<initial>"])) });
    if (el.tag === "svg") return;
    const scope = "data-app-theme" in el.attrs;
    el.children.forEach((ch, i) => visit(ch, c, scope ? "" : `${here}[${i}]`, inPart || scope));
  };
  visit(doc.children[0], null, "", false);
  return rows;
}

const PARTS: [string, ReactElement][] = [
  ...(["primary", "secondary", "ink", "ghost"] as ButtonVariant[]).map((v): [string, ReactElement] => [`Button ${v}`, h(Button, { variant: v, children: "Redrive" })]),
  ...(["neutral", "outline", "brand", "good", "warning", "serious", "critical"] as BadgeTone[]).map((t): [string, ReactElement] => [`Badge ${t}`, h(Badge, { tone: t, dot: true, children: "Label" })]),
  ...(["deleted", "in_flight", "retrying", "dead_lettered", "stuck", "filtered"] as Outcome[]).map((o): [string, ReactElement] => [`OutcomePill ${o}`, h(OutcomePill, { outcome: o })]),
  ["StatePill processing", h(StatePill, { m: { state: "processing", in_dead_letter_queue: false, deliveries: 1 } })],
  ["StatePill in the DLQ", h(StatePill, { m: { state: "x", in_dead_letter_queue: true, deliveries: 2 } })],
  ...(["otel", "package", "cloudtrail", "function_logs", "inferred"] as StepSource[]).map((s): [string, ReactElement] => [`SourceTag ${s}`, h(SourceTag, { source: s })]),
  ["StatCard", h(StatCard, { label: "Dead-lettered", value: 12, hint: "in the last hour", tone: "critical" })],
  ["Card", h(Card, { children: "A card" })],
  ["Eyebrow", h(Eyebrow, { children: "Queues" })],
  ["PageHeader", h(PageHeader, { title: "Queues", subtitle: "All of them" })],
  // the Tooltip's note as it renders once pointed at (Tooltip.tsx)
  ["Tooltip note", h("span", { role: "tooltip", className: "ui-tooltip" }, "Seen in CloudTrail, about a minute late")],
];

for (const theme of ["night", "day"] as const) {
  test(`the design parts draw the same in the shell and the hosted app (${theme})`, async () => {
    const markup = Object.fromEntries(PARTS.map(([name, el]) => [name, renderToStaticMarkup(h("div", { "data-app-theme": theme }, el))]));
    const candidates = [...new Set([...Object.values(markup).join(" ").matchAll(/class="([^"]*)"/g)].flatMap((m) => m[1].split(/\s+/)).concat("min-h-screen", "font-sans"))];
    const [shell, hosted] = await Promise.all([stylesheet(SHELL_CSS, candidates), stylesheet(HOSTED_CSS, candidates)]);
    for (const [name, m] of Object.entries(markup)) {
      const a = computeAll(shell, m, ""), b = computeAll(hosted, m, "min-h-screen font-sans");
      assert.ok(a.length > 0);
      assert.deepEqual(a, b, `${name} under data-app-theme="${theme}"`);
      for (const row of a) for (const v of Object.values(row.style)) assert.doesNotMatch(v, /<unset>|<cycle>/, `${name}: ${row.path}`);
    }
  });
}

test("inside a theme the parts are in Instrument Sans at the design sizes, by day in the day colours", async () => {
  const m = renderToStaticMarkup(h("div", { "data-app-theme": "day" }, h(Button, { variant: "primary", children: "Go" }), h(Badge, { children: "x" }), h("span", { className: "ui-tooltip" }, "n")));
  const css = await stylesheet(SHELL_CSS, [...new Set([...m.matchAll(/class="([^"]*)"/g)].flatMap((x) => x[1].split(/\s+/)))]);
  const [button, badge, note] = computeAll(css, m, "");
  assert.match(button.style["font-family"], /^"Instrument Sans"/);
  assert.equal(button.style["background-color"], "#5b4fe0");
  assert.equal(button.style.color, "#ffffff");
  assert.equal(badge.style["font-size"], "0.8125rem");
  assert.deepEqual([note.style["font-family"].split(",")[0], note.style["font-size"], note.style["background-color"]], ['"Instrument Sans"', "0.8125rem", "#ffffff"]);
});

test("outside a theme the shell's own views keep their look (Newsreader on the sky, Tailwind's xs)", async () => {
  const m = renderToStaticMarkup(h("div", null, h("span", { className: "text-xs text-ink bg-surface font-sans" }, "x"), h("p", null, "y")));
  const css = await stylesheet(SHELL_CSS, ["text-xs", "text-ink", "bg-surface", "font-sans"]);
  const doc = parse(`<html><body>${m}</body></html>`);
  const html = cascade(css, doc.children[0], null), body = cascade(css, doc.children[0].children[0], html);
  const div = cascade(css, doc.children[0].children[0].children[0], body);
  const span = cascade(css, doc.children[0].children[0].children[0].children[0], div);
  const p = cascade(css, doc.children[0].children[0].children[0].children[1], div);
  assert.match(p.get("font-family")!, /^"Newsreader"/);
  assert.equal(p.get("color"), "#f6f1e7");
  assert.match(span.get("font-family")!, /^-apple-system/);
  assert.deepEqual([span.get("font-size"), span.get("color"), span.get("background-color")], ["0.75rem", "#f6f1e7", "#12153a"]);
});
