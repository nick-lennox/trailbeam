/*
  The design rules every screen keeps, checked on the source of the shared views (src) and the open-source shell
  (shell), and the words-versus-values split of the mono font. Hover notes are the app's Tooltip on the specific mark, never the browser's native `title`
  (QV-588): a `title` attribute on a DOM element, or on a component that passes it through to one, fails here.
  A `title` prop of our own components (Panel, PageHeader, Chart, ...) is a heading, and `<title>` in an SVG is
  its accessible name; both stay.
  Run: npm test
*/
import { test } from "node:test";
import assert from "node:assert/strict";
import { readdirSync, readFileSync, statSync } from "node:fs";
import { join, relative } from "node:path";
import ts from "typescript";
import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { Rows } from "../src/now/MessageSequence.tsx";

const UI = new URL("../", import.meta.url).pathname;
/* our components that hand `title` on to the DOM element they draw */
const PASS_THROUGH = new Set(["UiLink"]);
/* not fixed here: views being deleted by their own issue, the legacy Overview drawing (QV-710) and the old Needs
   attention page behind /stuck (QV-708); and MessagesTable, which neither product draws and which is in the main
   entry, so it cannot import the design entry's Tooltip (test/server-import.test.ts) */
const RETIRING = new Set(["src/overview/Overview.tsx", "src/overview/FlowDrawing.tsx", "src/causes/NeedsAttention.tsx", "src/messages/Messages.tsx"]);

function tsxFiles(dir: string): string[] {
  return readdirSync(dir).flatMap((f) => {
    const p = join(dir, f);
    return statSync(p).isDirectory() ? tsxFiles(p) : p.endsWith(".tsx") ? [p] : [];
  });
}

/** "file:line <tag>" for each native title in one file. */
function nativeTitles(path: string, text = readFileSync(path, "utf8")): string[] {
  const src = ts.createSourceFile(path, text, ts.ScriptTarget.Latest, true, ts.ScriptKind.TSX);
  const out: string[] = [];
  const visit = (n: ts.Node) => {
    if (ts.isJsxAttribute(n) && n.name.getText(src) === "title") {
      const tag = (n.parent.parent as ts.JsxOpeningLikeElement).tagName.getText(src);
      if (/^[a-z]/.test(tag) || PASS_THROUGH.has(tag)) {
        out.push(`${relative(UI, path)}:${src.getLineAndCharacterOfPosition(n.getStart(src)).line + 1} <${tag}>`);
      }
    }
    ts.forEachChild(n, visit);
  };
  visit(src);
  return out;
}

test("no screen uses the browser's native title for a hover note", () => {
  const files = [...tsxFiles(join(UI, "src")), ...tsxFiles(join(UI, "shell"))].filter((p) => !RETIRING.has(relative(UI, p)));
  assert.ok(files.length > 50, "the check reads the views and the shell");
  assert.deepEqual(files.flatMap((p) => nativeTitles(p)), []);
});

test("no screen draws an arrow icon: a sort mark is a chevron, and a link says where it goes in words", () => {
  const files = [...tsxFiles(join(UI, "src")), ...tsxFiles(join(UI, "shell"))].filter((p) => !RETIRING.has(relative(UI, p)));
  const arrows = files.flatMap((p) => [...readFileSync(p, "utf8").matchAll(/import\s*\{([^}]*)\}\s*from\s*"lucide-react"/g)]
    .flatMap((m) => m[1].split(",").map((n) => n.trim()).filter((n) => /^Arrow[A-Z]/.test(n)).map((n) => `${relative(UI, p)} ${n}`)));
  assert.deepEqual(arrows, []);
});

const PROBE = `export const Probe = () => (
  <div>
    <span title="a native hover note">x</span>
    <Panel title="A heading">y</Panel>
    <button
      title={"on its own line"}>z</button>
    <svg><title>Accessible name</title></svg>
    <UiLink href="/" title="passed on to the a">w</UiLink>
  </div>
);`;

test("the native title check finds a title on a DOM element and on a pass-through link, and nowhere else", () => {
  assert.deepEqual(nativeTitles(join(UI, "probe.tsx"), PROBE), ["probe.tsx:3 <span>", "probe.tsx:6 <button>", "probe.tsx:8 <UiLink>"]);
});

test("in a list of attributes, an attribute's own name is in mono and a label in words is not (QV-588)", () => {
  const out = renderToStaticMarkup(createElement(Rows, { mono: true, rows: [["MessageId", "m-1"], ["Body size", "12 B"], ["RedrivePolicy dead-letter queue", "orders-dlq"], ["Tenant", "acme"]] }));
  const dts = [...out.matchAll(/<dt class="([^"]*)">([^<]*)<\/dt>/g)].map((m) => [m[2], m[1].includes("font-mono")]);
  assert.deepEqual(dts, [["MessageId", true], ["Body size", false], ["RedrivePolicy dead-letter queue", false], ["Tenant", false]]);
});
