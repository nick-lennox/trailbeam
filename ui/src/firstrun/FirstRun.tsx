/*
  The first-run screen: what `/` shows while the store holds no span, from "install the package" to the first
  message. It draws its props and fetches nothing: the host polls /api/health (status.ts decides the status) and
  renders the Messages list below it once the first message is in. Drawn in the sky (theme.css). Design: QV-146.
*/
import { useEffect, useId, useRef, useState, type KeyboardEvent, type ReactNode, type RefObject } from "react";
import { announcement, isDefaultEndpoint, type FirstRunStatus } from "./status.ts";

export type Runtime = "python" | "node";
export const RUNTIMES: [Runtime, string][] = [["python", "Python"], ["node", "Node.js"]];
export const RUNTIME_KEY = "trailbeam.runtime";

const REPO = "https://github.com/nick-lennox/trailbeam";
const LINKS = {
  token: `${REPO}#access-token`,
  quickstart: `${REPO}#quickstart`,
  python: `${REPO}/blob/main/python/README.md`,
  node: `${REPO}/blob/main/node/README.md`,
  collector: `${REPO}/blob/main/collector/README.md`,
};

export const INSTALL: Record<Runtime, string> = { python: "pip install trailbeam", node: "npm install trailbeam" };

export const EXAMPLE: Record<Runtime, { caption: string; code: string }> = {
  python: {
    caption: "send_one.py",
    code: `import boto3
import trailbeam.boto

trailbeam.boto.install()  # once per process, before or after creating clients

sqs = boto3.client("sqs")
sqs.send_message(QueueUrl="<queue URL>", MessageBody="hello from Trailbeam")`,
  },
  node: {
    caption: "send-one.js",
    code: `const { SQSClient, SendMessageCommand } = require("@aws-sdk/client-sqs");
const trailbeam = require("trailbeam");

const sqs = trailbeam.instrument(new SQSClient({}));

async function main() {
  await sqs.send(new SendMessageCommand({ QueueUrl: "<queue URL>", MessageBody: "hello from Trailbeam" }));
  await trailbeam.forceFlush(); // a script that exits right away must flush, or its spans are lost
}
main();`,
  },
};

/** The environment block for row 02. The hash key is left out on purpose: a pasted `$(openssl rand -hex 32)` makes a new key per terminal. */
export function environment(endpoint: string, auth: "off" | "token"): string {
  const lines = [`export TRAILBEAM_OTLP_TRACES_ENDPOINT=${endpoint}`, "export OTEL_SERVICE_NAME=orders-worker"];
  if (auth === "token") lines.push('export OTEL_EXPORTER_OTLP_HEADERS="Authorization=Bearer%20<token>"');
  return lines.join("\n");
}

const C = ({ children }: { children: ReactNode }) => <code className="font-mono text-caption">{children}</code>;
const Ext = ({ href, children }: { href: string; children: ReactNode }) => <a href={href} className="text-dawn hover:underline">{children}</a>;

/* ---- code windows ---------------------------------------------------------------------------------------- */

const TOKEN = /("(?:[^"\\]|\\.)*")|((?:#|\/\/) .*$)|\b(import|const|require|async|function|await|new|export)\b|(<token>|<queue URL>)/gm;
const PLACEHOLDER = /(<token>|<queue URL>)/;

const Placeholder = ({ text }: { text: string }) => <span className="text-trail underline decoration-sky-ink-3 decoration-dotted underline-offset-4">{text}</span>;

/** The docs' syntax colors and no others: strings Trail, keywords Wait, comments Haze italic, placeholders marked. */
export function highlight(code: string): ReactNode[] {
  const out: ReactNode[] = [];
  let at = 0;
  for (const m of code.matchAll(TOKEN)) {
    if (m.index > at) out.push(code.slice(at, m.index));
    const k = out.length;
    if (m[1]) out.push(<span key={k} className="text-trail">{m[1].split(PLACEHOLDER).map((p, i) => (i % 2 ? <Placeholder key={i} text={p} /> : p))}</span>);
    else if (m[2]) out.push(<span key={k} className="italic text-sky-ink-3">{m[2]}</span>);
    else if (m[3]) out.push(<span key={k} className="text-wait">{m[3]}</span>);
    else out.push(<Placeholder key={k} text={m[4]} />);
    at = m.index + m[0].length;
  }
  if (at < code.length) out.push(code.slice(at));
  return out;
}

type CopyState = "idle" | "copied" | "select";

/** Copy: the exact text of its target. Where the Clipboard API is missing or refuses, it selects the text instead, never failing silently. */
function CopyButton({ text, target, name, onCopied }: { text: string; target: RefObject<HTMLElement | null>; name: string; onCopied: (msg: string) => void }) {
  const [state, setState] = useState<CopyState>("idle");
  useEffect(() => {
    if (state !== "copied") return;
    const t = setTimeout(() => setState("idle"), 2000);
    return () => clearTimeout(t);
  }, [state]);
  const select = () => {
    const el = target.current, sel = typeof window !== "undefined" ? window.getSelection() : null;
    if (el && sel) sel.selectAllChildren(el);
    setState("select");
    onCopied("Selected. Press Ctrl+C or Cmd+C to copy.");
  };
  const copy = () => {
    const clip = typeof navigator !== "undefined" ? navigator.clipboard : undefined;
    if (!clip?.writeText) return select();
    clip.writeText(text).then(() => { setState("copied"); onCopied("Copied to the clipboard."); }, select);
  };
  return (
    <button type="button" onClick={copy} aria-label={name}
      className="h-7 shrink-0 rounded-[5px] border border-sky-line px-2.5 text-[13px] text-sky-ink-2 hover:border-sky-ink hover:text-sky-ink">
      {state === "copied" ? "Copied" : state === "select" ? "Select and copy" : "Copy"}
    </button>
  );
}

/** A captioned code block with a Copy button. */
export function CodeWindow({ id, caption, code, copyName, onCopied }: { id?: string; caption: string; code: string; copyName: string; onCopied: (m: string) => void }) {
  const ref = useRef<HTMLPreElement>(null);
  return (
    <div id={id} className="mt-3 overflow-hidden rounded-[12px] border border-sky-line bg-sky-2">
      <div className="flex items-center justify-between gap-3 border-b border-sky-line px-5 py-2">
        <span className="font-mono text-caption text-sky-ink-3">{caption}</span>
        <CopyButton text={code} target={ref} name={copyName} onCopied={onCopied} />
      </div>
      <pre ref={ref} className="overflow-x-auto px-5 py-4 font-mono text-[13px] leading-[1.75] text-sky-ink"><code>{highlight(code)}</code></pre>
    </div>
  );
}

/* ---- the screen ------------------------------------------------------------------------------------------ */

function Row({ n, title, aside, children }: { n: string; title: string; aside?: ReactNode; children: ReactNode }) {
  return (
    <section className="grid grid-cols-[2rem_1fr] border-t border-sky-line py-6 md:grid-cols-[2.5rem_1fr]">
      <span className="pt-0.5 font-mono text-caption text-sky-ink-3" aria-hidden>{n}</span>
      <div className="min-w-0">
        <div className="flex flex-wrap items-center justify-between gap-3">
          <h2 className="text-[15px] font-medium text-sky-ink">{title}</h2>
          {aside}
        </div>
        {children}
      </div>
    </section>
  );
}

function RuntimeSwitch({ runtime, onRuntime, controls }: { runtime: Runtime; onRuntime: (r: Runtime) => void; controls: string }) {
  const tabs = useRef<(HTMLButtonElement | null)[]>([]);
  const key = (e: KeyboardEvent, i: number) => {
    const d = e.key === "ArrowRight" || e.key === "ArrowDown" ? 1 : e.key === "ArrowLeft" || e.key === "ArrowUp" ? -1 : 0;
    if (!d && e.key !== "Home" && e.key !== "End") return;
    e.preventDefault();
    const j = e.key === "Home" ? 0 : e.key === "End" ? RUNTIMES.length - 1 : (i + d + RUNTIMES.length) % RUNTIMES.length;
    onRuntime(RUNTIMES[j][0]);
    tabs.current[j]?.focus();
  };
  return (
    <div role="tablist" aria-label="Runtime" className="flex w-full rounded-full border border-sky-line bg-sky-2 p-0.5 text-sm md:w-auto">
      {RUNTIMES.map(([r, label], i) => (
        <button key={r} ref={(el) => { tabs.current[i] = el; }} type="button" role="tab" aria-selected={r === runtime} aria-controls={controls}
          tabIndex={r === runtime ? 0 : -1} onClick={() => onRuntime(r)} onKeyDown={(e) => key(e, i)}
          className={`flex-1 rounded-full px-3 py-1 md:flex-none ${r === runtime ? "bg-sky-line text-sky-ink" : "text-sky-ink-2 hover:text-sky-ink"}`}>{label}</button>
      ))}
    </div>
  );
}

const DOT: Record<FirstRunStatus["kind"], string> = {
  waiting: "border border-sky-ink-3 animate-wait motion-reduce:animate-none",
  "no-spans": "border border-sky-ink-3",
  ignored: "bg-ember",
  rejected: "bg-lost",
  unreachable: "bg-lost",
  received: "bg-trail",
};

function WaitingLine({ status, auth, version, announce }: { status: FirstRunStatus; auth: "off" | "token"; version: string; announce: string }) {
  const reason = (() => {
    switch (status.kind) {
      case "no-spans":
        return <>No spans have arrived in 2 minutes. Check that the service ran with <C>TRAILBEAM_OTLP_TRACES_ENDPOINT</C> set to an address it can reach from where it runs, and that it made an SQS call after <C>install</C>.{auth === "token" && <> If its log shows <C>401</C>, <C>OTEL_EXPORTER_OTLP_HEADERS</C> is missing or has an old token.</>}</>;
      case "ignored":
        return <>Trailbeam received <span className="font-mono">{status.ignored}</span> spans, but none came from the Trailbeam package, so it set them aside. That&rsquo;s expected for other OpenTelemetry spans. Check that <C>trailbeam.boto.install()</C> (Python) or <C>trailbeam.instrument()</C> (Node.js) ran before the SQS call.</>;
      case "rejected":
        return <>Trailbeam rejected <span className="font-mono">{status.rejected}</span> spans that don&rsquo;t match its span contract. The server log has a warning with the reason for each. This usually means the package and the server are on different versions. This server is <span className="font-mono">{version}</span>.</>;
      case "unreachable":
        return <>Can&rsquo;t reach Trailbeam&rsquo;s API. Trying again every 10 s.</>;
      default:
        return null;
    }
  })();
  const received = status.kind === "received" ? status.first : null;
  return (
    <div className="mt-6 flex gap-3" data-status={status.kind}>
      <span aria-hidden className={`mt-[0.55rem] size-1.5 shrink-0 rounded-full ${DOT[status.kind]}`} />
      <div className="min-w-0">
        <p className="sr-only" aria-live="polite">{announce}</p>
        <p className="text-[15px] text-sky-ink" aria-hidden>{received ? "First message received" : "Waiting for your first message"}</p>
        {received ? (
          <>
            <p className="mt-0.5 break-all font-mono text-caption text-sky-ink-2" aria-hidden>
              {received.message_id} · {received.source_queue} · {received.consumer ?? "sent, not received yet"}
            </p>
            <a href={`/message?id=${encodeURIComponent(received.message_id)}`}
              className="mt-3 inline-block rounded-full bg-sky-ink px-4 py-1.5 text-[15px] text-sky ring-offset-sky hover:bg-trail">Open its timeline</a>
          </>
        ) : reason ? (
          <p className="mt-0.5 max-w-[72ch] text-[15px] leading-[1.5] text-sky-ink-2" aria-hidden>{reason}</p>
        ) : (
          <p className="mt-0.5 font-mono text-caption text-sky-ink-3" aria-hidden>
            {status.kind === "waiting" && status.hidden ? "Paused while this tab is hidden"
              : `Checked ${status.kind === "waiting" && status.checkedAgoS !== null ? status.checkedAgoS : 0} s ago · no spans yet`}
          </p>
        )}
      </div>
    </div>
  );
}

export type FirstRunProps = {
  /** where services send: `${location.origin}/v1/traces` */
  endpoint: string;
  /** /api/health config.auth */
  auth: "off" | "token";
  status: FirstRunStatus;
  /** /api/health version */
  version: string;
  runtime: Runtime;
  onRuntime: (r: Runtime) => void;
  /** set when retention emptied the store: how long it keeps spans ("7 days"); the setup starts collapsed */
  retainedFor?: string | null;
};

export function FirstRun({ endpoint, auth, status, version, runtime, onRuntime, retainedFor = null }: FirstRunProps) {
  const id = useId();
  const received = status.kind === "received";
  const [open, setOpen] = useState(!retainedFor && !received);
  useEffect(() => { if (received) setOpen(false); }, [received]);
  const [copied, setCopied] = useState("");
  const endpointRef = useRef<HTMLOutputElement>(null);
  const label = RUNTIMES.find((r) => r[0] === runtime)![1];
  const example = EXAMPLE[runtime];
  const env = environment(endpoint, auth);
  const installId = `${id}-install`, exampleId = `${id}-example`;

  return (
    <div>
      <p className="text-[12px] text-sky-ink-3">Get started</p>
      <h1 className="mt-1 text-[1.875rem] leading-tight text-sky-ink">{retainedFor ? `No spans in the last ${retainedFor}` : "Send your first SQS message to Trailbeam"}</h1>
      <p className="mt-2 max-w-[36rem] text-[17px] leading-[1.5] text-sky-ink-2">
        {retainedFor
          ? `Trailbeam keeps spans for ${retainedFor} and has deleted older ones. New messages show up here as your services send them.`
          : "Add the package to one service, point it here, and make one SQS call. The message shows up on this page by itself."}
      </p>

      <WaitingLine status={status} auth={auth} version={version} announce={announcement(status)} />
      <p className="sr-only" aria-live="polite">{copied}</p>

      <div className="mt-6">
        {!open && (
          <div className="flex flex-wrap items-center gap-x-3 gap-y-1 border-t border-sky-line py-4 text-[15px]">
            <span className="text-sky-ink">Setup</span>
            <span className="text-sky-ink-3" aria-hidden>·</span>
            <span className="text-sky-ink-2">{label}</span>
            <span className="text-sky-ink-3" aria-hidden>·</span>
            <span className="min-w-0 break-all font-mono text-caption text-sky-ink-2">{endpoint}</span>
            <button type="button" onClick={() => setOpen(true)} aria-expanded={false} className="ml-auto text-dawn hover:underline">
              {received ? "Show setup again" : "Show setup"}
            </button>
          </div>
        )}
        <div className={`grid transition-[grid-template-rows] duration-[240ms] ease-out motion-reduce:transition-none ${open ? "grid-rows-[1fr]" : "grid-rows-[0fr]"}`} inert={!open} aria-hidden={!open || undefined}>
          <div className="min-h-0 overflow-hidden">
            <Row n="01" title="Install the package" aside={<RuntimeSwitch runtime={runtime} onRuntime={onRuntime} controls={`${installId} ${exampleId}`} />}>
              <CodeWindow id={installId} caption="shell" code={INSTALL[runtime]} copyName="Copy the install command" onCopied={setCopied} />
              <p className="mt-3 text-[15px] text-sky-ink-2">
                {runtime === "python" ? "Python 3.10 or later, with boto3." : <>Node.js 20 or later, with the AWS SDK v3 SQS client (<C>@aws-sdk/client-sqs</C>).</>}
              </p>
            </Row>

            <Row n="02" title="Point it at this Trailbeam">
              <p className="mt-2 text-[15px] text-sky-ink-2">The package sends its spans over OTLP/HTTP to this endpoint:</p>
              <div className="mt-3 flex flex-wrap items-center gap-2">
                <output ref={endpointRef} className="min-w-0 break-all rounded-full border border-sky-line bg-sky-2 px-4 py-1.5 font-mono text-caption text-sky-ink">{endpoint}</output>
                <CopyButton text={endpoint} target={endpointRef} name="Copy the endpoint" onCopied={setCopied} />
              </div>
              <p className="mt-2 max-w-[72ch] text-[13px] leading-[1.5] text-sky-ink-3">
                {isDefaultEndpoint(endpoint)
                  ? <>This is the package&rsquo;s default, so a service on this machine can skip <C>TRAILBEAM_OTLP_TRACES_ENDPOINT</C>.</>
                  : <>This is the address your browser used. A service in a container or on another host may need a different one, such as <C>http://host.docker.internal:4318/v1/traces</C> from a container on Docker Desktop.</>}
              </p>
              <p className="mt-4 max-w-[72ch] text-[15px] leading-[1.5] text-sky-ink-2" data-auth={auth}>
                {auth === "off"
                  ? <>This Trailbeam has no access token, so services send without one. Anyone who can reach this port can send spans and read every message. Keep it on <C>127.0.0.1</C> or a trusted network, or <Ext href={LINKS.token}>set a token</Ext>.</>
                  : <>This Trailbeam needs its access token on every export. It&rsquo;s the token you signed in with: the value of <C>TRAILBEAM_TOKEN</C>, or the contents of the file <C>TRAILBEAM_TOKEN_FILE</C> or <C>--token-file</C> names, on the machine that runs Trailbeam. Trailbeam doesn&rsquo;t show it here.</>}
              </p>
              <CodeWindow caption="shell" code={env} copyName="Copy the environment variables" onCopied={setCopied} />
              {auth === "token" && (
                <p className="mt-2 max-w-[72ch] text-[13px] leading-[1.5] text-sky-ink-3">
                  Replace <C>&lt;token&gt;</C> with the access token. <C>%20</C> is the space after <C>Bearer</C>, encoded as OpenTelemetry expects. The header goes to every OTLP exporter in this process.
                </p>
              )}
              <p className="mt-2 max-w-[72ch] text-[13px] leading-[1.5] text-sky-ink-3">
                <C>OTEL_SERVICE_NAME</C> is the name timelines show for this service. Before a second service joins, give every service on the same queues the same <C>TRAILBEAM_RECEIPT_HASH_KEY</C> (<C>openssl rand -hex 32</C>, generated once). Without it the package uses a published default key and logs a warning.
              </p>
            </Row>

            <Row n="03" title="Make one SQS call">
              <p className="mt-2 max-w-[72ch] text-[15px] leading-[1.5] text-sky-ink-2">
                Call <C>install</C> once, then use SQS as usual. A send is enough for a first message. A receive and a delete fill in the rest of its timeline.
              </p>
              <CodeWindow id={exampleId} caption={example.caption} code={example.code} copyName="Copy the example" onCopied={setCopied} />
              <p className="mt-2 max-w-[72ch] text-[13px] leading-[1.5] text-sky-ink-3">
                Replace <C>&lt;queue URL&gt;</C> with a queue you can send to. {runtime === "python" ? <>Spans go out in batches every few seconds, and any still waiting go out when the process exits.</> : <>In a long-running service you don&rsquo;t need <C>forceFlush</C>: spans go out in batches every few seconds.</>} On AWS Lambda, wrap the handler instead: <Ext href={LINKS.python}>Python</Ext> · <Ext href={LINKS.node}>Node.js</Ext>.
              </p>
            </Row>

            <p className="border-t border-sky-line pt-4 text-[13px] text-sky-ink-3">
              More: <Ext href={LINKS.quickstart}>the quickstart</Ext> · <Ext href={LINKS.python}>Lambda</Ext> · <Ext href={LINKS.collector}>the Collector</Ext>
            </p>
          </div>
        </div>
      </div>
    </div>
  );
}
