/*
  A sentence that names code values (environment variables, flags, headers): the words stay in the sans and each
  value is set in IBM Plex Mono, so the words can stay plain strings (tested, and shared with the hosted app) and
  still read as the type rule says: mono for values only, never for the words around them (QV-737).
*/
import { Fragment } from "react";

/** The code values the page sentences name: the longest first, so a header example isn't cut at "Authorization". */
const CODE = /(OTEL_EXPORTER_OTLP_HEADERS=\S*?<token>|Authorization: Bearer(?: <token>)?|TRAILBEAM_[A-Z_]+|--[a-z][a-z-]*[a-z]|\/metrics\b)/g;

/** The pieces of a sentence, each marked as code or words. */
export function codeParts(text: string): { text: string; code: boolean }[] {
  return text.split(CODE).filter(Boolean).map((t) => ({ text: t, code: new RegExp(`^${CODE.source}$`).test(t) }));
}

export function CodeText({ text }: { text: string }) {
  return <>{codeParts(text).map((p, i) => p.code
    ? <code key={i} className="font-mono text-[0.92em]">{p.text}</code>
    : <Fragment key={i}>{p.text}</Fragment>)}</>;
}
