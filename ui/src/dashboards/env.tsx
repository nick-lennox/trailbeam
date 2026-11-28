"use client";

import { createContext, useContext, type ReactNode } from "react";
import type { QueryLanguage } from "../model/query.ts";
import type { DashboardMeasures, EventData } from "./measures.ts";
import type { DashboardDef } from "./types.ts";

/*
  What the dashboard views need from the product they are in, given once through DashboardsEnvProvider: the
  measures engine and query language, the events, links into the product, and the changes it stores. The hosted
  app's actions are server actions; self-hosted Trailbeam's call the server's /api/dashboards.
*/

/** A change refused: the sentence to show. */
export type Refused = { ok: false; error: string };

export type DashboardActions = {
  create: (name: string, from: string, imported?: string) => Promise<{ ok: true; id: string } | Refused>;
  save: (id: string, patch: { name?: string; def?: DashboardDef }) => Promise<{ ok: true } | Refused>;
  star: (id: string, starred: boolean) => Promise<{ ok: true } | Refused>;
  lock: (id: string, lock: boolean) => Promise<{ ok: true; lockedBy: string | null } | Refused>;
  duplicate: (id: string) => Promise<{ ok: true; id: string } | Refused>;
  remove: (id: string) => Promise<{ ok: true } | Refused>;
};

/** The hosted app's sample data: what the queue flow widget draws for a sample queue. */
export type SampleFlow = {
  producers: { from: string; perMin: number }[];
  consumer: string;
  p95Ms: number;
  dlq: string | null;
  dlqDepth: number;
  sentPerMin: number;
  visible: number;
  inFlight: number;
};

export type DashboardsEnv = {
  engine: DashboardMeasures;
  lang: QueryLanguage;
  data: EventData;
  /** a path in the product: "/dashboards/x" becomes the link to it ("" is the Overview) */
  href: (path?: string) => string;
  actions: DashboardActions;
  /** the field paths seen in message bodies, for suggestions while typing a filter */
  bodyPaths?: string[];
  /** on the hosted app's sample data, a sample queue's flow */
  sampleFlow?: (queue: string) => SampleFlow | undefined;
  /** drawn while a dashboard follows the present on live data: what reloads the events (the hosted app's LiveRefresh) */
  liveRefresh?: ReactNode;
};

const Ctx = createContext<DashboardsEnv | null>(null);

export function DashboardsEnvProvider({ value, children }: { value: DashboardsEnv; children: ReactNode }) {
  return <Ctx.Provider value={value}>{children}</Ctx.Provider>;
}

export function useDashboardsEnv(): DashboardsEnv {
  const env = useContext(Ctx);
  if (!env) throw new Error("The dashboard views need a DashboardsEnvProvider around them.");
  return env;
}
