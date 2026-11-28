"use client";

import { useEffect, useState } from "react";

/** "12s ago", "3 min ago": the time since `ms`, kept current every second. For TopBar's `fresh` ("Updated 12s ago"). */
export function Since({ ms }: { ms: number }) {
  const [now, setNow] = useState(() => Date.now());
  useEffect(() => {
    const id = setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(id);
  }, []);
  return <time dateTime={new Date(ms).toISOString()}>{sinceText(now - ms)}</time>;
}

/** 0s ago, 59s ago, 1 min ago, 2 h ago */
export function sinceText(ms: number) {
  const s = Math.max(0, Math.round(ms / 1000));
  return s < 60 ? `${s}s ago` : s < 3600 ? `${Math.floor(s / 60)} min ago` : `${Math.floor(s / 3600)} h ago`;
}
