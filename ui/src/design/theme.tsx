"use client";

import { createContext, useContext, useEffect, useState } from "react";

/*
  The app's theme. Night is the default; the choice is remembered per browser. The provider puts
  data-app-theme on its element; design.css gives the night values inside that scope, so every
  component follows without knowing about the theme. Tooltip draws its note inside the same scope.
*/
export type AppTheme = "night" | "day";
const Ctx = createContext<{ theme: AppTheme; setTheme: (t: AppTheme) => void }>({ theme: "night", setTheme: () => {} });
export const useAppTheme = () => useContext(Ctx);

export function AppThemeProvider({ children, className }: { children: React.ReactNode; className?: string }) {
  const [theme, set] = useState<AppTheme>("night");
  useEffect(() => {
    try { if (localStorage.getItem("trailbeam-app-theme") === "day") set("day"); } catch {}
  }, []);
  const setTheme = (t: AppTheme) => {
    set(t);
    try { localStorage.setItem("trailbeam-app-theme", t); } catch {}
  };
  return (
    <Ctx.Provider value={{ theme, setTheme }}>
      <div data-app-theme={theme} className={className}>{children}</div>
    </Ctx.Provider>
  );
}
