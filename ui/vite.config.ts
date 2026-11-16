/*
  Builds the shell (./shell) into the Python package, where Trailbeam serves it:
  /message is shell/index.html, and its files are under /ui/. The output is not committed: the
  container build and releases run `npm run build`; without it, Trailbeam serves its HTML timeline.

  npm run dev serves the shell on :5173 and sends /api to a Trailbeam on :4318.
*/
import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";

export default defineConfig({
  root: "shell",
  base: "/ui/",
  plugins: [react(), tailwindcss()],
  build: { outDir: "../../python/src/trailbeam/server/ui", emptyOutDir: true },
  server: { proxy: { "/api": "http://127.0.0.1:4318" } },
});
