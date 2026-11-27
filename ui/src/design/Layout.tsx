import type { ReactNode } from "react";

export function Container({ children, className = "", narrow = false }: { children: ReactNode; className?: string; narrow?: boolean }) {
  return <div className={`mx-auto px-6 ${narrow ? "max-w-5xl" : "max-w-6xl"} ${className}`}>{children}</div>;
}

/* Vertical rhythm: every landing section is py-24 (96px); tone switches the band color. */
export function Section({
  id, tone = "canvas", children, className = "",
}: { id?: string; tone?: "canvas" | "surface"; children: ReactNode; className?: string }) {
  const band = tone === "surface" ? "border-t border-line bg-surface" : "";
  return (
    <section id={id} className={`${band} ${className}`}>
      <Container className="py-24">{children}</Container>
    </section>
  );
}

export function Eyebrow({ children, night = false }: { children: ReactNode; night?: boolean }) {
  return <div className={`text-small font-medium ${night ? "text-brand-night-text" : "text-brand"}`}>{children}</div>;
}

/* Eyebrow → title → optional lead. Left-aligned by default; center only for hero and CTA. */
export function SectionHeader({
  eyebrow, title, lead, align = "left", night = false,
}: { eyebrow?: ReactNode; title: ReactNode; lead?: ReactNode; align?: "left" | "center"; night?: boolean }) {
  return (
    <div className={`max-w-2xl ${align === "center" ? "mx-auto text-center" : ""}`}>
      {eyebrow && <Eyebrow night={night}>{eyebrow}</Eyebrow>}
      <h2 className={`mt-2 font-serif text-title-sm text-balance md:text-title ${night ? "text-night-ink" : "text-ink"}`}>{title}</h2>
      {lead && <p className={`mt-4 text-lead text-pretty ${night ? "text-night-ink-2" : "text-ink-2"}`}>{lead}</p>}
    </div>
  );
}
