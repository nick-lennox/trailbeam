"use client";

import type { ComponentProps, ReactNode } from "react";
import { UiLink } from "./nav.tsx";

/*
  One button, six variants. Pick by surface and weight:
  day: primary (the one main action per view), secondary, ink, ghost
  night: inverse (main), inverse-outline, for the landing page's dark panels (bg-night) only. Inside the
  app the theme already makes the day variants dark, so an app page never needs these.
  With `href` it is a link, through the host's link component (nav.tsx).
*/
const VARIANT = {
  primary: "bg-brand text-on-brand shadow-glow hover:bg-brand-strong",
  secondary: "border border-line bg-surface text-ink shadow-card hover:border-line-strong",
  ink: "bg-ink text-canvas hover:bg-ink/85",
  ghost: "text-ink-2 hover:bg-sunken hover:text-ink",
  inverse: "bg-night-ink text-night hover:bg-white",
  "inverse-outline": "border border-white/15 text-night-ink hover:bg-white/5",
} as const;

const SIZE = {
  sm: "h-8 gap-1.5 px-3 text-small",
  md: "h-10 gap-2 px-4 text-small",
} as const;

export type ButtonVariant = keyof typeof VARIANT;

export type ButtonProps = {
  variant?: ButtonVariant;
  size?: keyof typeof SIZE;
  href?: string;
  children: ReactNode;
  className?: string;
} & Omit<ComponentProps<"button">, "className" | "children">;

export function Button({ variant = "secondary", size = "md", href, className = "", children, ...rest }: ButtonProps) {
  const cls = `inline-flex items-center justify-center whitespace-nowrap rounded-control font-medium transition-colors duration-150 ${VARIANT[variant]} ${SIZE[size]} ${className}`;
  if (href) return <UiLink href={href} className={cls}>{children}</UiLink>;
  return <button className={cls} {...rest}>{children}</button>;
}
