"use client";

import { createContext, useContext, type AnchorHTMLAttributes, type ComponentType, type ReactNode } from "react";

/*
  How the design system's links navigate. A view never imports a router: the host passes its own link
  component once, around the page. The hosted app passes next/link; the open-source shell can leave the
  default, a plain <a>, or pass one that uses history. (Row 0c of the one-UI plan grows this into the
  full Nav context: path, search parameters, push.)
*/
export type LinkProps = AnchorHTMLAttributes<HTMLAnchorElement> & { href: string; children?: ReactNode };
export type LinkComponent = ComponentType<LinkProps>;

const PlainLink: LinkComponent = ({ children, ...rest }) => <a {...rest}>{children}</a>;
const Ctx = createContext<LinkComponent>(PlainLink);

export function NavProvider({ Link, children }: { Link: LinkComponent; children: ReactNode }) {
  return <Ctx.Provider value={Link}>{children}</Ctx.Provider>;
}

/** The host's link component, or a plain <a> when there is no NavProvider. */
export const useLink = () => useContext(Ctx);

/** A link through the host's link component. */
export function UiLink(props: LinkProps) {
  const L = useLink();
  return <L {...props} />;
}
