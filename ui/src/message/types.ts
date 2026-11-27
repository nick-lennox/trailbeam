/* The message page's tabs and links (MessagePage.tsx, in @trailbeam/ui/design), for the main entry. */
export type MessageTab = "timeline" | "body";

export type MessageHrefs = {
  /** the Messages list */
  messages: string;
  /** a queue's messages */
  queue: (name: string) => string;
  /** a tenant's messages; without it the tenant is not a link */
  tenant?: (name: string) => string;
  /** another message's timeline (the messages an SNS publish reached) */
  message?: (id: string) => string;
  /** this page with a tab open: the tabs' links, and Copy link */
  tab: (tab: MessageTab) => string;
};
