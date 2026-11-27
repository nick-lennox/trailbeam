---
section: Changed
---

- **The Event explorer at `/events` is the hosted app's, and lists inferred events too** (QV-580, `@trailbeam/ui` 0.1.0-dev.23). The page now
  draws the shared explorer from `@trailbeam/ui/explorer` in the app frame, with the hosted app's filters, columns,
  time picker, CSV and PDF export and `body.*` search. It reads `GET /api/timeline-events` for the range you choose,
  so an expired visibility timeout, a move to the dead-letter queue and a Lambda invocation with no outcome are
  listed beside the recorded steps, with each message's tenant and body size. Before, it listed recorded steps
  only, from `GET /api/events`, and said bodies could not be searched. The range is picked or typed (45m, 2d,
  14:05 to 14:15, or exact times) and is in the link; Live follows a range that ends now every 15 seconds.
  Links from the old page (`range=24h`, `from_ms` and `to_ms`, `kind:` queries) still open what they named.
  `@trailbeam/ui` no longer exports the old explorer (`StepHistogram`, `parseQuery`, `EventPage` and the rest of
  `src/events`).
