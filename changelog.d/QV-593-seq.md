---
section: Changed
---

- **The server records when each span reached it** (QV-593, for triage). A new column `spans.seq` (SQLite schema 29,
  Postgres schema 23) holds the server's own arrival time for every span stored from now on, so triage can count the
  failures that arrived after a cause was resolved or ignored, by the server's clock and not the sending host's.
  Spans stored before the upgrade have no arrival time and never count as new failures. The upgrade adds the column
  without rewriting any row, and nothing on the pages or in the API changes yet.
