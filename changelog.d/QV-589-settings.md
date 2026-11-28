---
section: Added
---

- **AWS settings, off by default** (QV-589, part 2). A new `/api/settings/aws` (admins only) turns AWS reads on and
  names the queues to read, by exact name or a pattern such as `orders-*`, with an optional region and AWS account
  ID. Reads are on only when they are switched on and at least one queue is named. Until then the server makes no
  AWS call, looks for no credentials and never loads boto3. The credentials are boto3's own (environment variables,
  a profile, an instance or task role) and Trailbeam never stores them. The settings are kept in a new
  `server_settings` table (SQLite schema 31, Postgres schema 25), so every replica of one store shares them. The
  reader behind the coming AWS figures keeps each read for a short while, as the hosted app does: queue attributes
  15 s, CloudWatch metrics a minute (`GetMetricData` is billed), Lambda triggers 5 minutes. `/api/health` has a new
  `config.aws` field, and `trailbeam doctor` has a new "AWS reads" line that fails when reads are on but boto3 or
  the credentials are missing. No page shows AWS figures yet.
