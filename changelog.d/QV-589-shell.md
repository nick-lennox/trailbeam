---
section: Added
---

- **AWS figures on the pages, and a Settings page to turn them on** (QV-589, part 4). A new Settings page
  (`/settings`, admins only; anyone before accounts exist) has an AWS section: switch reads on, name the queues to
  read (one per line, `orders` or `payments-*`), and optionally a region and an AWS account ID. It says whether reads
  are on, where the server found its credentials, and what is wrong when they can't work, and it lists the three
  read-only permissions the credentials need. For a queue AWS was read for, the Overview's card has its depth, the
  oldest message's age, its settings and Lambda triggers, and its health from SQS and CloudWatch, as in the hosted
  app. Its queue page has its settings, the CloudWatch band, its Lambda triggers, a verdict from SQS and CloudWatch,
  and "In ‹queue› now" under the band. A queue that wasn't read says why in the server's words (reads off, the queue
  not named, or what AWS answered), with a link to the settings for admins. With reads off, the default, the pages
  are as before, and the queue page keeps "What Trailbeam can't see here".
