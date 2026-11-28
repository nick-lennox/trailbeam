---
section: Added
---

- **AWS figures in the queue APIs** (QV-589, part 3). With AWS reads on for a queue, `GET /api/queues/{queue}/now`
  goes by the queue's own settings from `GetQueueAttributes`: its visibility timeout, retention period,
  `maxReceiveCount` and dead-letter queue. It also gives SQS's own counts in `totals`, and in `uninstrumented` what
  the spans don't account for. Both used to be always null. `/now` and `/api/queues/{queue}/series` have a new
  `aws` field: what AWS says about the queue, with CloudWatch over the queue page's range on `/series`, and over the
  past `minutes` on `/now` when that is given. `/now` also has `live`, the queue's figures as the Overview's queue card
  shows them. On `/series`, `max_receive_count` is the queue's redrive policy's when it was read. The dashboards'
  queue measures (depth, age, headroom, time to drain) get their figures from AWS too. With reads off, the default,
  `aws.status` is `not_configured`, every answer is as before, and no AWS call is made. The pages show none of this yet.
