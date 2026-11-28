---
section: Changed
---

- **The Overview is the hosted app's** (QV-579). `/overview` now draws the shared `OverviewFlows` in the app's
  frame: every queue worst first, with why its messages didn't finish (from `/api/causes`), and the selected
  queue drawn large with its producers, consumers and dead-letter queue, their rates per minute, and "In ‹queue›
  now" (from `/api/queues/{queue}/now`). Health is from your services' spans alone, and says so. The queue's card
  keeps the old table's totals for the range: Sends, Receives, Deletes, visibility changes, the average body and
  receives on its DLQ. Copy link, the `/api/flows` JSON link and the footnote that counts come from spans and
  Trailbeam makes no AWS calls are kept. The range is chosen with the Event explorer's picker: any preset up to
  30 days, a typed range such as `45m` or `14:05 to 14:15`, or exact times (`range=<preset>` or `from=&to=`; the old
  page's `from_ms=&to_ms=` links still open). Copy link pins the exact window, and a fixed range isn't refreshed. The
  selected queue is in the URL (`flow=`), and the heading's counts agree with their nouns ("The queue is healthy").
