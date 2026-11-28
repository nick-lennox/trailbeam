---
section: Changed
---

- **The Overview's flow map shows the messages behind each part, and reads more clearly** (QV-579). Select a
  producer, a consumer or the dead-letter queue and the messages it handled in the range are listed under it, from
  your services' spans (the Event explorer's events), and a row opens to that message's sequence diagram. The top
  bar says when the figures were read ("Updated 12s ago"). Rates are to one decimal everywhere ("8.0/min", not
  "8/min" beside "6.3/min"), and the rate beside a wire sits on a pad so the dashed wire no longer runs through it.
  The dead-letter node is labelled "DLQ" and shows the receives on it in the range. Long service names give way in
  the middle, so `trailbeam-demo-checkout-api` and `trailbeam-demo-orders-api` no longer both read "trailbeam-d…".
  The mark beside "No data on their health" is grey, not green, and the "Why messages didn't finish" heading wraps
  instead of being cut off.
