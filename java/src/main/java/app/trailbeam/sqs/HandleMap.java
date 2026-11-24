package app.trailbeam.sqs;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.LongSupplier;

/**
 * Receipt handle to message ID, for the message ID on settle spans (contract section 8). Thread-safe and
 * bounded: the oldest entries go first, and entries expire after 12 hours, the longest visibility timeout.
 */
final class HandleMap {
  static final long MAX_AGE_NANOS = 12L * 60 * 60 * 1_000_000_000L;
  static final int DEFAULT_MAX_ENTRIES = 100_000;

  private static final class Entry {
    final String messageId;
    final long at;

    Entry(String messageId, long at) {
      this.messageId = messageId;
      this.at = at;
    }
  }

  private final LinkedHashMap<String, Entry> map = new LinkedHashMap<>();
  private final int maxEntries;
  private final long maxAgeNanos;
  private final LongSupplier clock;

  HandleMap() {
    this(DEFAULT_MAX_ENTRIES, MAX_AGE_NANOS, System::nanoTime);
  }

  HandleMap(int maxEntries, long maxAgeNanos, LongSupplier clock) {
    this.maxEntries = maxEntries;
    this.maxAgeNanos = maxAgeNanos;
    this.clock = clock;
  }

  synchronized void put(String receiptHandle, String messageId) {
    long now = clock.getAsLong();
    map.remove(receiptHandle);
    map.put(receiptHandle, new Entry(messageId, now));
    Iterator<Map.Entry<String, Entry>> it = map.entrySet().iterator();
    int excess = map.size() - maxEntries;
    // Insertion order is time order, so the oldest and the expired entries are at the front.
    while (it.hasNext()) {
      Entry e = it.next().getValue();
      if (excess > 0) {
        it.remove();
        excess--;
      } else if (now - e.at > maxAgeNanos) {
        it.remove();
      } else {
        break;
      }
    }
  }

  synchronized String get(String receiptHandle) {
    Entry e = map.get(receiptHandle);
    if (e == null) {
      return null;
    }
    if (clock.getAsLong() - e.at > maxAgeNanos) {
      map.remove(receiptHandle);
      return null;
    }
    return e.messageId;
  }

  synchronized void remove(String receiptHandle) {
    map.remove(receiptHandle);
  }

  synchronized int size() {
    return map.size();
  }
}
