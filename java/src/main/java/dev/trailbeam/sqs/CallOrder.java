package dev.trailbeam.sqs;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The process's call-order counter (contract section 2a): one per class loader, which is one per process for
 * a normal application and one per webapp in an application server. {@link #tick()} is strictly increasing and
 * safe from any thread, so "A returned before B was issued" holds across threads, as the contract argues.
 */
public final class CallOrder {
  /** A tick together with the scope it was taken in, so ticks of two scopes are never mixed. */
  public static final class Tick {
    public final String scope;
    public final long value;

    Tick(String scope, long value) {
      this.scope = scope;
      this.value = value;
    }
  }

  private static final class Counter {
    final String scope = UUID.randomUUID().toString();
    final AtomicLong next = new AtomicLong();
  }

  private static volatile Counter counter = new Counter();

  private CallOrder() {}

  public static Tick tick() {
    Counter c = counter;
    return new Tick(c.scope, c.next.incrementAndGet());
  }

  public static String scope() {
    return counter.scope;
  }

  /**
   * Starts a new scope whose counter begins again at 1. For a Lambda SnapStart restore, where one snapshot
   * becomes many execution environments that must not share a scope (QV-312 report, section 3, point 6).
   */
  public static void newScope() {
    counter = new Counter();
  }
}
