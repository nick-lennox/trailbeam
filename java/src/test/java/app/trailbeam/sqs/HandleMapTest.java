package app.trailbeam.sqs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class HandleMapTest {
  @Test
  void boundedOldestFirst() {
    HandleMap m = new HandleMap(2, Long.MAX_VALUE, System::nanoTime);
    m.put("a", "1");
    m.put("b", "2");
    m.put("c", "3");
    assertNull(m.get("a"));
    assertEquals("2", m.get("b"));
    assertEquals("3", m.get("c"));
    assertEquals(2, m.size());
  }

  @Test
  void expires() {
    AtomicLong now = new AtomicLong();
    HandleMap m = new HandleMap(10, 100, now::get);
    m.put("a", "1");
    now.set(50);
    m.put("b", "2");
    assertEquals("1", m.get("a"));
    now.set(101);
    assertNull(m.get("a"));
    assertEquals("2", m.get("b"));
    now.set(200);
    m.put("c", "3"); // drops the expired "b" on the way
    assertEquals(1, m.size());
  }

  @Test
  void removeOnDelete() {
    HandleMap m = new HandleMap();
    m.put("a", "1");
    m.remove("a");
    assertNull(m.get("a"));
  }
}
