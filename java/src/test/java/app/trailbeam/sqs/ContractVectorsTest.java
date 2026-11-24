package app.trailbeam.sqs;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

/** The receipt-hash vectors of contract section 7, with the standard library's HMAC. */
class ContractVectorsTest {
  static JsonNode vectors() throws Exception {
    return new ObjectMapper().readTree(Path.of("..", "contract", "span-contract-vectors.json").toFile());
  }

  @Test
  void receiptHandleHash() throws Exception {
    JsonNode d = vectors();
    int n = 0;
    for (JsonNode v : d.get("vectors")) {
      ReceiptHasher h = new ReceiptHasher(v.get("key").asText());
      assertEquals(v.get("hmac").asText(), h.hash(v.get("receipt_handle").asText()), v.get("name").asText());
      assertEquals(v.get("key_id").asText(), h.keyId(), v.get("name").asText());
      n++;
    }
    assertEquals(true, n >= 3);
    assertEquals("b5e633f7", new ReceiptHasher(d.get("default_key").asText()).keyId());
  }

  @Test
  void hasherNeverShowsTheKey() {
    assertEquals(false, new ReceiptHasher("a-secret-key-0123456789").toString().contains("secret"));
  }
}
