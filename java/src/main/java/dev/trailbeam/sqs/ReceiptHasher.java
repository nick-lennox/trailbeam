package dev.trailbeam.sqs;

import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

/**
 * Keyed hash of receipt handles (contract section 7). Holds the key; never logs or exports it.
 *
 * <pre>
 * hmac   = lower_hex(HMAC-SHA256(key, receipt_handle))[0:32]
 * key_id = lower_hex(HMAC-SHA256(key, "trailbeam key id"))[0:8]
 * </pre>
 */
public final class ReceiptHasher {
  private static final char[] HEX = "0123456789abcdef".toCharArray();

  private final SecretKeySpec key;
  private final String keyId;

  public ReceiptHasher(String key) {
    this.key = new SecretKeySpec(key.getBytes(StandardCharsets.UTF_8), "HmacSHA256");
    this.keyId = hex("trailbeam key id", 8);
  }

  /** The first 32 lowercase hex characters of HMAC-SHA256(key, receiptHandle). */
  public String hash(String receiptHandle) {
    return hex(receiptHandle, 32);
  }

  public String keyId() {
    return keyId;
  }

  private String hex(String data, int chars) {
    byte[] mac;
    try {
      Mac m = Mac.getInstance("HmacSHA256");
      m.init(key);
      mac = m.doFinal(data.getBytes(StandardCharsets.UTF_8));
    } catch (GeneralSecurityException e) { // HmacSHA256 is required on every Java platform
      throw new IllegalStateException(e);
    }
    char[] out = new char[chars];
    for (int i = 0; i < chars; i++) {
      int b = mac[i / 2] & 0xff;
      out[i] = HEX[(i % 2 == 0) ? (b >>> 4) : (b & 0x0f)];
    }
    return new String(out);
  }

  @Override
  public String toString() { // never show the key
    return "ReceiptHasher(keyId=" + keyId + ")";
  }
}
