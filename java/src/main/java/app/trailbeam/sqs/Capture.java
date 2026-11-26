package app.trailbeam.sqs;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Message contents (contract section 15, revision 3.2): body size, tenant, opt-in body capture with redaction,
 * and Lambda error text.
 *
 * <p>This mirrors {@code python/src/trailbeam/_capture.py} rule for rule, and is tested against the same
 * {@code capture} vectors in {@code contract/span-contract-vectors.json}. Nothing here makes an SQS call:
 * everything is read from the parameters and responses of calls the application made, or from the Lambda event.
 */
final class Capture {
  private Capture() {}

  // -- statuses (section 15.4) -------------------------------------------------------------------

  static final String STATUS_CAPTURED = "captured";
  static final String STATUS_OFF = "off";
  static final String STATUS_MD5_MISMATCH = "md5_mismatch";
  static final String STATUS_REDACTION_FAILED = "redaction_failed";
  static final String STATUS_EXCEEDS_LIMIT = "exceeds_capture_limit";
  static final String STATUS_OFFLOADED = "offloaded_to_s3";

  static final String REDACTED = "[REDACTED]";
  static final int DEFAULT_MAX_BODY_BYTES = 16 * 1024;
  static final int HARD_MAX_BODY_BYTES = 256 * 1024;
  static final int MAX_ERROR_MESSAGE_CHARS = 1024;
  static final int MAX_TENANT_CHARS = 256;
  static final int MAX_JSON_DEPTH = 100;
  static final String S3_POINTER_CLASS = "software.amazon.payloadoffloading.PayloadS3Pointer";
  /** Where the SQS Extended Client puts the offloaded payload's size, current name first (section 15.2). */
  static final List<String> OFFLOADED_SIZE_ATTRIBUTES =
      Collections.unmodifiableList(Arrays.asList("ExtendedPayloadSize", "SQSLargePayloadSize"));
  private static final Pattern SIZE_RE = Pattern.compile("[0-9]{1,15}");

  static final List<String> DEFAULT_REDACT_KEYS = Collections.unmodifiableList(Arrays.asList(
      "password", "passwd", "secret", "token", "access_token", "refresh_token", "authorization",
      "api_key", "apikey", "ssn", "card_number", "cardnumber", "cvv", "cvc"));

  /** Built-in text patterns (section 15.3), the same expressions as Python's and Node.js's: ASCII classes only. */
  static final Map<String, Pattern> PATTERNS;

  static {
    Map<String, Pattern> p = new LinkedHashMap<>();
    p.put("aws_access_key", Pattern.compile("(?<![A-Z0-9])(?:AKIA|ASIA)[A-Z0-9]{16}(?![A-Z0-9])"));
    p.put("bearer", Pattern.compile("(?<![A-Za-z])[Bb]earer [A-Za-z0-9._~+/=-]+"));
    p.put("email", Pattern.compile("[A-Za-z0-9._%+-]+@[A-Za-z0-9-]+(?:\\.[A-Za-z0-9-]+)*\\.[A-Za-z]{2,}"));
    p.put("long_number", Pattern.compile("(?<![0-9])[0-9](?:[ -]?[0-9]){8,}(?![0-9])"));
    PATTERNS = Collections.unmodifiableMap(p);
  }

  private static final Pattern KEY_RE = Pattern.compile("[A-Za-z0-9_.-]+");

  static final String ENV_TENANT_ATTRIBUTE = "TRAILBEAM_TENANT_ATTRIBUTE";
  static final String ENV_TENANT_BODY_PATH = "TRAILBEAM_TENANT_BODY_PATH";
  static final String ENV_BODY_QUEUES = "TRAILBEAM_CAPTURE_BODY_QUEUES";
  static final String ENV_ERROR_QUEUES = "TRAILBEAM_CAPTURE_ERROR_QUEUES";
  static final String ENV_REDACT_KEYS = "TRAILBEAM_REDACT_KEYS";
  static final String ENV_REDACT_PATTERNS = "TRAILBEAM_REDACT_PATTERNS";
  static final String ENV_MAX_BODY_BYTES = "TRAILBEAM_CAPTURE_MAX_BYTES";

  /** Redaction could not run: the body (or error text) is not exported. */
  static final class RedactionException extends Exception {
    RedactionException(String message) {
      super(message, null, false, false);
    }
  }

  /** The input is not one strict JSON document. */
  private static final class NotJson extends Exception {
    static final NotJson INSTANCE = new NotJson();

    private NotJson() {
      super("not JSON", null, false, false);
    }
  }

  // -- config (section 15.1) ---------------------------------------------------------------------

  /** What the package reads from message contents. Off unless configured. */
  static final class Config {
    final String tenantAttribute;
    final String tenantBodyPath;
    final Set<String> bodyQueues;
    final Set<String> errorQueues;
    final List<String> redactKeys;
    final List<String> redactPatterns;
    final int maxBodyBytes;
    /**
     * Why the redaction settings were refused, or null. Python and Node.js refuse them at install time; this
     * package has no install call (the SDK loads it), and its hooks never throw into the application, so it
     * fails closed instead: no body and no error text is exported, and a listed queue's bodies say
     * {@code redaction_failed}.
     */
    final String invalid;

    private final Pattern keyValue;
    private final List<Pattern> patterns;
    private final Set<String> names;
    private final Set<String> paths;

    Config(String tenantAttribute, String tenantBodyPath, Set<String> bodyQueues, Set<String> errorQueues,
           List<String> extraRedactKeys, List<String> redactPatterns, Integer maxBodyBytes) {
      this.tenantAttribute = blankToNull(tenantAttribute);
      this.tenantBodyPath = blankToNull(tenantBodyPath);
      this.bodyQueues = Collections.unmodifiableSet(new HashSet<>(bodyQueues));
      this.errorQueues = Collections.unmodifiableSet(new HashSet<>(errorQueues));
      Set<String> keys = new LinkedHashSet<>(DEFAULT_REDACT_KEYS);
      keys.addAll(extraRedactKeys);
      this.redactKeys = Collections.unmodifiableList(new ArrayList<>(keys));
      this.redactPatterns = Collections.unmodifiableList(new ArrayList<>(
          redactPatterns == null ? new ArrayList<>(PATTERNS.keySet()) : redactPatterns));
      this.maxBodyBytes = maxBodyBytes == null || maxBodyBytes == 0 ? DEFAULT_MAX_BODY_BYTES : maxBodyBytes;

      String bad = null;
      for (String k : this.redactKeys) {
        if (!KEY_RE.matcher(k).matches()) {
          bad = "redact key '" + k + "' may contain only A-Z a-z 0-9 _ . -";
          break;
        }
      }
      if (bad == null) {
        for (String p : this.redactPatterns) {
          if (!PATTERNS.containsKey(p)) {
            bad = "unknown redact pattern '" + p + "'; known: " + String.join(", ", PATTERNS.keySet());
            break;
          }
        }
      }
      if (bad == null && !(this.maxBodyBytes > 0 && this.maxBodyBytes <= HARD_MAX_BODY_BYTES)) {
        bad = "the capture limit must be 1 to " + HARD_MAX_BODY_BYTES + " bytes";
      }
      this.invalid = bad;

      Set<String> n = new HashSet<>();
      Set<String> p = new HashSet<>();
      List<String> alternatives = new ArrayList<>();
      for (String k : this.redactKeys) {
        if (k.indexOf('.') < 0) {
          n.add(asciiLower(k));
          alternatives.add(Pattern.quote(asciiLower(k)));
        } else {
          p.add(asciiLower(k));
        }
      }
      this.names = n;
      this.paths = p;
      // CASE_INSENSITIVE without UNICODE_CASE folds ASCII only, like Python's re.IGNORECASE | re.ASCII.
      this.keyValue = bad != null || alternatives.isEmpty() ? null : Pattern.compile(
          "(?<![A-Za-z0-9_])(" + String.join("|", alternatives)
              + ")([\"']?[ \\t]*[:=][ \\t]*)(\"[^\"]*\"|'[^']*'|[^ \\t\\r\\n,;&]+)",
          Pattern.CASE_INSENSITIVE);
      List<Pattern> res = new ArrayList<>();
      if (bad == null) {
        for (String name : this.redactPatterns) {
          res.add(PATTERNS.get(name));
        }
      }
      this.patterns = res;
    }

    /** No tenant, no capture. */
    static Config off() {
      return new Config(null, null, Collections.emptySet(), Collections.emptySet(), Collections.emptyList(), null,
          null);
    }

    /** Section 15.1, from environment variables (the Java package has no install call to pass options to). */
    static Config fromEnv(Function<String, String> env) {
      String patterns = env.apply(ENV_REDACT_PATTERNS);
      Integer max = null;
      String rawMax = env.apply(ENV_MAX_BODY_BYTES);
      boolean badMax = false;
      if (rawMax != null && !rawMax.isEmpty()) {
        try {
          max = Integer.parseInt(rawMax.trim());
        } catch (NumberFormatException e) {
          badMax = true;
        }
      }
      if (badMax) {
        max = -1; // not a number: refused below, like a value out of range
      }
      return new Config(env.apply(ENV_TENANT_ATTRIBUTE), env.apply(ENV_TENANT_BODY_PATH),
          new HashSet<>(csv(env.apply(ENV_BODY_QUEUES))), new HashSet<>(csv(env.apply(ENV_ERROR_QUEUES))),
          csv(env.apply(ENV_REDACT_KEYS)), patterns == null ? null : csv(patterns), max);
    }

    boolean bodyOn(String queueName) {
      return queueName != null && !queueName.isEmpty()
          && (bodyQueues.contains("*") || bodyQueues.contains(queueName));
    }

    boolean errorsOn(String queueName) {
      return queueName != null && !queueName.isEmpty()
          && (errorQueues.contains("*") || errorQueues.contains(queueName));
    }
  }

  static List<String> csv(String value) {
    List<String> out = new ArrayList<>();
    if (value == null) {
      return out;
    }
    for (String part : value.split(",", -1)) {
      String t = part.trim();
      if (!t.isEmpty()) {
        out.add(t);
      }
    }
    return out;
  }

  // -- redaction (section 15.3) --------------------------------------------------------------------

  /** A redacted text and how many values were replaced. */
  static final class Redacted {
    final String text;
    final int count;
    final String format;

    Redacted(String text, int count, String format) {
      this.text = text;
      this.count = count;
      this.format = format;
    }
  }

  /** Key-value pairs for the configured key names, then the built-in patterns. */
  static Redacted redactText(String text, Config config) {
    int count = 0;
    if (config.keyValue != null) {
      Matcher m = config.keyValue.matcher(text);
      StringBuilder sb = new StringBuilder();
      int last = 0;
      while (m.find()) {
        sb.append(text, last, m.start()).append(m.group(1)).append(m.group(2)).append(REDACTED);
        last = m.end();
        count++;
      }
      if (count > 0) {
        text = sb.append(text, last, text.length()).toString();
      }
    }
    for (Pattern p : config.patterns) {
      Matcher m = p.matcher(text);
      StringBuilder sb = new StringBuilder();
      int last = 0;
      int n = 0;
      while (m.find()) {
        sb.append(text, last, m.start()).append(REDACTED);
        last = m.end();
        n++;
      }
      if (n > 0) {
        text = sb.append(text, last, text.length()).toString();
        count += n;
      }
    }
    return new Redacted(text, count, "text");
  }

  /** (redacted body, values replaced, "json" or "text"). */
  static Redacted redactBody(String body, Config config) throws RedactionException {
    if (config.invalid != null) {
      throw new RedactionException(config.invalid);
    }
    String[] pointer = s3Pointer(body);
    if (pointer != null) {
      // Section 15.3: a pointer is not redacted, and is written back in one compact form.
      return new Redacted("[\"" + S3_POINTER_CLASS + "\",{\"s3BucketName\":" + encodeString(pointer[0])
          + ",\"s3Key\":" + encodeString(pointer[1]) + "}]", 0, "json");
    }
    Scanner scanner = new Scanner(body, config);
    try {
      return new Redacted(scanner.run(), scanner.count, "json");
    } catch (NotJson e) {
      return redactText(body, config);
    } catch (StackOverflowError e) { // MAX_JSON_DEPTH stops this first
      throw new RedactionException("JSON nested too deeply");
    }
  }

  /** A string as JSON writes it, the way Python's {@code json.dumps(s, ensure_ascii=False)} does. */
  static String encodeString(String s) throws RedactionException {
    StringBuilder sb = new StringBuilder(s.length() + 2).append('"');
    for (int i = 0; i < s.length(); i++) {
      char c = s.charAt(i);
      switch (c) {
        case '"': sb.append("\\\""); break;
        case '\\': sb.append("\\\\"); break;
        case '\n': sb.append("\\n"); break;
        case '\r': sb.append("\\r"); break;
        case '\t': sb.append("\\t"); break;
        case '\b': sb.append("\\b"); break;
        case '\f': sb.append("\\f"); break;
        default:
          if (c < 0x20) {
            sb.append(String.format(Locale.ROOT, "\\u%04x", (int) c));
          } else {
            sb.append(c);
          }
      }
    }
    String out = sb.append('"').toString();
    if (hasLoneSurrogate(out)) { // it cannot be exported as UTF-8: fail closed
      throw new RedactionException("lone surrogate");
    }
    return out;
  }

  /**
   * A strict JSON scanner that copies the input and rewrites only the values it redacts, so the output keeps
   * the input's formatting and number text.
   */
  private static final class Scanner {
    private static final Pattern SCALAR =
        Pattern.compile("-?(?:0|[1-9][0-9]*)(?:\\.[0-9]+)?(?:[eE][+-]?[0-9]+)?|true|false|null");

    final String s;
    final Config config;
    final StringBuilder out = new StringBuilder();
    final Matcher scalar;
    int count;

    Scanner(String s, Config config) {
      this.s = s;
      this.config = config;
      this.scalar = SCALAR.matcher(s);
    }

    String run() throws NotJson, RedactionException {
      int i = ws(0, true);
      i = value(i, null, 0, true); // null: the root, which has no path segment yet
      if (ws(i, true) != s.length()) {
        throw NotJson.INSTANCE;
      }
      return out.toString();
    }

    private int ws(int i, boolean emit) {
      while (i < s.length()) {
        char c = s.charAt(i);
        if (c != ' ' && c != '\t' && c != '\n' && c != '\r') {
          break;
        }
        if (emit) {
          out.append(c);
        }
        i++;
      }
      return i;
    }

    private int value(int i, String path, int depth, boolean emit) throws NotJson, RedactionException {
      if (depth > MAX_JSON_DEPTH) {
        throw new RedactionException("JSON nested too deeply");
      }
      if (i >= s.length()) {
        throw NotJson.INSTANCE;
      }
      char c = s.charAt(i);
      if (c == '{') {
        return object(i, path, depth, emit);
      }
      if (c == '[') {
        return array(i, path, depth, emit);
      }
      if (c == '"') {
        int j = stringEnd(s, i);
        if (emit) {
          out.append(redactString(s.substring(i, j)));
        }
        return j;
      }
      scalar.region(i, s.length());
      if (!scalar.lookingAt()) {
        throw NotJson.INSTANCE;
      }
      if (emit) {
        out.append(s, i, scalar.end());
      }
      return scalar.end();
    }

    private int object(int i, String path, int depth, boolean emit) throws NotJson, RedactionException {
      put(emit, "{");
      i = ws(i + 1, emit);
      if (i < s.length() && s.charAt(i) == '}') {
        put(emit, "}");
        return i + 1;
      }
      while (true) {
        if (i >= s.length() || s.charAt(i) != '"') {
          throw NotJson.INSTANCE;
        }
        int j = stringEnd(s, i);
        String key = decodeString(s, i, j);
        put(emit, s.substring(i, j));
        i = ws(j, emit);
        if (i >= s.length() || s.charAt(i) != ':') {
          throw NotJson.INSTANCE;
        }
        put(emit, ":");
        i = ws(i + 1, emit);
        String lower = asciiLower(key);
        String kpath = path == null ? lower : path + "." + lower; // arrays add no segment
        if (emit && (config.names.contains(lower) || config.paths.contains(kpath))) {
          out.append('"').append(REDACTED).append('"');
          count++;
          i = value(i, kpath, depth + 1, false);
        } else {
          i = value(i, kpath, depth + 1, emit);
        }
        i = ws(i, emit);
        if (i < s.length() && s.charAt(i) == ',') {
          put(emit, ",");
          i = ws(i + 1, emit);
          continue;
        }
        if (i < s.length() && s.charAt(i) == '}') {
          put(emit, "}");
          return i + 1;
        }
        throw NotJson.INSTANCE;
      }
    }

    private int array(int i, String path, int depth, boolean emit) throws NotJson, RedactionException {
      put(emit, "[");
      i = ws(i + 1, emit);
      if (i < s.length() && s.charAt(i) == ']') {
        put(emit, "]");
        return i + 1;
      }
      while (true) {
        i = value(i, path, depth + 1, emit);
        i = ws(i, emit);
        if (i < s.length() && s.charAt(i) == ',') {
          put(emit, ",");
          i = ws(i + 1, emit);
          continue;
        }
        if (i < s.length() && s.charAt(i) == ']') {
          put(emit, "]");
          return i + 1;
        }
        throw NotJson.INSTANCE;
      }
    }

    private void put(boolean emit, String text) {
      if (emit) {
        out.append(text);
      }
    }

    private String redactString(String token) throws NotJson, RedactionException {
      String value = decodeString(token, 0, token.length());
      int k = 0;
      while (k < value.length() && " \t\n\r".indexOf(value.charAt(k)) >= 0) {
        k++;
      }
      if (k < value.length() && (value.charAt(k) == '{' || value.charAt(k) == '[')) {
        // A JSON document inside a string (an SNS envelope's Message, for one): redact it as JSON.
        Scanner inner = new Scanner(value, config);
        String redacted = null;
        try {
          redacted = inner.run();
        } catch (NotJson ignored) {
          // not JSON after all: the text rules below
        }
        if (redacted != null) {
          if (inner.count > 0) {
            count += inner.count;
            return encodeString(redacted);
          }
          return token;
        }
      }
      Redacted r = redactText(value, config);
      if (r.count == 0) {
        return token;
      }
      count += r.count;
      return encodeString(r.text);
    }
  }

  /** The end (exclusive) of the JSON string token starting at {@code i}. */
  private static int stringEnd(String s, int i) throws NotJson {
    int n = s.length();
    int j = i + 1;
    while (j < n) {
      char c = s.charAt(j);
      if (c == '"') {
        return j + 1;
      }
      if (c < 0x20) {
        throw NotJson.INSTANCE;
      }
      if (c == '\\') {
        if (j + 1 >= n) {
          throw NotJson.INSTANCE;
        }
        char e = s.charAt(j + 1);
        if (e == 'u') {
          if (j + 6 > n) {
            throw NotJson.INSTANCE;
          }
          for (int h = j + 2; h < j + 6; h++) {
            char x = s.charAt(h);
            if (!((x >= '0' && x <= '9') || (x >= 'a' && x <= 'f') || (x >= 'A' && x <= 'F'))) {
              throw NotJson.INSTANCE;
            }
          }
          j += 6;
          continue;
        }
        if ("\"\\/bfnrt".indexOf(e) < 0) {
          throw NotJson.INSTANCE;
        }
        j += 2;
        continue;
      }
      j++;
    }
    throw NotJson.INSTANCE;
  }

  /** The value of the (already validated) JSON string token {@code s[i:j]}. */
  private static String decodeString(String s, int i, int j) {
    StringBuilder sb = new StringBuilder(j - i);
    for (int k = i + 1; k < j - 1; k++) {
      char c = s.charAt(k);
      if (c != '\\') {
        sb.append(c);
        continue;
      }
      char e = s.charAt(++k);
      switch (e) {
        case 'b': sb.append('\b'); break;
        case 'f': sb.append('\f'); break;
        case 'n': sb.append('\n'); break;
        case 'r': sb.append('\r'); break;
        case 't': sb.append('\t'); break;
        case 'u':
          sb.append((char) Integer.parseInt(s.substring(k + 1, k + 5), 16));
          k += 4;
          break;
        default: sb.append(e); // " \ /
      }
    }
    return sb.toString();
  }

  // -- a small JSON reader (tenant path, pointer and envelope tests) --------------------------------

  /** JSON null, as distinct from "not JSON" (null). */
  static final Object JSON_NULL = new Object();

  /** A JSON number, as its text. */
  static final class Num {
    final String text;

    Num(String text) {
      this.text = text;
    }
  }

  /** The parsed body: a Map, List, String, Num, Boolean or JSON_NULL; null if it is not one JSON document. */
  static Object parse(String body) {
    if (body == null) {
      return null;
    }
    try {
      Reader r = new Reader(body);
      int i = r.ws(0);
      Object v = r.value(i, 0);
      if (r.ws(r.pos) != body.length()) {
        return null;
      }
      return v;
    } catch (NotJson | RuntimeException | StackOverflowError e) {
      return null;
    }
  }

  private static final class Reader {
    private static final int MAX_DEPTH = 1000;
    final String s;
    final Matcher scalar;
    int pos;

    Reader(String s) {
      this.s = s;
      this.scalar = Scanner.SCALAR.matcher(s);
    }

    int ws(int i) {
      while (i < s.length() && " \t\n\r".indexOf(s.charAt(i)) >= 0) {
        i++;
      }
      return i;
    }

    Object value(int i, int depth) throws NotJson {
      if (depth > MAX_DEPTH || i >= s.length()) {
        throw NotJson.INSTANCE;
      }
      char c = s.charAt(i);
      if (c == '{') {
        Map<String, Object> m = new LinkedHashMap<>();
        i = ws(i + 1);
        if (i < s.length() && s.charAt(i) == '}') {
          pos = i + 1;
          return m;
        }
        while (true) {
          if (i >= s.length() || s.charAt(i) != '"') {
            throw NotJson.INSTANCE;
          }
          int j = stringEnd(s, i);
          String key = decodeString(s, i, j);
          i = ws(j);
          if (i >= s.length() || s.charAt(i) != ':') {
            throw NotJson.INSTANCE;
          }
          Object v = value(ws(i + 1), depth + 1);
          m.remove(key); // a duplicate member: the last one wins, as in Python and JavaScript
          m.put(key, v);
          i = ws(pos);
          if (i < s.length() && s.charAt(i) == ',') {
            i = ws(i + 1);
            continue;
          }
          if (i < s.length() && s.charAt(i) == '}') {
            pos = i + 1;
            return m;
          }
          throw NotJson.INSTANCE;
        }
      }
      if (c == '[') {
        List<Object> l = new ArrayList<>();
        i = ws(i + 1);
        if (i < s.length() && s.charAt(i) == ']') {
          pos = i + 1;
          return l;
        }
        while (true) {
          l.add(value(i, depth + 1));
          i = ws(pos);
          if (i < s.length() && s.charAt(i) == ',') {
            i = ws(i + 1);
            continue;
          }
          if (i < s.length() && s.charAt(i) == ']') {
            pos = i + 1;
            return l;
          }
          throw NotJson.INSTANCE;
        }
      }
      if (c == '"') {
        int j = stringEnd(s, i);
        pos = j;
        return decodeString(s, i, j);
      }
      scalar.region(i, s.length());
      if (!scalar.lookingAt()) {
        throw NotJson.INSTANCE;
      }
      pos = scalar.end();
      String t = scalar.group();
      switch (t) {
        case "true": return Boolean.TRUE;
        case "false": return Boolean.FALSE;
        case "null": return JSON_NULL;
        default: return new Num(t);
      }
    }
  }

  // -- reading the body ----------------------------------------------------------------------------

  /** UTF-8 length in bytes; a lone surrogate counts 3, as Python's "surrogatepass" encodes it. */
  static long utf8Length(String s) {
    long n = 0;
    for (int i = 0; i < s.length(); i++) {
      char c = s.charAt(i);
      if (c < 0x80) {
        n += 1;
      } else if (c < 0x800) {
        n += 2;
      } else if (Character.isHighSurrogate(c) && i + 1 < s.length() && Character.isLowSurrogate(s.charAt(i + 1))) {
        n += 4;
        i++;
      } else {
        n += 3;
      }
    }
    return n;
  }

  static boolean hasLoneSurrogate(String s) {
    for (int i = 0; i < s.length(); i++) {
      char c = s.charAt(i);
      if (Character.isHighSurrogate(c)) {
        if (i + 1 < s.length() && Character.isLowSurrogate(s.charAt(i + 1))) {
          i++;
          continue;
        }
        return true;
      }
      if (Character.isLowSurrogate(c)) {
        return true;
      }
    }
    return false;
  }

  private static boolean isPointerDoc(Object doc) {
    return doc instanceof List && ((List<?>) doc).size() == 2 && S3_POINTER_CLASS.equals(((List<?>) doc).get(0));
  }

  /** (bucket, key) if the body is exactly an Extended Client pointer (section 15.3), else null. */
  static String[] s3Pointer(String body) {
    if (body == null || !body.contains(S3_POINTER_CLASS)) {
      return null;
    }
    Object doc = parse(body);
    if (!isPointerDoc(doc) || !(((List<?>) doc).get(1) instanceof Map)) {
      return null;
    }
    Map<?, ?> m = (Map<?, ?>) ((List<?>) doc).get(1);
    if (m.size() != 2 || !m.containsKey("s3BucketName") || !m.containsKey("s3Key")) {
      return null;
    }
    Object bucket = m.get("s3BucketName");
    Object key = m.get("s3Key");
    return bucket instanceof String && key instanceof String ? new String[] {(String) bucket, (String) key} : null;
  }

  /** The offloaded payload's size from the Extended Client's message attribute, if it is a whole number. */
  static Long offloadedSize(Map<String, String> attributes) {
    if (attributes == null) {
      return null;
    }
    for (String name : OFFLOADED_SIZE_ATTRIBUTES) {
      String v = attributes.get(name);
      if (v != null && SIZE_RE.matcher(v).matches()) {
        return Long.parseLong(v);
      }
    }
    return null;
  }

  private static String envelope(Object doc) {
    if (doc instanceof Map) {
      Map<?, ?> m = (Map<?, ?>) doc;
      if ("Notification".equals(m.get("Type")) && m.containsKey("TopicArn")) {
        return "sns";
      }
      if (m.containsKey("detail-type") && m.containsKey("detail") && m.containsKey("source")) {
        return "eventbridge";
      }
    }
    return null;
  }

  static boolean md5Matches(String body, String md5) {
    if (md5 == null || md5.isEmpty()) {
      return true; // nothing to check against
    }
    try {
      byte[] d = MessageDigest.getInstance("MD5").digest(body.getBytes(StandardCharsets.UTF_8));
      StringBuilder hex = new StringBuilder(32);
      for (byte b : d) {
        hex.append(Character.forDigit((b >> 4) & 0xf, 16)).append(Character.forDigit(b & 0xf, 16));
      }
      return hex.toString().equals(md5.toLowerCase(Locale.ROOT));
    } catch (Exception e) {
      return false;
    }
  }

  /**
   * A non-empty string of at most 256 code points with no lone surrogate (QV-549): a lone surrogate, which a JSON
   * escape of half a surrogate pair can carry, cannot be exported as UTF-8 (the exporter would write {@code ?}), so
   * the tenant counts as not found.
   */
  static boolean validTenant(String v) {
    return v != null && !v.isEmpty() && codePoints(v) <= MAX_TENANT_CHARS && !hasLoneSurrogate(v);
  }

  /** (tenant, source) from the configured message attribute, else the configured body path; else null. */
  static String[] tenant(Config config, Map<String, String> attributes, String body) {
    if (config.tenantAttribute != null && attributes != null) {
      String v = attributes.get(config.tenantAttribute);
      if (validTenant(v)) {
        return new String[] {v, "message_attribute"};
      }
    }
    if (config.tenantBodyPath != null && body != null) {
      Object doc = parse(body);
      for (String part : config.tenantBodyPath.split("\\.", -1)) {
        doc = doc instanceof Map ? ((Map<?, ?>) doc).get(part) : null;
      }
      String v = null;
      if (doc instanceof String) {
        v = (String) doc;
      } else if (doc instanceof Num) {
        v = numberText(((Num) doc).text);
      }
      if (validTenant(v)) {
        return new String[] {v, "body_path"};
      }
    }
    return null;
  }

  /**
   * A number as Java prints it (section 15.2): an integer exactly ({@code 42} is {@code "42"}, at any size),
   * anything else as {@link Double#toString(double)}. Null for an integer longer than a tenant may be.
   */
  static String numberText(String literal) {
    if (literal.indexOf('.') < 0 && literal.indexOf('e') < 0 && literal.indexOf('E') < 0) {
      // JSON integers have no leading zeros, so the printed form is the literal (or "0" for "-0"): one
      // longer than the tenant limit is dropped anyway. Checking first keeps BigInteger's conversion,
      // which grows with the square of the length, off a body of up to 1 MiB of digits.
      if (literal.length() > MAX_TENANT_CHARS) {
        return null;
      }
      return new BigInteger(literal).toString();
    }
    return Double.toString(Double.parseDouble(literal));
  }

  private static int codePoints(String s) {
    return s.codePointCount(0, s.length());
  }

  /**
   * Section 15 attributes for one message on a send or receive span, in insertion order.
   *
   * @param attributes the message attributes' {@code StringValue}s, by name (null if none)
   * @param md5 SQS's MD5 of the body, or null to skip the check
   */
  static Map<String, Object> contentAttrs(Config config, String queueName, String body,
                                          Map<String, String> attributes, String md5) {
    Map<String, Object> out = new LinkedHashMap<>();
    if (body != null) {
      out.put("messaging.message.body.size", utf8Length(body));
    }
    String[] t = tenant(config, attributes, body);
    if (t != null) {
      out.put("trailbeam.tenant", t[0]);
      out.put("trailbeam.tenant.source", t[1]);
    }
    Long offloaded = offloadedSize(attributes);
    if (offloaded != null && body != null && isPointerDoc(parse(body))) {
      out.put("trailbeam.body.offloaded_size", offloaded);
    }
    if (body == null) {
      return out;
    }
    if (!config.bodyOn(queueName)) {
      out.put("trailbeam.body.capture_status", STATUS_OFF);
      return out;
    }
    if (!md5Matches(body, md5)) {
      out.put("trailbeam.body.capture_status", STATUS_MD5_MISMATCH);
      return out;
    }
    Redacted r;
    try {
      r = redactBody(body, config);
      if (hasLoneSurrogate(r.text)) {
        throw new RedactionException("lone surrogate");
      }
    } catch (Exception | StackOverflowError e) {
      out.put("trailbeam.body.capture_status", STATUS_REDACTION_FAILED);
      return out;
    }
    if (utf8Length(r.text) > config.maxBodyBytes) {
      out.put("trailbeam.body.capture_status", STATUS_EXCEEDS_LIMIT);
      return out;
    }
    Object doc = "json".equals(r.format) ? parse(body) : null;
    out.put("trailbeam.body.capture_status", isPointerDoc(doc) ? STATUS_OFFLOADED : STATUS_CAPTURED);
    out.put("trailbeam.body", r.text);
    out.put("trailbeam.body.format", r.format);
    out.put("trailbeam.body.redacted_count", (long) r.count);
    String env = envelope(doc);
    if (env != null) {
      out.put("trailbeam.body.envelope", env);
    }
    return out;
  }

  /** The error's message, redacted and cut to 1,024 code points, if error text is on for the queue (15.5). */
  static String errorMessage(Config config, String queueName, String message) {
    if (!config.errorsOn(queueName) || config.invalid != null || message == null || message.isEmpty()) {
      return null;
    }
    try {
      String text = redactText(message, config).text;
      if (codePoints(text) > MAX_ERROR_MESSAGE_CHARS) {
        text = text.substring(0, text.offsetByCodePoints(0, MAX_ERROR_MESSAGE_CHARS));
      }
      return hasLoneSurrogate(text) ? null : text; // fail closed: no text rather than unredacted text
    } catch (RuntimeException | StackOverflowError e) {
      return null;
    }
  }

  static String asciiLower(String s) {
    char[] c = s.toCharArray();
    for (int i = 0; i < c.length; i++) {
      if (c[i] >= 'A' && c[i] <= 'Z') {
        c[i] = (char) (c[i] + 32);
      }
    }
    return new String(c);
  }

  private static String blankToNull(String s) {
    return s == null || s.isEmpty() ? null : s;
  }
}
