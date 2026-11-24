using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Numerics;
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;

namespace Trailbeam.Sqs
{
    /// <summary>
    /// Message contents (contract section 15, revision 3.2): body size, tenant, opt-in body capture with redaction,
    /// and Lambda error text.
    ///
    /// This mirrors <c>python/src/trailbeam/_capture.py</c> and <c>java/.../Capture.java</c> rule for rule, and is
    /// tested against the same <c>capture</c> vectors in <c>contract/span-contract-vectors.json</c>. Nothing here
    /// makes an SQS call: everything is read from the parameters and responses of calls the application made, or
    /// from the Lambda event.
    /// </summary>
    internal static class Capture
    {
        // -- statuses (section 15.4) ---------------------------------------------------------------------

        internal const string StatusCaptured = "captured";
        internal const string StatusOff = "off";
        internal const string StatusMd5Mismatch = "md5_mismatch";
        internal const string StatusRedactionFailed = "redaction_failed";
        internal const string StatusExceedsLimit = "exceeds_capture_limit";
        internal const string StatusOffloaded = "offloaded_to_s3";

        internal const string Redacted = "[REDACTED]";
        internal const int DefaultMaxBodyBytes = 16 * 1024;
        internal const int HardMaxBodyBytes = 256 * 1024;
        internal const int MaxErrorMessageChars = 1024;
        internal const int MaxTenantChars = 256;
        internal const int MaxJsonDepth = 100;
        internal const string S3PointerClass = "software.amazon.payloadoffloading.PayloadS3Pointer";

        /// <summary>Where the SQS Extended Client puts the offloaded payload's size, current name first (section 15.2).</summary>
        internal static readonly IReadOnlyList<string> OffloadedSizeAttributes = new[] { "ExtendedPayloadSize", "SQSLargePayloadSize" };

        private static readonly Regex SizeRe = new Regex(@"\A[0-9]{1,15}\z", RegexOptions.CultureInvariant);

        internal static readonly IReadOnlyList<string> DefaultRedactKeys = new[]
        {
            "password", "passwd", "secret", "token", "access_token", "refresh_token", "authorization",
            "api_key", "apikey", "ssn", "card_number", "cardnumber", "cvv", "cvc",
        };

        /// <summary>
        /// Built-in text patterns (section 15.3), the same expressions as Python's, Node.js's and Java's: ASCII
        /// classes only, no case folding.
        /// </summary>
        internal static readonly IReadOnlyList<KeyValuePair<string, Regex>> Patterns = new[]
        {
            P("aws_access_key", "(?<![A-Z0-9])(?:AKIA|ASIA)[A-Z0-9]{16}(?![A-Z0-9])"),
            P("bearer", "(?<![A-Za-z])[Bb]earer [A-Za-z0-9._~+/=-]+"),
            P("email", @"[A-Za-z0-9._%+-]+@[A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)*\.[A-Za-z]{2,}"),
            P("long_number", "(?<![0-9])[0-9](?:[ -]?[0-9]){8,}(?![0-9])"),
        };

        private static KeyValuePair<string, Regex> P(string name, string pattern) =>
            new KeyValuePair<string, Regex>(name, new Regex(pattern, RegexOptions.CultureInvariant));

        private static Regex? PatternNamed(string name)
        {
            foreach (var p in Patterns)
            {
                if (p.Key == name)
                {
                    return p.Value;
                }
            }
            return null;
        }

        private static readonly Regex KeyRe = new Regex(@"\A[A-Za-z0-9_.-]+\z", RegexOptions.CultureInvariant);

        internal const string EnvTenantAttribute = "TRAILBEAM_TENANT_ATTRIBUTE";
        internal const string EnvTenantBodyPath = "TRAILBEAM_TENANT_BODY_PATH";
        internal const string EnvBodyQueues = "TRAILBEAM_CAPTURE_BODY_QUEUES";
        internal const string EnvErrorQueues = "TRAILBEAM_CAPTURE_ERROR_QUEUES";
        internal const string EnvRedactKeys = "TRAILBEAM_REDACT_KEYS";
        internal const string EnvRedactPatterns = "TRAILBEAM_REDACT_PATTERNS";
        internal const string EnvMaxBodyBytes = "TRAILBEAM_CAPTURE_MAX_BYTES";

        /// <summary>Redaction could not run: the body (or error text) is not exported.</summary>
        internal sealed class RedactionException : Exception
        {
            internal RedactionException(string message)
                : base(message)
            {
            }
        }

        /// <summary>The input is not one strict JSON document.</summary>
        private sealed class NotJsonException : Exception
        {
            internal static readonly NotJsonException Instance = new NotJsonException();

            private NotJsonException()
                : base("not JSON")
            {
            }
        }

        // -- config (section 15.1) -----------------------------------------------------------------------

        /// <summary>What the package reads from message contents. Off unless configured.</summary>
        internal sealed class Config
        {
            internal Config(string? tenantAttribute, string? tenantBodyPath, IEnumerable<string> bodyQueues,
                IEnumerable<string> errorQueues, IEnumerable<string> extraRedactKeys, IEnumerable<string>? redactPatterns,
                int? maxBodyBytes)
            {
                TenantAttribute = string.IsNullOrEmpty(tenantAttribute) ? null : tenantAttribute;
                TenantBodyPath = string.IsNullOrEmpty(tenantBodyPath) ? null : tenantBodyPath;
                BodyQueues = new HashSet<string>(bodyQueues, StringComparer.Ordinal);
                ErrorQueues = new HashSet<string>(errorQueues, StringComparer.Ordinal);
                var keys = new List<string>(DefaultRedactKeys);
                foreach (string k in extraRedactKeys)
                {
                    if (!keys.Contains(k))
                    {
                        keys.Add(k);
                    }
                }
                RedactKeys = keys;
                RedactPatterns = redactPatterns == null ? Patterns.Select(p => p.Key).ToList() : redactPatterns.ToList();
                MaxBodyBytes = maxBodyBytes == null || maxBodyBytes == 0 ? DefaultMaxBodyBytes : maxBodyBytes.Value;

                string? bad = null;
                foreach (string k in RedactKeys)
                {
                    if (!KeyRe.IsMatch(k))
                    {
                        bad = "redact key '" + k + "' may contain only A-Z a-z 0-9 _ . -";
                        break;
                    }
                }
                if (bad == null)
                {
                    foreach (string p in RedactPatterns)
                    {
                        if (PatternNamed(p) == null)
                        {
                            bad = "unknown redact pattern '" + p + "'; known: " + string.Join(", ", Patterns.Select(x => x.Key));
                            break;
                        }
                    }
                }
                if (bad == null && !(MaxBodyBytes > 0 && MaxBodyBytes <= HardMaxBodyBytes))
                {
                    bad = "the capture limit must be 1 to " + HardMaxBodyBytes + " bytes";
                }
                Invalid = bad;

                var alternatives = new List<string>();
                foreach (string k in RedactKeys)
                {
                    if (k.IndexOf('.') < 0)
                    {
                        Names.Add(AsciiLower(k));
                        alternatives.Add(AsciiCaseless(k));
                    }
                    else
                    {
                        Paths.Add(AsciiLower(k));
                    }
                }
                // Each letter spelt as [aA]: ASCII case folding only, like Python's re.IGNORECASE | re.ASCII. (.NET's
                // RegexOptions.IgnoreCase would also fold, for one, the Kelvin sign into k.)
                KeyValue = bad != null || alternatives.Count == 0 ? null : new Regex(
                    "(?<![A-Za-z0-9_])(" + string.Join("|", alternatives)
                    + ")([\"']?[ \\t]*[:=][ \\t]*)(\"[^\"]*\"|'[^']*'|[^ \\t\\r\\n,;&]+)",
                    RegexOptions.CultureInvariant);
                if (bad == null)
                {
                    foreach (string name in RedactPatterns)
                    {
                        CompiledPatterns.Add(PatternNamed(name)!);
                    }
                }
            }

            internal string? TenantAttribute { get; }
            internal string? TenantBodyPath { get; }
            internal HashSet<string> BodyQueues { get; }
            internal HashSet<string> ErrorQueues { get; }
            internal IReadOnlyList<string> RedactKeys { get; }
            internal IReadOnlyList<string> RedactPatterns { get; }
            internal int MaxBodyBytes { get; }

            /// <summary>
            /// Why the redaction settings were refused, or null. Python and Node.js refuse them at install time; this
            /// package's hooks never throw into the application, so it fails closed instead: no body and no error
            /// text is exported, and a listed queue's bodies say <c>redaction_failed</c> (as the Java package does).
            /// </summary>
            internal string? Invalid { get; }

            internal Regex? KeyValue { get; }
            internal List<Regex> CompiledPatterns { get; } = new List<Regex>();
            internal HashSet<string> Names { get; } = new HashSet<string>(StringComparer.Ordinal);
            internal HashSet<string> Paths { get; } = new HashSet<string>(StringComparer.Ordinal);

            /// <summary>No tenant, no capture.</summary>
            internal static Config Off() =>
                new Config(null, null, Array.Empty<string>(), Array.Empty<string>(), Array.Empty<string>(), null, null);

            /// <summary>Section 15.1, from environment variables.</summary>
            internal static Config FromEnv(Func<string, string?> env)
            {
                string? patterns = env(EnvRedactPatterns);
                int? max = null;
                string? rawMax = env(EnvMaxBodyBytes);
                if (!string.IsNullOrEmpty(rawMax))
                {
                    max = int.TryParse(rawMax!.Trim(), NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out int v)
                        ? v
                        : -1; // not a number: refused, like a value out of range
                }
                return new Config(env(EnvTenantAttribute), env(EnvTenantBodyPath), Csv(env(EnvBodyQueues)),
                    Csv(env(EnvErrorQueues)), Csv(env(EnvRedactKeys)), patterns == null ? null : Csv(patterns), max);
            }

            internal bool BodyOn(string? queueName) =>
                !string.IsNullOrEmpty(queueName) && (BodyQueues.Contains("*") || BodyQueues.Contains(queueName!));

            internal bool ErrorsOn(string? queueName) =>
                !string.IsNullOrEmpty(queueName) && (ErrorQueues.Contains("*") || ErrorQueues.Contains(queueName!));
        }

        internal static List<string> Csv(string? value)
        {
            var output = new List<string>();
            if (value == null)
            {
                return output;
            }
            foreach (string part in value.Split(','))
            {
                string t = part.Trim();
                if (t.Length > 0)
                {
                    output.Add(t);
                }
            }
            return output;
        }

        // -- redaction (section 15.3) ----------------------------------------------------------------------

        /// <summary>A redacted text, how many values were replaced, and its format.</summary>
        internal sealed class RedactedText
        {
            internal RedactedText(string text, int count, string format)
            {
                Text = text;
                Count = count;
                Format = format;
            }

            internal string Text { get; }
            internal int Count { get; }
            internal string Format { get; }
        }

        /// <summary>Key-value pairs for the configured key names, then the built-in patterns.</summary>
        internal static RedactedText RedactText(string text, Config config)
        {
            int count = 0;
            if (config.KeyValue != null)
            {
                text = config.KeyValue.Replace(text, m =>
                {
                    count++;
                    return m.Groups[1].Value + m.Groups[2].Value + Redacted;
                });
            }
            foreach (Regex p in config.CompiledPatterns)
            {
                text = p.Replace(text, m =>
                {
                    count++;
                    return Redacted;
                });
            }
            return new RedactedText(text, count, "text");
        }

        /// <summary>(redacted body, values replaced, "json" or "text").</summary>
        internal static RedactedText RedactBody(string body, Config config)
        {
            if (config.Invalid != null)
            {
                throw new RedactionException(config.Invalid);
            }
            var pointer = S3Pointer(body);
            if (pointer != null)
            {
                // Section 15.3: a pointer is not redacted, and is written back in one compact form.
                return new RedactedText("[\"" + S3PointerClass + "\",{\"s3BucketName\":" + EncodeString(pointer.Value.Bucket)
                    + ",\"s3Key\":" + EncodeString(pointer.Value.Key) + "}]", 0, "json");
            }
            var scanner = new Scanner(body, config);
            try
            {
                return new RedactedText(scanner.Run(), scanner.Count, "json");
            }
            catch (NotJsonException)
            {
                return RedactText(body, config);
            }
        }

        /// <summary>A string as JSON writes it, the way Python's <c>json.dumps(s, ensure_ascii=False)</c> does.</summary>
        internal static string EncodeString(string s)
        {
            var sb = new StringBuilder(s.Length + 2).Append('"');
            foreach (char c in s)
            {
                switch (c)
                {
                    case '"': sb.Append("\\\""); break;
                    case '\\': sb.Append("\\\\"); break;
                    case '\n': sb.Append("\\n"); break;
                    case '\r': sb.Append("\\r"); break;
                    case '\t': sb.Append("\\t"); break;
                    case '\b': sb.Append("\\b"); break;
                    case '\f': sb.Append("\\f"); break;
                    default:
                        if (c < 0x20)
                        {
                            sb.Append("\\u").Append(((int)c).ToString("x4", CultureInfo.InvariantCulture));
                        }
                        else
                        {
                            sb.Append(c);
                        }
                        break;
                }
            }
            string output = sb.Append('"').ToString();
            if (HasLoneSurrogate(output))
            {
                throw new RedactionException("lone surrogate"); // it cannot be exported as UTF-8: fail closed
            }
            return output;
        }

        private static readonly Regex Scalar = new Regex(
            @"\G(?:-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?|true|false|null)", RegexOptions.CultureInvariant);

        /// <summary>
        /// A strict JSON scanner that copies the input and rewrites only the values it redacts, so the output keeps
        /// the input's formatting and number text.
        /// </summary>
        private sealed class Scanner
        {
            private readonly string _s;
            private readonly Config _config;
            private readonly StringBuilder _out = new StringBuilder();

            internal Scanner(string s, Config config)
            {
                _s = s;
                _config = config;
            }

            internal int Count { get; private set; }

            internal string Run()
            {
                int i = Ws(0, true);
                i = Value(i, null, 0, true); // null: the root, which has no path segment yet
                if (Ws(i, true) != _s.Length)
                {
                    throw NotJsonException.Instance;
                }
                return _out.ToString();
            }

            private int Ws(int i, bool emit)
            {
                while (i < _s.Length)
                {
                    char c = _s[i];
                    if (c != ' ' && c != '\t' && c != '\n' && c != '\r')
                    {
                        break;
                    }
                    if (emit)
                    {
                        _out.Append(c);
                    }
                    i++;
                }
                return i;
            }

            private int Value(int i, string? path, int depth, bool emit)
            {
                if (depth > MaxJsonDepth)
                {
                    throw new RedactionException("JSON nested too deeply");
                }
                if (i >= _s.Length)
                {
                    throw NotJsonException.Instance;
                }
                char c = _s[i];
                if (c == '{')
                {
                    return Object(i, path, depth, emit);
                }
                if (c == '[')
                {
                    return Array(i, path, depth, emit);
                }
                if (c == '"')
                {
                    int j = StringEnd(_s, i);
                    if (emit)
                    {
                        _out.Append(RedactString(_s.Substring(i, j - i)));
                    }
                    return j;
                }
                Match m = Scalar.Match(_s, i);
                if (!m.Success)
                {
                    throw NotJsonException.Instance;
                }
                if (emit)
                {
                    _out.Append(_s, i, m.Length);
                }
                return i + m.Length;
            }

            private int Object(int i, string? path, int depth, bool emit)
            {
                Put(emit, "{");
                i = Ws(i + 1, emit);
                if (i < _s.Length && _s[i] == '}')
                {
                    Put(emit, "}");
                    return i + 1;
                }
                while (true)
                {
                    if (i >= _s.Length || _s[i] != '"')
                    {
                        throw NotJsonException.Instance;
                    }
                    int j = StringEnd(_s, i);
                    string key = DecodeString(_s, i, j);
                    Put(emit, _s.Substring(i, j - i));
                    i = Ws(j, emit);
                    if (i >= _s.Length || _s[i] != ':')
                    {
                        throw NotJsonException.Instance;
                    }
                    Put(emit, ":");
                    i = Ws(i + 1, emit);
                    string lower = AsciiLower(key);
                    string kpath = path == null ? lower : path + "." + lower; // arrays add no segment
                    if (emit && (_config.Names.Contains(lower) || _config.Paths.Contains(kpath)))
                    {
                        _out.Append('"').Append(Redacted).Append('"');
                        Count++;
                        i = Value(i, kpath, depth + 1, false);
                    }
                    else
                    {
                        i = Value(i, kpath, depth + 1, emit);
                    }
                    i = Ws(i, emit);
                    if (i < _s.Length && _s[i] == ',')
                    {
                        Put(emit, ",");
                        i = Ws(i + 1, emit);
                        continue;
                    }
                    if (i < _s.Length && _s[i] == '}')
                    {
                        Put(emit, "}");
                        return i + 1;
                    }
                    throw NotJsonException.Instance;
                }
            }

            private int Array(int i, string? path, int depth, bool emit)
            {
                Put(emit, "[");
                i = Ws(i + 1, emit);
                if (i < _s.Length && _s[i] == ']')
                {
                    Put(emit, "]");
                    return i + 1;
                }
                while (true)
                {
                    i = Value(i, path, depth + 1, emit);
                    i = Ws(i, emit);
                    if (i < _s.Length && _s[i] == ',')
                    {
                        Put(emit, ",");
                        i = Ws(i + 1, emit);
                        continue;
                    }
                    if (i < _s.Length && _s[i] == ']')
                    {
                        Put(emit, "]");
                        return i + 1;
                    }
                    throw NotJsonException.Instance;
                }
            }

            private void Put(bool emit, string text)
            {
                if (emit)
                {
                    _out.Append(text);
                }
            }

            private string RedactString(string token)
            {
                string value = DecodeString(token, 0, token.Length);
                int k = 0;
                while (k < value.Length && " \t\n\r".IndexOf(value[k]) >= 0)
                {
                    k++;
                }
                if (k < value.Length && (value[k] == '{' || value[k] == '['))
                {
                    // A JSON document inside a string (an SNS envelope's Message, for one): redact it as JSON.
                    var inner = new Scanner(value, _config);
                    string? redacted = null;
                    try
                    {
                        redacted = inner.Run();
                    }
                    catch (NotJsonException)
                    {
                        // not JSON after all: the text rules below
                    }
                    if (redacted != null)
                    {
                        if (inner.Count > 0)
                        {
                            Count += inner.Count;
                            return EncodeString(redacted);
                        }
                        return token;
                    }
                }
                RedactedText r = RedactText(value, _config);
                if (r.Count == 0)
                {
                    return token;
                }
                Count += r.Count;
                return EncodeString(r.Text);
            }
        }

        /// <summary>The end (exclusive) of the JSON string token starting at <paramref name="i"/>.</summary>
        private static int StringEnd(string s, int i)
        {
            int n = s.Length;
            int j = i + 1;
            while (j < n)
            {
                char c = s[j];
                if (c == '"')
                {
                    return j + 1;
                }
                if (c < 0x20)
                {
                    throw NotJsonException.Instance;
                }
                if (c == '\\')
                {
                    if (j + 1 >= n)
                    {
                        throw NotJsonException.Instance;
                    }
                    char e = s[j + 1];
                    if (e == 'u')
                    {
                        if (j + 6 > n)
                        {
                            throw NotJsonException.Instance;
                        }
                        for (int h = j + 2; h < j + 6; h++)
                        {
                            char x = s[h];
                            if (!((x >= '0' && x <= '9') || (x >= 'a' && x <= 'f') || (x >= 'A' && x <= 'F')))
                            {
                                throw NotJsonException.Instance;
                            }
                        }
                        j += 6;
                        continue;
                    }
                    if ("\"\\/bfnrt".IndexOf(e) < 0)
                    {
                        throw NotJsonException.Instance;
                    }
                    j += 2;
                    continue;
                }
                j++;
            }
            throw NotJsonException.Instance;
        }

        /// <summary>The value of the (already validated) JSON string token <c>s[i:j]</c>.</summary>
        private static string DecodeString(string s, int i, int j)
        {
            var sb = new StringBuilder(j - i);
            for (int k = i + 1; k < j - 1; k++)
            {
                char c = s[k];
                if (c != '\\')
                {
                    sb.Append(c);
                    continue;
                }
                char e = s[++k];
                switch (e)
                {
                    case 'b': sb.Append('\b'); break;
                    case 'f': sb.Append('\f'); break;
                    case 'n': sb.Append('\n'); break;
                    case 'r': sb.Append('\r'); break;
                    case 't': sb.Append('\t'); break;
                    case 'u':
                        sb.Append((char)int.Parse(s.Substring(k + 1, 4), NumberStyles.HexNumber, CultureInfo.InvariantCulture));
                        k += 4;
                        break;
                    default: sb.Append(e); break; // " \ /
                }
            }
            return sb.ToString();
        }

        // -- a small JSON reader (tenant path, pointer and envelope tests) -----------------------------------

        /// <summary>JSON null, as distinct from "not JSON" (null).</summary>
        internal static readonly object JsonNull = new object();

        /// <summary>A JSON number, as its text.</summary>
        internal sealed class Num
        {
            internal Num(string text)
            {
                Text = text;
            }

            internal string Text { get; }
        }

        /// <summary>The parsed body: a Dictionary, List, string, Num, bool or JsonNull; null if it is not one JSON document.</summary>
        internal static object? Parse(string? body)
        {
            if (body == null)
            {
                return null;
            }
            try
            {
                var r = new Reader(body);
                object v = r.Value(r.Ws(0), 0);
                return r.Ws(r.Pos) != body.Length ? null : v;
            }
            catch (Exception)
            {
                return null;
            }
        }

        private sealed class Reader
        {
            private const int MaxDepth = 1000;
            private readonly string _s;

            internal Reader(string s)
            {
                _s = s;
            }

            internal int Pos { get; private set; }

            internal int Ws(int i)
            {
                while (i < _s.Length && " \t\n\r".IndexOf(_s[i]) >= 0)
                {
                    i++;
                }
                return i;
            }

            internal object Value(int i, int depth)
            {
                if (depth > MaxDepth || i >= _s.Length)
                {
                    throw NotJsonException.Instance;
                }
                char c = _s[i];
                if (c == '{')
                {
                    var m = new Dictionary<string, object>(StringComparer.Ordinal);
                    i = Ws(i + 1);
                    if (i < _s.Length && _s[i] == '}')
                    {
                        Pos = i + 1;
                        return m;
                    }
                    while (true)
                    {
                        if (i >= _s.Length || _s[i] != '"')
                        {
                            throw NotJsonException.Instance;
                        }
                        int j = StringEnd(_s, i);
                        string key = DecodeString(_s, i, j);
                        i = Ws(j);
                        if (i >= _s.Length || _s[i] != ':')
                        {
                            throw NotJsonException.Instance;
                        }
                        object v = Value(Ws(i + 1), depth + 1);
                        m[key] = v; // a duplicate member: the last one wins, as in Python and JavaScript
                        i = Ws(Pos);
                        if (i < _s.Length && _s[i] == ',')
                        {
                            i = Ws(i + 1);
                            continue;
                        }
                        if (i < _s.Length && _s[i] == '}')
                        {
                            Pos = i + 1;
                            return m;
                        }
                        throw NotJsonException.Instance;
                    }
                }
                if (c == '[')
                {
                    var l = new List<object>();
                    i = Ws(i + 1);
                    if (i < _s.Length && _s[i] == ']')
                    {
                        Pos = i + 1;
                        return l;
                    }
                    while (true)
                    {
                        l.Add(Value(i, depth + 1));
                        i = Ws(Pos);
                        if (i < _s.Length && _s[i] == ',')
                        {
                            i = Ws(i + 1);
                            continue;
                        }
                        if (i < _s.Length && _s[i] == ']')
                        {
                            Pos = i + 1;
                            return l;
                        }
                        throw NotJsonException.Instance;
                    }
                }
                if (c == '"')
                {
                    int j = StringEnd(_s, i);
                    Pos = j;
                    return DecodeString(_s, i, j);
                }
                Match sm = Scalar.Match(_s, i);
                if (!sm.Success)
                {
                    throw NotJsonException.Instance;
                }
                Pos = i + sm.Length;
                switch (sm.Value)
                {
                    case "true": return true;
                    case "false": return false;
                    case "null": return JsonNull;
                    default: return new Num(sm.Value);
                }
            }
        }

        // -- reading the body --------------------------------------------------------------------------------

        /// <summary>UTF-8 length in bytes; a lone surrogate counts 3, as Python's "surrogatepass" encodes it.</summary>
        internal static long Utf8Length(string s)
        {
            long n = 0;
            for (int i = 0; i < s.Length; i++)
            {
                char c = s[i];
                if (c < 0x80)
                {
                    n += 1;
                }
                else if (c < 0x800)
                {
                    n += 2;
                }
                else if (char.IsHighSurrogate(c) && i + 1 < s.Length && char.IsLowSurrogate(s[i + 1]))
                {
                    n += 4;
                    i++;
                }
                else
                {
                    n += 3;
                }
            }
            return n;
        }

        internal static bool HasLoneSurrogate(string s)
        {
            for (int i = 0; i < s.Length; i++)
            {
                char c = s[i];
                if (char.IsHighSurrogate(c))
                {
                    if (i + 1 < s.Length && char.IsLowSurrogate(s[i + 1]))
                    {
                        i++;
                        continue;
                    }
                    return true;
                }
                if (char.IsLowSurrogate(c))
                {
                    return true;
                }
            }
            return false;
        }

        private static bool IsPointerDoc(object? doc) =>
            doc is List<object> l && l.Count == 2 && l[0] is string s && s == S3PointerClass;

        /// <summary>(bucket, key) if the body is exactly an Extended Client pointer (section 15.3), else null.</summary>
        internal static (string Bucket, string Key)? S3Pointer(string? body)
        {
            if (body == null || body.IndexOf(S3PointerClass, StringComparison.Ordinal) < 0)
            {
                return null;
            }
            object? doc = Parse(body);
            if (!IsPointerDoc(doc) || !(((List<object>)doc!)[1] is Dictionary<string, object> m))
            {
                return null;
            }
            if (m.Count != 2 || !m.ContainsKey("s3BucketName") || !m.ContainsKey("s3Key"))
            {
                return null;
            }
            return m["s3BucketName"] is string bucket && m["s3Key"] is string key ? (bucket, key) : ((string, string)?)null;
        }

        /// <summary>The offloaded payload's size from the Extended Client's message attribute, if it is a whole number.</summary>
        internal static long? OffloadedSize(IDictionary<string, string>? attributes)
        {
            if (attributes == null)
            {
                return null;
            }
            foreach (string name in OffloadedSizeAttributes)
            {
                if (attributes.TryGetValue(name, out string? v) && v != null && SizeRe.IsMatch(v))
                {
                    return long.Parse(v, CultureInfo.InvariantCulture);
                }
            }
            return null;
        }

        private static string? Envelope(object? doc)
        {
            if (doc is Dictionary<string, object> m)
            {
                if (m.TryGetValue("Type", out object? t) && t is string ts && ts == "Notification" && m.ContainsKey("TopicArn"))
                {
                    return "sns";
                }
                if (m.ContainsKey("detail-type") && m.ContainsKey("detail") && m.ContainsKey("source"))
                {
                    return "eventbridge";
                }
            }
            return null;
        }

        internal static bool Md5Matches(string body, string? md5)
        {
            if (string.IsNullOrEmpty(md5))
            {
                return true; // nothing to check against
            }
            try
            {
                byte[] d;
                using (var h = MD5.Create())
                {
                    d = h.ComputeHash(Encoding.UTF8.GetBytes(body));
                }
                var hex = new StringBuilder(32);
                foreach (byte b in d)
                {
                    hex.Append(b.ToString("x2", CultureInfo.InvariantCulture));
                }
                return hex.ToString() == md5!.ToLowerInvariant();
            }
            catch (Exception)
            {
                return false; // MD5 refused (FIPS mode): not shown as captured
            }
        }

        /// <summary>(tenant, source) from the configured message attribute, else the configured body path; else null.</summary>
        internal static (string Tenant, string Source)? Tenant(Config config, IDictionary<string, string>? attributes, string? body)
        {
            if (config.TenantAttribute != null && attributes != null
                && attributes.TryGetValue(config.TenantAttribute, out string? v) && !string.IsNullOrEmpty(v)
                && CodePoints(v!) <= MaxTenantChars)
            {
                return (v!, "message_attribute");
            }
            if (config.TenantBodyPath != null && body != null)
            {
                object? doc = Parse(body);
                foreach (string part in config.TenantBodyPath.Split('.'))
                {
                    doc = doc is Dictionary<string, object> m && m.TryGetValue(part, out object? next) ? next : null;
                }
                string? t = doc is string s ? s : doc is Num num ? NumberText(num.Text) : null;
                if (!string.IsNullOrEmpty(t) && CodePoints(t!) <= MaxTenantChars)
                {
                    return (t!, "body_path");
                }
            }
            return null;
        }

        /// <summary>
        /// A number as .NET prints it (section 15.2): an integer exactly (<c>42</c> is <c>"42"</c>, at any size),
        /// anything else as the shortest round-trip form of the double.
        /// </summary>
        internal static string NumberText(string literal)
        {
            if (literal.IndexOf('.') < 0 && literal.IndexOf('e') < 0 && literal.IndexOf('E') < 0)
            {
                return BigInteger.Parse(literal, NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture).ToString(CultureInfo.InvariantCulture);
            }
            return double.Parse(literal, NumberStyles.Float, CultureInfo.InvariantCulture).ToString("R", CultureInfo.InvariantCulture);
        }

        internal static int CodePoints(string s)
        {
            int n = 0;
            for (int i = 0; i < s.Length; i++)
            {
                if (char.IsHighSurrogate(s[i]) && i + 1 < s.Length && char.IsLowSurrogate(s[i + 1]))
                {
                    i++;
                }
                n++;
            }
            return n;
        }

        /// <summary>
        /// Section 15 attributes for one message on a send or receive span, in insertion order. Values are
        /// <c>long</c> or <c>string</c>.
        /// </summary>
        /// <param name="attributes">the message attributes' <c>StringValue</c>s, by name (null if none)</param>
        /// <param name="md5">SQS's MD5 of the body, or null to skip the check</param>
        internal static List<KeyValuePair<string, object>> ContentAttrs(Config config, string? queueName, string? body,
            IDictionary<string, string>? attributes, string? md5)
        {
            var output = new List<KeyValuePair<string, object>>();
            void Add(string k, object v) => output.Add(new KeyValuePair<string, object>(k, v));
            if (body != null)
            {
                Add("messaging.message.body.size", Utf8Length(body));
            }
            var t = Tenant(config, attributes, body);
            if (t != null)
            {
                Add("trailbeam.tenant", t.Value.Tenant);
                Add("trailbeam.tenant.source", t.Value.Source);
            }
            long? offloaded = OffloadedSize(attributes);
            if (offloaded != null && body != null && IsPointerDoc(Parse(body)))
            {
                Add("trailbeam.body.offloaded_size", offloaded.Value);
            }
            if (body == null)
            {
                return output;
            }
            if (!config.BodyOn(queueName))
            {
                Add("trailbeam.body.capture_status", StatusOff);
                return output;
            }
            if (!Md5Matches(body, md5))
            {
                Add("trailbeam.body.capture_status", StatusMd5Mismatch);
                return output;
            }
            RedactedText r;
            try
            {
                r = RedactBody(body, config);
                if (HasLoneSurrogate(r.Text))
                {
                    throw new RedactionException("lone surrogate");
                }
            }
            catch (Exception)
            {
                Add("trailbeam.body.capture_status", StatusRedactionFailed);
                return output;
            }
            if (Utf8Length(r.Text) > config.MaxBodyBytes)
            {
                Add("trailbeam.body.capture_status", StatusExceedsLimit);
                return output;
            }
            object? doc = r.Format == "json" ? Parse(body) : null;
            Add("trailbeam.body.capture_status", IsPointerDoc(doc) ? StatusOffloaded : StatusCaptured);
            Add("trailbeam.body", r.Text);
            Add("trailbeam.body.format", r.Format);
            Add("trailbeam.body.redacted_count", (long)r.Count);
            string? env = Envelope(doc);
            if (env != null)
            {
                Add("trailbeam.body.envelope", env);
            }
            return output;
        }

        /// <summary>The error's message, redacted and cut to 1,024 code points, if error text is on for the queue (15.5).</summary>
        internal static string? ErrorMessage(Config config, string? queueName, string? message)
        {
            if (!config.ErrorsOn(queueName) || config.Invalid != null || string.IsNullOrEmpty(message))
            {
                return null;
            }
            try
            {
                string text = RedactText(message!, config).Text;
                if (CodePoints(text) > MaxErrorMessageChars)
                {
                    int end = 0;
                    for (int n = 0; n < MaxErrorMessageChars; n++)
                    {
                        end += char.IsHighSurrogate(text[end]) && end + 1 < text.Length && char.IsLowSurrogate(text[end + 1]) ? 2 : 1;
                    }
                    text = text.Substring(0, end);
                }
                return HasLoneSurrogate(text) ? null : text; // fail closed: no text rather than unredacted text
            }
            catch (Exception)
            {
                return null;
            }
        }

        internal static string AsciiLower(string s)
        {
            char[] c = s.ToCharArray();
            for (int i = 0; i < c.Length; i++)
            {
                if (c[i] >= 'A' && c[i] <= 'Z')
                {
                    c[i] = (char)(c[i] + 32);
                }
            }
            return new string(c);
        }

        /// <summary>A key name as a regular expression that matches it with ASCII case folding only.</summary>
        private static string AsciiCaseless(string key)
        {
            var sb = new StringBuilder();
            foreach (char ch in AsciiLower(key))
            {
                if (ch >= 'a' && ch <= 'z')
                {
                    sb.Append('[').Append(ch).Append((char)(ch - 32)).Append(']');
                }
                else if (ch == '.' || ch == '-')
                {
                    sb.Append('\\').Append(ch);
                }
                else
                {
                    sb.Append(ch);
                }
            }
            return sb.ToString();
        }
    }
}
