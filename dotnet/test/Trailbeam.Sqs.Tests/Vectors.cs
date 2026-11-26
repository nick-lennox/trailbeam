using System;
using System.Collections.Generic;
using System.IO;
using System.Text.Json;

namespace Trailbeam.Sqs.Tests
{
    /// <summary>contract/span-contract-vectors.json, the vectors every package is tested against.</summary>
    internal static class Vectors
    {
        private static readonly Lazy<JsonElement> Root = new Lazy<JsonElement>(() =>
        {
            string dir = AppContext.BaseDirectory;
            while (dir != null && !File.Exists(Path.Combine(dir, "contract", "span-contract-vectors.json")))
            {
                dir = Path.GetDirectoryName(dir)!;
            }
            if (dir == null)
            {
                throw new FileNotFoundException("contract/span-contract-vectors.json above " + AppContext.BaseDirectory);
            }
            return JsonDocument.Parse(File.ReadAllText(Path.Combine(dir, "contract", "span-contract-vectors.json"))).RootElement;
        });

        internal static JsonElement Get(string name) => Root.Value.GetProperty(name);

        internal static string? Str(this JsonElement e, string name) =>
            e.ValueKind == JsonValueKind.Object && e.TryGetProperty(name, out var v) && v.ValueKind == JsonValueKind.String ? Text(v) : null;

        /// <summary>
        /// The string's value. System.Text.Json refuses an escaped lone surrogate (<c>"\ud800x"</c>, QV-549), which SQS
        /// can still deliver, so such a string is read with the package's own parser, which keeps it as .NET does.
        /// </summary>
        private static string Text(JsonElement v)
        {
            try
            {
                return v.GetString()!;
            }
            catch (InvalidOperationException)
            {
                return (string)Capture.Parse(v.GetRawText())!;
            }
        }

        internal static List<string> Strings(this JsonElement e, string name)
        {
            var output = new List<string>();
            if (e.ValueKind == JsonValueKind.Object && e.TryGetProperty(name, out var v) && v.ValueKind == JsonValueKind.Array)
            {
                foreach (var x in v.EnumerateArray())
                {
                    output.Add(x.GetString()!);
                }
            }
            return output;
        }

        internal static bool Has(this JsonElement e, string name) =>
            e.ValueKind == JsonValueKind.Object && e.TryGetProperty(name, out var v) && v.ValueKind != JsonValueKind.Null;
    }
}
