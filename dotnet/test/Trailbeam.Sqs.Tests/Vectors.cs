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
            e.ValueKind == JsonValueKind.Object && e.TryGetProperty(name, out var v) && v.ValueKind == JsonValueKind.String ? v.GetString() : null;

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
