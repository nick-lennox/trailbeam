using System.Security.Cryptography;
using System.Text;

namespace Trailbeam.Sqs
{
    /// <summary>
    /// Keyed hash of receipt handles (contract section 7). Holds the key; never logs or exports it.
    /// <code>
    /// hmac   = lower_hex(HMAC-SHA256(key, receipt_handle))[0:32]
    /// key_id = lower_hex(HMAC-SHA256(key, "trailbeam key id"))[0:8]
    /// </code>
    /// </summary>
    public sealed class ReceiptHasher
    {
        private const string Hex = "0123456789abcdef";
        private readonly byte[] _key;

        public ReceiptHasher(string key)
        {
            _key = Encoding.UTF8.GetBytes(key);
            KeyId = Digest("trailbeam key id", 8);
        }

        /// <summary>The key's ID: the first 8 lowercase hex characters of HMAC-SHA256(key, "trailbeam key id").</summary>
        public string KeyId { get; }

        /// <summary>The first 32 lowercase hex characters of HMAC-SHA256(key, receiptHandle).</summary>
        public string Hash(string receiptHandle) => Digest(receiptHandle, 32);

        private string Digest(string data, int chars)
        {
            byte[] mac;
            using (var h = new HMACSHA256(_key))
            {
                mac = h.ComputeHash(Encoding.UTF8.GetBytes(data));
            }
            var sb = new StringBuilder(chars);
            for (int i = 0; i < chars; i++)
            {
                int b = mac[i / 2];
                sb.Append(Hex[i % 2 == 0 ? b >> 4 : b & 0x0f]);
            }
            return sb.ToString();
        }

        /// <summary>Never shows the key.</summary>
        public override string ToString() => "ReceiptHasher(keyId=" + KeyId + ")";
    }
}
