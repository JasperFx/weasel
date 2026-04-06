namespace Weasel.Postgresql;

/// <summary>
/// Utility for ensuring PostgreSQL identifiers (table names, index names, FK names, etc.)
/// stay within the NAMEDATALEN limit (default 63 characters). When a name exceeds the limit,
/// it is truncated and a deterministic hash suffix is appended to avoid collisions.
/// </summary>
public static class PostgresqlIdentifier
{
    /// <summary>
    /// Default PostgreSQL NAMEDATALEN - 1 = 63 characters.
    /// </summary>
    public const int DefaultMaxLength = 63;

    /// <summary>
    /// Shorten an identifier name to fit within the PostgreSQL NAMEDATALEN limit.
    /// If the name is already short enough, it is returned unchanged.
    /// If it exceeds the limit, the name is truncated and a deterministic 4-character
    /// hex hash suffix is appended (e.g., "very_long_name" → "very_lon_a3f2").
    /// </summary>
    /// <param name="name">The identifier name to potentially shorten</param>
    /// <param name="maxLength">Maximum allowed length (default: 63)</param>
    /// <returns>The original name if short enough, or a deterministically shortened version</returns>
    public static string Shorten(string name, int maxLength = DefaultMaxLength)
    {
        if (name.Length <= maxLength) return name;

        // Reserve 5 characters for "_" + 4-char hex hash
        var hashSuffix = computeShortHash(name);
        var truncatedLength = maxLength - 5;
        return name[..truncatedLength] + "_" + hashSuffix;
    }

    /// <summary>
    /// Compute a deterministic 4-character hex hash from the full identifier name.
    /// Uses FNV-1a 32-bit hash, truncated to 16 bits for a compact suffix.
    /// The same input always produces the same output.
    /// </summary>
    private static string computeShortHash(string value)
    {
        unchecked
        {
            uint hash = 2166136261;
            foreach (var c in value)
            {
                hash ^= c;
                hash *= 16777619;
            }

            return (hash & 0xFFFF).ToString("x4");
        }
    }
}
