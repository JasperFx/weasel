using System.Text.RegularExpressions;

namespace Weasel.Core;

/// <summary>
///     A named table-level CHECK constraint. Weasel compares check constraints
///     conservatively: only tables that declare at least one expected check
///     participate in check-constraint delta detection, and actual constraints
///     unknown to the expected table are never dropped (inline column checks
///     and third-party constraints stay untouched).
/// </summary>
public class TableCheckConstraint: INamed
{
    public TableCheckConstraint(string name, string expression)
    {
        if (string.IsNullOrWhiteSpace(name))
        {
            throw new ArgumentOutOfRangeException(nameof(name));
        }

        if (string.IsNullOrWhiteSpace(expression))
        {
            throw new ArgumentOutOfRangeException(nameof(expression));
        }

        Name = name;
        Expression = expression;
    }

    public string Name { get; set; }

    /// <summary>The boolean SQL expression, without the CHECK (...) wrapper</summary>
    public string Expression { get; set; }

    /// <summary>
    ///     Normalize a check expression for comparison against the database
    ///     catalog's canonicalized rendering: strips CHECK wrappers, quoting,
    ///     parens, ::casts and whitespace differences, then lowercases.
    /// </summary>
    public static string Canonicalize(string expression)
    {
        var normalized = expression.Trim();

        if (normalized.StartsWith("CHECK", StringComparison.OrdinalIgnoreCase))
        {
            normalized = normalized[5..].Trim();
        }

        // strip ::type casts (PostgreSQL canonical output)
        normalized = Regex.Replace(normalized, "::\"?[a-zA-Z_][a-zA-Z_ 0-9]*\"?(\\([0-9, ]*\\))?(\\[\\])?", "");
        // strip identifier quoting, grouping noise, and all whitespace — the
        // canonical form is only ever compared for equality, and the catalogs
        // rewrite operator spacing freely ("[Price] > 0" vs "([Price]>(0))")
        normalized = normalized
            .Replace("\"", "")
            .Replace("[", "")
            .Replace("]", "")
            .Replace("(", "")
            .Replace(")", "");
        normalized = Regex.Replace(normalized, @"\s+", "");

        return normalized.ToLowerInvariant();
    }

    public bool Matches(TableCheckConstraint other)
        => Name.Equals(other.Name, StringComparison.OrdinalIgnoreCase)
           && Canonicalize(Expression) == Canonicalize(other.Expression);
}
