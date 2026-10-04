using System.Text;
using JasperFx.Core;

namespace Weasel.Firebird.Tables;

/// <summary>
///     How an SQL expression the catalog keeps as source text -- an index's <c>COMPUTED BY</c>, a
///     computed column's, a partial index's condition -- is read back and compared.
/// </summary>
internal static class ExpressionText
{
    /// <summary>
    ///     Source as the catalog stores it, <c>(UPPER(NAME))</c>, without the outer parentheses the
    ///     server keeps -- and only those, not the ones that close part of the way in.
    /// </summary>
    public static string? WithoutOuterParentheses(string? source)
    {
        if (source.IsEmpty())
        {
            return null;
        }

        var text = source!.Trim();
        return text.StartsWith('(') && text.EndsWith(')') && closesAtEnd(text) ? text[1..^1].Trim() : text;
    }

    /// <summary>
    ///     An expression as it is compared: letters folded, whitespace collapsed, and dropped altogether
    ///     beside an operator or a parenthesis -- so <c>a*b</c>, <c>A * B</c> and <c>( a * b )</c> are one
    ///     expression while <c>a or b</c> stays apart from <c>aor b</c>. A string literal or a delimited
    ///     identifier is kept exactly: its case and its spaces are data.
    /// </summary>
    public static string? Canonical(string? expression)
    {
        if (expression.IsEmpty())
        {
            return null;
        }

        var text = expression!.Trim();
        var builder = new StringBuilder(text.Length);
        char? quote = null;

        for (var i = 0; i < text.Length; i++)
        {
            var c = text[i];

            if (quote != null)
            {
                builder.Append(c);
                if (c == quote)
                {
                    quote = null;
                }

                continue;
            }

            if (c is '\'' or '"')
            {
                quote = c;
                builder.Append(c);
                continue;
            }

            if (char.IsWhiteSpace(c))
            {
                while (i + 1 < text.Length && char.IsWhiteSpace(text[i + 1]))
                {
                    i++;
                }

                var before = builder.Length > 0 ? builder[^1] : ' ';
                var after = i + 1 < text.Length ? text[i + 1] : ' ';
                if (isWordCharacter(before) && isWordCharacter(after))
                {
                    builder.Append(' ');
                }

                continue;
            }

            builder.Append(char.ToUpperInvariant(c));
        }

        return builder.ToString();
    }

    private static bool isWordCharacter(char c) => char.IsLetterOrDigit(c) || c is '_' or '$' or '\'' or '"';

    private static bool closesAtEnd(string text)
    {
        var depth = 0;
        for (var i = 0; i < text.Length; i++)
        {
            depth += text[i] switch { '(' => 1, ')' => -1, _ => 0 };
            if (depth == 0 && i < text.Length - 1)
            {
                return false;
            }
        }

        return true;
    }
}
