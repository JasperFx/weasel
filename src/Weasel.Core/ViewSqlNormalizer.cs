using System.Text;

namespace Weasel.Core;

/// <summary>
///     Normalizes a view body for comparison against the text a database hands back.
///     Inside a literal, every character is copied verbatim: <c>'active'</c> and <c>'ACTIVE'</c>
///     select different rows, and so do <c>'a b'</c> and <c>'ab'</c>.
/// </summary>
/// <remarks>
///     Delimited identifiers and comments get their own branches for one reason only: an
///     apostrophe is ordinary text inside <c>[Customer's Name]</c>, <c>"o'brien"</c>,
///     <c>`o'brien`</c> and prose, and must not open a literal there. Their contents are folded
///     like any other text outside a literal, so identifier case and spacing keep comparing equal
///     as they always have.
/// </remarks>
public static class ViewSqlNormalizer
{
    public static string Normalize(string sql)
    {
        if (string.IsNullOrEmpty(sql))
        {
            return string.Empty;
        }

        var builder = new StringBuilder(sql.Length);

        for (var i = 0; i < sql.Length; i++)
        {
            var c = sql[i];

            if (c == '\'')
            {
                i = appendLiteral(sql, i, builder);
                continue;
            }

            if (c == '"' || c == '[' || c == '`')
            {
                i = appendDelimitedIdentifier(sql, i, builder);
                continue;
            }

            if (c == '-' && i + 1 < sql.Length && sql[i + 1] == '-')
            {
                i = appendLineComment(sql, i, builder);
                continue;
            }

            if (c == '/' && i + 1 < sql.Length && sql[i + 1] == '*')
            {
                i = appendBlockComment(sql, i, builder);
                continue;
            }

            appendOutsideLiteral(builder, c);
        }

        return builder.ToString().TrimEnd(';');
    }

    private static void appendOutsideLiteral(StringBuilder builder, char c)
    {
        if (!char.IsWhiteSpace(c))
        {
            builder.Append(char.ToUpperInvariant(c));
        }
    }

    private static int appendLiteral(string sql, int start, StringBuilder builder)
    {
        builder.Append('\'');

        var i = start + 1;
        while (i < sql.Length)
        {
            if (sql[i] == '\'')
            {
                if (i + 1 < sql.Length && sql[i + 1] == '\'')
                {
                    builder.Append("''");
                    i += 2;
                    continue;
                }

                builder.Append('\'');
                return i;
            }

            builder.Append(sql[i]);
            i++;
        }

        return sql.Length - 1;
    }

    private static int appendDelimitedIdentifier(string sql, int start, StringBuilder builder)
    {
        var open = sql[start];
        var close = open == '[' ? ']' : open;

        builder.Append(open);

        var i = start + 1;
        while (i < sql.Length)
        {
            if (sql[i] == close)
            {
                builder.Append(close);

                if (i + 1 < sql.Length && sql[i + 1] == close)
                {
                    builder.Append(close);
                    i += 2;
                    continue;
                }

                return i;
            }

            appendOutsideLiteral(builder, sql[i]);
            i++;
        }

        return sql.Length - 1;
    }

    /// <summary>
    ///     Comment bodies are folded like any other text outside a literal, but a quote inside one
    ///     must not open a literal — an apostrophe in prose would otherwise swallow the rest of the
    ///     statement.
    /// </summary>
    private static int appendLineComment(string sql, int start, StringBuilder builder)
    {
        var i = start;
        while (i < sql.Length && sql[i] != '\n' && sql[i] != '\r')
        {
            appendOutsideLiteral(builder, sql[i]);
            i++;
        }

        return i - 1;
    }

    private static int appendBlockComment(string sql, int start, StringBuilder builder)
    {
        var i = start;
        while (i < sql.Length)
        {
            appendOutsideLiteral(builder, sql[i]);

            if (i > start + 2 && sql[i] == '/' && sql[i - 1] == '*')
            {
                return i;
            }

            i++;
        }

        return sql.Length - 1;
    }
}
