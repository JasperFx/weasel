namespace Weasel.Core;

/// <summary>
///     Splits a stored <c>CREATE VIEW … AS …</c> text into header and body.
/// </summary>
/// <remarks>
///     Only the providers whose catalog hands back the whole <c>CREATE VIEW</c> statement need this
///     -- SQL Server's <c>sys.sql_modules</c> and SQLite's <c>sqlite_master</c>. PostgreSQL's
///     <c>pg_get_viewdef</c> and Oracle's <c>all_views.text</c> return the body already, so they do
///     not call it. It lives here rather than in either provider because both had a copy and both
///     copies had the same defect; which characters delimit an identifier is passed in, the way
///     <see cref="IdentifierValidation" /> takes its unsafe characters.
/// </remarks>
public static class ViewDefinition
{
    /// <summary>
    ///     The body of a view, given the whole <c>CREATE VIEW</c> text a catalog hands back.
    /// </summary>
    /// <param name="definition">The stored <c>CREATE VIEW … AS …</c> text.</param>
    /// <param name="identifierDelimiters">
    ///     The characters that open a delimited identifier in this dialect: <c>"</c> everywhere,
    ///     <c>[</c> on SQL Server, <c>`</c> where MySQL syntax is accepted. A single quote is always
    ///     treated as a string literal and does not need to be listed.
    /// </param>
    /// <remarks>
    ///     Searching for the first <c>" AS "</c> is wrong whenever the view's own <c>AS</c> stands on
    ///     a line by itself, because the first match is then a column alias inside the SELECT list and
    ///     the body is cut short. Both sides of a delta go through this same extraction, so the
    ///     truncation cancels out and the comparison reports no drift while the rebuilt view fails to
    ///     be created at all.
    ///
    ///     The separator is instead the first <c>AS</c> that is a word of its own and is not nested
    ///     inside parentheses, skipping the three things a view header may legally contain ahead of
    ///     it: comments, delimited identifiers, and a declared column list.
    /// </remarks>
    public static string ExtractBody(string definition, string identifierDelimiters)
    {
        if (string.IsNullOrEmpty(definition))
        {
            return definition;
        }

        var depth = 0;
        var i = 0;

        while (i < definition.Length)
        {
            if (SkippedComment(definition, ref i) || SkippedDelimited(definition, identifierDelimiters, ref i))
            {
                continue;
            }

            switch (definition[i])
            {
                case '(':
                    depth++;
                    break;

                case ')':
                    depth--;
                    break;

                default:
                    if (depth == 0 && IsBareAsAt(definition, i))
                    {
                        return definition[(i + 2)..].Trim();
                    }

                    break;
            }

            i++;
        }

        return definition;
    }

    private static bool SkippedComment(string sql, ref int i)
    {
        if (i + 1 >= sql.Length || sql[i] != '-' && sql[i] != '/')
        {
            return false;
        }

        if (sql[i] == '-' && sql[i + 1] == '-')
        {
            while (i < sql.Length && sql[i] != '\n')
            {
                i++;
            }

            return true;
        }

        if (sql[i] == '/' && sql[i + 1] == '*')
        {
            i += 2;
            while (i + 1 < sql.Length && !(sql[i] == '*' && sql[i + 1] == '/'))
            {
                i++;
            }

            i = Math.Min(i + 2, sql.Length);
            return true;
        }

        return false;
    }

    private static bool SkippedDelimited(string sql, string identifierDelimiters, ref int i)
    {
        var opening = sql[i];
        if (opening != '\'' && !identifierDelimiters.Contains(opening))
        {
            return false;
        }

        var closing = opening == '[' ? ']' : opening;

        i++;
        while (i < sql.Length && sql[i] != closing)
        {
            i++;
        }

        i++;
        return true;
    }

    private static bool IsBareAsAt(string sql, int i)
        => (sql[i] == 'a' || sql[i] == 'A')
           && i + 1 < sql.Length
           && (sql[i + 1] == 's' || sql[i + 1] == 'S')
           && (i == 0 || !IsWordCharacter(sql[i - 1]))
           && (i + 2 >= sql.Length || !IsWordCharacter(sql[i + 2]));

    private static bool IsWordCharacter(char c)
        => char.IsLetterOrDigit(c) || c == '_' || c == '@' || c == '#' || c == '$';
}
