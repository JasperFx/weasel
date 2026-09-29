using System.Text;
using JasperFx.Core;
using Weasel.Core;

namespace Weasel.SqlServer.Tables;

/// <summary>
///     Canonicalization of an expression for SQL Server, where the catalog reports a definition the
///     server rewrote rather than the one that was declared.
/// </summary>
/// <remarks>
///     <para>
///         The shared <see cref="TableCheckConstraint.Canonicalize" /> strips quoting, parentheses and
///         whitespace and lowercases. That reconciles most of what SQL Server rewrites, but not
///         <c>CAST</c>: the server stores it as <c>CONVERT</c>, with the type moved from the end to the
///         front. Those differ in keyword <em>and</em> argument order, which is beyond what stripping
///         can do, so a model declaring <c>CAST</c> could never match its own column (weasel#637).
///     </para>
///     <para>
///         <c>CAST(x AS t)</c> is rewritten to <c>CONVERT(t, x)</c> before the shared canonicalization,
///         which is the tractable order -- afterwards the parentheses that delimit the cast are gone.
///     </para>
/// </remarks>
internal static class SqlServerExpression
{
    /// <summary>
    ///     <see cref="TableCheckConstraint.Canonicalize" /> with SQL Server's <c>CAST</c> rewrite in
    ///     front of it.
    /// </summary>
    public static string Canonicalize(string expression)
    {
        return TableCheckConstraint.Canonicalize(RewriteCastsToConverts(expression));
    }

    /// <summary>
    ///     Every <c>CAST(x AS t)</c> in <paramref name="expression" /> as <c>CONVERT(t, x)</c>, the
    ///     spelling <c>sys.computed_columns</c> and <c>sys.check_constraints</c> report back.
    /// </summary>
    /// <remarks>
    ///     Scanned rather than matched with a regular expression: the cast target sits at the end,
    ///     after an operand that can itself hold parentheses, string literals and further casts.
    ///     Literals are stepped over so that a <c>'('</c> or the word <c>AS</c> inside a JSON path
    ///     cannot be read as syntax.
    /// </remarks>
    internal static string RewriteCastsToConverts(string expression)
    {
        if (expression.IsEmpty())
        {
            return expression;
        }

        // Each pass rewrites the first CAST it finds, so the count strictly decreases and a cast
        // nested in the operand is picked up by the next pass.
        var current = expression;
        while (TryRewriteFirstCast(current, out var rewritten))
        {
            current = rewritten;
        }

        return current;
    }

    private static bool TryRewriteFirstCast(string expression, out string rewritten)
    {
        rewritten = expression;

        for (var i = 0; i < expression.Length; i++)
        {
            if (IsLiteralOrDelimiterStart(expression, i, out var skipTo))
            {
                i = skipTo;
                continue;
            }

            if (!StartsCastKeyword(expression, i))
            {
                continue;
            }

            var open = IndexOfOpenParen(expression, i + 4);
            if (open < 0)
            {
                continue;
            }

            var close = IndexOfMatchingParen(expression, open);
            if (close < 0)
            {
                continue;
            }

            var separator = IndexOfTopLevelAs(expression, open + 1, close);
            if (separator < 0)
            {
                continue;
            }

            var operand = expression[(open + 1)..separator].Trim();
            var target = expression[(separator + 4)..close].Trim();

            if (operand.IsEmpty() || target.IsEmpty())
            {
                continue;
            }

            rewritten = new StringBuilder(expression.Length)
                .Append(expression, 0, i)
                .Append("CONVERT(")
                .Append(target)
                .Append(", ")
                .Append(operand)
                .Append(')')
                .Append(expression, close + 1, expression.Length - close - 1)
                .ToString();

            return true;
        }

        return false;
    }

    /// <summary>
    ///     A single-quoted literal (<c>''</c> escapes a quote) or a bracket-delimited identifier
    ///     (<c>]]</c> escapes a bracket). <paramref name="skipTo" /> is the closing delimiter.
    /// </summary>
    private static bool IsLiteralOrDelimiterStart(string expression, int index, out int skipTo)
    {
        skipTo = index;

        var opener = expression[index];
        if (opener != '\'' && opener != '[')
        {
            return false;
        }

        var closer = opener == '\'' ? '\'' : ']';

        for (var i = index + 1; i < expression.Length; i++)
        {
            if (expression[i] != closer)
            {
                continue;
            }

            if (i + 1 < expression.Length && expression[i + 1] == closer)
            {
                i++;
                continue;
            }

            skipTo = i;
            return true;
        }

        // Unterminated: treat the rest as opaque rather than reading syntax out of it
        skipTo = expression.Length - 1;
        return true;
    }

    private static bool StartsCastKeyword(string expression, int index)
    {
        if (index + 4 > expression.Length)
        {
            return false;
        }

        if (!expression.AsSpan(index, 4).Equals("CAST".AsSpan(), StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        // A word boundary either side, so "downcast(" and "xCAST(" are not casts
        if (index > 0 && IsWordCharacter(expression[index - 1]))
        {
            return false;
        }

        var after = index + 4;
        return after >= expression.Length || !IsWordCharacter(expression[after]);
    }

    private static bool IsWordCharacter(char c) => char.IsLetterOrDigit(c) || c == '_' || c == '@' || c == '#';

    private static int IndexOfOpenParen(string expression, int from)
    {
        for (var i = from; i < expression.Length; i++)
        {
            if (expression[i] == '(')
            {
                return i;
            }

            if (!char.IsWhiteSpace(expression[i]))
            {
                return -1;
            }
        }

        return -1;
    }

    private static int IndexOfMatchingParen(string expression, int open)
    {
        var depth = 0;

        for (var i = open; i < expression.Length; i++)
        {
            if (IsLiteralOrDelimiterStart(expression, i, out var skipTo))
            {
                i = skipTo;
                continue;
            }

            switch (expression[i])
            {
                case '(':
                    depth++;
                    break;
                case ')':
                    depth--;
                    if (depth == 0)
                    {
                        return i;
                    }

                    break;
            }
        }

        return -1;
    }

    /// <summary>
    ///     The <c>AS</c> that separates this cast's operand from its target: the one at the cast's own
    ///     parenthesis depth, so an <c>AS</c> belonging to a nested cast is passed over.
    /// </summary>
    private static int IndexOfTopLevelAs(string expression, int from, int end)
    {
        var depth = 0;

        for (var i = from; i < end; i++)
        {
            if (IsLiteralOrDelimiterStart(expression, i, out var skipTo))
            {
                i = skipTo;
                continue;
            }

            switch (expression[i])
            {
                case '(':
                    depth++;
                    continue;
                case ')':
                    depth--;
                    continue;
            }

            if (depth != 0 || i + 4 > end)
            {
                continue;
            }

            if (!char.IsWhiteSpace(expression[i]))
            {
                continue;
            }

            if (expression.AsSpan(i + 1, 2).Equals("AS".AsSpan(), StringComparison.OrdinalIgnoreCase)
                && char.IsWhiteSpace(expression[i + 3]))
            {
                return i;
            }
        }

        return -1;
    }
}
