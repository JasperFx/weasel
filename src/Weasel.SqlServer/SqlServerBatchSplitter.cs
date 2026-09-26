using System.Text.RegularExpressions;

namespace Weasel.SqlServer;

/// <summary>
///     Splits rendered SQL Server DDL into the batches a <c>GO</c> line separates, so each one can be
///     sent as its own command. <c>GO</c> is a sqlcmd directive rather than T-SQL, and SqlClient will
///     raise "Incorrect syntax near 'GO'" if it is left in the text.
/// </summary>
/// <remarks>
///     The semantics are sqlcmd's, deliberately including its bluntness: a line reading only
///     <c>GO</c> ends the batch wherever it appears, because sqlcmd does not parse string literals or
///     comments and neither does this. A definition that needs a line of its own reading <c>GO</c>
///     inside a literal is not supported. The optional repeat count is accepted and ignored, so the
///     batch runs once: nothing in Weasel emits a count, and repeating DDL is never what a migration
///     means.
/// </remarks>
public static class SqlServerBatchSplitter
{
    /// <summary>
    ///     A batch separator: a line whose entire content is <c>GO</c>, optionally followed by a repeat
    ///     count and/or a semicolon. Case insensitive because <c>go</c> is equally valid, and multiline
    ///     so that <c>^</c> and <c>$</c> mean line boundaries. The group around the count is
    ///     non-capturing on purpose: <see cref="Regex.Split(string)" /> injects captured groups into its
    ///     output, which would emit the count as if it were a batch.
    /// </summary>
    /// <remarks>
    ///     Every whitespace class here is horizontal, <c>[ \t]</c> rather than <c>\s</c>, so the whole
    ///     match stays on the separator's own line. With <c>\s</c> the optional count and the trailing
    ///     run could both cross a newline, and <c>GO</c> followed by a line reading only <c>5</c> would
    ///     swallow that line as if the digit were a repeat count on the <c>GO</c>. The explicit
    ///     <c>\r?</c> is what <c>\s</c> was quietly doing for CRLF input: <c>$</c> matches before the
    ///     <c>\n</c>, not before the <c>\r\n</c> pair, so the carriage return has to be consumed here.
    /// </remarks>
    private static readonly Regex _separator = new(
        @"^[ \t]*GO(?:[ \t]+\d+)?[ \t]*;?[ \t]*\r?$",
        RegexOptions.Multiline | RegexOptions.IgnoreCase | RegexOptions.Compiled);

    /// <summary>
    ///     The non-empty batches of <paramref name="sql" />, in order, each trimmed of surrounding
    ///     whitespace but otherwise verbatim.
    /// </summary>
    public static IReadOnlyList<string> Split(string sql)
    {
        if (string.IsNullOrWhiteSpace(sql))
        {
            return [];
        }

        var batches = new List<string>();

        foreach (var candidate in _separator.Split(sql))
        {
            var batch = candidate.Trim();
            if (batch.Length > 0)
            {
                batches.Add(batch);
            }
        }

        return batches;
    }
}
