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
    private static readonly Regex _separator = new(
        @"^\s*GO(?:\s+\d+)?\s*;?\s*$",
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
