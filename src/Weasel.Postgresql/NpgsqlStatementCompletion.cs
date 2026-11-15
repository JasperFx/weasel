using System.Data.Common;
using Npgsql;

namespace Weasel.Postgresql;

/// <summary>
///     Reads whether a statement of an <see cref="NpgsqlBatch" /> has completed, for
///     <c>Weasel.Storage.OperationBatchOptions.StatementCompletion</c> (weasel#832).
/// </summary>
/// <remarks>
///     <para>
///         Npgsql 10.0.3 leaves <see cref="DbException.BatchCommand" /> null for an error raised inside an
///         <see cref="NpgsqlBatch" />, whichever statement raised it (measured). What it does record is each
///         statement's <see cref="NpgsqlBatchCommand.StatementType" />, set when the statement's
///         <c>CommandComplete</c> arrives and <see cref="StatementType.Unknown" /> until then. PostgreSQL
///         runs a batch's statements in order and stops at the first error, so on a batch's first
///         execution the failing statement is the first one still <see cref="StatementType.Unknown" />.
///     </para>
///     <para>
///         Npgsql never resets <see cref="NpgsqlBatchCommand.StatementType" />, so the answer describes the
///         batch's latest execution only where that execution got at least as far as every earlier one.
///         <c>Weasel.Storage.OperationBatchExecutor</c> checks for a stale batch before it runs and does not
///         use this then.
///     </para>
/// </remarks>
public static class NpgsqlStatementCompletion
{
    /// <summary>
    ///     <see langword="true" /> when <paramref name="command" /> has completed, <see langword="false" />
    ///     when it has not, <see langword="null" /> when it says nothing either way: it is not an
    ///     <see cref="NpgsqlBatchCommand" />, or its text is empty or only a comment. PostgreSQL answers
    ///     such a statement with <c>EmptyQueryResponse</c> rather than <c>CommandComplete</c>, so it stays
    ///     <see cref="StatementType.Unknown" /> even once it has run (measured) — and it cannot fail.
    /// </summary>
    public static bool? HasCompleted(DbBatchCommand command)
    {
        if (command is not NpgsqlBatchCommand npgsql)
        {
            return null;
        }

        if (npgsql.StatementType != StatementType.Unknown)
        {
            return true;
        }

        var text = npgsql.CommandText.AsSpan().Trim(" \t\r\n;");

        // An empty or comment-only statement answers EmptyQueryResponse, never CommandComplete, so it
        // stays Unknown after it has run. It cannot fail either, so it is no candidate.
        if (text.IsEmpty || text.StartsWith("--") || text.StartsWith("/*"))
        {
            return null;
        }

        return false;
    }
}
