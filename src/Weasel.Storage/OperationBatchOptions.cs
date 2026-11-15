#nullable enable
namespace Weasel.Storage;

/// <summary>
///     What <see cref="OperationBatchExecutor" /> does when a batch's reader has more result sets than its
///     operations declare.
/// </summary>
public enum SurplusResultSetPolicy
{
    /// <summary>
    ///     Throw a <see cref="SurplusResultSetsException" /> once the reader has been drained, inside the
    ///     caller's transaction, so the batch rolls back.
    /// </summary>
    Throw,

    /// <summary>
    ///     Drain the surplus silently. What the <see cref="ResultSetWalk" />-taking overloads have always
    ///     done, and still do.
    /// </summary>
    Ignore
}

/// <summary>
///     How <see cref="OperationBatchExecutor" /> walks, checks and reports one batch. Immutable once built,
///     so a store can keep one per configuration and share it across sessions.
/// </summary>
public sealed class OperationBatchOptions
{
    /// <summary>
    ///     <see cref="ResultSetWalk.SkipNoDataReturned" />, <see cref="SurplusResultSetPolicy.Throw" />, no
    ///     listener.
    /// </summary>
    public static OperationBatchOptions Default { get; } = new();

    /// <summary>
    ///     How the result sets are handed to the operations. <see cref="ResultSetWalk.SkipNoDataReturned" />
    ///     by default, because <c>SqlBatch</c>, <c>NpgsqlBatch</c> and Microsoft.Data.Sqlite all surface a
    ///     result set only for a statement that returns columns.
    /// </summary>
    public ResultSetWalk Walk { get; init; } = ResultSetWalk.SkipNoDataReturned;

    /// <summary>
    ///     What to do when the drained reader turns out to have had more result sets than the operations
    ///     own (polecat#831). <see cref="SurplusResultSetPolicy.Throw" /> by default. Applies to a
    ///     multi-operation walk only: under <c>ExecuteEachAsync</c> every operation has a reader of its
    ///     own and cannot be misaligned by another.
    /// </summary>
    public SurplusResultSetPolicy SurplusResultSets { get; init; } = SurplusResultSetPolicy.Throw;

    /// <summary>
    ///     Told about every command the executor runs — see <see cref="IOperationBatchListener" />.
    ///     <see langword="null" /> by default, which costs nothing.
    /// </summary>
    public IOperationBatchListener? Listener { get; init; }

    /// <summary>
    ///     Whether a <see cref="System.Data.Common.DbBatchCommand" /> has completed in the batch now
    ///     executing: <see langword="true" /> if it has, <see langword="false" /> if it has not,
    ///     <see langword="null" /> when the command says nothing either way — a statement that cannot fail,
    ///     such as an empty one, whose completion the driver may not record. <see langword="null" /> by
    ///     default.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         Used only by the <see cref="System.Data.Common.DbBatch" /> overload of
    ///         <see cref="OperationBatchExecutor" />'s <c>ExecuteAsync</c>, only when the driver does not name
    ///         the failing command, and only when an error surfaced while the reader was executing several
    ///         statements without a result set of their own. The driver runs them in order and stops at the
    ///         first error, so the first that never completed is the failing one, and only its operation may
    ///         translate it (weasel#832). A command answering <see langword="null" /> is passed over. Without
    ///         this, or when no command in the run answers <see langword="false" />, those operations are
    ///         asked in statement order.
    ///     </para>
    ///     <para>
    ///         For Npgsql, pass <c>Weasel.Postgresql.NpgsqlStatementCompletion.HasCompleted</c>. Npgsql never
    ///         resets a command's completion, so the executor ignores this for a batch in which any command
    ///         already reports itself complete before it runs: a re-executed batch falls back to statement
    ///         order. A batch re-executed <em>inside</em> the store's <c>executeReader</c> callback, by a
    ///         retry, looks to the executor like a first execution; such a store should leave this unset.
    ///     </para>
    /// </remarks>
    public Func<System.Data.Common.DbBatchCommand, bool?>? StatementCompletion { get; init; }
}

/// <summary>
///     A batch's reader returned more result sets than its operations declare, so an operation may have
///     read another statement's rows: a stale write that reads somebody else's row reports success, and a
///     valid one that reads nothing reports a conflict.
/// </summary>
/// <remarks>
///     The usual cause is a statement marked <see cref="NoDataReturnedCall" /> whose SQL does return rows
///     — queued SQL with a <c>SELECT</c> or <c>OUTPUT</c> — or a wrapper that hid a marker without
///     implementing <see cref="IStorageOperationWrapper" />. Thrown only after the reader is drained, so a
///     SQL error anywhere in the batch is reported instead, being the better answer. A batch in which no
///     operation reads a result set has nothing to misread, and does not throw.
/// </remarks>
public sealed class SurplusResultSetsException: InvalidOperationException
{
    /// <summary>Report <paramref name="surplus" /> result sets nobody declared.</summary>
    public SurplusResultSetsException(int surplus)
        : base($"This batch returned {surplus} more result set(s) than its operations declare, so an operation "
               + "may have read another statement's rows (a concurrency check among them). An operation marked "
               + "NoDataReturnedCall must not return a result set: remove its SELECT or OUTPUT clause, run it "
               + "outside the batch, or drop the marker. A wrapping operation must implement "
               + "IStorageOperationWrapper so the marker on what it wraps is seen.")
    {
        SurplusResultSets = surplus;
    }

    /// <summary>How many result sets were left over after every operation had its own.</summary>
    public int SurplusResultSets { get; }
}
