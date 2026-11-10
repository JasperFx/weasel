#nullable enable
using System.Data.Common;
using System.Runtime.ExceptionServices;
using JasperFx.Core.Exceptions;

namespace Weasel.Storage;

/// <summary>
///     How <see cref="OperationBatchExecutor" /> walks the result sets of one batch.
/// </summary>
public enum ResultSetWalk
{
    /// <summary>
    ///     Every operation owns exactly one result set: postprocess it, advance, and after the last
    ///     operation drain whatever is left. The drain matters on SQL Server, which surfaces a
    ///     per-statement error (a unique index, a foreign key) only when the reader advances into that
    ///     statement's result set — undrained, it is swallowed by the reader's disposal.
    /// </summary>
    AdvanceAfterEach,

    /// <summary>
    ///     An operation marked <see cref="NoDataReturnedCall" /> owns no result set and is skipped
    ///     entirely; every other operation is postprocessed and then advanced past. No drain. Correct
    ///     only as long as the marker is truthful: a marked operation whose SQL does return rows
    ///     misaligns every operation after it.
    /// </summary>
    SkipNoDataReturned
}

/// <summary>
///     The dialect-neutral part of executing a unit of work's storage operations: configure them into a
///     command builder, walk the reader postprocessing each, map provider exceptions through the
///     operations' <see cref="IExceptionTransform" />, and throw what the postprocessing collected.
/// </summary>
/// <remarks>
///     <para>
///         The store keeps everything that is its own: opening the reader (connection, transaction,
///         resilience, logging) and paging. The executor never opens or closes a connection.
///     </para>
///     <para>
///         Two execution shapes are supported, because the stores genuinely differ. A store that sends
///         the whole batch at once calls <see cref="Configure" /> then <see cref="ExecuteAsync" /> with a
///         <see cref="ResultSetWalk" />. A store whose provider is better served by one command per
///         operation (SQLite, where binding N statements' parameters on one command is quadratic) calls
///         <see cref="ExecuteEachAsync" />.
///     </para>
/// </remarks>
public static class OperationBatchExecutor
{
    /// <summary>
    ///     Configure every operation into <paramref name="builder" />, separated by
    ///     <see cref="Weasel.Core.ICommandBuilder.StartNewCommand" />.
    /// </summary>
    /// <remarks>
    ///     ⚠️ This adds no terminator of its own, so <paramref name="builder" /> must be one whose
    ///     <see cref="Weasel.Core.ICommandBuilder.StartNewCommand" /> really starts a new command: a
    ///     <c>DbBatch</c> builder (Weasel.Postgresql's or Weasel.SqlServer's <c>BatchBuilder</c>), or the
    ///     Oracle and Firebird builders. On a builder that concatenates into one command, where
    ///     <c>StartNewCommand</c> is a no-op, an operation that does not terminate its own statement —
    ///     <see cref="QueuedSqlOperation" /> and <see cref="WhereStorageOperation" /> do not — runs
    ///     straight into the next one (the weasel#515 shape). Adding a <c>;</c> here is not the fix:
    ///     Oracle and Firebird reject it. On such a provider, use <see cref="ExecuteEachAsync" />.
    /// </remarks>
    public static void Configure(Weasel.Core.ICommandBuilder builder, IReadOnlyList<IStorageOperation> operations,
        IStorageSession session)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(operations);

        for (var i = 0; i < operations.Count; i++)
        {
            if (i > 0)
            {
                builder.StartNewCommand();
            }

            operations[i].ConfigureCommand(builder, session);
        }
    }

    /// <summary>
    ///     Open the batch's reader through <paramref name="executeReader" />, postprocess every operation
    ///     against it, and throw what went wrong.
    /// </summary>
    /// <remarks>
    ///     A provider exception thrown while executing or walking the reader is offered to each
    ///     operation's <see cref="IExceptionTransform" /> in order, and the first that accepts it wins;
    ///     the original is rethrown with its stack trace when none does. Exceptions an operation's
    ///     postprocessing <em>collects</em> (a concurrency miss, say) are thrown after the walk: one as
    ///     itself, several as an <see cref="AggregateException" />.
    /// </remarks>
    public static async Task ExecuteAsync(IReadOnlyList<IStorageOperation> operations,
        Func<CancellationToken, Task<DbDataReader>> executeReader, ResultSetWalk walk, CancellationToken token)
    {
        ArgumentNullException.ThrowIfNull(operations);
        ArgumentNullException.ThrowIfNull(executeReader);

        if (operations.Count == 0)
        {
            return;
        }

        var exceptions = new List<Exception>();

        try
        {
            var reader = await executeReader(token).ConfigureAwait(false);
            await using (reader.ConfigureAwait(false))
            {
                await PostprocessAsync(reader, operations, exceptions, walk, token).ConfigureAwait(false);
            }
        }
        catch (Exception e) when (TryTransform(operations, e, out var transformed))
        {
            ExceptionDispatchInfo.Capture(transformed).Throw();
        }

        ThrowIfAny(exceptions);
    }

    /// <summary>
    ///     Execute the operations one at a time, each through its own reader, in order.
    /// </summary>
    /// <remarks>
    ///     <paramref name="executeOne" /> configures, compiles and executes one operation however the
    ///     store likes (reusing a prepared command, logging); this postprocesses the reader it returns and
    ///     disposes it. A provider exception is offered to <em>that</em> operation's
    ///     <see cref="IExceptionTransform" /> only, and stops the run.
    /// </remarks>
    public static async Task ExecuteEachAsync(IReadOnlyList<IStorageOperation> operations,
        Func<IStorageOperation, CancellationToken, Task<DbDataReader>> executeOne, CancellationToken token)
    {
        ArgumentNullException.ThrowIfNull(operations);
        ArgumentNullException.ThrowIfNull(executeOne);

        var exceptions = new List<Exception>();

        foreach (var operation in operations)
        {
            try
            {
                var reader = await executeOne(operation, token).ConfigureAwait(false);
                await using (reader.ConfigureAwait(false))
                {
                    await operation.PostprocessAsync(reader, exceptions, token).ConfigureAwait(false);
                }
            }
            catch (Exception e) when (TryTransform(operation, e, out var transformed))
            {
                ExceptionDispatchInfo.Capture(transformed).Throw();
            }
        }

        ThrowIfAny(exceptions);
    }

    /// <summary>
    ///     Postprocess every operation against one multi-result reader, advancing per
    ///     <paramref name="walk" />. Exceptions from the reader propagate untransformed; postprocessing
    ///     collects its own into <paramref name="exceptions" />.
    /// </summary>
    public static async Task PostprocessAsync(DbDataReader reader, IReadOnlyList<IStorageOperation> operations,
        IList<Exception> exceptions, ResultSetWalk walk, CancellationToken token)
    {
        ArgumentNullException.ThrowIfNull(reader);
        ArgumentNullException.ThrowIfNull(operations);
        ArgumentNullException.ThrowIfNull(exceptions);

        for (var i = 0; i < operations.Count; i++)
        {
            var operation = operations[i];

            if (walk == ResultSetWalk.SkipNoDataReturned && operation is NoDataReturnedCall)
            {
                continue;
            }

            await operation.PostprocessAsync(reader, exceptions, token).ConfigureAwait(false);
            await reader.NextResultAsync(token).ConfigureAwait(false);
        }

        if (walk == ResultSetWalk.AdvanceAfterEach)
        {
            while (await reader.NextResultAsync(token).ConfigureAwait(false))
            {
            }
        }
    }

    /// <summary>
    ///     Offer <paramref name="exception" /> to each operation's <see cref="IExceptionTransform" />, in
    ///     order; the first that maps it wins.
    /// </summary>
    public static bool TryTransform(IEnumerable<IStorageOperation> operations, Exception exception,
        out Exception transformed)
    {
        foreach (var operation in operations)
        {
            if (TryTransform(operation, exception, out transformed))
            {
                return true;
            }
        }

        transformed = exception;
        return false;
    }

    /// <summary>
    ///     Offer <paramref name="exception" /> to one operation's <see cref="IExceptionTransform" />.
    /// </summary>
    public static bool TryTransform(IStorageOperation operation, Exception exception, out Exception transformed)
    {
        if (operation is IExceptionTransform transform
            && transform.TryTransform(exception, out var mapped) && mapped is not null)
        {
            transformed = mapped;
            return true;
        }

        transformed = exception;
        return false;
    }

    /// <summary>
    ///     Throw what postprocessing collected: nothing when empty, the exception itself (with its
    ///     original stack trace) when there is one, an <see cref="AggregateException" /> otherwise.
    /// </summary>
    public static void ThrowIfAny(IList<Exception> exceptions)
    {
        ArgumentNullException.ThrowIfNull(exceptions);

        if (exceptions.Count == 1)
        {
            ExceptionDispatchInfo.Capture(exceptions[0]).Throw();
        }

        if (exceptions.Count > 1)
        {
            throw new AggregateException(exceptions);
        }
    }
}
