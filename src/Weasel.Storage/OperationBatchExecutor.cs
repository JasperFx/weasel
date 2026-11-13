#nullable enable
using System.Data.Common;
using System.Runtime.ExceptionServices;
using JasperFx.Core.Exceptions;

namespace Weasel.Storage;

/// <summary>
///     How <see cref="OperationBatchExecutor" /> walks the result sets of one batch.
/// </summary>
/// <remarks>
///     Both walks end by draining the reader: advancing it until it has no more result sets. The drain
///     matters on SQL Server, which surfaces a per-statement error (a unique index, a foreign key, a
///     <c>THROW</c>) only when the reader advances into that statement — undrained, it is swallowed by
///     the reader's disposal and the transaction commits around it — and on Microsoft.Data.Sqlite, which
///     runs a statement the reader never reached when the reader is disposed and swallows its error the
///     same way (both measured). Npgsql throws the error from the disposal; the drain makes the three
///     agree, and puts the error inside the executor, where the transforms and the listener see it.
/// </remarks>
public enum ResultSetWalk
{
    /// <summary>
    ///     Every operation owns exactly one result set: postprocess it, advance, and after the last
    ///     operation drain whatever is left.
    /// </summary>
    AdvanceAfterEach,

    /// <summary>
    ///     An operation that returns no result set — marked <see cref="NoDataReturnedCall" />, itself or
    ///     through an <see cref="IStorageOperationWrapper" /> — owns none and is skipped entirely, its
    ///     <c>PostprocessAsync</c> not called; every other operation is postprocessed against its own
    ///     result set. Then the reader is drained.
    /// </summary>
    /// <remarks>
    ///     Correct only as long as the marker is truthful: a marked operation whose SQL does return rows
    ///     misaligns every operation after it. Through
    ///     <see cref="OperationBatchOptions.SurplusResultSets" /> the executor counts what the drain finds
    ///     and refuses a batch that returned more result sets than its operations declare. Before
    ///     weasel#810 this walk did not drain at all, which lost a later statement's error behind such a
    ///     surplus result set.
    /// </remarks>
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
///         resilience) and paging. The executor never opens or closes a connection. Logging and
///         store-wide exception translation can stay in the store's loop too, or move into an
///         <see cref="IOperationBatchListener" /> passed through <see cref="OperationBatchOptions" />, which
///         sees each command before it runs and after it succeeds or fails.
///     </para>
///     <para>
///         Two execution shapes are supported, because the stores genuinely differ. A store that sends
///         the whole batch at once calls <see cref="Configure" /> then an <c>ExecuteAsync</c> overload. A
///         store whose provider is better served by one command per operation (SQLite, where binding N
///         statements' parameters on one command is quadratic) calls an <c>ExecuteEachAsync</c> overload.
///     </para>
///     <para>
///         The overloads taking a <see cref="ResultSetWalk" /> are the original surface and keep its
///         behaviour, except that the <see cref="ResultSetWalk.SkipNoDataReturned" /> walk now drains the
///         reader. The overloads taking <see cref="OperationBatchOptions" /> add the surplus-result-set
///         guard (on by default), the listener, and — given the <see cref="DbBatch" /> — exception
///         transformation by the operation whose command actually failed.
///     </para>
/// </remarks>
public static class OperationBatchExecutor
{
    // Deeper than any adapter stack a store builds; only a cycle reaches it.
    private const int MaxWrapperDepth = 32;

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
    ///     Oracle and Firebird reject it. On such a provider, use <c>ExecuteEachAsync</c>.
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
    ///     A provider exception thrown while executing, walking or draining the reader is offered to each
    ///     operation's <see cref="IExceptionTransform" /> in order, and the first that accepts it wins;
    ///     the original is rethrown with its stack trace when none does. Exceptions an operation's
    ///     postprocessing <em>collects</em> (a concurrency miss, say) are thrown after the walk: one as
    ///     itself, several as an <see cref="AggregateException" />. Surplus result sets are drained
    ///     silently; the <see cref="OperationBatchOptions" /> overload refuses them.
    /// </remarks>
    public static async Task ExecuteAsync(IReadOnlyList<IStorageOperation> operations,
        Func<CancellationToken, Task<DbDataReader>> executeReader, ResultSetWalk walk, CancellationToken token)
    {
        ArgumentNullException.ThrowIfNull(operations);
        ArgumentNullException.ThrowIfNull(executeReader);

        await executeBatchAsync(operations, null, null, executeReader, walk, SurplusResultSetPolicy.Ignore, null,
            token).ConfigureAwait(false);
    }

    /// <summary>
    ///     Open the batch's reader through <paramref name="executeReader" />, postprocess every operation
    ///     against it per <paramref name="options" />, and throw what went wrong.
    /// </summary>
    /// <remarks>
    ///     As <see cref="ExecuteAsync(IReadOnlyList{IStorageOperation}, Func{CancellationToken, Task{DbDataReader}}, ResultSetWalk, CancellationToken)" />,
    ///     plus <see cref="OperationBatchOptions.SurplusResultSets" /> and
    ///     <see cref="OperationBatchOptions.Listener" />, whose <see cref="OperationBatchCommand" /> carries
    ///     no provider object. A provider exception is offered to the operations' transforms in order,
    ///     then to the listener's.
    /// </remarks>
    public static async Task ExecuteAsync(IReadOnlyList<IStorageOperation> operations,
        Func<CancellationToken, Task<DbDataReader>> executeReader, OperationBatchOptions options,
        CancellationToken token)
    {
        ArgumentNullException.ThrowIfNull(operations);
        ArgumentNullException.ThrowIfNull(executeReader);
        ArgumentNullException.ThrowIfNull(options);

        await executeBatchAsync(operations, null, null, executeReader, options.Walk, options.SurplusResultSets,
            options.Listener, token).ConfigureAwait(false);
    }

    /// <summary>
    ///     Execute <paramref name="batch" />, into which <paramref name="operations" /> were configured one
    ///     batch command each, through <paramref name="executeReader" />; postprocess every operation per
    ///     <paramref name="options" />, and throw what went wrong.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         The listener's <see cref="OperationBatchCommand.Batch" /> is <paramref name="batch" />, so a
    ///         store can log it. <paramref name="executeReader" /> is where the store attaches its
    ///         connection and transaction and applies its resilience; the executor does not dispose the
    ///         batch.
    ///     </para>
    ///     <para>
    ///         A provider exception that names its failing command (<see cref="DbException.BatchCommand" />)
    ///         is offered to <em>that</em> command's operation only, when the batch has exactly one command
    ///         per operation — so a batch that starts a new stream beside an existing one reports the
    ///         collision against the stream that collided, not the first operation able to recognise the
    ///         error (polecat#781). Otherwise every operation is asked in order. The listener is asked after.
    ///         Whether the command is named is the driver's call: Microsoft.Data.SqlClient names it, while
    ///         Npgsql 10.0.3 was measured leaving it null for an error a later statement raised.
    ///     </para>
    /// </remarks>
    public static async Task ExecuteAsync(DbBatch batch, IReadOnlyList<IStorageOperation> operations,
        Func<DbBatch, CancellationToken, Task<DbDataReader>> executeReader, OperationBatchOptions options,
        CancellationToken token)
    {
        ArgumentNullException.ThrowIfNull(batch);
        ArgumentNullException.ThrowIfNull(operations);
        ArgumentNullException.ThrowIfNull(executeReader);
        ArgumentNullException.ThrowIfNull(options);

        await executeBatchAsync(operations, batch, null, t => executeReader(batch, t), options.Walk,
            options.SurplusResultSets, options.Listener, token).ConfigureAwait(false);
    }

    /// <summary>
    ///     Execute <paramref name="command" />, into which all of <paramref name="operations" /> were
    ///     configured, through <paramref name="executeReader" />; postprocess every operation per
    ///     <paramref name="options" />, and throw what went wrong.
    /// </summary>
    /// <remarks>
    ///     For a provider whose builder concatenates statements into one command. The listener's
    ///     <see cref="OperationBatchCommand.Command" /> is <paramref name="command" />; the executor does
    ///     not dispose it. A provider exception is offered to the operations' transforms in order, then to
    ///     the listener's.
    /// </remarks>
    public static async Task ExecuteAsync(DbCommand command, IReadOnlyList<IStorageOperation> operations,
        Func<DbCommand, CancellationToken, Task<DbDataReader>> executeReader, OperationBatchOptions options,
        CancellationToken token)
    {
        ArgumentNullException.ThrowIfNull(command);
        ArgumentNullException.ThrowIfNull(operations);
        ArgumentNullException.ThrowIfNull(executeReader);
        ArgumentNullException.ThrowIfNull(options);

        await executeBatchAsync(operations, null, command, t => executeReader(command, t), options.Walk,
            options.SurplusResultSets, options.Listener, token).ConfigureAwait(false);
    }

    private static async Task executeBatchAsync(IReadOnlyList<IStorageOperation> operations, DbBatch? batch,
        DbCommand? command, Func<CancellationToken, Task<DbDataReader>> executeReader, ResultSetWalk walk,
        SurplusResultSetPolicy surplus, IOperationBatchListener? listener, CancellationToken token)
    {
        if (operations.Count == 0)
        {
            return;
        }

        var context = listener is null ? null : new OperationBatchCommand(operations, batch, command);
        var exceptions = new List<Exception>();

        listener?.BeforeExecute(context!);

        try
        {
            var reader = await executeReader(token).ConfigureAwait(false);
            await using (reader.ConfigureAwait(false))
            {
                await walkAsync(reader, operations, exceptions, walk, surplus, token).ConfigureAwait(false);
            }
        }
        catch (Exception e)
        {
            listener?.OnFailure(context!, e);

            if (tryTransformBatch(operations, batch, e, out var transformed)
                || tryTransformWithListener(listener, context, e, out transformed))
            {
                ExceptionDispatchInfo.Capture(transformed).Throw();
            }

            throw;
        }

        listener?.OnSuccess(context!);

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
    ///     Execute the operations one at a time, each through its own reader, in order, reporting each to
    ///     <see cref="OperationBatchOptions.Listener" />.
    /// </summary>
    /// <remarks>
    ///     As <see cref="ExecuteEachAsync(IReadOnlyList{IStorageOperation}, Func{IStorageOperation, CancellationToken, Task{DbDataReader}}, CancellationToken)" />,
    ///     except that each reader is drained after its operation's postprocessing, so an error from a
    ///     later statement in the same command is surfaced, and a provider exception the operation's own
    ///     transform declines is offered to the listener's. The listener's
    ///     <see cref="OperationBatchCommand" /> carries the one operation and no provider object.
    ///     <see cref="OperationBatchOptions.Walk" /> and <see cref="OperationBatchOptions.SurplusResultSets" />
    ///     do not apply: every operation has its reader to itself.
    /// </remarks>
    public static async Task ExecuteEachAsync(IReadOnlyList<IStorageOperation> operations,
        Func<IStorageOperation, CancellationToken, Task<DbDataReader>> executeOne, OperationBatchOptions options,
        CancellationToken token)
    {
        ArgumentNullException.ThrowIfNull(operations);
        ArgumentNullException.ThrowIfNull(executeOne);
        ArgumentNullException.ThrowIfNull(options);

        await executeEachAsync(operations, null, null, executeOne, options.Listener, token).ConfigureAwait(false);
    }

    /// <summary>
    ///     Execute the operations one at a time, in order: <paramref name="prepare" /> compiles each into a
    ///     command, <paramref name="executeReader" /> runs it, and each command is reported to
    ///     <see cref="OperationBatchOptions.Listener" />.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         The shape of a session loop that logs every statement: the listener's
    ///         <see cref="OperationBatchCommand.Command" /> is what <paramref name="prepare" /> returned, so
    ///         the before, success and failure lines can carry the SQL. The command belongs to the store —
    ///         a prepared statement reused across operations, say — and the executor never disposes it. An
    ///         exception from <paramref name="prepare" /> propagates as itself, unreported: no command ran.
    ///     </para>
    ///     <para>
    ///         Otherwise as
    ///         <see cref="ExecuteEachAsync(IReadOnlyList{IStorageOperation}, Func{IStorageOperation, CancellationToken, Task{DbDataReader}}, OperationBatchOptions, CancellationToken)" />.
    ///     </para>
    /// </remarks>
    public static async Task ExecuteEachAsync(IReadOnlyList<IStorageOperation> operations,
        Func<IStorageOperation, DbCommand> prepare, Func<DbCommand, CancellationToken, Task<DbDataReader>> executeReader,
        OperationBatchOptions options, CancellationToken token)
    {
        ArgumentNullException.ThrowIfNull(operations);
        ArgumentNullException.ThrowIfNull(prepare);
        ArgumentNullException.ThrowIfNull(executeReader);
        ArgumentNullException.ThrowIfNull(options);

        await executeEachAsync(operations, prepare, executeReader, null, options.Listener, token).ConfigureAwait(false);
    }

    private static async Task executeEachAsync(IReadOnlyList<IStorageOperation> operations,
        Func<IStorageOperation, DbCommand>? prepare, Func<DbCommand, CancellationToken, Task<DbDataReader>>? execute,
        Func<IStorageOperation, CancellationToken, Task<DbDataReader>>? executeOne, IOperationBatchListener? listener,
        CancellationToken token)
    {
        var exceptions = new List<Exception>();

        foreach (var operation in operations)
        {
            var command = prepare?.Invoke(operation);
            var context = listener is null ? null : new OperationBatchCommand([operation], null, command);

            listener?.BeforeExecute(context!);

            try
            {
                var reader = prepare is null
                    ? await executeOne!(operation, token).ConfigureAwait(false)
                    : await execute!(command!, token).ConfigureAwait(false);

                await using (reader.ConfigureAwait(false))
                {
                    await operation.PostprocessAsync(reader, exceptions, token).ConfigureAwait(false);
                    await drainAsync(reader, token).ConfigureAwait(false);
                }
            }
            catch (Exception e)
            {
                listener?.OnFailure(context!, e);

                if (TryTransform(operation, e, out var transformed)
                    || tryTransformWithListener(listener, context, e, out transformed))
                {
                    ExceptionDispatchInfo.Capture(transformed).Throw();
                }

                throw;
            }

            listener?.OnSuccess(context!);
        }

        ThrowIfAny(exceptions);
    }

    /// <summary>
    ///     Postprocess every operation against one multi-result reader, advancing per
    ///     <paramref name="walk" />, then drain the reader. Exceptions from the reader propagate
    ///     untransformed; postprocessing collects its own into <paramref name="exceptions" />. Surplus
    ///     result sets are drained silently.
    /// </summary>
    public static async Task PostprocessAsync(DbDataReader reader, IReadOnlyList<IStorageOperation> operations,
        IList<Exception> exceptions, ResultSetWalk walk, CancellationToken token)
    {
        ArgumentNullException.ThrowIfNull(reader);
        ArgumentNullException.ThrowIfNull(operations);
        ArgumentNullException.ThrowIfNull(exceptions);

        await walkAsync(reader, operations, exceptions, walk, SurplusResultSetPolicy.Ignore, token).ConfigureAwait(false);
    }

    /// <summary>
    ///     Postprocess every operation against one multi-result reader per <paramref name="options" />'
    ///     <see cref="OperationBatchOptions.Walk" />, drain the reader, and apply
    ///     <see cref="OperationBatchOptions.SurplusResultSets" />.
    /// </summary>
    /// <remarks>
    ///     For a store that opens and owns the reader itself — a daemon batch, a bulk path. Exceptions from
    ///     the reader propagate untransformed, and <see cref="OperationBatchOptions.Listener" /> is not
    ///     consulted: there is no command here, only a reader.
    /// </remarks>
    public static async Task PostprocessAsync(DbDataReader reader, IReadOnlyList<IStorageOperation> operations,
        IList<Exception> exceptions, OperationBatchOptions options, CancellationToken token)
    {
        ArgumentNullException.ThrowIfNull(reader);
        ArgumentNullException.ThrowIfNull(operations);
        ArgumentNullException.ThrowIfNull(exceptions);
        ArgumentNullException.ThrowIfNull(options);

        await walkAsync(reader, operations, exceptions, options.Walk, options.SurplusResultSets, token).ConfigureAwait(false);
    }

    private static async Task walkAsync(DbDataReader reader, IReadOnlyList<IStorageOperation> operations,
        IList<Exception> exceptions, ResultSetWalk walk, SurplusResultSetPolicy surplusPolicy,
        CancellationToken token)
    {
        // The reader opens on the first result set in the batch, wherever in the batch that is, so the
        // first operation to own one reads without advancing and every later one advances first. That
        // leaves the reader ON the last owned result set, where the drain's count starts.
        var owners = 0;

        for (var i = 0; i < operations.Count; i++)
        {
            var operation = operations[i];

            if (walk == ResultSetWalk.SkipNoDataReturned && !ReturnsResultSet(operation))
            {
                continue;
            }

            if (owners > 0)
            {
                await reader.NextResultAsync(token).ConfigureAwait(false);
            }

            await operation.PostprocessAsync(reader, exceptions, token).ConfigureAwait(false);
            owners++;
        }

        // weasel#810 / polecat#273 E2b: always drain, so an error raised by a statement behind the last
        // owned result set is thrown here instead of being swallowed by the reader's disposal (SQL
        // Server, Microsoft.Data.Sqlite) or thrown from it, past the transforms (Npgsql).
        var surplus = await drainAsync(reader, token).ConfigureAwait(false);

        // polecat#831: a set nobody declared means some operation read another statement's rows. Only
        // after the drain, because a SQL error in the batch is the better answer. A batch in which no
        // operation reads a result set has nothing to misread.
        if (surplusPolicy == SurplusResultSetPolicy.Throw && owners > 0 && surplus > 0)
        {
            throw new SurplusResultSetsException(surplus);
        }
    }

    private static async Task<int> drainAsync(DbDataReader reader, CancellationToken token)
    {
        var count = 0;
        while (await reader.NextResultAsync(token).ConfigureAwait(false))
        {
            count++;
        }

        return count;
    }

    /// <summary>
    ///     Whether <paramref name="operation" />'s command contributes a result set to a batch: false when
    ///     it, or any operation it wraps through <see cref="IStorageOperationWrapper" />, is marked
    ///     <see cref="NoDataReturnedCall" />.
    /// </summary>
    /// <remarks>
    ///     The <see cref="ResultSetWalk.SkipNoDataReturned" /> walk's question. A store with a walk of its
    ///     own should ask this rather than type-testing for the marker, which a wrapper hides.
    /// </remarks>
    public static bool ReturnsResultSet(IStorageOperation operation)
    {
        ArgumentNullException.ThrowIfNull(operation);

        var current = operation;
        for (var depth = 0; depth < MaxWrapperDepth; depth++)
        {
            if (current is NoDataReturnedCall)
            {
                return false;
            }

            if (current is not IStorageOperationWrapper { Inner: { } inner } || ReferenceEquals(inner, current))
            {
                return true;
            }

            current = inner;
        }

        throw wrapperCycle(operation);
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
    ///     Offer <paramref name="exception" /> to one operation's <see cref="IExceptionTransform" />. An
    ///     <see cref="IStorageOperationWrapper" /> that is not a transform itself is looked through to the
    ///     operation it wraps; one that is a transform answers for itself.
    /// </summary>
    public static bool TryTransform(IStorageOperation operation, Exception exception, out Exception transformed)
    {
        var current = operation;
        for (var depth = 0; depth < MaxWrapperDepth; depth++)
        {
            if (current is IExceptionTransform transform)
            {
                if (transform.TryTransform(exception, out var mapped) && mapped is not null)
                {
                    transformed = mapped;
                    return true;
                }

                break;
            }

            if (current is not IStorageOperationWrapper { Inner: { } inner } || ReferenceEquals(inner, current))
            {
                break;
            }

            current = inner;
        }

        transformed = exception;
        return false;
    }

    private static bool tryTransformBatch(IReadOnlyList<IStorageOperation> operations, DbBatch? batch,
        Exception exception, out Exception transformed)
    {
        // polecat#781: when the driver names the failing batch command and commands map one-to-one onto
        // operations, only the operation that owns it may translate the error.
        if (batch is not null && exception is DbException { BatchCommand: { } failed }
                              && batch.BatchCommands.Count == operations.Count)
        {
            for (var i = 0; i < batch.BatchCommands.Count; i++)
            {
                if (ReferenceEquals(batch.BatchCommands[i], failed))
                {
                    return TryTransform(operations[i], exception, out transformed);
                }
            }
        }

        return TryTransform(operations, exception, out transformed);
    }

    private static bool tryTransformWithListener(IOperationBatchListener? listener, OperationBatchCommand? command,
        Exception exception, out Exception transformed)
    {
        if (listener is not null && listener.TryTransform(command!, exception, out var mapped))
        {
            transformed = mapped;
            return true;
        }

        transformed = exception;
        return false;
    }

    private static InvalidOperationException wrapperCycle(IStorageOperation operation)
        => new($"{operation.GetType().FullName} wraps operations more than {MaxWrapperDepth} deep, which only "
               + $"a cycle of {nameof(IStorageOperationWrapper)}.{nameof(IStorageOperationWrapper.Inner)} "
               + "reaches.");

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
