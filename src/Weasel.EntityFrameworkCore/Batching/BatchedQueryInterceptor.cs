using System.Data.Common;
using System.Runtime.CompilerServices;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Infrastructure;

namespace Weasel.EntityFrameworkCore.Batching;

/// <summary>
///     Lets <see cref="BatchedQuery" /> send its queries in one round trip while EF Core still
///     materializes every result. When EF Core runs a batched query, the interceptor supplies the
///     batch's reader, positioned on that query's result set, instead of executing the command, and
///     keeps EF Core from closing it so the batch can move on to the next result set.
///     Register it with <see cref="BatchQueryExtensions.UseWeaselBatchedQueries(DbContextOptionsBuilder)" />.
/// </summary>
public sealed class BatchedQueryInterceptor : DbCommandInterceptor
{
    public static BatchedQueryInterceptor Instance { get; } = new();

    private static readonly ConditionalWeakTable<DbContext, BatchResult> Results = new();

    // A result set of the batch, from the time it is supplied until the batch moves past it
    private sealed class BatchResult(DbDataReader reader, string commandText)
    {
        public DbDataReader Reader { get; } = reader;
        public string CommandText { get; } = commandText;
        public bool Taken { get; set; }
    }

    internal static bool IsRegistered(DbContext context) =>
        context.GetService<IDbContextOptions>().FindExtension<CoreOptionsExtension>()?.Interceptors?
            .OfType<BatchedQueryInterceptor>().Any() == true;

    internal static void Supply(DbContext context, DbDataReader reader, string commandText) =>
        Results.AddOrUpdate(context, new BatchResult(reader, commandText));

    // The context's command interceptors, aggregated by EF Core from AddInterceptors() and registered services
    internal static bool IsOnlyCommandInterceptor(DbContext context) =>
        context.GetService<IInterceptors>().Aggregate<IDbCommandInterceptor>() is BatchedQueryInterceptor;

    internal static bool IsPending(DbContext context) => Results.TryGetValue(context, out var result) && !result.Taken;

    internal static void Clear(DbContext context) => Results.Remove(context);

    public override InterceptionResult<DbDataReader> ReaderExecuting(DbCommand command, CommandEventData eventData,
        InterceptionResult<DbDataReader> result) => take(command, eventData, result);

    public override ValueTask<InterceptionResult<DbDataReader>> ReaderExecutingAsync(DbCommand command,
        CommandEventData eventData, InterceptionResult<DbDataReader> result, CancellationToken cancellationToken = default)
        => new(take(command, eventData, result));

    // The batch reads its next result set from the same reader, so EF Core must not close it
    public override InterceptionResult DataReaderClosing(DbCommand command, DataReaderClosingEventData eventData,
        InterceptionResult result) => isBatchReader(eventData.Context, eventData.DataReader) ? InterceptionResult.Suppress() : result;

    public override ValueTask<InterceptionResult> DataReaderClosingAsync(DbCommand command,
        DataReaderClosingEventData eventData, InterceptionResult result) => new(DataReaderClosing(command, eventData, result));

    public override InterceptionResult DataReaderDisposing(DbCommand command, DataReaderDisposingEventData eventData,
        InterceptionResult result)
    {
        if (!isBatchReader(eventData.Context, eventData.DataReader)) return result;

        // Suppressing the disposal skips the rest of EF Core's cleanup too, so do that here: the batch
        // disposes its reader, but EF Core's own command and its hold on the connection are released now
        command.Parameters.Clear();
        command.Dispose();
        eventData.Context!.Database.CloseConnection();

        return InterceptionResult.Suppress();
    }

    private static bool isBatchReader(DbContext? context, DbDataReader reader) =>
        context != null && Results.TryGetValue(context, out var result) && ReferenceEquals(result.Reader, reader);

    private static InterceptionResult<DbDataReader> take(DbCommand command, CommandEventData eventData,
        InterceptionResult<DbDataReader> result)
    {
        if (eventData.Context == null || !Results.TryGetValue(eventData.Context, out var pending) || pending.Taken)
        {
            return result;
        }

        pending.Taken = true;

        // Only the command the batch ran may read its result set. The batch reader still holds the
        // connection, so any other command could not run anyway.
        if (pending.CommandText != command.CommandText)
        {
            throw new InvalidOperationException(
                "EF Core sent a different command for a batched query than the batch ran, so it can't be given the " +
                "batch's result set. This happens when the query's SQL depends on something evaluated at execution " +
                "time. Run this query outside BatchedQuery.");
        }

        return InterceptionResult<DbDataReader>.SuppressWithResult(pending.Reader);
    }
}
