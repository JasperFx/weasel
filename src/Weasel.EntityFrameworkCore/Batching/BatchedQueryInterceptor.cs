using System.Data.Common;
using System.Runtime.CompilerServices;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Infrastructure;

namespace Weasel.EntityFrameworkCore.Batching;

/// <summary>
///     Lets <see cref="BatchedQuery" /> send its queries in one round trip while EF Core still
///     materializes every result. When EF Core runs a batched query, the interceptor supplies that
///     query's result set from the batch instead of executing the command.
///     Register it with <see cref="BatchQueryExtensions.UseWeaselBatchedQueries(DbContextOptionsBuilder)" />.
/// </summary>
public sealed class BatchedQueryInterceptor : DbCommandInterceptor
{
    public static BatchedQueryInterceptor Instance { get; } = new();

    private static readonly ConditionalWeakTable<DbContext, PendingResult> Pending = new();

    private sealed record PendingResult(DbDataReader Reader, string CommandText);

    internal static bool IsRegistered(DbContext context) =>
        context.GetService<IDbContextOptions>().FindExtension<CoreOptionsExtension>()?.Interceptors?
            .OfType<BatchedQueryInterceptor>().Any() == true;

    internal static void Supply(DbContext context, DbDataReader reader, string commandText) =>
        Pending.AddOrUpdate(context, new PendingResult(new BatchResultSetReader(reader), commandText));

    // The context's command interceptors, aggregated by EF Core from AddInterceptors() and registered services
    internal static bool IsOnlyCommandInterceptor(DbContext context) =>
        context.GetService<IInterceptors>().Aggregate<IDbCommandInterceptor>() is BatchedQueryInterceptor;

    internal static bool IsPending(DbContext context) => Pending.TryGetValue(context, out _);

    internal static void Clear(DbContext context) => Pending.Remove(context);

    public override InterceptionResult<DbDataReader> ReaderExecuting(DbCommand command, CommandEventData eventData,
        InterceptionResult<DbDataReader> result) => take(command, eventData, result);

    public override ValueTask<InterceptionResult<DbDataReader>> ReaderExecutingAsync(DbCommand command,
        CommandEventData eventData, InterceptionResult<DbDataReader> result, CancellationToken cancellationToken = default)
        => new(take(command, eventData, result));

    private static InterceptionResult<DbDataReader> take(DbCommand command, CommandEventData eventData,
        InterceptionResult<DbDataReader> result)
    {
        if (eventData.Context == null || !Pending.TryGetValue(eventData.Context, out var pending)) return result;

        Pending.Remove(eventData.Context);

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
