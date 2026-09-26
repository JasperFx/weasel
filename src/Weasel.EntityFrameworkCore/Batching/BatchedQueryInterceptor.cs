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

    internal static void Clear(DbContext context) => Pending.Remove(context);

    public override InterceptionResult<DbDataReader> ReaderExecuting(DbCommand command, CommandEventData eventData,
        InterceptionResult<DbDataReader> result) => Take(command, eventData, result);

    public override ValueTask<InterceptionResult<DbDataReader>> ReaderExecutingAsync(DbCommand command,
        CommandEventData eventData, InterceptionResult<DbDataReader> result, CancellationToken cancellationToken = default)
        => new(Take(command, eventData, result));

    private static InterceptionResult<DbDataReader> Take(DbCommand command, CommandEventData eventData,
        InterceptionResult<DbDataReader> result)
    {
        // Only the command the batch ran may read its result set. Any other command EF Core issues
        // while materializing executes normally, and fails loudly on the busy connection.
        if (eventData.Context == null || !Pending.TryGetValue(eventData.Context, out var pending) ||
            pending.CommandText != command.CommandText)
        {
            return result;
        }

        Pending.Remove(eventData.Context);
        return InterceptionResult<DbDataReader>.SuppressWithResult(pending.Reader);
    }
}
