using System.Collections.Concurrent;
using Weasel.Core.Migrations;

namespace Weasel.Core.CommandLine;

/// <summary>
///     One database that failed during a batch run, with the exception that felled it.
/// </summary>
internal sealed record DatabaseFailure(IDatabase Database, Exception Exception);

/// <summary>
///     Runs an operation across many databases with bounded parallelism (weasel#431). At 1,037 target
///     databases a strictly sequential walk cost a field deployment 8m41s of dead time for a no-op
///     apply; both <c>db-apply</c> and <c>db-assert</c> have this loop and this pain, which is why the
///     scheduler is shared rather than living in either command.
///     <para>
///     Targets are grouped by <c>Describe().DatabaseUri()</c> and the parallelism is applied *across*
///     groups while each group runs sequentially *within* itself. Parallel DDL against the same
///     physical database only contends on locks — nothing corrupts, but the speedup goes sublinear —
///     so <c>--parallel N</c> means "N physical databases in flight", which is also the right unit for
///     reasoning about a server's <c>max_connections</c> ceiling.
///     </para>
/// </summary>
internal static class DatabaseBatch
{
    /// <summary>
    ///     Runs <paramref name="operation" /> once for every database. A throwing operation never stops
    ///     the rest of the batch: <see cref="Parallel.ForEachAsync{TSource}(IEnumerable{TSource},ParallelOptions,Func{TSource,CancellationToken,ValueTask})" />
    ///     cancels its remaining iterations when a body throws, so keep-going-and-aggregate has to be
    ///     built by catching inside the body — which is done here, once, instead of in every caller.
    ///     Failures come back with their database so the caller can report them together and decide the
    ///     exit semantics. Only cancellation stops the run early, and it propagates as
    ///     <see cref="OperationCanceledException" />.
    /// </summary>
    /// <param name="databases">Every database the batch should touch.</param>
    /// <param name="maxParallel">
    ///     Maximum number of physical databases in flight at once. Values below 1 are treated as 1,
    ///     which preserves the strictly sequential behavior.
    /// </param>
    /// <param name="operation">
    ///     The per-database work. Exceptions it throws are collected, not fatal — except an
    ///     <see cref="OperationCanceledException" /> for the supplied token, which cancels the batch.
    /// </param>
    /// <param name="ct">Cancellation token, propagated into both the scheduler and the operation.</param>
    public static async Task<IReadOnlyList<DatabaseFailure>> RunAsync(
        IEnumerable<IDatabase> databases,
        int maxParallel,
        Func<IDatabase, CancellationToken, Task> operation,
        CancellationToken ct = default)
    {
        var groups = databases
            .GroupBy(x => x.Describe().DatabaseUri())
            .Select(x => x.ToArray())
            .ToArray();

        var options = new ParallelOptions
        {
            MaxDegreeOfParallelism = Math.Max(1, maxParallel), CancellationToken = ct
        };

        var failures = new ConcurrentQueue<DatabaseFailure>();

        await Parallel.ForEachAsync(groups, options, async (group, token) =>
        {
            foreach (var database in group)
            {
                token.ThrowIfCancellationRequested();

                try
                {
                    await operation(database, token).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (token.IsCancellationRequested)
                {
                    throw;
                }
                catch (Exception e)
                {
                    failures.Enqueue(new DatabaseFailure(database, e));
                }
            }
        }).ConfigureAwait(false);

        return failures.ToList();
    }
}
