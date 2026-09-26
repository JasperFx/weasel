using System.Collections.Concurrent;
using System.Data.Common;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.Logging;

namespace Weasel.EntityFrameworkCore.Batching;

/// <summary>
///     Collects multiple EF Core queries and executes them in a single database round trip
///     using <see cref="DbBatch" />.
///     <para>
///     EF Core runs every query and materializes its results, so a batched query returns exactly
///     what the same query returns on its own: tracking and identity resolution, owned, complex
///     and JSON members, <c>Include</c>s and projections all behave as usual. EF Core prepares each
///     query when it is queued, so the values it captures are read then, once, and EF Core reports a
///     query it can't translate right away. Queries run in the order they were queued.
///     </para>
///     <para>
///     Batching into one round trip requires <see cref="BatchedQueryInterceptor" /> on the
///     <see cref="DbContext" /> (see <see cref="BatchQueryExtensions.UseWeaselBatchedQueries(DbContextOptionsBuilder)" />).
///     When a batch can't be used — the interceptor isn't registered, the provider doesn't support
///     <see cref="DbBatch" />, other command interceptors are registered, the execution strategy
///     retries on failure, or a query is a split query — every
///     query runs on its own round trip instead, with the same results.
///     </para>
/// </summary>
public sealed class BatchedQuery : IAsyncDisposable
{
    private static readonly ConcurrentDictionary<(Type, string), bool> WarnedContextTypes = new();

    private readonly DbContext _context;
    private readonly List<QueuedQuery> _queries = new();

    public BatchedQuery(DbContext context)
    {
        _context = context;
    }

    /// <summary>
    ///     Queues a query that returns a list of results.
    ///     Not executed until <see cref="ExecuteAsync" />.
    /// </summary>
    public Task<IReadOnlyList<T>> Query<T>(IQueryable<T> queryable)
    {
        return enqueue(queryable, async (results, ct) =>
        {
            var list = new List<T>();
            await foreach (var result in results.WithCancellation(ct).ConfigureAwait(false))
            {
                list.Add(result);
            }

            return (IReadOnlyList<T>)list;
        });
    }

    /// <summary>
    ///     Queues a query that returns the first result, or the default value if there is none.
    /// </summary>
    public Task<T?> QuerySingle<T>(IQueryable<T> queryable)
    {
        return enqueue(queryable, firstOrDefaultAsync);
    }

    /// <summary>
    ///     Queues a scalar query (e.g., COUNT, MAX), returning the first value or the default value if there is none.
    /// </summary>
    public Task<T> Scalar<T>(IQueryable<T> queryable)
    {
        return enqueue(queryable, async (results, ct) => (await firstOrDefaultAsync(results, ct).ConfigureAwait(false))!);
    }

    // The batch runs exactly the queued query's SQL, so the first result comes from reading that
    // query's results rather than from FirstOrDefaultAsync(), whose SQL would differ (LIMIT 1).
    // Stopping after the first result also leaves the rest untracked, as FirstOrDefaultAsync() would.
    private static async Task<T?> firstOrDefaultAsync<T>(IAsyncEnumerable<T> results, CancellationToken ct)
    {
        await foreach (var result in results.WithCancellation(ct).ConfigureAwait(false))
        {
            return result;
        }

        return default;
    }

    private Task<TResult> enqueue<T, TResult>(IQueryable<T> queryable,
        Func<IAsyncEnumerable<T>, CancellationToken, Task<TResult>> read)
    {
        var query = new QueuedQuery<T, TResult>(_context, queryable, retriesOnFailure(), read);
        _queries.Add(query);
        return query.Result;
    }

    // EF Core retries a failed query by running it again, and a retrying strategy makes EF Core buffer each
    // query's results and close its reader, including while an outer strategy is running (EF Core's own check)
    private bool retriesOnFailure() =>
        ExecutionStrategy.Current?.RetriesOnFailure ?? _context.Database.CreateExecutionStrategy().RetriesOnFailure;

    /// <summary>
    ///     Executes all queued queries in the order they were queued, in a single database round trip
    ///     when possible. After this call, all <see cref="Task{T}" /> futures returned by
    ///     <see cref="Query{T}" />, <see cref="QuerySingle{T}" />, and
    ///     <see cref="Scalar{T}" /> are resolved.
    /// </summary>
    public async Task ExecuteAsync(CancellationToken ct = default)
    {
        if (_queries.Count == 0) return;

        try
        {
            if (canBatch())
            {
                await executeBatchAsync(ct).ConfigureAwait(false);
            }
            else
            {
                foreach (var query in _queries)
                {
                    await query.ExecuteAsync(ct).ConfigureAwait(false);
                }
            }
        }
        catch (Exception e)
        {
            // Don't leave the futures of queries that never ran pending forever
            foreach (var query in _queries)
            {
                query.Fail(e);
            }

            throw;
        }
    }

    private bool canBatch()
    {
        if (!BatchedQueryInterceptor.IsRegistered(_context))
        {
            warnOnce("does not have the BatchedQueryInterceptor registered. Call UseWeaselBatchedQueries() on its " +
                     "DbContextOptionsBuilder to batch them");
            return false;
        }

        // Not every ADO.NET provider supports DbBatch (SQLite and Oracle don't)
        if (!_context.Database.GetDbConnection().CanCreateBatch)
        {
            warnOnce("uses a database provider that can't batch commands");
            return false;
        }

        // A batch bypasses other command interceptors, so whatever they change about a command wouldn't apply
        if (!BatchedQueryInterceptor.IsOnlyCommandInterceptor(_context))
        {
            warnOnce("has other command interceptors registered, whose changes to commands a batch can't apply");
            return false;
        }

        // A result set read from a batch can't be retried or buffered, whether the strategy runs when the
        // queries were queued (EF Core prepared them then) or now
        if (retriesOnFailure() || _queries.Any(x => x.PreparedWhileRetrying))
        {
            warnOnce("uses an execution strategy that retries on failure");
            return false;
        }

        if (_queries.Any(x => x.SourceCommand == null))
        {
            warnOnce("uses a version of EF Core that doesn't provide a prepared query's command " +
                     "(IRelationalQueryingEnumerable)");
            return false;
        }

        // Running a split query separately would change the order the queries run in
        return !_queries.Any(x => x.IsSplitQuery);
    }

    private async Task executeBatchAsync(CancellationToken ct)
    {
        await _context.Database.OpenConnectionAsync(ct).ConfigureAwait(false);
        try
        {
            await using var batch = _context.Database.GetDbConnection().CreateBatch();
            batch.Transaction = _context.Database.CurrentTransaction?.GetDbTransaction();

            foreach (var query in _queries)
            {
                var batchCommand = batch.CreateBatchCommand();
                query.ConfigureCommand(batchCommand);
                batch.BatchCommands.Add(batchCommand);
            }

            await using var reader = await batch.ExecuteReaderAsync(ct).ConfigureAwait(false);

            for (var i = 0; i < _queries.Count; i++)
            {
                if (i > 0 && !await reader.NextResultAsync(ct).ConfigureAwait(false))
                {
                    throw new InvalidOperationException(
                        $"Expected {_queries.Count} result sets but only received {i}.");
                }

                await _queries[i].ReadFromBatchAsync(reader, ct).ConfigureAwait(false);
            }
        }
        finally
        {
            await _context.Database.CloseConnectionAsync().ConfigureAwait(false);
        }
    }

    private void warnOnce(string reason)
    {
        if (!WarnedContextTypes.TryAdd((_context.GetType(), reason), true)) return;

        _context.GetService<ILoggerFactory>().CreateLogger<BatchedQuery>().LogWarning(
            "{DbContext} {Reason}, so BatchedQuery runs each query on its own round trip.",
            _context.GetType().Name, reason);
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var query in _queries.Where(x => x.HasSourceCommand))
        {
            await query.SourceCommand!.DisposeAsync().ConfigureAwait(false);
        }

        _queries.Clear();
    }
}
