using System.Collections.Concurrent;
using System.Data.Common;
using System.Linq.Expressions;
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
///     and JSON members, <c>Include</c>s and projections all behave as usual. Variables a query
///     captures are read when it is queued. Queries run in the order they were queued.
///     </para>
///     <para>
///     Batching into one round trip requires <see cref="BatchedQueryInterceptor" /> on the
///     <see cref="DbContext" /> (see <see cref="BatchQueryExtensions.UseWeaselBatchedQueries(DbContextOptionsBuilder)" />).
///     When a batch can't be used — the interceptor isn't registered, other command interceptors are
///     registered, the execution strategy retries on failure, or a query is a split query — every
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
        var query = CapturedValues.Freeze(queryable);
        return enqueue(query, async ct => (IReadOnlyList<T>)await query.ToListAsync(ct).ConfigureAwait(false));
    }

    /// <summary>
    ///     Queues a query that returns the first result, or the default value if there is none.
    /// </summary>
    public Task<T?> QuerySingle<T>(IQueryable<T> queryable)
    {
        var query = CapturedValues.Freeze(queryable);
        return enqueue(query, ct => firstOrDefaultAsync(query, ct));
    }

    /// <summary>
    ///     Queues a scalar query (e.g., COUNT, MAX), returning the first value or the default value if there is none.
    /// </summary>
    public Task<T> Scalar<T>(IQueryable<T> queryable)
    {
        var query = CapturedValues.Freeze(queryable);
        return enqueue(query, async ct => (await firstOrDefaultAsync(query, ct).ConfigureAwait(false))!);
    }

    // The batch runs exactly the queued query's SQL, so the first result comes from reading that
    // query's results rather than from FirstOrDefaultAsync(), whose SQL would differ (LIMIT 1).
    // Stopping after the first result also leaves the rest untracked, as FirstOrDefaultAsync() would.
    private static async Task<T?> firstOrDefaultAsync<T>(IQueryable<T> queryable, CancellationToken ct)
    {
        await foreach (var result in queryable.AsAsyncEnumerable().WithCancellation(ct).ConfigureAwait(false))
        {
            return result;
        }

        return default;
    }

    private Task<TResult> enqueue<TResult>(IQueryable queryable, Func<CancellationToken, Task<TResult>> execute)
    {
        var query = new QueuedQuery<TResult>(_context, queryable, isSplitQuery(queryable), execute);
        _queries.Add(query);
        return query.Result;
    }

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

        // A batch bypasses other command interceptors, so whatever they change about a command wouldn't apply
        if (!BatchedQueryInterceptor.IsOnlyCommandInterceptor(_context))
        {
            warnOnce("has other command interceptors registered, whose changes to commands a batch can't apply");
            return false;
        }

        // EF Core retries a failed query by running it again, which a result set read from a batch can't support
        if (_context.Database.CreateExecutionStrategy().RetriesOnFailure)
        {
            warnOnce("uses an execution strategy that retries on failure");
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

    private bool isSplitQuery(IQueryable queryable)
    {
        var visitor = new SplitQueryVisitor();
        visitor.Visit(queryable.Expression);
        if (visitor.Splitting.HasValue) return visitor.Splitting.Value;

        return _context.GetService<IDbContextOptions>().Extensions
            .OfType<RelationalOptionsExtension>()
            .Any(x => x.QuerySplittingBehavior == QuerySplittingBehavior.SplitQuery);
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var query in _queries.Where(x => x.HasSourceCommand))
        {
            await query.SourceCommand.DisposeAsync().ConfigureAwait(false);
        }

        _queries.Clear();
    }

    private sealed class SplitQueryVisitor : ExpressionVisitor
    {
        public bool? Splitting { get; private set; }

        protected override Expression VisitMethodCall(MethodCallExpression node)
        {
            // The outermost call wins, as it does in EF Core
            if (node.Method.DeclaringType == typeof(RelationalQueryableExtensions))
            {
                if (node.Method.Name == nameof(RelationalQueryableExtensions.AsSplitQuery)) Splitting ??= true;
                if (node.Method.Name == nameof(RelationalQueryableExtensions.AsSingleQuery)) Splitting ??= false;
            }

            return base.VisitMethodCall(node);
        }
    }
}
