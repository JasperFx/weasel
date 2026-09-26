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
///     and JSON members, <c>Include</c>s and projections all behave as usual. Batching into one
///     round trip requires <see cref="BatchedQueryInterceptor" /> on the <see cref="DbContext" />
///     (see <see cref="BatchQueryExtensions.UseWeaselBatchedQueries(DbContextOptionsBuilder)" />).
///     Without it, and for split queries, each query runs on its own round trip.
///     </para>
/// </summary>
public sealed class BatchedQuery : IAsyncDisposable
{
    private static readonly ConcurrentDictionary<Type, bool> WarnedContextTypes = new();

    private readonly DbContext _context;
    private readonly bool _canBatch;
    private readonly List<QueuedQuery> _batched = new();
    private readonly List<QueuedQuery> _separate = new();

    public BatchedQuery(DbContext context)
    {
        _context = context;
        _canBatch = BatchedQueryInterceptor.IsRegistered(context);
    }

    /// <summary>
    ///     Queues a query that returns a list of results.
    ///     Not executed until <see cref="ExecuteAsync" />.
    /// </summary>
    public Task<IReadOnlyList<T>> Query<T>(IQueryable<T> queryable)
    {
        return Enqueue(queryable, async ct => (IReadOnlyList<T>)await queryable.ToListAsync(ct).ConfigureAwait(false));
    }

    /// <summary>
    ///     Queues a query that returns the first result, or the default value if there is none.
    /// </summary>
    public Task<T?> QuerySingle<T>(IQueryable<T> queryable)
    {
        return Enqueue(queryable, ct => FirstOrDefaultAsync(queryable, ct));
    }

    /// <summary>
    ///     Queues a scalar query (e.g., COUNT, MAX), returning the first value or the default value if there is none.
    /// </summary>
    public Task<T> Scalar<T>(IQueryable<T> queryable)
    {
        return Enqueue(queryable, async ct => (await FirstOrDefaultAsync(queryable, ct).ConfigureAwait(false))!);
    }

    // The batch runs exactly the queued query's SQL, so the first result comes from reading that
    // query's results rather than from FirstOrDefaultAsync(), whose SQL would differ (LIMIT 1).
    // Stopping after the first result also leaves the rest untracked, as FirstOrDefaultAsync() would.
    private static async Task<T?> FirstOrDefaultAsync<T>(IQueryable<T> queryable, CancellationToken ct)
    {
        await foreach (var result in queryable.AsAsyncEnumerable().WithCancellation(ct).ConfigureAwait(false))
        {
            return result;
        }

        return default;
    }

    private Task<TResult> Enqueue<TResult>(IQueryable queryable, Func<CancellationToken, Task<TResult>> execute)
    {
        var query = new QueuedQuery<TResult>(_context, queryable, execute);

        // A split query sends more than one command, which a single result set can't answer
        if (_canBatch && !IsSplitQuery(queryable))
        {
            _ = query.SourceCommand; // compile the SQL now, as documented
            _batched.Add(query);
        }
        else
        {
            if (!_canBatch) WarnNotBatching();
            _separate.Add(query);
        }

        return query.Result;
    }

    /// <summary>
    ///     Executes all queued queries, in a single database round trip when possible.
    ///     After this call, all <see cref="Task{T}" /> futures returned by
    ///     <see cref="Query{T}" />, <see cref="QuerySingle{T}" />, and
    ///     <see cref="Scalar{T}" /> are resolved.
    /// </summary>
    public async Task ExecuteAsync(CancellationToken ct = default)
    {
        try
        {
            if (_batched.Count > 0)
            {
                await ExecuteBatchAsync(ct).ConfigureAwait(false);
            }

            foreach (var query in _separate)
            {
                await query.ExecuteAsync(ct).ConfigureAwait(false);
            }
        }
        catch (Exception e)
        {
            // Don't leave the futures of queries that never ran pending forever
            foreach (var query in _batched.Concat(_separate))
            {
                query.Fail(e);
            }

            throw;
        }
    }

    private async Task ExecuteBatchAsync(CancellationToken ct)
    {
        await _context.Database.OpenConnectionAsync(ct).ConfigureAwait(false);
        try
        {
            await using var batch = _context.Database.GetDbConnection().CreateBatch();
            batch.Transaction = _context.Database.CurrentTransaction?.GetDbTransaction();

            foreach (var query in _batched)
            {
                var batchCommand = batch.CreateBatchCommand();
                query.ConfigureCommand(batchCommand);
                batch.BatchCommands.Add(batchCommand);
            }

            await using var reader = await batch.ExecuteReaderAsync(ct).ConfigureAwait(false);

            for (var i = 0; i < _batched.Count; i++)
            {
                if (i > 0 && !await reader.NextResultAsync(ct).ConfigureAwait(false))
                {
                    throw new InvalidOperationException(
                        $"Expected {_batched.Count} result sets but only received {i}.");
                }

                await _batched[i].ReadFromBatchAsync(reader, ct).ConfigureAwait(false);
            }
        }
        finally
        {
            await _context.Database.CloseConnectionAsync().ConfigureAwait(false);
        }
    }

    private void WarnNotBatching()
    {
        if (!WarnedContextTypes.TryAdd(_context.GetType(), true)) return;

        _context.GetService<ILoggerFactory>().CreateLogger<BatchedQuery>().LogWarning(
            "{DbContext} does not have the {Interceptor} registered, so BatchedQuery runs each query on its own " +
            "round trip. Call UseWeaselBatchedQueries() on its DbContextOptionsBuilder to batch them.",
            _context.GetType().Name, nameof(BatchedQueryInterceptor));
    }

    private bool IsSplitQuery(IQueryable queryable)
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
        foreach (var query in _batched)
        {
            await query.SourceCommand.DisposeAsync().ConfigureAwait(false);
        }

        _batched.Clear();
        _separate.Clear();
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
