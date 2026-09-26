using System.Data;
using System.Data.Common;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Storage;

namespace Weasel.EntityFrameworkCore.Batching;

/// <summary>
///     Collects multiple EF Core queries and executes them in a single database round trip
///     using <see cref="DbBatch" />. Results are materialized using EF Core's entity type metadata.
///     <para>
///     SQL is extracted from <see cref="IQueryable{T}" /> via EF Core's
///     <c>CreateDbCommand()</c> method. Parameters are preserved exactly as EF Core generates them.
///     </para>
/// </summary>
public sealed class BatchedQuery : IAsyncDisposable
{
    private readonly DbContext _context;
    private readonly List<IBatchQueryItem> _items = new();
    private readonly List<DbCommand> _sourceCommands = new();

    // Queries the flat materializer can't handle faithfully; run through EF Core after the batch
    private readonly List<Func<CancellationToken, Task>> _deferred = new();

    // What a tracking EF Core query does for a flat entity: identity resolution against the
    // change tracker, otherwise start tracking it as Unchanged
    private T Track<T>(T entity) where T : class
    {
        var key = _context.Model.FindEntityType(typeof(T))!.FindPrimaryKey()!;
        var keyValues = key.Properties.Select(p => p.PropertyInfo!.GetValue(entity));
        var existing = _context.Set<T>().Local.FindEntryUntyped(keyValues);
        if (existing != null) return existing.Entity;

        _context.Attach(entity);
        return entity;
    }

    private Task<TResult> Enlist<TResult>(IQueryable queryable, Func<CancellationToken, Task<TResult>> execute)
    {
        var command = queryable.CreateDbCommand();
        _sourceCommands.Add(command);
        var item = new EfPipelineBatchQueryItem<TResult>(_context, command, execute);
        _items.Add(item);
        return item.Result;
    }

    private Task<TResult> Defer<TResult>(Func<CancellationToken, Task<TResult>> query)
    {
        var completion = new TaskCompletionSource<TResult>();
        _deferred.Add(async ct =>
        {
            try { completion.SetResult(await query(ct).ConfigureAwait(false)); }
            catch (Exception e) { completion.SetException(e); throw; }
        });
        return completion.Task;
    }

    public BatchedQuery(DbContext context)
    {
        _context = context;
    }

    /// <summary>
    ///     Queues a query that returns a list of entities.
    ///     The query is compiled to SQL immediately but not executed until <see cref="ExecuteAsync" />.
    ///     <para>
    ///     <see cref="Microsoft.EntityFrameworkCore.Metadata.IModel.FindEntityType(Type)" />
    ///     reflects over the entity type's members; EF Core itself isn't AOT-ready upstream
    ///     (tracked as <c>dotnet/efcore#29761</c>). Suppressed locally — Weasel.EntityFrameworkCore
    ///     consumers targeting AOT inherit EF Core's overall AOT limitations. weasel#263.
    ///     </para>
    /// </summary>
    [System.Diagnostics.CodeAnalysis.UnconditionalSuppressMessage("Trimming", "IL2087",
        Justification = "EF Core's FindEntityType reflects on the entity type; EF Core upstream is not AOT-ready (dotnet/efcore#29761). weasel#263.")]
    public Task<IReadOnlyList<T>> Query<T>(IQueryable<T> queryable) where T : class, new()
    {
        var entityType = _context.Model.FindEntityType(typeof(T))
            ?? throw new InvalidOperationException(
                $"Type {typeof(T).FullName} is not a mapped entity type in this DbContext.");

        if (BatchedQueryInterceptor.IsRegistered(_context) && !BatchSafety.IsSplitQuery(_context, queryable))
        {
            return Enlist(queryable, async ct => (IReadOnlyList<T>)await queryable.ToListAsync(ct).ConfigureAwait(false));
        }

        if (!BatchSafety.CanMaterialize(_context, entityType, queryable, out var tracking))
        {
            return Defer(ct => queryable.ToListAsync(ct).ContinueWith(t => (IReadOnlyList<T>)t.Result, ct));
        }

        var command = queryable.CreateDbCommand();
        _sourceCommands.Add(command);

        var item = new ListBatchQueryItem<T>(command, entityType, tracking ? Track : null);
        _items.Add(item);
        return item.Result;
    }

    /// <summary>
    ///     Queues a query that returns a single entity or null.
    /// </summary>
    [System.Diagnostics.CodeAnalysis.UnconditionalSuppressMessage("Trimming", "IL2087",
        Justification = "EF Core's FindEntityType reflects on the entity type; EF Core upstream is not AOT-ready (dotnet/efcore#29761). weasel#263.")]
    public Task<T?> QuerySingle<T>(IQueryable<T> queryable) where T : class, new()
    {
        var entityType = _context.Model.FindEntityType(typeof(T))
            ?? throw new InvalidOperationException(
                $"Type {typeof(T).FullName} is not a mapped entity type in this DbContext.");

        if (BatchedQueryInterceptor.IsRegistered(_context) && !BatchSafety.IsSplitQuery(_context, queryable))
        {
            // Same query for the batch SQL and for EF Core's execution, so the result shape matches
            var first = queryable.Take(1);
            return Enlist(first, async ct => (await first.ToListAsync(ct).ConfigureAwait(false)).FirstOrDefault());
        }

        if (!BatchSafety.CanMaterialize(_context, entityType, queryable, out var tracking))
        {
            return Defer(ct => queryable.FirstOrDefaultAsync(ct));
        }

        var command = queryable.CreateDbCommand();
        _sourceCommands.Add(command);

        var item = new SingleBatchQueryItem<T>(command, entityType, tracking ? Track : null);
        _items.Add(item);
        return item.Result;
    }

    /// <summary>
    ///     Queues a scalar query (e.g., COUNT, MAX, SUM).
    /// </summary>
    public Task<T> Scalar<T>(IQueryable<T> queryable)
    {
        var command = queryable.CreateDbCommand();
        _sourceCommands.Add(command);

        var item = new ScalarBatchQueryItem<T>(command);
        _items.Add(item);
        return item.Result;
    }

    /// <summary>
    ///     Executes all queued queries in a single database round trip.
    ///     After this call, all <see cref="Task{T}" /> futures returned by
    ///     <see cref="Query{T}" />, <see cref="QuerySingle{T}" />, and
    ///     <see cref="Scalar{T}" /> are resolved.
    /// </summary>
    public async Task ExecuteAsync(CancellationToken ct = default)
    {
        await ExecuteBatchAsync(ct).ConfigureAwait(false);

        foreach (var deferred in _deferred)
        {
            await deferred(ct).ConfigureAwait(false);
        }
    }

    private async Task ExecuteBatchAsync(CancellationToken ct)
    {
        if (_items.Count == 0) return;

        var conn = _context.Database.GetDbConnection();
        var controlled = false;

        if (conn.State != ConnectionState.Open)
        {
            controlled = true;
            await conn.OpenAsync(ct).ConfigureAwait(false);
        }

        try
        {
            await using var batch = conn.CreateBatch();
            batch.Transaction = _context.Database.CurrentTransaction?.GetDbTransaction();

            foreach (var item in _items)
            {
                var batchCommand = batch.CreateBatchCommand();
                item.ConfigureCommand(batchCommand);
                batch.BatchCommands.Add(batchCommand);
            }

            await using var reader = await batch.ExecuteReaderAsync(ct).ConfigureAwait(false);

            // Read first result set
            await _items[0].ReadAsync(reader, ct).ConfigureAwait(false);

            // Iterate remaining result sets
            for (var i = 1; i < _items.Count; i++)
            {
                if (!await reader.NextResultAsync(ct).ConfigureAwait(false))
                {
                    throw new InvalidOperationException(
                        $"Expected {_items.Count} result sets but only received {i}.");
                }

                await _items[i].ReadAsync(reader, ct).ConfigureAwait(false);
            }
        }
        finally
        {
            if (controlled)
            {
                await conn.CloseAsync().ConfigureAwait(false);
            }
        }
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var cmd in _sourceCommands)
        {
            await cmd.DisposeAsync().ConfigureAwait(false);
        }
        _sourceCommands.Clear();
        _items.Clear();
    }
}
