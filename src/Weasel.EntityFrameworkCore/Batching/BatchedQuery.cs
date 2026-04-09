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

    public BatchedQuery(DbContext context)
    {
        _context = context;
    }

    /// <summary>
    ///     Queues a query that returns a list of entities.
    ///     The query is compiled to SQL immediately but not executed until <see cref="ExecuteAsync" />.
    /// </summary>
    public Task<IReadOnlyList<T>> Query<T>(IQueryable<T> queryable) where T : class, new()
    {
        var entityType = _context.Model.FindEntityType(typeof(T))
            ?? throw new InvalidOperationException(
                $"Type {typeof(T).FullName} is not a mapped entity type in this DbContext.");

        var command = queryable.CreateDbCommand();
        _sourceCommands.Add(command);

        var item = new ListBatchQueryItem<T>(command, entityType);
        _items.Add(item);
        return item.Result;
    }

    /// <summary>
    ///     Queues a query that returns a single entity or null.
    /// </summary>
    public Task<T?> QuerySingle<T>(IQueryable<T> queryable) where T : class, new()
    {
        var entityType = _context.Model.FindEntityType(typeof(T))
            ?? throw new InvalidOperationException(
                $"Type {typeof(T).FullName} is not a mapped entity type in this DbContext.");

        var command = queryable.CreateDbCommand();
        _sourceCommands.Add(command);

        var item = new SingleBatchQueryItem<T>(command, entityType);
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
