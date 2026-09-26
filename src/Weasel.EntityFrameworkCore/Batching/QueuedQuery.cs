using System.Data.Common;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Query;

namespace Weasel.EntityFrameworkCore.Batching;

/// <summary>
///     A query queued in a <see cref="BatchedQuery" />. EF Core prepares the query when it is queued,
///     reading the values it captures once, and that prepared query supplies the batch's SQL and then
///     materializes the results. When the query is batched, <see cref="BatchedQueryInterceptor" /> hands
///     EF Core the query's result set from the batch instead of letting it hit the database.
/// </summary>
internal abstract class QueuedQuery
{
    /// <summary>
    ///     A split query sends more than one command, which a single result set can't answer. Only EF Core
    ///     knows once it compiles the query (query interceptors or the context's options can make a query
    ///     split, and composed queries follow EF Core's own rules), so ask it: a split query's
    ///     <c>ToQueryString()</c> ends with <see cref="RelationalStrings.SplitQueryString" />.
    /// </summary>
    public abstract bool IsSplitQuery { get; }

    /// <summary>Whether a retrying execution strategy was running when EF Core prepared the query.</summary>
    public abstract bool PreparedWhileRetrying { get; }

    /// <summary>
    ///     The SQL and parameters of the prepared query, used to build the batch command, or null when EF Core
    ///     doesn't provide them (see <see cref="QueryCommand" />).
    /// </summary>
    public abstract DbCommand? SourceCommand { get; }

    public abstract bool HasSourceCommand { get; }

    public abstract void ConfigureCommand(DbBatchCommand command);

    /// <summary>Runs the query through EF Core against the batch reader's current result set.</summary>
    public abstract Task ReadFromBatchAsync(DbDataReader reader, CancellationToken ct);

    /// <summary>Runs the query through EF Core on its own round trip.</summary>
    public abstract Task ExecuteAsync(CancellationToken ct);

    public abstract void Fail(Exception exception);
}

internal sealed class QueuedQuery<T, TResult> : QueuedQuery
{
    private readonly DbContext _context;
    private readonly IAsyncEnumerable<T> _prepared;
    private readonly Lazy<DbCommand?> _sourceCommand;
    private readonly Lazy<bool> _isSplitQuery;
    private readonly Func<IAsyncEnumerable<T>, CancellationToken, Task<TResult>> _read;
    private readonly TaskCompletionSource<TResult> _completion = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public QueuedQuery(DbContext context, IQueryable<T> queryable, bool preparedWhileRetrying,
        Func<IAsyncEnumerable<T>, CancellationToken, Task<TResult>> read)
    {
        if (queryable.Provider is not IAsyncQueryProvider provider)
        {
            throw new InvalidOperationException(
                $"BatchedQuery can only run EF Core queries, but this query's provider is {queryable.Provider.GetType().Name}.");
        }

        // EF Core gives each DbContext its own query provider, which runs the query on that context
        if (!ReferenceEquals(provider, context.GetService<IAsyncQueryProvider>()))
        {
            throw new InvalidOperationException(
                $"BatchedQuery can only run queries of the {context.GetType().Name} it was created for, but this query belongs to another DbContext.");
        }

        _context = context;
        _read = read;
        PreparedWhileRetrying = preparedWhileRetrying;

        // The cancellation token comes later, when the prepared query is enumerated, as in EF Core's own queries
        _prepared = provider.ExecuteAsync<IAsyncEnumerable<T>>(queryable.Expression, CancellationToken.None);
        _sourceCommand = new Lazy<DbCommand?>(() => QueryCommand.TryCreate(_prepared));
        _isSplitQuery = new Lazy<bool>(() => _prepared is IQueryingEnumerable querying &&
            querying.ToQueryString().EndsWith(RelationalStrings.SplitQueryString, StringComparison.Ordinal));
    }

    public Task<TResult> Result => _completion.Task;

    public override bool IsSplitQuery => _isSplitQuery.Value;

    public override bool PreparedWhileRetrying { get; }

    public override DbCommand? SourceCommand => _sourceCommand.Value;

    public override bool HasSourceCommand => _sourceCommand.IsValueCreated && _sourceCommand.Value != null;

    public override void ConfigureCommand(DbBatchCommand command)
    {
        var source = SourceCommand!;
        command.CommandText = source.CommandText;
        foreach (DbParameter param in source.Parameters)
        {
            // The provider's own clone keeps provider-specific types, like Npgsql's jsonb, that DbType can't express
            if (param is ICloneable cloneable)
            {
                command.Parameters.Add(cloneable.Clone());
                continue;
            }

            var clone = command.CreateParameter();
            clone.ParameterName = param.ParameterName;
            clone.Value = param.Value;
            clone.DbType = param.DbType;
            clone.Direction = param.Direction;
            clone.Size = param.Size;
            command.Parameters.Add(clone);
        }
    }

    public override async Task ReadFromBatchAsync(DbDataReader reader, CancellationToken ct)
    {
        BatchedQueryInterceptor.Supply(_context, reader, SourceCommand!.CommandText);
        try
        {
            var result = await _read(_prepared, ct).ConfigureAwait(false);

            if (BatchedQueryInterceptor.IsPending(_context))
            {
                throw new InvalidOperationException(
                    "EF Core completed a batched query without reading the result set the batch returned for it.");
            }

            _completion.TrySetResult(result);
        }
        catch (Exception e)
        {
            _completion.TrySetException(e);
            throw;
        }
        finally
        {
            BatchedQueryInterceptor.Clear(_context);
        }
    }

    public override async Task ExecuteAsync(CancellationToken ct)
    {
        try
        {
            _completion.TrySetResult(await _read(_prepared, ct).ConfigureAwait(false));
        }
        catch (Exception e)
        {
            _completion.TrySetException(e);
            throw;
        }
    }

    public override void Fail(Exception exception) => _completion.TrySetException(exception);
}
