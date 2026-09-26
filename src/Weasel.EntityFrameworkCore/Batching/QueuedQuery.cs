using System.Data.Common;
using Microsoft.EntityFrameworkCore;

namespace Weasel.EntityFrameworkCore.Batching;

/// <summary>
///     A query queued in a <see cref="BatchedQuery" />. EF Core always runs the query and
///     materializes its results; when the query is batched, <see cref="BatchedQueryInterceptor" />
///     hands EF Core the query's result set from the batch instead of letting it hit the database.
/// </summary>
internal abstract class QueuedQuery
{
    /// <summary>A split query sends more than one command, which a single result set can't answer.</summary>
    public abstract bool IsSplitQuery { get; }

    /// <summary>The SQL and parameters EF Core generates for the query, used to build the batch command.</summary>
    public abstract DbCommand SourceCommand { get; }

    public abstract bool HasSourceCommand { get; }

    public abstract void ConfigureCommand(DbBatchCommand command);

    /// <summary>Runs the query through EF Core against the batch reader's current result set.</summary>
    public abstract Task ReadFromBatchAsync(DbDataReader reader, CancellationToken ct);

    /// <summary>Runs the query through EF Core on its own round trip.</summary>
    public abstract Task ExecuteAsync(CancellationToken ct);

    public abstract void Fail(Exception exception);
}

internal sealed class QueuedQuery<TResult> : QueuedQuery
{
    private readonly DbContext _context;
    private readonly Lazy<DbCommand> _sourceCommand;
    private readonly Func<CancellationToken, Task<TResult>> _execute;
    private readonly TaskCompletionSource<TResult> _completion = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public QueuedQuery(DbContext context, IQueryable queryable, bool isSplitQuery,
        Func<CancellationToken, Task<TResult>> execute)
    {
        _context = context;
        _sourceCommand = new Lazy<DbCommand>(queryable.CreateDbCommand);
        _execute = execute;
        IsSplitQuery = isSplitQuery;
    }

    public Task<TResult> Result => _completion.Task;

    public override bool IsSplitQuery { get; }

    public override DbCommand SourceCommand => _sourceCommand.Value;

    public override bool HasSourceCommand => _sourceCommand.IsValueCreated;

    public override void ConfigureCommand(DbBatchCommand command)
    {
        command.CommandText = SourceCommand.CommandText;
        foreach (DbParameter param in SourceCommand.Parameters)
        {
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
        BatchedQueryInterceptor.Supply(_context, reader, SourceCommand.CommandText);
        try
        {
            var result = await _execute(ct).ConfigureAwait(false);

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
            _completion.TrySetResult(await _execute(ct).ConfigureAwait(false));
        }
        catch (Exception e)
        {
            _completion.TrySetException(e);
            throw;
        }
    }

    public override void Fail(Exception exception) => _completion.TrySetException(exception);
}
