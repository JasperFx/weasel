using System.Data.Common;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;

namespace Weasel.EntityFrameworkCore.Batching;

/// <summary>
///     A queued query that returns a list of entities.
///     Materializes results using EF Core's <see cref="IEntityType" /> metadata.
/// </summary>
internal sealed class ListBatchQueryItem<T> : IBatchQueryItem where T : class, new()
{
    private readonly DbCommand _sourceCommand;
    private readonly IEntityType _entityType;
    private readonly TaskCompletionSource<IReadOnlyList<T>> _completion = new();

    public Task<IReadOnlyList<T>> Result => _completion.Task;

    public ListBatchQueryItem(DbCommand sourceCommand, IEntityType entityType)
    {
        _sourceCommand = sourceCommand;
        _entityType = entityType;
    }

    public void ConfigureCommand(DbBatchCommand command)
    {
        command.CommandText = _sourceCommand.CommandText;
        foreach (DbParameter param in _sourceCommand.Parameters)
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

    public async Task ReadAsync(DbDataReader reader, CancellationToken ct)
    {
        var results = new List<T>();
        var properties = EntityMaterializer.GetColumnMap<T>(_entityType, reader);

        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            var entity = EntityMaterializer.Materialize<T>(reader, properties);
            results.Add(entity);
        }

        _completion.SetResult(results);
    }
}

/// <summary>
///     A queued query that returns a single entity or null.
/// </summary>
internal sealed class SingleBatchQueryItem<T> : IBatchQueryItem where T : class, new()
{
    private readonly DbCommand _sourceCommand;
    private readonly IEntityType _entityType;
    private readonly TaskCompletionSource<T?> _completion = new();

    public Task<T?> Result => _completion.Task;

    public SingleBatchQueryItem(DbCommand sourceCommand, IEntityType entityType)
    {
        _sourceCommand = sourceCommand;
        _entityType = entityType;
    }

    public void ConfigureCommand(DbBatchCommand command)
    {
        command.CommandText = _sourceCommand.CommandText;
        foreach (DbParameter param in _sourceCommand.Parameters)
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

    public async Task ReadAsync(DbDataReader reader, CancellationToken ct)
    {
        var properties = EntityMaterializer.GetColumnMap<T>(_entityType, reader);

        if (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            _completion.SetResult(EntityMaterializer.Materialize<T>(reader, properties));
        }
        else
        {
            _completion.SetResult(null);
        }
    }
}

/// <summary>
///     A queued query that returns a scalar value.
/// </summary>
internal sealed class ScalarBatchQueryItem<T> : IBatchQueryItem
{
    private readonly DbCommand _sourceCommand;
    private readonly TaskCompletionSource<T> _completion = new();

    public Task<T> Result => _completion.Task;

    public ScalarBatchQueryItem(DbCommand sourceCommand)
    {
        _sourceCommand = sourceCommand;
    }

    public void ConfigureCommand(DbBatchCommand command)
    {
        command.CommandText = _sourceCommand.CommandText;
        foreach (DbParameter param in _sourceCommand.Parameters)
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

    public async Task ReadAsync(DbDataReader reader, CancellationToken ct)
    {
        if (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            var value = await reader.GetFieldValueAsync<T>(0, ct).ConfigureAwait(false);
            _completion.SetResult(value);
        }
        else
        {
            _completion.SetResult(default!);
        }
    }
}
