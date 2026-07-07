using System.Data.Common;
using Npgsql;
using NpgsqlTypes;

namespace Weasel.Postgresql;

/// <summary>
///     PostgreSQL grouped-parameter surface. Derives from the dialect-neutral
///     <see cref="Weasel.Core.IGroupedParameterBuilder" /> and adds the Npgsql-typed overloads that
///     return <see cref="NpgsqlParameter" />.
/// </summary>
public interface IGroupedParameterBuilder: Weasel.Core.IGroupedParameterBuilder
{
    NpgsqlParameter AppendParameter<T>(T? value) where T : notnull;
    NpgsqlParameter AppendParameter<T>(T? value, NpgsqlDbType dbType) where T : notnull;
}

public sealed class GroupedParameterBuilder: IGroupedParameterBuilder
{
    private readonly ICommandBuilder _commandBuilder;
    private readonly char? _seperator;
    private int _count = 0;

    public GroupedParameterBuilder(ICommandBuilder commandBuilder, char? seperator)
    {
        _commandBuilder = commandBuilder;
        _seperator = seperator;
    }

    public NpgsqlParameter AppendParameter<T>(T? value) where T : notnull
    {
        if(_count > 0 && _seperator.HasValue)
            _commandBuilder.Append(_seperator.Value);

        _count++;
        return _commandBuilder.AppendParameter(value);
    }

    public NpgsqlParameter AppendParameter<T>(T? value, NpgsqlDbType dbType) where T : notnull
    {
        if(_count > 0 && _seperator.HasValue)
            _commandBuilder.Append(_seperator.Value);

        _count++;
        return _commandBuilder.AppendParameter(value, dbType);
    }

    DbParameter Weasel.Core.IGroupedParameterBuilder.AppendParameter(object? value)
    {
        if (_count > 0 && _seperator.HasValue)
            _commandBuilder.Append(_seperator.Value);

        _count++;
        return _commandBuilder.AppendParameter(value ?? DBNull.Value);
    }
}
