using System.Data;
using JasperFx.Core;
using JasperFx.Core.Reflection;
using Npgsql;
using NpgsqlTypes;
using Weasel.Core.Operations;
using Weasel.Core.Serialization;

namespace Weasel.Postgresql;

public sealed class GroupedParameterBuilder: IGroupedParameterBuilder
{
    private readonly IPostgresqlCommandBuilder _commandBuilder;
    private readonly char? _separator;
    private int _count = 0;

    public GroupedParameterBuilder(IPostgresqlCommandBuilder commandBuilder, char? separator)
    {
        _commandBuilder = commandBuilder;
        _separator = separator;
    }

    public void AppendParameter<T>(T? value) where T : notnull
    {
        if(_count > 0 && _separator.HasValue)
            _commandBuilder.Append(_separator.Value);

        _count++;
        _commandBuilder.AppendParameter(value);
    }

    public void AppendParameter(object value, DbType dbType)
    {
        if(_count > 0 && _separator.HasValue)
        {
            _commandBuilder.Append(_separator.Value);
        }

        _count++;
        _commandBuilder.AppendParameter(value, dbType);
    }

    public NpgsqlParameter AppendParameter<T>(T? value, NpgsqlDbType? dbType) where T : notnull
    {
        if(_count > 0 && _separator.HasValue)
        {
            _commandBuilder.Append(_separator.Value);
        }

        _count++;
        return _commandBuilder.AppendParameter(value, dbType);
    }

    public void AppendJsonParameter(ISerializer serializer, object value)
    {
        if (value == null)
        {
            AppendParameter(DBNull.Value, NpgsqlDbType.Jsonb);
        }
        else
        {
            AppendParameter(serializer.ToJson(value), NpgsqlDbType.Jsonb);
        }
    }

    public void AppendJsonArrayParameter(ISerializer serializer, object[] value)
    {
        var values = value.Select(x => serializer.ToJson(x)).ToArray();
        AppendParameter(values, NpgsqlDbType.Array | NpgsqlDbType.Jsonb);
    }

    public void AppendTextParameter(string value)
    {
        AppendParameter(value);
    }

    public void AppendNull(DbType dbType)
    {
        if(_count > 0 && _separator.HasValue)
            _commandBuilder.Append(_separator.Value);

        _count++;
        _commandBuilder.AppendParameter(DBNull.Value, dbType);
    }

    public void AppendStringArrayParameter(string[] values)
    {
        AppendParameter(values, NpgsqlDbType.Array | NpgsqlDbType.Varchar);
    }

    public void AppendGuidArrayParameter(Guid[] values)
    {
        AppendParameter(values, NpgsqlDbType.Array | NpgsqlDbType.Uuid);
    }

    public void AppendEnumAsString<T>(T? value) where T : struct
    {
        if (value != null)
        {
            AppendParameter(value.ToString());
        }
        else
        {
            AppendNull(DbType.String);
        }
    }

    public void AppendEnumAsInteger<T>(T? value) where T : struct
    {
        if (value != null)
        {
            AppendParameter(value.As<int>());
        }
        else
        {
            AppendNull(DbType.Int32);
        }
    }
}
