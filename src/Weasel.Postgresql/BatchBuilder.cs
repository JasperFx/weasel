using System.Collections;
using System.Data;
using System.Data.Common;
using System.Text;
using JasperFx.Core.Reflection;
using Npgsql;
using NpgsqlTypes;
using Weasel.Core;
using Weasel.Core.Operations;
using Weasel.Core.Serialization;

namespace Weasel.Postgresql;

public class BatchBuilder: IPostgresqlCommandBuilder
{
    private readonly NpgsqlBatch _batch;
    private readonly StringBuilder _builder = new();
    private NpgsqlBatchCommand? _current;

    public BatchBuilder(NpgsqlBatch batch)
    {
        _batch = batch;
    }

    public BatchBuilder()
    {
        _batch = new NpgsqlBatch();
    }

    private NpgsqlBatchCommand appendCommand()
    {
        var command = _batch.CreateBatchCommand();
        _batch.BatchCommands.Add(command);

        return command;
    }

    public string TenantId { get; set; }

    public void SetParameterAsJson(DbParameter parameter, string json)
    {
        parameter.Value = json;
        parameter.As<NpgsqlParameter>().NpgsqlDbType = NpgsqlDbType.Jsonb;
    }

    /// <summary>
    /// Preview the parameter name of the last appended parameter
    /// </summary>
    public string? LastParameterName => _current?.Parameters.Count == 0
        ? null
        : _current!.Parameters[^1].ParameterName;

    public void Append(string sql)
    {
        _current ??= appendCommand();
        _builder.Append(sql);
    }

    public void Append(char character)
    {
        _current ??= appendCommand();
        _builder.Append(character);
    }

    public void AppendParameter<T>(T value)
    {
        _current ??= appendCommand();

        if (value == null || typeof(T) == typeof(DBNull))
        {
            var nullParam = new NpgsqlParameter
            {
                Value = DBNull.Value, NpgsqlDbType = PostgresqlProvider.Instance.ToParameterType(typeof(T))
            };

            _current.Parameters.Add(nullParam);

            _builder.Append('$');
            _builder.Append(_current.Parameters.Count);

            return;
        }

        var param = new NpgsqlParameter<T>() {
            TypedValue = value,
        };

        _current.Parameters.Add(param);

        _builder.Append('$');
        _builder.Append(_current.Parameters.Count);
    }

    public void AppendParameter<T>(T value, DbType? dbType)
    {
        _current ??= appendCommand();

        if (value is DBNull)
        {
            var nullParam = new NpgsqlParameter
            {
                Value = DBNull.Value, DbType = dbType.HasValue ? dbType.Value : DbTypeMapper.Lookup(typeof(T)).Value
            };

            _current.Parameters.Add(nullParam);

            _builder.Append('$');
            _builder.Append(_current.Parameters.Count);

            return;
        }

        var param = new NpgsqlParameter<T>() {
            TypedValue = value
        };

        if (dbType.HasValue) param.DbType = dbType.Value;

        _current.Parameters.Add(param);

        _builder.Append('$');
        _builder.Append(_current.Parameters.Count);
    }


    public void AppendParameter(object value)
    {
        _current ??= appendCommand();
        var param = new NpgsqlParameter {
            Value = value,
            // This is still important for the Marten compiled query feature
            //ParameterName = "p" + _current.Parameters.Count
        };

        _current.Parameters.Add(param);

        _builder.Append('$');
        _builder.Append(_current.Parameters.Count);
    }

    public void AppendParameters(params object[] parameters)
    {
        if (!parameters.Any())
            throw new ArgumentOutOfRangeException(nameof(parameters),
                "Must be at least one parameter value, but got " + parameters.Length);

        AppendParameter(parameters[0]);

        for (var i = 1; i < parameters.Length; i++)
        {
            _builder.Append(", ");
            AppendParameter(parameters[i]);
        }
    }

    public void AppendLongArrayParameter(long[] values)
    {
        AppendParameter(values, NpgsqlDbType.Array | NpgsqlDbType.Bigint);
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

    public void AppendJsonParameter(string json)
    {
        AppendParameter(json, NpgsqlDbType.Jsonb);
    }

    public void AppendStringArrayParameter(string[] values)
    {
        AppendParameter(values, NpgsqlDbType.Array | NpgsqlDbType.Varchar);
    }

    public void AppendGuidArrayParameter(Guid[] values)
    {
        AppendParameter(values, NpgsqlDbType.Array | NpgsqlDbType.Uuid);
    }

    public IGroupedParameterBuilder CreateGroupedParameterBuilder(char? seperator = null)
    {
        return new GroupedParameterBuilder(this, seperator);
    }

    /// <summary>
    ///     Append a SQL string with user defined placeholder characters for new parameters, and returns an
    ///     array of the newly created parameters
    /// </summary>
    /// <param name="text"></param>
    /// <param name="separator"></param>
    /// <returns></returns>
    public DbParameter[] AppendWithParameters(string text)
    {
        return AppendWithParameters(text, '?');
    }

    public DbParameter[] AppendWithParameters(string text, char separator)
    {
        var span = text.AsSpan();

        var parameters = new NpgsqlParameter[span.Count(separator)];

        var enumerator = span.Split(separator);

        var pos = 0;
        foreach (var range in enumerator)
        {
            if (range.Start.Value == 0)
            {
                // append the first part of the SQL string
                _builder.Append(span[range]);
                continue;
            }

            // Just need a placeholder parameter type and value
            AppendParameter<object>(DBNull.Value, DbType.String);
            var parameter = _current.Parameters[^1];
            parameters[pos] = parameter;
            _builder.Append(span[range]);
            pos++;
        }

        return parameters;
    }

    public void StartNewCommand()
    {
        if (_current != null)
        {
            _current.CommandText = _builder.ToString();
        }

        _builder.Clear();
        _current = appendCommand();
    }

    public void AddParameters(object parameters)
    {
        _current ??= appendCommand();

        if (parameters == null)
        {
            return;
        }

        // dictionaries should also be treated as parameter maps.
        if (parameters is IDictionary<string, object?> paramDict)
        {
            AddParameters(paramDict);
            return;
        }

        // dictionaries of any type are supported, as long as the key is string
        if (parameters is IDictionary { Keys: ICollection<string> keys } anyTypeDict)
        {
            AddParameters(keys.ToDictionary(k => k, k => anyTypeDict[k]));
            return;
        }

        var properties = parameters.GetType().GetProperties();
        foreach (var property in properties)
        {
            var value = property.GetValue(parameters);

            _current.Parameters.Add(new() { Value = value ?? DBNull.Value, ParameterName = property.Name });
        }
    }

    public void AddParameters(IDictionary<string, object?> parameters)
    {
        AddParameters<object?>(parameters);
    }

    public void AddParameters<T>(IDictionary<string, T> parameters)
    {
        _current ??= appendCommand();

        if (parameters == null)
        {
            return;
        }

        foreach (var (key, value) in parameters)
        {
            _current.Parameters.Add(new() { Value = (object?)value ?? DBNull.Value, ParameterName = key });
        }
    }

    public NpgsqlBatch Compile()
    {
        if (_batch.BatchCommands.Count == 0)
        {
            _current ??= appendCommand();
        }

        if (_current != null)
        {
            _current.CommandText = _builder.ToString();
        }

        return _batch;
    }

    // TODO -- eliminate the lookup of this thing. å
    public NpgsqlParameter AppendParameter(object? value, NpgsqlDbType? dbType)
    {
        _current ??= appendCommand();
        var param = new NpgsqlParameter { Value = value};
        if (dbType.HasValue) param.NpgsqlDbType = dbType.Value;
        _current.Parameters.Add(param);

        _builder.Append('$');
        _builder.Append(_current.Parameters.Count);

        return param;
    }
}
