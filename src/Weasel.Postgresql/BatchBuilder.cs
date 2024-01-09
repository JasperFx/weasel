using System.Text;
using Npgsql;
using NpgsqlTypes;

namespace Weasel.Postgresql;

public class BatchBuilder: ICommandBuilder
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

    public NpgsqlParameter AppendParameter(object value)
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

        return param;
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

    /// <summary>
    ///     Append a SQL string with user defined placeholder characters for new parameters, and returns an
    ///     array of the newly created parameters
    /// </summary>
    /// <param name="text"></param>
    /// <param name="separator"></param>
    /// <returns></returns>
    public NpgsqlParameter[] AppendWithParameters(string text)
    {
        return AppendWithParameters(text, '?');
    }

    public NpgsqlParameter[] AppendWithParameters(string text, char placeholder)
    {
        var split = text.Split(placeholder);
        var parameters = new NpgsqlParameter[split.Length - 1];

        _builder.Append(split[0]);
        for (var i = 0; i < parameters.Length; i++)
        {
            // Just need a placeholder parameter type and value
            var parameter = AppendParameter(DBNull.Value, NpgsqlDbType.Text);
            parameters[i] = parameter;
            _builder.Append(split[i + 1]);
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

        var properties = parameters.GetType().GetProperties();
        foreach (var property in properties)
        {
            var value = property.GetValue(parameters);

            _current.Parameters.Add(new() { Value = value ?? DBNull.Value, ParameterName = property.Name });
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
