using System.Data.Common;
using System.Text;
using JasperFx.Core;

namespace Weasel.Core;

public interface ICommandBuilder<out TCommand>
    where TCommand : DbCommand
{
    public TCommand Compile();
}

/// <summary>
///     Base class for batch queries against a relational database
/// </summary>
/// <typeparam name="TCommand"></typeparam>
/// <typeparam name="TParameter"></typeparam>
/// <typeparam name="TTransaction"></typeparam>
/// <typeparam name="TParameterType"></typeparam>
/// <typeparam name="TDataReader"></typeparam>
public class CommandBuilderBase<TCommand, TParameter, TParameterType>: ICommandBuilder<TCommand>
    where TCommand : DbCommand
    where TParameter : DbParameter
    where TParameterType : struct
{
    protected readonly TCommand _command;
    private readonly char _parameterPrefix;

    private readonly IDatabaseProvider<TCommand, TParameter, TParameterType>
        _provider;

    // TEMP -- will shift this to being pooled later
    private readonly StringBuilder _sql = new();

    protected CommandBuilderBase(
        IDatabaseProvider<TCommand, TParameter, TParameterType> provider,
        char parameterPrefix,
        TCommand command
    )
    {
        _provider = provider;
        _parameterPrefix = parameterPrefix;
        _command = command;

        if (_command.CommandText.IsNotEmpty())
        {
            _sql.Append(_command.CommandText);
        }
    }

    /// <summary>
    /// Preview the parameter name of the last appended parameter
    /// </summary>
    public string? LastParameterName => _command.Parameters.Count == 0
        ? null
        : _command.Parameters[^1].ParameterName;

    /// <summary>
    ///     Add text to the batched command SQL string
    /// </summary>
    /// <param name="text"></param>
    public void Append(string text)
    {
        _sql.Append(text);
    }

    /// <summary>
    ///     Add text to the batched command SQL string
    /// </summary>
    /// <param name="character"></param>
    public void Append(char character)
    {
        _sql.Append(character);
    }

    /// <summary>
    ///     Add text to the batched command SQL string
    /// </summary>
    /// <param name="value"></param>
    public void Append(object value)
    {
        _sql.Append(value);
    }

    /// <summary>
    ///     Build out the batched ADO.Net command
    /// </summary>
    /// <returns></returns>
    public TCommand Compile()
    {
        _command.CommandText = _sql.ToString();
        return _command;
    }

    /// <summary>
    ///     Adds a parameter to the underlying command, but does NOT add the
    ///     parameter usage to the command text
    /// </summary>
    /// <param name="value"></param>
    /// <param name="dbType"></param>
    /// <returns></returns>
    public TParameter AddParameter(object? value, TParameterType? dbType = null)
    {
        var name = "p" + _command.Parameters.Count;

        var parameter = (TParameter)_command.CreateParameter();
        parameter.ParameterName = name;
        parameter.Value = value ?? DBNull.Value;

        if (dbType.HasValue)
        {
            _provider.SetParameterType(parameter, dbType.Value);
        }

        _provider.AddParameter(_command, parameter);

        return parameter;
    }

    /// <summary>
    ///     Finds or adds a new parameter with the specified name and returns the parameter
    /// </summary>
    /// <param name="command"></param>
    /// <param name="name"></param>
    /// <param name="value"></param>
    /// <param name="dbType"></param>
    /// <returns></returns>
    public TParameter AddNamedParameter(string name, object value, TParameterType? dbType = null)
    {
        var existing = _command.Parameters.OfType<TParameter>().FirstOrDefault(x => x.ParameterName == name);
        if (existing != null)
        {
            return existing;
        }

        var parameter = (TParameter)_command.CreateParameter();
        parameter.ParameterName = name;
        parameter.Value = value ?? DBNull.Value;


        if (dbType.HasValue)
        {
            _provider.SetParameterType(parameter, dbType.Value);
        }
        else if (value != null)
        {
            _provider.SetParameterType(parameter, _provider.ToParameterType(value.GetType()));
        }

        parameter.Value = value ?? DBNull.Value;
        _command.Parameters.Add(parameter);

        return parameter;
    }

    /// <summary>
    ///     Finds or adds a new parameter with the specified name and returns the parameter
    /// </summary>
    /// <param name="command"></param>
    /// <param name="name"></param>
    /// <param name="value"></param>
    /// <param name="dbType"></param>
    /// <returns></returns>
    public TParameter AddNamedParameter(string name, string value)
    {
        return AddNamedParameter(name, value, _provider.StringParameterType);
    }

    /// <summary>
    ///     Finds or adds a new parameter with the specified name and returns the parameter
    /// </summary>
    /// <param name="command"></param>
    /// <param name="name"></param>
    /// <param name="value"></param>
    /// <param name="dbType"></param>
    /// <returns></returns>
    public TParameter AddNamedParameter(string name, bool value)
    {
        return AddNamedParameter(name, value, _provider.BoolParameterType);
    }

    /// <summary>
    ///     Finds or adds a new parameter with the specified name and returns the parameter
    /// </summary>
    /// <param name="command"></param>
    /// <param name="name"></param>
    /// <param name="value"></param>
    /// <param name="dbType"></param>
    /// <returns></returns>
    public TParameter AddNamedParameter(string name, int value)
    {
        return AddNamedParameter(name, value, _provider.IntegerParameterType);
    }

    /// <summary>
    ///     Finds or adds a new parameter with the specified name and returns the parameter
    /// </summary>
    /// <param name="command"></param>
    /// <param name="name"></param>
    /// <param name="value"></param>
    /// <param name="dbType"></param>
    /// <returns></returns>
    public TParameter AddNamedParameter(string name, double value)
    {
        return AddNamedParameter(name, value, _provider.DoubleParameterType);
    }

    /// <summary>
    ///     Finds or adds a new parameter with the specified name and returns the parameter
    /// </summary>
    /// <param name="command"></param>
    /// <param name="name"></param>
    /// <param name="value"></param>
    /// <param name="dbType"></param>
    /// <returns></returns>
    public TParameter AddNamedParameter(string name, long value)
    {
        return AddNamedParameter(name, value, _provider.LongParameterType);
    }

    public override string ToString()
    {
        return _sql.ToString();
    }

    /// <summary>
    ///     Append a parameter with the supplied value to the underlying command
    ///     parameter collection *and* the command text
    /// </summary>
    /// <param name="value"></param>
    /// <param name="dbType"></param>
    public void AppendParameter(object? value, TParameterType? dbType = null)
    {
        var parameter = AddParameter(value, dbType);
        Append(_parameterPrefix);
        Append(parameter.ParameterName);
    }

    /// <summary>
    ///     Append a parameter with the supplied value to the underlying command
    ///     parameter collection *and* the command text
    /// </summary>
    /// <param name="value"></param>
    /// <param name="dbType"></param>
    public void AppendParameter(int value)
    {
        AppendParameter(value, _provider.IntegerParameterType);
    }

    /// <summary>
    ///     Append a parameter with the supplied value to the underlying command
    ///     parameter collection *and* the command text
    /// </summary>
    /// <param name="value"></param>
    /// <param name="dbType"></param>
    public void AppendParameter(Guid value)
    {
        AppendParameter(value, _provider.GuidParameterType);
    }

    /// <summary>
    ///     Append a parameter with the supplied value to the underlying command parameter
    ///     collection and adds the parameter usage to the SQL
    /// </summary>
    /// <param name="value"></param>
    /// <param name="dbType"></param>
    public void AppendParameter(string value)
    {
        AppendParameter(value, _provider.StringParameterType);
    }

    /// <summary>
    ///     For each public property of the parameters object, adds a new parameter
    ///     to the command with the name of the property and the current value of the property
    ///     on the parameters object. Does *not* affect the command text
    /// </summary>
    /// <param name="parameters"></param>
    public void AddParameters(object parameters)
    {
        if (parameters == null)
        {
            return;
        }

        var properties = parameters.GetType().GetProperties();
        foreach (var property in properties)
        {
            var value = property.GetValue(parameters);
            AddNamedParameter(property.Name, value ?? DBNull.Value, _provider.ToParameterType(property.PropertyType));
        }
    }

    /// <summary>
    ///     Append a SQL string with `?` placeholders for new parameters, and returns an
    ///     array of the newly created parameters
    /// </summary>
    /// <param name="text"></param>
    /// <returns></returns>
    public TParameter[] AppendWithParameters(string text)
    {
        var separator = '?';
        return AppendWithParameters(text, separator);
    }

    /// <summary>
    ///     Append a SQL string with user defined placeholder characters for new parameters, and returns an
    ///     array of the newly created parameters
    /// </summary>
    /// <param name="text"></param>
    /// <param name="separator"></param>
    /// <returns></returns>
    public TParameter[] AppendWithParameters(string text, char separator)
    {
        var split = text.Split(separator);
        var parameters = new TParameter[split.Length - 1];

        _sql.Append(split[0]);
        for (var i = 0; i < parameters.Length; i++)
        {
            // Just need a placeholder parameter type and value
            var parameter = AddParameter(DBNull.Value, _provider.StringParameterType);
            parameters[i] = parameter;
            _sql.Append(_parameterPrefix);
            _sql.Append(parameter.ParameterName);
            _sql.Append(split[i + 1]);
        }

        return parameters;
    }
}

// Note: Those methods are intentionally not written as extension methods
// as the preference is to use the strongly typed methods of specific databases
// instead of those generic ones
public static class CommandBuilderExtensions
{
    /// <summary>
    ///     Compile and execute the command against the user supplied connection
    /// </summary>
    /// <param name="connection"></param>
    /// <param name="commandBuilder"></param>
    /// <param name="ct"></param>
    /// <returns></returns>
    public static Task<int> ExecuteNonQueryAsync<T>(
        DbConnection connection,
        ICommandBuilder<T> commandBuilder,
        CancellationToken ct = default
    ) where T : DbCommand =>
        ExecuteNonQueryAsync(connection, commandBuilder, null, ct);

    /// <summary>
    ///     Compile and execute the command against the user supplied connection
    /// </summary>
    /// <param name="connection"></param>
    /// <param name="commandBuilder"></param>
    /// <param name="tx"></param>
    /// <param name="ct"></param>
    /// <returns></returns>
    public static Task<int> ExecuteNonQueryAsync<T>(
        DbConnection connection,
        ICommandBuilder<T> commandBuilder,
        DbTransaction? tx,
        CancellationToken ct = default
    ) where T : DbCommand
    {
        var cmd = commandBuilder.Compile();

        cmd.Connection = connection;
        cmd.Transaction = tx;

        return cmd.ExecuteNonQueryAsync(ct);
    }

    /// <summary>
    ///     Compile and execute the command against the user supplied connection and
    ///     return a data reader for the results
    /// </summary>
    /// <param name="connection"></param>
    /// <param name="commandBuilder"></param>
    /// <param name="ct"></param>
    /// <returns></returns>
    public static Task<DbDataReader> ExecuteReaderAsync<T>(
        DbConnection connection,
        ICommandBuilder<T> commandBuilder,
        CancellationToken ct = default
    ) where T : DbCommand =>
        ExecuteReaderAsync(connection, commandBuilder, null, ct);

    /// <summary>
    ///     Compile and execute the command against the user supplied connection and
    ///     return a data reader for the results
    /// </summary>
    /// <param name="connection"></param>
    /// <param name="commandBuilder"></param>
    /// <param name="tx"></param>
    /// <param name="ct"></param>
    /// <returns></returns>
    public static Task<DbDataReader> ExecuteReaderAsync<T>(
        DbConnection connection,
        ICommandBuilder<T> commandBuilder,
        DbTransaction? tx,
        CancellationToken ct = default
    ) where T : DbCommand
    {
        var cmd = commandBuilder.Compile();

        cmd.Connection = connection;
        cmd.Transaction = tx;

        return cmd.ExecuteReaderAsync(ct);
    }

    public static Task<IReadOnlyList<T>> FetchListAsync<T, TCommand>(
        DbConnection connection,
        ICommandBuilder<TCommand> commandBuilder,
        Func<DbDataReader, CancellationToken, Task<T>> transform,
        CancellationToken ct = default
    ) where TCommand : DbCommand =>
        FetchListAsync(connection, commandBuilder, transform, null, ct);

    /// <summary>
    ///     Compile and execute the query and returns the results transformed from the raw database reader
    /// </summary>
    /// <param name="connection"></param>
    /// <param name="commandBuilder"></param>
    /// <param name="transform"></param>
    /// <param name="tx"></param>
    /// <param name="ct"></param>
    /// <typeparam name="T"></typeparam>
    /// <returns></returns>
    public static async Task<IReadOnlyList<T>> FetchListAsync<T, TCommand>(
        DbConnection connection,
        ICommandBuilder<TCommand> commandBuilder,
        Func<DbDataReader, CancellationToken, Task<T>> transform,
        DbTransaction? tx,
        CancellationToken ct = default
    ) where TCommand : DbCommand
    {
        var cmd = commandBuilder.Compile();

        cmd.Connection = connection;
        cmd.Transaction = tx;

        var list = new List<T>();

        await using var reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            list.Add(await transform(reader, ct).ConfigureAwait(false));
        }

        return list;
    }
}
