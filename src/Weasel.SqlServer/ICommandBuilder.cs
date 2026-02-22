using System.Data;
using Microsoft.Data.SqlClient;

namespace Weasel.SqlServer;

public interface ICommandBuilder
{
    /// <summary>
    /// It became so common, that it's turned out to be convenient to place
    /// this here
    /// </summary>
    string TenantId { get; set; }

    /// <summary>
    /// Preview the parameter name of the last appended parameter
    /// </summary>
    string? LastParameterName { get; }

    void Append(string sql);
    void Append(char character);

    SqlParameter AppendParameter<T>(T value);
    SqlParameter AppendParameter<T>(T value, SqlDbType dbType);
    SqlParameter AppendParameter(object value);
    SqlParameter AppendParameter(object? value, SqlDbType? dbType);
    void AppendParameters(params object[] parameters);

    IGroupedParameterBuilder CreateGroupedParameterBuilder(char? seperator = null);

    /// <summary>
    ///     Append a SQL string with user defined placeholder characters for new parameters, and returns an
    ///     array of the newly created parameters
    /// </summary>
    /// <param name="text"></param>
    /// <returns></returns>
    SqlParameter[] AppendWithParameters(string text);

    /// <summary>
    ///     Append a SQL string with user defined placeholder characters for new parameters, and returns an
    ///     array of the newly created parameters
    /// </summary>
    /// <param name="text"></param>
    /// <param name="placeholder"></param>
    /// <returns></returns>
    SqlParameter[] AppendWithParameters(string text, char placeholder);

    void StartNewCommand();

    /// <summary>
    ///     Use an anonymous type to add named parameters.
    ///     If a dictionary is passed in then its key-value pairs will be used as named parameters
    /// </summary>
    /// <param name="parameters"></param>
    void AddParameters(object parameters);

    /// <summary>
    ///     Use a dictionary to add named parameters
    /// </summary>
    /// <param name="parameters"></param>
    void AddParameters(IDictionary<string, object?> parameters);

    /// <summary>
    ///     Use a dictionary to add named parameters
    /// </summary>
    /// <param name="parameters"></param>
    void AddParameters<T>(IDictionary<string, T> parameters);
}

public interface IGroupedParameterBuilder
{
    SqlParameter AppendParameter<T>(T? value) where T : notnull;
    SqlParameter AppendParameter<T>(T? value, SqlDbType dbType) where T : notnull;
}

public sealed class GroupedParameterBuilder: IGroupedParameterBuilder
{
    private readonly ICommandBuilder _commandBuilder;
    private readonly char? _seperator;
    private int _count;

    public GroupedParameterBuilder(ICommandBuilder commandBuilder, char? seperator)
    {
        _commandBuilder = commandBuilder;
        _seperator = seperator;
    }

    public SqlParameter AppendParameter<T>(T? value) where T : notnull
    {
        if (_count > 0 && _seperator.HasValue)
            _commandBuilder.Append(_seperator.Value);

        _count++;
        return _commandBuilder.AppendParameter(value);
    }

    public SqlParameter AppendParameter<T>(T? value, SqlDbType dbType) where T : notnull
    {
        if (_count > 0 && _seperator.HasValue)
            _commandBuilder.Append(_seperator.Value);

        _count++;
        return _commandBuilder.AppendParameter(value, dbType);
    }
}
