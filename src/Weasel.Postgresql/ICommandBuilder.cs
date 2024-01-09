using Npgsql;
using NpgsqlTypes;

namespace Weasel.Postgresql;

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
    NpgsqlParameter AppendParameter(object value);
    NpgsqlParameter AppendParameter(object? value, NpgsqlDbType? dbType);
    void AppendParameters(params object[] parameters);

    /// <summary>
    ///     Append a SQL string with user defined placeholder characters for new parameters, and returns an
    ///     array of the newly created parameters
    /// </summary>
    /// <param name="text"></param>
    /// <param name="separator"></param>
    /// <returns></returns>
    NpgsqlParameter[] AppendWithParameters(string text);

    /// <summary>
    ///     Append a SQL string with user defined placeholder characters for new parameters, and returns an
    ///     array of the newly created parameters
    /// </summary>
    /// <param name="text"></param>
    /// <param name="separator"></param>
    /// <returns></returns>
    NpgsqlParameter[] AppendWithParameters(string text, char placeholder);

    void StartNewCommand();

    /// <summary>
    /// Use an anonymous type to add named parameters
    /// </summary>
    /// <param name="parameters"></param>
    void AddParameters(object parameters);
}
