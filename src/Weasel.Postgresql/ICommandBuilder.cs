using Npgsql;
using NpgsqlTypes;

namespace Weasel.Postgresql;

/// <summary>
///     PostgreSQL command-builder surface. Derives from the dialect-neutral
///     <see cref="Weasel.Core.ICommandBuilder" /> (which contributes <see cref="Weasel.Core.ICommandBuilder.Append(string)" />,
///     <see cref="Weasel.Core.ICommandBuilder.AppendWithDbParameters(string)" />, <c>AddParameters</c>, tenant id, etc.)
///     and adds the Npgsql-typed overloads that return <see cref="NpgsqlParameter" />.
/// </summary>
public interface ICommandBuilder: Weasel.Core.ICommandBuilder
{
    NpgsqlParameter AppendParameter<T>(T value);
    NpgsqlParameter AppendParameter<T>(T value, NpgsqlDbType dbType);
    NpgsqlParameter AppendParameter(object value);
    NpgsqlParameter AppendParameter(object? value, NpgsqlDbType? dbType);

    IGroupedParameterBuilder CreateGroupedParameterBuilder(char? seperator = null);

    /// <summary>
    ///     Append a SQL string with user defined placeholder characters for new parameters, and returns an
    ///     array of the newly created parameters
    /// </summary>
    /// <param name="text"></param>
    /// <returns></returns>
    NpgsqlParameter[] AppendWithParameters(string text);

    /// <summary>
    ///     Append a SQL string with user defined placeholder characters for new parameters, and returns an
    ///     array of the newly created parameters
    /// </summary>
    /// <param name="text"></param>
    /// <param name="placeholder"></param>
    /// <returns></returns>
    NpgsqlParameter[] AppendWithParameters(string text, char placeholder);
}
