using Oracle.ManagedDataAccess.Client;

namespace Weasel.Oracle;

/// <summary>
///     Oracle command-builder surface. Derives from the dialect-neutral
///     <see cref="Weasel.Core.ICommandBuilder" /> (which contributes <see cref="Weasel.Core.ICommandBuilder.Append(string)" />,
///     <see cref="Weasel.Core.ICommandBuilder.AppendWithDbParameters(string)" />, <c>AddParameters</c>, tenant id, etc.)
///     and adds the ODP.NET-typed overloads that return <see cref="OracleParameter" />.
/// </summary>
public interface ICommandBuilder: Weasel.Core.ICommandBuilder
{
    OracleParameter AppendParameter<T>(T value);
    OracleParameter AppendParameter<T>(T value, OracleDbType dbType);

    /// <summary>
    ///     ODP.NET-typed override of <see cref="Weasel.Core.ICommandBuilder.AppendParameter(object)" />.
    /// </summary>
    new OracleParameter AppendParameter(object value);

    OracleParameter AppendParameter(object? value, OracleDbType? dbType);

    /// <summary>
    ///     Append a SQL string with `?` placeholders for new parameters, and returns an
    ///     array of the newly created parameters
    /// </summary>
    /// <param name="text"></param>
    /// <returns></returns>
    OracleParameter[] AppendWithParameters(string text);

    /// <summary>
    ///     Append a SQL string with user defined placeholder characters for new parameters, and returns an
    ///     array of the newly created parameters
    /// </summary>
    /// <param name="text"></param>
    /// <param name="placeholder"></param>
    /// <returns></returns>
    OracleParameter[] AppendWithParameters(string text, char placeholder);
}
