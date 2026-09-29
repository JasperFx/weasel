using FirebirdSql.Data.FirebirdClient;

namespace Weasel.Firebird;

/// <summary>
///     Firebird command-builder surface. Derives from the dialect-neutral
///     <see cref="Weasel.Core.ICommandBuilder" /> and adds the FirebirdClient-typed overloads that return
///     <see cref="FbParameter" />.
/// </summary>
public interface ICommandBuilder: Weasel.Core.ICommandBuilder
{
    FbParameter AppendParameter<T>(T value);
    FbParameter AppendParameter<T>(T value, FbDbType dbType);

    /// <summary>
    ///     FirebirdClient-typed override of <see cref="Weasel.Core.ICommandBuilder.AppendParameter(object)" />.
    /// </summary>
    new FbParameter AppendParameter(object value);

    FbParameter AppendParameter(object? value, FbDbType? dbType);

    /// <summary>
    ///     Append a SQL string with `?` placeholders for new parameters, and returns an
    ///     array of the newly created parameters
    /// </summary>
    FbParameter[] AppendWithParameters(string text);

    /// <summary>
    ///     Append a SQL string with user defined placeholder characters for new parameters, and returns an
    ///     array of the newly created parameters
    /// </summary>
    FbParameter[] AppendWithParameters(string text, char placeholder);
}
