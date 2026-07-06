using System.Data.Common;
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

    NpgsqlParameter AppendParameter<T>(T value);
    NpgsqlParameter AppendParameter<T>(T value, NpgsqlDbType dbType);
    NpgsqlParameter AppendParameter(object value);
    NpgsqlParameter AppendParameter(object? value, NpgsqlDbType? dbType);
    void AppendParameters(params object[] parameters);

    IGroupedParameterBuilder CreateGroupedParameterBuilder(char? seperator = null);

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

    /// <summary>
    ///     Append a SQL string with `?` placeholders for new parameters, and returns an
    ///     array of the newly created parameters upcast to the dialect-neutral <see cref="DbParameter" />.
    ///     Lets a database-agnostic consumer fill parameter slots without referencing <see cref="NpgsqlParameter" />.
    /// </summary>
    /// <param name="text"></param>
    /// <returns></returns>
    DbParameter[] AppendWithDbParameters(string text);

    /// <summary>
    ///     Append a SQL string with user defined placeholder characters for new parameters, and returns an
    ///     array of the newly created parameters upcast to the dialect-neutral <see cref="DbParameter" />.
    ///     Lets a database-agnostic consumer fill parameter slots without referencing <see cref="NpgsqlParameter" />.
    /// </summary>
    /// <param name="text"></param>
    /// <param name="placeholder"></param>
    /// <returns></returns>
    DbParameter[] AppendWithDbParameters(string text, char placeholder);

    void StartNewCommand();

    /// <summary>
    ///     Use an anonymous type to add named parameters.
    ///     If a dictionary is passed in then its key-value pairs will be used as named parameters.
    ///     <para>
    ///     Annotated <see cref="System.Diagnostics.CodeAnalysis.RequiresUnreferencedCodeAttribute" /> to
    ///     match <see cref="Core.CommandBuilderBase{TCommand, TParameter, TParameterType}.AddParameters(object)" />
    ///     — both reflect over the parameters object's public properties. AOT-trim-clean
    ///     consumers should prefer the dictionary overloads below.
    ///     </para>
    /// </summary>
    /// <param name="parameters"></param>
    [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode(
        "AddParameters(object) reflects on the parameters object's public properties via Type.GetProperties(). Use the IDictionary<string, T> overload when publishing AOT-trim-clean.")]
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
