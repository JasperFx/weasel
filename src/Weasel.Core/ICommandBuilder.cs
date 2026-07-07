using System.Data.Common;
using System.Diagnostics.CodeAnalysis;

namespace Weasel.Core;

/// <summary>
///     Dialect-neutral, non-generic command-builder surface shared by every Weasel provider's
///     command builders. Only exposes members whose signatures are identical across dialects —
///     parameters flow through the ADO.NET-neutral <see cref="DbParameter" /> (see
///     <see cref="AppendWithDbParameters(string)" />) rather than a provider-specific parameter type.
///     <para>
///     A database-agnostic consumer (e.g. a shared storage runtime) can build SQL and fill
///     parameter slots against this interface without referencing <c>Weasel.Postgresql</c> or
///     <c>Weasel.SqlServer</c>. Each provider's own <c>ICommandBuilder</c> derives from this and
///     adds the provider-typed overloads (<c>AppendParameter</c> returning the native parameter,
///     grouped parameter builders, etc.).
///     </para>
/// </summary>
public interface ICommandBuilder
{
    /// <summary>
    ///     It became so common, that it's turned out to be convenient to place
    ///     this here
    /// </summary>
    string TenantId { get; set; }

    /// <summary>
    ///     Preview the parameter name of the last appended parameter
    /// </summary>
    string? LastParameterName { get; }

    void Append(string sql);
    void Append(char character);

    void AppendParameters(params object[] parameters);

    /// <summary>
    ///     Append a single parameter with the supplied value to the underlying command's parameter
    ///     collection *and* the command text, returning the newly created parameter upcast to the
    ///     dialect-neutral <see cref="DbParameter" />. Lets a database-agnostic consumer bind a value and
    ///     set <see cref="DbParameter.DbType" /> on it without referencing the provider parameter type.
    ///     Each provider's own <c>ICommandBuilder</c> hides this with a provider-typed overload.
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    DbParameter AppendParameter(object value);

    /// <summary>
    ///     Create a dialect-neutral <see cref="IGroupedParameterBuilder" /> that appends a separated run
    ///     of positional parameters against this command builder. Each provider's own
    ///     <c>ICommandBuilder</c> hides this with an overload returning its provider-typed grouped builder.
    /// </summary>
    /// <param name="separator">Character emitted between parameters; when null, no separator is written.</param>
    /// <returns></returns>
    IGroupedParameterBuilder CreateGroupedParameterBuilder(char? separator = null);

    /// <summary>
    ///     Append a SQL string with `?` placeholders for new parameters, and returns an
    ///     array of the newly created parameters upcast to the dialect-neutral <see cref="DbParameter" />.
    ///     Lets a database-agnostic consumer fill parameter slots without referencing the
    ///     provider-specific parameter type.
    /// </summary>
    /// <param name="text"></param>
    /// <returns></returns>
    DbParameter[] AppendWithDbParameters(string text);

    /// <summary>
    ///     Append a SQL string with user defined placeholder characters for new parameters, and returns an
    ///     array of the newly created parameters upcast to the dialect-neutral <see cref="DbParameter" />.
    ///     Lets a database-agnostic consumer fill parameter slots without referencing the
    ///     provider-specific parameter type.
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
    ///     Annotated <see cref="RequiresUnreferencedCodeAttribute" /> to
    ///     match <see cref="CommandBuilderBase{TCommand, TParameter, TParameterType}.AddParameters(object)" />
    ///     — both reflect over the parameters object's public properties. AOT-trim-clean
    ///     consumers should prefer the dictionary overloads below.
    ///     </para>
    /// </summary>
    /// <param name="parameters"></param>
    [RequiresUnreferencedCode(
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
