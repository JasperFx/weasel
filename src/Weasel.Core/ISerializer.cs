using System.Data.Common;
using System.Text;

namespace Weasel.Core;

/// <summary>
///     The database-agnostic serialization surface shared by the Critter Stack document stores
///     (Marten, Polecat). Deliberately the intersection both stores rely on — BCL-typed, with no
///     provider (Npgsql / SqlClient) or store-specific members. Each store extends this with its own
///     interface: Marten adds <c>ValueCasting</c> / <c>WriteToParameter</c> / <c>ToJsonWithTypes</c>.
///     The string-based <c>FromJson</c> overloads Polecat and Fisher each declared on their own
///     interface live here since weasel#742, so a store's own declarations of them are redundant.
/// </summary>
/// <remarks>
///     Intentionally carries NO <c>RequiresUnreferencedCode</c> / <c>RequiresDynamicCode</c>
///     annotations so Marten's existing unannotated <c>ISerializer</c> can extend it without
///     inheriting new AOT warnings. Concrete reflection-based implementations (STJ / Newtonsoft)
///     carry their own trim/AOT suppressions. Part of the Critter Stack dedupe (JasperFx/polecat#273).
/// </remarks>
public interface ISerializer
{
    /// <summary>
    ///     Whether enums are persisted as integers or strings in the JSON.
    /// </summary>
    EnumStorage EnumStorage { get; }

    /// <summary>
    ///     The property naming casing strategy for persisted JSON.
    /// </summary>
    Casing Casing { get; }

    /// <summary>
    ///     Serialize a document object into a JSON string.
    /// </summary>
    string ToJson(object document);

    /// <summary>
    ///     Deserialize a JSON string into an object of type T.
    /// </summary>
    /// <remarks>
    ///     Default-implemented (weasel#742) so a serializer compiled before this member existed still
    ///     loads: the default encodes the string as UTF-8 and routes it through
    ///     <see cref="FromJson{T}(Stream)" />, which every implementer already has. A serializer that
    ///     can parse a string directly should override it, as <see cref="SystemTextJsonSerializer" />
    ///     does, to skip the encode.
    /// </remarks>
    T FromJson<T>(string json)
    {
        ArgumentNullException.ThrowIfNull(json);
        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(json), false);
        return FromJson<T>(stream);
    }

    /// <summary>
    ///     Deserialize a JSON string into the supplied type.
    /// </summary>
    /// <remarks>
    ///     Default-implemented through <see cref="FromJson(Type, Stream)" />; see
    ///     <see cref="FromJson{T}(string)" />.
    /// </remarks>
    object FromJson(Type type, string json)
    {
        ArgumentNullException.ThrowIfNull(json);
        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(json), false);
        return FromJson(type, stream);
    }

    /// <summary>
    ///     Deserialize a JSON stream into an object of type T.
    /// </summary>
    T FromJson<T>(Stream stream);

    /// <summary>
    ///     Deserialize a JSON stream into the supplied type.
    /// </summary>
    object FromJson(Type type, Stream stream);

    /// <summary>
    ///     Deserialize a JSON value from a data-reader column into an object of type T.
    /// </summary>
    T FromJson<T>(DbDataReader reader, int index);

    /// <summary>
    ///     Deserialize a JSON value from a data-reader column into the supplied type.
    /// </summary>
    object FromJson(Type type, DbDataReader reader, int index);

    /// <summary>
    ///     Asynchronously deserialize a JSON stream into an object of type T.
    /// </summary>
    ValueTask<T> FromJsonAsync<T>(Stream stream, CancellationToken cancellationToken = default);

    /// <summary>
    ///     Asynchronously deserialize a JSON stream into the supplied type.
    /// </summary>
    ValueTask<object> FromJsonAsync(Type type, Stream stream, CancellationToken cancellationToken = default);
}
