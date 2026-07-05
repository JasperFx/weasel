using System.Data.Common;

namespace Weasel.Core;

/// <summary>
///     The database-agnostic serialization surface shared by the Critter Stack document stores
///     (Marten, Polecat). Deliberately the intersection both stores rely on — BCL-typed, with no
///     provider (Npgsql / SqlClient) or store-specific members. Each store extends this with its own
///     interface: Marten adds <c>ValueCasting</c> / <c>WriteToParameter</c> / <c>ToJsonWithTypes</c>;
///     Polecat adds string-based <c>FromJson</c> overloads.
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
