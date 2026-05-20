namespace Weasel.Core;

/// <summary>
///     Governs the JSON serialization behavior of how .Net
///     member names are persisted in the JSON stored in
///     the database.
///     <para>
///     Lifted into Weasel.Core in weasel#286 — byte-identical between Marten
///     (<c>Marten.Casing</c>) and Polecat (<c>Polecat.Serialization.Casing</c>),
///     and a natural neighbour of the already-canonical
///     <see cref="EnumStorage" /> that both stores' serializers consume.
///     </para>
/// </summary>
public enum Casing
{
    /// <summary>
    ///     Exactly mimic the .Net member names in the JSON persisted to the database
    /// </summary>
    Default,

    /// <summary>
    ///     Force the .Net member names to camel casing when serialized to JSON in
    ///     the database
    /// </summary>
    CamelCase,

    /// <summary>
    ///     Force the .Net member names to snake casing when serialized to JSON in
    ///     the database
    /// </summary>
    SnakeCase
}
