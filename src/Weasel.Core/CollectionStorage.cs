namespace Weasel.Core;

/// <summary>
///     Governs .Net collection serialization.
///     <para>
///     Lifted into Weasel.Core in weasel#286 — byte-identical between Marten
///     (<c>Marten.CollectionStorage</c>) and Polecat
///     (<c>Polecat.Serialization.CollectionStorage</c>).
///     </para>
/// </summary>
public enum CollectionStorage
{
    /// <summary>
    ///     Use default serialization for collections according to the serializer
    ///     being used
    /// </summary>
    Default,

    /// <summary>
    ///     Direct the underlying serializer to serialize collections as JSON arrays
    /// </summary>
    AsArray
}
