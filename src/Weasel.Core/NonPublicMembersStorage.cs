namespace Weasel.Core;

/// <summary>
///     Governs which non-public .Net members the serializer is allowed to use
///     when reading documents back from the database (non-public setters,
///     constructors, etc.).
///     <para>
///     Lifted into Weasel.Core in weasel#286 — byte-identical (including the
///     <see cref="All" /> flag composition) between Marten
///     (<c>Marten.NonPublicMembersStorage</c>) and Polecat
///     (<c>Polecat.Serialization.NonPublicMembersStorage</c>).
///     </para>
/// </summary>
[Flags]
public enum NonPublicMembersStorage
{
    Default = 0,
    NonPublicSetters = 1,
    NonPublicDefaultConstructor = 2,
    NonPublicConstructor = 4,
    All = Default | NonPublicSetters | NonPublicDefaultConstructor | NonPublicConstructor
}
