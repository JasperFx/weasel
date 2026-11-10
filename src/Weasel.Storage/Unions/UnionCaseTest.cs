#nullable enable
using System;
using System.Collections.Generic;

namespace Weasel.Storage;

/// <summary>
///     The JSON value kind a union case serializes as. System.Text.Json writes the bare active case
///     of a C# union (no wrapper, no discriminator), so the kind of the stored value is the first
///     thing that tells one case from another.
/// </summary>
public enum UnionCaseJsonKind
{
    /// <summary>JSON <c>null</c> — what STJ writes for a default union, whose <c>Value</c> is null.</summary>
    Null,

    /// <summary>A JSON string.</summary>
    String,

    /// <summary>A JSON number, integral or not.</summary>
    Number,

    /// <summary>JSON <c>true</c> or <c>false</c>.</summary>
    Boolean,

    /// <summary>A JSON array.</summary>
    Array,

    /// <summary>A JSON object.</summary>
    Object
}

/// <summary>
///     A discriminator property the case type writes itself, e.g. <c>"$kind": "withdrawn"</c>. STJ
///     never writes one for a union, so this exists only when the case types carry it.
/// </summary>
/// <param name="PropertyName">The property's name exactly as serialized (naming policy applied).</param>
/// <param name="Value">The string value that identifies the case.</param>
public sealed record UnionCaseDiscriminator(string PropertyName, string Value);

/// <summary>
///     How to recognise one case of a union-typed document member in its stored JSON (weasel#744).
///     The caller derives it from the union's metadata — on .NET 11 from
///     <c>JsonTypeInfo.UnionCases</c> and each case's <c>JsonTypeInfo.Properties</c>, which already
///     apply the naming policy — and an <see cref="IUnionSqlDialect"/> renders it as a predicate.
/// </summary>
/// <remarks>
///     <para>
///         Primitive cases are told apart by <see cref="Kind"/> alone. Object cases all share
///         <see cref="UnionCaseJsonKind.Object"/>, so they are told apart by
///         <see cref="RequiredProperties"/>, <see cref="AbsentProperties"/> or a
///         <see cref="Discriminator"/>.
///     </para>
///     <para>
///         <b>Absence matters when one case's properties contain another's.</b> For
///         <c>Deposited(Amount)</c> against <c>Withdrawn(Amount, Reason)</c> the Deposited test has to
///         say <c>Reason</c> is absent, or every Withdrawn row matches it too.
///     </para>
///     <para>
///         Presence means the property exists, even with a JSON <c>null</c> value. Property names
///         are matched exactly (JSON is case-sensitive), so they must be the serialized names.
///     </para>
/// </remarks>
public sealed record UnionCaseTest
{
    public UnionCaseTest(UnionCaseJsonKind kind)
    {
        Kind = kind;
    }

    /// <summary>The JSON value kind the case serializes as.</summary>
    public UnionCaseJsonKind Kind { get; }

    /// <summary>Properties that must exist on the stored object. Object cases only.</summary>
    public IReadOnlyList<string> RequiredProperties { get; init; } = [];

    /// <summary>Properties that must not exist on the stored object. Object cases only.</summary>
    public IReadOnlyList<string> AbsentProperties { get; init; } = [];

    /// <summary>A discriminator the case type writes itself, if it writes one. Object cases only.</summary>
    public UnionCaseDiscriminator? Discriminator { get; init; }

    /// <summary>A case recognised by its JSON value kind alone.</summary>
    public static UnionCaseTest OfKind(UnionCaseJsonKind kind) => new(kind);

    /// <summary>An object case recognised by which properties it has and which it lacks.</summary>
    public static UnionCaseTest ObjectWith(IReadOnlyList<string> required, IReadOnlyList<string>? absent = null)
        => new(UnionCaseJsonKind.Object) { RequiredProperties = required, AbsentProperties = absent ?? [] };

    /// <summary>An object case recognised by a discriminator property its type writes itself.</summary>
    public static UnionCaseTest ObjectWithDiscriminator(string propertyName, string value)
        => new(UnionCaseJsonKind.Object) { Discriminator = new UnionCaseDiscriminator(propertyName, value) };

    internal void Validate()
    {
        if (Kind != UnionCaseJsonKind.Object &&
            (RequiredProperties.Count > 0 || AbsentProperties.Count > 0 || Discriminator != null))
        {
            throw new ArgumentException(
                $"Only an object case can be tested by its properties or a discriminator; this case is {Kind}.");
        }
    }
}
