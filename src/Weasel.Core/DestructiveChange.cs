using JasperFx;

namespace Weasel.Core;

/// <summary>
///     The migrator's one data-destroying branch, named so it can be warned about before it runs
///     and refused by teams who want <see cref="AutoCreate.All" />'s convenience for additive
///     changes only (weasel#600).
/// </summary>
/// <remarks>
///     When a delta reports <see cref="SchemaPatchDifference.Invalid" /> and cannot rebuild in
///     place, <see cref="Migrator.WriteUpdate" /> writes a <c>DROP</c> followed by a
///     <c>CREATE</c>. For a table that is the table's data. Every <see cref="AutoCreate" /> except
///     <see cref="AutoCreate.All" /> is refused before reaching it, so the branch is only live in
///     the mode a developer sets on their own machine -- and then points at a database with rows
///     in it.
/// </remarks>
public static class DestructiveChange
{
    /// <summary>
    ///     Would applying this delta drop and recreate the object rather than alter or rebuild it?
    /// </summary>
    public static bool DropsAndRecreates(ISchemaObjectDelta delta)
    {
        return delta.Difference == SchemaPatchDifference.Invalid
               && delta is not ISchemaObjectDeltaWithRebuild { CanRebuildInPlace: true };
    }

    /// <summary>
    ///     Why this delta cannot be applied incrementally, from the delta itself when it says and
    ///     from a generic description when it does not.
    /// </summary>
    public static string DescribeReason(ISchemaObjectDelta delta)
    {
        if (delta is ISchemaObjectDeltaWithReason { InvalidReason: { } reason } && !string.IsNullOrWhiteSpace(reason))
        {
            return reason;
        }

        return "the change cannot be expressed as an ALTER";
    }

    /// <summary>
    ///     The warning text, used both by the apply path before it runs the DROP and by
    ///     <c>resources check</c> / <c>db-patch</c> before anything runs at all, so the two say the
    ///     same thing.
    /// </summary>
    public static string Describe(ISchemaObjectDelta delta)
    {
        return
            $"AutoCreate.All is dropping and recreating {delta.SchemaObject.Identifier} because {DescribeReason(delta)}; "
            + "any rows in it will be lost. Use db-patch to see the migration, or AutoCreate.CreateOrUpdate to be refused instead.";
    }

    /// <summary>
    ///     The same sentence in the future tense, for the paths that report on a migration without
    ///     applying it.
    /// </summary>
    public static string DescribePending(ISchemaObjectDelta delta)
    {
        return
            $"{delta.SchemaObject.Identifier} would be dropped and recreated because {DescribeReason(delta)}; "
            + "any rows in it would be lost.";
    }
}
