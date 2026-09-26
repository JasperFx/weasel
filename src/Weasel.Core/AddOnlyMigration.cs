namespace Weasel.Core;

/// <summary>
///     Narrows the ACTUAL side of a table delta to the objects the model declares, so that
///     <see cref="ITable.AddOnlyMigrations" /> tables never produce a <c>DROP</c> for something the
///     model does not know about (weasel#629).
/// </summary>
/// <remarks>
///     <para>
///     <c>AutoCreate.CreateOrUpdate</c> drops the columns, indexes and foreign keys the model no
///     longer declares -- reasonable when the model is the whole truth about the schema. It is not
///     reasonable when the model is <em>translated</em> from another one: a column the source model
///     knows about and the translation could not express is not "removed from the model", it is "not
///     understood", and dropping it is data loss caused by a gap in the translator.
///     </para>
///     <para>
///     Filtering at delta-construction time rather than at emission is deliberate. An extra that
///     never enters the <c>ItemDelta</c> also never makes <c>HasChanges()</c> true, so an
///     EF-derived table whose only difference is an unmapped column reports <c>None</c> instead of
///     an <c>Update</c> that would write no SQL -- the empty-change-set shape weasel#399 was about.
///     </para>
/// </remarks>
public static class AddOnlyMigration
{
    /// <summary>
    ///     Keep only the actual items whose name the expected side declares. Names are matched
    ///     case-insensitively, the same way <c>ItemDelta</c> pairs the two sides, so exactly the
    ///     items that would have become <c>Extras</c> are the ones withheld.
    /// </summary>
    /// <param name="expectedItems">What the model declares</param>
    /// <param name="actualItems">What the catalog reports</param>
    /// <param name="kind">Singular noun for the message: "column", "index", "foreign key"</param>
    /// <param name="withheld">Collects a description of each drop that was withheld, for logging</param>
    public static IReadOnlyList<T> DeclaredOnly<T>(
        IEnumerable<T> expectedItems,
        IEnumerable<T> actualItems,
        string kind,
        ICollection<string> withheld) where T : INamed
    {
        var declared = expectedItems.Select(x => x.Name).ToHashSet(StringComparer.OrdinalIgnoreCase);

        var kept = new List<T>();
        foreach (var actual in actualItems)
        {
            if (declared.Contains(actual.Name))
            {
                kept.Add(actual);
            }
            else
            {
                withheld.Add($"{kind} {actual.Name}");
            }
        }

        return kept;
    }

    /// <summary>
    ///     The warning text for a table whose delta withheld one or more drops, so the apply path
    ///     and any reporting path say the same thing.
    /// </summary>
    public static string Describe(DbObjectName identifier, IReadOnlyList<string> withheld)
    {
        return
            $"Not dropping {string.Join(", ", withheld)} from {identifier}: the table is marked "
            + $"{nameof(ITable.AddOnlyMigrations)}, so a migration never removes what the model does not "
            + "declare. This is the default for a table mapped from an EF Core model, where an "
            + "undeclared object is more likely a gap in the translation than a deliberate removal. "
            + "Drop it by hand, or set AllowDrops on the mapping customization to migrate it away.";
    }
}

/// <summary>
///     Implemented by a delta that withheld a <c>DROP</c> because its table is
///     <see cref="ITable.AddOnlyMigrations" />, so the apply path can say so rather than leaving the
///     omission invisible (weasel#629).
/// </summary>
public interface ISchemaObjectDeltaWithWithheldDrops
{
    /// <summary>
    ///     One entry per object left in place, e.g. <c>"column Total_Amount"</c>. Empty when the
    ///     delta withheld nothing, which is the case for every table that is not add-only.
    /// </summary>
    IReadOnlyList<string> WithheldDrops { get; }
}
