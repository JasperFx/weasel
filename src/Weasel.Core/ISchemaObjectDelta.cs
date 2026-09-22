namespace Weasel.Core;

public interface ISchemaObjectDeltaWithPostProcessing : ISchemaObjectDelta
{
    void PostProcess(IList<ISchemaObjectDelta> allDeltas);
}

/// <summary>
///     A delta that can say <em>why</em> it reports
///     <see cref="SchemaPatchDifference.Invalid" /> (weasel#600).
/// </summary>
/// <remarks>
///     <para>
///     <c>Invalid</c> only says "I cannot express this as an ALTER", and the answer to it -- under
///     <see cref="JasperFx.AutoCreate.All" /> a drop and recreate, under every other mode a
///     refusal -- is the migrator's single destructive branch and its single refusal. Both used to
///     name the object and nothing else, leaving the reader to diff the table by hand to find out
///     which change was the one that could not be applied.
///     </para>
///     <para>
///     Implement this on a delta that knows. Deltas that do not implement it fall back to a
///     generic description, so this is additive.
///     </para>
/// </remarks>
public interface ISchemaObjectDeltaWithReason: ISchemaObjectDelta
{
    /// <summary>
    ///     A short phrase naming the change that cannot be applied incrementally -- "the type of
    ///     column 'amount' cannot be altered in place", say. Null when the delta has no reason to
    ///     offer.
    /// </summary>
    string? InvalidReason { get; }
}

/// <summary>
///     Models the difference between a configured ISchemaObject and the actual
///     database version of that object
/// </summary>
public interface ISchemaObjectDelta
{
    /// <summary>
    ///     The subject of this delta
    /// </summary>
    ISchemaObject SchemaObject { get; }

    SchemaPatchDifference Difference { get; }

    /// <summary>
    ///     Write the SQL to make incremental changes to the existing object
    ///     in the database to make it match the as desired configuration
    /// </summary>
    /// <param name="rules"></param>
    /// <param name="writer"></param>
    void WriteUpdate(Migrator rules, TextWriter writer);

    /// <summary>
    ///     Write the necessary SQL to rollback any incremental changes to the
    ///     existing object in this delta
    /// </summary>
    /// <param name="rules"></param>
    /// <param name="writer"></param>
    void WriteRollback(Migrator rules, TextWriter writer);

    /// <summary>
    ///     Only used to express the current state in the database for an object when
    ///     Weasel is unable to execute the detected changes
    /// </summary>
    /// <param name="rules"></param>
    /// <param name="writer"></param>
    void WriteRestorationOfPreviousState(Migrator rules, TextWriter writer);
}

/// <summary>
///     A delta that writes foreign keys as part of its object's creation, and which can hold some of
///     them back so they are applied once every delta in the migration has run.
/// </summary>
/// <remarks>
///     A key pointing at a table the same migration has not created yet references something that does
///     not exist, and ordering cannot solve it when two tables reference each other -- neither can go
///     first, so the schema could never be created from scratch. <see cref="SchemaMigration" /> decides
///     which keys to defer; it defers only the ones that would fail, so the DDL generated for schemas
///     that never had the problem is unchanged.
/// </remarks>
public interface ISchemaObjectDeltaWithDeferrableForeignKeys: ISchemaObjectDelta
{
    IEnumerable<(string Name, DbObjectName LinkedTable)> ForeignKeysToCreate { get; }

    bool HasDeferredForeignKeys { get; }

    /// <summary>
    ///     Hold back the key named <paramref name="name" />, which is one of the names this delta
    ///     reported from <see cref="ForeignKeysToCreate" />.
    /// </summary>
    void DeferForeignKey(string name);

    void WriteCreateWithoutDeferredForeignKeys(Migrator rules, TextWriter writer);

    void WriteDeferredForeignKeys(Migrator rules, TextWriter writer);
}
