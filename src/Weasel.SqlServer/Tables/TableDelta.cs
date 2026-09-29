using JasperFx.Core;
using Weasel.Core;

namespace Weasel.SqlServer.Tables;

public class TableDelta: SchemaObjectDelta<Table>, ISchemaObjectDeltaWithDeferrableForeignKeys,
    ISchemaObjectDeltaWithReason, ISchemaObjectDeltaWithWithheldDrops
{
    private readonly List<string> _withheldDrops = new();

    /// <inheritdoc cref="ISchemaObjectDeltaWithWithheldDrops.WithheldDrops" />
    public IReadOnlyList<string> WithheldDrops => _withheldDrops;

    /// <summary>
    ///     Which change made this delta <see cref="SchemaPatchDifference.Invalid" /> (weasel#600).
    /// </summary>
    public string? InvalidReason { get; private set; }

    public TableDelta(Table expected, Table? actual): base(expected, actual)
    {
    }

    private readonly HashSet<string> _deferredForeignKeys = new(StringComparer.OrdinalIgnoreCase);

    public bool HasDeferredForeignKeys => _deferredForeignKeys.Count > 0;

    public void DeferForeignKey(string name) => _deferredForeignKeys.Add(name);

    public IEnumerable<(string Name, DbObjectName LinkedTable)> ForeignKeysToCreate =>
        foreignKeysThisDeltaCreates()
            .Where(x => x.LinkedTable != null)
            .Select(x => (x.Name, x.LinkedTable!));

    private IEnumerable<ForeignKey> foreignKeysThisDeltaCreates()
        => Difference switch
        {
            SchemaPatchDifference.Create or SchemaPatchDifference.Invalid => Expected.ForeignKeys,
            SchemaPatchDifference.Update => ForeignKeys.Missing,
            _ => []
        };

    public void WriteCreateWithoutDeferredForeignKeys(Migrator rules, TextWriter writer)
        => Expected.WriteCreateStatement(rules, writer, _deferredForeignKeys);

    public void WriteDeferredForeignKeys(Migrator rules, TextWriter writer)
    {
        foreach (var foreignKey in Expected.ForeignKeys.Where(x => _deferredForeignKeys.Contains(x.Name)))
        {
            foreignKey.WriteAddStatement(Expected, writer);
        }
    }

    internal ItemDelta<TableColumn> Columns { get; private set; } = null!;
    internal ItemDelta<IndexDefinition> Indexes { get; private set; } = null!;

    internal ItemDelta<ForeignKey> ForeignKeys { get; private set; } = null!;

    internal ItemDelta<TableCheckConstraint> CheckConstraints { get; private set; } = null!;


    public SchemaPatchDifference PrimaryKeyDifference { get; private set; }

    /// <summary>
    ///     Difference between the declared SQL Server RANGE partitioning and what is in the database.
    ///     <see cref="SchemaPatchDifference.Update" /> means new boundaries can be added via
    ///     <c>ALTER PARTITION FUNCTION ... SPLIT RANGE</c>; <see cref="SchemaPatchDifference.Invalid" />
    ///     means the partitioning would have to be rebuilt (column/type change or boundaries removed).
    /// </summary>
    public SchemaPatchDifference PartitioningDifference { get; private set; } = SchemaPatchDifference.None;

    protected override SchemaPatchDifference compare(Table expected, Table? actual)
    {
        if (actual == null)
        {
            return SchemaPatchDifference.Create;
        }

        _withheldDrops.Clear();

        Columns = new ItemDelta<TableColumn>(expected.Columns,
            droppable(expected, expected.Columns, actual.Columns, "column"),
            (e, a) => e.MatchesForDelta(a, expected.DetectColumnDrift));
        // IgnoreIndex is Weasel.Core API and is honoured by the PostgreSQL and SQLite twins; without
        // this SQL Server put an ignored index in Extras and WriteUpdate dropped it.
        var expectedIndexes = expected.Indexes.Where(x => !expected.HasIgnoredIndex(x.Name)).ToArray();
        Indexes = new ItemDelta<IndexDefinition>(
            expectedIndexes,
            droppable(expected, expectedIndexes, actual.Indexes.Where(x => !expected.HasIgnoredIndex(x.Name)),
                "index"),
            (e, a) => e.Matches(a, Expected));

        ForeignKeys = new ItemDelta<ForeignKey>(expected.ForeignKeys,
            droppable(expected, expected.ForeignKeys, actual.ForeignKeys, "foreign key"));

        // Conservative check-constraint comparison: only the checks the expected
        // table declares participate, and actual constraints the expected table
        // doesn't know about are never treated as extras to drop.
        var relevantActualChecks = actual.CheckConstraints
            .Where(a => expected.CheckConstraints.Any(e =>
                SchemaUtils.Unbracket(e.Name).Equals(SchemaUtils.Unbracket(a.Name), StringComparison.OrdinalIgnoreCase)));
        CheckConstraints = new ItemDelta<TableCheckConstraint>(expected.CheckConstraints, relevantActualChecks,
            checkConstraintsMatch);

        PrimaryKeyDifference = SchemaPatchDifference.None;
        if (expected.PrimaryKeyName.IsEmpty())
        {
            if (actual.PrimaryKeyName.IsNotEmpty())
            {
                PrimaryKeyDifference = SchemaPatchDifference.Update;
            }
        }
        else if (actual.PrimaryKeyName.IsEmpty())
        {
            PrimaryKeyDifference = SchemaPatchDifference.Create;
        }
        else if (!expected.PrimaryKeyOrderMatches(actual.PrimaryKeyColumns, StringComparer.Ordinal))
        {
            PrimaryKeyDifference = SchemaPatchDifference.Update;
        }

        // RANGE partitioning round-trip. Only strategies that can migrate a boundary change in place are
        // handled here; a strategy that owns its boundaries purely at runtime (ManagedTenantPartitions,
        // whose ordinals are allocated on tenant sign-up) does not implement ISplittablePartitioning and
        // is left alone.
        PartitioningDifference = SchemaPatchDifference.None;
        if (expected.SqlServerPartitioning is Partitioning.ISplittablePartitioning rangePartitioning)
        {
            PartitioningDifference = rangePartitioning.CreateDelta(actual.PartitionInfo) switch
            {
                Partitioning.PartitionDelta.None => SchemaPatchDifference.None,
                Partitioning.PartitionDelta.Additive => SchemaPatchDifference.Update,
                _ => SchemaPatchDifference.Invalid
            };
        }

        return determinePatchDifference();
    }

    /// <summary>
    ///     <see cref="TableCheckConstraint.Matches" /> compares names verbatim, and a name the
    ///     caller bracketed themselves never equals the bare name the catalog reports back.
    ///     Pairing has already matched the two on the undelimited name by this point, so all
    ///     that is left to decide is whether the expression changed.
    /// </summary>
    private static bool checkConstraintsMatch(TableCheckConstraint expected, TableCheckConstraint actual)
        => SqlServerExpression.Canonicalize(expected.Expression)
           == SqlServerExpression.Canonicalize(actual.Expression);

    public override void WriteUpdate(Migrator rules, TextWriter writer)
    {
        if (Difference == SchemaPatchDifference.Invalid)
        {
            throw new InvalidOperationException($"TableDelta for {Expected.Identifier} is invalid");
        }

        if (Difference == SchemaPatchDifference.Create)
        {
            SchemaObject.WriteCreateStatement(rules, writer);
            return;
        }

        // Extra indexes
        foreach (var extra in Indexes.Extras) writer.WriteDropIndex(Expected, extra);

        // Different indexes
        foreach (var change in Indexes.Different) writer.WriteDropIndex(Expected, change.Actual);

        // A computed column is changed by dropping and re-adding it, and SQL Server refuses to drop a
        // column anything depends on. An index or foreign key that matches the model is in neither set
        // above, so nothing had dropped it and the migration failed on the dependency (weasel#638).
        var recreatedIndexes = matchedIndexesOnRecomputedColumns();
        var recreatedForeignKeys = matchedForeignKeysOnRecomputedColumns();

        foreach (var index in recreatedIndexes) writer.WriteDropIndex(Expected, index);
        foreach (var foreignKey in recreatedForeignKeys) foreignKey.WriteDropStatement(Expected, writer);

        var primaryKeyDroppedBeforeColumnChanges = requiresPrimaryKeyDropBeforeUpdate();
        if (primaryKeyDroppedBeforeColumnChanges)
        {
            writer.WriteLine($"alter table {Expected.Identifier} drop constraint {SchemaUtils.QuoteName(Actual!.PrimaryKeyName)};");
        }

        // Missing columns
        foreach (var column in Columns.Missing) writer.WriteLine(column.AddColumnSql(Expected));


        // Different columns
        foreach (var change1 in Columns.Different)
        {
            if (change1.Expected.ComputedDefinitionChanged(change1.Actual))
            {
                // a computed column definition can't be altered in place; the
                // data is derived, so drop + re-add is lossless
                writer.WriteLine(change1.Expected.DropColumnSql(Expected));
                writer.WriteLine(change1.Expected.AddColumnSql(Expected));
            }
            else if (change1.Expected.Equals(change1.Actual))
            {
                // same name/type — the difference is default/nullability drift
                change1.Expected.WriteDriftCorrections(Expected, change1.Actual, writer);
            }
            else
            {
                writer.WriteLine(change1.Expected.AlterColumnTypeSql(Expected, change1.Actual));
            }
        }

        writeForeignKeyUpdates(writer);
        writeCheckConstraintUpdates(writer);

        // Missing indexes
        foreach (var indexDefinition in Indexes.Missing) indexDefinition.WriteCreateStatement(Expected, writer);

        // Different indexes
        foreach (var change in Indexes.Different) change.Expected.WriteCreateStatement(Expected, writer);

        // ...and back, in the same order the table declares them
        foreach (var foreignKey in recreatedForeignKeys) foreignKey.WriteAddStatement(Expected, writer);
        foreach (var index in recreatedIndexes) index.WriteCreateStatement(Expected, writer);


        // Extra columns
        foreach (var column in Columns.Extras) writer.WriteLine(column.DropColumnSql(Expected));

        // Additive RANGE partition boundaries -> ALTER PARTITION FUNCTION ... SPLIT RANGE
        if (PartitioningDifference == SchemaPatchDifference.Update
            && Expected.SqlServerPartitioning is Partitioning.ISplittablePartitioning rangePartitioning
            && Actual?.PartitionInfo != null)
        {
            rangePartitioning.WriteSplitStatements(writer, Expected, Actual.PartitionInfo);
        }

        switch (PrimaryKeyDifference)
        {
            case SchemaPatchDifference.Invalid:
            case SchemaPatchDifference.Update:
                if (!primaryKeyDroppedBeforeColumnChanges)
                {
                    writer.WriteLine($"alter table {Expected.Identifier} drop constraint {SchemaUtils.QuoteName(Actual!.PrimaryKeyName)};");
                }

                writer.WriteLine($"alter table {Expected.Identifier} add {Expected.PrimaryKeyDeclaration()};");
                break;

            case SchemaPatchDifference.Create:
                writer.WriteLine($"alter table {Expected.Identifier} add {Expected.PrimaryKeyDeclaration()};");
                break;

            case SchemaPatchDifference.None:
                if (primaryKeyDroppedBeforeColumnChanges)
                {
                    writer.WriteLine($"alter table {Expected.Identifier} add {Expected.PrimaryKeyDeclaration()};");
                }
                break;
        }
    }

    /// <summary>
    ///     The columns this update changes by dropping and re-adding them, because their computed
    ///     definition changed.
    /// </summary>
    private HashSet<string> recomputedColumnNames()
    {
        return Columns.Different
            .Where(x => x.Expected.ComputedDefinitionChanged(x.Actual))
            .Select(x => x.Expected.Name)
            .ToHashSet(StringComparer.OrdinalIgnoreCase);
    }

    /// <summary>
    ///     Indexes that match the model and sit on a column being recomputed, so they have to come down
    ///     before the column is dropped and go back afterwards.
    /// </summary>
    /// <remarks>
    ///     An index that is Extra or Different is already dropped by the passes above and, if
    ///     Different, recreated by them, so taking it from Matched alone is what avoids dropping or
    ///     creating anything twice. Derived from the model rather than from a catalog round trip: the
    ///     delta already knows both sides' column names.
    /// </remarks>
    private IReadOnlyList<IndexDefinition> matchedIndexesOnRecomputedColumns()
    {
        var recomputed = recomputedColumnNames();
        if (recomputed.Count == 0)
        {
            return [];
        }

        return Indexes.Matched
            .Where(x => x.Columns.Concat(x.IncludedColumns).Any(c => recomputed.Contains(SchemaUtils.Unbracket(c))))
            .ToList();
    }

    /// <summary>
    ///     Foreign keys that match the model and sit on a column being recomputed. Nothing dropped these
    ///     at any point before the column change, so the <c>DROP COLUMN</c> was refused.
    /// </summary>
    private IReadOnlyList<ForeignKey> matchedForeignKeysOnRecomputedColumns()
    {
        var recomputed = recomputedColumnNames();
        if (recomputed.Count == 0)
        {
            return [];
        }

        return ForeignKeys.Matched
            .Where(x => x.ColumnNames.Any(c => recomputed.Contains(SchemaUtils.Unbracket(c))))
            .ToList();
    }

    private void writeForeignKeyUpdates(TextWriter writer)
    {
        foreach (var foreignKey in ForeignKeys.Missing.Where(x => !_deferredForeignKeys.Contains(x.Name)))
            foreignKey.WriteAddStatement(Expected, writer);

        foreach (var foreignKey in ForeignKeys.Extras) foreignKey.WriteDropStatement(Expected, writer);

        foreach (var change in ForeignKeys.Different)
        {
            change.Actual.WriteDropStatement(Expected, writer);
            change.Expected.WriteAddStatement(Expected, writer);
        }
    }

    private void writeCheckConstraintUpdates(TextWriter writer)
    {
        // Extras never appear here — unknown actual checks are filtered out of
        // the comparison entirely (see the delta construction)
        foreach (var check in CheckConstraints.Missing)
            writer.WriteLine($"alter table {Expected.Identifier} add {Table.CheckConstraintDeclaration(check)};");

        foreach (var change in CheckConstraints.Different)
        {
            writer.WriteLine($"alter table {Expected.Identifier} drop constraint {SchemaUtils.BracketName(change.Actual.Name)};");
            writer.WriteLine($"alter table {Expected.Identifier} add {Table.CheckConstraintDeclaration(change.Expected)};");
        }
    }

    public override void WriteRollback(Migrator rules, TextWriter writer)
    {
        if (Actual == null)
        {
            Expected.WriteDropStatement(rules, writer);
            return;
        }

        foreach (var foreignKey in ForeignKeys.Missing) foreignKey.WriteDropStatement(Expected, writer);

        foreach (var change in ForeignKeys.Different) change.Expected.WriteDropStatement(Expected, writer);

        var primaryKeyDroppedBeforeColumnChanges = requiresPrimaryKeyDropBeforeRollback();
        if (primaryKeyDroppedBeforeColumnChanges)
        {
            writer.WriteLine(
                $"alter table {Expected.Identifier} drop constraint if exists {SchemaUtils.QuoteName(Expected.PrimaryKeyName)};");
        }

        // Extra columns
        foreach (var column in Columns.Extras) writer.WriteLine(column.AddColumnSql(Expected));

        // Different columns
        foreach (var change1 in Columns.Different)
        {
            if (change1.Expected.ComputedDefinitionChanged(change1.Actual))
            {
                // restore the actual column definition by drop + re-add
                writer.WriteLine(change1.Expected.DropColumnSql(Expected));
                writer.WriteLine(change1.Actual.AddColumnSql(Expected));
            }
            else if (change1.Expected.Equals(change1.Actual))
            {
                change1.Actual.WriteDriftCorrections(Expected, change1.Expected, writer);
            }
            else
            {
                writer.WriteLine(change1.Actual.AlterColumnTypeSql(Actual, change1.Expected));
            }
        }

        foreach (var change in ForeignKeys.Different) change.Actual.WriteAddStatement(Expected, writer);

        rollbackIndexes(writer);

        // Missing columns
        foreach (var column in Columns.Missing) writer.WriteLine(column.DropColumnSql(Expected));

        foreach (var foreignKey in ForeignKeys.Extras) foreignKey.WriteAddStatement(Expected, writer);

        // Roll an additive partition split back out -> ALTER PARTITION FUNCTION ... MERGE RANGE
        if (PartitioningDifference == SchemaPatchDifference.Update
            && Expected.SqlServerPartitioning is Partitioning.ISplittablePartitioning rangePartitioning
            && Actual?.PartitionInfo != null)
        {
            rangePartitioning.WriteMergeStatements(writer, Expected, Actual.PartitionInfo);
        }

        switch (PrimaryKeyDifference)
        {
            case SchemaPatchDifference.Invalid:
            case SchemaPatchDifference.Update:
                if (!primaryKeyDroppedBeforeColumnChanges)
                {
                    writer.WriteLine($"alter table {Expected.Identifier} drop constraint if exists {SchemaUtils.QuoteName(Expected.PrimaryKeyName)};");
                }

                writer.WriteLine($"alter table {Expected.Identifier} add {Actual!.PrimaryKeyDeclaration()};");
                break;

            case SchemaPatchDifference.Create:
                if (!primaryKeyDroppedBeforeColumnChanges)
                {
                    writer.WriteLine($"alter table {Expected.Identifier} drop constraint if exists {SchemaUtils.QuoteName(Expected.PrimaryKeyName)};");
                }
                break;

            case SchemaPatchDifference.None:
                if (primaryKeyDroppedBeforeColumnChanges)
                {
                    writer.WriteLine($"alter table {Expected.Identifier} add {Actual!.PrimaryKeyDeclaration()};");
                }
                break;
        }
    }

    private void rollbackIndexes(TextWriter writer)
    {
        // Missing indexes
        foreach (var indexDefinition in Indexes.Missing) writer.WriteDropIndex(Expected, indexDefinition);

        // Extra indexes
        foreach (var extra in Indexes.Extras) extra.WriteCreateStatement(Actual!, writer);

        // Different indexes
        foreach (var change in Indexes.Different)
        {
            writer.WriteDropIndex(Actual!, change.Expected);
            change.Actual.WriteCreateStatement(Actual!, writer);
        }
    }

    /// <summary>
    ///     The actual objects this delta is allowed to consider for removal. On an
    ///     <see cref="ITable.AddOnlyMigrations" /> table the actual side is narrowed to what the
    ///     model declares, so an undeclared object never becomes an Extra and is never dropped
    ///     (weasel#629). Otherwise everything the catalog reported is in play, exactly as before.
    /// </summary>
    private IEnumerable<T> droppable<T>(Table expected, IEnumerable<T> expectedItems,
        IEnumerable<T> actualItems, string kind) where T : INamed
        => expected.AddOnlyMigrations
            ? AddOnlyMigration.DeclaredOnly(expectedItems, actualItems, kind, _withheldDrops)
            : actualItems;

    private SchemaPatchDifference determinePatchDifference()
    {
        InvalidReason = null;

        if (Actual!.PartitionStrategy != Expected.PartitionStrategy)
        {
            InvalidReason = "the table's partition strategy cannot be changed in place";
            return SchemaPatchDifference.Invalid;
        }

        if (!Actual.PartitionExpressions.SequenceEqual(Expected.PartitionExpressions))
        {
            InvalidReason = "the table's partition expressions cannot be changed in place";
            return SchemaPatchDifference.Invalid;
        }


        if (!HasChanges())
        {
            return SchemaPatchDifference.None;
        }


        // If there are any columns that are different and at least one cannot
        // automatically generate an `ALTER TABLE` statement, the patch is invalid
        var unalterable = Columns.Different.Where(x => !x.Expected.CanAlter(x.Actual)).ToArray();
        if (unalterable.Any())
        {
            InvalidReason =
                $"{unalterable.Select(x => $"column '{x.Expected.Name}'").Join(" and ")} cannot be altered in place";
            return SchemaPatchDifference.Invalid;
        }

        // If there are any missing columns and at least one
        // cannot generate an `ALTER TABLE * ADD COLUMN` statement
        var unaddable = Columns.Missing.Where(x => !x.CanAdd()).ToArray();
        if (unaddable.Any())
        {
            InvalidReason =
                $"{unaddable.Select(x => $"column '{x.Name}'").Join(" and ")} cannot be added to an existing table";
            return SchemaPatchDifference.Invalid;
        }

        var differences = new (SchemaPatchDifference Difference, string Reason)[]
        {
            (Columns.Difference(), "a column change cannot be applied incrementally"),
            (ForeignKeys.Difference(), "a foreign key change cannot be applied incrementally"),
            (Indexes.Difference(), "an index change cannot be applied incrementally"),
            (CheckConstraints.Difference(), "a check constraint change cannot be applied incrementally"),
            (PrimaryKeyDifference, "the primary key cannot be changed in place"),
            (PartitioningDifference, "the table's partitioning cannot be changed in place")
        };

        var worst = differences.MinBy(x => x.Difference);
        if (worst.Difference == SchemaPatchDifference.Invalid)
        {
            InvalidReason = worst.Reason;
        }

        return worst.Difference;
    }

    private bool requiresPrimaryKeyDropBeforeUpdate()
    {
        return Actual != null && Actual.PrimaryKeyColumns.Any() &&
               Columns.Different.Any(change => Actual.PrimaryKeyColumns.Contains(change.Actual.Name));
    }

    private bool requiresPrimaryKeyDropBeforeRollback()
    {
        return Expected.PrimaryKeyColumns.Any() &&
               Columns.Different.Any(change => Expected.PrimaryKeyColumns.Contains(change.Expected.Name));
    }

    public bool HasChanges()
    {
        return Columns.HasChanges() || Indexes.HasChanges() || ForeignKeys.HasChanges() ||
               CheckConstraints.HasChanges() ||
               PrimaryKeyDifference != SchemaPatchDifference.None ||
               PartitioningDifference != SchemaPatchDifference.None;
    }

    public override string ToString()
    {
        return $"TableDelta for {Expected.Identifier}";
    }
}
