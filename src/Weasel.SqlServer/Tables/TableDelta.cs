using JasperFx.Core;
using Weasel.Core;

namespace Weasel.SqlServer.Tables;

public class TableDelta: SchemaObjectDelta<Table>, ISchemaObjectDeltaWithDeferrableForeignKeys
{
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

        Columns = new ItemDelta<TableColumn>(expected.Columns, actual.Columns,
            (e, a) => e.MatchesForDelta(a, expected.DetectColumnDrift));
        // IgnoreIndex is Weasel.Core API and is honoured by the PostgreSQL and SQLite twins; without
        // this SQL Server put an ignored index in Extras and WriteUpdate dropped it.
        Indexes = new ItemDelta<IndexDefinition>(
            expected.Indexes.Where(x => !expected.HasIgnoredIndex(x.Name)),
            actual.Indexes.Where(x => !expected.HasIgnoredIndex(x.Name)),
            (e, a) => e.Matches(a, Expected));

        ForeignKeys = new ItemDelta<ForeignKey>(expected.ForeignKeys, actual.ForeignKeys);

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
        => TableCheckConstraint.Canonicalize(expected.Expression)
           == TableCheckConstraint.Canonicalize(actual.Expression);

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
        foreach (var indexDefinition in Indexes.Missing) writer.WriteLine(indexDefinition.ToDDL(Expected));

        // Different indexes
        foreach (var change in Indexes.Different) writer.WriteLine(change.Expected.ToDDL(Expected));


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
        foreach (var extra in Indexes.Extras) writer.WriteLine(extra.ToDDL(Actual!));

        // Different indexes
        foreach (var change in Indexes.Different)
        {
            writer.WriteDropIndex(Actual!, change.Expected);
            writer.WriteLine(change.Actual.ToDDL(Actual!));
        }
    }

    private SchemaPatchDifference determinePatchDifference()
    {
        if (Actual!.PartitionStrategy != Expected.PartitionStrategy)
        {
            return SchemaPatchDifference.Invalid;
        }

        if (!Actual.PartitionExpressions.SequenceEqual(Expected.PartitionExpressions))
        {
            return SchemaPatchDifference.Invalid;
        }


        if (!HasChanges())
        {
            return SchemaPatchDifference.None;
        }


        // If there are any columns that are different and at least one cannot
        // automatically generate an `ALTER TABLE` statement, the patch is invalid
        if (Columns.Different.Any(x => !x.Expected.CanAlter(x.Actual)))
        {
            return SchemaPatchDifference.Invalid;
        }

        // If there are any missing columns and at least one
        // cannot generate an `ALTER TABLE * ADD COLUMN` statement
        if (Columns.Missing.Any(x => !x.CanAdd()))
        {
            return SchemaPatchDifference.Invalid;
        }

        var differences = new[]
        {
            Columns.Difference(), ForeignKeys.Difference(), Indexes.Difference(), CheckConstraints.Difference(),
            PrimaryKeyDifference, PartitioningDifference
        };

        return differences.Min();
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
