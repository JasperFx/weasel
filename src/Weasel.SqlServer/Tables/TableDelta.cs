using JasperFx.Core;
using Weasel.Core;

namespace Weasel.SqlServer.Tables;

public class TableDelta: SchemaObjectDelta<Table>
{
    public TableDelta(Table expected, Table? actual): base(expected, actual)
    {
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
        Indexes = new ItemDelta<IndexDefinition>(expected.Indexes, actual.Indexes,
            (e, a) => e.Matches(a, Expected));

        ForeignKeys = new ItemDelta<ForeignKey>(expected.ForeignKeys, actual.ForeignKeys);

        // Conservative check-constraint comparison: only the checks the expected
        // table declares participate, and actual constraints the expected table
        // doesn't know about are never treated as extras to drop.
        var relevantActualChecks = actual.CheckConstraints
            .Where(a => expected.CheckConstraints.Any(e => e.Name.Equals(a.Name, StringComparison.OrdinalIgnoreCase)));
        CheckConstraints = new ItemDelta<TableCheckConstraint>(expected.CheckConstraints, relevantActualChecks,
            (e, a) => e.Matches(a));

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
        else if (!expected.PrimaryKeyColumns.SequenceEqual(actual.PrimaryKeyColumns))
        {
            PrimaryKeyDifference = SchemaPatchDifference.Update;
        }

        // Declarative RANGE partitioning round-trip. Only RangePartitioning is migrated here; managed
        // strategies (e.g. ManagedTenantPartitions) own their boundaries at runtime and are left alone.
        PartitioningDifference = SchemaPatchDifference.None;
        if (expected.SqlServerPartitioning is Partitioning.RangePartitioning rangePartitioning)
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
            writer.WriteLine($"alter table {Expected.Identifier} drop constraint {Actual!.PrimaryKeyName};");
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
            && Expected.SqlServerPartitioning is Partitioning.RangePartitioning rangePartitioning
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
                    writer.WriteLine($"alter table {Expected.Identifier} drop constraint {Actual!.PrimaryKeyName};");
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
        foreach (var foreignKey in ForeignKeys.Missing) foreignKey.WriteAddStatement(Expected, writer);

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
            writer.WriteLine($"alter table {Expected.Identifier} drop constraint [{change.Actual.Name}];");
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
                $"alter table {Expected.Identifier} drop constraint if exists {Expected.PrimaryKeyName};");
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
            && Expected.SqlServerPartitioning is Partitioning.RangePartitioning rangePartitioning
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
                    writer.WriteLine($"alter table {Expected.Identifier} drop constraint if exists {Expected.PrimaryKeyName};");
                }

                writer.WriteLine($"alter table {Expected.Identifier} add {Actual!.PrimaryKeyDeclaration()};");
                break;

            case SchemaPatchDifference.Create:
                if (!primaryKeyDroppedBeforeColumnChanges)
                {
                    writer.WriteLine($"alter table {Expected.Identifier} drop constraint if exists {Expected.PrimaryKeyName};");
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
