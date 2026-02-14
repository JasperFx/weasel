using JasperFx.Core;
using Weasel.Core;

namespace Weasel.Oracle.Tables;

public class TableDelta: SchemaObjectDelta<Table>
{
    public TableDelta(Table expected, Table? actual): base(expected, actual)
    {
    }

    public ItemDelta<TableColumn> Columns { get; private set; } = null!;
    public ItemDelta<IndexDefinition> Indexes { get; private set; } = null!;

    public ItemDelta<ForeignKey> ForeignKeys { get; private set; } = null!;

    public SchemaPatchDifference PrimaryKeyDifference { get; private set; }

    protected override SchemaPatchDifference compare(Table expected, Table? actual)
    {
        if (actual == null)
        {
            // Initialize deltas with empty actuals so HasChanges() works
            Columns = new ItemDelta<TableColumn>(expected.Columns, Array.Empty<TableColumn>());
            Indexes = new ItemDelta<IndexDefinition>(expected.Indexes, Array.Empty<IndexDefinition>(),
                (e, a) => e.Matches(a, Expected));
            ForeignKeys = new ItemDelta<ForeignKey>(expected.ForeignKeys, Array.Empty<ForeignKey>());
            return SchemaPatchDifference.Create;
        }

        Columns = new ItemDelta<TableColumn>(expected.Columns, actual.Columns);
        Indexes = new ItemDelta<IndexDefinition>(expected.Indexes, actual.Indexes,
            (e, a) => e.Matches(a, Expected));

        ForeignKeys = new ItemDelta<ForeignKey>(expected.ForeignKeys, actual.ForeignKeys);

        PrimaryKeyDifference = SchemaPatchDifference.None;

        // Only compare PKs if both sides have actual PK columns defined
        // If the actual table has no PK columns detected (possibly due to Oracle permission issues),
        // we skip PK comparison to avoid false positives
        var expectedHasPk = expected.PrimaryKeyColumns.Any();
        var actualHasPk = actual.PrimaryKeyColumns.Any();

        if (expectedHasPk && !actualHasPk)
        {
            // Expected has PK but actual doesn't - need to create
            PrimaryKeyDifference = SchemaPatchDifference.Create;
        }
        else if (!expectedHasPk && actualHasPk)
        {
            // Actual has PK but expected doesn't - this would require dropping
            PrimaryKeyDifference = SchemaPatchDifference.Update;
        }
        else if (expectedHasPk && actualHasPk)
        {
            // Both have PKs - compare the columns
            if (!expected.PrimaryKeyColumns.SequenceEqual(actual.PrimaryKeyColumns, StringComparer.OrdinalIgnoreCase))
            {
                PrimaryKeyDifference = SchemaPatchDifference.Update;
            }
        }
        // If neither has PK, leave as None

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
        foreach (var extra in Indexes.Extras)
        {
            writer.WriteDropIndex(Expected, extra);
            writer.WriteLine("/");
        }

        // Different indexes
        foreach (var change in Indexes.Different)
        {
            writer.WriteDropIndex(Expected, change.Actual);
            writer.WriteLine("/");
        }

        // Missing columns
        foreach (var column in Columns.Missing)
        {
            writer.WriteLine(column.AddColumnSql(Expected));
            writer.WriteLine("/");
        }

        // Different columns
        foreach (var change1 in Columns.Different)
        {
            writer.WriteLine(change1.Expected.AlterColumnTypeSql(Expected, change1.Actual));
            writer.WriteLine("/");
        }

        writeForeignKeyUpdates(writer);

        // Missing indexes
        foreach (var indexDefinition in Indexes.Missing)
        {
            writer.WriteLine(indexDefinition.ToDDL(Expected));
            writer.WriteLine("/");
        }

        // Different indexes
        foreach (var change in Indexes.Different)
        {
            writer.WriteLine(change.Expected.ToDDL(Expected));
            writer.WriteLine("/");
        }

        // Extra columns
        foreach (var column in Columns.Extras)
        {
            writer.WriteLine(column.DropColumnSql(Expected));
            writer.WriteLine("/");
        }

        switch (PrimaryKeyDifference)
        {
            case SchemaPatchDifference.Invalid:
            case SchemaPatchDifference.Update:
                // Only drop the constraint if Actual has a valid PK name
                if (Actual?.PrimaryKeyName != null && Actual.PrimaryKeyColumns.Any())
                {
                    writer.WriteLine($"ALTER TABLE {Expected.Identifier} DROP CONSTRAINT {Actual.PrimaryKeyName}");
                    writer.WriteLine("/");
                }
                writer.WriteLine($"ALTER TABLE {Expected.Identifier} ADD {Expected.PrimaryKeyDeclaration()}");
                writer.WriteLine("/");
                break;

            case SchemaPatchDifference.Create:
                writer.WriteLine($"ALTER TABLE {Expected.Identifier} ADD {Expected.PrimaryKeyDeclaration()}");
                writer.WriteLine("/");
                break;
        }
    }

    private void writeForeignKeyUpdates(TextWriter writer)
    {
        foreach (var foreignKey in ForeignKeys.Missing)
        {
            foreignKey.WriteAddStatement(Expected, writer);
            writer.WriteLine("/");
        }

        foreach (var foreignKey in ForeignKeys.Extras)
        {
            foreignKey.WriteDropStatement(Expected, writer);
            writer.WriteLine("/");
        }

        foreach (var change in ForeignKeys.Different)
        {
            change.Actual.WriteDropStatement(Expected, writer);
            writer.WriteLine("/");
            change.Expected.WriteAddStatement(Expected, writer);
            writer.WriteLine("/");
        }
    }

    public override void WriteRollback(Migrator rules, TextWriter writer)
    {
        if (Actual == null)
        {
            Expected.WriteDropStatement(rules, writer);
            return;
        }

        foreach (var foreignKey in ForeignKeys.Missing)
        {
            foreignKey.WriteDropStatement(Expected, writer);
            writer.WriteLine("/");
        }

        foreach (var change in ForeignKeys.Different)
        {
            change.Expected.WriteDropStatement(Expected, writer);
            writer.WriteLine("/");
        }

        // Extra columns
        foreach (var column in Columns.Extras)
        {
            writer.WriteLine(column.AddColumnSql(Expected));
            writer.WriteLine("/");
        }

        // Different columns
        foreach (var change1 in Columns.Different)
        {
            writer.WriteLine(change1.Actual.AlterColumnTypeSql(Actual, change1.Expected));
            writer.WriteLine("/");
        }

        foreach (var change in ForeignKeys.Different)
        {
            change.Actual.WriteAddStatement(Expected, writer);
            writer.WriteLine("/");
        }

        rollbackIndexes(writer);

        // Missing columns
        foreach (var column in Columns.Missing)
        {
            writer.WriteLine(column.DropColumnSql(Expected));
            writer.WriteLine("/");
        }

        foreach (var foreignKey in ForeignKeys.Extras)
        {
            foreignKey.WriteAddStatement(Expected, writer);
            writer.WriteLine("/");
        }

        switch (PrimaryKeyDifference)
        {
            case SchemaPatchDifference.Invalid:
            case SchemaPatchDifference.Update:
                // Only drop if Expected has PK columns
                if (Expected.PrimaryKeyColumns.Any())
                {
                    writer.WriteLine(
                        $"ALTER TABLE {Expected.Identifier} DROP CONSTRAINT {Expected.PrimaryKeyName}");
                    writer.WriteLine("/");
                }
                // Only add if Actual has PK columns
                if (Actual?.PrimaryKeyColumns.Any() == true)
                {
                    writer.WriteLine($"ALTER TABLE {Expected.Identifier} ADD {Actual.PrimaryKeyDeclaration()}");
                    writer.WriteLine("/");
                }
                break;

            case SchemaPatchDifference.Create:
                // Only drop if Expected has PK columns
                if (Expected.PrimaryKeyColumns.Any())
                {
                    writer.WriteLine(
                        $"ALTER TABLE {Expected.Identifier} DROP CONSTRAINT {Expected.PrimaryKeyName}");
                    writer.WriteLine("/");
                }
                break;
        }
    }

    private void rollbackIndexes(TextWriter writer)
    {
        // Missing indexes
        foreach (var indexDefinition in Indexes.Missing)
        {
            writer.WriteDropIndex(Expected, indexDefinition);
            writer.WriteLine("/");
        }

        // Extra indexes
        foreach (var extra in Indexes.Extras)
        {
            writer.WriteLine(extra.ToDDL(Actual!));
            writer.WriteLine("/");
        }

        // Different indexes
        foreach (var change in Indexes.Different)
        {
            writer.WriteDropIndex(Actual!, change.Expected);
            writer.WriteLine("/");
            writer.WriteLine(change.Actual.ToDDL(Actual!));
            writer.WriteLine("/");
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
            Columns.Difference(), ForeignKeys.Difference(), Indexes.Difference(), PrimaryKeyDifference
        };

        // Use Min() to get the most severe required action:
        // Invalid (0) > Update (1) > Create (2) > None (3)
        return differences.Min();
    }

    public bool HasChanges()
    {
        return Columns.HasChanges() || Indexes.HasChanges() || ForeignKeys.HasChanges() ||
               PrimaryKeyDifference != SchemaPatchDifference.None;
    }

    public override string ToString()
    {
        return $"TableDelta for {Expected.Identifier}";
    }
}
