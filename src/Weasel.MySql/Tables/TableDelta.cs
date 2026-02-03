using JasperFx.Core;
using Weasel.Core;

namespace Weasel.MySql.Tables;

public class TableDelta: ISchemaObjectDelta
{
    public TableDelta(Table expected, Table? actual)
    {
        Expected = expected;
        Actual = actual;

        if (actual == null)
        {
            Difference = SchemaPatchDifference.Create;
            return;
        }

        Columns = new ItemDelta<TableColumn>(
            expected.Columns,
            actual.Columns,
            (e, a) => e.IsEquivalentTo(a));

        Indexes = new ItemDelta<IndexDefinition>(
            expected.Indexes,
            actual.Indexes,
            (e, a) => e.Matches(a, expected));

        ForeignKeys = new ItemDelta<ForeignKey>(
            expected.ForeignKeys,
            actual.ForeignKeys,
            (e, a) => e.IsEquivalentTo(a));

        // Check primary key differences
        var expectedPks = expected.PrimaryKeyColumns.OrderBy(x => x).ToList();
        var actualPks = actual.PrimaryKeyColumns.OrderBy(x => x).ToList();

        if (!expectedPks.SequenceEqual(actualPks, StringComparer.OrdinalIgnoreCase))
        {
            PrimaryKeyDifference = SchemaPatchDifference.Update;
        }

        // Check partition differences
        if (expected.PartitionStrategy != actual.PartitionStrategy)
        {
            Difference = SchemaPatchDifference.Invalid;
            return;
        }

        // Determine overall difference
        if (HasChanges())
        {
            Difference = SchemaPatchDifference.Update;
        }
        else
        {
            Difference = SchemaPatchDifference.None;
        }
    }

    public Table Expected { get; }
    public Table? Actual { get; }

    public ItemDelta<TableColumn>? Columns { get; }
    public ItemDelta<IndexDefinition>? Indexes { get; }
    public ItemDelta<ForeignKey>? ForeignKeys { get; }

    public SchemaPatchDifference PrimaryKeyDifference { get; } = SchemaPatchDifference.None;

    public ISchemaObject SchemaObject => Expected;
    public SchemaPatchDifference Difference { get; }

    public bool HasChanges()
    {
        if (Actual == null) return true;

        if (Columns?.HasChanges() == true) return true;
        if (Indexes?.HasChanges() == true) return true;
        if (ForeignKeys?.HasChanges() == true) return true;
        if (PrimaryKeyDifference != SchemaPatchDifference.None) return true;

        return false;
    }

    public void WriteUpdate(Migrator migrator, TextWriter writer)
    {
        if (Difference == SchemaPatchDifference.Create)
        {
            Expected.WriteCreateStatement(migrator, writer);
            return;
        }

        if (Difference == SchemaPatchDifference.Invalid)
        {
            writer.WriteLine($"-- Cannot automatically migrate table {Expected.Identifier.QualifiedName}");
            writer.WriteLine($"-- Partition strategy has changed and requires manual intervention");
            return;
        }

        // Handle column changes
        if (Columns != null)
        {
            foreach (var column in Columns.Missing)
            {
                writer.WriteLine(
                    $"ALTER TABLE {Expected.Identifier.QualifiedName} ADD COLUMN {column.ToDeclaration()};");
            }

            foreach (var column in Columns.Extras)
            {
                writer.WriteLine($"ALTER TABLE {Expected.Identifier.QualifiedName} DROP COLUMN `{column.Name}`;");
            }

            foreach (var change in Columns.Different)
            {
                writer.WriteLine(
                    $"ALTER TABLE {Expected.Identifier.QualifiedName} MODIFY COLUMN {change.Expected.ToDeclaration()};");
            }
        }

        // Handle index changes - drop extras first, then add missing
        if (Indexes != null)
        {
            foreach (var index in Indexes.Extras)
            {
                writer.WriteLine($"DROP INDEX `{index.Name}` ON {Expected.Identifier.QualifiedName};");
            }

            foreach (var change in Indexes.Different)
            {
                writer.WriteLine($"DROP INDEX `{change.Actual.Name}` ON {Expected.Identifier.QualifiedName};");
                writer.WriteLine(change.Expected.ToDDL(Expected));
            }

            foreach (var index in Indexes.Missing)
            {
                writer.WriteLine(index.ToDDL(Expected));
            }
        }

        // Handle foreign key changes - drop extras first, then add missing
        if (ForeignKeys != null)
        {
            foreach (var fk in ForeignKeys.Extras)
            {
                writer.WriteLine(
                    $"ALTER TABLE {Expected.Identifier.QualifiedName} DROP FOREIGN KEY `{fk.Name}`;");
            }

            foreach (var change in ForeignKeys.Different)
            {
                writer.WriteLine(
                    $"ALTER TABLE {Expected.Identifier.QualifiedName} DROP FOREIGN KEY `{change.Actual.Name}`;");
                writer.WriteLine(change.Expected.ToDDL(Expected));
            }

            foreach (var fk in ForeignKeys.Missing)
            {
                writer.WriteLine(fk.ToDDL(Expected));
            }
        }

        // Handle primary key changes
        if (PrimaryKeyDifference == SchemaPatchDifference.Update)
        {
            if (Actual?.PrimaryKeyColumns.Any() == true)
            {
                writer.WriteLine($"ALTER TABLE {Expected.Identifier.QualifiedName} DROP PRIMARY KEY;");
            }

            if (Expected.PrimaryKeyColumns.Any())
            {
                var pkColumns = Expected.PrimaryKeyColumns.Select(c => $"`{c}`").Join(", ");
                writer.WriteLine($"ALTER TABLE {Expected.Identifier.QualifiedName} ADD PRIMARY KEY ({pkColumns});");
            }
        }
    }

    public void WriteRollback(Migrator migrator, TextWriter writer)
    {
        if (Actual == null)
        {
            Expected.WriteDropStatement(migrator, writer);
            return;
        }

        // Rollback column changes
        if (Columns != null)
        {
            foreach (var column in Columns.Missing)
            {
                writer.WriteLine($"ALTER TABLE {Expected.Identifier.QualifiedName} DROP COLUMN `{column.Name}`;");
            }

            foreach (var column in Columns.Extras)
            {
                writer.WriteLine(
                    $"ALTER TABLE {Expected.Identifier.QualifiedName} ADD COLUMN {column.ToDeclaration()};");
            }

            foreach (var change in Columns.Different)
            {
                writer.WriteLine(
                    $"ALTER TABLE {Expected.Identifier.QualifiedName} MODIFY COLUMN {change.Actual.ToDeclaration()};");
            }
        }

        // Rollback index changes
        if (Indexes != null)
        {
            foreach (var index in Indexes.Missing)
            {
                writer.WriteLine($"DROP INDEX `{index.Name}` ON {Expected.Identifier.QualifiedName};");
            }

            foreach (var index in Indexes.Extras)
            {
                writer.WriteLine(index.ToDDL(Expected));
            }
        }

        // Rollback foreign key changes
        if (ForeignKeys != null)
        {
            foreach (var fk in ForeignKeys.Missing)
            {
                writer.WriteLine(
                    $"ALTER TABLE {Expected.Identifier.QualifiedName} DROP FOREIGN KEY `{fk.Name}`;");
            }

            foreach (var fk in ForeignKeys.Extras)
            {
                writer.WriteLine(fk.ToDDL(Expected));
            }
        }
    }

    public void WriteRestorationOfPreviousState(Migrator migrator, TextWriter writer)
    {
        Actual?.WriteCreateStatement(migrator, writer);
    }
}
