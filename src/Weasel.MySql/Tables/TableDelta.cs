using JasperFx.Core;
using Weasel.Core;

namespace Weasel.MySql.Tables;

/// <summary>
///     MySQL table delta. Brought into the standard <see cref="SchemaObjectDelta{T}" />
///     shape in 9.0 so it composes uniformly with the other providers' deltas (PG,
///     SQL Server, Oracle, SQLite) — same constructor signature, same Expected/Actual
///     properties from the base, same protected <see cref="compare" /> hook that
///     populates the per-item deltas as a side-effect. The override surface is just
///     the MySQL-specific update / rollback DDL.
/// </summary>
public class TableDelta: SchemaObjectDelta<Table>
{
    public TableDelta(Table expected, Table? actual): base(expected, actual)
    {
    }

    public ItemDelta<TableColumn>? Columns { get; private set; }
    public ItemDelta<IndexDefinition>? Indexes { get; private set; }
    public ItemDelta<ForeignKey>? ForeignKeys { get; private set; }

    public SchemaPatchDifference PrimaryKeyDifference { get; private set; } = SchemaPatchDifference.None;

    protected override SchemaPatchDifference compare(Table expected, Table? actual)
    {
        if (actual == null)
        {
            return SchemaPatchDifference.Create;
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

        // Partition strategy can't be altered in place — flag as needing manual intervention
        if (expected.PartitionStrategy != actual.PartitionStrategy)
        {
            return SchemaPatchDifference.Invalid;
        }

        return HasChanges() ? SchemaPatchDifference.Update : SchemaPatchDifference.None;
    }

    /// <summary>
    ///     True when at least one column, index, foreign key or the primary key
    ///     differs between Expected and Actual. Public because callers (e.g.
    ///     migration runners) want to query "anything to do?" without unpacking
    ///     the SchemaPatchDifference enum.
    /// </summary>
    public bool HasChanges()
    {
        if (Actual == null) return true;

        if (Columns?.HasChanges() == true) return true;
        if (Indexes?.HasChanges() == true) return true;
        if (ForeignKeys?.HasChanges() == true) return true;
        if (PrimaryKeyDifference != SchemaPatchDifference.None) return true;

        return false;
    }

    public override void WriteUpdate(Migrator migrator, TextWriter writer)
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

    public override void WriteRollback(Migrator migrator, TextWriter writer)
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

    /// <summary>
    ///     The base default would throw NRE if <see cref="SchemaObjectDelta{T}.Actual" />
    ///     is null (which it is for a Create delta). MySQL has historically been
    ///     tolerant — a Create delta has no "previous state" to restore, so this
    ///     is a no-op rather than a throw.
    /// </summary>
    public override void WriteRestorationOfPreviousState(Migrator migrator, TextWriter writer)
    {
        Actual?.WriteCreateStatement(migrator, writer);
    }
}
