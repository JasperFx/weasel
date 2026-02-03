using JasperFx.Core;
using Weasel.Core;

namespace Weasel.Sqlite.Tables;

/// <summary>
/// Represents the differences between an expected table schema and the actual table in the database.
/// SQLite has limited ALTER TABLE support, so many changes require table recreation.
/// </summary>
public class TableDelta: SchemaObjectDelta<Table>
{
    public TableDelta(Table expected, Table? actual): base(expected, actual)
    {
    }

    public ItemDelta<TableColumn> Columns { get; internal set; } = null!;
    public ItemDelta<IndexDefinition> Indexes { get; internal set; } = null!;
    public ItemDelta<ForeignKey> ForeignKeys { get; internal set; } = null!;

    public SchemaPatchDifference PrimaryKeyDifference { get; private set; }
    public bool RequiresTableRecreation { get; private set; }

    protected override SchemaPatchDifference compare(Table expected, Table? actual)
    {
        if (actual == null)
        {
            return SchemaPatchDifference.Create;
        }

        Columns = new ItemDelta<TableColumn>(expected.Columns, actual.Columns);
        Indexes = new ItemDelta<IndexDefinition>(
            expected.Indexes.Where(x => !expected.IgnoredIndexes.Contains(x.Name)),
            actual.Indexes.Where(x => !expected.IgnoredIndexes.Contains(x.Name)));

        ForeignKeys = new ItemDelta<ForeignKey>(expected.ForeignKeys, actual.ForeignKeys);

        // Check primary key differences
        PrimaryKeyDifference = SchemaPatchDifference.None;
        if (expected.PrimaryKeyColumns.Any() != actual.PrimaryKeyColumns.Any())
        {
            PrimaryKeyDifference = SchemaPatchDifference.Update;
        }
        else if (expected.PrimaryKeyColumns.Any() &&
                 !expected.PrimaryKeyColumns.SequenceEqual(actual.PrimaryKeyColumns, StringComparer.OrdinalIgnoreCase))
        {
            PrimaryKeyDifference = SchemaPatchDifference.Update;
        }

        return determinePatchDifference();
    }

    private SchemaPatchDifference determinePatchDifference()
    {
        // Check if table recreation is required due to SQLite limitations
        RequiresTableRecreation = requiresTableRecreation();

        if (RequiresTableRecreation)
        {
            // Table recreation is effectively an Invalid state that requires drop+create
            return SchemaPatchDifference.Invalid;
        }

        if (Columns.Missing.Any() || Columns.Extras.Any() || Columns.Different.Any())
        {
            return SchemaPatchDifference.Update;
        }

        if (Indexes.Missing.Any() || Indexes.Extras.Any() || Indexes.Different.Any())
        {
            return SchemaPatchDifference.Update;
        }

        if (ForeignKeys.Missing.Any() || ForeignKeys.Extras.Any() || ForeignKeys.Different.Any())
        {
            // Foreign keys require table recreation in SQLite
            return SchemaPatchDifference.Invalid;
        }

        if (PrimaryKeyDifference != SchemaPatchDifference.None)
        {
            return SchemaPatchDifference.Invalid;
        }

        return SchemaPatchDifference.None;
    }

    private bool requiresTableRecreation()
    {
        // SQLite requires table recreation for:
        // 1. Any column type changes
        // 2. Adding/removing foreign keys
        // 3. Changing primary key
        // 4. Dropping columns that are part of constraints

        if (Columns.Different.Any())
        {
            // Column type or constraint changes require recreation
            return true;
        }

        if (ForeignKeys.Missing.Any() || ForeignKeys.Extras.Any() || ForeignKeys.Different.Any())
        {
            // FK changes require recreation
            return true;
        }

        if (PrimaryKeyDifference != SchemaPatchDifference.None)
        {
            // PK changes require recreation
            return true;
        }

        // Check if we need to drop columns that might be referenced
        if (Columns.Extras.Any())
        {
            // Dropping columns is supported in SQLite 3.35+, but we should check dependencies
            // For now, we'll allow simple column drops
            // TODO: Check if dropped columns are referenced by indexes or FKs
        }

        return false;
    }

    public override void WriteUpdate(Migrator rules, TextWriter writer)
    {
        if (Difference == SchemaPatchDifference.Invalid || RequiresTableRecreation)
        {
            // SQLite limitation: Table recreation required
            writeTableRecreation(rules, writer);
            return;
        }

        if (Difference == SchemaPatchDifference.Create)
        {
            SchemaObject.WriteCreateStatement(rules, writer);
            return;
        }

        // Drop extra indexes first (these are separate objects)
        foreach (var extra in Indexes.Extras)
        {
            writer.WriteDropIndex(Expected, extra);
        }

        // Drop different indexes
        foreach (var change in Indexes.Different)
        {
            writer.WriteDropIndex(Expected, change.Actual);
        }

        // Add missing columns (SQLite supports ADD COLUMN with restrictions)
        foreach (var column in Columns.Missing)
        {
            if (column.CanAdd())
            {
                writer.WriteLine(column.AddColumnSql(Expected));
            }
            else
            {
                throw new InvalidOperationException(
                    $"Cannot add column '{column.Name}' to table '{Expected.Identifier}' " +
                    "without a default value or allowing NULL. Table recreation required.");
            }
        }

        // Drop extra columns (SQLite 3.35+)
        foreach (var column in Columns.Extras)
        {
            writer.WriteLine(column.DropColumnSql(Expected));
        }

        // Create missing indexes
        foreach (var index in Indexes.Missing)
        {
            writer.WriteLine(index.ToDDL(Expected));
        }

        // Recreate different indexes
        foreach (var change in Indexes.Different)
        {
            writer.WriteLine(change.Expected.ToDDL(Expected));
        }
    }

    private void writeTableRecreation(Migrator rules, TextWriter writer)
    {
        // SQLite table recreation pattern:
        // 1. Create new table with desired schema
        // 2. Copy data from old table
        // 3. Drop old table
        // 4. Rename new table

        var tempName = new SqliteObjectName(Expected.Identifier.Schema, Expected.Identifier.Name + "_new");

        writer.WriteLine("-- Table recreation required due to SQLite ALTER TABLE limitations");
        writer.WriteLine();

        // Create new table with temp name
        var tempTable = new Table(tempName);
        foreach (var column in Expected.Columns)
        {
            tempTable._columns.Add(column);
        }
        foreach (var pk in Expected.PrimaryKeyColumns)
        {
            tempTable._primaryKeyColumns.Add(pk);
        }
        tempTable.PrimaryKeyName = Expected.PrimaryKeyName;
        foreach (var fk in Expected.ForeignKeys)
        {
            tempTable.ForeignKeys.Add(fk);
        }

        tempTable.WriteCreateStatement(rules, writer);
        writer.WriteLine();

        // Copy data - only copy columns that exist in both tables
        var commonColumns = Expected.Columns
            .Where(e => Actual?.Columns.Any(a =>
                a.Name.Equals(e.Name, StringComparison.OrdinalIgnoreCase)) ?? false)
            .Select(c => SchemaUtils.QuoteName(c.Name))
            .ToList();

        if (commonColumns.Any())
        {
            var columnList = commonColumns.Join(", ");
            writer.WriteLine($"INSERT INTO {tempName.QualifiedName} ({columnList})");
            writer.WriteLine($"SELECT {columnList} FROM {Expected.Identifier.QualifiedName};");
            writer.WriteLine();
        }

        // Drop old table
        writer.WriteLine($"DROP TABLE {Expected.Identifier.QualifiedName};");
        writer.WriteLine();

        // Rename new table to original name
        writer.WriteLine($"ALTER TABLE {tempName.QualifiedName} RENAME TO {SchemaUtils.QuoteName(Expected.Identifier.Name)};");
        writer.WriteLine();

        // Recreate indexes (they were dropped with the old table)
        foreach (var index in Expected.Indexes)
        {
            writer.WriteLine(index.ToDDL(Expected));
        }
    }
}
