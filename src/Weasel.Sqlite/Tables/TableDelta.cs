using JasperFx.Core;
using Weasel.Core;

namespace Weasel.Sqlite.Tables;

/// <summary>
/// Represents the differences between an expected table schema and the actual table in the database.
/// SQLite has limited ALTER TABLE support, so many changes require table recreation.
/// </summary>
public class TableDelta: SchemaObjectDelta<Table>, ISchemaObjectDeltaWithRebuild
{
    public TableDelta(Table expected, Table? actual): base(expected, actual)
    {
    }

    public ItemDelta<TableColumn> Columns { get; internal set; } = null!;
    public ItemDelta<IndexDefinition> Indexes { get; internal set; } = null!;
    public ItemDelta<ForeignKey> ForeignKeys { get; internal set; } = null!;

    /// <summary>
    /// Columns detected as renames: Expected has the new name, Actual has the old name.
    /// These are excluded from Missing/Extras processing in DDL generation.
    /// </summary>
    public IReadOnlyList<Change<TableColumn>> RenamedColumns => _renamedColumns;

    private readonly List<Change<TableColumn>> _renamedColumns = new();

    /// <summary>
    ///     SQLite cannot change a column's type, add or drop a foreign key, or change a primary key
    ///     with <c>ALTER TABLE</c>, so every such change reports
    ///     <see cref="SchemaPatchDifference.Invalid" /> — but <see cref="writeTableRecreation" />
    ///     has always known how to make it properly: new table, copy the surviving columns across,
    ///     drop the old one, rename, put the indexes and triggers back.
    /// </summary>
    /// <remarks>
    ///     Nothing called it. <c>Migrator</c> answered <c>Invalid</c> by dropping and recreating the
    ///     table, so a column type change emptied it and left a schema that looked correct — one row
    ///     before, none after (weasel#477). This is what tells the migrator to use the rebuild.
    ///     <para>
    ///     False when there is no existing table to copy from, because then there is nothing to
    ///     preserve and the ordinary create path is right.
    ///     </para>
    /// </remarks>
    public bool CanRebuildInPlace => Actual != null;

    public SchemaPatchDifference PrimaryKeyDifference { get; private set; }
    public bool RequiresTableRecreation { get; private set; }


    protected override SchemaPatchDifference compare(Table expected, Table? actual)
    {
        if (actual == null)
        {
            return SchemaPatchDifference.Create;
        }

        Columns = new ItemDelta<TableColumn>(expected.Columns, actual.Columns);
        // The comparison is not optional. Without it ItemDelta falls back to IndexDefinition's
        // Equals, which compares the name and nothing else -- so an index that changed from
        // non-unique to unique, or moved to different columns, reported no difference at all and
        // was never corrected. SQLite was the only provider not passing its Matches (weasel#449);
        // PostgreSQL, SQL Server and Oracle all did.
        Indexes = new ItemDelta<IndexDefinition>(
            expected.Indexes.Where(x => !expected.IgnoredIndexes.Contains(x.Name)),
            actual.Indexes.Where(x => !expected.IgnoredIndexes.Contains(x.Name)),
            (e, a) => e.Matches(a, expected));

        ForeignKeys = new ItemDelta<ForeignKey>(expected.ForeignKeys, actual.ForeignKeys);

        // Detect column renames: match Missing (new name) with Extras (old name) by structural equality
        detectRenamedColumns();

        // Check primary key differences
        PrimaryKeyDifference = SchemaPatchDifference.None;
        if (expected.PrimaryKeyColumns.Any() != actual.PrimaryKeyColumns.Any())
        {
            PrimaryKeyDifference = SchemaPatchDifference.Update;
        }
        else if (expected.PrimaryKeyColumns.Any() && !expected.PrimaryKeyOrderMatches(actual.PrimaryKeyColumns, StringComparer.OrdinalIgnoreCase))
        {
            PrimaryKeyDifference = SchemaPatchDifference.Update;
        }

        return determinePatchDifference();
    }

    /// <summary>
    /// Detects column renames by pairing Missing columns (new names) with Extra columns (old names)
    /// that have the same type and constraints. Only unambiguous 1:1 matches are treated as renames.
    /// </summary>
    private void detectRenamedColumns()
    {
        var unmatchedMissing = Columns.Missing.ToList();
        var unmatchedExtras = Columns.Extras.ToList();

        // For each missing column, find extra columns with matching structure
        foreach (var missing in Columns.Missing)
        {
            var candidates = unmatchedExtras
                .Where(extra => missing.IsStructuralMatch(extra))
                .ToList();

            // Only accept unambiguous 1:1 matches
            if (candidates.Count != 1)
            {
                continue;
            }

            var match = candidates[0];

            // Verify the reverse is also unambiguous: the extra column should only match this one missing
            var reverseCandidates = unmatchedMissing
                .Where(m => m.IsStructuralMatch(match))
                .ToList();

            if (reverseCandidates.Count != 1)
            {
                continue;
            }

            _renamedColumns.Add(new Change<TableColumn>(missing, match));
            unmatchedMissing.Remove(missing);
            unmatchedExtras.Remove(match);
        }
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

        var renamedMissingNames = new HashSet<string>(
            _renamedColumns.Select(r => r.Expected.Name), StringComparer.OrdinalIgnoreCase);
        var renamedExtraNames = new HashSet<string>(
            _renamedColumns.Select(r => r.Actual.Name), StringComparer.OrdinalIgnoreCase);

        var nonRenameMissing = Columns.Missing.Where(c => !renamedMissingNames.Contains(c.Name));
        var nonRenameExtras = Columns.Extras.Where(c => !renamedExtraNames.Contains(c.Name));

        if (nonRenameMissing.Any() || nonRenameExtras.Any() || Columns.Different.Any() ||
            _renamedColumns.Any())
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

        // SQLite accepts ALTER TABLE ADD COLUMN for a VIRTUAL generated column, but rejects a STORED
        // one outright ("cannot add a STORED column") -- it would have to compute and materialize a
        // value for every existing row. Recreation is the only way to introduce one.
        var renamedMissing = new HashSet<string>(
            _renamedColumns.Select(r => r.Expected.Name), StringComparer.OrdinalIgnoreCase);

        if (Columns.Missing.Any(c =>
                c.GeneratedType == GeneratedColumnType.Stored
                && c.GeneratedExpression.IsNotEmpty()
                && !renamedMissing.Contains(c.Name)))
        {
            return true;
        }

        // Check if columns being dropped are referenced by FKs or are part of the PK
        if (Columns.Extras.Any())
        {
            var renamedExtraNames = new HashSet<string>(
                _renamedColumns.Select(r => r.Actual.Name), StringComparer.OrdinalIgnoreCase);

            foreach (var extra in Columns.Extras)
            {
                // Skip columns handled as renames
                if (renamedExtraNames.Contains(extra.Name))
                {
                    continue;
                }

                // Column is part of primary key — requires recreation
                if (extra.IsPrimaryKey)
                {
                    return true;
                }

                // Column is referenced by a foreign key — requires recreation
                if (Actual!.ForeignKeys.Any(fk =>
                        fk.ColumnNames.Any(cn =>
                            cn.Equals(extra.Name, StringComparison.OrdinalIgnoreCase))))
                {
                    return true;
                }
            }
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

        // Build sets of renamed column names for filtering
        var renamedMissingNames = new HashSet<string>(
            _renamedColumns.Select(r => r.Expected.Name), StringComparer.OrdinalIgnoreCase);
        var renamedExtraNames = new HashSet<string>(
            _renamedColumns.Select(r => r.Actual.Name), StringComparer.OrdinalIgnoreCase);

        // 1. Drop extra indexes and indexes on columns being renamed/dropped
        foreach (var extra in Indexes.Extras)
        {
            writer.WriteDropIndex(Expected, extra);
        }

        foreach (var change in Indexes.Different)
        {
            writer.WriteDropIndex(Expected, change.Actual);
        }

        // 2. Rename columns (SQLite 3.25+)
        foreach (var rename in _renamedColumns)
        {
            writer.WriteLine(rename.Expected.RenameColumnSql(Expected, rename.Actual.Name));
        }

        // 3. Add missing columns, excluding those handled as renames
        foreach (var column in Columns.Missing.Where(c => !renamedMissingNames.Contains(c.Name)))
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

        // 4. Drop extra columns (SQLite 3.35+), excluding those handled as renames
        foreach (var column in Columns.Extras.Where(c => !renamedExtraNames.Contains(c.Name)))
        {
            writer.WriteLine(column.DropColumnSql(Expected));
        }

        // 5. Recreate/add indexes
        foreach (var index in Indexes.Missing)
        {
            writer.WriteLine(index.ToDDL(Expected));
        }

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

        // Same schema as the table being rebuilt. The single-argument constructor defaults to
        // "main", which is right for the overwhelmingly common case and wrong for every other one:
        // a temp-schema rebuild built main."x_new", copied out of temp."x", dropped it, and left the
        // replacement in main. Identical DDL for a main-schema table, so nothing else moves.
        var tempName = new SqliteObjectName(Expected.Identifier.Schema, Expected.Identifier.Name + "_new");

        writer.WriteLine("-- Table recreation required due to SQLite ALTER TABLE limitations");
        writer.WriteLine();

        // Create new table with temp name. STRICT and WITHOUT ROWID are part of what the table
        // IS, not decoration -- a rebuild that leaves them off silently returns the table to type
        // affinity and to a rowid it was defined without.
        var tempTable = new Table(tempName) { StrictTypes = Expected.StrictTypes, WithoutRowId = Expected.WithoutRowId };
        foreach (var column in Expected.Columns)
        {
            tempTable.AddColumn(column);
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

        // Copy data - only copy columns that exist in both tables (accounting for renames)
        var renameMap = _renamedColumns.ToDictionary(
            r => r.Expected.Name, r => r.Actual.Name, StringComparer.OrdinalIgnoreCase);

        var targetColumns = new List<string>();
        var sourceColumns = new List<string>();

        foreach (var expectedCol in Expected.Columns)
        {
            // SQLite refuses writes to a generated column, so it must stay out of the INSERT even when
            // it exists on both sides. Its value re-derives from the base columns we do copy. Before
            // weasel#426 this was accidentally safe -- generated columns were invisible in Actual, so
            // they were never "in both" -- and reading them properly is what makes it necessary.
            if (expectedCol.GeneratedExpression.IsNotEmpty())
            {
                continue;
            }

            if (renameMap.TryGetValue(expectedCol.Name, out var oldName))
            {
                // Renamed column: select from old name into new name
                targetColumns.Add(SchemaUtils.QuoteName(expectedCol.Name));
                sourceColumns.Add(SchemaUtils.QuoteName(oldName));
            }
            else if (Actual?.Columns.Any(a =>
                         a.Name.Equals(expectedCol.Name, StringComparison.OrdinalIgnoreCase)) ?? false)
            {
                // Unchanged column
                targetColumns.Add(SchemaUtils.QuoteName(expectedCol.Name));
                sourceColumns.Add(SchemaUtils.QuoteName(expectedCol.Name));
            }
        }

        if (targetColumns.Any())
        {
            writer.WriteLine($"INSERT INTO {tempName.QualifiedName} ({targetColumns.Join(", ")})");
            writer.WriteLine($"SELECT {sourceColumns.Join(", ")} FROM {Expected.Identifier.QualifiedName};");
            writer.WriteLine();
        }

        writeAutoIncrementCarryOver(writer, tempName);

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

        writeTriggerRestoration(writer);
    }

    private void writeAutoIncrementCarryOver(TextWriter writer, SqliteObjectName tempName)
    {
        if (!Expected.Columns.Any(x => x.IsAutoNumber) || Actual?.Columns.Any(x => x.IsAutoNumber) != true)
        {
            return;
        }

        var previous = SchemaUtils.EscapeLiteral(Expected.Identifier.Name);
        var replacement = SchemaUtils.EscapeLiteral(tempName.Name);

        // Name the schema, even for "main". sqlite_sequence is per-database, and an unqualified name
        // resolves against temp first -- so on a connection holding any temp AUTOINCREMENT table these
        // statements read and write temp.sqlite_sequence, match nothing, and silently carry nothing
        // over. The rebuilt table then reissues an id it had already handed out, which is the exact
        // failure this method exists to prevent.
        var seq = $"{SchemaUtils.QuoteName(Expected.Identifier.Schema)}.sqlite_sequence";

        writer.WriteLine(
            $"UPDATE {seq} SET seq = (SELECT seq FROM {seq} WHERE name = '{previous}') " +
            $"WHERE name = '{replacement}' AND seq < (SELECT seq FROM {seq} WHERE name = '{previous}');");
        writer.WriteLine(
            $"INSERT INTO {seq} (name, seq) SELECT '{replacement}', seq FROM {seq} " +
            $"WHERE name = '{previous}' AND NOT EXISTS (SELECT 1 FROM {seq} WHERE name = '{replacement}');");
        writer.WriteLine();
    }

    /// <summary>
    ///     Put back the triggers the DROP TABLE just destroyed.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         SQLite drops a table's triggers along with the table and says nothing about it. Every
    ///         rebuild — a column type change, a foreign key change, a primary key change — therefore
    ///         silently destroyed the table's data-integrity logic and left the schema looking
    ///         correct (weasel#452).
    ///     </para>
    ///     <para>
    ///         The statements come from <see cref="Table.ExistingTriggers" />, captured verbatim from
    ///         <c>sqlite_master</c> during introspection, so a trigger Weasel never declared is
    ///         preserved too. They are re-emitted after the rename, when the table exists again under
    ///         its own name.
    ///     </para>
    /// </remarks>
    private void writeTriggerRestoration(TextWriter writer)
    {
        if (Actual == null || !Actual.ExistingTriggers.Any())
        {
            return;
        }

        writer.WriteLine();

        foreach (var trigger in Actual.ExistingTriggers)
        {
            writer.WriteLine($"{trigger.TrimEnd(';')};");
        }
    }

    public bool HasChanges()
    {
        return Columns.HasChanges() || Indexes.HasChanges() || ForeignKeys.HasChanges() ||
               PrimaryKeyDifference != SchemaPatchDifference.None || _renamedColumns.Any();
    }

    public override void WriteRollback(Migrator rules, TextWriter writer)
    {
        // If the table doesn't exist in the database (Actual == null), rollback means dropping it
        if (Actual == null)
        {
            Expected.WriteDropStatement(rules, writer);
            return;
        }

        // For SQLite, many changes require table recreation due to ALTER TABLE limitations
        // If table recreation was required for the forward migration, rollback also requires recreation
        if (RequiresTableRecreation)
        {
            // Rollback to the actual (previous) state by recreating with old schema
            writeTableRecreationRollback(rules, writer);
            return;
        }

        // For simple changes (add/drop columns and indexes), we can rollback incrementally
        var renamedMissingNames = new HashSet<string>(
            _renamedColumns.Select(r => r.Expected.Name), StringComparer.OrdinalIgnoreCase);
        var renamedExtraNames = new HashSet<string>(
            _renamedColumns.Select(r => r.Actual.Name), StringComparer.OrdinalIgnoreCase);

        // Rollback indexes first
        rollbackIndexes(writer);

        // Rollback columns: reverse the operations
        // If we added columns, drop them (excluding renames)
        foreach (var column in Columns.Missing.Where(c => !renamedMissingNames.Contains(c.Name)))
        {
            writer.WriteLine(column.DropColumnSql(Expected));
        }

        // If we dropped columns, add them back (excluding renames)
        foreach (var column in Columns.Extras.Where(c => !renamedExtraNames.Contains(c.Name)))
        {
            if (column.CanAdd())
            {
                writer.WriteLine(column.AddColumnSql(Actual));
            }
        }

        // Reverse renames
        foreach (var rename in _renamedColumns)
        {
            writer.WriteLine(rename.Actual.RenameColumnSql(Expected, rename.Expected.Name));
        }
    }

    private void rollbackIndexes(TextWriter writer)
    {
        // Rollback missing indexes (we created them, so drop them)
        foreach (var index in Indexes.Missing)
        {
            writer.WriteDropIndex(Expected, index);
        }

        // Rollback extra indexes (we dropped them, so recreate them)
        foreach (var index in Indexes.Extras)
        {
            writer.WriteLine(index.ToDDL(Actual!));
        }

        // Rollback different indexes (we changed them, so restore original)
        foreach (var change in Indexes.Different)
        {
            writer.WriteDropIndex(Expected, change.Expected);
            writer.WriteLine(change.Actual.ToDDL(Actual!));
        }
    }

    private void writeTableRecreationRollback(Migrator rules, TextWriter writer)
    {
        // SQLite rollback for table recreation: restore the Actual (previous) schema
        // This is the reverse of writeTableRecreation()

        var tempName = new SqliteObjectName(Actual.Identifier.Name + "_rollback");

        writer.WriteLine("-- Rollback: Table recreation required due to SQLite ALTER TABLE limitations");
        writer.WriteLine();

        // Create temp table with actual (old) schema
        var tempTable = new Table(tempName) { StrictTypes = Actual.StrictTypes, WithoutRowId = Actual.WithoutRowId };
        foreach (var column in Actual.Columns)
        {
            tempTable.AddColumn(column);
        }
        foreach (var pk in Actual.PrimaryKeyColumns)
        {
            tempTable._primaryKeyColumns.Add(pk);
        }
        tempTable.PrimaryKeyName = Actual.PrimaryKeyName;
        foreach (var fk in Actual.ForeignKeys)
        {
            tempTable.ForeignKeys.Add(fk);
        }

        tempTable.WriteCreateStatement(rules, writer);
        writer.WriteLine();

        // Copy data - only copy columns that exist in both tables
        var commonColumns = Actual.Columns
            .Where(a => Expected.Columns.Any(e =>
                e.Name.Equals(a.Name, StringComparison.OrdinalIgnoreCase)))
            .Select(c => SchemaUtils.QuoteName(c.Name))
            .ToList();

        if (commonColumns.Any())
        {
            var columnList = commonColumns.Join(", ");
            writer.WriteLine($"INSERT INTO {tempName.QualifiedName} ({columnList})");
            writer.WriteLine($"SELECT {columnList} FROM {Expected.Identifier.QualifiedName};");
            writer.WriteLine();
        }

        // Drop current table
        writer.WriteLine($"DROP TABLE {Expected.Identifier.QualifiedName};");
        writer.WriteLine();

        // Rename temp table to original name
        writer.WriteLine($"ALTER TABLE {tempName.QualifiedName} RENAME TO {SchemaUtils.QuoteName(Actual.Identifier.Name)};");
        writer.WriteLine();

        // Recreate indexes from actual (old) schema
        foreach (var index in Actual.Indexes)
        {
            writer.WriteLine(index.ToDDL(Actual));
        }
    }
}
