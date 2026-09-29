using JasperFx.Core;
using Weasel.Core;

namespace Weasel.Sqlite.Tables;

/// <summary>
/// Represents the differences between an expected table schema and the actual table in the database.
/// SQLite has limited ALTER TABLE support, so many changes require table recreation.
/// </summary>
public class TableDelta: SchemaObjectDelta<Table>, ISchemaObjectDeltaWithRebuild, ISchemaObjectDeltaWithReason,
    ISchemaObjectDeltaWithWithheldDrops
{
    private readonly List<string> _withheldDrops = new();

    /// <inheritdoc cref="ISchemaObjectDeltaWithWithheldDrops.WithheldDrops" />
    public IReadOnlyList<string> WithheldDrops => _withheldDrops;

    public TableDelta(Table expected, Table? actual): base(expected, actual)
    {
    }

    /// <summary>
    ///     Which change made this delta <see cref="SchemaPatchDifference.Invalid" /> (weasel#600).
    ///     SQLite's deltas can almost always rebuild in place, so this mostly explains a refusal
    ///     under <c>CreateOnly</c> rather than a drop.
    /// </summary>
    public string? InvalidReason { get; private set; }

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
            // Nothing below this point runs, so every ItemDelta has to be built here as well --
            // against no actuals, which is exactly what a table that does not exist yet is. The
            // fields were left null, and HasChanges() threw a NullReferenceException for the one
            // case whose answer is unambiguously "yes, it needs creating" (weasel#658).
            noActuals(expected);

            return SchemaPatchDifference.Create;
        }

        _withheldDrops.Clear();

        Columns = new ItemDelta<TableColumn>(expected.Columns,
            droppable(expected, expected.Columns, actual.Columns, "column"));
        // The comparison is not optional. Without it ItemDelta falls back to IndexDefinition's
        // Equals, which compares the name and nothing else -- so an index that changed from
        // non-unique to unique, or moved to different columns, reported no difference at all and
        // was never corrected. SQLite was the only provider not passing its Matches (weasel#449);
        // PostgreSQL, SQL Server and Oracle all did.
        var expectedIndexes = expected.Indexes.Where(x => !expected.IgnoredIndexes.Contains(x.Name)).ToArray();
        Indexes = new ItemDelta<IndexDefinition>(
            expectedIndexes,
            droppable(expected, expectedIndexes,
                actual.Indexes.Where(x => !expected.IgnoredIndexes.Contains(x.Name)), "index"),
            (e, a) => e.Matches(a, expected));

        ForeignKeys = new ItemDelta<ForeignKey>(expected.ForeignKeys,
            droppable(expected, expected.ForeignKeys, actual.ForeignKeys, "foreign key"));

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

        // Check if table recreation is required due to SQLite limitations
        RequiresTableRecreation = requiresTableRecreation();

        if (RequiresTableRecreation)
        {
            // Table recreation is effectively an Invalid state that requires drop+create
            InvalidReason = "SQLite cannot make this change with ALTER TABLE, so the table has to be rebuilt";
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
            InvalidReason = "SQLite cannot add or drop a foreign key with ALTER TABLE";
            return SchemaPatchDifference.Invalid;
        }

        if (PrimaryKeyDifference != SchemaPatchDifference.None)
        {
            InvalidReason = "SQLite cannot change a primary key with ALTER TABLE";
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

        // Before this table writes anything, so a refusal does not leave its statements half-written
        var carried = carriedOver();

        writer.WriteLine("-- Table recreation required due to SQLite ALTER TABLE limitations");
        writer.WriteLine();

        // Create new table with temp name. STRICT and WITHOUT ROWID are part of what the table
        // IS, not decoration -- a rebuild that leaves them off silently returns the table to type
        // affinity and to a rowid it was defined without.
        var tempTable = new Table(tempName) { StrictTypes = Expected.StrictTypes, WithoutRowId = Expected.WithoutRowId };
        foreach (var column in Expected.Columns)
        {
            tempTable.AddColumn(column.Clone());
        }
        foreach (var pk in Expected.PrimaryKeyColumns)
        {
            tempTable._primaryKeyColumns.Add(pk);
        }
        tempTable.PrimaryKeyName = Expected.PrimaryKeyName;
        foreach (var fk in Expected.ForeignKeys.Concat(carried.ForeignKeys))
        {
            tempTable.ForeignKeys.Add(fk);
        }

        tempTable.CarriedColumnDefinitions.AddRange(carried.Columns.Select(x => x.Definition));
        tempTable.CarriedConstraints.AddRange(carried.Constraints);

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

        // The same rule as above for a generated column: its definition came across, so its value
        // re-derives, and SQLite would refuse the write anyway
        foreach (var (column, _) in carried.Columns.Where(x => !x.Column.IsGeneratedInDatabase))
        {
            targetColumns.Add(SchemaUtils.QuoteName(column.Name));
            sourceColumns.Add(SchemaUtils.QuoteName(column.Name));
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

        foreach (var index in carried.Indexes)
        {
            writer.WriteLine(index);
        }

        writeTriggerRestoration(writer);
    }

    /// <summary>
    ///     What the rebuild has to keep of the table it replaces, beyond what the model declares.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         The rebuild builds the new table from the model, and <c>DROP TABLE</c> takes everything
    ///         else with the old one. That is right when the model is the whole truth about the
    ///         table. It is not on an <see cref="ITable.AddOnlyMigrations" /> table, whose delta
    ///         leaves every undeclared column, index and foreign key out of the comparison precisely
    ///         so that no migration removes one (weasel#629) — and then the rebuild removed them all,
    ///         with the rows, under <c>CreateOrUpdate</c>.
    ///     </para>
    ///     <para>
    ///         So on an add-only table the rebuild keeps them, each taken from what SQLite recorded
    ///         rather than re-rendered from what the pragmas report, which is the same principle it
    ///         already applies to the table's triggers:
    ///     </para>
    ///     <list type="bullet">
    ///         <item>
    ///             a column by its definition in the stored <c>CREATE TABLE</c> text, which is the
    ///             only place its collation, <c>CHECK</c>, generation expression or inline
    ///             <c>REFERENCES</c> is recorded (<see cref="StoredTableDefinition" />)
    ///         </item>
    ///         <item>an index by the <c>CREATE INDEX</c> statement <c>sqlite_master</c> holds for it</item>
    ///         <item>
    ///             a <c>UNIQUE</c> or <c>CHECK</c> table constraint verbatim — the SQLite model cannot
    ///             declare either (weasel#488), so every one is undeclared
    ///         </item>
    ///         <item>
    ///             a foreign key as read back from the catalog, unless the model declares the same
    ///             key under another name or its column's own definition already carries it
    ///         </item>
    ///     </list>
    ///     <para>
    ///         An index in <see cref="TableBase{TColumn,TIndex,TForeignKey}.IgnoredIndexes" /> is kept
    ///         on every table, add-only or not: it is owned by someone else, and ignoring it is a
    ///         promise that Weasel neither drops nor recreates it.
    ///     </para>
    /// </remarks>
    private CarriedOver carriedOver()
    {
        var carried = new CarriedOver();
        if (Actual == null)
        {
            return carried;
        }

        foreach (var index in Actual.Indexes)
        {
            var declared = Expected.Indexes.Any(x => x.Name.Equals(index.Name, StringComparison.OrdinalIgnoreCase));
            if (!declared && (Expected.AddOnlyMigrations || Expected.IgnoredIndexes.Contains(index.Name)))
            {
                var statement = index.ExistingCreateStatement ?? index.ToDDL(Expected);
                carried.Indexes.Add($"{statement.Trim().TrimEnd(';')};");
            }
        }

        if (!Expected.AddOnlyMigrations)
        {
            return carried;
        }

        var stored = StoredTableDefinition.Parse(Actual.ExistingCreateStatement);
        var inlineKeys = new List<(string Column, string Table)>();

        // Matched the way AddOnlyMigration.DeclaredOnly matches them, so exactly the columns the
        // delta withheld are the ones kept
        var declaredColumns = Expected.Columns.Select(x => x.Name).ToHashSet(StringComparer.OrdinalIgnoreCase);

        foreach (var column in Actual.Columns.Where(x => !declaredColumns.Contains(x.Name)))
        {
            if (column.IsPrimaryKey)
            {
                throw new SchemaMigrationException(
                    $"Refusing to rebuild {Expected.Identifier}: column '{column.Name}' is part of the table's primary key, " +
                    "and the model neither declares the column nor includes it in the primary key it declares. " +
                    $"The table is marked {nameof(ITable.AddOnlyMigrations)}, so the rebuild will not decide between keeping " +
                    "a key the model does not declare and taking the column out of it. Declare the column in the model, " +
                    $"or clear {nameof(ITable.AddOnlyMigrations)} to rebuild the table to the model alone.");
            }

            var definition = stored?.ColumnNamed(column.Name)
                             ?? throw new SchemaMigrationException(
                                 $"Refusing to rebuild {Expected.Identifier}: column '{column.Name}', which the model does not " +
                                 "declare, could not be read back from the table's stored CREATE TABLE statement, so the rebuild " +
                                 $"cannot keep it as it is. The table is marked {nameof(ITable.AddOnlyMigrations)}, so a migration " +
                                 "never removes what the model does not declare. Declare the column in the model, or clear " +
                                 $"{nameof(ITable.AddOnlyMigrations)} to rebuild the table to the model alone.");

            carried.Columns.Add((column, definition.Text));
            inlineKeys.AddRange(definition.ReferencedTables.Select(table => (column.Name, table)));
        }

        if (stored != null)
        {
            carried.Constraints.AddRange(stored.Constraints
                .Where(x => x.Kind is "UNIQUE" or "CHECK")
                .Select(x => x.Text));
        }

        foreach (var fk in Actual.ForeignKeys)
        {
            if (Expected.ForeignKeys.Any(x =>
                    x.Name.Equals(fk.Name, StringComparison.OrdinalIgnoreCase) || x.LinksSameColumnsAs(fk)))
            {
                continue;
            }

            var inline = inlineKeys.FindIndex(x =>
                fk.ColumnNames.Length == 1
                && fk.ColumnNames[0].Equals(x.Column, StringComparison.OrdinalIgnoreCase)
                && string.Equals(fk.LinkedTable?.Name, x.Table, StringComparison.OrdinalIgnoreCase));

            if (inline >= 0)
            {
                inlineKeys.RemoveAt(inline);
                continue;
            }

            carried.ForeignKeys.Add(fk);
        }

        return carried;
    }

    private sealed class CarriedOver
    {
        public List<(TableColumn Column, string Definition)> Columns { get; } = new();
        public List<string> Constraints { get; } = new();
        public List<ForeignKey> ForeignKeys { get; } = new();
        public List<string> Indexes { get; } = new();
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

    /// <summary>
    ///     Populate the deltas for a table that is not in the database at all: every column, index
    ///     and foreign key the expected table declares is missing.
    /// </summary>
    private void noActuals(Table expected)
    {
        _withheldDrops.Clear();
        _renamedColumns.Clear();

        Columns = ItemDelta<TableColumn>.AllMissing(expected.Columns);
        Indexes = ItemDelta<IndexDefinition>.AllMissing(
            expected.Indexes.Where(x => !expected.IgnoredIndexes.Contains(x.Name)));
        ForeignKeys = ItemDelta<ForeignKey>.AllMissing(expected.ForeignKeys);
    }

    public bool HasChanges()
    {
        // A table that is not there yet has to be created, whatever else is or is not declared on it.
        if (Actual == null) return true;

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
        // If table recreation was required for the forward migration, rollback also requires recreation.
        // The same condition WriteUpdate rebuilds on, so the rollback is always the mirror of it.
        if (Difference == SchemaPatchDifference.Invalid || RequiresTableRecreation)
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

    /// <summary>
    ///     The reverse of <see cref="writeTableRecreation" />: rebuild the table back into the shape
    ///     it had, with its rows.
    /// </summary>
    /// <remarks>
    ///     Unreachable until <c>SchemaMigration.WriteAllRollbacks</c> learned to call it for a
    ///     rebuildable delta instead of dropping the table and recreating it empty, so it had drifted
    ///     from the forward rebuild. It now mirrors it step for step: the temporary table in the
    ///     table's own schema, a rename copied back under the old name, a generated column left out
    ///     of the copy, the <c>AUTOINCREMENT</c> high-water mark carried across, and the triggers
    ///     <c>DROP TABLE</c> takes put back.
    /// </remarks>
    private void writeTableRecreationRollback(Migrator rules, TextWriter writer)
    {
        var actual = Actual!;
        var tempName = new SqliteObjectName(actual.Identifier.Schema, actual.Identifier.Name + "_rollback");

        writer.WriteLine("-- Rollback: Table recreation required due to SQLite ALTER TABLE limitations");
        writer.WriteLine();

        // Create temp table with actual (old) schema
        var tempTable = new Table(tempName) { StrictTypes = actual.StrictTypes, WithoutRowId = actual.WithoutRowId };
        foreach (var column in actual.Columns)
        {
            tempTable.AddColumn(column.Clone());
        }
        foreach (var pk in actual.PrimaryKeyColumns)
        {
            tempTable._primaryKeyColumns.Add(pk);
        }
        tempTable.PrimaryKeyName = actual.PrimaryKeyName;
        foreach (var fk in actual.ForeignKeys)
        {
            tempTable.ForeignKeys.Add(fk);
        }

        tempTable.WriteCreateStatement(rules, writer);
        writer.WriteLine();

        // Copy back every column of the old shape that the rebuilt table still has, under the name
        // it has there: its own, or the one a rename in the rebuild gave it
        var renamedTo = _renamedColumns.ToDictionary(
            r => r.Actual.Name, r => r.Expected.Name, StringComparer.OrdinalIgnoreCase);

        var targetColumns = new List<string>();
        var sourceColumns = new List<string>();

        foreach (var column in actual.Columns)
        {
            // The forward copy's rule, by the forward copy's test: a column the recreated table
            // declares as generated re-derives its value, and SQLite refuses the write. A column read
            // back from the catalog carries no expression today, so it is recreated plain and copied.
            if (column.GeneratedExpression.IsNotEmpty())
            {
                continue;
            }

            if (renamedTo.TryGetValue(column.Name, out var newName))
            {
                targetColumns.Add(SchemaUtils.QuoteName(column.Name));
                sourceColumns.Add(SchemaUtils.QuoteName(newName));
            }
            // An add-only table's rebuild keeps the columns the model does not declare (weasel#639),
            // so they are all still there to copy back. Asking the model for them, as a declared
            // table's rollback does, would bring every undeclared column back NULL.
            else if (Expected.AddOnlyMigrations
                     || Expected.Columns.Any(e => e.Name.Equals(column.Name, StringComparison.OrdinalIgnoreCase)))
            {
                targetColumns.Add(SchemaUtils.QuoteName(column.Name));
                sourceColumns.Add(SchemaUtils.QuoteName(column.Name));
            }
        }

        if (targetColumns.Any())
        {
            writer.WriteLine($"INSERT INTO {tempName.QualifiedName} ({targetColumns.Join(", ")})");
            writer.WriteLine($"SELECT {sourceColumns.Join(", ")} FROM {Expected.Identifier.QualifiedName};");
            writer.WriteLine();
        }

        writeAutoIncrementCarryOver(writer, tempName);

        // Drop current table
        writer.WriteLine($"DROP TABLE {Expected.Identifier.QualifiedName};");
        writer.WriteLine();

        // Rename temp table to original name
        writer.WriteLine($"ALTER TABLE {tempName.QualifiedName} RENAME TO {SchemaUtils.QuoteName(actual.Identifier.Name)};");
        writer.WriteLine();

        // Recreate indexes from actual (old) schema
        foreach (var index in actual.Indexes)
        {
            writer.WriteLine(index.ToDDL(actual));
        }

        writeTriggerRestoration(writer);
    }
}
