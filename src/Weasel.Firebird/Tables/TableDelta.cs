using JasperFx.Core;
using Weasel.Core;

namespace Weasel.Firebird.Tables;

/// <summary>
///     The difference between a Firebird table model and the table in the database, and the DDL that
///     closes it: one guarded or plain statement per change, so a migration runs one statement per
///     command and each commits on its own.
/// </summary>
public class TableDelta: SchemaObjectDelta<Table>, ISchemaObjectDeltaWithDeferrableForeignKeys,
    ISchemaObjectDeltaWithReason, ISchemaObjectDeltaWithWithheldDrops
{
    private readonly List<string> _withheldDrops = new();
    private readonly HashSet<string> _deferredForeignKeys = new(StringComparer.OrdinalIgnoreCase);

    public TableDelta(Table expected, Table? actual): base(expected, actual)
    {
    }

    /// <inheritdoc cref="ISchemaObjectDeltaWithWithheldDrops.WithheldDrops" />
    public IReadOnlyList<string> WithheldDrops => _withheldDrops;

    /// <summary>
    ///     Which change made this delta <see cref="SchemaPatchDifference.Invalid" /> (weasel#600).
    /// </summary>
    public string? InvalidReason { get; private set; }

    public ItemDelta<TableColumn> Columns { get; private set; } = null!;
    public ItemDelta<IndexDefinition> Indexes { get; private set; } = null!;
    public ItemDelta<ForeignKey> ForeignKeys { get; private set; } = null!;

    public SchemaPatchDifference PrimaryKeyDifference { get; private set; }

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

    protected override SchemaPatchDifference compare(Table expected, Table? actual)
    {
        if (actual == null)
        {
            Columns = new ItemDelta<TableColumn>(expected.Columns, [], (_, _) => true);
            Indexes = new ItemDelta<IndexDefinition>(expected.Indexes, [], (_, _) => true);
            ForeignKeys = new ItemDelta<ForeignKey>(expected.ForeignKeys, [], (_, _) => true);
            return SchemaPatchDifference.Create;
        }

        _withheldDrops.Clear();
        InvalidReason = null;

        var version = actual.ServerVersion;

        Columns = new ItemDelta<TableColumn>(expected.Columns,
            droppable(expected, expected.Columns, actual.Columns, "column"),
            (e, a) => e.TypeMatches(a, version)
                      && e.ComputationMatches(a)
                      && (!expected.DetectColumnDrift || e.IsComputed
                                                       || (e.DefaultMatches(a) && e.NullabilityMatches(a))));

        ForeignKeys = new ItemDelta<ForeignKey>(expected.ForeignKeys,
            droppable(expected, expected.ForeignKeys, actual.ForeignKeys, "foreign key"),
            (e, a) => e.IsEquivalentTo(a));

        // IgnoreIndex is Weasel.Core API and is honoured on both sides, as the PostgreSQL, SQLite, SQL
        // Server and MySQL twins do (weasel#642); without it an index a third party owns becomes an
        // extra and is dropped. Matched ignoring case, as Firebird resolves an index name.
        var expectedIndexes = expected.Indexes.Where(x => !isIgnored(expected, x)).ToArray();
        Indexes = new ItemDelta<IndexDefinition>(expectedIndexes,
            droppable(expected, expectedIndexes, actual.Indexes.Where(x => !isIgnored(expected, x)), "index"),
            (e, a) => e.Matches(a, expected));

        PrimaryKeyDifference = comparePrimaryKeys(expected, actual);

        if (!HasChanges())
        {
            return SchemaPatchDifference.None;
        }

        var reason = findInvalidReason(actual, version);
        if (reason != null)
        {
            InvalidReason = reason;
            return SchemaPatchDifference.Invalid;
        }

        return SchemaPatchDifference.Update;
    }

    private static SchemaPatchDifference comparePrimaryKeys(Table expected, Table actual)
    {
        var expectedHasKey = expected.PrimaryKeyColumns.Any();
        var actualHasKey = actual.PrimaryKeyColumns.Any();

        if (expectedHasKey && !actualHasKey)
        {
            return SchemaPatchDifference.Create;
        }

        if (expectedHasKey != actualHasKey)
        {
            return SchemaPatchDifference.Update;
        }

        // Order is compared only when pinned: the actual key carries the order the catalog declares,
        // and a model that only flags columns cannot express any other one.
        return expectedHasKey && !expected.PrimaryKeyOrderMatches(actual.PrimaryKeyColumns, StringComparer.OrdinalIgnoreCase)
            ? SchemaPatchDifference.Update
            : SchemaPatchDifference.None;
    }

    /// <summary>
    ///     The change Firebird cannot make in place, if there is one.
    /// </summary>
    private string? findInvalidReason(Table actual, FirebirdServerVersion? version)
    {
        // Firebird refuses to add or remove COMPUTED from a column; only dropping it and adding it back
        // would do, and that is the data's decision, not the schema's.
        var switched = Columns.Different.Where(x => x.Expected.IsComputed != x.Actual.IsComputed).ToArray();
        if (switched.Any())
        {
            return
                $"{switched.Select(x => $"column '{x.Expected.Name}'").Join(" and ")} would change between computed and stored, which Firebird cannot do in place";
        }

        // Firebird refuses to change the type of a column a primary key, unique constraint, unique index
        // or foreign key covers (335544538), widening included. Weasel never drops a key to make room: that is a
        // decision about the data, not about the schema.
        var keyed = keyColumns(actual);
        // A computed column holds no data, so any change to its type is made in place.
        var typeChanges = Columns.Different
            .Where(x => !x.Expected.IsComputed && !x.Expected.TypeMatches(x.Actual, version))
            .ToArray();

        var keyTypeChanges = typeChanges.Where(x => keyed.Contains(x.Actual.Name)).ToArray();
        if (keyTypeChanges.Any())
        {
            return
                $"{keyTypeChanges.Select(x => $"column '{x.Expected.Name}' ({x.Actual.Type} to {x.Expected.Type})").Join(" and ")} {(keyTypeChanges.Length == 1 ? "is" : "are")} part of a primary key, unique constraint, unique index or foreign key, and Firebird cannot change the type of such a column in place";
        }

        var unalterable = typeChanges
            .Where(x => !x.Expected.ColumnType(version).CanAlterFrom(x.Actual.ColumnType(version)))
            .ToArray();
        if (unalterable.Any())
        {
            return
                $"{unalterable.Select(x => $"column '{x.Expected.Name}' ({x.Actual.Type} to {x.Expected.Type})").Join(" and ")} cannot be altered in place: Firebird only widens a column's type";
        }

        var unaddable = Columns.Missing.Where(x => !x.CanAdd()).ToArray();
        if (unaddable.Any())
        {
            return
                $"{unaddable.Select(x => $"column '{x.Name}'").Join(" and ")} cannot be added to an existing table: Firebird refuses an identity column, or a NOT NULL column without a default, on a table that has rows";
        }

        return null;
    }

    /// <summary>
    ///     The columns a primary key, a unique constraint, a unique index or a foreign key of
    ///     <paramref name="actual" /> covers.
    /// </summary>
    private static HashSet<string> keyColumns(Table actual)
    {
        var keyed = new HashSet<string>(actual.PrimaryKeyColumns, StringComparer.OrdinalIgnoreCase);
        keyed.UnionWith(actual.UniqueConstraintColumns);

        foreach (var index in actual.Indexes.Where(x => x.IsUnique))
        {
            keyed.UnionWith(index.Columns);
        }

        foreach (var foreignKey in actual.ForeignKeys)
        {
            keyed.UnionWith(foreignKey.ColumnNames);
        }

        return keyed;
    }

    private static bool isIgnored(Table expected, IndexDefinition index)
        => expected.IgnoredIndexes.Contains(index.Name, StringComparer.OrdinalIgnoreCase);

    /// <summary>
    ///     True when at least one column, index, foreign key or the primary key differs.
    /// </summary>
    public bool HasChanges()
    {
        if (Actual == null)
        {
            return true;
        }

        return Columns.HasChanges() || Indexes.HasChanges() || ForeignKeys.HasChanges()
               || PrimaryKeyDifference != SchemaPatchDifference.None;
    }

    /// <summary>
    ///     The changes, in the order Firebird accepts them: foreign keys and indexes come off before the
    ///     columns under them are dropped or retyped, the primary key before its columns change, then
    ///     columns are added and altered, and the key, the indexes and the foreign keys go back on.
    /// </summary>
    public override void WriteUpdate(Migrator rules, TextWriter writer)
    {
        if (Difference == SchemaPatchDifference.Invalid)
        {
            throw new InvalidOperationException(
                $"TableDelta for {Expected.Identifier} is invalid: {InvalidReason ?? "the change cannot be applied in place"}");
        }

        if (Difference == SchemaPatchDifference.Create)
        {
            Expected.WriteCreateStatement(rules, writer, _deferredForeignKeys);
            return;
        }

        FirebirdObjectName.AssertDefaultSchema(Expected.Identifier.Schema, $"table {Expected.Identifier.Name}");

        foreach (var foreignKey in ForeignKeys.Extras)
        {
            foreignKey.WriteDropStatement(Expected, writer);
        }

        foreach (var change in ForeignKeys.Different)
        {
            change.Actual.WriteDropStatement(Expected, writer);
        }

        foreach (var index in Indexes.Extras)
        {
            Expected.WriteDropIndex(writer, index);
        }

        foreach (var change in Indexes.Different)
        {
            Expected.WriteDropIndex(writer, change.Actual);
        }

        if (PrimaryKeyDifference == SchemaPatchDifference.Update && Actual!.PrimaryKeyColumns.Any())
        {
            writeDropPrimaryKey(writer, Actual.PrimaryKeyName);
        }

        foreach (var column in Columns.Extras)
        {
            FirebirdScript.WritePsql(writer, column.DropColumnSql(Expected));
        }

        foreach (var column in Columns.Missing)
        {
            FirebirdScript.WritePsql(writer, column.AddColumnSql(Expected));
        }

        foreach (var change in Columns.Different)
        {
            writeColumnChange(writer, change.Expected, change.Actual);
        }

        if (PrimaryKeyDifference != SchemaPatchDifference.None && Expected.PrimaryKeyColumns.Any())
        {
            writeAddPrimaryKey(writer, Expected);
        }

        foreach (var index in Indexes.Missing)
        {
            Expected.WriteCreateIndex(writer, index);
        }

        foreach (var change in Indexes.Different)
        {
            Expected.WriteCreateIndex(writer, change.Expected);
        }

        foreach (var foreignKey in ForeignKeys.Missing.Where(x => !_deferredForeignKeys.Contains(x.Name)))
        {
            foreignKey.WriteAddStatement(Expected, writer);
        }

        foreach (var change in ForeignKeys.Different)
        {
            change.Expected.WriteAddStatement(Expected, writer);
        }
    }

    /// <summary>
    ///     Only what differs: a type, and -- when the table asks for <see cref="ITable.DetectColumnDrift" />
    ///     -- a default and nullability. The default goes before <c>SET NOT NULL</c>, which Firebird
    ///     checks against the rows already there. A computed column's type and expression are altered
    ///     together, the only way Firebird takes either.
    /// </summary>
    private void writeColumnChange(TextWriter writer, TableColumn expected, TableColumn actual)
    {
        if (expected.IsComputed)
        {
            FirebirdScript.WriteStatement(writer, expected.AlterComputedSql(Expected));
            return;
        }

        if (!expected.TypeMatches(actual, Actual!.ServerVersion))
        {
            FirebirdScript.WriteStatement(writer, expected.AlterTypeSql(Expected));
        }

        if (!Expected.DetectColumnDrift)
        {
            return;
        }

        if (!expected.DefaultMatches(actual))
        {
            FirebirdScript.WriteStatement(writer, expected.AlterDefaultSql(Expected));
        }

        if (!expected.NullabilityMatches(actual))
        {
            FirebirdScript.WriteStatement(writer, expected.AlterNullabilitySql(Expected));
        }
    }

    private void writeDropPrimaryKey(TextWriter writer, string constraintName)
    {
        FirebirdScript.WriteGuardedWhenExists(writer,
            CatalogProbes.Constraint(Expected.CatalogName,
                SchemaUtils.CatalogName(constraintName, Expected.PreserveIdentifierCase), "PRIMARY KEY"),
            $"ALTER TABLE {Expected.QuotedName} DROP CONSTRAINT {SchemaUtils.QuoteName(constraintName, Expected.PreserveIdentifierCase)}");
    }

    private void writeAddPrimaryKey(TextWriter writer, Table keyed)
    {
        FirebirdScript.WriteGuarded(writer, CatalogProbes.PrimaryKey(Expected.CatalogName),
            $"ALTER TABLE {Expected.QuotedName} ADD {keyed.PrimaryKeyDeclaration()}");
    }

    /// <summary>
    ///     The update, undone: what it added is dropped, what it dropped is put back empty, and what it
    ///     changed is changed back where Firebird can do that in place.
    /// </summary>
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
        }

        foreach (var change in ForeignKeys.Different)
        {
            change.Expected.WriteDropStatement(Expected, writer);
        }

        foreach (var index in Indexes.Missing)
        {
            Expected.WriteDropIndex(writer, index);
        }

        foreach (var change in Indexes.Different)
        {
            Expected.WriteDropIndex(writer, change.Expected);
        }

        if (PrimaryKeyDifference != SchemaPatchDifference.None && Expected.PrimaryKeyColumns.Any())
        {
            writeDropPrimaryKey(writer, Expected.PrimaryKeyName);
        }

        foreach (var column in Columns.Missing)
        {
            FirebirdScript.WritePsql(writer, column.DropColumnSql(Expected));
        }

        foreach (var column in Columns.Extras)
        {
            FirebirdScript.WritePsql(writer, column.AddColumnSql(Expected));
        }

        foreach (var change in Columns.Different)
        {
            // The forward change was a widening -- nothing else is Update -- so going back is a
            // narrowing, which Firebird refuses. Only defaults and nullability can be restored.
            if (change.Actual.IsComputed)
            {
                FirebirdScript.WriteStatement(writer, change.Actual.AlterComputedSql(Expected));
                continue;
            }

            var version = Actual.ServerVersion;
            if (!change.Expected.TypeMatches(change.Actual, version))
            {
                if (change.Actual.ColumnType(version).CanAlterFrom(change.Expected.ColumnType(version)))
                {
                    FirebirdScript.WriteStatement(writer, change.Actual.AlterTypeSql(Expected));
                }
                else
                {
                    writer.WriteLine(
                        $"-- Column {change.Actual.Name} of {Expected.Identifier} stays {change.Expected.Type}: Firebird cannot narrow it back to {change.Actual.Type} in place.");
                }
            }

            if (Expected.DetectColumnDrift)
            {
                if (!change.Expected.DefaultMatches(change.Actual))
                {
                    FirebirdScript.WriteStatement(writer, change.Actual.AlterDefaultSql(Expected));
                }

                if (!change.Expected.NullabilityMatches(change.Actual))
                {
                    FirebirdScript.WriteStatement(writer, change.Actual.AlterNullabilitySql(Expected));
                }
            }
        }

        if (PrimaryKeyDifference != SchemaPatchDifference.None && Actual.PrimaryKeyColumns.Any())
        {
            writeAddPrimaryKey(writer, Actual);
        }

        foreach (var index in Indexes.Extras)
        {
            Expected.WriteCreateIndex(writer, index);
        }

        foreach (var change in Indexes.Different)
        {
            Expected.WriteCreateIndex(writer, change.Actual);
        }

        foreach (var foreignKey in ForeignKeys.Extras)
        {
            foreignKey.WriteAddStatement(Expected, writer);
        }

        foreach (var change in ForeignKeys.Different)
        {
            change.Actual.WriteAddStatement(Expected, writer);
        }
    }

    /// <summary>
    ///     A Create delta has no previous state to restore, so this is a no-op rather than a throw.
    /// </summary>
    public override void WriteRestorationOfPreviousState(Migrator rules, TextWriter writer)
    {
        Actual?.WriteCreateStatement(rules, writer);
    }

    /// <summary>
    ///     The actual objects this delta is allowed to consider for removal. On an
    ///     <see cref="ITable.AddOnlyMigrations" /> table the actual side is narrowed to what the model
    ///     declares, so an undeclared object never becomes an Extra and is never dropped (weasel#629).
    /// </summary>
    private IEnumerable<T> droppable<T>(Table expected, IEnumerable<T> expectedItems,
        IEnumerable<T> actualItems, string kind) where T : INamed
        => expected.AddOnlyMigrations
            ? AddOnlyMigration.DeclaredOnly(expectedItems, actualItems, kind, _withheldDrops)
            : actualItems;

    public override string ToString()
    {
        return $"TableDelta for {Expected.Identifier}";
    }
}
