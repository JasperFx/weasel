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

        ForeignKeys = new ItemDelta<ForeignKey>(
            expected.ForeignKeys,
            actual.ForeignKeys,
            (e, a) => e.IsEquivalentTo(a));

        // Ahead of the index comparison, which needs to know whether the primary key is
        // staying put: MySQL is happy to back a foreign key with the primary key index.
        var expectedPks = expected.PrimaryKeyColumns.OrderBy(x => x).ToList();
        var actualPks = actual.PrimaryKeyColumns.OrderBy(x => x).ToList();

        if (!expectedPks.SequenceEqual(actualPks, StringComparer.OrdinalIgnoreCase))
        {
            PrimaryKeyDifference = SchemaPatchDifference.Update;
        }

        Indexes = new ItemDelta<IndexDefinition>(
            expected.Indexes,
            comparableIndexes(expected, actual, ForeignKeys,
                PrimaryKeyDifference == SchemaPatchDifference.None),
            (e, a) => e.Matches(a, expected));

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

    /// <summary>
    ///     MySQL creates a backing index for every FOREIGN KEY constraint — normally one
    ///     named after the constraint, or an existing index that already covers the
    ///     constrained columns — and <c>information_schema.STATISTICS</c> reports it like
    ///     any other index. InnoDB then refuses to drop it while the constraint still
    ///     needs it (error 1553).
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         What InnoDB actually requires is that <em>some</em> index cover the constrained
    ///         columns, not that any particular one survive. So only the last remaining cover is
    ///         held back from the comparison: as soon as another index will still be there once
    ///         the migration finishes, every other backing index is an ordinary extra and gets
    ///         dropped like one. That is what keeps a redundant index — the implicit one left
    ///         behind after the caller declares their own, say — from being protected forever.
    ///     </para>
    ///     <para>
    ///         A cover has to be one the DROP INDEX statements can rely on already being in
    ///         place: an index the expected table declares and that the database already has
    ///         unchanged, or the primary key when it is not being rebuilt. An index that is
    ///         about to be created, or dropped and recreated, is not there when it would be
    ///         needed and does not count. That costs nothing when a declared index is replacing
    ///         an implicit one, because InnoDB retires its own backing index by itself the
    ///         moment another index can serve the constraint — so the CREATE INDEX alone
    ///         settles it, and the next comparison sees no extra at all.
    ///     </para>
    ///     <para>
    ///         Indexes the expected table declares by name are always compared, so real drift on
    ///         a deliberately declared index is still detected.
    ///     </para>
    /// </remarks>
    private static IEnumerable<IndexDefinition> comparableIndexes(
        Table expected,
        Table actual,
        ItemDelta<ForeignKey> foreignKeys,
        bool primaryKeyIsStable)
    {
        var dropped = foreignKeys.Extras.Select(x => x.Name).ToHashSet(StringComparer.OrdinalIgnoreCase);

        // A foreign key that is itself being dropped stops protecting its index in the
        // same migration, because the DROP FOREIGN KEY is emitted first.
        var surviving = actual.ForeignKeys.Where(fk => !dropped.Contains(fk.Name)).ToArray();
        if (surviving.Length == 0)
        {
            return actual.Indexes;
        }

        var declared = expected.Indexes.Select(x => x.Name).ToHashSet(StringComparer.OrdinalIgnoreCase);
        var kept = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (var fk in surviving)
        {
            if (isCovered(expected, actual, fk, kept, primaryKeyIsStable))
            {
                continue;
            }

            // Nothing is guaranteed to be there for this constraint, so exactly one of the
            // indexes that could be backing it has to stay. Prefer the one MySQL creates
            // itself, then the narrowest — the least useful for anything but the constraint.
            var keeper = actual.Indexes
                .Where(index => backs(index, fk))
                .OrderBy(index => index.Name.Equals(fk.Name, StringComparison.OrdinalIgnoreCase) ? 0 : 1)
                .ThenBy(index => index.Columns.Length)
                .ThenBy(index => index.Name, StringComparer.OrdinalIgnoreCase)
                .FirstOrDefault();

            if (keeper != null)
            {
                kept.Add(keeper.Name);
            }
        }

        return actual.Indexes.Where(index => declared.Contains(index.Name) || !kept.Contains(index.Name));
    }

    /// <summary>
    ///     True when <paramref name="foreignKey" /> is certain to still have a backing index
    ///     after the migration runs, without holding anything else back.
    /// </summary>
    private static bool isCovered(
        Table expected,
        Table actual,
        ForeignKey foreignKey,
        IReadOnlyCollection<string> kept,
        bool primaryKeyIsStable)
    {
        if (primaryKeyIsStable && covers(actual.PrimaryKeyColumns, foreignKey))
        {
            return true;
        }

        if (actual.Indexes.Any(index => kept.Contains(index.Name) && backs(index, foreignKey)))
        {
            return true;
        }

        // Only an index that is already in place and staying that way. One the expected table
        // declares but the database does not have yet is created after the drops, and one that
        // is being altered is dropped and recreated — neither is there when it would be needed.
        return expected.Indexes.Any(index =>
        {
            if (!backs(index, foreignKey))
            {
                return false;
            }

            var current = actual.Indexes.FirstOrDefault(x =>
                x.Name.Equals(index.Name, StringComparison.OrdinalIgnoreCase));

            return current != null && index.Matches(current, expected);
        });
    }

    /// <summary>
    ///     True when MySQL could be using <paramref name="index" /> as the backing index for
    ///     <paramref name="foreignKey" /> — that is, the constrained columns are the leftmost
    ///     prefix of the index. More than one index can qualify, which is exactly why
    ///     <see cref="isCovered" /> exists: only the last of them is worth protecting.
    /// </summary>
    private static bool backs(IndexDefinition index, ForeignKey foreignKey)
        => covers(index.Columns, foreignKey);

    private static bool covers(IReadOnlyList<string> indexColumns, ForeignKey foreignKey)
    {
        var keyColumns = foreignKey.ColumnNames;

        if (keyColumns.Length == 0 || indexColumns.Count < keyColumns.Length)
        {
            return false;
        }

        for (var i = 0; i < keyColumns.Length; i++)
        {
            if (!indexColumns[i].Equals(keyColumns[i], StringComparison.OrdinalIgnoreCase))
            {
                return false;
            }
        }

        return true;
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

        // Ordering matters, and it is not symmetric: InnoDB refuses to drop an index that a
        // foreign key still needs (error 1553), and refuses to add a foreign key that has no
        // covering index. So every constraint comes off first, then indexes are reshaped, then
        // the constraints go back on against the indexes they need.
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
            }
        }

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

        if (ForeignKeys != null)
        {
            foreach (var change in ForeignKeys.Different)
            {
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
