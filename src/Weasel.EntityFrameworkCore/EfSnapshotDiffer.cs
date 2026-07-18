using JasperFx;
using JasperFx.Core;
using Microsoft.EntityFrameworkCore.Migrations.Operations;
using Weasel.Core;
using Weasel.Core.Migrations;

namespace Weasel.EntityFrameworkCore;

/// <summary>
///     The Up / Down operation lists for one incremental migration.
/// </summary>
public class EfIncrementalOperations
{
    public EfIncrementalOperations(
        IReadOnlyList<MigrationOperation> upOperations,
        IReadOnlyList<MigrationOperation> downOperations)
    {
        UpOperations = upOperations;
        DownOperations = downOperations;
    }

    public IReadOnlyList<MigrationOperation> UpOperations { get; }
    public IReadOnlyList<MigrationOperation> DownOperations { get; }

    public bool HasChanges => UpOperations.Any();

    public static readonly EfIncrementalOperations Empty =
        new(Array.Empty<MigrationOperation>(), Array.Empty<MigrationOperation>());
}

/// <summary>
///     Produces incremental EF migration operations for migration N+1. Primary
///     mode: diff two <see cref="EfSchemaSnapshot" /> instances (the serialized
///     baseline of the last migration vs the current model) entirely in memory —
///     no live database and no shadow container (the Sable lesson). Secondary
///     mode: diff against an actual database via Weasel's own delta detection,
///     wrapping the generated SQL in Sql() operations — the path that also
///     covers everything the snapshot diff can't express (partition changes,
///     function bodies, ...).
/// </summary>
public static class EfSnapshotDiffer
{
    /// <summary>
    ///     Diff the current model against the serialized baseline snapshot and
    ///     return the incremental Up / Down operations.
    /// </summary>
    public static EfIncrementalOperations Diff(
        EfSchemaSnapshot baseline,
        EfSchemaSnapshot target,
        MigrationOperationTranslationOptions options)
    {
        var up = new List<MigrationOperation>();
        var down = new List<MigrationOperation>();

        diffSchemas(baseline, target, up);
        diffSequences(baseline, target, options, up, down);
        diffRawObjects(baseline, target, up, down);
        diffTables(baseline, target, options, up, down);

        // down entries are appended alongside their up counterparts, so the
        // rollback must run in reverse order (e.g. drop a new index before
        // dropping the column it covers)
        down.Reverse();

        return up.Any() || down.Any()
            ? new EfIncrementalOperations(up, down)
            : EfIncrementalOperations.Empty;
    }

    /// <summary>
    ///     Live-database baseline mode: run Weasel's own delta detection against
    ///     the actual database and wrap the resulting migration SQL in Sql()
    ///     operations (rollback SQL for Down). Requires a reachable database but
    ///     handles everything Weasel can migrate — including partition deltas and
    ///     function changes that the snapshot diff refuses.
    /// </summary>
    public static async Task<EfIncrementalOperations> DiffAgainstDatabaseAsync(
        IDatabase database,
        AutoCreate autoCreate = AutoCreate.CreateOrUpdate,
        CancellationToken ct = default)
    {
        var migration = await database.CreateMigrationAsync(ct).ConfigureAwait(false);

        if (migration.Difference == SchemaPatchDifference.None)
        {
            return EfIncrementalOperations.Empty;
        }

        var upWriter = new StringWriter();
        migration.WriteAllUpdates(upWriter, database.Migrator, autoCreate);

        var downWriter = new StringWriter();
        migration.WriteAllRollbacks(downWriter, database.Migrator);

        var up = new List<MigrationOperation>();
        if (upWriter.ToString().IsNotEmpty())
        {
            up.Add(new SqlOperation { Sql = upWriter.ToString() });
        }

        var down = new List<MigrationOperation>();
        if (downWriter.ToString().IsNotEmpty())
        {
            down.Add(new SqlOperation { Sql = downWriter.ToString() });
        }

        return new EfIncrementalOperations(up, down);
    }

    // ------------------------------------------------------------------
    // schemas
    // ------------------------------------------------------------------

    private static void diffSchemas(EfSchemaSnapshot baseline, EfSchemaSnapshot target, List<MigrationOperation> up)
    {
        var known = new HashSet<string>(
            baseline.Tables.Select(x => x.Schema).Concat(baseline.Sequences.Select(x => x.Schema)),
            StringComparer.OrdinalIgnoreCase);

        foreach (var schema in target.Tables.Select(x => x.Schema)
                     .Concat(target.Sequences.Select(x => x.Schema))
                     .Where(s => s.IsNotEmpty() && !known.Contains(s))
                     .Distinct(StringComparer.OrdinalIgnoreCase))
        {
            // schemas are additive-only: never dropped on Down (may be shared)
            up.Add(new EnsureSchemaOperation { Name = schema });
        }
    }

    // ------------------------------------------------------------------
    // sequences
    // ------------------------------------------------------------------

    private static void diffSequences(
        EfSchemaSnapshot baseline,
        EfSchemaSnapshot target,
        MigrationOperationTranslationOptions options,
        List<MigrationOperation> up,
        List<MigrationOperation> down)
    {
        var baselines = baseline.Sequences.ToDictionary(x => $"{x.Schema}.{x.Name}", StringComparer.OrdinalIgnoreCase);
        var targets = target.Sequences.ToDictionary(x => $"{x.Schema}.{x.Name}", StringComparer.OrdinalIgnoreCase);

        foreach (var added in target.Sequences.Where(x => !baselines.ContainsKey($"{x.Schema}.{x.Name}")))
        {
            up.Add(createSequence(added, options));
            down.Add(new DropSequenceOperation
            {
                Name = added.Name, Schema = MigrationOperationTranslation.SchemaFor(added.Schema, options)
            });
        }

        foreach (var removed in baseline.Sequences.Where(x => !targets.ContainsKey($"{x.Schema}.{x.Name}")))
        {
            up.Add(new DropSequenceOperation
            {
                Name = removed.Name, Schema = MigrationOperationTranslation.SchemaFor(removed.Schema, options)
            });
            down.Add(createSequence(removed, options));
        }

        foreach (var pair in target.Sequences
                     .Select(t => (Target: t, Baseline: baselines.GetValueOrDefault($"{t.Schema}.{t.Name}")))
                     .Where(x => x.Baseline != null))
        {
            if ((pair.Target.IncrementBy ?? 1L) != (pair.Baseline!.IncrementBy ?? 1L))
            {
                up.Add(alterSequence(pair.Target, options));
                down.Add(alterSequence(pair.Baseline, options));
            }
        }
    }

    private static CreateSequenceOperation createSequence(
        SnapshotSequence sequence, MigrationOperationTranslationOptions options) =>
        new()
        {
            Name = sequence.Name,
            Schema = MigrationOperationTranslation.SchemaFor(sequence.Schema, options),
            ClrType = typeof(long),
            StartValue = sequence.StartWith ?? 1L,
            IncrementBy = (int)(sequence.IncrementBy ?? 1L)
        };

    private static AlterSequenceOperation alterSequence(
        SnapshotSequence sequence, MigrationOperationTranslationOptions options) =>
        new()
        {
            Name = sequence.Name,
            Schema = MigrationOperationTranslation.SchemaFor(sequence.Schema, options),
            IncrementBy = (int)(sequence.IncrementBy ?? 1L)
        };

    // ------------------------------------------------------------------
    // raw-SQL objects
    // ------------------------------------------------------------------

    private static void diffRawObjects(
        EfSchemaSnapshot baseline,
        EfSchemaSnapshot target,
        List<MigrationOperation> up,
        List<MigrationOperation> down)
    {
        var baselines = baseline.RawObjects.ToDictionary(x => x.Identifier, StringComparer.OrdinalIgnoreCase);
        var targets = target.RawObjects.ToDictionary(x => x.Identifier, StringComparer.OrdinalIgnoreCase);

        foreach (var added in target.RawObjects.Where(x => !baselines.ContainsKey(x.Identifier)))
        {
            up.Add(new SqlOperation { Sql = added.CreateSql });
            down.Add(new SqlOperation { Sql = added.DropSql });
        }

        foreach (var removed in baseline.RawObjects.Where(x => !targets.ContainsKey(x.Identifier)))
        {
            up.Add(new SqlOperation { Sql = removed.DropSql });
            down.Add(new SqlOperation { Sql = removed.CreateSql });
        }

        foreach (var changed in target.RawObjects
                     .Where(x => baselines.TryGetValue(x.Identifier, out var b) &&
                                 !string.Equals(b.CreateSql, x.CreateSql, StringComparison.Ordinal)))
        {
            throw new NotSupportedException(
                $"The raw-SQL schema object '{changed.Identifier}' changed since the last snapshot. " +
                "The snapshot diff cannot infer a safe transformation for raw objects (partitioned " +
                "tables, functions, ...) — generate this migration with the live-database baseline " +
                $"mode ({nameof(EfSnapshotDiffer)}.{nameof(DiffAgainstDatabaseAsync)}) or author it by hand.");
        }
    }

    // ------------------------------------------------------------------
    // tables
    // ------------------------------------------------------------------

    private static void diffTables(
        EfSchemaSnapshot baseline,
        EfSchemaSnapshot target,
        MigrationOperationTranslationOptions options,
        List<MigrationOperation> up,
        List<MigrationOperation> down)
    {
        var baselines = baseline.Tables.ToDictionary(x => x.QualifiedName, StringComparer.OrdinalIgnoreCase);
        var targets = target.Tables.ToDictionary(x => x.QualifiedName, StringComparer.OrdinalIgnoreCase);

        foreach (var added in target.Tables.Where(x => !baselines.ContainsKey(x.QualifiedName)))
        {
            up.AddRange(MigrationOperationTranslation.TableOperations(added, options));
            down.Add(new DropTableOperation
            {
                Name = added.Name, Schema = MigrationOperationTranslation.SchemaFor(added.Schema, options)
            });
        }

        foreach (var removed in baseline.Tables.Where(x => !targets.ContainsKey(x.QualifiedName)))
        {
            up.Add(new DropTableOperation
            {
                Name = removed.Name, Schema = MigrationOperationTranslation.SchemaFor(removed.Schema, options)
            });
            // recreating the dropped table (schema only — data is gone) is the
            // best available rollback
            down.AddRange(MigrationOperationTranslation.TableOperations(removed, options));
        }

        foreach (var pair in target.Tables
                     .Select(t => (Target: t, Baseline: baselines.GetValueOrDefault(t.QualifiedName)))
                     .Where(x => x.Baseline != null))
        {
            diffTable(pair.Baseline!, pair.Target, options, up, down);
        }
    }

    private static void diffTable(
        SnapshotTable baseline,
        SnapshotTable target,
        MigrationOperationTranslationOptions options,
        List<MigrationOperation> up,
        List<MigrationOperation> down)
    {
        var tableName = target.Name;
        var schema = MigrationOperationTranslation.SchemaFor(target.Schema, options);

        var baselineColumns = baseline.Columns.ToDictionary(x => x.Name, StringComparer.OrdinalIgnoreCase);
        var targetColumns = target.Columns.ToDictionary(x => x.Name, StringComparer.OrdinalIgnoreCase);
        var baselineIndexes = baseline.Indexes.ToDictionary(x => x.Name, StringComparer.OrdinalIgnoreCase);
        var targetIndexes = target.Indexes.ToDictionary(x => x.Name, StringComparer.OrdinalIgnoreCase);
        var baselineFks = baseline.ForeignKeys.ToDictionary(x => x.Name, StringComparer.OrdinalIgnoreCase);
        var targetFks = target.ForeignKeys.ToDictionary(x => x.Name, StringComparer.OrdinalIgnoreCase);
        var baselineChecks = baseline.CheckConstraints.ToDictionary(x => x.Name, StringComparer.OrdinalIgnoreCase);
        var targetChecks = target.CheckConstraints.ToDictionary(x => x.Name, StringComparer.OrdinalIgnoreCase);

        // roughly mirrors TableDelta.WriteUpdate ordering: drop dependents
        // first, mutate columns, then add dependents back

        // indexes: removed or changed → drop up front
        foreach (var index in baseline.Indexes.Where(x =>
                     !targetIndexes.TryGetValue(x.Name, out var t) || !x.HasSameDefinition(t)))
        {
            up.Add(new DropIndexOperation { Name = index.Name, Table = tableName, Schema = schema });
            down.Add(MigrationOperationTranslation.IndexOperation(index, tableName, schema, options));
        }

        // foreign keys: removed or changed → drop
        foreach (var fk in baseline.ForeignKeys.Where(x =>
                     !targetFks.TryGetValue(x.Name, out var t) || !x.HasSameDefinition(t)))
        {
            up.Add(new DropForeignKeyOperation { Name = fk.Name, Table = tableName, Schema = schema });
            down.Add(MigrationOperationTranslation.ForeignKeyOperation(fk, tableName, schema, options));
        }

        // check constraints: removed or changed → drop
        foreach (var check in baseline.CheckConstraints.Where(x =>
                     !targetChecks.TryGetValue(x.Name, out var t) ||
                     !string.Equals(t.Expression, x.Expression, StringComparison.Ordinal)))
        {
            up.Add(new DropCheckConstraintOperation { Name = check.Name, Table = tableName, Schema = schema });
            down.Add(new AddCheckConstraintOperation
            {
                Name = check.Name, Table = tableName, Schema = schema, Sql = check.Expression
            });
        }

        // primary key change
        var pkChanged = baseline.PrimaryKeyName != target.PrimaryKeyName ||
                        !baseline.PrimaryKeyColumns.SequenceEqual(target.PrimaryKeyColumns,
                            StringComparer.OrdinalIgnoreCase);
        if (pkChanged)
        {
            if (baseline.PrimaryKeyName.IsNotEmpty())
            {
                up.Add(new DropPrimaryKeyOperation
                {
                    Name = baseline.PrimaryKeyName!, Table = tableName, Schema = schema
                });
                // appended before the target-PK drop so the reversed rollback
                // drops the new PK first, then restores this one
                down.Add(new AddPrimaryKeyOperation
                {
                    Name = baseline.PrimaryKeyName!, Table = tableName, Schema = schema,
                    Columns = baseline.PrimaryKeyColumns.ToArray()
                });
            }

            if (target.PrimaryKeyName.IsNotEmpty())
            {
                down.Add(new DropPrimaryKeyOperation
                {
                    Name = target.PrimaryKeyName!, Table = tableName, Schema = schema
                });
            }
        }

        // added columns
        foreach (var column in target.Columns.Where(x => !baselineColumns.ContainsKey(x.Name)))
        {
            up.Add(MigrationOperationTranslation.ColumnOperation(column, tableName, schema, options));
            down.Add(new DropColumnOperation { Name = column.Name, Table = tableName, Schema = schema });
        }

        // altered columns
        foreach (var pair in target.Columns
                     .Select(t => (Target: t, Baseline: baselineColumns.GetValueOrDefault(t.Name)))
                     .Where(x => x.Baseline != null && !x.Target.HasSameDefinition(x.Baseline)))
        {
            up.Add(alterColumn(pair.Target, pair.Baseline!, tableName, schema, options));
            down.Add(alterColumn(pair.Baseline!, pair.Target, tableName, schema, options));
        }

        // primary key (re-)creation after column changes
        if (pkChanged && target.PrimaryKeyName.IsNotEmpty())
        {
            up.Add(new AddPrimaryKeyOperation
            {
                Name = target.PrimaryKeyName!, Table = tableName, Schema = schema,
                Columns = target.PrimaryKeyColumns.ToArray()
            });
        }

        // removed columns (after PK changes so a former key column can go)
        foreach (var column in baseline.Columns.Where(x => !targetColumns.ContainsKey(x.Name)))
        {
            up.Add(new DropColumnOperation { Name = column.Name, Table = tableName, Schema = schema });
            down.Add(MigrationOperationTranslation.ColumnOperation(column, tableName, schema, options));
        }

        // check constraints: added or changed → add
        foreach (var check in target.CheckConstraints.Where(x =>
                     !baselineChecks.TryGetValue(x.Name, out var b) ||
                     !string.Equals(b.Expression, x.Expression, StringComparison.Ordinal)))
        {
            up.Add(new AddCheckConstraintOperation
            {
                Name = check.Name, Table = tableName, Schema = schema, Sql = check.Expression
            });
            if (!baselineChecks.ContainsKey(check.Name))
            {
                down.Add(new DropCheckConstraintOperation
                {
                    Name = check.Name, Table = tableName, Schema = schema
                });
            }
        }

        // foreign keys: added or changed → add
        foreach (var fk in target.ForeignKeys.Where(x =>
                     !baselineFks.TryGetValue(x.Name, out var b) || !x.HasSameDefinition(b)))
        {
            up.Add(MigrationOperationTranslation.ForeignKeyOperation(fk, tableName, schema, options));
            if (!baselineFks.ContainsKey(fk.Name))
            {
                down.Add(new DropForeignKeyOperation { Name = fk.Name, Table = tableName, Schema = schema });
            }
        }

        // indexes: added or changed → create
        foreach (var index in target.Indexes.Where(x =>
                     !baselineIndexes.TryGetValue(x.Name, out var b) || !x.HasSameDefinition(b)))
        {
            up.Add(MigrationOperationTranslation.IndexOperation(index, tableName, schema, options));
            if (!baselineIndexes.ContainsKey(index.Name))
            {
                down.Add(new DropIndexOperation { Name = index.Name, Table = tableName, Schema = schema });
            }
        }
    }

    private static AlterColumnOperation alterColumn(
        SnapshotColumn target,
        SnapshotColumn old,
        string tableName,
        string? schema,
        MigrationOperationTranslationOptions options)
    {
        var newDefinition = MigrationOperationTranslation.ColumnOperation(target, tableName, schema, options);
        var oldDefinition = MigrationOperationTranslation.ColumnOperation(old, tableName, schema, options);

        var operation = new AlterColumnOperation
        {
            Name = target.Name,
            Table = tableName,
            Schema = schema,
            ClrType = newDefinition.ClrType,
            ColumnType = newDefinition.ColumnType,
            IsNullable = newDefinition.IsNullable,
            DefaultValueSql = newDefinition.DefaultValueSql,
            ComputedColumnSql = newDefinition.ComputedColumnSql,
            IsStored = newDefinition.IsStored
        };

        foreach (var annotation in newDefinition.GetAnnotations())
        {
            operation.AddAnnotation(annotation.Name, annotation.Value);
        }

        operation.OldColumn.ClrType = oldDefinition.ClrType;
        operation.OldColumn.ColumnType = oldDefinition.ColumnType;
        operation.OldColumn.IsNullable = oldDefinition.IsNullable;
        operation.OldColumn.DefaultValueSql = oldDefinition.DefaultValueSql;
        operation.OldColumn.ComputedColumnSql = oldDefinition.ComputedColumnSql;
        operation.OldColumn.IsStored = oldDefinition.IsStored;

        return operation;
    }
}
