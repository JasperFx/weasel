using System.Text.Json;
using System.Text.Json.Serialization;
using JasperFx.Core;
using Weasel.Core;

namespace Weasel.EntityFrameworkCore;

/// <summary>
///     Design-time snapshot of a Weasel schema model — Weasel's analog of EF's
///     ModelSnapshot, but serialized JSON written beside the generated
///     migrations and never compiled. The incremental `add` flow deserializes
///     the snapshot of the last migration, diffs it against the current model
///     entirely in memory (no live database, no shadow container), emits the
///     incremental operations, and rewrites the snapshot.
/// </summary>
public class EfSchemaSnapshot
{
    private static readonly JsonSerializerOptions SerializerOptions = new()
    {
        WriteIndented = true,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        Converters = { new JsonStringEnumConverter() }
    };

    public int Version { get; set; } = 1;

    /// <summary>The last generated migration id this snapshot corresponds to</summary>
    public string? MigrationId { get; set; }

    public List<SnapshotTable> Tables { get; set; } = new();
    public List<SnapshotSequence> Sequences { get; set; } = new();

    /// <summary>
    ///     Schema objects that route through the raw-SQL fallback (partitioned
    ///     tables, functions, stored procedures, ...) — captured as their CREATE
    ///     / DROP DDL so changes can at least be detected.
    /// </summary>
    public List<SnapshotRawObject> RawObjects { get; set; } = new();

    /// <summary>
    ///     Capture the current Weasel model. The same ForceRawSql hook used for
    ///     operation translation decides which objects are carried as raw DDL.
    /// </summary>
    public static EfSchemaSnapshot FromSchemaObjects(
        IEnumerable<ISchemaObject> schemaObjects,
        MigrationOperationTranslationOptions options)
    {
        var snapshot = new EfSchemaSnapshot();

        foreach (var schemaObject in schemaObjects)
        {
            if (options.ForceRawSql?.Invoke(schemaObject) == true ||
                schemaObject is not (ITable or SequenceBase))
            {
                snapshot.RawObjects.Add(SnapshotRawObject.From(schemaObject, options));
            }
            else if (schemaObject is ITable table)
            {
                snapshot.Tables.Add(SnapshotTable.From(table));
            }
            else if (schemaObject is SequenceBase sequence)
            {
                snapshot.Sequences.Add(new SnapshotSequence
                {
                    Schema = sequence.Identifier.Schema,
                    Name = sequence.Identifier.Name,
                    StartWith = sequence.StartWith,
                    IncrementBy = sequence.IncrementBy
                });
            }
        }

        return snapshot;
    }

    public string ToJson() => JsonSerializer.Serialize(this, SerializerOptions);

    public static EfSchemaSnapshot FromJson(string json)
        => JsonSerializer.Deserialize<EfSchemaSnapshot>(json, SerializerOptions)
           ?? throw new InvalidOperationException("Could not deserialize the Weasel schema snapshot");
}

public class SnapshotTable
{
    public string Schema { get; set; } = string.Empty;
    public string Name { get; set; } = string.Empty;
    public string QualifiedName => $"{Schema}.{Name}";

    public List<SnapshotColumn> Columns { get; set; } = new();
    public string? PrimaryKeyName { get; set; }
    public List<string> PrimaryKeyColumns { get; set; } = new();
    public List<SnapshotIndex> Indexes { get; set; } = new();
    public List<SnapshotForeignKey> ForeignKeys { get; set; } = new();
    public List<SnapshotCheckConstraint> CheckConstraints { get; set; } = new();

    public static SnapshotTable From(ITable table)
    {
        var snapshot = new SnapshotTable
        {
            Schema = table.Identifier.Schema,
            Name = table.Identifier.Name,
            PrimaryKeyName = table.PrimaryKeyColumns.Any() ? table.PrimaryKeyName : null,
            PrimaryKeyColumns = table.PrimaryKeyColumns.ToList()
        };

        foreach (var column in table.Columns)
        {
            snapshot.Columns.Add(new SnapshotColumn
            {
                Name = column.Name,
                Type = column.Type,
                Nullable = column.AllowNulls && !column.IsPrimaryKey,
                DefaultExpression = column.DefaultExpression,
                Identity = column.IsAutoNumber,
                ComputedExpression = column.ComputedExpression,
                ComputedIsStored = column.ComputedExpression.IsNotEmpty() ? column.ComputedColumnIsStored : null
            });
        }

        foreach (var index in table.Indexes)
        {
            snapshot.Indexes.Add(new SnapshotIndex
            {
                Name = index.Name,
                Columns = index.Columns?.ToList() ?? new List<string>(),
                IsUnique = index.IsUnique,
                Predicate = index.Predicate,
                IncludeColumns = index.IncludeColumns?.ToList(),
                Method = index.Method
            });
        }

        foreach (var foreignKey in table.ForeignKeys)
        {
            snapshot.ForeignKeys.Add(new SnapshotForeignKey
            {
                Name = foreignKey.Name,
                Columns = foreignKey.ColumnNames.ToList(),
                PrincipalSchema = foreignKey.LinkedTable?.Schema,
                PrincipalTable = foreignKey.LinkedTable?.Name ?? string.Empty,
                PrincipalColumns = foreignKey.LinkedNames.ToList(),
                OnDelete = foreignKey.DeleteAction,
                OnUpdate = foreignKey.UpdateAction
            });
        }

        foreach (var check in table.CheckConstraints)
        {
            snapshot.CheckConstraints.Add(new SnapshotCheckConstraint
            {
                Name = check.Name, Expression = check.Expression
            });
        }

        return snapshot;
    }
}

public class SnapshotColumn
{
    public string Name { get; set; } = string.Empty;
    public string Type { get; set; } = string.Empty;
    public bool Nullable { get; set; }
    public string? DefaultExpression { get; set; }
    public bool Identity { get; set; }
    public string? ComputedExpression { get; set; }
    public bool? ComputedIsStored { get; set; }

    public bool HasSameDefinition(SnapshotColumn other)
        => Type.EqualsIgnoreCase(other.Type)
           && Nullable == other.Nullable
           && string.Equals(DefaultExpression, other.DefaultExpression, StringComparison.Ordinal)
           && Identity == other.Identity
           && string.Equals(ComputedExpression, other.ComputedExpression, StringComparison.Ordinal)
           && ComputedIsStored == other.ComputedIsStored;
}

public class SnapshotIndex
{
    public string Name { get; set; } = string.Empty;
    public List<string> Columns { get; set; } = new();
    public bool IsUnique { get; set; }
    public string? Predicate { get; set; }
    public List<string>? IncludeColumns { get; set; }
    public string? Method { get; set; }

    public bool HasSameDefinition(SnapshotIndex other)
        => Columns.SequenceEqual(other.Columns, StringComparer.OrdinalIgnoreCase)
           && IsUnique == other.IsUnique
           && string.Equals(Predicate, other.Predicate, StringComparison.Ordinal)
           && (IncludeColumns ?? new List<string>()).SequenceEqual(
               other.IncludeColumns ?? new List<string>(), StringComparer.OrdinalIgnoreCase)
           && string.Equals(Method, other.Method, StringComparison.OrdinalIgnoreCase);
}

public class SnapshotForeignKey
{
    public string Name { get; set; } = string.Empty;
    public List<string> Columns { get; set; } = new();
    public string? PrincipalSchema { get; set; }
    public string PrincipalTable { get; set; } = string.Empty;
    public List<string> PrincipalColumns { get; set; } = new();
    public CascadeAction OnDelete { get; set; }
    public CascadeAction OnUpdate { get; set; }

    public bool HasSameDefinition(SnapshotForeignKey other)
        => Columns.SequenceEqual(other.Columns, StringComparer.OrdinalIgnoreCase)
           && string.Equals(PrincipalSchema, other.PrincipalSchema, StringComparison.OrdinalIgnoreCase)
           && PrincipalTable.EqualsIgnoreCase(other.PrincipalTable)
           && PrincipalColumns.SequenceEqual(other.PrincipalColumns, StringComparer.OrdinalIgnoreCase)
           && OnDelete == other.OnDelete
           && OnUpdate == other.OnUpdate;
}

public class SnapshotCheckConstraint
{
    public string Name { get; set; } = string.Empty;
    public string Expression { get; set; } = string.Empty;
}

public class SnapshotSequence
{
    public string Schema { get; set; } = string.Empty;
    public string Name { get; set; } = string.Empty;
    public long? StartWith { get; set; }
    public long? IncrementBy { get; set; }
}

public class SnapshotRawObject
{
    public string Identifier { get; set; } = string.Empty;
    public string CreateSql { get; set; } = string.Empty;
    public string DropSql { get; set; } = string.Empty;

    public static SnapshotRawObject From(ISchemaObject schemaObject, MigrationOperationTranslationOptions options)
    {
        if (options.Migrator == null)
        {
            throw new InvalidOperationException(
                $"{schemaObject.Identifier.QualifiedName} requires the raw-SQL fallback, so " +
                $"{nameof(MigrationOperationTranslationOptions)}.{nameof(MigrationOperationTranslationOptions.Migrator)} must be provided");
        }

        var create = new StringWriter();
        schemaObject.WriteCreateStatement(options.Migrator, create);
        var drop = new StringWriter();
        schemaObject.WriteDropStatement(options.Migrator, drop);

        return new SnapshotRawObject
        {
            Identifier = schemaObject.Identifier.QualifiedName,
            CreateSql = create.ToString(),
            DropSql = drop.ToString()
        };
    }
}
