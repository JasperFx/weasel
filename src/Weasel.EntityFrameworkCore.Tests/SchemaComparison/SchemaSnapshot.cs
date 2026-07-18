namespace Weasel.EntityFrameworkCore.Tests.SchemaComparison;

/// <summary>
///     Provider-neutral snapshot of one database schema, read back from the
///     database catalog after DDL has been applied. Used to compare the schema
///     EF Core's migration system creates against the schema Weasel creates for
///     the same DbContext model.
/// </summary>
public class SchemaSnapshot
{
    public SchemaSnapshot(string schemaName, IReadOnlyList<TableSnapshot> tables,
        IReadOnlyList<SequenceSnapshot>? sequences = null)
    {
        SchemaName = schemaName;
        Tables = tables;
        Sequences = sequences ?? [];
    }

    public string SchemaName { get; }
    public IReadOnlyList<TableSnapshot> Tables { get; }
    public IReadOnlyList<SequenceSnapshot> Sequences { get; }

    public TableSnapshot? TableFor(string tableName)
        => Tables.FirstOrDefault(t => t.Name.Equals(tableName, StringComparison.OrdinalIgnoreCase));

    public SequenceSnapshot? SequenceFor(string sequenceName)
        => Sequences.FirstOrDefault(s => s.Name.Equals(sequenceName, StringComparison.OrdinalIgnoreCase));
}

public class SequenceSnapshot
{
    public required string Name { get; init; }
    public long StartValue { get; init; }
    public long IncrementBy { get; init; }
}

public class TableSnapshot
{
    public required string Name { get; init; }
    public required IReadOnlyList<ColumnSnapshot> Columns { get; init; }
    public string? PrimaryKeyName { get; init; }
    public IReadOnlyList<string> PrimaryKeyColumns { get; init; } = [];
    public IReadOnlyList<ForeignKeySnapshot> ForeignKeys { get; init; } = [];
    public IReadOnlyList<IndexSnapshot> Indexes { get; init; } = [];
    public IReadOnlyList<CheckConstraintSnapshot> CheckConstraints { get; init; } = [];

    public ColumnSnapshot? ColumnFor(string columnName)
        => Columns.FirstOrDefault(c => c.Name.Equals(columnName, StringComparison.OrdinalIgnoreCase));

    public IndexSnapshot? IndexFor(string indexName)
        => Indexes.FirstOrDefault(i => i.Name.Equals(indexName, StringComparison.OrdinalIgnoreCase));
}

public class ColumnSnapshot
{
    public required string Name { get; init; }

    /// <summary>Formatted store type, e.g. "character varying(200)", "numeric(18,6)", "nvarchar(450)"</summary>
    public required string DataType { get; init; }

    public required bool IsNullable { get; init; }

    /// <summary>Raw default expression text as reported by the catalog, null if none</summary>
    public string? DefaultExpression { get; init; }

    /// <summary>True for identity columns (PG GENERATED ... AS IDENTITY, SQL Server IDENTITY)</summary>
    public bool IsIdentity { get; init; }

    /// <summary>True when the default expression is a sequence nextval() (PG serial-style)</summary>
    public bool IsSerialStyle { get; init; }

    /// <summary>True for computed / generated columns</summary>
    public bool IsComputed { get; init; }
}

public class ForeignKeySnapshot
{
    public required string Name { get; init; }
    public required IReadOnlyList<string> Columns { get; init; }
    public required string PrincipalSchema { get; init; }
    public required string PrincipalTable { get; init; }
    public required IReadOnlyList<string> PrincipalColumns { get; init; }

    /// <summary>Normalized referential action: "NO ACTION", "CASCADE", "SET NULL", "SET DEFAULT", "RESTRICT"</summary>
    public required string OnDelete { get; init; }
}

public class IndexSnapshot
{
    public required string Name { get; init; }

    /// <summary>Key columns in order. Expression columns appear as their expression text.</summary>
    public required IReadOnlyList<string> KeyColumns { get; init; }

    /// <summary>Non-key (INCLUDE) columns</summary>
    public IReadOnlyList<string> IncludedColumns { get; init; } = [];

    public required bool IsUnique { get; init; }

    /// <summary>Per-key-column descending flags, aligned with <see cref="KeyColumns" /></summary>
    public IReadOnlyList<bool> IsDescending { get; init; } = [];

    /// <summary>Partial-index / filter predicate, null if none</summary>
    public string? Predicate { get; init; }

    /// <summary>Index access method (PG: btree/gin/gist/..., SQL Server: CLUSTERED/NONCLUSTERED)</summary>
    public string? Method { get; init; }

    /// <summary>True if the index is the implementation of a PRIMARY KEY or UNIQUE constraint</summary>
    public bool IsConstraintBacked { get; init; }

    /// <summary>True if this is the primary key index (excluded from index comparison)</summary>
    public bool IsPrimaryKey { get; init; }
}

public class CheckConstraintSnapshot
{
    public required string Name { get; init; }
    public required string Expression { get; init; }
}
