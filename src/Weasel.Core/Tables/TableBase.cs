using JasperFx.Core;

namespace Weasel.Core.Tables;

/// <summary>
/// Abstract base class for table definitions that defines common properties and behavior
/// shared between PostgreSQL and SQL Server implementations.
/// </summary>
/// <typeparam name="TColumn">The table column type</typeparam>
/// <typeparam name="TForeignKey">The foreign key type</typeparam>
/// <typeparam name="TIndex">The index definition type</typeparam>
public abstract class TableBase<TColumn, TForeignKey, TIndex>
    where TColumn : class, INamed
    where TForeignKey : ForeignKeyBase
    where TIndex : IndexDefinitionBase
{
    protected readonly List<TColumn> _columns = new();
    protected string? _primaryKeyName;

    protected TableBase(DbObjectName identifier)
    {
        Identifier = identifier ?? throw new ArgumentNullException(nameof(identifier));
    }

    /// <summary>
    /// The qualified identifier (schema.name) for this table
    /// </summary>
    public DbObjectName Identifier { get; protected set; }

    /// <summary>
    /// The columns defined on this table
    /// </summary>
    public IReadOnlyList<TColumn> Columns => _columns;

    /// <summary>
    /// The foreign key constraints defined on this table
    /// </summary>
    public IList<TForeignKey> ForeignKeys { get; } = new List<TForeignKey>();

    /// <summary>
    /// The indexes defined on this table
    /// </summary>
    public IList<TIndex> Indexes { get; } = new List<TIndex>();

    /// <summary>
    /// The names of columns that make up the primary key
    /// </summary>
    public abstract IReadOnlyList<string> PrimaryKeyColumns { get; }

    /// <summary>
    /// The name of the primary key constraint
    /// </summary>
    public string PrimaryKeyName
    {
        get => _primaryKeyName.IsNotEmpty()
            ? _primaryKeyName
            : $"pkey_{Identifier.Name}_{PrimaryKeyColumns.Join("_")}";
        set => _primaryKeyName = value;
    }

    /// <summary>
    /// Finds a column by name
    /// </summary>
    public TColumn? ColumnFor(string columnName)
    {
        return Columns.FirstOrDefault(x => x.Name.EqualsIgnoreCase(columnName));
    }

    /// <summary>
    /// Checks if the table has a column with the specified name
    /// </summary>
    public bool HasColumn(string columnName)
    {
        return Columns.Any(x => x.Name.EqualsIgnoreCase(columnName));
    }

    /// <summary>
    /// Finds an index by name
    /// </summary>
    public TIndex? IndexFor(string indexName)
    {
        return Indexes.FirstOrDefault(x => x.Name == indexName);
    }

    /// <summary>
    /// Checks if the table has an index with the specified name
    /// </summary>
    public bool HasIndex(string indexName)
    {
        return Indexes.Any(x => x.Name == indexName);
    }

    /// <summary>
    /// Removes a column by name
    /// </summary>
    public virtual void RemoveColumn(string columnName)
    {
        _columns.RemoveAll(x => x.Name.EqualsIgnoreCase(columnName));
    }

    /// <summary>
    /// Generates the PRIMARY KEY constraint declaration
    /// </summary>
    public string PrimaryKeyDeclaration()
    {
        return $"CONSTRAINT {PrimaryKeyName} PRIMARY KEY ({PrimaryKeyColumns.Join(", ")})";
    }

    public override string ToString()
    {
        return $"Table: {Identifier}";
    }

    /// <summary>
    /// Generates the CREATE TABLE SQL with default DDL rules.
    /// Useful for quick diagnostics.
    /// </summary>
    public abstract string ToBasicCreateTableSql();

    /// <summary>
    /// Returns all the database object names created by this table
    /// (the table itself plus any indexes and foreign keys)
    /// </summary>
    public abstract IEnumerable<DbObjectName> AllNames();

    /// <summary>
    /// Adds a column to the table
    /// </summary>
    protected void AddColumnInternal(TColumn column)
    {
        _columns.Add(column);
    }
}
