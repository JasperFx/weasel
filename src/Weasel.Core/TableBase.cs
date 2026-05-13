using JasperFx.Core;

namespace Weasel.Core;

/// <summary>
///     Cross-provider base for the five concrete <c>Table</c> classes
///     (PostgreSQL, SQL Server, Oracle, MySQL, SQLite). Owns the parts of the
///     table model that have been re-implemented identically (or nearly so) in
///     every provider: the column / index / foreign-key collections, the
///     navigation helpers (<see cref="ColumnFor" />, <see cref="HasColumn" />,
///     <see cref="IndexFor" />, <see cref="HasIndex" />), the
///     <see cref="MaxIdentifierLength" /> / <see cref="TruncatedNameIdentifier" />
///     pair, and the <see cref="ITable" /> interface boilerplate that wraps
///     <c>AddColumn(name, columnType)</c> / <c>AddColumn(name, dotnetType)</c> /
///     <c>AddForeignKey</c> behind explicit interface implementations.
///     <para>
///     Three pieces stay subclass-controlled because they genuinely differ:
///     <list type="bullet">
///         <item>
///             <see cref="PrimaryKeyColumns" /> — PG and SQLite store the PK
///             columns as an explicit <see cref="List{T}" />; SQL Server, Oracle
///             and MySQL derive them from <c>Columns.Where(IsPrimaryKey)</c>.
///             Both shapes are preserved via this abstract property.
///         </item>
///         <item>
///             <see cref="DefaultPrimaryKeyName" /> — providers spell the
///             auto-generated PK constraint name differently
///             (<c>pkey_{name}_{cols}</c> on PG / SS, <c>pk_{name}_{cols}</c> on
///             Oracle / MySQL, <c>pk_{name}</c> on SQLite).
///         </item>
///         <item>
///             <see cref="WriteCreateStatement" /> / <see cref="WriteDropStatement" />
///             — the CREATE / DROP algorithm itself; step 8 introduced
///             <see cref="IDdlSyntaxStrategy" /> to start routing the
///             syntax-only parts through a pluggable strategy, with PG and SQLite
///             wired in as the prototype.
///         </item>
///     </list>
///     </para>
///     <para>
///     The audit at #270 predicted ~600–700 LOC of removable duplication from
///     <c>Table.cs</c> alone. This step lifts the high-confidence shared state
///     and helpers; the remaining <c>WriteCreateStatement</c> body lifts in a
///     follow-up when the strategy interface has settled across all five
///     providers.
///     </para>
/// </summary>
/// <typeparam name="TColumn">
///     The provider's concrete <c>TableColumn</c> type (each provider currently
///     defines its own; #270 step 10 may unify these under a
///     <c>TableColumnBase</c>).
/// </typeparam>
/// <typeparam name="TIndex">
///     The provider's concrete <c>IndexDefinition</c> type. <see cref="INamed" />
///     is the lowest common denominator the navigation helpers need.
/// </typeparam>
/// <typeparam name="TForeignKey">
///     The provider's concrete <c>ForeignKey</c> type, constrained to
///     <see cref="ForeignKeyBase" /> so the <see cref="ITable.ForeignKeys" />
///     contravariance works.
/// </typeparam>
public abstract class TableBase<TColumn, TIndex, TForeignKey>: SchemaObjectBase, ITable
    where TColumn : ITableColumn
    where TIndex : INamed
    where TForeignKey : ForeignKeyBase
{
    protected readonly List<TColumn> _columns = new();
    private string? _primaryKeyName;

    protected TableBase(DbObjectName identifier) : base(identifier)
    {
    }

    public IReadOnlyList<TColumn> Columns => _columns;
    public IList<TForeignKey> ForeignKeys { get; } = new List<TForeignKey>();
    public IList<TIndex> Indexes { get; } = new List<TIndex>();

    /// <summary>
    ///     Names of indexes that this table intentionally ignores during delta
    ///     comparison — useful when a third party (e.g. <c>pg_partman</c> on
    ///     PostgreSQL, an external migration tool) owns those indexes and
    ///     Weasel should not try to drop or recreate them. Previously a
    ///     PostgreSQL- and SQLite-only property; lifted here for uniform
    ///     access. SS / Oracle / MySQL inherit an empty set, which is a no-op
    ///     until they need the feature.
    /// </summary>
    public ISet<string> IgnoredIndexes { get; } = new HashSet<string>();

    /// <inheritdoc cref="ITable.PrimaryKeyColumns" />
    public abstract IReadOnlyList<string> PrimaryKeyColumns { get; }

    /// <summary>
    ///     Max identifier length supported by the underlying engine. PostgreSQL
    ///     defaults to 63, SQL Server to 128, Oracle 12c+ to 128, MySQL to 64,
    ///     SQLite is effectively unlimited but 64 is a sensible practical cap.
    ///     Subclasses adjust via the public setter if needed.
    /// </summary>
    public int MaxIdentifierLength { get; set; } = 63;

    /// <summary>
    ///     Truncate a candidate identifier to at most <see cref="MaxIdentifierLength" />
    ///     characters. Used by the partition / index / FK naming helpers to
    ///     stay within engine limits.
    /// </summary>
    public string TruncatedNameIdentifier(string nameIdentifier)
        => nameIdentifier.Substring(0, Math.Min(MaxIdentifierLength, nameIdentifier.Length));

    public string PrimaryKeyName
    {
        get => _primaryKeyName.IsNotEmpty() ? _primaryKeyName : DefaultPrimaryKeyName();
        set => _primaryKeyName = value;
    }

    /// <summary>
    ///     Provider-specific default for the auto-generated primary-key
    ///     constraint name. PG / SS use <c>pkey_{name}_{cols}</c>, Oracle /
    ///     MySQL use <c>pk_{name}_{cols}</c>, SQLite uses <c>pk_{name}</c>.
    /// </summary>
    protected abstract string DefaultPrimaryKeyName();

    /// <summary>
    ///     How column / index / FK names are compared during lookup. PostgreSQL,
    ///     SQL Server, Oracle and MySQL use <see cref="StringComparison.Ordinal" />;
    ///     SQLite overrides to <see cref="StringComparison.OrdinalIgnoreCase" />
    ///     because SQLite identifiers are case-folded.
    /// </summary>
    protected virtual StringComparison NameComparison => StringComparison.Ordinal;

    public TColumn? ColumnFor(string columnName)
        => Columns.FirstOrDefault(x => x.Name.Equals(columnName, NameComparison));

    public bool HasColumn(string columnName)
        => Columns.Any(x => x.Name.Equals(columnName, NameComparison));

    public TIndex? IndexFor(string indexName)
        => Indexes.FirstOrDefault(x => x.Name.Equals(indexName, NameComparison));

    public bool HasIndex(string indexName)
        => Indexes.Any(x => x.Name.Equals(indexName, NameComparison));

    public bool HasIgnoredIndex(string indexName)
        => IgnoredIndexes.Contains(indexName);

    public void IgnoreIndex(string indexName)
    {
        if (HasIndex(indexName))
        {
            throw new ArgumentException($"Cannot ignore defined index {indexName} on table {Identifier}");
        }
        IgnoredIndexes.Add(indexName);
    }

    /// <summary>
    ///     Remove a column by name. Always case-insensitive — every concrete
    ///     provider used <c>EqualsIgnoreCase</c> in its own implementation, so
    ///     the base preserves that, distinct from the case-sensitivity of
    ///     <see cref="HasColumn" /> / <see cref="ColumnFor" /> which is per-
    ///     provider via <see cref="NameComparison" />.
    /// </summary>
    public virtual void RemoveColumn(string columnName)
    {
        _columns.RemoveAll(x => x.Name.EqualsIgnoreCase(columnName));
    }

    public override string ToString() => $"Table: {Identifier}";

    /// <summary>
    ///     Generate the CREATE TABLE DDL with the provider's default formatting
    ///     ("concise"). Useful for diagnostics and tests.
    /// </summary>
    public string ToBasicCreateTableSql()
    {
        var writer = new StringWriter();
        var rules = GetDefaultMigratorForBasicSql();
        WriteCreateStatement(rules, writer);
        return writer.ToString();
    }

    /// <summary>
    ///     Provider-specific concise <see cref="Migrator" /> for
    ///     <see cref="ToBasicCreateTableSql" />.
    /// </summary>
    protected abstract Migrator GetDefaultMigratorForBasicSql();

    // ---- ITable explicit interface implementations -------------------------
    //
    // These wrap the provider's typed AddColumn / AddForeignKey via abstract
    // hooks so the ITable surface is implemented exactly once here and providers
    // only specialise the type-resolution + factory calls.

    IReadOnlyList<ForeignKeyBase> ITable.ForeignKeys
        => ForeignKeys.Cast<ForeignKeyBase>().ToList();

    ForeignKeyBase ITable.AddForeignKey(string name, DbObjectName linkedTable, string[] columnNames, string[] linkedColumnNames)
    {
        var fk = CreateForeignKey(name);
        fk.LinkedTable = linkedTable;
        fk.ColumnNames = columnNames;
        fk.LinkedNames = linkedColumnNames;
        ForeignKeys.Add(fk);
        return fk;
    }

    ITableColumn ITable.AddColumn(string name, string columnType)
        => AddColumnAndReturn(name, columnType);

    ITableColumn ITable.AddColumn(string name, Type dotnetType)
        => AddColumnAndReturn(name, GetDatabaseTypeFor(dotnetType));

    ITableColumn ITable.AddPrimaryKeyColumn(string name, string columnType)
        => AddPrimaryKeyColumnAndReturn(name, columnType);

    ITableColumn ITable.AddPrimaryKeyColumn(string name, Type dotnetType)
        => AddPrimaryKeyColumnAndReturn(name, GetDatabaseTypeFor(dotnetType));

    /// <summary>
    ///     Factory hook for the provider-specific <c>ForeignKey</c> subclass.
    ///     Used by <see cref="ITable.AddForeignKey" />. Subclasses just
    ///     <c>=&gt; new ForeignKey(name)</c>.
    /// </summary>
    protected abstract TForeignKey CreateForeignKey(string name);

    /// <summary>
    ///     Add a column with a fully-resolved provider-specific type string and
    ///     return the typed column (the provider's <c>AddColumn(...)</c> path
    ///     adds the column to <see cref="Columns" /> and returns the column
    ///     wrapped inside its <c>ColumnExpression</c>; this hook unwraps to
    ///     the column itself for the <see cref="ITable" /> contract).
    /// </summary>
    protected abstract ITableColumn AddColumnAndReturn(string name, string columnType);

    /// <summary>
    ///     Same as <see cref="AddColumnAndReturn" /> but immediately flags the
    ///     column as a primary key.
    /// </summary>
    protected abstract ITableColumn AddPrimaryKeyColumnAndReturn(string name, string columnType);

    /// <summary>
    ///     Resolve a .NET type to the provider-specific database type string
    ///     (e.g. <c>typeof(Guid)</c> → <c>"uuid"</c> on PG, <c>"UNIQUEIDENTIFIER"</c>
    ///     on SS). Subclasses route to their <c>Provider.Instance.GetDatabaseType</c>.
    /// </summary>
    protected abstract string GetDatabaseTypeFor(Type dotnetType);
}
