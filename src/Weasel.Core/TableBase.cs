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
public abstract class TableBase<TColumn, TIndex, TForeignKey>: SchemaObjectBase, ITable,
    ISchemaObjectWithLocalIdentifiers
    where TColumn : ITableColumn
    where TIndex : ITableIndex
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

    private IList<TableCheckConstraint>? _checkConstraints;

    /// <summary>
    ///     Named table-level CHECK constraints. Emitted in CREATE TABLE and compared during delta
    ///     detection by the providers that support it (PostgreSQL, SQL Server); see
    ///     <see cref="TableCheckConstraint" /> for the conservative comparison semantics.
    /// </summary>
    /// <remarks>
    ///     On a provider that does not emit them this collection refuses everything added to it
    ///     rather than holding a constraint that will never reach the database (weasel#488). See
    ///     <see cref="SupportsCheckConstraints" />.
    /// </remarks>
    public IList<TableCheckConstraint> CheckConstraints
        => _checkConstraints ??= SupportsCheckConstraints
            ? new List<TableCheckConstraint>()
            : new UnsupportedCheckConstraints(ProviderName);

    /// <summary>
    ///     Whether this provider writes check constraints into its DDL and compares them.
    ///     PostgreSQL and SQL Server do; Oracle, MySQL and SQLite do not yet (weasel#488), and
    ///     override this to <c>false</c> so that asking for one is refused rather than ignored.
    /// </summary>
    /// <remarks>
    ///     Read lazily rather than in the constructor, so an override is never called before the
    ///     subclass is built.
    /// </remarks>
    protected virtual bool SupportsCheckConstraints => true;

    /// <summary>
    ///     The provider's name, for the message a refused check constraint carries.
    /// </summary>
    protected virtual string ProviderName => GetType().Namespace?.Split('.').ElementAtOrDefault(1) ?? "this provider";

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

    private string[]? _primaryKeyOrder;

    /// <summary>
    ///     Whether the key's column order was pinned with <see cref="SetPrimaryKeyOrder" />, rather
    ///     than taken from the order the columns were flagged or added in.
    /// </summary>
    public bool HasExplicitPrimaryKeyOrder => _primaryKeyOrder != null;

    /// <summary>
    ///     Apply <see cref="SetPrimaryKeyOrder" /> to the key columns a provider derived for itself.
    /// </summary>
    /// <remarks>
    ///     Every provider's <see cref="PrimaryKeyColumns" /> has to route through this, or a pin set
    ///     on that provider's table is silently ignored. <c>primary_key_order_is_honoured_by_every_provider</c>
    ///     in Weasel.Core.Tests is what stops that being possible to forget.
    ///     <para>
    ///     The pin ORDERS the supplied set rather than replacing it, so flagging or removing a column
    ///     afterwards still takes effect and a pin naming a since-dropped column cannot resurrect it.
    ///     </para>
    /// </remarks>
    protected IReadOnlyList<string> ApplyPrimaryKeyOrder(IReadOnlyList<string> declared)
    {
        var pinned = _primaryKeyOrder;
        if (pinned == null)
        {
            return declared;
        }

        return declared
            .OrderBy(name =>
            {
                for (var i = 0; i < pinned.Length; i++)
                {
                    if (PrimaryKeyColumnComparer.Equals(pinned[i], name))
                    {
                        return i;
                    }
                }

                return int.MaxValue;
            })
            .ToList();
    }

    /// <summary>
    ///     How this provider compares primary key column names. Case-insensitive by default; SQL
    ///     Server overrides it to stay byte-for-byte compatible with the comparison it shipped.
    /// </summary>
    protected virtual StringComparer PrimaryKeyColumnComparer => StringComparer.OrdinalIgnoreCase;

    /// <summary>
    ///     Pin the primary key's column order explicitly, rather than taking the order the columns
    ///     were flagged or added in. Passing an empty list clears the pin.
    /// </summary>
    /// <remarks>
    ///     A table read out of the database carries the order the catalog declares, which for a
    ///     composite key need not match the order the columns appear in the table. A model that only
    ///     flags columns cannot express any other order, so a provider that compares order must
    ///     compare it only when it was pinned here — otherwise every such table reports drift the
    ///     user cannot resolve, and "fixing" it rewrites their key.
    /// </remarks>
    /// <exception cref="ArgumentException">
    ///     The list repeats a column, names one that is not part of the primary key, or covers only
    ///     part of the key. Each produces a migration that drops the existing key and then fails to
    ///     add the replacement, so they are rejected here rather than at the database.
    /// </exception>
    public void SetPrimaryKeyOrder(IEnumerable<string> columnNames)
    {
        var ordered = columnNames.ToArray();
        if (ordered.Length == 0)
        {
            _primaryKeyOrder = null;
            return;
        }

        var duplicates = ordered.GroupBy(x => x, PrimaryKeyColumnComparer)
            .Where(g => g.Count() > 1).Select(g => g.Key).ToArray();
        if (duplicates.Any())
        {
            throw new ArgumentException(
                $"Primary key order for {Identifier} repeats {duplicates.Join(", ")}.", nameof(columnNames));
        }

        // Deliberately the CURRENT key columns, not the pinned ones: the pin is being replaced, and
        // ordering never changes membership, so this is the same set either way.
        var keyColumns = PrimaryKeyColumns.ToArray();

        var unknown = ordered.Where(x => !keyColumns.Contains(x, PrimaryKeyColumnComparer)).ToArray();
        if (unknown.Any())
        {
            throw new ArgumentException(
                $"Primary key order for {Identifier} names {unknown.Join(", ")}, which is not part of the key. The key is: {(keyColumns.Any() ? keyColumns.Join(", ") : "(no columns flagged as primary key)")}.",
                nameof(columnNames));
        }

        // A partial pin would still opt the table into strict order comparison, silently reordering
        // the columns it does not name. An order is only meaningful for the whole key.
        if (ordered.Length != keyColumns.Length)
        {
            throw new ArgumentException(
                $"Primary key order for {Identifier} lists {ordered.Length} of {keyColumns.Length} key columns. Name every column of the key, in order. The key is: {keyColumns.Join(", ")}.",
                nameof(columnNames));
        }

        _primaryKeyOrder = ordered;
    }

    /// <summary>
    ///     Does <paramref name="actualColumns" /> satisfy this table's key, comparing column order
    ///     only when it was pinned with <see cref="SetPrimaryKeyOrder" />?
    /// </summary>
    /// <remarks>
    ///     For the providers that derive the key from flagged columns. PostgreSQL stores its key as
    ///     an explicit list and so can express order natively — it compares positionally and does not
    ///     use this.
    /// </remarks>
    public bool PrimaryKeyOrderMatches(IReadOnlyList<string> actualColumns, StringComparer comparer)
        => HasExplicitPrimaryKeyOrder
            ? PrimaryKeyColumns.SequenceEqual(actualColumns, comparer)
            : PrimaryKeyColumns.OrderBy(x => x, comparer)
                .SequenceEqual(actualColumns.OrderBy(x => x, comparer), comparer);

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
        set => _primaryKeyName = NormalizeIdentifier(value);
    }

    /// <summary>
    ///     The names this table writes into its DDL that are not database objects in their own
    ///     right, and so cannot travel through <see cref="SchemaObjectBase.AllNames" />: every
    ///     column, the primary key constraint name, and every check constraint name.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         The primary key name only appears when the table actually declares a primary key.
    ///         <see cref="PrimaryKeyName" /> falls back to <see cref="DefaultPrimaryKeyName" />
    ///         whenever it has not been set, so reading it unconditionally would validate a name
    ///         that is never emitted — and every provider derives that default from the table name,
    ///         which has already been checked.
    ///     </para>
    ///     <para>
    ///         Index and foreign key names are deliberately absent: they are real named objects and
    ///         belong in <see cref="SchemaObjectBase.AllNames" />, where every provider already
    ///         yields them.
    ///     </para>
    /// </remarks>
    public virtual IEnumerable<string> LocalIdentifiers()
    {
        foreach (var column in Columns) yield return column.Name;

        if (PrimaryKeyColumns.Any()) yield return PrimaryKeyName;

        foreach (var constraint in CheckConstraints) yield return constraint.Name;
    }

    /// <summary>
    ///     Hook for a provider that accepts identifiers the caller has already delimited.
    ///     The model has to hold the name the database will report back, or the two never
    ///     compare equal and the table reports drift on every check. Default is a no-op;
    ///     only SQL Server overrides it today, because only its callers had to bracket
    ///     names themselves to get valid DDL.
    /// </summary>
    protected virtual string NormalizeIdentifier(string name) => name;

    /// <inheritdoc cref="ITable.PreserveIdentifierCase" />
    public bool PreserveIdentifierCase { get; set; }

    /// <inheritdoc cref="ITable.DetectColumnDrift" />
    public bool DetectColumnDrift { get; set; }

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

    IReadOnlyList<ITableColumn> ITable.Columns
        => _columns.OfType<ITableColumn>().ToList();

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

    IReadOnlyList<ITableIndex> ITable.Indexes
        => Indexes.OfType<ITableIndex>().ToList();

    IReadOnlyList<TableCheckConstraint> ITable.CheckConstraints
        => CheckConstraints.ToList();

    TableCheckConstraint ITable.AddCheckConstraint(string name, string expression)
    {
        var constraint = new TableCheckConstraint(name, expression);

        // Throws on a provider that will not emit it -- see UnsupportedCheckConstraints.
        CheckConstraints.Add(constraint);

        return constraint;
    }

    ITableIndex ITable.AddIndex(string name, string[] columnNames, bool isUnique)
    {
        var index = CreateIndexFor(name, columnNames);
        index.IsUnique = isUnique;
        Indexes.Add(index);
        return index;
    }

    /// <summary>
    ///     Factory hook for the provider-specific <c>IndexDefinition</c> type,
    ///     used by <see cref="ITable.AddIndex" />. Subclasses construct their
    ///     index definition with the given name and column list.
    /// </summary>
    protected abstract TIndex CreateIndexFor(string name, string[] columnNames);

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
