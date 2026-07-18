using System.Globalization;
using JasperFx.Core;
using Microsoft.Data.SqlClient;
using Weasel.Core;

namespace Weasel.SqlServer.Tables;

public enum PartitionStrategy
{
    /// <summary>
    ///     No partitioning
    /// </summary>
    None,

    /// <summary>
    ///     Postgresql PARTITION BY RANGE semantics
    /// </summary>
    Range


    //List
}


public partial class Table: TableBase<TableColumn, IndexDefinition, ForeignKey>
{
    public Table(DbObjectName name)
        : base(name ?? throw new ArgumentNullException(nameof(name)))
    {
    }

    public Table(string tableName): this(DbObjectName.Parse(SqlServerProvider.Instance, tableName))
    {
    }

    /// <inheritdoc />
    /// <remarks>
    ///     SQL Server derives the primary key column list from
    ///     <see cref="TableBase{TColumn,TIndex,TForeignKey}.Columns" /> rather
    ///     than storing it separately, so the list refreshes automatically as
    ///     columns are flagged with <c>IsPrimaryKey</c>.
    /// </remarks>
    public override IReadOnlyList<string> PrimaryKeyColumns =>
        _columns.Where(x => x.IsPrimaryKey).Select(x => x.Name).ToList();

    /// <inheritdoc />
    protected override string DefaultPrimaryKeyName()
        => $"pkey_{Identifier.Name}_{PrimaryKeyColumns.Join("_")}";

    /// <inheritdoc />
    protected override ForeignKey CreateForeignKey(string name) => new ForeignKey(name);

    protected override IndexDefinition CreateIndexFor(string name, string[] columnNames)
        => new IndexDefinition(name) { Columns = columnNames };

    /// <inheritdoc />
    protected override ITableColumn AddColumnAndReturn(string name, string columnType)
        => AddColumn(name, columnType).Column;

    /// <inheritdoc />
    protected override ITableColumn AddPrimaryKeyColumnAndReturn(string name, string columnType)
        => AddColumn(name, columnType).AsPrimaryKey().Column;

    /// <inheritdoc />
    protected override string GetDatabaseTypeFor(Type dotnetType)
        => SqlServerProvider.Instance.GetDatabaseType(dotnetType, EnumStorage.AsInteger);

    /// <inheritdoc />
    protected override Migrator GetDefaultMigratorForBasicSql()
        => new SqlServerMigrator { Formatting = SqlFormatting.Concise };

    /// <inheritdoc />
    /// <remarks>SQL Server identifier comparison is case-insensitive by default.</remarks>
    protected override StringComparison NameComparison => StringComparison.OrdinalIgnoreCase;

    public IList<string> PartitionExpressions { get; } = new List<string>();


    /// <summary>
    ///     PARTITION strategy for this table
    /// </summary>
    public PartitionStrategy PartitionStrategy { get; set; } = PartitionStrategy.None;

    /// <summary>
    ///     SQL Server partitioning configuration (partition function + scheme).
    ///     When set, the table will be created ON the partition scheme.
    /// </summary>
    public Partitioning.ISqlServerPartitioning? SqlServerPartitioning { get; set; }

    /// <summary>
    ///     The actual RANGE partitioning metadata read back from the database by
    ///     <see cref="FetchExistingAsync" />. Null when the table is not partitioned. Consumed by
    ///     <see cref="TableDelta" /> to tell an additive boundary change (which migrates via
    ///     <c>ALTER PARTITION FUNCTION ... SPLIT RANGE</c>) apart from a partitioning rebuild.
    /// </summary>
    public Partitioning.SqlServerPartitionInfo? PartitionInfo { get; set; }

    /// <summary>
    ///     Configure SQL Server RANGE partitioning on a column.
    /// </summary>
    public Partitioning.RangePartitioning PartitionByRange(string column, string sqlDataType)
    {
        var partitioning = new Partitioning.RangePartitioning(column, sqlDataType);
        SqlServerPartitioning = partitioning;
        return partitioning;
    }

    /// <summary>
    ///     Configure SQL Server partitioning driven by a runtime-managed tenant
    ///     registry. Mirrors PostgreSQL <c>ManagedListPartitions</c>: tenants are
    ///     allocated integer ordinals at sign-up, the table is partitioned on the
    ///     ordinal column via <c>RANGE RIGHT</c>, and the partition function is
    ///     SPLIT each time a new tenant lands. See
    ///     <see cref="Partitioning.ManagedTenantPartitions" />. Weasel #301.
    /// </summary>
    /// <param name="manager">
    ///     The shared tenant-partition manager. The same instance is typically
    ///     assigned to every partitioned tenant table in the database, so a
    ///     single tenant registration splits all tables in one call.
    /// </param>
    public Partitioning.ManagedTenantPartitions PartitionByManagedTenants(
        Partitioning.ManagedTenantPartitions manager)
    {
        SqlServerPartitioning = manager ?? throw new ArgumentNullException(nameof(manager));
        return manager;
    }

    public override void WriteCreateStatement(Migrator migrator, TextWriter writer)
    {
        // Write partition function and scheme DDL before the table if partitioning is configured
        if (SqlServerPartitioning != null)
        {
            SqlServerPartitioning.WritePartitionDdl(writer, this);
        }

        if (migrator.TableCreation == CreationStyle.DropThenCreate)
        {
            // drop all FK constraints
            var sqlVariableName = $"@sql_{Guid.NewGuid().ToString().ToLowerInvariant().Replace("-", "_")}";
            writer.WriteLine("DECLARE {0} NVARCHAR(MAX) = '';", sqlVariableName);
            writer.WriteLine("SELECT {0} = {1} + 'ALTER TABLE ' + QUOTENAME(OBJECT_SCHEMA_NAME(fk.parent_object_id)) + '.' + QUOTENAME(OBJECT_NAME(fk.parent_object_id)) + ' DROP CONSTRAINT ' + QUOTENAME(fk.name) + ';'",
                sqlVariableName, sqlVariableName);
            writer.WriteLine("FROM sys.foreign_keys AS fk");
            writer.WriteLine("WHERE fk.referenced_object_id = OBJECT_ID('{0}');", Identifier);
            writer.WriteLine("EXEC sp_executesql {0};", sqlVariableName);

            writer.WriteLine("DROP TABLE IF EXISTS {0};", Identifier);
            writer.WriteLine("CREATE TABLE {0} (", Identifier);
        }
        else
        {
            writer.WriteLine("IF OBJECT_ID('{0}') IS NULL", Identifier);
            writer.WriteLine("BEGIN");
            writer.WriteLine("CREATE TABLE {0} (", Identifier);
        }

        if (migrator.Formatting == SqlFormatting.Pretty)
        {
            var columnLength = Columns.Max(x => x.QuotedName.Length) + 4;
            var typeLength = Columns.Max(x => x.Type.Length) + 4;

            var lines = Columns.Select(column =>
                    $"    {column.QuotedName.PadRight(columnLength)}{column.Type.PadRight(typeLength)}{column.Declaration()}")
                .ToList();

            if (PrimaryKeyColumns.Any())
            {
                lines.Add(PrimaryKeyDeclaration());
            }

            for (var i = 0; i < lines.Count - 1; i++)
            {
                writer.WriteLine(lines[i] + ",");
            }

            writer.WriteLine(lines.Last());
        }
        else
        {
            var lines = Columns
                .Select(column => column.ToDeclaration())
                .ToList();

            if (PrimaryKeyColumns.Any())
            {
                lines.Add(PrimaryKeyDeclaration());
            }

            for (var i = 0; i < lines.Count - 1; i++)
            {
                writer.WriteLine(lines[i] + ",");
            }
            writer.WriteLine(lines.Last());
        }

        if (SqlServerPartitioning != null)
        {
            writer.Write(")");
            SqlServerPartitioning.WriteOnClause(writer, this);
            writer.WriteLine(";");
        }
        else
        {
            switch (PartitionStrategy)
            {
                case PartitionStrategy.None:
                    writer.WriteLine(");");
                    break;

                case PartitionStrategy.Range:
                    writer.WriteLine($") PARTITION BY RANGE ({PartitionExpressions.Join(", ")});");
                    break;
            }
        }

        if (migrator.TableCreation != CreationStyle.DropThenCreate)
        {
            writer.WriteLine("END");
        }


        // TODO -- support OriginWriter
        //writer.WriteLine(OriginWriter.OriginStatement("TABLE", Identifier.QualifiedName));

        foreach (var foreignKey in ForeignKeys)
        {
            writer.WriteLine();
            writer.WriteLine(foreignKey.ToDDL(this));
        }


        foreach (var index in Indexes)
        {
            writer.WriteLine();
            writer.WriteLine(index.ToDDL(this));
        }
    }

    public override void WriteDropStatement(Migrator rules, TextWriter writer)
    {
        writer.WriteLine($"DROP TABLE IF EXISTS {Identifier};");
    }

    public override IEnumerable<DbObjectName> AllNames()
    {
        yield return Identifier;

        foreach (var index in Indexes) yield return new SqlServerObjectName(Identifier.Schema, index.Name);

        foreach (var fk in ForeignKeys) yield return new SqlServerObjectName(Identifier.Schema, fk.Name);
    }

    internal string PrimaryKeyDeclaration()
    {
        return $"CONSTRAINT {PrimaryKeyName} PRIMARY KEY ({PrimaryKeyColumns.Join(", ")})";
    }

    public ColumnExpression AddColumn(TableColumn column)
    {
        _columns.Add(column);
        column.Parent = this;

        return new ColumnExpression(this, column);
    }

    public ColumnExpression AddColumn(string columnName, string columnType)
    {
        var column = new TableColumn(columnName, columnType) { Parent = this };
        return AddColumn(column);
    }

    public ColumnExpression AddColumn<T>() where T : TableColumn, new()
    {
        var column = new T();
        return AddColumn(column);
    }

    public ColumnExpression AddColumn<T>(string columnName)
    {
        if (typeof(T).IsEnum)
        {
            throw new InvalidOperationException(
                "Database column types cannot be automatically derived for enums. Explicitly specify as varchar or integer");
        }

        var type = SqlServerProvider.Instance.GetDatabaseType(typeof(T), EnumStorage.AsInteger);
        return AddColumn(columnName, type);
    }

    public async Task<bool> ExistsInDatabaseAsync(SqlConnection conn, CancellationToken ct = default)
    {
        var cmd = conn
            .CreateCommand(
                "SELECT * FROM information_schema.tables WHERE table_name = @table AND table_schema = @schema;")
            .With("table", Identifier.Name)
            .With("schema", Identifier.Schema);

        await using var reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
        {
            var any = await reader.ReadAsync(ct).ConfigureAwait(false);
            await reader.CloseAsync().ConfigureAwait(false);
            return any;
        }
    }

    public ColumnExpression ModifyColumn(string columnName)
    {
        var column = ColumnFor(columnName) ??
                     throw new ArgumentOutOfRangeException(
                         $"Column '{columnName}' does not exist in table {Identifier}");
        return new ColumnExpression(this, column);
    }

    public void PartitionByRange(params string[] columnOrExpressions)
    {
        PartitionStrategy = PartitionStrategy.Range;
        PartitionExpressions.Clear();
        PartitionExpressions.AddRange(columnOrExpressions);
    }

    public void ClearPartitions()
    {
        PartitionStrategy = PartitionStrategy.None;
        PartitionExpressions.Clear();
    }

    public ForeignKey FindOrCreateForeignKey(string fkName)
    {
        var fk = ForeignKeys.FirstOrDefault(x => x.Name == fkName);
        if (fk == null)
        {
            fk = new ForeignKey(fkName);
            ForeignKeys.Add(fk);
        }

        return fk;
    }

    public class ColumnExpression
    {
        private readonly Table _parent;

        public ColumnExpression(Table parent, TableColumn column)
        {
            _parent = parent;
            Column = column;
        }

        internal TableColumn Column { get; }

        public ColumnExpression ForeignKeyTo(string referencedTableName, string referencedColumnName,
            string? fkName = null, CascadeAction onDelete = CascadeAction.NoAction,
            CascadeAction onUpdate = CascadeAction.NoAction)
        {
            return ForeignKeyTo(DbObjectName.Parse(SqlServerProvider.Instance, referencedTableName),
                referencedColumnName, fkName, onDelete,
                onUpdate);
        }

        public ColumnExpression ForeignKeyTo(Table referencedTable, string referencedColumnName,
            string? fkName = null, CascadeAction onDelete = CascadeAction.NoAction,
            CascadeAction onUpdate = CascadeAction.NoAction)
        {
            return ForeignKeyTo(referencedTable.Identifier, referencedColumnName, fkName, onDelete, onUpdate);
        }

        public ColumnExpression ForeignKeyTo(DbObjectName referencedIdentifier, string referencedColumnName,
            string? fkName = null, CascadeAction onDelete = CascadeAction.NoAction,
            CascadeAction onUpdate = CascadeAction.NoAction)
        {
            var fk = new ForeignKey(fkName ?? _parent.Identifier.ToIndexName("fkey", Column.Name))
            {
                LinkedTable = referencedIdentifier,
                ColumnNames = new[] { Column.Name },
                LinkedNames = new[] { referencedColumnName },
                OnDelete = onDelete,
                OnUpdate = onUpdate
            };

            _parent.ForeignKeys.Add(fk);

            return this;
        }

        // ---- Core.CascadeAction overloads (resolves #261) -----------------
        //
        // Siblings of the three overloads above that take Weasel.Core.CascadeAction
        // directly, so callers can opt out of the [Obsolete]
        // Weasel.SqlServer.CascadeAction enum. No defaults to avoid call-site
        // ambiguity with the existing local-enum overloads — callers pass all
        // three trailing arguments explicitly when using Core.

        /// <summary>
        ///     Configure a foreign key from this column using <see cref="Core.CascadeAction" />.
        ///     Provided alongside the <c>[Obsolete]</c> <c>Weasel.SqlServer.CascadeAction</c>
        ///     overload so callers can migrate to the canonical cross-provider enum.
        /// </summary>
        public ColumnExpression ForeignKeyTo(string referencedTableName, string referencedColumnName,
            string? fkName, Core.CascadeAction onDelete, Core.CascadeAction onUpdate)
        {
            return ForeignKeyTo(DbObjectName.Parse(SqlServerProvider.Instance, referencedTableName),
                referencedColumnName, fkName, onDelete, onUpdate);
        }

        /// <inheritdoc cref="ForeignKeyTo(string, string, string?, Core.CascadeAction, Core.CascadeAction)" />
        public ColumnExpression ForeignKeyTo(Table referencedTable, string referencedColumnName,
            string? fkName, Core.CascadeAction onDelete, Core.CascadeAction onUpdate)
        {
            return ForeignKeyTo(referencedTable.Identifier, referencedColumnName, fkName, onDelete, onUpdate);
        }

        /// <inheritdoc cref="ForeignKeyTo(string, string, string?, Core.CascadeAction, Core.CascadeAction)" />
        public ColumnExpression ForeignKeyTo(DbObjectName referencedIdentifier, string referencedColumnName,
            string? fkName, Core.CascadeAction onDelete, Core.CascadeAction onUpdate)
        {
            var fk = new ForeignKey(fkName ?? _parent.Identifier.ToIndexName("fkey", Column.Name))
            {
                LinkedTable = referencedIdentifier,
                ColumnNames = new[] { Column.Name },
                LinkedNames = new[] { referencedColumnName },
                DeleteAction = onDelete,
                UpdateAction = onUpdate
            };

            _parent.ForeignKeys.Add(fk);

            return this;
        }

        /// <summary>
        ///     Marks this column as being part of the parent table's primary key
        /// </summary>
        /// <returns></returns>
        public ColumnExpression AsPrimaryKey()
        {
            Column.IsPrimaryKey = true;
            Column.AllowNulls = false;
            return this;
        }

        public ColumnExpression AllowNulls()
        {
            Column.AllowNulls = true;
            return this;
        }

        public ColumnExpression NotNull()
        {
            Column.AllowNulls = false;
            return this;
        }

        public ColumnExpression AddIndex(Action<IndexDefinition>? configure = null)
        {
            var index = new IndexDefinition(_parent.Identifier.ToIndexName("idx", Column.Name))
            {
                Columns = new[] { Column.Name }
            };

            _parent.Indexes.Add(index);

            configure?.Invoke(index);

            return this;
        }

        /// <summary>
        ///     Mark this column as an auto-incrementing identity column. SQL Server
        ///     renders this as <c>IDENTITY(1,1)</c>.
        ///     <para>
        ///     Canonical cross-provider spelling — every provider's
        ///     <c>ColumnExpression</c> exposes <c>AutoIncrement()</c> with
        ///     provider-appropriate SQL emission (#270 step 10).
        ///     </para>
        /// </summary>
        public ColumnExpression AutoIncrement()
        {
            Column.IsAutoNumber = true;
            return this;
        }

        /// <summary>
        ///     Historical SQL Server-specific spelling for <see cref="AutoIncrement" />.
        ///     Kept as an alias for backward compatibility.
        /// </summary>
        [Obsolete("Use AutoIncrement() — the cross-provider canonical name. AutoNumber() will be removed in a future major.")]
        public ColumnExpression AutoNumber() => AutoIncrement();

        public ColumnExpression DefaultValueByString(string value)
        {
            return DefaultValueByExpression($"'{value}'");
        }

        public ColumnExpression DefaultValue(int value)
        {
            return DefaultValueByExpression(value.ToString());
        }

        public ColumnExpression DefaultValue(long value)
        {
            return DefaultValueByExpression(value.ToString());
        }

        public ColumnExpression DefaultValue(double value)
        {
            return DefaultValueByExpression(value.ToString(CultureInfo.InvariantCulture));
        }

        public ColumnExpression DefaultValueFromSequence(Sequence sequence)
        {
            return DefaultValueFromSequence(sequence.Identifier);
        }

        public ColumnExpression DefaultValueFromSequence(DbObjectName sequenceName)
        {
            return DefaultValueByExpression($"next value for {sequenceName.QualifiedName}");
        }

        public ColumnExpression DefaultValueByExpression(string expression)
        {
            Column.DefaultExpression = expression;

            return this;
        }

        public ColumnExpression PartitionByRange()
        {
            _parent.PartitionStrategy = PartitionStrategy.Range;
            _parent.PartitionExpressions.Add(Column.Name);

            Column.IsPrimaryKey = true;

            return this;
        }
    }
}
