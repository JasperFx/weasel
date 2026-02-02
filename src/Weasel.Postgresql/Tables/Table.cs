using System.Globalization;
using JasperFx.Core;
using Npgsql;
using Weasel.Core;
using Weasel.Postgresql.Tables.Indexes;
using Weasel.Postgresql.Tables.Partitioning;

namespace Weasel.Postgresql.Tables;

public partial class Table: ISchemaObjectWithPostProcessing, ITable
{
    private readonly List<TableColumn> _columns = new();

    private readonly List<string> _primaryKeyColumns = new();

    private string? _primaryKeyName;

    public Table(DbObjectName name)
    {
        Identifier = name ?? throw new ArgumentNullException(nameof(name));
    }

    public Table(string tableName): this(DbObjectName.Parse(PostgresqlProvider.Instance, tableName))
    {
    }

    ITableColumn ITable.AddColumn(string name, string columnType)
    {
        var expression = AddColumn(name, columnType);
        return expression.Column;
    }

    ITableColumn ITable.AddColumn(string name, Type dotnetType)
    {
        var type = PostgresqlProvider.Instance.GetDatabaseType(dotnetType, EnumStorage.AsInteger);
        var expression = AddColumn(name, type);
        return expression.Column;
    }

    ITableColumn ITable.AddPrimaryKeyColumn(string name, string columnType)
    {
        var expression = AddColumn(name, columnType).AsPrimaryKey();
        return expression.Column;
    }

    ITableColumn ITable.AddPrimaryKeyColumn(string name, Type dotnetType)
    {
        var type = PostgresqlProvider.Instance.GetDatabaseType(dotnetType, EnumStorage.AsInteger);
        var expression = AddColumn(name, type).AsPrimaryKey();
        return expression.Column;
    }

    /// <summary>
    ///     Default is false. If true, Weasel assumes that *something* else like pg_partman is controlling
    ///     the database partitions outside of Weasel control
    /// </summary>
    public bool IgnorePartitionsInMigration { get; set; }

    public IReadOnlyList<TableColumn> Columns => _columns;

    public IList<ForeignKey> ForeignKeys { get; } = new List<ForeignKey>();
    public IList<IndexDefinition> Indexes { get; } = new List<IndexDefinition>();
    public ISet<string> IgnoredIndexes { get; } = new HashSet<string>();

    /// <summary>
    ///     Max identifier length for identifiers like table name, column name, constraints, primary key etc
    /// </summary>
    public int MaxIdentifierLength { get; set; } = 63;

    public IReadOnlyList<string> PrimaryKeyColumns => _primaryKeyColumns;

    public string PrimaryKeyName
    {
        get => _primaryKeyName.IsNotEmpty() ? _primaryKeyName : $"pkey_{Identifier.Name}_{PrimaryKeyColumns.Join("_")}";
        set => _primaryKeyName = value;
    }

    public void WriteCreateStatement(Migrator migrator, TextWriter writer)
    {
        if (migrator.TableCreation == CreationStyle.DropThenCreate)
        {
            writer.WriteLine("DROP TABLE IF EXISTS {0} CASCADE;", Identifier);
            writer.WriteLine("CREATE TABLE {0} (", Identifier);
        }
        else
        {
            writer.WriteLine("CREATE TABLE IF NOT EXISTS {0} (", Identifier);
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

        if (Partitioning != null)
        {
            Partitioning.WritePartitionBy(writer);
        }
        else
        {
            writer.WriteLine(");");
        }

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

        if (Partitioning != null)
        {
            writer.WriteLine();
            Partitioning.WriteCreateStatement(writer, this);
        }
    }

    public void WriteDropStatement(Migrator rules, TextWriter writer)
    {
        writer.WriteLine($"DROP TABLE IF EXISTS {Identifier} CASCADE;");
    }

    public DbObjectName Identifier { get; private set; }


    public IEnumerable<DbObjectName> AllNames()
    {
        yield return Identifier;

        foreach (var index in Indexes) yield return new PostgresqlObjectName(Identifier.Schema, index.Name);

        foreach (var fk in ForeignKeys) yield return new PostgresqlObjectName(Identifier.Schema, fk.Name);
    }

    public void PostProcess(ISchemaObject[] allObjects)
    {
        if (!ForeignKeys.Any())
        {
            return;
        }

        foreach (var key in ForeignKeys)
        {
            var matching = allObjects.OfType<Table>().FirstOrDefault(x => x.Identifier.Equals(key.LinkedTable));
            if (matching != null)
            {
                key.TryToCorrectForLink(this, matching);
            }
        }
    }

    public override string ToString()
    {
        return $"Table: {Identifier}";
    }

    internal void ReadPrimaryKeyColumns(List<string> pks)
    {
        _primaryKeyColumns.Clear();
        _primaryKeyColumns.AddRange(pks);
    }

    /// <summary>
    ///     Mutate this table to change the identifier to being in a different schema
    /// </summary>
    /// <param name="schemaName"></param>
    public void MoveToSchema(string schemaName)
    {
        var identifier = new PostgresqlObjectName(schemaName, Identifier.Name);
        Identifier = identifier;
    }

    /// <summary>
    ///     Generate the CREATE TABLE SQL expression with default
    ///     DDL rules. This is useful for quick diagnostics
    /// </summary>
    /// <returns></returns>
    public string ToBasicCreateTableSql()
    {
        var writer = new StringWriter();
        var rules = new PostgresqlMigrator { Formatting = SqlFormatting.Concise };
        WriteCreateStatement(rules, writer);

        return writer.ToString();
    }


    internal string PrimaryKeyDeclaration()
    {
        return $"CONSTRAINT {PrimaryKeyName} PRIMARY KEY ({PrimaryKeyColumns.Join(", ")})";
    }

    public TableColumn? ColumnFor(string columnName)
    {
        return Columns.FirstOrDefault(x => x.Name == columnName);
    }


    public bool HasColumn(string columnName)
    {
        return Columns.Any(x => x.Name == columnName);
    }

    public IndexDefinition? IndexFor(string indexName)
    {
        return Indexes.FirstOrDefault(x => x.Name == indexName);
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

        var type = PostgresqlProvider.Instance.GetDatabaseType(typeof(T), EnumStorage.AsInteger);
        return AddColumn(columnName, type);
    }

    public async Task<bool> ExistsInDatabaseAsync(NpgsqlConnection conn, CancellationToken ct = default)
    {
        var cmd = conn
            .CreateCommand(
                "SELECT * FROM information_schema.tables WHERE table_name = :table AND table_schema = :schema;")
            .With("table", Identifier.Name)
            .With("schema", Identifier.Schema);

        await using var reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
        var any = await reader.ReadAsync(ct).ConfigureAwait(false);
        await reader.CloseAsync().ConfigureAwait(false);
        return any;
    }

    public string TruncatedNameIdentifier(string nameIdentifier)
    {
        return nameIdentifier.Substring(0, Math.Min(MaxIdentifierLength, nameIdentifier.Length));
    }

    public void RemoveColumn(string columnName)
    {
        _columns.RemoveAll(x => x.Name.EqualsIgnoreCase(columnName));
        _primaryKeyColumns.Remove(columnName);
    }

    public ColumnExpression ModifyColumn(string columnName)
    {
        var column = ColumnFor(columnName) ??
                     throw new ArgumentOutOfRangeException(
                         $"Column '{columnName}' does not exist in table {Identifier}");
        return new ColumnExpression(this, column);
    }

    public void IgnoreIndex(string indexName)
    {
        if (Indexes.Any(idx => idx.Name == indexName))
        {
            throw new ArgumentException($"Cannot ignore defined index {indexName} on table {Identifier}");
        }

        IgnoredIndexes.Add(indexName);
    }

    public bool HasIndex(string indexName)
    {
        return Indexes.Any(x => x.Name == indexName);
    }

    public bool HasIgnoredIndex(string indexName)
    {
        return IgnoredIndexes.Contains(indexName);
    }

    public IEnumerable<string> PartitionTableNames()
    {
        if (Partitioning == null)
        {
            yield break;
        }

        foreach (var tableName in Partitioning.PartitionTableNames(this)) yield return tableName;
    }

    public void ReadOtherTables(Table[] tables)
    {
        // The only reason this exists is to remove FK's that point to partitioned tables from the
        // weasel configuration
        foreach (var foreignKey in ForeignKeys.ToArray())
        {
            var otherTable = tables.FirstOrDefault(x => Equals(x.Identifier, foreignKey.LinkedTable));
            if (otherTable?.Partitioning != null)
            {
                var partitionNames = otherTable.PartitionTableNames().ToArray();
                ForeignKeys.RemoveAll(x => partitionNames.Contains(x.LinkedTable.Name));
            }
        }
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
            return ForeignKeyTo(DbObjectName.Parse(PostgresqlProvider.Instance, referencedTableName),
                referencedColumnName, fkName, onDelete, onUpdate);
        }

        public ColumnExpression ForeignKeyTo(Table referencedTable, string referencedColumnName, string? fkName = null,
            CascadeAction onDelete = CascadeAction.NoAction, CascadeAction onUpdate = CascadeAction.NoAction)
        {
            return ForeignKeyTo(referencedTable.Identifier, referencedColumnName, fkName, onDelete, onUpdate);
        }


        [Obsolete("Use Overload with PostgresqlObjectName identifier name")]
        public ColumnExpression ForeignKeyTo(DbObjectName referencedIdentifier, string referencedColumnName,
            string? fkName = null, CascadeAction onDelete = CascadeAction.NoAction,
            CascadeAction onUpdate = CascadeAction.NoAction)
        {
            return ForeignKeyTo(PostgresqlObjectName.From(referencedIdentifier), referencedColumnName, fkName, onDelete,
                onUpdate);
        }

        public ColumnExpression ForeignKeyTo(PostgresqlObjectName referencedIdentifier, string referencedColumnName,
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

        /// <summary>
        ///     Marks this column as being part of the parent table's primary key
        /// </summary>
        /// <returns></returns>
        public ColumnExpression AsPrimaryKey()
        {
            Column.IsPrimaryKey = true;
            Column.AllowNulls = false;
            _parent._primaryKeyColumns.Fill(Column.Name);
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

            if (_parent.HasIgnoredIndex(index.Name))
            {
                throw new ArgumentException($"Cannot add ignored index {index.Name} on table {_parent.Identifier}");
            }

            _parent.Indexes.Add(index);

            configure?.Invoke(index);

            return this;
        }

        public ColumnExpression AddFullTextIndex(
            string? regConfig = null,
            string? documentConfig = null,
            string? indexName = null,
            string? indexPrefix = null
        )
        {
            var index = new FullTextIndexDefinition(
                PostgresqlObjectName.From(_parent.Identifier),
                documentConfig ?? Column.Name,
                regConfig,
                indexName, indexPrefix);

            if (_parent.HasIgnoredIndex(index.Name))
            {
                throw new ArgumentException($"Cannot add ignored index {index.Name} on table {_parent.Identifier}");
            }

            _parent.Indexes.Add(index);

            return this;
        }

        public ColumnExpression Serial()
        {
            Column.Type = "SERIAL";
            return this;
        }

        public ColumnExpression BigSerial()
        {
            Column.Type = "BIGSERIAL";
            return this;
        }

        public ColumnExpression SmallSerial()
        {
            Column.Type = "SMALLSERIAL";
            return this;
        }

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
            return DefaultValueByExpression($"nextval('{sequenceName}')");
        }

        public ColumnExpression DefaultValueByExpression(string expression)
        {
            Column.DefaultExpression = expression;

            return this;
        }

        public RangePartitioning PartitionByRange()
        {
            Column.IsPrimaryKey = true;
            _parent._primaryKeyColumns.Fill(Column.Name);
            return _parent.PartitionByRange(Column.Name);
        }

        /// <summary>
        ///     Partition this table using LIST partitioning of supplied values
        ///     on this column
        /// </summary>
        /// <returns></returns>
        /// <exception cref="NotImplementedException"></exception>
        public ListPartitioning PartitionByListValues()
        {
            Column.IsPrimaryKey = true;
            _parent._primaryKeyColumns.Fill(Column.Name);
            return _parent.PartitionByList(Column.Name);
        }

        /// <summary>
        ///     Partition this table using HASH partitioning on this column
        ///     using the following table suffixes as a series of partition
        ///     tables
        /// </summary>
        /// <param name="suffixNames">
        ///     List of table suffixes. Marten will create hashed partitions for each suffix that distributes
        ///     the data accordingly
        /// </param>
        public ColumnExpression PartitionByHash(params string[] suffixNames)
        {
            Column.IsPrimaryKey = true;
            _parent._primaryKeyColumns.Fill(Column.Name);
            _parent.PartitionByHash(new HashPartitioning { Columns = [Column.Name], Suffixes = suffixNames });

            return this;
        }
    }
}
