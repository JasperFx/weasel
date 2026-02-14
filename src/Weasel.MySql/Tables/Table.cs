using System.Globalization;
using JasperFx.Core;
using MySqlConnector;
using Weasel.Core;

namespace Weasel.MySql.Tables;

public enum PartitionStrategy
{
    None,
    Range,
    Hash,
    List,
    Key
}

public partial class Table: ITable
{
    private readonly List<TableColumn> _columns = new();
    private string? _primaryKeyName;

    public Table(DbObjectName name)
    {
        Identifier = name ?? throw new ArgumentNullException(nameof(name));
    }

    public Table(string tableName): this(DbObjectName.Parse(MySqlProvider.Instance, tableName))
    {
    }

    ITableColumn ITable.AddColumn(string name, string columnType)
    {
        var expression = AddColumn(name, columnType);
        return expression.Column;
    }

    ITableColumn ITable.AddColumn(string name, Type dotnetType)
    {
        var type = MySqlProvider.Instance.GetDatabaseType(dotnetType, EnumStorage.AsInteger);
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
        var type = MySqlProvider.Instance.GetDatabaseType(dotnetType, EnumStorage.AsInteger);
        var expression = AddColumn(name, type).AsPrimaryKey();
        return expression.Column;
    }

    IReadOnlyList<ForeignKeyBase> ITable.ForeignKeys => ForeignKeys.Cast<ForeignKeyBase>().ToList();

    ForeignKeyBase ITable.AddForeignKey(string name, DbObjectName linkedTable, string[] columnNames, string[] linkedColumnNames)
    {
        var fk = new ForeignKey(name)
        {
            LinkedTable = linkedTable,
            ColumnNames = columnNames,
            LinkedNames = linkedColumnNames
        };
        ForeignKeys.Add(fk);
        return fk;
    }

    public IReadOnlyList<TableColumn> Columns => _columns;

    public IList<ForeignKey> ForeignKeys { get; } = new List<ForeignKey>();
    public IList<IndexDefinition> Indexes { get; } = new List<IndexDefinition>();

    public IReadOnlyList<string> PrimaryKeyColumns =>
        _columns.Where(x => x.IsPrimaryKey).Select(x => x.Name).ToList();

    public IList<string> PartitionExpressions { get; } = new List<string>();

    public PartitionStrategy PartitionStrategy { get; set; } = PartitionStrategy.None;

    /// <summary>
    ///     Number of partitions for HASH or KEY partitioning.
    /// </summary>
    public int? PartitionCount { get; set; }

    /// <summary>
    ///     MySQL storage engine (InnoDB, MyISAM, etc.)
    /// </summary>
    public string Engine { get; set; } = "InnoDB";

    /// <summary>
    ///     Character set for the table.
    /// </summary>
    public string? Charset { get; set; }

    /// <summary>
    ///     Collation for the table.
    /// </summary>
    public string? Collation { get; set; }

    public string PrimaryKeyName
    {
        get => _primaryKeyName.IsNotEmpty()
            ? _primaryKeyName
            : $"pk_{Identifier.Name}_{PrimaryKeyColumns.Join("_")}";
        set => _primaryKeyName = value;
    }

    public void WriteCreateStatement(Migrator migrator, TextWriter writer)
    {
        if (migrator.TableCreation == CreationStyle.DropThenCreate)
        {
            writer.WriteLine($"DROP TABLE IF EXISTS {Identifier.QualifiedName};");
        }

        writer.WriteLine($"CREATE TABLE IF NOT EXISTS {Identifier.QualifiedName} (");

        // Find AUTO_INCREMENT columns that need inline UNIQUE KEY declarations
        // MySQL requires AUTO_INCREMENT columns to be part of a key at CREATE TABLE time
        var autoIncrementUniqueIndexes = Indexes
            .Where(idx => idx.IsUnique && idx.Columns.Length == 1)
            .Where(idx =>
            {
                var col = ColumnFor(idx.Columns[0]);
                if (col == null) return false;
                // Check if column is AUTO_INCREMENT and not already a primary key
                var isAutoIncrement = col.IsAutoNumber ||
                                      col.Type.Contains("AUTO_INCREMENT", StringComparison.OrdinalIgnoreCase);
                return isAutoIncrement && !col.IsPrimaryKey;
            })
            .ToList();

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

            // Add inline UNIQUE KEY declarations for AUTO_INCREMENT columns
            foreach (var index in autoIncrementUniqueIndexes)
            {
                lines.Add($"    UNIQUE KEY `{index.Name}` (`{index.Columns[0]}`)");
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

            // Add inline UNIQUE KEY declarations for AUTO_INCREMENT columns
            foreach (var index in autoIncrementUniqueIndexes)
            {
                lines.Add($"UNIQUE KEY `{index.Name}` (`{index.Columns[0]}`)");
            }

            for (var i = 0; i < lines.Count - 1; i++)
            {
                writer.WriteLine(lines[i] + ",");
            }

            writer.WriteLine(lines.Last());
        }

        writer.Write(")");

        // Table options
        var options = new List<string>();
        options.Add($"ENGINE={Engine}");

        if (Charset.IsNotEmpty())
        {
            options.Add($"DEFAULT CHARSET={Charset}");
        }

        if (Collation.IsNotEmpty())
        {
            options.Add($"COLLATE={Collation}");
        }

        if (options.Any())
        {
            writer.Write(" " + options.Join(" "));
        }

        // Partitioning
        switch (PartitionStrategy)
        {
            case PartitionStrategy.Range:
                writer.Write($" PARTITION BY RANGE ({PartitionExpressions.Join(", ")})");
                break;
            case PartitionStrategy.Hash:
                writer.Write($" PARTITION BY HASH ({PartitionExpressions.Join(", ")})");
                if (PartitionCount.HasValue)
                {
                    writer.Write($" PARTITIONS {PartitionCount}");
                }

                break;
            case PartitionStrategy.List:
                writer.Write($" PARTITION BY LIST ({PartitionExpressions.Join(", ")})");
                break;
            case PartitionStrategy.Key:
                writer.Write($" PARTITION BY KEY ({PartitionExpressions.Join(", ")})");
                if (PartitionCount.HasValue)
                {
                    writer.Write($" PARTITIONS {PartitionCount}");
                }

                break;
        }

        writer.WriteLine(";");

        foreach (var foreignKey in ForeignKeys)
        {
            writer.WriteLine();
            writer.WriteLine(foreignKey.ToDDL(this));
        }

        // Skip indexes that were already written inline (AUTO_INCREMENT unique indexes)
        var autoIncrementIndexNames = Indexes
            .Where(idx => idx.IsUnique && idx.Columns.Length == 1)
            .Where(idx =>
            {
                var col = ColumnFor(idx.Columns[0]);
                if (col == null) return false;
                var isAutoIncrement = col.IsAutoNumber ||
                                      col.Type.Contains("AUTO_INCREMENT", StringComparison.OrdinalIgnoreCase);
                return isAutoIncrement && !col.IsPrimaryKey;
            })
            .Select(idx => idx.Name)
            .ToHashSet(StringComparer.OrdinalIgnoreCase);

        foreach (var index in Indexes.Where(idx => !autoIncrementIndexNames.Contains(idx.Name)))
        {
            writer.WriteLine();
            writer.WriteLine(index.ToDDL(this));
        }
    }

    public void WriteDropStatement(Migrator rules, TextWriter writer)
    {
        writer.WriteLine($"DROP TABLE IF EXISTS {Identifier.QualifiedName};");
    }

    public DbObjectName Identifier { get; }

    public IEnumerable<DbObjectName> AllNames()
    {
        yield return Identifier;

        foreach (var index in Indexes)
        {
            yield return new MySqlObjectName(Identifier.Schema, index.Name);
        }

        foreach (var fk in ForeignKeys)
        {
            yield return new MySqlObjectName(Identifier.Schema, fk.Name);
        }
    }

    public string ToBasicCreateTableSql()
    {
        var writer = new StringWriter();
        var rules = new MySqlMigrator { Formatting = SqlFormatting.Concise };
        WriteCreateStatement(rules, writer);
        return writer.ToString();
    }

    internal string PrimaryKeyDeclaration()
    {
        var columns = PrimaryKeyColumns.Select(c => $"`{c}`").Join(", ");
        return $"    PRIMARY KEY ({columns})";
    }

    public TableColumn? ColumnFor(string columnName)
    {
        return Columns.FirstOrDefault(x => x.Name.EqualsIgnoreCase(columnName));
    }

    public bool HasColumn(string columnName)
    {
        return Columns.Any(x => x.Name.EqualsIgnoreCase(columnName));
    }

    public IndexDefinition? IndexFor(string indexName)
    {
        return Indexes.FirstOrDefault(x => x.Name.EqualsIgnoreCase(indexName));
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

        var type = MySqlProvider.Instance.GetDatabaseType(typeof(T), EnumStorage.AsInteger);
        return AddColumn(columnName, type);
    }

    public async Task<bool> ExistsInDatabaseAsync(MySqlConnection conn, CancellationToken ct = default)
    {
        var cmd = conn
            .CreateCommand(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = @schema AND table_name = @table;")
            .With("schema", Identifier.Schema)
            .With("table", Identifier.Name);

        var result = await cmd.ExecuteScalarAsync(ct).ConfigureAwait(false);
        return Convert.ToInt64(result) > 0;
    }

    public void RemoveColumn(string columnName)
    {
        _columns.RemoveAll(x => x.Name.EqualsIgnoreCase(columnName));
    }

    public ColumnExpression ModifyColumn(string columnName)
    {
        var column = ColumnFor(columnName) ??
                     throw new ArgumentOutOfRangeException(
                         $"Column '{columnName}' does not exist in table {Identifier}");
        return new ColumnExpression(this, column);
    }

    public bool HasIndex(string indexName)
    {
        return Indexes.Any(x => x.Name.EqualsIgnoreCase(indexName));
    }

    public void PartitionByRange(params string[] columnOrExpressions)
    {
        PartitionStrategy = PartitionStrategy.Range;
        PartitionExpressions.Clear();
        PartitionExpressions.AddRange(columnOrExpressions);
    }

    public void PartitionByHash(params string[] columnOrExpressions)
    {
        PartitionStrategy = PartitionStrategy.Hash;
        PartitionExpressions.Clear();
        PartitionExpressions.AddRange(columnOrExpressions);
    }

    public void PartitionByList(params string[] columnOrExpressions)
    {
        PartitionStrategy = PartitionStrategy.List;
        PartitionExpressions.Clear();
        PartitionExpressions.AddRange(columnOrExpressions);
    }

    public void PartitionByKey(params string[] columnOrExpressions)
    {
        PartitionStrategy = PartitionStrategy.Key;
        PartitionExpressions.Clear();
        PartitionExpressions.AddRange(columnOrExpressions);
    }

    public void ClearPartitions()
    {
        PartitionStrategy = PartitionStrategy.None;
        PartitionExpressions.Clear();
        PartitionCount = null;
    }

    public ForeignKey FindOrCreateForeignKey(string fkName)
    {
        var fk = ForeignKeys.FirstOrDefault(x => x.Name.EqualsIgnoreCase(fkName));
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
            return ForeignKeyTo(DbObjectName.Parse(MySqlProvider.Instance, referencedTableName),
                referencedColumnName, fkName, onDelete, onUpdate);
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
            var identifier = _parent.Identifier as MySqlObjectName ?? MySqlObjectName.From(_parent.Identifier);
            var fk = new ForeignKey(fkName ?? identifier.ToIndexName("fk", Column.Name))
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
            var identifier = _parent.Identifier as MySqlObjectName ?? MySqlObjectName.From(_parent.Identifier);
            var index = new IndexDefinition(identifier.ToIndexName("idx", Column.Name))
            {
                Columns = new[] { Column.Name }
            };

            _parent.Indexes.Add(index);
            configure?.Invoke(index);

            return this;
        }

        public ColumnExpression AddFulltextIndex(Action<IndexDefinition>? configure = null)
        {
            var identifier = _parent.Identifier as MySqlObjectName ?? MySqlObjectName.From(_parent.Identifier);
            var index = new IndexDefinition(identifier.ToIndexName("ft", Column.Name))
            {
                Columns = new[] { Column.Name },
                IndexType = MySqlIndexType.Fulltext
            };

            _parent.Indexes.Add(index);
            configure?.Invoke(index);

            return this;
        }

        public ColumnExpression AddSpatialIndex(Action<IndexDefinition>? configure = null)
        {
            var identifier = _parent.Identifier as MySqlObjectName ?? MySqlObjectName.From(_parent.Identifier);
            var index = new IndexDefinition(identifier.ToIndexName("sp", Column.Name))
            {
                Columns = new[] { Column.Name },
                IndexType = MySqlIndexType.Spatial
            };

            _parent.Indexes.Add(index);
            configure?.Invoke(index);

            return this;
        }

        public ColumnExpression AutoNumber()
        {
            Column.IsAutoNumber = true;
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

        public ColumnExpression PartitionByHash()
        {
            _parent.PartitionStrategy = PartitionStrategy.Hash;
            _parent.PartitionExpressions.Add(Column.Name);
            return this;
        }
    }
}
