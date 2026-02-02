using System.Globalization;
using JasperFx.Core;
using Oracle.ManagedDataAccess.Client;
using Weasel.Core;

namespace Weasel.Oracle.Tables;

public enum PartitionStrategy
{
    /// <summary>
    ///     No partitioning
    /// </summary>
    None,

    /// <summary>
    ///     PARTITION BY RANGE semantics
    /// </summary>
    Range,

    /// <summary>
    ///     PARTITION BY HASH semantics
    /// </summary>
    Hash,

    /// <summary>
    ///     PARTITION BY LIST semantics
    /// </summary>
    List
}

public partial class Table: ITable
{
    private readonly List<TableColumn> _columns = new();

    private string? _primaryKeyName;

    public Table(DbObjectName name)
    {
        Identifier = name ?? throw new ArgumentNullException(nameof(name));
    }

    public Table(string tableName): this(DbObjectName.Parse(OracleProvider.Instance, tableName))
    {
    }

    ITableColumn ITable.AddColumn(string name, string columnType)
    {
        var expression = AddColumn(name, columnType);
        return expression.Column;
    }

    ITableColumn ITable.AddColumn(string name, Type dotnetType)
    {
        var type = OracleProvider.Instance.GetDatabaseType(dotnetType, EnumStorage.AsInteger);
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
        var type = OracleProvider.Instance.GetDatabaseType(dotnetType, EnumStorage.AsInteger);
        var expression = AddColumn(name, type).AsPrimaryKey();
        return expression.Column;
    }

    public IReadOnlyList<TableColumn> Columns => _columns;

    public IList<ForeignKey> ForeignKeys { get; } = new List<ForeignKey>();
    public IList<IndexDefinition> Indexes { get; } = new List<IndexDefinition>();

    public IReadOnlyList<string> PrimaryKeyColumns =>
        _columns.Where(x => x.IsPrimaryKey).Select(x => x.Name).ToList();

    public IList<string> PartitionExpressions { get; } = new List<string>();

    /// <summary>
    ///     PARTITION strategy for this table
    /// </summary>
    public PartitionStrategy PartitionStrategy { get; set; } = PartitionStrategy.None;

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
            writer.WriteLine($@"
DECLARE
    v_count NUMBER;
BEGIN
    SELECT COUNT(*) INTO v_count FROM user_tables WHERE table_name = '{Identifier.Name.ToUpperInvariant()}';
    IF v_count > 0 THEN
        EXECUTE IMMEDIATE 'DROP TABLE {Identifier} CASCADE CONSTRAINTS';
    END IF;
END;
/
");
            writer.WriteLine($"CREATE TABLE {Identifier} (");
        }
        else
        {
            writer.WriteLine($@"
DECLARE
    v_count NUMBER;
BEGIN
    SELECT COUNT(*) INTO v_count FROM user_tables WHERE table_name = '{Identifier.Name.ToUpperInvariant()}';
    IF v_count = 0 THEN
        EXECUTE IMMEDIATE '");
            writer.WriteLine($"CREATE TABLE {Identifier} (");
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

        switch (PartitionStrategy)
        {
            case PartitionStrategy.None:
                writer.WriteLine(")");
                break;

            case PartitionStrategy.Range:
                writer.WriteLine($") PARTITION BY RANGE ({PartitionExpressions.Join(", ")})");
                break;

            case PartitionStrategy.Hash:
                writer.WriteLine($") PARTITION BY HASH ({PartitionExpressions.Join(", ")})");
                break;

            case PartitionStrategy.List:
                writer.WriteLine($") PARTITION BY LIST ({PartitionExpressions.Join(", ")})");
                break;
        }

        if (migrator.TableCreation != CreationStyle.DropThenCreate)
        {
            writer.WriteLine("';");
            writer.WriteLine("    END IF;");
            writer.WriteLine("END;");
            writer.WriteLine("/");
        }
        else
        {
            writer.WriteLine("/");
        }

        foreach (var foreignKey in ForeignKeys)
        {
            writer.WriteLine();
            writer.WriteLine(foreignKey.ToDDL(this));
            writer.WriteLine("/");
        }

        foreach (var index in Indexes)
        {
            writer.WriteLine();
            writer.WriteLine(index.ToDDL(this));
            writer.WriteLine("/");
        }
    }

    public void WriteDropStatement(Migrator rules, TextWriter writer)
    {
        writer.WriteLine($@"
DECLARE
    v_count NUMBER;
BEGIN
    SELECT COUNT(*) INTO v_count FROM user_tables WHERE table_name = '{Identifier.Name.ToUpperInvariant()}';
    IF v_count > 0 THEN
        EXECUTE IMMEDIATE 'DROP TABLE {Identifier} CASCADE CONSTRAINTS';
    END IF;
END;
/
");
    }

    public DbObjectName Identifier { get; }

    public IEnumerable<DbObjectName> AllNames()
    {
        yield return Identifier;

        foreach (var index in Indexes) yield return new OracleObjectName(Identifier.Schema, index.Name);

        foreach (var fk in ForeignKeys) yield return new OracleObjectName(Identifier.Schema, fk.Name);
    }

    /// <summary>
    ///     Generate the CREATE TABLE SQL expression with default
    ///     DDL rules. This is useful for quick diagnostics
    /// </summary>
    /// <returns></returns>
    public string ToBasicCreateTableSql()
    {
        var writer = new StringWriter();
        var rules = new OracleMigrator { Formatting = SqlFormatting.Concise };
        WriteCreateStatement(rules, writer);

        return writer.ToString();
    }

    internal string PrimaryKeyDeclaration()
    {
        return $"CONSTRAINT {PrimaryKeyName} PRIMARY KEY ({PrimaryKeyColumns.Join(", ")})";
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

        var type = OracleProvider.Instance.GetDatabaseType(typeof(T), EnumStorage.AsInteger);
        return AddColumn(columnName, type);
    }

    public async Task<bool> ExistsInDatabaseAsync(OracleConnection conn, CancellationToken ct = default)
    {
        var cmd = conn
            .CreateCommand(
                "SELECT COUNT(*) FROM all_tables WHERE table_name = :table_name AND owner = :owner")
            .With("table_name", Identifier.Name.ToUpperInvariant())
            .With("owner", Identifier.Schema.ToUpperInvariant());

        var result = await cmd.ExecuteScalarAsync(ct).ConfigureAwait(false);
        return Convert.ToInt32(result) > 0;
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

    public void ClearPartitions()
    {
        PartitionStrategy = PartitionStrategy.None;
        PartitionExpressions.Clear();
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
            return ForeignKeyTo(DbObjectName.Parse(OracleProvider.Instance, referencedTableName),
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
            var fk = new ForeignKey(fkName ?? _parent.Identifier.ToIndexName("fk", Column.Name))
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

        public ColumnExpression DefaultValueFromSequence(Sequence sequence)
        {
            return DefaultValueFromSequence(sequence.Identifier);
        }

        public ColumnExpression DefaultValueFromSequence(DbObjectName sequenceName)
        {
            return DefaultValueByExpression($"{sequenceName.QualifiedName}.NEXTVAL");
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
