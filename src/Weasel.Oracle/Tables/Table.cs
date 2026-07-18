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

public partial class Table: TableBase<TableColumn, IndexDefinition, ForeignKey>
{
    public Table(DbObjectName name)
        : base(name ?? throw new ArgumentNullException(nameof(name)))
    {
    }

    public Table(string tableName): this(DbObjectName.Parse(OracleProvider.Instance, tableName))
    {
    }

    /// <inheritdoc />
    public override IReadOnlyList<string> PrimaryKeyColumns =>
        _columns.Where(x => x.IsPrimaryKey).Select(x => x.Name).ToList();

    /// <inheritdoc />
    protected override string DefaultPrimaryKeyName()
        => $"pk_{Identifier.Name}_{PrimaryKeyColumns.Join("_")}";

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
        => OracleProvider.Instance.GetDatabaseType(dotnetType, EnumStorage.AsInteger);

    /// <inheritdoc />
    protected override Migrator GetDefaultMigratorForBasicSql()
        => new OracleMigrator { Formatting = SqlFormatting.Concise };

    /// <inheritdoc />
    /// <remarks>Oracle identifier comparison is case-insensitive by default.</remarks>
    protected override StringComparison NameComparison => StringComparison.OrdinalIgnoreCase;

    public IList<string> PartitionExpressions { get; } = new List<string>();

    /// <summary>
    ///     PARTITION strategy for this table
    /// </summary>
    public PartitionStrategy PartitionStrategy { get; set; } = PartitionStrategy.None;

    public override void WriteCreateStatement(Migrator migrator, TextWriter writer)
    {
        if (migrator.TableCreation == CreationStyle.DropThenCreate)
        {
            writer.WriteLine($@"
DECLARE
    v_count NUMBER;
BEGIN
    SELECT COUNT(*) INTO v_count FROM all_tables WHERE table_name = '{Identifier.Name.ToUpperInvariant()}' AND owner = '{Identifier.Schema.ToUpperInvariant()}';
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
    SELECT COUNT(*) INTO v_count FROM all_tables WHERE table_name = '{Identifier.Name.ToUpperInvariant()}' AND owner = '{Identifier.Schema.ToUpperInvariant()}';
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

    public override void WriteDropStatement(Migrator rules, TextWriter writer)
    {
        writer.WriteLine($@"
DECLARE
    v_count NUMBER;
BEGIN
    SELECT COUNT(*) INTO v_count FROM all_tables WHERE table_name = '{Identifier.Name.ToUpperInvariant()}' AND owner = '{Identifier.Schema.ToUpperInvariant()}';
    IF v_count > 0 THEN
        EXECUTE IMMEDIATE 'DROP TABLE {Identifier} CASCADE CONSTRAINTS';
    END IF;
END;
/
");
    }

    public override IEnumerable<DbObjectName> AllNames()
    {
        yield return Identifier;

        foreach (var index in Indexes) yield return new OracleObjectName(Identifier.Schema, index.Name);

        foreach (var fk in ForeignKeys) yield return new OracleObjectName(Identifier.Schema, fk.Name);
    }

    internal string PrimaryKeyDeclaration()
    {
        // Case-preserved identifiers must be quoted or Oracle folds them to
        // uppercase; the conventional (folded) path stays unquoted as before
        if (PreserveIdentifierCase)
        {
            var columns = PrimaryKeyColumns.Select(x => $"\"{x}\"").Join(", ");
            return $"CONSTRAINT \"{PrimaryKeyName}\" PRIMARY KEY ({columns})";
        }

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
        var column = new TableColumn(columnName, columnType, PreserveIdentifierCase) { Parent = this };
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

        /// <summary>
        ///     Mark this column as an auto-incrementing identity column. Oracle
        ///     renders this as <c>GENERATED BY DEFAULT AS IDENTITY</c>.
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
        ///     Historical Oracle-specific spelling for <see cref="AutoIncrement" />.
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
