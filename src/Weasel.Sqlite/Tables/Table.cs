using JasperFx.Core;
using Weasel.Core;

namespace Weasel.Sqlite.Tables;

/// <summary>
/// Represents a SQLite table with support for JSON columns, foreign keys, indexes, and generated columns.
/// Note: SQLite has limited ALTER TABLE support, many schema changes require table recreation.
/// </summary>
public partial class Table: TableBase<TableColumn, IndexDefinition, ForeignKey>
{
    internal readonly List<string> _primaryKeyColumns = new();

    public Table(DbObjectName name)
        : base(name ?? throw new ArgumentNullException(nameof(name)))
    {
    }

    public Table(string tableName): this(DbObjectName.Parse(SqliteProvider.Instance, tableName))
    {
    }

    /// <inheritdoc />
    public override IReadOnlyList<string> PrimaryKeyColumns => _primaryKeyColumns;

    /// <inheritdoc />
    /// <remarks>SQLite spells the auto-PK constraint name as <c>pk_{tableName}</c>.</remarks>
    protected override string DefaultPrimaryKeyName() => $"pk_{Identifier.Name}";

    /// <inheritdoc />
    protected override ForeignKey CreateForeignKey(string name) => new ForeignKey(name);

    /// <inheritdoc />
    protected override ITableColumn AddColumnAndReturn(string name, string columnType)
        => AddColumn(name, columnType).Column;

    /// <inheritdoc />
    protected override ITableColumn AddPrimaryKeyColumnAndReturn(string name, string columnType)
        => AddColumn(name, columnType).AsPrimaryKey().Column;

    /// <inheritdoc />
    protected override string GetDatabaseTypeFor(Type dotnetType)
        => SqliteProvider.Instance.GetDatabaseType(dotnetType, EnumStorage.AsInteger);

    /// <inheritdoc />
    protected override Migrator GetDefaultMigratorForBasicSql()
        => new SqliteMigrator { Formatting = SqlFormatting.Concise };

    /// <summary>
    /// Enable foreign key constraints (disabled by default in SQLite)
    /// </summary>
    public bool EnableForeignKeys { get; set; } = true;

    /// <summary>
    /// Use WITHOUT ROWID table optimization (requires explicit PRIMARY KEY)
    /// </summary>
    public bool WithoutRowId { get; set; }

    /// <summary>
    /// Use STRICT table (SQLite 3.37+) for strict type checking
    /// </summary>
    public bool StrictTypes { get; set; }

    /// <summary>
    /// Change the table's schema (supports "main" and "temp" schemas in SQLite)
    /// </summary>
    public void MoveToSchema(string schemaName)
    {
        Identifier = new SqliteObjectName(schemaName, Identifier.Name);
    }

    /// <summary>
    ///     The pluggable DDL syntax strategy this table uses. Routed through for
    ///     DROP and CREATE-header emission as part of #270 step 8 (prototype);
    ///     step 9 will move the full CREATE algorithm to <c>TableBase</c> and the
    ///     strategy will own more of the emission. Surfaces SQLite's
    ///     <see cref="IDdlSyntaxStrategy.InlineForeignKeyConstraints" /> = true
    ///     trait so cross-provider code can ask the table how its FKs are
    ///     emitted without having to know it's SQLite-specifically.
    /// </summary>
    public IDdlSyntaxStrategy Syntax => SqliteDdlSyntax.Instance;

    public override void WriteCreateStatement(Migrator migrator, TextWriter writer)
    {
        if (migrator.TableCreation == CreationStyle.DropThenCreate)
        {
            Syntax.WriteDropTable(writer, Identifier);
        }
        Syntax.WriteCreateTableHeader(writer, Identifier, migrator.TableCreation);

        var lines = new List<string>();

        // SQLite rejects two inline PRIMARY KEY columns ("table ... has more than one primary
        // key"), so for composite primary keys we suppress the inline emission on each column
        // and rely on a single table-level CONSTRAINT ... PRIMARY KEY (col1, col2) clause
        // below. Single-column PKs keep the historical inline-emission shape.
        var hasCompositePrimaryKey = PrimaryKeyColumns.Count > 1;
        var emitInlinePrimaryKey = !hasCompositePrimaryKey;

        // Add column definitions
        if (migrator.Formatting == SqlFormatting.Pretty)
        {
            var columnLength = Columns.Any() ? Columns.Max(x => x.QuotedName.Length) + 4 : 20;
            var typeLength = Columns.Any() ? Columns.Max(x => x.Type.Length) + 4 : 10;

            lines.AddRange(Columns.Select(column =>
                $"    {column.QuotedName.PadRight(columnLength)}{column.Type.PadRight(typeLength)}{column.Declaration(emitInlinePrimaryKey)}"));
        }
        else
        {
            lines.AddRange(Columns.Select(column => column.ToDeclaration(emitInlinePrimaryKey)));
        }

        // Add primary key if not already defined inline. Either path emits the table-level
        // constraint exactly once: composite PK case (we suppressed every inline emission above)
        // or the legacy "registered via PrimaryKeyColumns without IsPrimaryKey set" case.
        if (hasCompositePrimaryKey || (PrimaryKeyColumns.Any() && !Columns.Any(c => c.IsPrimaryKey)))
        {
            lines.Add(PrimaryKeyDeclaration());
        }

        // Add foreign key constraints (must be inline in SQLite)
        foreach (var fk in ForeignKeys)
        {
            var fkWriter = new StringWriter();
            fk.WriteInlineDefinition(fkWriter);
            lines.Add($"    {fkWriter}");
        }

        // Write lines with commas
        for (var i = 0; i < lines.Count - 1; i++)
        {
            writer.WriteLine(lines[i] + ",");
        }

        if (lines.Any())
        {
            writer.WriteLine(lines.Last());
        }

        // Table options
        writer.Write(")");
        if (WithoutRowId)
        {
            writer.Write(" WITHOUT ROWID");
        }
        if (StrictTypes)
        {
            writer.Write(" STRICT");
        }
        writer.WriteLine(";");

        // Create indexes (must be separate statements)
        foreach (var index in Indexes)
        {
            writer.WriteLine();
            writer.WriteLine(index.ToDDL(this));
        }
    }

    public override void WriteDropStatement(Migrator rules, TextWriter writer)
    {
        Syntax.WriteDropTable(writer, Identifier);
    }

    // Implemented in Table.Deltas.cs partial class

    /// <inheritdoc />
    /// <remarks>
    ///     SQLite identifiers are case-insensitive — override the
    ///     <see cref="TableBase{TColumn,TIndex,TForeignKey}.NameComparison" />
    ///     hook so <c>HasColumn</c>, <c>ColumnFor</c>, <c>IndexFor</c> and
    ///     <c>HasIndex</c> all do case-folded lookups.
    /// </remarks>
    protected override StringComparison NameComparison => StringComparison.OrdinalIgnoreCase;

    public override IEnumerable<DbObjectName> AllNames()
    {
        yield return Identifier;

        foreach (var index in Indexes)
        {
            yield return new SqliteObjectName(index.Name);
        }
    }

    private string PrimaryKeyDeclaration()
    {
        var pkCols = PrimaryKeyColumns.Select(SchemaUtils.QuoteName).Join(", ");
        return $"    CONSTRAINT {SchemaUtils.QuoteName(PrimaryKeyName)} PRIMARY KEY ({pkCols})";
    }

    /// <summary>
    /// Modify an existing column's properties
    /// Note: SQLite has very limited ALTER COLUMN support, so this mostly just finds the column
    /// </summary>
    public ColumnExpression? ModifyColumn(string columnName)
    {
        var column = ColumnFor(columnName);
        return column == null ? null : new ColumnExpression(this, column);
    }

    /// <summary>
    /// Fluent API for adding columns
    /// </summary>
    public ColumnExpression AddColumn(string name, string type)
    {
        var column = new TableColumn(name, type) { Parent = this };
        _columns.Add(column);
        return new ColumnExpression(this, column);
    }

    /// <summary>
    /// Fluent API for adding columns using CLR type mapping
    /// </summary>
    public ColumnExpression AddColumn<T>(string name)
    {
        var type = SqliteProvider.Instance.GetDatabaseType(typeof(T), EnumStorage.AsInteger);
        return AddColumn(name, type);
    }

    /// <summary>
    /// Add a pre-configured TableColumn instance
    /// </summary>
    public ColumnExpression AddColumn(TableColumn column)
    {
        _columns.Add(column);
        column.Parent = this;
        return new ColumnExpression(this, column);
    }

    /// <summary>
    /// Add a column using a custom TableColumn subclass
    /// </summary>
    public ColumnExpression AddColumn<T>() where T : TableColumn, new()
    {
        var column = new T();
        return AddColumn(column);
    }

    /// <summary>
    /// Fluent API for column configuration
    /// </summary>
    public class ColumnExpression
    {
        private readonly Table _table;

        public ColumnExpression(Table table, TableColumn column)
        {
            _table = table;
            Column = column;
        }

        public TableColumn Column { get; }

        public ColumnExpression AsPrimaryKey(string? name = null)
        {
            Column.IsPrimaryKey = true;
            _table._primaryKeyColumns.Add(Column.Name);
            if (name.IsNotEmpty())
            {
                _table.PrimaryKeyName = name!;
            }
            return this;
        }

        public ColumnExpression NotNull()
        {
            Column.AllowNulls = false;
            return this;
        }

        public ColumnExpression AllowNulls()
        {
            Column.AllowNulls = true;
            return this;
        }

        public ColumnExpression DefaultValue(object value)
        {
            Column.DefaultExpression = value switch
            {
                string s => $"'{s}'",
                bool b => b ? "1" : "0",
                _ => value.ToString()
            };
            return this;
        }

        public ColumnExpression DefaultValue(int value)
        {
            Column.DefaultExpression = value.ToString();
            return this;
        }

        public ColumnExpression DefaultValue(long value)
        {
            Column.DefaultExpression = value.ToString();
            return this;
        }

        public ColumnExpression DefaultValue(double value)
        {
            Column.DefaultExpression = value.ToString();
            return this;
        }

        public ColumnExpression DefaultValue(bool value)
        {
            Column.DefaultExpression = value ? "1" : "0";
            return this;
        }

        public ColumnExpression DefaultValueByString(string value)
        {
            Column.DefaultExpression = $"'{value}'";
            return this;
        }

        public ColumnExpression DefaultValueByExpression(string expression)
        {
            Column.DefaultExpression = expression;
            return this;
        }

        public ColumnExpression AutoIncrement()
        {
            Column.IsAutoNumber = true;
            return this;
        }

        public ColumnExpression GeneratedAs(string expression, GeneratedColumnType type = GeneratedColumnType.Virtual)
        {
            Column.GeneratedExpression = expression;
            Column.GeneratedType = type;
            return this;
        }

        /// <summary>
        /// Add an index on this column
        /// </summary>
        public ColumnExpression AddIndex(Action<IndexDefinition>? configure = null)
        {
            var index = new IndexDefinition(_table.Identifier.ToIndexName("idx", Column.Name))
            {
                Columns = new[] { Column.Name }
            };

            if (_table.HasIgnoredIndex(index.Name))
            {
                throw new ArgumentException($"Cannot add ignored index {index.Name} on table {_table.Identifier}");
            }

            _table.Indexes.Add(index);

            configure?.Invoke(index);

            return this;
        }

        /// <summary>
        /// Create a foreign key relationship to another table using a string table name
        /// </summary>
        public ColumnExpression ForeignKeyTo(string referencedTableName, string referencedColumnName,
            string? fkName = null,
            CascadeAction onDelete = CascadeAction.NoAction,
            CascadeAction onUpdate = CascadeAction.NoAction)
        {
            var referencedIdentifier = DbObjectName.Parse(SqliteProvider.Instance, referencedTableName);
            return ForeignKeyTo(referencedIdentifier, referencedColumnName, fkName, onDelete, onUpdate);
        }

        /// <summary>
        /// Create a foreign key relationship to another table using a Table object
        /// </summary>
        public ColumnExpression ForeignKeyTo(Table referencedTable, string referencedColumnName, string? fkName = null,
            CascadeAction onDelete = CascadeAction.NoAction,
            CascadeAction onUpdate = CascadeAction.NoAction)
        {
            return ForeignKeyTo(referencedTable.Identifier, referencedColumnName, fkName, onDelete, onUpdate);
        }

        /// <summary>
        /// Create a foreign key relationship to another table using a DbObjectName
        /// </summary>
        public ColumnExpression ForeignKeyTo(DbObjectName referencedIdentifier, string referencedColumnName,
            string? fkName = null,
            CascadeAction onDelete = CascadeAction.NoAction,
            CascadeAction onUpdate = CascadeAction.NoAction)
        {
            var fk = new ForeignKey(fkName ?? _table.Identifier.ToIndexName("fkey", Column.Name))
            {
                LinkedTable = referencedIdentifier,
                ColumnNames = new[] { Column.Name },
                LinkedNames = new[] { referencedColumnName },
                DeleteAction = onDelete,
                UpdateAction = onUpdate
            };

            _table.ForeignKeys.Add(fk);

            return this;
        }

        /// <summary>
        /// Create a foreign key relationship to another table using a SqliteObjectName
        /// </summary>
        public ColumnExpression ForeignKeyTo(SqliteObjectName referencedIdentifier, string referencedColumnName,
            string? fkName = null,
            CascadeAction onDelete = CascadeAction.NoAction,
            CascadeAction onUpdate = CascadeAction.NoAction)
        {
            return ForeignKeyTo((DbObjectName)referencedIdentifier, referencedColumnName, fkName, onDelete, onUpdate);
        }
    }
}
