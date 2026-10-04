using System.Globalization;
using FirebirdSql.Data.FirebirdClient;
using JasperFx.Core;
using Weasel.Core;

namespace Weasel.Firebird.Tables;

public partial class Table: TableBase<TableColumn, IndexDefinition, ForeignKey>
{
    /// <summary>
    ///     The identifier is always normalized to a <see cref="FirebirdObjectName" />, so a hand-built
    ///     name compares equal to the same table read back out of the catalog and renders without the
    ///     pseudo-schema (wolverine#3983 is the MySQL bug this avoids).
    /// </summary>
    public Table(DbObjectName name)
        : base(FirebirdObjectName.From(name ?? throw new ArgumentNullException(nameof(name))))
    {
    }

    public Table(string tableName): this(FirebirdProvider.Instance.Parse(tableName))
    {
    }

    /// <inheritdoc />
    public override IReadOnlyList<string> PrimaryKeyColumns =>
        ApplyPrimaryKeyOrder(_columns.Where(x => x.IsPrimaryKey).Select(x => x.Name).ToList());

    /// <summary>
    ///     <c>pk_{table}</c>, as on SQLite: a key name derived from the columns as well would pass
    ///     Firebird 3's 31-byte limit for almost no table.
    /// </summary>
    protected override string DefaultPrimaryKeyName() => $"pk_{Identifier.Name}";

    /// <inheritdoc />
    protected override ForeignKey CreateForeignKey(string name) => new(name);

    protected override IndexDefinition CreateIndexFor(string name, string[] columnNames)
        => new(name) { Columns = columnNames };

    /// <inheritdoc />
    protected override ITableColumn AddColumnAndReturn(string name, string columnType)
        => AddColumn(name, columnType).Column;

    /// <inheritdoc />
    protected override ITableColumn AddPrimaryKeyColumnAndReturn(string name, string columnType)
        => AddColumn(name, columnType).AsPrimaryKey().Column;

    /// <inheritdoc />
    protected override string GetDatabaseTypeFor(Type dotnetType)
        => FirebirdProvider.Instance.GetDatabaseType(dotnetType, EnumStorage.AsInteger);

    /// <inheritdoc />
    protected override Migrator GetDefaultMigratorForBasicSql()
        => new FirebirdMigrator { Formatting = SqlFormatting.Concise };

    /// <inheritdoc />
    /// <remarks>Firebird folds an undelimited identifier, so names compare ignoring case.</remarks>
    protected override StringComparison NameComparison => StringComparison.OrdinalIgnoreCase;

    /// <inheritdoc />
    protected override string NormalizeIdentifier(string name) => SchemaUtils.Unquote(name);

    /// <summary>
    ///     Firebird has check constraints; Weasel does not emit them here yet (weasel#488). False so
    ///     that asking for one throws rather than being accepted and silently dropped.
    /// </summary>
    protected override bool SupportsCheckConstraints => false;

    /// <inheritdoc />
    protected override string ProviderName => "Firebird";

    /// <summary>
    ///     The table's name as DDL writes it: bare where Firebird allows, delimited in the folded
    ///     spelling where it must be, or delimited exactly as written when the table preserves case.
    /// </summary>
    public string QuotedName => SchemaUtils.QuoteName(Identifier.Name, PreserveIdentifierCase);

    /// <summary>
    ///     The table's name as Firebird's catalog stores it.
    /// </summary>
    internal string CatalogName => SchemaUtils.CatalogName(Identifier.Name, PreserveIdentifierCase);

    /// <summary>
    ///     Read from the catalog: the columns a unique constraint covers, which a model never has --
    ///     it says "unique" with a unique index -- but a table created outside Weasel may.
    /// </summary>
    internal ISet<string> UniqueConstraintColumns { get; } = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

    /// <summary>
    ///     A guarded <c>CREATE TABLE</c>, then each foreign key and index as its own guarded statement.
    /// </summary>
    /// <remarks>
    ///     Every statement is conditioned on the object being missing, so a script runs again cleanly
    ///     and two appliers racing each other both succeed (weasel#620). Foreign keys are separate
    ///     statements rather than part of <c>CREATE TABLE</c> so that one pointing at a table created
    ///     later in the same migration can be held back until that table exists.
    /// </remarks>
    public override void WriteCreateStatement(Migrator migrator, TextWriter writer)
        => WriteCreateStatement(migrator, writer, new HashSet<string>());

    internal void WriteCreateStatement(Migrator migrator, TextWriter writer, IReadOnlySet<string> deferredForeignKeys)
    {
        FirebirdObjectName.AssertDefaultSchema(Identifier.Schema, $"table {Identifier.Name}");
        AssertCreatable(migrator, true, Columns, true, Indexes, ForeignKeys);

        if (migrator.TableCreation == CreationStyle.DropThenCreate)
        {
            WriteDropStatement(migrator, writer);
        }

        FirebirdScript.WriteGuarded(writer, CatalogProbes.Table(CatalogName), createTableSql(migrator));

        foreach (var foreignKey in ForeignKeys.Where(x => !deferredForeignKeys.Contains(x.Name)))
        {
            foreignKey.WriteAddStatement(this, writer);
        }

        foreach (var index in Indexes)
        {
            WriteCreateIndex(writer, index);
        }
    }

    internal void WriteCreateIndex(TextWriter writer, IndexDefinition index)
    {
        FirebirdScript.WriteGuarded(writer, CatalogProbes.Index(CatalogName, index.CatalogName(this)), index.ToDDL(this));
    }

    internal void WriteDropIndex(TextWriter writer, IndexDefinition index)
    {
        FirebirdScript.WriteGuardedWhenExists(writer, CatalogProbes.Index(CatalogName, index.CatalogName(this)),
            $"DROP INDEX {SchemaUtils.QuoteName(index.Name, PreserveIdentifierCase)}");
    }

    /// <summary>
    ///     The bare <c>CREATE TABLE</c> statement: no guard around it and no terminator.
    /// </summary>
    private string createTableSql(Migrator migrator)
    {
        if (!Columns.Any())
        {
            throw new InvalidOperationException($"Table {Identifier} has no columns, and Firebird needs at least one");
        }

        var writer = new StringWriter { NewLine = "\n" };
        writer.WriteLine($"CREATE TABLE {QuotedName} (");

        List<string> lines;
        if (migrator.Formatting == SqlFormatting.Pretty)
        {
            var columnLength = Columns.Max(x => x.QuotedName.Length) + 4;
            var typeLength = Columns.Max(x => x.DeclaredType.Length) + 4;

            lines = Columns.Select(column =>
                    $"    {column.QuotedName.PadRight(columnLength)}{column.DeclaredType.PadRight(typeLength)}{column.Declaration()}"
                        .TrimEnd())
                .ToList();
        }
        else
        {
            lines = Columns.Select(column => column.ToDeclaration()).ToList();
        }

        if (PrimaryKeyColumns.Any())
        {
            lines.Add((migrator.Formatting == SqlFormatting.Pretty ? "    " : "") + PrimaryKeyDeclaration());
        }

        for (var i = 0; i < lines.Count - 1; i++)
        {
            writer.WriteLine(lines[i] + ",");
        }

        writer.WriteLine(lines.Last());
        writer.Write(")");

        return writer.ToString();
    }

    /// <summary>
    ///     Refuse, before anything runs, what this table's DDL would write and Firebird would refuse: a
    ///     key over <see cref="MaxKeyColumns" /> columns, or a name over
    ///     <see cref="FirebirdMigrator.MaxIdentifierLength" />. A migration would otherwise stop at the
    ///     first statement that carries one, with every statement before it committed. Weasel does not
    ///     truncate a name either: a truncation scheme could never change later without renaming the
    ///     object. A derived primary key name says which setting to use.
    /// </summary>
    /// <remarks>
    ///     A create passes everything and an update what it adds. Only a <see cref="FirebirdMigrator" />
    ///     knows the name limit, so another migrator checks the keys alone.
    /// </remarks>
    internal void AssertCreatable(Migrator migrator, bool table, IEnumerable<TableColumn> columns, bool primaryKey,
        IEnumerable<IndexDefinition> indexes, IEnumerable<ForeignKey> foreignKeys)
    {
        indexes = indexes.ToArray();
        foreignKeys = foreignKeys.ToArray();

        assertKeyFits(primaryKey ? PrimaryKeyColumns.Count : 0, $"the primary key of table {Identifier.Name}");

        foreach (var index in indexes)
        {
            assertKeyFits(index.Columns.Length, $"index {index.Name} on table {Identifier.Name}");
        }

        foreach (var foreignKey in foreignKeys)
        {
            assertKeyFits(foreignKey.ColumnNames.Length, $"foreign key {foreignKey.Name} of table {Identifier.Name}");
        }

        if (migrator is not FirebirdMigrator firebird)
        {
            return;
        }

        if (table)
        {
            firebird.AssertFits(Identifier.Name, null, "the name of a table");
        }

        foreach (var column in columns)
        {
            firebird.AssertFits(column.Name, null, $"a column of table {Identifier.Name}");
        }

        if (primaryKey && PrimaryKeyColumns.Any())
        {
            var remedy = PrimaryKeyName == DefaultPrimaryKeyName()
                ? $"The primary key name '{PrimaryKeyName}' is derived from the table name. Set {nameof(PrimaryKeyName)} on table {Identifier.Name} to a shorter name."
                : null;
            firebird.AssertFits(PrimaryKeyName, remedy, $"the primary key of table {Identifier.Name}");
        }

        foreach (var index in indexes)
        {
            firebird.AssertFits(index.Name, null, $"an index on table {Identifier.Name}");
        }

        foreach (var foreignKey in foreignKeys)
        {
            firebird.AssertFits(foreignKey.Name, null, $"a foreign key of table {Identifier.Name}");
        }
    }

    /// <summary>
    ///     The most columns a Firebird index has, and so a primary key, unique index or foreign key.
    /// </summary>
    internal const int MaxKeyColumns = 16;

    /// <summary>
    ///     Firebird refuses a wider key at commit with "too many keys defined for index" (335544631) --
    ///     the very error the loser of a race to create the same index gets, which Weasel runs again --
    ///     so the refusal has to come from here.
    /// </summary>
    private static void assertKeyFits(int columns, string key)
    {
        if (columns > MaxKeyColumns)
        {
            throw new InvalidOperationException(
                $"Firebird indexes at most {MaxKeyColumns} columns, and {key} has {columns}. Key fewer columns.");
        }
    }

    /// <summary>
    ///     Drop the foreign keys in other tables that reference this one, then the table. Firebird has no
    ///     <c>DROP TABLE … CASCADE</c> and refuses to drop a table another table still references
    ///     (335544530); the table's own keys go with it.
    /// </summary>
    public override void WriteDropStatement(Migrator rules, TextWriter writer)
    {
        var name = FirebirdScript.Literal(CatalogName);

        FirebirdScript.WritePsql(writer, $"""
            EXECUTE BLOCK AS
              DECLARE VARIABLE referencing_table TYPE OF COLUMN RDB$RELATION_CONSTRAINTS.RDB$RELATION_NAME;
              DECLARE VARIABLE constraint_name TYPE OF COLUMN RDB$RELATION_CONSTRAINTS.RDB$CONSTRAINT_NAME;
            BEGIN
              FOR SELECT fk.RDB$RELATION_NAME, fk.RDB$CONSTRAINT_NAME
                  FROM RDB$RELATION_CONSTRAINTS fk
                  JOIN RDB$REF_CONSTRAINTS ref ON ref.RDB$CONSTRAINT_NAME = fk.RDB$CONSTRAINT_NAME
                  JOIN RDB$RELATION_CONSTRAINTS pk ON pk.RDB$CONSTRAINT_NAME = ref.RDB$CONST_NAME_UQ
                  WHERE pk.RDB$RELATION_NAME = {name} AND fk.RDB$RELATION_NAME <> {name}
                  INTO :referencing_table, :constraint_name
              DO
                EXECUTE STATEMENT 'ALTER TABLE "' || REPLACE(TRIM(referencing_table), '"', '""') || '" DROP CONSTRAINT "' || REPLACE(TRIM(constraint_name), '"', '""') || '"';
            END
            """);

        FirebirdScript.WriteGuardedWhenExists(writer, CatalogProbes.Table(CatalogName), $"DROP TABLE {QuotedName}");
    }

    public override IEnumerable<DbObjectName> AllNames()
    {
        yield return Identifier;

        foreach (var index in Indexes)
        {
            yield return new FirebirdObjectName(Identifier.Schema, index.Name);
        }

        foreach (var fk in ForeignKeys)
        {
            yield return new FirebirdObjectName(Identifier.Schema, fk.Name);
        }
    }

    internal string PrimaryKeyDeclaration()
    {
        return
            $"CONSTRAINT {SchemaUtils.QuoteName(PrimaryKeyName, PreserveIdentifierCase)} PRIMARY KEY ({PrimaryKeyColumns.Select(x => SchemaUtils.QuoteName(x, PreserveIdentifierCase)).Join(", ")})";
    }

    public ColumnExpression AddColumn(TableColumn column)
    {
        _columns.Add(column);
        column.Parent = this;

        return new ColumnExpression(this, column);
    }

    public ColumnExpression AddColumn(string columnName, string columnType)
    {
        return AddColumn(new TableColumn(columnName, columnType));
    }

    public ColumnExpression AddColumn<T>() where T : TableColumn, new()
    {
        return AddColumn(new T());
    }

    public ColumnExpression AddColumn<T>(string columnName)
    {
        if (typeof(T).IsEnum)
        {
            throw new InvalidOperationException(
                "Database column types cannot be automatically derived for enums. Explicitly specify as varchar or integer");
        }

        return AddColumn(columnName, FirebirdProvider.Instance.GetDatabaseType(typeof(T), EnumStorage.AsInteger));
    }

    public ColumnExpression ModifyColumn(string columnName)
    {
        var column = ColumnFor(columnName) ??
                     throw new ArgumentOutOfRangeException(
                         $"Column '{columnName}' does not exist in table {Identifier}");
        return new ColumnExpression(this, column);
    }

    /// <exception cref="InvalidOperationException">The name is longer than the server's catalog holds.</exception>
    public async Task<bool> ExistsInDatabaseAsync(FbConnection conn, CancellationToken ct = default)
    {
        FirebirdMigrator.AssertCatalogCanHold(CatalogName, FirebirdServerVersion.Of(conn), "the name of a table");

        await using var cmd = conn.CreateCommand(
            "SELECT COUNT(*) FROM RDB$RELATIONS WHERE RDB$RELATION_NAME = @table AND RDB$VIEW_BLR IS NULL");
        cmd.With("table", CatalogName);

        var result = await cmd.ExecuteScalarAsync(ct).ConfigureAwait(false);
        return Convert.ToInt64(result, CultureInfo.InvariantCulture) > 0;
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
            return ForeignKeyTo(FirebirdProvider.Instance.Parse(referencedTableName), referencedColumnName, fkName,
                onDelete, onUpdate);
        }

        public ColumnExpression ForeignKeyTo(Table referencedTable, string referencedColumnName,
            string? fkName = null, CascadeAction onDelete = CascadeAction.NoAction,
            CascadeAction onUpdate = CascadeAction.NoAction)
        {
            return ForeignKeyTo(referencedTable.Identifier, referencedColumnName, fkName, onDelete, onUpdate);
        }

        /// <summary>
        ///     A foreign key over this column. Named <c>fk_{table}_{column}</c> unless named here; a
        ///     derived name longer than the identifier limit is refused on the migration path rather than
        ///     truncated.
        /// </summary>
        public ColumnExpression ForeignKeyTo(DbObjectName referencedIdentifier, string referencedColumnName,
            string? fkName = null, CascadeAction onDelete = CascadeAction.NoAction,
            CascadeAction onUpdate = CascadeAction.NoAction)
        {
            var fk = new ForeignKey(fkName ?? $"fk_{_parent.Identifier.Name}_{Column.Name}")
            {
                LinkedTable = FirebirdObjectName.From(referencedIdentifier),
                ColumnNames = [Column.Name],
                LinkedNames = [referencedColumnName],
                DeleteAction = onDelete,
                UpdateAction = onUpdate
            };

            _parent.ForeignKeys.Add(fk);

            return this;
        }

        /// <summary>
        ///     Marks this column as being part of the parent table's primary key
        /// </summary>
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

        /// <summary>
        ///     An index over this column, named <c>idx_{table}_{column}</c>.
        /// </summary>
        public ColumnExpression AddIndex(Action<IndexDefinition>? configure = null)
        {
            var index = new IndexDefinition($"idx_{_parent.Identifier.Name}_{Column.Name}")
            {
                Columns = [Column.Name]
            };

            _parent.Indexes.Add(index);

            configure?.Invoke(index);

            return this;
        }

        /// <summary>
        ///     Mark this column as an identity column, written
        ///     <c>GENERATED BY DEFAULT AS IDENTITY NOT NULL</c>.
        ///     <para>
        ///     Canonical cross-provider spelling — every provider's <c>ColumnExpression</c> exposes
        ///     <c>AutoIncrement()</c> with provider-appropriate SQL emission (#270 step 10).
        ///     </para>
        /// </summary>
        public ColumnExpression AutoIncrement()
        {
            Column.IsAutoNumber = true;
            Column.AllowNulls = false;
            return this;
        }

        public ColumnExpression DefaultValueByString(string value)
        {
            return DefaultValueByExpression(FirebirdScript.Literal(value));
        }

        public ColumnExpression DefaultValue(int value)
        {
            return DefaultValueByExpression(value.ToString(CultureInfo.InvariantCulture));
        }

        public ColumnExpression DefaultValue(long value)
        {
            return DefaultValueByExpression(value.ToString(CultureInfo.InvariantCulture));
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

        /// <summary>
        ///     Make this a computed column, <c>COMPUTED BY (expression)</c>: evaluated when the row is read,
        ///     never stored. The expression is written without its outer parentheses.
        /// </summary>
        public ColumnExpression ComputedBy(string expression)
        {
            Column.ComputedExpression = expression;
            Column.ComputedColumnIsStored = false;

            return this;
        }
    }
}
