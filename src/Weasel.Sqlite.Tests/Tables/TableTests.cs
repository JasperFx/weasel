using JasperFx.Core;
using Shouldly;
using Weasel.Core;
using Weasel.Sqlite.Tables;
using Xunit;

namespace Weasel.Sqlite.Tests.Tables;

public class TableTests
{
    [Fact]
    public void build_table_by_name_only_puts_it_in_main()
    {
        var table = new Table("users");
        table.Identifier.Schema.ShouldBe("main");
        table.Identifier.Name.ShouldBe("users");
    }

    [Fact]
    public void add_column_by_name_and_type()
    {
        var table = new Table("users");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("name").NotNull();
        table.AddColumn<string>("email");

        table.Columns.Count.ShouldBe(3);
        table.PrimaryKeyColumns.Count.ShouldBe(1);
        table.PrimaryKeyColumns[0].ShouldBe("id");
    }

    [Fact]
    public void add_autoincrement_column()
    {
        var table = new Table("users");
        table.AddColumn<int>("id").AsPrimaryKey().AutoIncrement();

        var idColumn = table.Columns.First();
        idColumn.IsPrimaryKey.ShouldBeTrue();
        idColumn.IsAutoNumber.ShouldBeTrue();
    }

    [Fact]
    public void add_generated_column_stored()
    {
        var table = new Table("users");
        table.AddColumn<string>("email");
        table.AddColumn<string>("domain")
            .GeneratedAs("substr(email, instr(email, '@') + 1)", GeneratedColumnType.Stored);

        var domainColumn = table.Columns.Last();
        domainColumn.GeneratedExpression.ShouldBe("substr(email, instr(email, '@') + 1)");
        domainColumn.GeneratedType.ShouldBe(GeneratedColumnType.Stored);
    }

    [Fact]
    public void add_generated_column_virtual()
    {
        var table = new Table("users");
        table.AddColumn<string>("first_name");
        table.AddColumn<string>("last_name");
        table.AddColumn<string>("full_name")
            .GeneratedAs("first_name || ' ' || last_name", GeneratedColumnType.Virtual);

        var column = table.Columns.Last();
        column.GeneratedType.ShouldBe(GeneratedColumnType.Virtual);
    }

    [Fact]
    public void add_foreign_key_constraint()
    {
        var table = new Table("orders");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<int>("user_id");

        var fk = new ForeignKey("fk_orders_users");
        fk.ColumnNames = new[] { "user_id" };
        fk.LinkedTable = new SqliteObjectName("users");
        fk.LinkedNames = new[] { "id" };
        fk.OnDelete = CascadeAction.Cascade;

        table.ForeignKeys.Add(fk);

        table.ForeignKeys.Count.ShouldBe(1);
        table.ForeignKeys[0].OnDelete.ShouldBe(CascadeAction.Cascade);
    }

    [Fact]
    public void add_index_definitions()
    {
        var table = new Table("users");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("email");
        table.AddColumn<string>("created_at");

        var emailIndex = new IndexDefinition("idx_users_email") { IsUnique = true };
        emailIndex.AgainstColumns("email");
        table.Indexes.Add(emailIndex);

        var createdIndex = new IndexDefinition("idx_users_created");
        createdIndex.AgainstColumns("created_at");
        createdIndex.SortOrder = SortOrder.Desc;
        table.Indexes.Add(createdIndex);

        table.Indexes.Count.ShouldBe(2);
        table.Indexes[0].IsUnique.ShouldBeTrue();
        table.Indexes[1].SortOrder.ShouldBe(SortOrder.Desc);
    }

    [Fact]
    public void create_json_expression_index()
    {
        var table = new Table("users");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("settings"); // JSON column

        var index = new IndexDefinition("idx_settings_theme");
        index.ForJsonPath("settings", "$.theme");
        table.Indexes.Add(index);

        index.Expression.ShouldContain("json_extract");
        index.Expression.ShouldContain("settings");
        index.Expression.ShouldContain("$.theme");
    }

    [Fact]
    public void generate_create_table_ddl()
    {
        var table = new Table("users");
        table.AddColumn<int>("id").AsPrimaryKey().AutoIncrement();
        table.AddColumn<string>("name").NotNull();
        table.AddColumn<string>("email").NotNull();

        var migrator = new SqliteMigrator();
        var writer = new StringWriter();
        table.WriteCreateStatement(migrator, writer);
        var ddl = writer.ToString();

        ddl.ShouldContain("CREATE TABLE IF NOT EXISTS");
        ddl.ShouldContain("users");
        ddl.ShouldContain("id");
        ddl.ShouldContain("INTEGER");
        ddl.ShouldContain("PRIMARY KEY AUTOINCREMENT");
        ddl.ShouldContain("name");
        ddl.ShouldContain("TEXT");
        ddl.ShouldContain("NOT NULL");
    }

    [Fact]
    public void generate_strict_table()
    {
        var table = new Table("users");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("name");
        table.StrictTypes = true;

        var migrator = new SqliteMigrator();
        var writer = new StringWriter();
        table.WriteCreateStatement(migrator, writer);
        var ddl = writer.ToString();

        ddl.ShouldContain("STRICT");
    }

    [Fact]
    public void generate_without_rowid_table()
    {
        var table = new Table("users");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("name");
        table.WithoutRowId = true;

        var migrator = new SqliteMigrator();
        var writer = new StringWriter();
        table.WriteCreateStatement(migrator, writer);
        var ddl = writer.ToString();

        ddl.ShouldContain("WITHOUT ROWID");
    }

    [Fact]
    public void generate_table_with_foreign_keys()
    {
        var table = new Table("orders");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<int>("user_id");

        var fk = new ForeignKey("fk_orders_users");
        fk.ColumnNames = new[] { "user_id" };
        fk.LinkedTable = new SqliteObjectName("users");
        fk.LinkedNames = new[] { "id" };
        fk.OnDelete = CascadeAction.Cascade;
        table.ForeignKeys.Add(fk);

        var migrator = new SqliteMigrator();
        var writer = new StringWriter();
        table.WriteCreateStatement(migrator, writer);
        var ddl = writer.ToString();

        ddl.ShouldContain("FOREIGN KEY");
        ddl.ShouldContain("user_id");
        ddl.ShouldContain("REFERENCES");
        ddl.ShouldContain("users");
        ddl.ShouldContain("ON DELETE CASCADE");
    }

    [Fact]
    public void has_column()
    {
        var table = new Table("users");
        table.AddColumn<int>("id");
        table.AddColumn<string>("name");

        table.HasColumn("id").ShouldBeTrue();
        table.HasColumn("name").ShouldBeTrue();
        table.HasColumn("email").ShouldBeFalse();
    }

    [Fact]
    public void has_column_is_case_insensitive()
    {
        var table = new Table("users");
        table.AddColumn<string>("name");

        table.HasColumn("NAME").ShouldBeTrue();
        table.HasColumn("Name").ShouldBeTrue();
        table.HasColumn("name").ShouldBeTrue();
    }

    [Fact]
    public void remove_column()
    {
        var table = new Table("users");
        table.AddColumn<int>("id");
        table.AddColumn<string>("name");
        table.AddColumn<string>("email");

        table.Columns.Count.ShouldBe(3);

        table.RemoveColumn("name");

        table.Columns.Count.ShouldBe(2);
        table.HasColumn("name").ShouldBeFalse();
    }

    [Fact]
    public void column_for_finds_existing_column()
    {
        var table = new Table("users");
        table.AddColumn<int>("id");
        table.AddColumn<string>("name");

        var column = table.ColumnFor("name");
        column.ShouldNotBeNull();
        column!.Name.ShouldBe("name");
        column.Type.ShouldBe("TEXT");
    }

    [Fact]
    public void column_for_returns_null_when_not_found()
    {
        var table = new Table("users");
        table.AddColumn<int>("id");

        var column = table.ColumnFor("email");
        column.ShouldBeNull();
    }

    [Fact]
    public void index_for_finds_existing_index()
    {
        var table = new Table("users");
        var index = new IndexDefinition("idx_users_email");
        index.AgainstColumns("email");
        table.Indexes.Add(index);

        var found = table.IndexFor("idx_users_email");
        found.ShouldNotBeNull();
        found!.Name.ShouldBe("idx_users_email");
    }

    [Fact]
    public void index_for_returns_null_when_not_found()
    {
        var table = new Table("users");

        var index = table.IndexFor("idx_not_exist");
        index.ShouldBeNull();
    }

    [Fact]
    public void set_composite_primary_key()
    {
        var table = new Table("order_items");
        table.AddColumn<int>("order_id");
        table.AddColumn<int>("product_id");
        table.AddColumn<int>("quantity");

        // Use the internal ReadPrimaryKeyColumns method to set composite PK
        var pks = new List<string> { "order_id", "product_id" };
        table.ReadPrimaryKeyColumns(pks);

        table.PrimaryKeyColumns.Count.ShouldBe(2);
        table.PrimaryKeyColumns.ShouldContain("order_id");
        table.PrimaryKeyColumns.ShouldContain("product_id");
    }

    [Fact]
    public void set_primary_key_name()
    {
        var table = new Table("users");
        table.AddColumn<int>("id").AsPrimaryKey("pk_users_custom");

        table.PrimaryKeyName.ShouldBe("pk_users_custom");
    }

    [Fact]
    public void default_primary_key_name_follows_convention()
    {
        var table = new Table("users");
        table.AddColumn<int>("id").AsPrimaryKey();

        table.PrimaryKeyName.ShouldBe("pk_users");
    }

    [Fact]
    public void add_column_with_default_value()
    {
        var table = new Table("users");
        table.AddColumn<string>("status").DefaultValue("active");

        var column = table.Columns.First(c => c.Name == "status");
        column.DefaultExpression.ShouldContain("active");
    }

    [Fact]
    public void add_column_by_dotnet_type()
    {
        var table = new Table("users");
        table.AddColumn<int>("id");
        table.AddColumn<string>("name");
        table.AddColumn<DateTime>("created_at");

        var idCol = table.ColumnFor("id");
        idCol!.Type.ShouldBe("INTEGER");

        var nameCol = table.ColumnFor("name");
        nameCol!.Type.ShouldBe("TEXT");

        var createdCol = table.ColumnFor("created_at");
        createdCol!.Type.ShouldBe("TEXT");
    }

    [Fact]
    public void use_itable_interface()
    {
        ITable table = new Table("users");

        var idColumn = table.AddColumn("id", typeof(int));
        var nameColumn = table.AddColumn("name", typeof(string));

        idColumn.ShouldNotBeNull();
        nameColumn.ShouldNotBeNull();

        table.AddPrimaryKeyColumn("pk_id", "INTEGER").ShouldNotBeNull();
        table.AddPrimaryKeyColumn("pk_guid", typeof(Guid)).ShouldNotBeNull();
    }

    /// <summary>
    /// Regression for the bug surfaced by https://github.com/JasperFx/wolverine/issues/2680.
    /// Two columns each calling <c>AsPrimaryKey()</c> used to emit two inline
    /// <c>PRIMARY KEY</c> column definitions, which SQLite rejects with
    /// <c>'table … has more than one primary key'</c>. The fix detects the
    /// composite case in <see cref="Table.WriteCreateStatement"/> and emits a
    /// single table-level <c>PRIMARY KEY (col1, col2)</c> constraint instead.
    /// </summary>
    [Fact]
    public void composite_primary_key_emits_table_level_constraint_only()
    {
        var table = new Table("order_items");
        table.AddColumn<int>("order_id").AsPrimaryKey();
        table.AddColumn<int>("product_id").AsPrimaryKey();
        table.AddColumn<int>("quantity").NotNull();

        var migrator = new SqliteMigrator();
        var writer = new StringWriter();
        table.WriteCreateStatement(migrator, writer);
        var ddl = writer.ToString();

        // The table-level CONSTRAINT clause must be present, listing both PK columns.
        // Identifier names that aren't reserved keywords are emitted unquoted by
        // Weasel.Sqlite's QuoteName helper, so the expected form is `PRIMARY KEY (order_id, product_id)`.
        ddl.ShouldContain("CONSTRAINT");
        ddl.ShouldContain("PRIMARY KEY (order_id, product_id)");

        // No column may carry an inline `PRIMARY KEY` token in its declaration —
        // that is exactly what SQLite rejected. Match the column-line shape rather
        // than just `PRIMARY KEY` substring (the table-level CONSTRAINT clause also
        // contains `PRIMARY KEY`).
        foreach (var line in ddl.Split('\n')
                     .Where(l => l.TrimStart().StartsWith("order_id")
                                 || l.TrimStart().StartsWith("product_id")))
        {
            line.ShouldNotContain(
                "PRIMARY KEY",
                customMessage: $"composite-PK columns must NOT emit inline PRIMARY KEY (column line: {line.Trim()})");
        }

        // PK columns must be NOT NULL — SQLite only auto-applies that for inline
        // single-column PKs, so we explicitly emit it for the composite case.
        ddl.ShouldContain("NOT NULL");
    }

    [Fact]
    public void single_column_primary_key_still_emits_inline()
    {
        // Belt-and-braces: the composite-PK fix must not regress the historical
        // shape for single-column PKs (which SQLite accepts inline, and the rest
        // of the ecosystem expects).
        var table = new Table("users");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("name").NotNull();

        var migrator = new SqliteMigrator();
        var writer = new StringWriter();
        table.WriteCreateStatement(migrator, writer);
        var ddl = writer.ToString();

        // Single inline PK; no separate table-level CONSTRAINT clause.
        ddl.ShouldContain("PRIMARY KEY");
        ddl.ShouldNotContain("CONSTRAINT");
        ddl.ShouldNotContain("PRIMARY KEY (id)");
    }

    [Fact]
    public async Task composite_primary_key_ddl_executes_against_sqlite_in_memory()
    {
        // End-to-end repro of GH-2680: the broken DDL hit `'table ... has more
        // than one primary key'` from SQLite itself. Run the produced statement
        // against an in-memory connection so any future regression in the
        // emission shape would surface even if the static-string assertions
        // above drift.
        var table = new Table("order_items");
        table.AddColumn<int>("order_id").AsPrimaryKey();
        table.AddColumn<int>("product_id").AsPrimaryKey();
        table.AddColumn<int>("quantity").NotNull();

        var migrator = new SqliteMigrator();
        var writer = new StringWriter();
        table.WriteCreateStatement(migrator, writer);
        var ddl = writer.ToString();

        await using var conn = new Microsoft.Data.Sqlite.SqliteConnection("Data Source=:memory:");
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = ddl;
        await cmd.ExecuteNonQueryAsync();

        // PRAGMA table_info reports the pk slot per column: 0 = not in PK,
        // 1+ = position within the composite key. Verify both columns participate.
        await using var pragma = conn.CreateCommand();
        pragma.CommandText = "PRAGMA table_info(order_items);";
        var pkColumns = new List<string>();
        await using var reader = await pragma.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            var name = reader.GetString(1);
            var pk = reader.GetInt32(5);
            if (pk > 0) pkColumns.Add(name);
        }

        pkColumns.ShouldContain("order_id");
        pkColumns.ShouldContain("product_id");
        pkColumns.Count.ShouldBe(2);
    }
}