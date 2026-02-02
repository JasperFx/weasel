using Shouldly;
using Weasel.Core;
using Weasel.Sqlite.Tables;
using Xunit;

namespace Weasel.Sqlite.Tests.Tables;

public class TableTests
{
    [Fact]
    public void can_create_simple_table()
    {
        var table = new Table("users");
        table.AddColumn("id", "INTEGER").AsPrimaryKey();
        table.AddColumn("name", "TEXT").NotNull();
        table.AddColumn("email", "TEXT");

        table.Identifier.Name.ShouldBe("users");
        table.Columns.Count.ShouldBe(3);
        table.PrimaryKeyColumns.Count.ShouldBe(1);
        table.PrimaryKeyColumns[0].ShouldBe("id");
    }

    [Fact]
    public void can_add_autoincrement_column()
    {
        var table = new Table("users");
        table.AddColumn("id", "INTEGER").AsPrimaryKey().AutoIncrement();

        var idColumn = table.Columns.First();
        idColumn.IsPrimaryKey.ShouldBeTrue();
        idColumn.IsAutoNumber.ShouldBeTrue();
    }

    [Fact]
    public void can_add_generated_column()
    {
        var table = new Table("users");
        table.AddColumn("email", "TEXT");
        table.AddColumn("domain", "TEXT")
            .GeneratedAs("substr(email, instr(email, '@') + 1)", GeneratedColumnType.Stored);

        var domainColumn = table.Columns.Last();
        domainColumn.GeneratedExpression.ShouldBe("substr(email, instr(email, '@') + 1)");
        domainColumn.GeneratedType.ShouldBe(GeneratedColumnType.Stored);
    }

    [Fact]
    public void can_add_foreign_key()
    {
        var table = new Table("orders");
        table.AddColumn("id", "INTEGER").AsPrimaryKey();
        table.AddColumn("user_id", "INTEGER");

        var fk = new ForeignKey("fk_orders_users");
        fk.ColumnNames = new[] { "user_id" };
        fk.LinkedTable = new SqliteObjectName("main", "users");
        fk.LinkedNames = new[] { "id" };
        fk.OnDelete = CascadeAction.Cascade;

        table.ForeignKeys.Add(fk);

        table.ForeignKeys.Count.ShouldBe(1);
        table.ForeignKeys[0].OnDelete.ShouldBe(CascadeAction.Cascade);
    }

    [Fact]
    public void can_add_indexes()
    {
        var table = new Table("users");
        table.AddColumn("id", "INTEGER").AsPrimaryKey();
        table.AddColumn("email", "TEXT");
        table.AddColumn("created_at", "TEXT");

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
    public void can_create_json_expression_index()
    {
        var table = new Table("users");
        table.AddColumn("id", "INTEGER").AsPrimaryKey();
        table.AddColumn("settings", "TEXT"); // JSON column

        var index = new IndexDefinition("idx_settings_theme");
        index.ForJsonPath("settings", "$.theme");
        table.Indexes.Add(index);

        index.Expression.ShouldContain("json_extract");
        index.Expression.ShouldContain("settings");
        index.Expression.ShouldContain("$.theme");
    }

    [Fact]
    public void can_generate_create_table_ddl()
    {
        var table = new Table("users");
        table.AddColumn("id", "INTEGER").AsPrimaryKey().AutoIncrement();
        table.AddColumn("name", "TEXT").NotNull();
        table.AddColumn("email", "TEXT").NotNull();

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
    public void can_generate_strict_table()
    {
        var table = new Table("users");
        table.AddColumn("id", "INTEGER").AsPrimaryKey();
        table.AddColumn("name", "TEXT");
        table.StrictTypes = true;

        var migrator = new SqliteMigrator();
        var writer = new StringWriter();
        table.WriteCreateStatement(migrator, writer);
        var ddl = writer.ToString();

        ddl.ShouldContain("STRICT");
    }

    [Fact]
    public void can_generate_without_rowid_table()
    {
        var table = new Table("users");
        table.AddColumn("id", "INTEGER").AsPrimaryKey();
        table.AddColumn("name", "TEXT");
        table.WithoutRowId = true;

        var migrator = new SqliteMigrator();
        var writer = new StringWriter();
        table.WriteCreateStatement(migrator, writer);
        var ddl = writer.ToString();

        ddl.ShouldContain("WITHOUT ROWID");
    }

    [Fact]
    public void can_generate_table_with_foreign_keys()
    {
        var table = new Table("orders");
        table.AddColumn("id", "INTEGER").AsPrimaryKey();
        table.AddColumn("user_id", "INTEGER");

        var fk = new ForeignKey("fk_orders_users");
        fk.ColumnNames = new[] { "user_id" };
        fk.LinkedTable = new SqliteObjectName("main", "users");
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
    public void has_column_should_find_existing_column()
    {
        var table = new Table("users");
        table.AddColumn("id", "INTEGER");
        table.AddColumn("name", "TEXT");

        table.HasColumn("id").ShouldBeTrue();
        table.HasColumn("name").ShouldBeTrue();
        table.HasColumn("email").ShouldBeFalse();
    }

    [Fact]
    public void has_column_should_be_case_insensitive()
    {
        var table = new Table("users");
        table.AddColumn("name", "TEXT");

        table.HasColumn("NAME").ShouldBeTrue();
        table.HasColumn("Name").ShouldBeTrue();
        table.HasColumn("name").ShouldBeTrue();
    }

    [Fact]
    public void can_remove_column()
    {
        var table = new Table("users");
        table.AddColumn("id", "INTEGER");
        table.AddColumn("name", "TEXT");
        table.AddColumn("email", "TEXT");

        table.Columns.Count.ShouldBe(3);

        table.RemoveColumn("name");

        table.Columns.Count.ShouldBe(2);
        table.HasColumn("name").ShouldBeFalse();
    }

    [Fact]
    public void can_use_itableinterface()
    {
        ITable table = new Table("users");

        var idColumn = table.AddColumn("id", typeof(int));
        var nameColumn = table.AddColumn("name", typeof(string));

        idColumn.ShouldNotBeNull();
        nameColumn.ShouldNotBeNull();

        table.AddPrimaryKeyColumn("pk_id", "INTEGER").ShouldNotBeNull();
        table.AddPrimaryKeyColumn("pk_guid", typeof(Guid)).ShouldNotBeNull();
    }
}
