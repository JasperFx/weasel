using Shouldly;
using Weasel.Core;
using Weasel.Sqlite.Tables;
using Xunit;

namespace Weasel.Sqlite.Tests.Tables;

public class TableDeltaTests
{
    [Fact]
    public void no_difference_when_tables_match()
    {
        var expected = new Table("users");
        expected.AddColumn<int>("id").AsPrimaryKey();
        expected.AddColumn<string>("name");

        var actual = new Table("users");
        actual.AddColumn<int>("id").AsPrimaryKey();
        actual.AddColumn<string>("name");

        var delta = new TableDelta(expected, actual);

        delta.Difference.ShouldBe(SchemaPatchDifference.None);
        delta.RequiresTableRecreation.ShouldBeFalse();
    }

    [Fact]
    public void create_when_table_does_not_exist()
    {
        var expected = new Table("users");
        expected.AddColumn<int>("id").AsPrimaryKey();

        var delta = new TableDelta(expected, null);

        delta.Difference.ShouldBe(SchemaPatchDifference.Create);
    }

    [Fact]
    public void update_when_column_added()
    {
        var expected = new Table("users");
        expected.AddColumn<int>("id").AsPrimaryKey();
        expected.AddColumn<string>("name");
        expected.AddColumn<string>("email");

        var actual = new Table("users");
        actual.AddColumn<int>("id").AsPrimaryKey();
        actual.AddColumn<string>("name");

        var delta = new TableDelta(expected, actual);

        delta.Difference.ShouldBe(SchemaPatchDifference.Update);
        delta.Columns.Missing.Count.ShouldBe(1);
        delta.Columns.Missing[0].Name.ShouldBe("email");
    }

    [Fact]
    public void update_when_column_removed()
    {
        var expected = new Table("users");
        expected.AddColumn<int>("id").AsPrimaryKey();
        expected.AddColumn<string>("name");

        var actual = new Table("users");
        actual.AddColumn<int>("id").AsPrimaryKey();
        actual.AddColumn<string>("name");
        actual.AddColumn<string>("email");

        var delta = new TableDelta(expected, actual);

        delta.Difference.ShouldBe(SchemaPatchDifference.Update);
        delta.Columns.Extras.Count.ShouldBe(1);
        delta.Columns.Extras[0].Name.ShouldBe("email");
    }

    [Fact]
    public void invalid_when_column_type_changed()
    {
        var expected = new Table("users");
        expected.AddColumn<int>("id").AsPrimaryKey();
        expected.AddColumn<string>("age");

        var actual = new Table("users");
        actual.AddColumn<int>("id").AsPrimaryKey();
        actual.AddColumn<int>("age");

        var delta = new TableDelta(expected, actual);

        delta.Difference.ShouldBe(SchemaPatchDifference.Invalid);
        delta.RequiresTableRecreation.ShouldBeTrue();
        delta.Columns.Different.Count.ShouldBe(1);
    }

    [Fact]
    public void update_when_index_added()
    {
        var expected = new Table("users");
        expected.AddColumn<int>("id").AsPrimaryKey();
        expected.AddColumn<string>("email");

        var index = new IndexDefinition("idx_users_email");
        index.AgainstColumns("email");
        expected.Indexes.Add(index);

        var actual = new Table("users");
        actual.AddColumn<int>("id").AsPrimaryKey();
        actual.AddColumn<string>("email");

        var delta = new TableDelta(expected, actual);

        delta.Difference.ShouldBe(SchemaPatchDifference.Update);
        delta.Indexes.Missing.Count.ShouldBe(1);
    }

    [Fact]
    public void update_when_index_removed()
    {
        var expected = new Table("users");
        expected.AddColumn<int>("id").AsPrimaryKey();
        expected.AddColumn<string>("email");

        var actual = new Table("users");
        actual.AddColumn<int>("id").AsPrimaryKey();
        actual.AddColumn<string>("email");

        var index = new IndexDefinition("idx_users_email");
        index.AgainstColumns("email");
        actual.Indexes.Add(index);

        var delta = new TableDelta(expected, actual);

        delta.Difference.ShouldBe(SchemaPatchDifference.Update);
        delta.Indexes.Extras.Count.ShouldBe(1);
    }

    [Fact]
    public void invalid_when_foreign_key_added()
    {
        var expected = new Table("posts");
        expected.AddColumn<int>("id").AsPrimaryKey();
        expected.AddColumn<int>("user_id");

        var fk = new ForeignKey("fk_posts_user");
        fk.ColumnNames = new[] { "user_id" };
        fk.LinkedTable = new SqliteObjectName("main", "users");
        fk.LinkedNames = new[] { "id" };
        expected.ForeignKeys.Add(fk);

        var actual = new Table("posts");
        actual.AddColumn<int>("id").AsPrimaryKey();
        actual.AddColumn<int>("user_id");

        var delta = new TableDelta(expected, actual);

        // Foreign key changes require table recreation in SQLite
        delta.Difference.ShouldBe(SchemaPatchDifference.Invalid);
        delta.RequiresTableRecreation.ShouldBeTrue();
    }

    [Fact]
    public void invalid_when_primary_key_changed()
    {
        var expected = new Table("users");
        expected.AddColumn<int>("id").AsPrimaryKey();
        expected.AddColumn<string>("email");

        var actual = new Table("users");
        actual.AddColumn<int>("id");
        actual.AddColumn<string>("email").AsPrimaryKey();

        var delta = new TableDelta(expected, actual);

        delta.Difference.ShouldBe(SchemaPatchDifference.Invalid);
        delta.RequiresTableRecreation.ShouldBeTrue();
        delta.PrimaryKeyDifference.ShouldBe(SchemaPatchDifference.Update);
    }

    [Fact]
    public void write_update_for_simple_column_add()
    {
        var expected = new Table("users");
        expected.AddColumn<int>("id").AsPrimaryKey();
        expected.AddColumn<string>("name");
        expected.AddColumn<string>("email");

        var actual = new Table("users");
        actual.AddColumn<int>("id").AsPrimaryKey();
        actual.AddColumn<string>("name");

        var delta = new TableDelta(expected, actual);
        var writer = new StringWriter();
        var migrator = new SqliteMigrator();

        delta.WriteUpdate(migrator, writer);
        var ddl = writer.ToString();

        ddl.ShouldContain("ALTER TABLE");
        ddl.ShouldContain("ADD COLUMN");
        ddl.ShouldContain("email");
    }

    [Fact]
    public void write_update_for_table_recreation()
    {
        var expected = new Table("users");
        expected.AddColumn<int>("id").AsPrimaryKey();
        expected.AddColumn<string>("age");

        var actual = new Table("users");
        actual.AddColumn<int>("id").AsPrimaryKey();
        actual.AddColumn<int>("age");

        var delta = new TableDelta(expected, actual);
        var writer = new StringWriter();
        var migrator = new SqliteMigrator();

        delta.WriteUpdate(migrator, writer);
        var ddl = writer.ToString();

        ddl.ShouldContain("Table recreation required");
        ddl.ShouldContain("users_new");
        ddl.ShouldContain("INSERT INTO");
        ddl.ShouldContain("DROP TABLE");
        ddl.ShouldContain("ALTER TABLE");
        ddl.ShouldContain("RENAME TO");
    }

    [Fact]
    public void write_create_for_new_table()
    {
        var expected = new Table("users");
        expected.AddColumn<int>("id").AsPrimaryKey();
        expected.AddColumn<string>("name");

        var delta = new TableDelta(expected, null);
        var writer = new StringWriter();
        var migrator = new SqliteMigrator();

        delta.WriteUpdate(migrator, writer);
        var ddl = writer.ToString();

        ddl.ShouldContain("CREATE TABLE");
        ddl.ShouldContain("users");
        ddl.ShouldContain("id");
        ddl.ShouldContain("name");
    }
}
