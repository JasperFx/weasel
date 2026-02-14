using Shouldly;
using Weasel.Sqlite;
using Weasel.Sqlite.Tables;
using Xunit;

namespace Weasel.Sqlite.Tests;

public class SqliteObjectNameTests
{
    [Fact]
    public void main_schema_omits_schema_prefix()
    {
        var name = new SqliteObjectName("main", "users");

        name.ToString().ShouldBe("users");
        name.Schema.ShouldBe("main");
        name.Name.ShouldBe("users");
    }

    [Fact]
    public void main_schema_case_insensitive()
    {
        var name1 = new SqliteObjectName("MAIN", "users");
        var name2 = new SqliteObjectName("Main", "users");
        var name3 = new SqliteObjectName("main", "users");

        name1.ToString().ShouldBe("users");
        name2.ToString().ShouldBe("users");
        name3.ToString().ShouldBe("users");
    }

    [Fact]
    public void temp_schema_includes_schema_prefix()
    {
        var name = new SqliteObjectName("temp", "users");

        name.ToString().ShouldBe("\"temp\".users");
        name.Schema.ShouldBe("temp");
        name.Name.ShouldBe("users");
    }

    [Fact]
    public void equality_is_case_insensitive()
    {
        var name1 = new SqliteObjectName("main", "Users");
        var name2 = new SqliteObjectName("main", "users");

        name1.Equals(name2).ShouldBeTrue();
        name1.GetHashCode().ShouldBe(name2.GetHashCode());
    }

    [Fact]
    public void create_table_in_main_omits_schema()
    {
        var table = new Table("main.users");
        table.AddColumn<int>("id").AsPrimaryKey();

        var sql = table.ToBasicCreateTableSql();

        sql.ShouldContain("CREATE TABLE IF NOT EXISTS users");
        sql.ShouldNotContain("main.");
    }

    [Fact]
    public void create_table_in_temp_includes_schema()
    {
        var table = new Table(new SqliteObjectName("temp", "users"));
        table.AddColumn<int>("id").AsPrimaryKey();

        var sql = table.ToBasicCreateTableSql();

        sql.ShouldContain("CREATE TABLE IF NOT EXISTS \"temp\".users");
    }
}
