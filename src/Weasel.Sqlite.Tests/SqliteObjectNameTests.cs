using Shouldly;
using Weasel.Core;
using Weasel.Sqlite;
using Xunit;

namespace Weasel.Sqlite.Tests;

public class SqliteObjectNameTests
{
    [Fact]
    public void parse_simple_name()
    {
        var name = DbObjectName.Parse(SqliteProvider.Instance, "users");

        name.Name.ShouldBe("users");
        name.Schema.ShouldBe("main");
    }

    [Fact]
    public void parse_qualified_name()
    {
        var name = DbObjectName.Parse(SqliteProvider.Instance, "mydb.users");

        name.Name.ShouldBe("users");
        name.Schema.ShouldBe("mydb");
    }

    [Fact]
    public void qualified_name_uses_schema_and_name()
    {
        var name = new SqliteObjectName("mydb", "users");

        name.QualifiedName.ShouldBe("mydb.users");
    }

    [Fact]
    public void qualified_name_with_reserved_word()
    {
        var name = new SqliteObjectName("main", "order");

        // QualifiedName returns the schema.name format
        name.QualifiedName.ShouldBe("main.order");
    }

    [Fact]
    public void comparison_is_case_insensitive()
    {
        var name1 = new SqliteObjectName("main", "Users");
        var name2 = new SqliteObjectName("main", "users");

        name1.Equals(name2).ShouldBeTrue();
    }

    [Fact]
    public void schema_comparison_is_case_insensitive()
    {
        var name1 = new SqliteObjectName("Main", "users");
        var name2 = new SqliteObjectName("main", "users");

        name1.Equals(name2).ShouldBeTrue();
    }

    [Fact]
    public void different_names_are_not_equal()
    {
        var name1 = new SqliteObjectName("main", "users");
        var name2 = new SqliteObjectName("main", "posts");

        name1.Equals(name2).ShouldBeFalse();
    }

    [Fact]
    public void different_schemas_are_not_equal()
    {
        var name1 = new SqliteObjectName("main", "users");
        var name2 = new SqliteObjectName("temp", "users");

        name1.Equals(name2).ShouldBeFalse();
    }

    [Fact]
    public void to_string_returns_qualified_name()
    {
        var name = new SqliteObjectName("mydb", "users");

        name.ToString().ShouldBe("mydb.users");
    }
}
