using Shouldly;
using Xunit;

namespace Weasel.MySql.Tests;

public class MySqlObjectNameTests
{
    [Fact]
    public void qualified_name_with_schema()
    {
        var name = new MySqlObjectName("mydb", "users");
        name.QualifiedName.ShouldBe("`mydb`.`users`");
    }

    [Fact]
    public void qualified_name_without_schema()
    {
        var name = new MySqlObjectName("", "users");
        name.QualifiedName.ShouldBe("`users`");
    }

    [Fact]
    public void to_string_returns_qualified_name()
    {
        var name = new MySqlObjectName("mydb", "users");
        name.ToString().ShouldBe("`mydb`.`users`");
    }

    [Fact]
    public void name_comparison_is_case_insensitive()
    {
        var name1 = new MySqlObjectName("mydb", "Users");
        var name2 = new MySqlObjectName("mydb", "users");

        name1.Equals(name2).ShouldBeTrue();
    }

    [Fact]
    public void schema_comparison_is_case_insensitive()
    {
        var name1 = new MySqlObjectName("MyDb", "users");
        var name2 = new MySqlObjectName("mydb", "users");

        name1.Equals(name2).ShouldBeTrue();
    }

    [Fact]
    public void to_temp_copy_table_appends_temp_suffix()
    {
        var name = new MySqlObjectName("mydb", "users");
        var temp = name.ToTempCopyTable();

        temp.Name.ShouldBe("users_temp");
        temp.Schema.ShouldBe("mydb");
    }

    [Fact]
    public void to_index_name_generates_valid_name()
    {
        var name = new MySqlObjectName("mydb", "users");
        var indexName = name.ToIndexName("idx", "email", "name");

        indexName.ShouldBe("idx_users_email_name");
    }

    [Fact]
    public void to_index_name_truncates_long_names()
    {
        var name = new MySqlObjectName("mydb", "very_long_table_name_that_exceeds_normal_limits");
        var indexName = name.ToIndexName("idx", "very_long_column_name_one", "very_long_column_name_two");

        indexName.Length.ShouldBeLessThanOrEqualTo(64);
    }

    [Fact]
    public void parse_qualified_name()
    {
        var name = MySqlObjectName.Parse(MySqlProvider.Instance, "mydb.users") as MySqlObjectName;

        name.ShouldNotBeNull();
        name.Schema.ShouldBe("mydb");
        name.Name.ShouldBe("users");
    }

    [Fact]
    public void parse_unqualified_name_uses_default_schema()
    {
        var name = MySqlObjectName.Parse(MySqlProvider.Instance, "users") as MySqlObjectName;

        name.ShouldNotBeNull();
        name.Schema.ShouldBe("public");
        name.Name.ShouldBe("users");
    }
}
