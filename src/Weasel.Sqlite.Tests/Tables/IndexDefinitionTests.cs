using Shouldly;
using Weasel.Sqlite.Tables;
using Xunit;

namespace Weasel.Sqlite.Tests.Tables;

public class IndexDefinitionTests
{
    [Fact]
    public void can_create_simple_index()
    {
        var table = new Table("users");
        var index = new IndexDefinition("idx_users_email");
        index.AgainstColumns("email");

        var ddl = index.ToDDL(table);

        ddl.ShouldContain("CREATE INDEX IF NOT EXISTS");
        ddl.ShouldContain("idx_users_email");
        ddl.ShouldContain("ON");
        ddl.ShouldContain("users");
        ddl.ShouldContain("email");
    }

    [Fact]
    public void can_create_unique_index()
    {
        var table = new Table("users");
        var index = new IndexDefinition("idx_users_email") { IsUnique = true };
        index.AgainstColumns("email");

        var ddl = index.ToDDL(table);

        ddl.ShouldContain("CREATE UNIQUE INDEX");
    }

    [Fact]
    public void can_create_multi_column_index()
    {
        var table = new Table("users");
        var index = new IndexDefinition("idx_users_name_email");
        index.AgainstColumns("last_name", "first_name", "email");

        var ddl = index.ToDDL(table);

        ddl.ShouldContain("last_name");
        ddl.ShouldContain("first_name");
        ddl.ShouldContain("email");
    }

    [Fact]
    public void can_create_descending_index()
    {
        var table = new Table("posts");
        var index = new IndexDefinition("idx_posts_created");
        index.AgainstColumns("created_at");
        index.SortOrder = SortOrder.Desc;

        var ddl = index.ToDDL(table);

        ddl.ShouldContain("DESC");
    }

    [Fact]
    public void can_create_partial_index()
    {
        var table = new Table("users");
        var index = new IndexDefinition("idx_active_users");
        index.AgainstColumns("email");
        index.Predicate = "is_active = 1";

        var ddl = index.ToDDL(table);

        ddl.ShouldContain("WHERE");
        ddl.ShouldContain("is_active = 1");
    }

    [Fact]
    public void can_create_expression_index()
    {
        var table = new Table("users");
        var index = new IndexDefinition("idx_users_lower_email");
        index.WithExpression("lower(email)");

        var ddl = index.ToDDL(table);

        ddl.ShouldContain("lower(email)");
    }

    [Fact]
    public void can_create_json_path_index()
    {
        var table = new Table("users");
        var index = new IndexDefinition("idx_settings_theme");
        index.ForJsonPath("settings", "$.theme");

        var ddl = index.ToDDL(table);

        ddl.ShouldContain("json_extract");
        ddl.ShouldContain("settings");
        ddl.ShouldContain("$.theme");
    }

    [Fact]
    public void can_create_json_path_index_with_collation()
    {
        var table = new Table("users");
        var index = new IndexDefinition("idx_settings_lang");
        index.ForJsonPath("settings", "$.language");
        index.Collation = "NOCASE";

        var ddl = index.ToDDL(table);

        ddl.ShouldContain("json_extract");
        ddl.ShouldContain("COLLATE");
        ddl.ShouldContain("NOCASE");
    }

    [Fact]
    public void should_quote_column_names()
    {
        var table = new Table("users");
        var index = new IndexDefinition("idx_users_order");
        index.AgainstColumns("order"); // 'order' is a reserved keyword

        var ddl = index.ToDDL(table);

        // Should be quoted because 'order' is reserved
        ddl.ShouldContain("\"order\"");
    }

    [Fact]
    public void should_throw_if_no_columns_or_expression()
    {
        var table = new Table("users");
        var index = new IndexDefinition("idx_invalid");

        Should.Throw<InvalidOperationException>(() => index.ToDDL(table));
    }

    [Fact]
    public void can_chain_fluent_methods()
    {
        var table = new Table("users");
        var index = new IndexDefinition("idx_users_email");

        var result = index
            .AgainstColumns("email")
            .ForJsonPath("settings", "$.theme")
            .WithExpression("lower(name)");

        result.ShouldBeSameAs(index);
    }
}
