using Shouldly;
using Weasel.Sqlite.Tables;
using Xunit;

namespace Weasel.Sqlite.Tests.Tables;

public class IndexDefinitionTests
{
    private readonly Table table = new Table("users");

    [Fact]
    public void write_basic_index()
    {
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
    public void write_unique_index()
    {
        var index = new IndexDefinition("idx_users_email") { IsUnique = true };
        index.AgainstColumns("email");

        var ddl = index.ToDDL(table);

        ddl.ShouldContain("CREATE UNIQUE INDEX");
    }

    [Fact]
    public void write_multi_column_index()
    {
        var index = new IndexDefinition("idx_users_name_email");
        index.AgainstColumns("last_name", "first_name", "email");

        var ddl = index.ToDDL(table);

        ddl.ShouldContain("last_name");
        ddl.ShouldContain("first_name");
        ddl.ShouldContain("email");
    }

    [Fact]
    public void write_descending_index()
    {
        var table = new Table("posts");
        var index = new IndexDefinition("idx_posts_created");
        index.AgainstColumns("created_at");
        index.SortOrder = SortOrder.Desc;

        var ddl = index.ToDDL(table);

        ddl.ShouldContain("DESC");
    }

    [Fact]
    public void write_partial_index()
    {
        var index = new IndexDefinition("idx_active_users");
        index.AgainstColumns("email");
        index.Predicate = "is_active = 1";

        var ddl = index.ToDDL(table);

        ddl.ShouldContain("WHERE");
        ddl.ShouldContain("is_active = 1");
    }

    [Fact]
    public void write_expression_index()
    {
        var index = new IndexDefinition("idx_users_lower_email");
        index.WithExpression("lower(email)");

        var ddl = index.ToDDL(table);

        ddl.ShouldContain("lower(email)");
    }

    [Fact]
    public void write_json_path_index()
    {
        var index = new IndexDefinition("idx_settings_theme");
        index.ForJsonPath("settings", "$.theme");

        var ddl = index.ToDDL(table);

        ddl.ShouldContain("json_extract");
        ddl.ShouldContain("settings");
        ddl.ShouldContain("$.theme");
    }

    [Fact]
    public void write_json_path_index_with_collation()
    {
        var index = new IndexDefinition("idx_settings_lang");
        index.ForJsonPath("settings", "$.language");
        index.Collation = "NOCASE";

        var ddl = index.ToDDL(table);

        ddl.ShouldContain("json_extract");
        ddl.ShouldContain("COLLATE");
        ddl.ShouldContain("NOCASE");
    }

    [Fact]
    public void quote_reserved_column_names()
    {
        var index = new IndexDefinition("idx_users_order");
        index.AgainstColumns("order"); // 'order' is a reserved keyword

        var ddl = index.ToDDL(table);

        // Should be quoted because 'order' is reserved
        ddl.ShouldContain("\"order\"");
    }

    [Fact]
    public void throw_if_no_columns_or_expression()
    {
        var index = new IndexDefinition("idx_invalid");

        Should.Throw<InvalidOperationException>(() => index.ToDDL(table));
    }

    [Fact]
    public void chain_fluent_methods()
    {
        var index = new IndexDefinition("idx_users_email");

        var result = index
            .AgainstColumns("email")
            .ForJsonPath("settings", "$.theme")
            .WithExpression("lower(name)");

        result.ShouldBeSameAs(index);
    }

    [Fact]
    public void default_sort_order_is_asc()
    {
        var index = new IndexDefinition("idx_test");
        index.SortOrder.ShouldBe(SortOrder.Asc);
    }

    [Fact]
    public void is_not_unique_by_default()
    {
        var index = new IndexDefinition("idx_test");
        index.IsUnique.ShouldBeFalse();
    }

    [Fact]
    public void write_index_with_collation()
    {
        var index = new IndexDefinition("idx_users_name");
        index.AgainstColumns("name");
        index.Collation = "NOCASE";

        var ddl = index.ToDDL(table);

        ddl.ShouldContain("COLLATE NOCASE");
    }
}