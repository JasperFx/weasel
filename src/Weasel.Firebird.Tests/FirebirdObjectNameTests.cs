using Shouldly;
using Weasel.Core;
using Xunit;

namespace Weasel.Firebird.Tests;

public class FirebirdObjectNameTests
{
    [Fact]
    public void a_bare_name_lives_in_the_default_schema()
    {
        var name = new FirebirdObjectName("orders");

        name.Schema.ShouldBe("PUBLIC");
        name.Name.ShouldBe("orders");
    }

    [Fact]
    public void the_default_schema_is_never_part_of_the_qualified_name()
    {
        new FirebirdObjectName("PUBLIC", "orders").QualifiedName.ShouldBe("orders");
        new FirebirdObjectName("public", "orders").QualifiedName.ShouldBe("orders");
        new FirebirdObjectName("", "orders").QualifiedName.ShouldBe("orders");
    }

    [Fact]
    public void to_string_is_the_name_ddl_uses()
    {
        new FirebirdObjectName("orders").ToString().ShouldBe("orders");
        new FirebirdObjectName("order").ToString().ShouldBe("\"ORDER\"");
    }

    [Fact]
    public void a_name_that_needs_delimiting_is_delimited_in_the_folded_spelling()
    {
        new FirebirdObjectName("order date").QualifiedName.ShouldBe("\"ORDER DATE\"");
        new FirebirdObjectName("Table").QualifiedName.ShouldBe("\"TABLE\"");
    }

    [Fact]
    public void a_delimited_name_is_held_bare()
    {
        var name = new FirebirdObjectName("\"PUBLIC\"", "\"MY TABLE\"");

        name.Schema.ShouldBe("PUBLIC");
        name.Name.ShouldBe("MY TABLE");
    }

    [Fact]
    public void another_schema_is_kept_rather_than_refused_on_construction()
    {
        var name = new FirebirdObjectName("sales", "orders");

        name.Schema.ShouldBe("sales");
        name.QualifiedName.ShouldBe("sales.orders");
    }

    [Fact]
    public void names_compare_ignoring_case()
    {
        new FirebirdObjectName("orders").ShouldBe(new FirebirdObjectName("ORDERS"));
        new FirebirdObjectName("orders").GetHashCode()
            .ShouldBe(new FirebirdObjectName("ORDERS").GetHashCode());
    }

    [Fact]
    public void the_default_schema_spelled_out_is_the_same_object()
    {
        new FirebirdObjectName("PUBLIC", "orders").ShouldBe(new FirebirdObjectName("orders"));
    }

    [Fact]
    public void from_keeps_a_firebird_name_and_converts_anything_else()
    {
        var firebird = new FirebirdObjectName("orders");
        FirebirdObjectName.From(firebird).ShouldBeSameAs(firebird);

#pragma warning disable CS0618
        var converted = FirebirdObjectName.From(new DbObjectName("PUBLIC", "orders"));
#pragma warning restore CS0618
        converted.ShouldBeOfType<FirebirdObjectName>();
        converted.QualifiedName.ShouldBe("orders");
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("PUBLIC")]
    [InlineData("public")]
    public void the_default_schema_passes_the_ddl_boundary(string? schema)
    {
        FirebirdObjectName.AssertDefaultSchema(schema);
    }

    [Fact]
    public void another_schema_is_refused_at_the_ddl_boundary_with_a_reason()
    {
        var ex = Should.Throw<NotSupportedException>(() =>
            FirebirdObjectName.AssertDefaultSchema("sales", "table orders"));

        ex.Message.ShouldContain("table orders in schema 'sales'");
        ex.Message.ShouldContain("no schemas");
        ex.Message.ShouldContain("PUBLIC");
    }
}
