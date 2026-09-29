using Shouldly;
using Weasel.Core;
using Weasel.Firebird.Tables;
using Xunit;

namespace Weasel.Firebird.Tests.Tables;

public class IndexDefinitionTests
{
    private readonly Table parent = new("people");

    [Fact]
    public void a_plain_index()
    {
        new IndexDefinition("idx_people_name") { Columns = ["name"] }.ToDDL(parent)
            .ShouldBe("CREATE INDEX idx_people_name ON people (name)");
    }

    [Fact]
    public void a_unique_index_over_several_columns()
    {
        new IndexDefinition("idx_people_name") { Columns = ["last_name", "first_name"], IsUnique = true }
            .ToDDL(parent).ShouldBe("CREATE UNIQUE INDEX idx_people_name ON people (last_name, first_name)");
    }

    [Fact]
    public void names_are_quoted_where_firebird_needs_it()
    {
        new IndexDefinition("idx order") { Columns = ["order date", "value"] }.ToDDL(new Table("my table"))
            .ShouldBe("CREATE INDEX \"IDX ORDER\" ON \"MY TABLE\" (\"ORDER DATE\", \"VALUE\")");
    }

    [Fact]
    public void a_descending_index_by_sort_order()
    {
        new IndexDefinition("idx_desc") { Columns = ["a", "b"], SortOrder = SortOrder.Desc }.ToDDL(parent)
            .ShouldBe("CREATE DESCENDING INDEX idx_desc ON people (a, b)");
    }

    [Fact]
    public void descending_columns_that_name_every_key_column_is_a_descending_index()
    {
        var index = new IndexDefinition("idx_desc") { Columns = ["a", "b"] };
        index.DescendingColumns.Add("A");
        index.DescendingColumns.Add("b");

        index.ToDDL(parent).ShouldBe("CREATE DESCENDING INDEX idx_desc ON people (a, b)");
    }

    /// <summary>
    ///     A Firebird index is ascending or descending as a whole (a per-column direction is a syntax
    ///     error), so a mix is refused when the DDL is rendered -- which is before anything runs.
    /// </summary>
    [Fact]
    public void mixed_directions_are_refused_when_rendered()
    {
        var index = new IndexDefinition("idx_mixed") { Columns = ["a", "b"] };
        index.DescendingColumns.Add("b");

        var ex = Should.Throw<InvalidOperationException>(() => index.ToDDL(parent));
        ex.Message.ShouldContain("mixes directions");
        ex.Message.ShouldContain("a ascending");
    }

    [Fact]
    public void a_descending_column_that_is_not_a_key_column_is_refused()
    {
        var index = new IndexDefinition("idx_stray") { Columns = ["a"] };
        index.DescendingColumns.Add("nope");

        Should.Throw<InvalidOperationException>(() => index.ToDDL(parent)).Message.ShouldContain("nope");
    }

    [Fact]
    public void an_expression_index()
    {
        new IndexDefinition("idx_upper") { Expression = "UPPER(name)" }.ToDDL(parent)
            .ShouldBe("CREATE INDEX idx_upper ON people COMPUTED BY (UPPER(name))");
    }

    [Fact]
    public void an_expression_index_takes_its_direction_from_sort_order_only()
    {
        var index = new IndexDefinition("idx_upper") { Expression = "UPPER(name)", SortOrder = SortOrder.Desc };
        index.ToDDL(parent).ShouldBe("CREATE DESCENDING INDEX idx_upper ON people COMPUTED BY (UPPER(name))");

        index.DescendingColumns.Add("name");
        Should.Throw<InvalidOperationException>(() => index.ToDDL(parent));
    }

    [Fact]
    public void a_partial_index()
    {
        new IndexDefinition("idx_active") { Columns = ["name"], Predicate = "active = TRUE" }.ToDDL(parent)
            .ShouldBe("CREATE INDEX idx_active ON people (name) WHERE active = TRUE");
    }

    [Fact]
    public void an_index_with_neither_columns_nor_an_expression_is_refused()
    {
        Should.Throw<InvalidOperationException>(() => new IndexDefinition("idx_empty").ToDDL(parent));
    }

    [Fact]
    public void a_case_preserving_table_delimits_every_name_exactly()
    {
        var table = new Table("Blogs") { PreserveIdentifierCase = true };

        new IndexDefinition("IX_Blogs_Url") { Columns = ["Url"] }.ToDDL(table)
            .ShouldBe("CREATE INDEX \"IX_Blogs_Url\" ON \"Blogs\" (\"Url\")");
    }

    [Fact]
    public void an_index_method_is_refused()
    {
        ITableIndex index = new IndexDefinition("idx");

        index.Method = null;
        index.Method.ShouldBeNull();
        Should.Throw<NotSupportedException>(() => index.Method = "hash");
    }

    [Fact]
    public void included_columns_are_refused()
    {
        ITableIndex index = new IndexDefinition("idx");

        index.IncludeColumns = null;
        index.IncludeColumns.ShouldBeNull();
        Should.Throw<NotSupportedException>(() => index.IncludeColumns = ["a"]);
    }

    [Fact]
    public void direction_and_expressions_are_provider_specific()
    {
        ((ITableIndex)new IndexDefinition("idx") { Columns = ["a"] }).HasProviderSpecificOptions.ShouldBeFalse();
        ((ITableIndex)new IndexDefinition("idx") { Columns = ["a"], SortOrder = SortOrder.Desc })
            .HasProviderSpecificOptions.ShouldBeTrue();
        ((ITableIndex)new IndexDefinition("idx") { Expression = "UPPER(a)" }).HasProviderSpecificOptions.ShouldBeTrue();
    }

    [Fact]
    public void matches_ignoring_case()
    {
        var model = new IndexDefinition("idx_people_name") { Columns = ["name"] };
        var catalog = new IndexDefinition("IDX_PEOPLE_NAME") { Columns = ["NAME"] };

        model.Matches(catalog, parent).ShouldBeTrue();
    }

    [Theory]
    [InlineData(true, false, false)]
    [InlineData(false, true, false)]
    [InlineData(false, false, true)]
    public void uniqueness_direction_and_activity_are_compared(bool unique, bool descending, bool inactive)
    {
        var model = new IndexDefinition("idx") { Columns = ["name"] };
        var catalog = new IndexDefinition("IDX")
        {
            Columns = ["NAME"],
            IsUnique = unique,
            SortOrder = descending ? SortOrder.Desc : SortOrder.Asc,
            IsInactive = inactive
        };

        model.Matches(catalog, parent).ShouldBeFalse();
    }

    [Fact]
    public void column_order_is_compared()
    {
        new IndexDefinition("idx") { Columns = ["a", "b"] }
            .Matches(new IndexDefinition("IDX") { Columns = ["B", "A"] }, parent).ShouldBeFalse();
    }

    [Fact]
    public void descending_columns_match_a_descending_index_read_back()
    {
        var model = new IndexDefinition("idx") { Columns = ["a", "b"] };
        model.DescendingColumns.Add("a");
        model.DescendingColumns.Add("b");

        model.Matches(new IndexDefinition("IDX") { Columns = ["A", "B"], SortOrder = SortOrder.Desc }, parent)
            .ShouldBeTrue();
    }

    [Fact]
    public void an_expression_and_a_condition_are_compared_ignoring_case_and_whitespace()
    {
        var model = new IndexDefinition("idx") { Expression = "upper(name)", Predicate = "b > 0 and c is not null" };
        var catalog = new IndexDefinition("IDX") { Expression = "UPPER( NAME )", Predicate = "B > 0  AND C IS NOT NULL" };

        model.Matches(catalog, parent).ShouldBeTrue();
    }

    [Fact]
    public void a_literal_in_an_expression_keeps_its_case()
    {
        new IndexDefinition("idx") { Expression = "COALESCE(a, 'x')" }
            .Matches(new IndexDefinition("IDX") { Expression = "COALESCE(A, 'X')" }, parent).ShouldBeFalse();
    }

    [Theory]
    [InlineData("(UPPER(A))", "UPPER(A)")]
    [InlineData("  (UPPER(A)) ", "UPPER(A)")]
    [InlineData("(A) || (B)", "(A) || (B)")]
    [InlineData("UPPER(A)", "UPPER(A)")]
    [InlineData(null, null)]
    public void an_expression_read_back_loses_the_outer_parentheses_the_server_keeps(string? source, string? read)
    {
        IndexDefinition.ReadExpression(source).ShouldBe(read);
    }

    [Theory]
    [InlineData("WHERE b > 0 and C IS NOT NULL", "b > 0 and C IS NOT NULL")]
    [InlineData("where b > 0", "b > 0")]
    [InlineData(null, null)]
    public void a_condition_read_back_loses_its_where(string? source, string? read)
    {
        IndexDefinition.ReadPredicate(source).ShouldBe(read);
    }

    [Fact]
    public void assert_matches_names_both_sides()
    {
        var ex = Should.Throw<Exception>(() =>
            new IndexDefinition("idx") { Columns = ["a"] }
                .AssertMatches(new IndexDefinition("IDX") { Columns = ["B"] }, parent));

        ex.Message.ShouldContain("(a)");
        ex.Message.ShouldContain("(B)");
    }

    [Fact]
    public void names_and_columns_are_held_undelimited()
    {
        var index = new IndexDefinition("\"IDX X\"") { Columns = ["\"A B\""] };
        index.AddColumn("\"C\"");

        index.Name.ShouldBe("IDX X");
        index.Columns.ShouldBe(["A B", "C"]);
    }
}
