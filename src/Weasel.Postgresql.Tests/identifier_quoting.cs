using Shouldly;
using Weasel.Postgresql.Tables;
using Xunit;

namespace Weasel.Postgresql.Tests;

/// <summary>
///     PostgreSQL's <c>QuoteName</c> quoted for keywords and for case, but not for shape — a
///     lowercase name containing a hyphen, a dot or a leading digit went out bare and produced DDL
///     that will not parse. It also never doubled an embedded double quote. See weasel#447.
/// </summary>
public class identifier_quoting
{
    [Theory]
    [InlineData("orders", "orders")]                                 // ordinary, still bare
    [InlineData("_internal", "_internal")]
    [InlineData("MiXeD", "\"MiXeD\"")]                               // case preserved, as before
    [InlineData("select", "\"select\"")]                             // reserved keyword, as before
    [InlineData("order-date", "\"order-date\"")]                     // hyphen: went out bare before
    [InlineData("unit price", "\"unit price\"")]                     // space: went out bare before
    [InlineData("2ndplace", "\"2ndplace\"")]                         // leading digit: bare before
    [InlineData("pk_dbo.__migrationhistory", "\"pk_dbo.__migrationhistory\"")] // dot: bare before
    public void quotes_for_shape_as_well_as_for_case_and_keywords(string name, string expected)
    {
        SchemaUtils.QuoteName(name, SchemaUtils.IdentifierUsage.General).ShouldBe(expected);
    }

    [Fact]
    public void an_embedded_double_quote_is_doubled_rather_than_closing_the_quoting()
    {
        var quoted = SchemaUtils.QuoteName("we\"ird", SchemaUtils.IdentifierUsage.General);

        quoted.ShouldBe("\"we\"\"ird\"");
        SchemaUtils.Unquote(quoted).ShouldBe("we\"ird");
    }

    [Fact]
    public void a_name_carrying_ddl_stays_inside_its_delimiters()
    {
        var injection = "ix\" ON t(id); DROP TABLE victim; --";

        var quoted = SchemaUtils.QuoteName(injection, SchemaUtils.IdentifierUsage.General);

        quoted.ShouldBe("\"ix\"\" ON t(id); DROP TABLE victim; --\"");
        SchemaUtils.Unquote(quoted).ShouldBe(injection);
    }

    /// <summary>
    ///     The usage distinction has to survive the refactoring: a type/function name keyword is
    ///     quoted in general use but not as a function name, and a column name keyword the reverse.
    /// </summary>
    [Fact]
    public void the_usage_distinction_still_decides_the_keyword_categories()
    {
        // "left" is a type/function name keyword (catcode T)
        SchemaUtils.QuoteName("left", SchemaUtils.IdentifierUsage.General).ShouldBe("\"left\"");
        SchemaUtils.QuoteName("left", SchemaUtils.IdentifierUsage.Function).ShouldBe("left");

        // "between" is a column name keyword (catcode C)
        SchemaUtils.QuoteName("between", SchemaUtils.IdentifierUsage.Function).ShouldBe("\"between\"");
        SchemaUtils.QuoteName("between", SchemaUtils.IdentifierUsage.General).ShouldBe("between");
    }

    [Fact]
    public void a_name_the_caller_already_quoted_is_left_alone()
    {
        SchemaUtils.QuoteName("\"orders\"", SchemaUtils.IdentifierUsage.General).ShouldBe("\"orders\"");
        SchemaUtils.QuoteName("\"we\"\"ird\"", SchemaUtils.IdentifierUsage.General).ShouldBe("\"we\"\"ird\"");
    }

    /// <summary>
    ///     The identity half: the catalog reports the bare name, so a model holding the quoted
    ///     spelling never compares equal to it and the table drifts on every check.
    /// </summary>
    [Fact]
    public void names_the_caller_quoted_are_normalized_on_the_way_into_the_model()
    {
        var table = new Table("quoting.normalized");
        table.AddColumn("\"id\"", "int").AsPrimaryKey();
        table.PrimaryKeyName = "\"pk normalized\"";

        table.Columns[0].Name.ShouldBe("id");
        table.PrimaryKeyName.ShouldBe("pk normalized");

        new IndexDefinition("\"ix normalized\"").Name.ShouldBe("ix normalized");
        new ForeignKey("\"fk normalized\"").Name.ShouldBe("fk normalized");
    }
}
