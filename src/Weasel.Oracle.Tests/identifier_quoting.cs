using Shouldly;
using Weasel.Oracle.Tables;
using Xunit;

namespace Weasel.Oracle.Tests;

/// <summary>
///     Oracle's <c>QuoteName</c> quoted only reserved keywords, so a name needing delimiting for its
///     shape — a space, a hyphen, a leading digit — went out bare and produced DDL that will not
///     parse. It also never doubled an embedded double quote. See weasel#447.
/// </summary>
public class identifier_quoting
{
    [Theory]
    [InlineData("orders", "orders")]                    // ordinary, still bare and still folded by Oracle
    [InlineData("MiXeD", "MiXeD")]                      // bare: Oracle folds it, as it always has
    [InlineData("table", "\"TABLE\"")]                  // reserved word, unchanged from before
    [InlineData("order date", "\"ORDER DATE\"")]        // space: went out bare before
    [InlineData("order-date", "\"ORDER-DATE\"")]        // hyphen: went out bare before
    [InlineData("2ndplace", "\"2NDPLACE\"")]            // leading digit: went out bare before
    public void quotes_for_shape_as_well_as_for_reserved_words(string name, string expected)
    {
        SchemaUtils.QuoteName(name).ShouldBe(expected);
    }

    /// <summary>
    ///     The folding is deliberate, and it is what the old implementation already did for reserved
    ///     words. Oracle resolves an undelimited identifier by folding it to upper case, so a name
    ///     that has to be delimited must be delimited in the folded spelling — otherwise
    ///     <c>"order date"</c> would name a different column from the <c>ORDER_DATE</c> that
    ///     everything else in the schema resolves to.
    /// </summary>
    [Fact]
    public void a_delimited_name_lands_on_the_object_the_bare_name_would_have()
    {
        SchemaUtils.QuoteName("table").ShouldBe("\"TABLE\"");
        SchemaUtils.QuoteName("TABLE").ShouldBe("\"TABLE\"");
        SchemaUtils.QuoteName("Table").ShouldBe("\"TABLE\"");
    }

    [Fact]
    public void an_embedded_double_quote_is_doubled_rather_than_closing_the_quoting()
    {
        var quoted = SchemaUtils.QuoteName("we\"ird");

        quoted.ShouldBe("\"WE\"\"IRD\"");
        SchemaUtils.Unquote(quoted).ShouldBe("WE\"IRD");
    }

    [Fact]
    public void a_name_carrying_ddl_stays_inside_its_delimiters()
    {
        var injection = "ix\" ON t(id); DROP TABLE victim; --";

        var quoted = SchemaUtils.QuoteName(injection);

        quoted.ShouldBe("\"IX\"\" ON T(ID); DROP TABLE VICTIM; --\"");
        SchemaUtils.Unquote(quoted).ShouldBe(injection.ToUpperInvariant());
    }

    [Fact]
    public void a_name_the_caller_already_quoted_is_left_alone()
    {
        // Passed through exactly, case included: a caller who delimits it themselves has said
        // precisely which object they mean.
        SchemaUtils.QuoteName("\"MyTable\"").ShouldBe("\"MyTable\"");
        SchemaUtils.QuoteName("\"we\"\"ird\"").ShouldBe("\"we\"\"ird\"");
    }

    [Fact]
    public void names_the_caller_quoted_are_normalized_on_the_way_into_the_model()
    {
        var table = new Table("WEASEL.quoting_normalized");
        table.AddColumn("\"ID\"", "NUMBER").AsPrimaryKey();
        table.PrimaryKeyName = "\"PK_QUOTING\"";

        table.Columns[0].Name.ShouldBe("id");
        table.PrimaryKeyName.ShouldBe("PK_QUOTING");

        new IndexDefinition("\"IX_QUOTING\"").Name.ShouldBe("IX_QUOTING");
        new ForeignKey("\"FK_QUOTING\"").Name.ShouldBe("FK_QUOTING");
    }

    /// <summary>
    ///     Object names reach string literals in the anonymous PL/SQL existence checks and the
    ///     catalog queries. A name carrying a single quote used to close the literal.
    /// </summary>
    [Fact]
    public void a_single_quote_in_a_name_is_escaped_for_a_literal()
    {
        SchemaUtils.EscapeLiteral("O'BRIEN").ShouldBe("O''BRIEN");
        SchemaUtils.EscapeLiteral("'; DROP TABLE VICTIM; --").ShouldBe("''; DROP TABLE VICTIM; --");
    }
}
