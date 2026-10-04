using Shouldly;
using Xunit;

namespace Weasel.Firebird.Tests;

public class FirebirdIdentifierRulesTests
{
    private static readonly FirebirdIdentifierRules Rules = FirebirdIdentifierRules.Instance;

    [Theory]
    [InlineData("orders")]
    [InlineData("ORDERS")]
    [InlineData("MixedCase")]
    [InlineData("price$")]
    [InlineData("qrtz_job_details")]
    [InlineData("a1")]
    public void a_regular_identifier_is_written_bare(string name)
    {
        Rules.IsRegularIdentifier(name).ShouldBeTrue();
        SchemaUtils.QuoteName(name).ShouldBe(name);
    }

    /// <summary>
    ///     Measured against Firebird 3, 4 and 5: an unquoted leading underscore or digit is a syntax
    ///     error, and so is any character outside ASCII.
    /// </summary>
    [Theory]
    [InlineData("_internal", "\"_INTERNAL\"")]
    [InlineData("2ndPlace", "\"2NDPLACE\"")]
    [InlineData("order date", "\"ORDER DATE\"")]
    [InlineData("order-date", "\"ORDER-DATE\"")]
    [InlineData("col#1", "\"COL#1\"")]
    [InlineData("Grüße", "\"GRÜßE\"")]
    public void anything_else_is_delimited_in_the_folded_spelling(string name, string expected)
    {
        Rules.IsRegularIdentifier(name).ShouldBeFalse();
        SchemaUtils.QuoteName(name).ShouldBe(expected);
    }

    /// <summary>
    ///     <c>VALUE</c> and <c>POSITION</c> need quoting even though they read like ordinary column
    ///     names; <c>KEY</c> does not, although it is reserved almost everywhere else.
    /// </summary>
    [Theory]
    [InlineData("value", true)]
    [InlineData("position", true)]
    [InlineData("order", true)]
    [InlineData("TABLE", true)]
    [InlineData("comment", true)]
    [InlineData("int128", true)]
    [InlineData("decfloat", true)]
    [InlineData("rdb$db_key", true)]
    [InlineData("key", false)]
    [InlineData("name", false)]
    [InlineData("data", false)]
    public void knows_the_reserved_words_of_firebird_3_4_and_5(string word, bool reserved)
    {
        SchemaUtils.IsReservedKeyword(word).ShouldBe(reserved);
    }

    [Fact]
    public void a_reserved_word_is_delimited_in_the_folded_spelling()
    {
        SchemaUtils.QuoteName("value").ShouldBe("\"VALUE\"");
    }

    [Fact]
    public void an_embedded_double_quote_is_doubled()
    {
        Rules.Delimit("a\"b").ShouldBe("\"a\"\"b\"");
        SchemaUtils.Unquote("\"a\"\"b\"").ShouldBe("a\"b");
    }

    [Fact]
    public void a_caret_is_legal_inside_a_delimited_identifier()
    {
        SchemaUtils.QuoteName("a^b").ShouldBe("\"A^B\"");
    }

    [Fact]
    public void two_spellings_that_differ_only_in_case_are_one_object()
    {
        Rules.SameObject("orders", "ORDERS").ShouldBeTrue();
        Rules.SameObject("orders", "order").ShouldBeFalse();
    }

    [Fact]
    public void preserving_case_delimits_the_name_exactly_as_written()
    {
        SchemaUtils.QuoteName("BlogId", preserveCase: true).ShouldBe("\"BlogId\"");
        SchemaUtils.QuoteName("BlogId", preserveCase: false).ShouldBe("BlogId");
    }

    [Theory]
    [InlineData("orders", false, "ORDERS")]
    [InlineData("order date", false, "ORDER DATE")]
    [InlineData("QRTZ_LOCKS", false, "QRTZ_LOCKS")]
    [InlineData("BlogId", true, "BlogId")]
    public void the_catalog_name_is_the_spelling_the_ddl_lands_on(string name, bool preserveCase, string expected)
    {
        SchemaUtils.CatalogName(name, preserveCase).ShouldBe(expected);
    }
}
