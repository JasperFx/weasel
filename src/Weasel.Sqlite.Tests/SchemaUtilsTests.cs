using Shouldly;
using Weasel.Sqlite;
using Xunit;

namespace Weasel.Sqlite.Tests;

public class SchemaUtilsTests
{
    [Theory]
    [InlineData("SELECT")]
    [InlineData("select")]
    [InlineData("WHERE")]
    [InlineData("ORDER")]
    [InlineData("order")]
    [InlineData("TABLE")]
    [InlineData("INDEX")]
    public void reserved_keywords_are_quoted(string keyword)
    {
        // Reserved keywords should be quoted
        var quoted = SchemaUtils.QuoteName(keyword);
        quoted.ShouldStartWith("\"");
        quoted.ShouldEndWith("\"");
    }

    [Theory]
    [InlineData("users", "users")]
    [InlineData("my_table", "my_table")]
    [InlineData("id", "id")]
    [InlineData("name", "name")]
    public void quote_normal_identifiers_returns_unquoted(string name, string expected)
    {
        SchemaUtils.QuoteName(name).ShouldBe(expected);
    }

    [Theory]
    [InlineData("SELECT", "\"SELECT\"")]
    [InlineData("order", "\"order\"")]
    [InlineData("WHERE", "\"WHERE\"")]
    [InlineData("table", "\"table\"")]
    [InlineData("index", "\"index\"")]
    public void quote_reserved_keywords(string name, string expected)
    {
        SchemaUtils.QuoteName(name).ShouldBe(expected);
    }

    [Theory]
    [InlineData("my table", "\"my table\"")]
    [InlineData("first-name", "\"first-name\"")]
    [InlineData("user name", "\"user name\"")]
    public void quote_names_with_special_characters(string name, string expected)
    {
        SchemaUtils.QuoteName(name).ShouldBe(expected);
    }

    [Theory]
    [InlineData("1users", "\"1users\"")]
    [InlineData("9column", "\"9column\"")]
    public void quote_names_starting_with_digit(string name, string expected)
    {
        SchemaUtils.QuoteName(name).ShouldBe(expected);
    }

    [Theory]
    [InlineData("my column\"test", "\"my column\"\"test\"")]  // Has space, so gets quoted and embedded quote doubled
    [InlineData("test-col\"umn", "\"test-col\"\"umn\"")]       // Has hyphen, gets quoted and embedded quote doubled
    [InlineData("9col\"umn", "\"9col\"\"umn\"")]              // Starts with digit, gets quoted and embedded quote doubled
    public void quote_name_with_embedded_quotes_when_quoting_needed(string name, string expected)
    {
        var quoted = SchemaUtils.QuoteName(name);
        quoted.ShouldBe(expected);
    }

    [Fact]
    public void quote_name_with_embedded_quotes_no_quoting_needed()
    {
        // If the name doesn't meet any criteria for quoting, embedded quotes are left as-is
        var name = "my\"column";
        var quoted = SchemaUtils.QuoteName(name);

        // Since "my\"column" doesn't have spaces, isn't a keyword, doesn't start with digit, it's not quoted
        quoted.ShouldBe("my\"column");
    }

    [Theory]
    [InlineData("SELECT")]
    [InlineData("INSERT")]
    [InlineData("UPDATE")]
    [InlineData("DELETE")]
    [InlineData("CREATE")]
    [InlineData("DROP")]
    [InlineData("ALTER")]
    [InlineData("TABLE")]
    [InlineData("INDEX")]
    [InlineData("WHERE")]
    [InlineData("ORDER")]
    [InlineData("GROUP")]
    [InlineData("HAVING")]
    [InlineData("JOIN")]
    [InlineData("UNION")]
    public void common_sql_keywords_are_quoted(string keyword)
    {
        // All common SQL keywords should be quoted
        var quoted = SchemaUtils.QuoteName(keyword);
        quoted.ShouldBe($"\"{keyword}\"");
    }
}
