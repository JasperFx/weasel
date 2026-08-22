using Shouldly;
using Xunit;

namespace Weasel.SqlServer.Tests;

/// <summary>
///     <see cref="Canonicalization.CanonicizeSql" /> reconciles a declared T-SQL function body with
///     what <c>sys.sql_modules</c> stores.
/// </summary>
public class CanonicalizationTests
{
    [Theory]
    [InlineData("CREATE OR ALTER FUNCTION dbo.f()")]
    [InlineData("Create Or Alter Function dbo.f()")]
    [InlineData("CREATE OR ALTER\r\nFUNCTION dbo.f()")]
    [InlineData("CREATE FUNCTION dbo.f()")]
    public void the_preamble_reduces_to_what_the_catalog_returns(string declared)
    {
        // OR ALTER is blanked in place, so the catalog holds CREATE + 3 spaces + FUNCTION. Every
        // spelling of the preamble has to meet it there.
        declared.CanonicizeSql().ShouldBe("CREATE   FUNCTION dbo.f()".CanonicizeSql(), StringCompareShould.IgnoreCase);
    }

    [Fact]
    public void the_declared_casing_is_kept()
    {
        "create   function dbo.f()".CanonicizeSql().ShouldBe("create function dbo.f()");
    }

    [Fact]
    public void surrounding_whitespace_and_a_trailing_semicolon_are_trimmed()
    {
        "  select 1;  ".CanonicizeSql().ShouldBe("select 1");
    }

    [Fact]
    public void whitespace_inside_a_string_literal_is_left_alone()
    {
        // Collapsing it would change the value the function returns, and fold this body together
        // with one that returns something different.
        "select 'foo       bar'".CanonicizeSql().ShouldBe("select 'foo       bar'");
    }

    [Fact]
    public void whitespace_inside_a_delimited_identifier_is_left_alone()
    {
        // [unit  price] and [unit price] are two different columns.
        "select [unit  price] from t".CanonicizeSql().ShouldBe("select [unit  price] from t");
    }

    [Fact]
    public void line_endings_are_normalized()
    {
        // One source file, CRLF on one machine and LF on another, both applying to one database.
        "CREATE FUNCTION dbo.f()\r\nAS\r\nBEGIN\r\n    return 1;\r\nEND".CanonicizeSql()
            .ShouldBe("CREATE FUNCTION dbo.f()\nAS\nBEGIN\n    return 1;\nEND");
    }

    [Fact]
    public void nothing_else_is_normalized()
    {
        // Indentation and internal spacing are compared as written, so reformatting a body is a
        // change. It costs one drop and recreate of the function, after which it stays clean.
        const string sql = "CREATE FUNCTION dbo.f()\nAS\nBEGIN\n\treturn 'a  b';\nEND";

        sql.CanonicizeSql().ShouldBe(sql);
    }
}
