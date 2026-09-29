using Shouldly;
using Weasel.SqlServer.Tables;
using Xunit;

namespace Weasel.SqlServer.Tests.Tables;

/// <summary>
///     SQL Server does not store <c>CAST</c>. It rewrites it to <c>CONVERT</c> when it persists a
///     definition, so a model declaring the natural spelling could never match its own column.
/// </summary>
/// <remarks>
///     Measured against <c>sys.computed_columns</c>:
///     <c>CAST(JSON_VALUE(data, '$.customerId') AS uniqueidentifier)</c> comes back as
///     <c>(CONVERT([uniqueidentifier],json_value([data],'$.customerId')))</c>. Stripping quoting,
///     parentheses and whitespace — all the shared canonicalization can do — leaves
///     <c>castjson_valuedata,'$.customerid'asuniqueidentifier</c> against
///     <c>convertuniqueidentifier,json_valuedata,'$.customerid'</c>: different in keyword
///     <em>and</em> argument order (weasel#637).
/// </remarks>
public class rewriting_casts_to_converts
{
    private static string rewrite(string expression) => SqlServerExpression.RewriteCastsToConverts(expression);

    [Fact]
    public void moves_the_target_type_to_the_front()
    {
        rewrite("CAST(x AS int)").ShouldBe("CONVERT(int, x)");
    }

    [Fact]
    public void carries_the_types_own_arguments_across()
    {
        rewrite("CAST(x AS varchar(250))").ShouldBe("CONVERT(varchar(250), x)");
        rewrite("CAST(x AS decimal(10, 2))").ShouldBe("CONVERT(decimal(10, 2), x)");
    }

    /// <summary>The reported shape: an operand carrying its own parentheses and a string literal.</summary>
    [Fact]
    public void rewrites_an_operand_holding_parens_and_a_literal()
    {
        rewrite("CAST(JSON_VALUE(data, '$.customerId') AS uniqueidentifier)")
            .ShouldBe("CONVERT(uniqueidentifier, JSON_VALUE(data, '$.customerId'))");
    }

    /// <summary>
    ///     A literal is stepped over, so a parenthesis or the word AS inside a JSON path cannot be read
    ///     as syntax. This is why the rewrite is scanned rather than matched with a regex.
    /// </summary>
    [Fact]
    public void does_not_read_syntax_out_of_a_string_literal()
    {
        rewrite("CAST(JSON_VALUE(data, '$.a AS b') AS int)")
            .ShouldBe("CONVERT(int, JSON_VALUE(data, '$.a AS b'))");

        rewrite("CAST(JSON_VALUE(data, '$.weird((') AS int)")
            .ShouldBe("CONVERT(int, JSON_VALUE(data, '$.weird(('))");
    }

    [Fact]
    public void rewrites_a_cast_nested_in_another_cast()
    {
        rewrite("CAST(CAST(x AS int) AS varchar(10))")
            .ShouldBe("CONVERT(varchar(10), CONVERT(int, x))");
    }

    [Fact]
    public void rewrites_every_cast_in_one_expression()
    {
        rewrite("CAST(a AS int) + CAST(b AS int)")
            .ShouldBe("CONVERT(int, a) + CONVERT(int, b)");
    }

    [Fact]
    public void leaves_a_convert_alone()
    {
        const string convert = "CONVERT(datetimeoffset, JSON_VALUE(data, '$.placed'), 126)";
        rewrite(convert).ShouldBe(convert);
    }

    [Fact]
    public void leaves_an_expression_with_no_cast_alone()
    {
        rewrite("JSON_VALUE([data], '$.name')").ShouldBe("JSON_VALUE([data], '$.name')");
    }

    /// <summary>CAST has to be a word of its own, not the tail of an identifier.</summary>
    [Fact]
    public void does_not_rewrite_an_identifier_that_merely_ends_in_cast()
    {
        rewrite("downcast(x AS int)").ShouldBe("downcast(x AS int)");
        rewrite("[forecast](x)").ShouldBe("[forecast](x)");
    }

    [Fact]
    public void handles_a_bracketed_identifier_holding_punctuation()
    {
        rewrite("CAST([odd (name)] AS int)").ShouldBe("CONVERT(int, [odd (name)])");
    }

    [Fact]
    public void is_case_insensitive_on_both_keywords()
    {
        rewrite("cast(x as int)").ShouldBe("CONVERT(int, x)");
        rewrite("Cast(x As Int)").ShouldBe("CONVERT(Int, x)");
    }

    [Theory]
    [InlineData("")]
    [InlineData("CAST")]
    [InlineData("CAST(")]
    [InlineData("CAST(x)")]
    [InlineData("CAST(x AS)")]
    public void leaves_something_it_cannot_parse_untouched(string expression)
    {
        Should.NotThrow(() => rewrite(expression)).ShouldBe(expression);
    }

    /// <summary>The point of the whole exercise: the two spellings canonicalize the same.</summary>
    [Fact]
    public void the_declared_cast_and_the_stored_convert_canonicalize_alike()
    {
        var declared = SqlServerExpression.Canonicalize(
            "CAST(JSON_VALUE(data, '$.customerId') AS uniqueidentifier)");
        var stored = SqlServerExpression.Canonicalize(
            "(CONVERT([uniqueidentifier],json_value([data],'$.customerId')))");

        declared.ShouldBe(stored);
    }

    /// <summary>The row that already reconciled has to keep reconciling.</summary>
    [Fact]
    public void a_declared_convert_still_matches_its_stored_form()
    {
        var declared = SqlServerExpression.Canonicalize(
            "CONVERT(datetimeoffset, JSON_VALUE(data, '$.placed'), 126)");
        var stored = SqlServerExpression.Canonicalize(
            "(CONVERT([datetimeoffset],json_value([data],'$.placed'),(126)))");

        declared.ShouldBe(stored);
    }

    /// <summary>A genuinely different expression still differs — the rewrite is not folding everything together.</summary>
    [Fact]
    public void two_different_expressions_still_differ()
    {
        SqlServerExpression.Canonicalize("CAST(JSON_VALUE(data, '$.a') AS int)")
            .ShouldNotBe(SqlServerExpression.Canonicalize("CAST(JSON_VALUE(data, '$.b') AS int)"));

        SqlServerExpression.Canonicalize("CAST(x AS varchar(250))")
            .ShouldNotBe(SqlServerExpression.Canonicalize("CAST(x AS varchar(500))"));
    }
}
