using Shouldly;
using Weasel.Firebird.Tables;
using Xunit;

namespace Weasel.Firebird.Tests.Tables;

public class ExpressionTextTests
{
    [Theory]
    [InlineData("a*b", "A * B")]
    [InlineData("( a * b )", "(a*b)")]
    [InlineData("upper(name)", "UPPER( NAME )")]
    [InlineData("a   or\n b", "A OR B")]
    [InlineData("CAST(a AS INTEGER)", "cast(a as integer)")]
    public void spellings_of_one_expression_compare_equal(string one, string other)
    {
        ExpressionText.Canonical(one).ShouldBe(ExpressionText.Canonical(other));
    }

    [Theory]
    [InlineData("a or b", "aor b")]
    [InlineData("COALESCE(a, 'x')", "COALESCE(a, 'X')")]
    [InlineData("COALESCE(a, 'x y')", "COALESCE(a, 'x  y')")]
    [InlineData("\"Mixed\" + 1", "\"MIXED\" + 1")]
    [InlineData("a - b", "a + b")]
    public void different_expressions_stay_apart(string one, string other)
    {
        ExpressionText.Canonical(one).ShouldNotBe(ExpressionText.Canonical(other));
    }

    [Fact]
    public void nothing_is_no_expression()
    {
        ExpressionText.Canonical(null).ShouldBeNull();
        ExpressionText.Canonical("").ShouldBeNull();
    }

    [Theory]
    [InlineData("(a + b)", "a + b")]
    [InlineData("((a + b))", "(a + b)")]
    [InlineData("(a) + (b)", "(a) + (b)")]
    [InlineData("a + b", "a + b")]
    [InlineData(null, null)]
    public void only_the_outer_parentheses_the_server_keeps_are_removed(string? source, string? read)
    {
        ExpressionText.WithoutOuterParentheses(source).ShouldBe(read);
    }
}
