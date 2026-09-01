using Shouldly;
using Xunit;

namespace Weasel.Core.Tests;

public class ViewSqlNormalizerTests
{
    [Theory]
    [InlineData("select id, name from users", "select id,name from users")]
    [InlineData("select id, name from users", "SELECT ID, NAME FROM USERS")]
    [InlineData("select id from users", "select id\r\n\tfrom\tusers")]
    [InlineData("select 1", "select 1;")]
    [InlineData("select 1", "  select 1 ;;  ")]
    public void equal_outside_string_literals(string left, string right)
        => ViewSqlNormalizer.Normalize(left).ShouldBe(ViewSqlNormalizer.Normalize(right));

    [Theory]
    [InlineData("where name = 'active'", "where name = 'ACTIVE'")]
    [InlineData("where name = 'a b'", "where name = 'ab'")]
    [InlineData("where name = 'a\tb'", "where name = 'ab'")]
    [InlineData("where name = 'it''s'", "where name = 'IT''S'")]
    [InlineData("select 'x' as tag", "select 'X' as tag")]
    public void different_inside_string_literals(string left, string right)
        => ViewSqlNormalizer.Normalize(left).ShouldNotBe(ViewSqlNormalizer.Normalize(right));

    [Fact]
    public void literal_contents_survive_verbatim()
        => ViewSqlNormalizer.Normalize("select 'a b' as tag").ShouldBe("SELECT'a b'ASTAG");

    [Fact]
    public void an_escaped_quote_does_not_end_the_literal()
        => ViewSqlNormalizer.Normalize("select 'it''s a b'").ShouldBe("SELECT'it''s a b'");

    [Fact]
    public void an_unterminated_literal_is_copied_rather_than_folded()
        => ViewSqlNormalizer.Normalize("select 'a b").ShouldBe("SELECT'a b");

    [Fact]
    public void a_trailing_semicolon_inside_a_literal_is_kept()
        => ViewSqlNormalizer.Normalize("select ';'").ShouldBe("SELECT';'");

    [Theory]
    [InlineData("select id -- don't fold this\nfrom users", "select id -- don't fold this\n  from  users")]
    [InlineData("select id /* don't */ from users", "select id /* don't */    from users")]
    public void a_quote_in_a_comment_does_not_open_a_literal(string left, string right)
        => ViewSqlNormalizer.Normalize(left).ShouldBe(ViewSqlNormalizer.Normalize(right));

    [Fact]
    public void a_comment_does_not_swallow_a_following_literal()
        => ViewSqlNormalizer.Normalize("select 1 /* c */ , 'a b'").ShouldBe("SELECT1/*C*/,'a b'");

    [Theory]
    [InlineData("select total as [Customer's Total] , id from t", "select total as [Customer's Total], id from t")]
    [InlineData("select \"o'brien\" , id from t", "select \"o'brien\", id from t")]
    [InlineData("select [a]]b] , id from t", "select [a]]b], id from t")]
    [InlineData("select \"a\"\"b\" , id from t", "select \"a\"\"b\", id from t")]
    [InlineData("select `o'brien` , id from t", "select `o'brien`, id from t")]
    [InlineData("select `a``b` , id from t", "select `a``b`, id from t")]
    [InlineData("select [Name] from t", "select [name]   from   t")]
    public void an_apostrophe_in_a_delimited_identifier_does_not_open_a_literal(string left, string right)
        => ViewSqlNormalizer.Normalize(left).ShouldBe(ViewSqlNormalizer.Normalize(right));

    [Theory]
    [InlineData("select [Customer's Name] from t where s = 'active'",
        "select [Customer's Name] from t where s = 'ACTIVE'")]
    [InlineData("select \"o'brien\" from t where s = 'a b'", "select \"o'brien\" from t where s = 'ab'")]
    [InlineData("select `o'brien` from t where s = 'active'", "select `o'brien` from t where s = 'ACTIVE'")]
    public void a_delimited_identifier_does_not_hide_drift_in_a_later_literal(string left, string right)
        => ViewSqlNormalizer.Normalize(left).ShouldNotBe(ViewSqlNormalizer.Normalize(right));

    [Fact]
    public void a_delimited_identifier_is_folded_like_any_text_outside_a_literal()
        => ViewSqlNormalizer.Normalize("select [Customer's Total] from t").ShouldBe("SELECT[CUSTOMER'STOTAL]FROMT");

    [Fact]
    public void a_bracket_inside_a_literal_is_still_literal_text()
        => ViewSqlNormalizer.Normalize("select 'a [b] c'").ShouldBe("SELECT'a [b] c'");

    [Fact]
    public void an_unterminated_delimited_identifier_is_folded_to_the_end()
        => ViewSqlNormalizer.Normalize("select [a b").ShouldBe("SELECT[AB");

    [Fact]
    public void empty_sql_normalizes_to_empty()
        => ViewSqlNormalizer.Normalize("").ShouldBe("");
}
