using Shouldly;
using Xunit;

namespace Weasel.Core.Tests;

public class IdentifierValidationTests
{
    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void null_empty_and_all_whitespace_are_rejected(string? name)
    {
        IdentifierValidation.FindProblem(name, "\"").ShouldBe("it is null, empty, or entirely whitespace");
    }

    /// <summary>
    ///     A line break or a tab can introduce a <c>--</c> comment into an unquoted name, so those stay
    ///     rejected. A plain interior space cannot, and every provider quotes for shape as of weasel#447,
    ///     so <c>unit price</c> — somebody's real legacy column — is now allowed through (weasel#448).
    /// </summary>
    [Theory]
    [InlineData("us\ters")]
    [InlineData("us\ners")]
    [InlineData("us\rers")]
    public void a_line_break_or_tab_is_still_rejected(string name)
    {
        IdentifierValidation.FindProblem(name, "\"").ShouldBe("it contains a line break or tab");
    }

    [Theory]
    [InlineData("us ers")]
    [InlineData("unit price")]
    [InlineData("PK_dbo.__MigrationHistory")]
    public void an_interior_space_is_allowed_now_that_every_provider_quotes_for_shape(string name)
    {
        IdentifierValidation.FindProblem(name, "\"").ShouldBeNull();
    }

    /// <summary>
    ///     Leading or trailing whitespace is a typo every time. Allowing it would silently create an
    ///     object under a name nobody meant, which then drifts forever — so it stays rejected even
    ///     though the interior case is now permitted.
    /// </summary>
    [Theory]
    [InlineData(" users")]
    [InlineData("users ")]
    [InlineData("\tusers")]
    public void leading_or_trailing_whitespace_is_still_rejected(string name)
    {
        IdentifierValidation.FindProblem(name, "\"").ShouldBe("it starts or ends with whitespace");
    }

    /// <summary>
    ///     The semicolon and the single quote are unsafe for every provider, so they are checked whatever
    ///     the caller passes as its own unsafe set: a ';' starts a new statement, and object names reach
    ///     string literals on all of them via the existence checks and introspection queries.
    /// </summary>
    [Theory]
    [InlineData("us;ers", "it contains a semicolon")]
    [InlineData("us'ers", "it contains a single quote")]
    public void the_universal_characters_are_rejected_without_being_asked_for(string name, string expected)
    {
        IdentifierValidation.FindProblem(name, "").ShouldBe(expected);
    }

    [Theory]
    [InlineData("us\"ers", "\"", "it contains a double quote")]
    [InlineData("us`ers", "`", "it contains a backtick")]
    [InlineData("us]ers", "[]", "it contains a closing square bracket")]
    [InlineData("[users]", "[]", "it contains an opening square bracket")]
    [InlineData("users\\", "\\", "it contains a backslash")]
    [InlineData("us~ers", "~", "it contains the character '~'")]
    public void the_provider_specific_characters_are_named_in_the_reason(
        string name,
        string unsafeCharacters,
        string expected
    )
    {
        IdentifierValidation.FindProblem(name, unsafeCharacters).ShouldBe(expected);
    }

    /// <summary>
    ///     A character one provider delimits with is not necessarily unsafe for another -- a backtick is
    ///     nothing special to PostgreSQL or SQL Server -- so the set has to stay per-provider.
    /// </summary>
    [Theory]
    [InlineData("us`ers", "\"")]
    [InlineData("us\"ers", "`")]
    [InlineData("us]ers", "\"")]
    public void characters_outside_the_supplied_set_pass(string name, string unsafeCharacters)
    {
        IdentifierValidation.FindProblem(name, unsafeCharacters).ShouldBeNull();
    }

    [Theory]
    [InlineData("mt_doc_user")]
    [InlineData("users$1")]
    [InlineData("_leading_underscore")]
    [InlineData("MixedCaseName")]
    [InlineData("naïve_café")]
    [InlineData("table-with-dashes")]
    [InlineData("mt_doc_target.p_tenant_one")]
    public void ordinary_names_pass(string name)
    {
        IdentifierValidation.FindProblem(name, "\"[]`").ShouldBeNull();
    }
}
