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

    [Theory]
    [InlineData("us ers")]
    [InlineData("us\ters")]
    [InlineData("us\ners")]
    [InlineData("us\rers")]
    public void all_whitespace_characters_are_rejected_not_just_the_space(string name)
    {
        IdentifierValidation.FindProblem(name, "\"").ShouldBe("it contains whitespace");
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
