using Shouldly;
using Xunit;

namespace Weasel.Postgresql.Tests;

/// <summary>
///     weasel#602. Two of the six message defects found in the 2026-09-22 exception sweep, both in
///     text a user reads at the moment a migration refuses their schema.
/// </summary>
public class identifier_exception_messages
{
    /// <summary>
    ///     <c>$"The {nameof(PostgresqlMigrator)}{nameof(PostgresqlMigrator.NameDataLength)}"</c>
    ///     rendered as <c>PostgresqlMigratorNameDataLength</c> -- one token short of a property a
    ///     reader can go and find.
    /// </summary>
    [Fact]
    public void the_too_long_message_names_a_property_that_exists()
    {
        var ex = new PostgresqlIdentifierTooLongException(64, new string('a', 70));

        ex.Message.ShouldContain("PostgresqlMigrator.NameDataLength");
        ex.Message.ShouldNotContain("PostgresqlMigratorNameDataLength");
    }

    /// <summary>
    ///     The message still claimed "Weasel does not quote identifiers", which has been untrue
    ///     since the 9.x quoting work -- so it sent the reader off to rename an object that Weasel
    ///     would have delimited perfectly well, and said nothing about the rule they actually
    ///     broke.
    /// </summary>
    [Fact]
    public void the_invalid_message_no_longer_claims_identifiers_are_never_quoted()
    {
        var ex = new PostgresqlIdentifierInvalidException("us\"ers", "it contains a double quote");

        ex.Message.ShouldNotContain("does not quote identifiers");
        ex.Message.ShouldContain("delimits identifiers where it has to");

        // And it still says which rule was broken, which is what made it useful.
        ex.Message.ShouldContain("it contains a double quote");
    }
}
