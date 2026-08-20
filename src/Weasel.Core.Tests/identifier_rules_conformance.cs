using Shouldly;
using Weasel.Core;
using Weasel.MySql;
using Weasel.SqlServer;
using Xunit;

namespace Weasel.Core.Tests;

/// <summary>
///     One suite of hostile names, run against every provider's <see cref="IdentifierRules" />.
///     The five providers each grew their quoting in isolation and diverged sharply — one could not
///     escape its own delimiter, one conflated quoting with case folding, one quoted for a
///     hand-written list of characters and crashed on an empty string. This is the shared floor:
///     for any name, a provider either quotes it correctly or leaves it correctly bare, and never
///     emits something that changes which object the name refers to.
/// </summary>
/// <remarks>
///     <para>
///         Providers join the suite as their fixes land (weasel#447). SQL Server is here because
///         it is the reference implementation the contract was lifted from, and MySQL joined with
///         its own fix. SQLite, PostgreSQL and Oracle each join in the PR that fixes them — adding
///         a provider is one entry in <see cref="Providers" />.
///     </para>
///     <para>
///         Deliberately pure string logic: no connection, no container, so it runs everywhere and
///         costs nothing.
///     </para>
/// </remarks>
public class identifier_rules_conformance
{
    public static TheoryData<string, IdentifierRules> Providers =>
        new()
        {
            { "SqlServer", SqlServerIdentifierRules.Instance },
            { "MySql", MySqlIdentifierRules.Instance }
        };

    /// <summary>
    ///     Names that have to survive a round trip through the dialect. Each one either broke a
    ///     provider in the wild or is the shape that broke one.
    /// </summary>
    public static readonly string[] HostileNames =
    [
        "orders",                                   // ordinary
        "Table",                                    // reserved word
        "unit price",                               // space
        "order-date",                               // hyphen
        "PK_dbo.__MigrationHistory",                // dot: the key EF6 gives its history table
        "<Name of Missing Index, sysname,>",        // SQL Server's own unfilled template
        "2ndPlace",                                 // leading digit
        "Grüße",                                    // non-ASCII letters
        "MiXeDcAsE",                                // case that must not be folded away
        "price$",
        "col#1",
        "_internal"
    ];

    [Theory]
    [MemberData(nameof(Providers))]
    public void delimiting_round_trips_every_hostile_name(string provider, IdentifierRules rules)
    {
        foreach (var name in HostileNames)
        {
            rules.Undelimit(rules.Delimit(name))
                .ShouldBe(name, $"{provider} lost '{name}' delimiting and undelimiting it");
        }
    }

    [Theory]
    [MemberData(nameof(Providers))]
    public void a_delimited_name_is_recognized_as_delimited(string provider, IdentifierRules rules)
    {
        foreach (var name in HostileNames)
        {
            rules.IsDelimited(rules.Delimit(name))
                .ShouldBeTrue($"{provider} did not recognize its own output for '{name}'");
        }
    }

    /// <summary>
    ///     A name carrying the close delimiter is where the naive implementations broke: MySQL's
    ///     <c>$"`{name}`"</c> let it close its own quoting. Doubling has to survive a round trip.
    /// </summary>
    [Theory]
    [MemberData(nameof(Providers))]
    public void an_embedded_close_delimiter_is_escaped_and_round_trips(string provider, IdentifierRules rules)
    {
        var delimited = rules.Delimit("a]b\"c`d");

        rules.IsDelimited(delimited).ShouldBeTrue($"{provider} produced output it cannot parse back");
        rules.Undelimit(delimited).ShouldBe("a]b\"c`d", $"{provider} mangled an embedded delimiter");
    }

    /// <summary>
    ///     The pass-through must not become a hole. A name that merely starts and ends with the
    ///     delimiters is not delimited — an unbalanced interior close character means the DDL it is
    ///     carrying would escape the identifier.
    /// </summary>
    [Theory]
    [MemberData(nameof(Providers))]
    public void a_name_carrying_ddl_cannot_escape_its_delimiters(string provider, IdentifierRules rules)
    {
        foreach (var injection in new[]
                 {
                     "[ix] ON t(id); DROP TABLE victim; --]",
                     "\"ix\" ON t(id); DROP TABLE victim; --\"",
                     "`ix` ON t(id); DROP TABLE victim; --`"
                 })
        {
            var quoted = rules.Quote(injection);

            // Either it was delimited on its own terms, or it was already delimited end to end
            // with everything interior escaped. Never passed through with a loose close character.
            rules.IsDelimited(quoted).ShouldBeTrue($"{provider} left '{injection}' able to escape");
            rules.Undelimit(quoted).ShouldBe(injection, $"{provider} altered '{injection}'");
        }
    }

    [Theory]
    [MemberData(nameof(Providers))]
    public void quoting_never_changes_which_object_a_name_refers_to(string provider, IdentifierRules rules)
    {
        foreach (var name in HostileNames)
        {
            var quoted = rules.Quote(name);
            var resolved = rules.IsDelimited(quoted) ? rules.Undelimit(quoted) : quoted;

            resolved.ShouldBe(name, $"{provider} renamed '{name}' by quoting it");
        }
    }

    /// <summary>
    ///     Weasel emitted most identifiers bare until 9.25, so delimiting the name yourself was the
    ///     only way to use one that needed it. Re-escaping those would silently rename the object.
    /// </summary>
    [Theory]
    [MemberData(nameof(Providers))]
    public void a_name_the_caller_already_delimited_is_left_alone(string provider, IdentifierRules rules)
    {
        foreach (var name in HostileNames)
        {
            var alreadyDelimited = rules.Delimit(name);

            rules.Quote(alreadyDelimited)
                .ShouldBe(alreadyDelimited, $"{provider} re-escaped an already delimited '{name}'");
        }
    }

    /// <summary>
    ///     SQLite's <c>QuoteName</c> indexed <c>name[0]</c> without checking, so an empty name threw
    ///     <c>IndexOutOfRangeException</c> where every other provider returned it unchanged.
    /// </summary>
    [Theory]
    [MemberData(nameof(Providers))]
    public void an_empty_name_is_returned_rather_than_throwing(string provider, IdentifierRules rules)
    {
        rules.Quote("").ShouldBe("", $"{provider} did not pass an empty name through");
        rules.Delimit("").ShouldBe("", $"{provider} did not pass an empty name through");
        rules.Undelimit("").ShouldBe("", $"{provider} did not pass an empty name through");
        rules.IsDelimited("").ShouldBeFalse($"{provider} called an empty name delimited");
    }

    /// <summary>
    ///     Not every dialect quotes conditionally — MySQL delimits every identifier, which is what
    ///     keeps its generated DDL byte-identical to what it emitted before it could escape its own
    ///     delimiter. So the invariant is keyed off the provider's own answer: whatever it says does
    ///     not need delimiting, it has to leave alone.
    /// </summary>
    [Theory]
    [MemberData(nameof(Providers))]
    public void a_name_that_needs_no_delimiting_is_left_bare(string provider, IdentifierRules rules)
    {
        foreach (var name in new[] { "orders", "_internal", "price$" })
        {
            if (rules.RequiresDelimiting(name))
            {
                continue;
            }

            rules.Quote(name).ShouldBe(name, $"{provider} delimited '{name}' after saying it need not be");
        }
    }

    [Theory]
    [MemberData(nameof(Providers))]
    public void a_reserved_word_is_delimited(string provider, IdentifierRules rules)
    {
        rules.IsReservedWord("Table").ShouldBeTrue($"{provider} does not know 'Table' is reserved");
        rules.IsDelimited(rules.Quote("Table")).ShouldBeTrue($"{provider} left a reserved word bare");
    }

    /// <summary>
    ///     Object names reach string literals on every provider — existence checks, introspection
    ///     queries, and PostgreSQL's <c>DEFAULT nextval('...')</c>. This is dialect-independent, so
    ///     it is asserted once rather than per provider.
    /// </summary>
    [Fact]
    public void a_single_quote_is_doubled_for_a_string_literal()
    {
        IdentifierRules.EscapeLiteral("O'Brien").ShouldBe("O''Brien");
        IdentifierRules.EscapeLiteral("'; drop table victim; --").ShouldBe("''; drop table victim; --");
        IdentifierRules.EscapeLiteral("").ShouldBe("");
        IdentifierRules.EscapeLiteral("plain").ShouldBe("plain");
    }
}
