using Shouldly;
using Weasel.Core;
using Weasel.MySql;
using Weasel.Oracle;
using Weasel.Postgresql;
using Weasel.SqlServer;
using Weasel.Sqlite;
using Xunit;

namespace Weasel.Core.Tests;

/// <summary>
///     A name can reach the model already delimited, and the model has to hold the spelling the
///     database will report rather than the spelling the caller wrote.
/// </summary>
/// <remarks>
///     <para>
///         <see cref="identifier_rules_conformance" /> pins the string layer, and that layer is
///         correct: every provider delimits and undelimits without losing a name. What was missing is
///         one level up. <c>QualifiedNameParser</c> splits a qualified name on <c>.</c> and keeps the
///         parts exactly as written, so <c>DbObjectName.Parse(provider, "\"MySchema\".things")</c>
///         handed back a schema still carrying its quotes, and nothing normalized it afterwards.
///     </para>
///     <para>
///         Every provider's <c>ConfigureQueryCommand</c> binds <c>Identifier.Schema</c> straight into
///         an introspection query against a catalog that holds the bare name. The delimited spelling
///         never matched, introspection came back empty, the object read as absent, and the delta was
///         <c>Create</c> on every run — emitted as <c>CREATE TABLE IF NOT EXISTS</c>, which succeeds
///         and does nothing. So the object was created once and then never migrated again, with every
///         later change silently discarded (weasel#499).
///     </para>
///     <para>
///         <see cref="IdentifierRules.Undelimit" />'s own remarks already prescribe the fix — "A
///         provider normalizes names through this as they arrive so that what the model holds is what
///         the database will report." These tests are that sentence, made enforceable for all five.
///     </para>
///     <para>
///         Stated through <see cref="IdentifierRules.SameObject" /> rather than string equality,
///         exactly as the sibling suite states its invariants, because Oracle folds: there,
///         <c>myschema</c> and <c>MYSCHEMA</c> are one object, and the contract has to hold for a
///         folding dialect without weakening into case-insensitivity for the preserving ones.
///     </para>
///     <para>
///         Pure string and model logic: no connection, no container.
///     </para>
/// </remarks>
public class object_name_normalization_conformance
{
    public static TheoryData<string, IDatabaseProvider, IdentifierRules> Providers =>
        new()
        {
            { "SqlServer", SqlServerProvider.Instance, SqlServerIdentifierRules.Instance },
            { "MySql", MySqlProvider.Instance, MySqlIdentifierRules.Instance },
            { "Sqlite", SqliteProvider.Instance, SqliteIdentifierRules.Instance },
            { "Postgresql", PostgresqlProvider.Instance, PostgresqlIdentifierRules.General },
            { "Oracle", OracleProvider.Instance, OracleIdentifierRules.Instance }
        };

    /// <summary>
    ///     Names a caller had to delimit themselves. Weasel emitted most identifiers bare until 9.25,
    ///     so writing the delimiters by hand was the only way to use one of these at all — which is
    ///     precisely why they are the names most likely to arrive delimited.
    /// </summary>
    public static readonly string[] NamesACallerWouldDelimit =
    [
        "MixedCase",
        "order date",
        "Table",
        "order-date",
        "2ndPlace"
    ];

    /// <summary>
    ///     The core of weasel#499: what the model holds has to be what the catalog will report, so
    ///     that binding it into an introspection query finds the object.
    /// </summary>
    [Theory]
    [MemberData(nameof(Providers))]
    public void a_name_that_arrives_delimited_is_held_as_the_catalog_reports_it(
        string provider, IDatabaseProvider databaseProvider, IdentifierRules rules)
    {
        foreach (var name in NamesACallerWouldDelimit)
        {
            var delimited = rules.Quote(name);

            if (!rules.IsDelimited(delimited))
            {
                // The dialect writes this one bare, so there is no delimited spelling to normalize.
                continue;
            }

            var parsed = databaseProvider.Parse(delimited, delimited);

            rules.IsDelimited(parsed.Schema).ShouldBeFalse(
                $"{provider} left the schema of '{name}' holding its delimiters: '{parsed.Schema}'");
            rules.IsDelimited(parsed.Name).ShouldBeFalse(
                $"{provider} left the name of '{name}' holding its delimiters: '{parsed.Name}'");

            rules.SameObject(parsed.Schema, rules.Undelimit(delimited)).ShouldBeTrue(
                $"{provider} changed which schema '{name}' refers to: got '{parsed.Schema}'");
            rules.SameObject(parsed.Name, rules.Undelimit(delimited)).ShouldBeTrue(
                $"{provider} changed which object '{name}' refers to: got '{parsed.Name}'");
        }
    }

    /// <summary>
    ///     Normalizing on the way in must not change the DDL. Whatever the model ends up holding, the
    ///     qualified name written into a statement still has to resolve to the object the caller
    ///     named — delimited again where the dialect needs it.
    /// </summary>
    [Theory]
    [MemberData(nameof(Providers))]
    public void normalizing_does_not_change_which_object_the_ddl_names(
        string provider, IDatabaseProvider databaseProvider, IdentifierRules rules)
    {
        foreach (var name in NamesACallerWouldDelimit)
        {
            var delimited = rules.Quote(name);

            if (!rules.IsDelimited(delimited))
            {
                continue;
            }

            var emitted = databaseProvider.ToQualifiedName(databaseProvider.Parse(delimited, delimited).Schema);
            var resolved = rules.IsDelimited(emitted) ? rules.Undelimit(emitted) : rules.Undelimit(rules.Quote(emitted));

            rules.SameObject(resolved, rules.Undelimit(delimited)).ShouldBeTrue(
                $"{provider} renamed '{name}' by normalizing it: DDL says '{emitted}'");
        }
    }

    /// <summary>
    ///     A name that arrives bare is already the catalog spelling and must be left exactly as it is.
    ///     The normalization is only ever allowed to strip delimiters the caller supplied.
    /// </summary>
    [Theory]
    [MemberData(nameof(Providers))]
    public void a_bare_name_is_untouched(
        string provider, IDatabaseProvider databaseProvider, IdentifierRules rules)
    {
        foreach (var name in new[] { "orders", "_internal", "things" })
        {
            var parsed = databaseProvider.Parse(name, name);

            parsed.Schema.ShouldBe(name, $"{provider} altered the bare schema '{name}'");
            parsed.Name.ShouldBe(name, $"{provider} altered the bare name '{name}'");
        }
    }

    /// <summary>
    ///     A dot inside a delimited identifier is an ordinary character on every provider here, so a
    ///     qualified name has to be split on the delimiters rather than on every dot.
    /// </summary>
    /// <remarks>
    ///     <c>QualifiedNameParser</c> used to call <c>qualifiedName.Split('.')</c> and throw when that
    ///     produced anything other than two parts, so a legal name simply could not be modelled:
    ///     <c>"my.schema".things</c>, <c>[my.table]</c>, and the <c>PK_dbo.__MigrationHistory</c> that
    ///     EF6 gives its own history table — already in this suite's sibling as a hostile name
    ///     (weasel#501).
    /// </remarks>
    [Theory]
    [MemberData(nameof(Providers))]
    public void a_dot_inside_a_delimited_identifier_is_not_a_separator(
        string provider, IDatabaseProvider databaseProvider, IdentifierRules rules)
    {
        var schema = rules.Delimit("my.schema");
        var name = rules.Delimit("my.table");

        var parsed = databaseProvider.Parse($"{schema}.{name}");

        rules.SameObject(parsed.Schema, "my.schema").ShouldBeTrue(
            $"{provider} mis-split the schema: got '{parsed.Schema}'");
        rules.SameObject(parsed.Name, "my.table").ShouldBeTrue(
            $"{provider} mis-split the name: got '{parsed.Name}'");
    }

    /// <summary>
    ///     And an undelimited qualified name still splits the way it always has.
    /// </summary>
    [Theory]
    [MemberData(nameof(Providers))]
    public void an_ordinary_qualified_name_still_splits_on_the_dot(
        string provider, IDatabaseProvider databaseProvider, IdentifierRules rules)
    {
        var parsed = databaseProvider.Parse("things.orders");

        parsed.Schema.ShouldBe("things", $"{provider} mis-split an ordinary schema");
        parsed.Name.ShouldBe("orders", $"{provider} mis-split an ordinary name");
    }

    /// <summary>
    ///     A bare name still takes the provider's default schema rather than being treated as qualified.
    /// </summary>
    [Theory]
    [MemberData(nameof(Providers))]
    public void a_bare_name_takes_the_default_schema(
        string provider, IDatabaseProvider databaseProvider, IdentifierRules rules)
    {
        var parsed = databaseProvider.Parse("orders");

        parsed.Name.ShouldBe("orders", $"{provider} altered a bare name");
        parsed.Schema.ShouldBe(databaseProvider.DefaultDatabaseSchemaName,
            $"{provider} did not default the schema");
    }

    /// <summary>
    ///     The pass-through in <see cref="IdentifierRules.Quote" /> is deliberately narrow: a name
    ///     that merely starts and ends with the delimiters, but carries a loose close character, is
    ///     not delimited and must not be stripped. Stripping it would hand DDL back out of an
    ///     identifier.
    /// </summary>
    [Theory]
    [MemberData(nameof(Providers))]
    public void a_name_carrying_ddl_is_not_treated_as_delimited(
        string provider, IDatabaseProvider databaseProvider, IdentifierRules rules)
    {
        foreach (var injection in new[]
                 {
                     "[ix] ON t(id); DROP TABLE victim; --]",
                     "\"ix\" ON t(id); DROP TABLE victim; --\"",
                     "`ix` ON t(id); DROP TABLE victim; --`"
                 })
        {
            var parsed = databaseProvider.Parse(injection, injection);

            parsed.Name.ShouldBe(injection, $"{provider} stripped delimiters off '{injection}'");
        }
    }
}
