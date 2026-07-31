using Oracle.ManagedDataAccess.Client;
using Shouldly;
using Weasel.Oracle.Tables;
using Xunit;

namespace Weasel.Oracle.Tests;

public class OracleMigratorTests
{
    [Fact]
    public void matches_oracle_connection()
    {
        var migrator = new OracleMigrator();

        using var connection = new OracleConnection();
        migrator.MatchesConnection(connection).ShouldBeTrue();
    }

    [Fact]
    public void create_table_returns_oracle_table()
    {
        var migrator = new OracleMigrator();
        var identifier = new OracleObjectName("TEST_SCHEMA", "TEST_TABLE");

        var table = migrator.CreateTable(identifier);

        table.ShouldBeOfType<Table>();
        table.Identifier.ShouldBe(identifier);
    }

    /// <summary>
    ///     weasel#416. AssertValidIdentifier is the only identifier check in the stack, and this provider's
    ///     checked length only until now, so it has to reject the characters that let a name escape the
    ///     statement it is written into. A <c>"</c> closes a quoted identifier and a <c>;</c> starts a new
    ///     statement. The <c>'</c> matters especially here: Weasel wraps Oracle DDL in an anonymous PL/SQL
    ///     block and runs it via EXECUTE IMMEDIATE, so the name is written inside a string literal.
    /// </summary>
    [Theory]
    [InlineData("USERS\"", "a trailing double quote")]
    [InlineData("\"USERS", "a leading double quote")]
    [InlineData("US\"ERS", "an embedded double quote")]
    [InlineData("\"USERS\"", "a fully quote-wrapped name")]
    [InlineData("USERS\"; DROP TABLE USERS; --", "a quote-and-semicolon payload")]
    [InlineData("USERS'", "a trailing single quote")]
    [InlineData("US'ERS", "an embedded single quote")]
    [InlineData("USERS'; EXECUTE IMMEDIATE 'DROP TABLE USERS", "a literal-breaking payload")]
    [InlineData("USERS;", "a trailing semicolon")]
    [InlineData("US;ERS", "an embedded semicolon")]
    public void assert_identifier_rejects_quote_and_semicolon(string name, string description)
    {
        var migrator = new OracleMigrator();

        Should.Throw<InvalidOperationException>(
            () => migrator.AssertValidIdentifier(name),
            $"Expected {description} to be rejected");
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData("US ERS")]
    [InlineData("US\tERS")]
    [InlineData("US\nERS")]
    [InlineData("US\rERS")]
    [InlineData("USERS\n-- the rest of this statement is now a comment")]
    public void assert_identifier_rejects_null_empty_and_whitespace(string? name)
    {
        var migrator = new OracleMigrator();

        Should.Throw<InvalidOperationException>(() => migrator.AssertValidIdentifier(name!));
    }

    [Fact]
    public void assert_identifier_rejects_names_past_the_length_limit()
    {
        var migrator = new OracleMigrator();

        Should.NotThrow(() => migrator.AssertValidIdentifier(new string('A', 128)));
        Should.Throw<InvalidOperationException>(() => migrator.AssertValidIdentifier(new string('A', 129)));
    }

    [Fact]
    public void invalid_identifier_message_says_which_rule_was_broken()
    {
        var migrator = new OracleMigrator();

        var ex = Should.Throw<InvalidOperationException>(() => migrator.AssertValidIdentifier("US\"ERS"));

        ex.Message.ShouldContain("US\"ERS");
        ex.Message.ShouldContain("double quote");
    }

    /// <summary>
    ///     Names Weasel and its consumers actually generate must keep working -- the tightening is aimed at
    ///     a handful of characters, not at narrowing the identifier grammar.
    /// </summary>
    [Theory]
    [InlineData("MT_DOC_USER")]
    [InlineData("MT_STREAM")]
    [InlineData("mt_doc_user_hilo")]
    [InlineData("USERS$1")]
    [InlineData("_LEADING_UNDERSCORE")]
    [InlineData("MixedCaseName")]
    [InlineData("naïve_café")]
    [InlineData("TABLE-WITH-DASHES")]
    [InlineData("MT_DOC_TARGET.P_TENANT_ONE")]
    public void assert_identifier_still_accepts_ordinary_names(string name)
    {
        Should.NotThrow(() => new OracleMigrator().AssertValidIdentifier(name));
    }

    [Fact]
    public async Task ensure_database_is_idempotent()
    {
        var migrator = new OracleMigrator();

        // Use the existing test connection/schema - should not throw
        await using var connection = new OracleConnection(ConnectionSource.ConnectionString);
        await migrator.EnsureDatabaseExistsAsync(connection);
    }
}
