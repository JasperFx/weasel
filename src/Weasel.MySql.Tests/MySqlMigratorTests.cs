using MySqlConnector;
using Shouldly;
using Weasel.MySql.Tables;
using Xunit;

namespace Weasel.MySql.Tests;

public class MySqlMigratorTests
{
    [Fact]
    public void matches_mysql_connection()
    {
        var migrator = new MySqlMigrator();

        using var connection = new MySqlConnection();
        migrator.MatchesConnection(connection).ShouldBeTrue();
    }

    [Fact]
    public void create_table_returns_mysql_table()
    {
        var migrator = new MySqlMigrator();
        var identifier = new MySqlObjectName("test_db", "test_table");

        var table = migrator.CreateTable(identifier);

        table.ShouldBeOfType<Table>();
        table.Identifier.ShouldBe(identifier);
    }

    /// <summary>
    ///     weasel#416. AssertValidIdentifier is the only identifier check in the stack, and this provider's
    ///     checked length only until now, so it has to reject the characters that let a name escape the
    ///     statement it is written into. MySQL delimits identifiers with backticks -- and with <c>"</c>
    ///     under ANSI_QUOTES -- a <c>'</c> closes a string literal, a <c>\</c> escapes the character after
    ///     it inside one, and a <c>;</c> starts a new statement.
    /// </summary>
    [Theory]
    [InlineData("users`", "a trailing backtick")]
    [InlineData("`users`", "a fully backtick-wrapped name")]
    [InlineData("us`ers", "an embedded backtick")]
    [InlineData("users`; drop table users; --", "a backtick-and-semicolon payload")]
    [InlineData("us\"ers", "an embedded double quote")]
    [InlineData("us'ers", "an embedded single quote")]
    [InlineData("users\\", "a trailing backslash")]
    [InlineData("users;", "a trailing semicolon")]
    [InlineData("us;ers", "an embedded semicolon")]
    public void assert_identifier_rejects_quote_backtick_backslash_and_semicolon(string name, string description)
    {
        var migrator = new MySqlMigrator();

        Should.Throw<ArgumentException>(
            () => migrator.AssertValidIdentifier(name),
            $"Expected {description} to be rejected");
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData("us\ters")]
    [InlineData("us\ners")]
    [InlineData("us\rers")]
    [InlineData("users\n-- the rest of this statement is now a comment")]
    /// <remarks>
    ///     An interior space is deliberately absent from this list as of weasel#448: every provider
    ///     quotes for shape now (weasel#447), so "unit price" is safe and is somebody's real legacy
    ///     column. A line break or tab still is not — it can smuggle a '--' comment into the statement.
    /// </remarks>
    public void assert_identifier_rejects_null_empty_and_whitespace(string? name)
    {
        var migrator = new MySqlMigrator();

        Should.Throw<ArgumentException>(() => migrator.AssertValidIdentifier(name!));
    }

    [Fact]
    public void assert_identifier_rejects_names_past_the_length_limit()
    {
        var migrator = new MySqlMigrator();

        Should.NotThrow(() => migrator.AssertValidIdentifier(new string('a', 64)));
        Should.Throw<ArgumentException>(() => migrator.AssertValidIdentifier(new string('a', 65)));
    }

    [Fact]
    public void invalid_identifier_message_says_which_rule_was_broken()
    {
        var migrator = new MySqlMigrator();

        var ex = Should.Throw<ArgumentException>(() => migrator.AssertValidIdentifier("us`ers"));

        ex.Message.ShouldContain("us`ers");
        ex.Message.ShouldContain("backtick");
    }

    /// <summary>
    ///     Names Weasel and its consumers actually generate must keep working -- the tightening is aimed at
    ///     a handful of characters, not at narrowing the identifier grammar.
    /// </summary>
    [Theory]
    [InlineData("mt_doc_user")]
    [InlineData("mt_stream")]
    [InlineData("mt_doc_user_hilo")]
    [InlineData("users$1")]
    [InlineData("_leading_underscore")]
    [InlineData("MixedCaseName")]
    [InlineData("naïve_café")]
    [InlineData("table-with-dashes")]
    [InlineData("mt_doc_target.p_tenant_one")]
    public void assert_identifier_still_accepts_ordinary_names(string name)
    {
        Should.NotThrow(() => new MySqlMigrator().AssertValidIdentifier(name));
    }

    [Fact]
    public async Task can_ensure_database_that_does_not_exist()
    {
        var migrator = new MySqlMigrator();
        var databaseName = $"weasel_ensure_{Guid.NewGuid():N}";

        // Use root credentials for CREATE DATABASE privileges
        var rootBuilder = new MySqlConnectionStringBuilder(ConnectionSource.ConnectionString)
        {
            UserID = "root",
            Password = "P@55w0rd",
            Database = databaseName
        };

        try
        {
            await using var targetConn = new MySqlConnection(rootBuilder.ConnectionString);
            await migrator.EnsureDatabaseExistsAsync(targetConn);

            // Verify the database was created by opening a connection to it
            await using var verifyConn = new MySqlConnection(rootBuilder.ConnectionString);
            await verifyConn.OpenAsync();
        }
        finally
        {
            var adminBuilder = new MySqlConnectionStringBuilder(ConnectionSource.ConnectionString)
            {
                UserID = "root",
                Password = "P@55w0rd",
                Database = ""
            };
            await using var adminConn = new MySqlConnection(adminBuilder.ConnectionString);
            await adminConn.OpenAsync();

            var cmd = adminConn.CreateCommand();
            cmd.CommandText = $"DROP DATABASE IF EXISTS `{databaseName}`";
            await cmd.ExecuteNonQueryAsync();
        }
    }

    [Fact]
    public async Task ensure_database_is_idempotent()
    {
        var migrator = new MySqlMigrator();

        // Use the existing test database - should not throw
        await using var connection = new MySqlConnection(ConnectionSource.ConnectionString);
        await migrator.EnsureDatabaseExistsAsync(connection);
    }
}
