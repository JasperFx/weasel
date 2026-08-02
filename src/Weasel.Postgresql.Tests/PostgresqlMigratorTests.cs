using Npgsql;
using Shouldly;
using Weasel.Postgresql.Tables;
using Xunit;

namespace Weasel.Postgresql.Tests;

public class PostgresqlMigratorTests
{
    [Fact]
    public void matches_npgsql_connection()
    {
        var migrator = new PostgresqlMigrator();

        using var connection = new NpgsqlConnection();
        migrator.MatchesConnection(connection).ShouldBeTrue();
    }

    [Fact]
    public void create_table_returns_postgresql_table()
    {
        var migrator = new PostgresqlMigrator();
        var identifier = new PostgresqlObjectName("public", "test_table");

        var table = migrator.CreateTable(identifier);

        table.ShouldBeOfType<Table>();
        table.Identifier.ShouldBe(identifier);
    }

    [Fact]
    public void default_name_data_length_is_64()
    {
        new PostgresqlMigrator().NameDataLength.ShouldBe(64);
    }

    [Fact]
    public void assert_identifier_length_happy_path()
    {
        var options = new PostgresqlMigrator();

        for (var i = 1; i < options.NameDataLength; i++)
        {
            var text = new string('a', i);

            options.AssertValidIdentifier(text);
        }
    }

    [Fact]
    public void assert_identifier_must_not_contain_space()
    {
        var random = new Random();
        var options = new PostgresqlMigrator();

        for (var i = 1; i < options.NameDataLength; i++)
        {
            var text = new string('a', i);
            var position = random.Next(0, i);

            Should.Throw<PostgresqlIdentifierInvalidException>(() =>
            {
                options.AssertValidIdentifier(text.Remove(position).Insert(position, " "));
            });
        }
    }

    [Fact]
    public void assert_identifier_null_or_whitespace()
    {
        var options = new PostgresqlMigrator();

        Should.Throw<PostgresqlIdentifierInvalidException>(() =>
        {
            options.AssertValidIdentifier(null);
        });

        for (var i = 0; i < options.NameDataLength; i++)
        {
            var text = new string(' ', i);

            Should.Throw<PostgresqlIdentifierInvalidException>(() =>
            {
                options.AssertValidIdentifier(text);
            });
        }
    }

    [Fact]
    public void assert_identifier_length_exceeding_maximum()
    {
        var options = new PostgresqlMigrator();

        var text = new string('a', options.NameDataLength);

        Should.Throw<PostgresqlIdentifierTooLongException>(() =>
        {
            options.AssertValidIdentifier(text);
        });
    }

    /// <summary>
    ///     weasel#416. AssertValidIdentifier is the only identifier check in the stack, so it has to reject
    ///     the characters that let a name escape the statement it is written into. A '"' closes a quoted
    ///     identifier -- Weasel quotes without doubling an embedded quote -- and a ';' starts a new
    ///     statement. Both were permitted before this.
    ///     <para>
    ///         The single quote came with the move onto the shared IdentifierValidation. It is not
    ///         uniformity for its own sake: Table.ColumnExpression.DefaultValueFromSequence writes a
    ///         sequence's name into a string literal, DEFAULT nextval('{name}'), so a name carrying one
    ///         closes that literal. PostgreSQL's introspection queries parameterise the name, which is why
    ///         this is narrower here than on the providers that interpolate it.
    ///     </para>
    /// </summary>
    [Theory]
    [InlineData("users\"", "a trailing double quote")]
    [InlineData("\"users", "a leading double quote")]
    [InlineData("us\"ers", "an embedded double quote")]
    [InlineData("\"users\"", "a fully quote-wrapped name")]
    [InlineData("users\"; drop table users; --", "a quote-and-semicolon payload")]
    [InlineData("users'", "a trailing single quote")]
    [InlineData("us'ers", "an embedded single quote")]
    [InlineData("users'); drop table users; --", "a literal-breaking payload")]
    [InlineData("users;", "a trailing semicolon")]
    [InlineData("us;ers", "an embedded semicolon")]
    public void assert_identifier_rejects_quote_and_semicolon(string name, string description)
    {
        var options = new PostgresqlMigrator();

        Should.Throw<PostgresqlIdentifierInvalidException>(
            () => options.AssertValidIdentifier(name),
            $"Expected {description} to be rejected");
    }

    /// <summary>
    ///     Only the literal space character used to be checked, so a newline could still smuggle a '--'
    ///     comment into an otherwise unquoted name.
    /// </summary>
    [Theory]
    [InlineData("us\ters")]
    [InlineData("us\ners")]
    [InlineData("us\rers")]
    [InlineData("users\n-- the rest of this statement is now a comment")]
    public void assert_identifier_rejects_all_whitespace_not_just_the_space_character(string name)
    {
        var options = new PostgresqlMigrator();

        Should.Throw<PostgresqlIdentifierInvalidException>(() => options.AssertValidIdentifier(name));
    }

    [Fact]
    public void invalid_identifier_exception_says_which_rule_was_broken()
    {
        var options = new PostgresqlMigrator();

        var ex = Should.Throw<PostgresqlIdentifierInvalidException>(
            () => options.AssertValidIdentifier("us\"ers"));

        ex.Name.ShouldBe("us\"ers");
        ex.Reason.ShouldBe("it contains a double quote");
        ex.Message.ShouldContain("double quote");
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
        Should.NotThrow(() => new PostgresqlMigrator().AssertValidIdentifier(name));
    }

    [Fact]
    public async Task can_ensure_database_that_does_not_exist()
    {
        var migrator = new PostgresqlMigrator();
        var databaseName = $"weasel_ensure_test_{Guid.NewGuid():N}";

        var builder = new NpgsqlConnectionStringBuilder(ConnectionSource.ConnectionString)
        {
            Database = databaseName
        };

        try
        {
            await using var targetConn = new NpgsqlConnection(builder.ConnectionString);
            await migrator.EnsureDatabaseExistsAsync(targetConn);

            // Verify the database was created
            var adminBuilder = new NpgsqlConnectionStringBuilder(ConnectionSource.ConnectionString)
            {
                Database = "postgres"
            };
            await using var adminConn = new NpgsqlConnection(adminBuilder.ConnectionString);
            await adminConn.OpenAsync();
            (await adminConn.DatabaseExists(databaseName)).ShouldBeTrue();
        }
        finally
        {
            var adminBuilder = new NpgsqlConnectionStringBuilder(ConnectionSource.ConnectionString)
            {
                Database = "postgres"
            };
            await using var adminConn = new NpgsqlConnection(adminBuilder.ConnectionString);
            await adminConn.OpenAsync();
            await adminConn.KillIdleSessions(databaseName);
            await adminConn.DropDatabase(databaseName);
        }
    }

    [Fact]
    public async Task ensure_database_is_idempotent()
    {
        var migrator = new PostgresqlMigrator();

        // Use the existing test database - should not throw
        await using var connection = new NpgsqlConnection(ConnectionSource.ConnectionString);
        await migrator.EnsureDatabaseExistsAsync(connection);
    }
}
