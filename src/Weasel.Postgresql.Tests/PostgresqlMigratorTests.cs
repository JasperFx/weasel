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
