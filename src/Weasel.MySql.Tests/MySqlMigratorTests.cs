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
