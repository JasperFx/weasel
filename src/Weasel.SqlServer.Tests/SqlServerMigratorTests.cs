using Microsoft.Data.SqlClient;
using Shouldly;
using Weasel.SqlServer.Tables;
using Xunit;

namespace Weasel.SqlServer.Tests;

public class SqlServerMigratorTests
{
    [Fact]
    public void matches_sql_connection()
    {
        var migrator = new SqlServerMigrator();

        using var connection = new SqlConnection();
        migrator.MatchesConnection(connection).ShouldBeTrue();
    }

    [Fact]
    public void create_table_returns_sql_server_table()
    {
        var migrator = new SqlServerMigrator();
        var identifier = new SqlServerObjectName("dbo", "test_table");

        var table = migrator.CreateTable(identifier);

        table.ShouldBeOfType<Table>();
        table.Identifier.ShouldBe(identifier);
    }

    [Fact]
    public async Task can_ensure_database_that_does_not_exist()
    {
        var migrator = new SqlServerMigrator();
        var databaseName = $"weasel_ensure_{Guid.NewGuid():N}";

        var builder = new SqlConnectionStringBuilder(ConnectionSource.ConnectionString)
        {
            InitialCatalog = databaseName
        };

        try
        {
            await using var targetConn = new SqlConnection(builder.ConnectionString);
            await migrator.EnsureDatabaseExistsAsync(targetConn);

            // Verify the database was created by opening a connection to it
            await using var verifyConn = new SqlConnection(builder.ConnectionString);
            await verifyConn.OpenAsync();
        }
        finally
        {
            var adminBuilder = new SqlConnectionStringBuilder(ConnectionSource.ConnectionString)
            {
                InitialCatalog = "master"
            };
            await using var adminConn = new SqlConnection(adminBuilder.ConnectionString);
            await adminConn.OpenAsync();

            var cmd = adminConn.CreateCommand();
            cmd.CommandText = $@"
                IF DB_ID('{databaseName}') IS NOT NULL
                BEGIN
                    ALTER DATABASE [{databaseName}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
                    DROP DATABASE [{databaseName}];
                END";
            await cmd.ExecuteNonQueryAsync();
        }
    }

    [Fact]
    public async Task ensure_database_is_idempotent()
    {
        var migrator = new SqlServerMigrator();

        // Use the existing test database - should not throw
        await using var connection = new SqlConnection(ConnectionSource.ConnectionString);
        await migrator.EnsureDatabaseExistsAsync(connection);
    }
}
