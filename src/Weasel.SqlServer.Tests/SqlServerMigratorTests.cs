using JasperFx.Core;
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

        // Ensure the connection string has an Initial Catalog (CI may omit it)
        var builder = new SqlConnectionStringBuilder(ConnectionSource.ConnectionString)
        {
            InitialCatalog = "master"
        };

        // Use the existing master database - should not throw
        await using var connection = new SqlConnection(builder.ConnectionString);
        await migrator.EnsureDatabaseExistsAsync(connection);
    }

    /// <summary>
    ///     weasel#415. Several callers provisioning the same database at once used to leave the losers of
    ///     the check-then-create race holding SqlException 1801. Against 9.22.0 this fails.
    /// </summary>
    [Fact]
    public async Task ensure_database_is_safe_under_concurrent_callers()
    {
        var databaseName = $"weasel_ensure_race_{Guid.NewGuid():N}";

        var builder = new SqlConnectionStringBuilder(ConnectionSource.ConnectionString)
        {
            InitialCatalog = databaseName
        };

        try
        {
            // All of these see DB_ID() return null and race into CREATE DATABASE together.
            var attempts = Enumerable.Range(0, 8).Select(async _ =>
            {
                await using var conn = new SqlConnection(builder.ConnectionString);
                await new SqlServerMigrator().EnsureDatabaseExistsAsync(conn);
            });

            await Task.WhenAll(attempts);

            // Every caller must be able to take the postcondition at face value: the database exists
            // and accepts a connection by the time EnsureDatabaseExistsAsync returns.
            await using var verifyConn = new SqlConnection(builder.ConnectionString);
            await verifyConn.OpenAsync();
        }
        finally
        {
            await dropDatabaseAsync(databaseName);
        }
    }

    /// <summary>
    ///     A database name carrying a ']' would otherwise close the delimited identifier early.
    /// </summary>
    [Fact]
    public async Task ensure_database_escapes_a_bracket_in_the_database_name()
    {
        var databaseName = $"weasel_ensure_]_{Guid.NewGuid():N}";

        var builder = new SqlConnectionStringBuilder(ConnectionSource.ConnectionString)
        {
            InitialCatalog = databaseName
        };

        try
        {
            await using var conn = new SqlConnection(builder.ConnectionString);
            await new SqlServerMigrator().EnsureDatabaseExistsAsync(conn);

            await using var verifyConn = new SqlConnection(builder.ConnectionString);
            await verifyConn.OpenAsync();
        }
        finally
        {
            await dropDatabaseAsync(databaseName);
        }
    }

    /// <summary>
    ///     The wait for the database to come online has to end in a clear failure rather than in silence
    ///     or a hang. An offline database is the reproducible stand-in for "created, but still refusing
    ///     logins": DB_ID() reports it, so nothing is created, and no connection to it will ever succeed.
    /// </summary>
    [Fact]
    public async Task times_out_with_a_clear_message_when_the_database_never_accepts_connections()
    {
        var databaseName = $"weasel_ensure_offline_{Guid.NewGuid():N}";

        var builder = new SqlConnectionStringBuilder(ConnectionSource.ConnectionString)
        {
            InitialCatalog = databaseName
        };

        try
        {
            await using (var conn = new SqlConnection(builder.ConnectionString))
            {
                await new SqlServerMigrator().EnsureDatabaseExistsAsync(conn);
            }

            await executeAgainstMasterAsync(
                $"ALTER DATABASE [{databaseName}] SET OFFLINE WITH ROLLBACK IMMEDIATE;");

            var migrator = new SqlServerMigrator
            {
                DatabaseAvailabilityTimeout = 1.Seconds(), DatabaseAvailabilityPollingInterval = 100.Milliseconds()
            };

            await using var offlineConn = new SqlConnection(builder.ConnectionString);

            var ex = await Should.ThrowAsync<TimeoutException>(async () =>
                await migrator.EnsureDatabaseExistsAsync(offlineConn));

            ex.Message.ShouldContain(databaseName);
            ex.Message.ShouldContain(nameof(SqlServerMigrator.DatabaseAvailabilityTimeout));
            ex.InnerException.ShouldBeOfType<SqlException>();
        }
        finally
        {
            await executeAgainstMasterAsync($"ALTER DATABASE [{databaseName}] SET ONLINE;");
            await dropDatabaseAsync(databaseName);
        }
    }

    private static async Task executeAgainstMasterAsync(string sql)
    {
        var adminBuilder = new SqlConnectionStringBuilder(ConnectionSource.ConnectionString)
        {
            InitialCatalog = "master"
        };
        await using var adminConn = new SqlConnection(adminBuilder.ConnectionString);
        await adminConn.OpenAsync();

        var cmd = adminConn.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync();
    }

    private static async Task dropDatabaseAsync(string databaseName)
    {
        var adminBuilder = new SqlConnectionStringBuilder(ConnectionSource.ConnectionString)
        {
            InitialCatalog = "master"
        };
        await using var adminConn = new SqlConnection(adminBuilder.ConnectionString);
        await adminConn.OpenAsync();

        var cmd = adminConn.CreateCommand();
        cmd.CommandText = $@"
            IF DB_ID(@name) IS NOT NULL
            BEGIN
                ALTER DATABASE [{databaseName.Replace("]", "]]")}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
                DROP DATABASE [{databaseName.Replace("]", "]]")}];
            END";
        var param = cmd.CreateParameter();
        param.ParameterName = "@name";
        param.Value = databaseName;
        cmd.Parameters.Add(param);
        await cmd.ExecuteNonQueryAsync();
    }
}
