using JasperFx.Core;
using Microsoft.Data.SqlClient;
using Shouldly;
using Weasel.Core;
using Xunit;

namespace Weasel.SqlServer.Tests;

/// <summary>
///     weasel#875: the <c>CREATE DATABASE</c> in <see cref="SqlServerMigrator.EnsureDatabaseExistsAsync" />
///     gets <see cref="Migrator.ProvisioningCommandTimeout" /> (two minutes by default) instead of the
///     connection's command timeout. Every connection here has a one second command timeout.
/// </summary>
public class provisioning_command_timeout
{
    private static string OneSecondTimeout(string initialCatalog) =>
        new SqlConnectionStringBuilder(ConnectionSource.ConnectionString)
        {
            InitialCatalog = initialCatalog, CommandTimeout = 1
        }.ConnectionString;

    [Fact]
    public void the_default_is_two_minutes()
    {
        new SqlServerMigrator().ProvisioningCommandTimeout.ShouldBe(2.Minutes());
    }

    [Fact]
    public async Task create_database_gets_the_provisioning_timeout_and_ordinary_commands_do_not()
    {
        var migrator = new SqlServerMigrator();
        await using var conn = new SqlConnection(OneSecondTimeout("master"));

        await using (var create = migrator.CreateDatabaseCommand(conn, "anything"))
        {
            create.CommandTimeout.ShouldBe(120);
        }

        migrator.ProvisioningCommandTimeout = 30.Seconds();
        await using (var create = migrator.CreateDatabaseCommand(conn, "anything"))
        {
            create.CommandTimeout.ShouldBe(30);
        }

        await using var query = conn.CreateCommand();
        query.CommandTimeout.ShouldBe(1);
    }

    [Fact]
    public async Task ensure_database_exists_creates_a_database_under_a_one_second_command_timeout()
    {
        var databaseName = $"weasel_provisioning_{Guid.NewGuid():N}";

        try
        {
            await using var conn = new SqlConnection(OneSecondTimeout(databaseName));
            await new SqlServerMigrator().EnsureDatabaseExistsAsync(conn);

            await using var verify = new SqlConnection(OneSecondTimeout(databaseName));
            await verify.OpenAsync();
        }
        finally
        {
            await dropDatabaseAsync(databaseName);
        }
    }

    private static async Task dropDatabaseAsync(string databaseName)
    {
        var adminBuilder = new SqlConnectionStringBuilder(ConnectionSource.ConnectionString)
        {
            InitialCatalog = "master"
        };
        await using var adminConn = new SqlConnection(adminBuilder.ConnectionString);
        await adminConn.OpenAsync();

        await using var cmd = adminConn.CreateCommand();
        cmd.CommandTimeout = 120;
        cmd.CommandText = $@"
            IF DB_ID('{databaseName}') IS NOT NULL
            BEGIN
                ALTER DATABASE [{databaseName}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
                DROP DATABASE [{databaseName}];
            END";
        await cmd.ExecuteNonQueryAsync();
    }
}
