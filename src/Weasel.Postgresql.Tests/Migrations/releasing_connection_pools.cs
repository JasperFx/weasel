using System;
using System.Linq;
using System.Threading.Tasks;
using JasperFx;
using JasperFx.Core;
using JasperFx.Descriptors;
using Npgsql;
using Shouldly;
using Weasel.Core;
using Weasel.Core.Migrations;
using Xunit;

namespace Weasel.Postgresql.Tests.Migrations;

/// <summary>
///     weasel#356: a one-shot walk over many databases (db-apply against a sharded store) should not drag a
///     tail of idle pools behind it. Proves that releasing a finished database's pool actually closes its
///     physical connections on the server, rather than leaving them idle until the connection idle lifetime
///     expires -- and that the database is still usable afterward.
/// </summary>
[Collection("pool_release")]
public class releasing_connection_pools: IAsyncLifetime
{
    // Tags this test's backends so the assertions can't be confused by any other activity on the server.
    private readonly string theApplicationName = "weasel_356_" + Guid.NewGuid().ToString("N")[..8];
    private NpgsqlDataSource theDataSource = null!;
    private TestDatabaseWithTables theDatabase = null!;

    public Task InitializeAsync()
    {
        var builder = new NpgsqlDataSourceBuilder(ConnectionSource.ConnectionString);
        builder.ConnectionStringBuilder.ApplicationName = theApplicationName;

        theDataSource = builder.Build();
        theDatabase = new TestDatabaseWithTables("pool_release", theDataSource);

        return Task.CompletedTask;
    }

    public Task DisposeAsync()
    {
        theDataSource.Dispose();
        return Task.CompletedTask;
    }

    [Fact]
    public async Task releasing_the_pool_closes_the_idle_connections_on_the_server()
    {
        await openAndReturnConnections(3);

        // Returned to the pool, but still open on the server -- this is the tail that piles up across a
        // many-database walk.
        (await countServerConnections()).ShouldBe(3);

        await theDatabase.ReleaseConnectionPoolAsync();

        await waitForServerConnectionsToReach(0);
    }

    [Fact]
    public async Task a_connection_string_database_releases_through_the_migrator()
    {
        // PostgresqlDatabase clears its NpgsqlDataSource directly, but a PostgreSQL database built on
        // DatabaseBase<NpgsqlConnection> with a connection string never inherits that override -- it has to
        // reach PostgresqlMigrator.ReleaseConnectionPoolAsync instead, like every other provider does.
        var database = new TestConnectionStringDatabase(theApplicationName);

        await using (var conn = database.CreateConnection())
        {
            await conn.OpenAsync();
        }

        await Should.NotThrowAsync(async () => await database.ReleaseConnectionPoolAsync());
        await database.AssertConnectivityAsync();
    }

    public class TestConnectionStringDatabase: DatabaseBase<NpgsqlConnection>
    {
        // Npgsql keys its connection-string pools by the whole connection string, so a distinct
        // ApplicationName gives this test its own pool. Without that, clearing it would evict connections
        // out from under the tests running in parallel in other collections -- the process-wide blast
        // radius that Migrator.ReleaseConnectionPoolAsync warns about, aimed at our own test suite.
        public TestConnectionStringDatabase(string applicationName): base(new DefaultMigrationLogger(),
            AutoCreate.All, new PostgresqlMigrator(), "pool_release", ConnectionStringFor(applicationName))
        {
        }

        private static string ConnectionStringFor(string applicationName) =>
            new NpgsqlConnectionStringBuilder(ConnectionSource.ConnectionString)
            {
                ApplicationName = applicationName
            }.ConnectionString;

        public override IFeatureSchema[] BuildFeatureSchemas() => [];

        public override DatabaseDescriptor Describe() => new()
        {
            Engine = PostgresqlProvider.EngineName, ServerName = "localhost", DatabaseName = "marten_testing"
        };
    }

    [Fact]
    public async Task the_database_is_still_usable_after_releasing_its_pool()
    {
        await openAndReturnConnections(1);
        await theDatabase.ReleaseConnectionPoolAsync();

        // Release closes idle connections; it does not tear the database's connectivity down.
        await theDatabase.AssertConnectivityAsync();
    }

    private async Task openAndReturnConnections(int count)
    {
        var connections = Enumerable.Range(0, count).Select(_ => theDataSource.CreateConnection()).ToArray();
        foreach (var conn in connections) await conn.OpenAsync();

        // Dispose only after all of them are open, so the pool really does hold `count` physical connections
        // rather than re-using a single one.
        foreach (var conn in connections) await conn.DisposeAsync();
    }

    private async Task<int> countServerConnections()
    {
        await using var conn = new NpgsqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "select count(*) from pg_stat_activity where application_name = $1";
        cmd.Parameters.AddWithValue(theApplicationName);

        return Convert.ToInt32(await cmd.ExecuteScalarAsync());
    }

    private async Task waitForServerConnectionsToReach(int expected)
    {
        // pg_stat_activity lags the actual backend teardown slightly, so poll rather than assert once.
        var timeout = DateTimeOffset.UtcNow.Add(5.Seconds());
        while (DateTimeOffset.UtcNow < timeout)
        {
            if (await countServerConnections() == expected) return;
            await Task.Delay(100);
        }

        (await countServerConnections()).ShouldBe(expected);
    }
}
