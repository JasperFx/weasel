using JasperFx;
using JasperFx.Descriptors;
using Microsoft.Data.SqlClient;
using Shouldly;
using Weasel.Core;
using Weasel.Core.Migrations;
using Xunit;

namespace Weasel.SqlServer.Tests;

/// <summary>
///     weasel#356: proves the pool-release hook reaches SQL Server. SQL Server databases are built on
///     <see cref="DatabaseBase{T}" /> directly (there is no SqlServerDatabase class), so release has to
///     arrive via the provider-specific <see cref="Migrator" /> -- which is what these tests exercise.
/// </summary>
public class releasing_connection_pools
{
    [Fact]
    public async Task the_migrator_releases_the_pool_through_the_database_base()
    {
        var database = new TestSqlServerDatabase();

        await using (var conn = database.CreateConnection())
        {
            await conn.OpenAsync();
        }

        // Reaches SqlServerMigrator.ReleaseConnectionPoolAsync via DatabaseBase, without any
        // SQL-Server-specific IDatabase implementation existing.
        await database.ReleaseConnectionPoolAsync();

        // Releasing the pool must not break the database -- it drops idle connections, nothing more.
        await database.AssertConnectivityAsync();
    }

    [Fact]
    public async Task releasing_the_pool_is_safe_when_nothing_has_been_pooled_yet()
    {
        var database = new TestSqlServerDatabase();

        await Should.NotThrowAsync(async () => await database.ReleaseConnectionPoolAsync());
    }

    [Fact]
    public async Task releasing_the_pool_closes_the_idle_connections_on_the_server()
    {
        // Tags this test's sessions so the count can't be confused by other activity on the server.
        var applicationName = "weasel_356_" + Guid.NewGuid().ToString("N")[..8];
        var connectionString =
            new SqlConnectionStringBuilder(ConnectionSource.ConnectionString) { ApplicationName = applicationName }
                .ConnectionString;

        var database = new TestSqlServerDatabase(connectionString);

        var connections = new List<SqlConnection>();
        for (var i = 0; i < 3; i++)
        {
            var conn = database.CreateConnection();
            await conn.OpenAsync();
            connections.Add(conn);
        }

        // Return them to the pool only once all three are open, so the pool really holds three physical
        // connections rather than re-using one.
        foreach (var conn in connections) await conn.DisposeAsync();

        // Idle in the pool, but still open on the server -- the tail that piles up across a long walk.
        (await countServerSessions(applicationName)).ShouldBe(3);

        await database.ReleaseConnectionPoolAsync();

        await waitForServerSessionsToReach(applicationName, 0);
    }

    private static async Task<int> countServerSessions(string applicationName)
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "select count(*) from sys.dm_exec_sessions where program_name = @name";
        cmd.Parameters.AddWithValue("@name", applicationName);

        return Convert.ToInt32(await cmd.ExecuteScalarAsync());
    }

    private static async Task waitForServerSessionsToReach(string applicationName, int expected)
    {
        // The DMV lags the actual teardown slightly, so poll rather than assert once.
        var timeout = DateTimeOffset.UtcNow.AddSeconds(5);
        while (DateTimeOffset.UtcNow < timeout)
        {
            if (await countServerSessions(applicationName) == expected) return;
            await Task.Delay(100);
        }

        (await countServerSessions(applicationName)).ShouldBe(expected);
    }

    public class TestSqlServerDatabase: DatabaseBase<SqlConnection>
    {
        public TestSqlServerDatabase(): this(ConnectionSource.ConnectionString)
        {
        }

        public TestSqlServerDatabase(string connectionString): base(new DefaultMigrationLogger(), AutoCreate.All,
            new SqlServerMigrator(), "pool_release", connectionString)
        {
        }

        public override IFeatureSchema[] BuildFeatureSchemas() => [];

        public override DatabaseDescriptor Describe() => new()
        {
            Engine = SqlServerProvider.EngineName, ServerName = "localhost", DatabaseName = "master"
        };
    }
}

/// <summary>
///     Unit coverage for the SQL Server transient-connection-failure classification (weasel#356). Pure over
///     the error number, so the retry-worthy set is testable without provoking a real throttling event.
/// </summary>
public class SqlServerTransientConnectionErrorTests
{
    [Theory]
    [InlineData(17809)] // too many user connections (on-premises)
    [InlineData(40501)] // service is busy
    [InlineData(40613)] // database currently unavailable
    [InlineData(10928)] // resource limits reached
    [InlineData(10929)]
    [InlineData(49918)] // not enough resources
    [InlineData(49920)]
    [InlineData(40197)]
    public void transient_connection_errors(int number)
    {
        SqlServerMigrator.IsTransientConnectionError(number).ShouldBeTrue();
    }

    [Theory]
    [InlineData(-2)] // timeout expired -- SqlClient raises this for COMMAND timeouts too, so retrying it
                     // would silently re-run a migration that merely exceeded CommandTimeout
    [InlineData(10054)] // transport-level drop -- indistinguishable from a half-applied migration
    [InlineData(233)]
    [InlineData(18456)] // login failed -- a credential problem, never clears on retry
    [InlineData(1205)] // deadlock -- a statement conflict, not a connection refusal
    [InlineData(2601)] // duplicate key -- a real migration failure
    [InlineData(0)]
    public void non_transient_errors(int number)
    {
        SqlServerMigrator.IsTransientConnectionError(number).ShouldBeFalse();
    }

    [Fact]
    public void non_sql_exceptions_are_not_transient()
    {
        new SqlServerMigrator().IsTransientConnectionFailure(new InvalidOperationException("boom")).ShouldBeFalse();
    }
}
