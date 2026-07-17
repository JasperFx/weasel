using JasperFx;
using JasperFx.Descriptors;
using Oracle.ManagedDataAccess.Client;
using Shouldly;
using Weasel.Core;
using Weasel.Core.Migrations;
using Xunit;

namespace Weasel.Oracle.Tests;

/// <summary>
///     weasel#356: proves the pool-release hook reaches Oracle. Oracle databases are built on
///     <see cref="DatabaseBase{T}" /> directly, so release arrives via the provider-specific
///     <see cref="Migrator" />.
/// </summary>
public class releasing_connection_pools
{
    [Fact]
    public async Task the_migrator_releases_the_pool_through_the_database_base()
    {
        var database = new TestOracleDatabase();

        await using (var conn = database.CreateConnection())
        {
            await conn.OpenAsync();
        }

        await database.ReleaseConnectionPoolAsync();

        // Releasing drops idle connections; the database must still work afterward.
        await database.AssertConnectivityAsync();
    }

    [Fact]
    public async Task releasing_the_pool_is_safe_when_nothing_has_been_pooled_yet()
    {
        var database = new TestOracleDatabase();

        await Should.NotThrowAsync(async () => await database.ReleaseConnectionPoolAsync());
    }

    public class TestOracleDatabase: DatabaseBase<OracleConnection>
    {
        public TestOracleDatabase(): base(new DefaultMigrationLogger(), AutoCreate.All, new OracleMigrator(),
            "pool_release", IsolatedConnectionString())
        {
        }

        // ODP.NET has no ApplicationName, but pools are keyed by the connection string all the same, so any
        // harmless distinct value gives this test its own pool -- and keeps its ClearPool from evicting
        // connections out from under tests running in parallel in other collections.
        private static string IsolatedConnectionString() =>
            new OracleConnectionStringBuilder(ConnectionSource.ConnectionString)
            {
                ConnectionLifeTime = Random.Shared.Next(100_000, 200_000)
            }.ConnectionString;

        public override IFeatureSchema[] BuildFeatureSchemas() => [];

        public override DatabaseDescriptor Describe() => new()
        {
            Engine = OracleProvider.EngineName, ServerName = "localhost", DatabaseName = "FREEPDB1"
        };
    }
}

/// <summary>
///     Unit coverage for the Oracle transient-connection-failure classification (weasel#356).
/// </summary>
public class OracleTransientConnectionErrorTests
{
    [Theory]
    [InlineData(20)] // ORA-00020 maximum number of processes exceeded
    [InlineData(12516)] // listener could not find an available handler
    [InlineData(12518)] // listener could not hand off the client connection
    [InlineData(12520)]
    public void transient_connection_errors(int number)
    {
        OracleMigrator.IsTransientConnectionError(number).ShouldBeTrue();
    }

    [Theory]
    [InlineData(12537)] // connection closed -- an established session dropped, not a refusal to connect
    [InlineData(12570)] // unexpected packet read error -- ditto; retrying could replay committed DDL
    [InlineData(1017)] // ORA-01017 invalid credentials -- never clears on retry
    [InlineData(955)] // ORA-00955 name already used -- a real migration failure
    [InlineData(60)] // ORA-00060 deadlock -- a statement conflict, not a connection refusal
    [InlineData(942)] // ORA-00942 table or view does not exist
    public void non_transient_errors(int number)
    {
        OracleMigrator.IsTransientConnectionError(number).ShouldBeFalse();
    }

    [Fact]
    public void non_oracle_exceptions_are_not_transient()
    {
        new OracleMigrator().IsTransientConnectionFailure(new InvalidOperationException("boom")).ShouldBeFalse();
    }
}
