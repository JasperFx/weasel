using JasperFx;
using JasperFx.Descriptors;
using MySqlConnector;
using Shouldly;
using Weasel.Core;
using Weasel.Core.Migrations;
using Xunit;

namespace Weasel.MySql.Tests;

/// <summary>
///     weasel#356: proves the pool-release hook reaches MySQL. MySQL databases are built on
///     <see cref="DatabaseBase{T}" /> directly, so release arrives via the provider-specific
///     <see cref="Migrator" />.
/// </summary>
public class releasing_connection_pools
{
    [Fact]
    public async Task the_migrator_releases_the_pool_through_the_database_base()
    {
        var database = new TestMySqlDatabase();

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
        var database = new TestMySqlDatabase();

        await Should.NotThrowAsync(async () => await database.ReleaseConnectionPoolAsync());
    }

    public class TestMySqlDatabase: DatabaseBase<MySqlConnection>
    {
        public TestMySqlDatabase(): base(new DefaultMigrationLogger(), AutoCreate.All, new MySqlMigrator(),
            "pool_release", ConnectionSource.ConnectionString)
        {
        }

        public override IFeatureSchema[] BuildFeatureSchemas() => [];

        public override DatabaseDescriptor Describe() => new()
        {
            Engine = MySqlProvider.EngineName, ServerName = "localhost", DatabaseName = "weasel_testing"
        };
    }
}

/// <summary>
///     Unit coverage for the MySQL transient-connection-failure classification (weasel#356).
/// </summary>
public class MySqlTransientConnectionErrorTests
{
    [Theory]
    [InlineData(MySqlErrorCode.ConnectionCountError)] // 1040 too many connections
    [InlineData(MySqlErrorCode.TooManyUserConnections)] // 1203
    [InlineData(MySqlErrorCode.UserLimitReached)] // 1226
    [InlineData(MySqlErrorCode.UnableToConnectToHost)]
    public void transient_connection_errors(MySqlErrorCode code)
    {
        MySqlMigrator.IsTransientConnectionError(code).ShouldBeTrue();
    }

    [Theory]
    [InlineData(MySqlErrorCode.AccessDenied)] // credentials never clear on retry
    [InlineData(MySqlErrorCode.LockDeadlock)] // statement conflict, not a connection refusal
    [InlineData(MySqlErrorCode.DuplicateKeyEntry)] // a real migration failure
    [InlineData(MySqlErrorCode.ParseError)]
    public void non_transient_errors(MySqlErrorCode code)
    {
        MySqlMigrator.IsTransientConnectionError(code).ShouldBeFalse();
    }

    [Fact]
    public void non_mysql_exceptions_are_not_transient()
    {
        new MySqlMigrator().IsTransientConnectionFailure(new InvalidOperationException("boom")).ShouldBeFalse();
    }
}
