using FirebirdSql.Data.FirebirdClient;
using JasperFx;
using JasperFx.Descriptors;
using NSubstitute;
using Shouldly;
using Weasel.Core;
using Weasel.Core.Migrations;
using Xunit;

namespace Weasel.Firebird.Tests;

/// <summary>
///     weasel#356 for Firebird. FirebirdClient does not validate a pooled connection when it hands one
///     out, so after a server restart the pool is full of dead attachments -- which is exactly when a
///     host wants to release it -- and an idle pooled attachment still holds the tables it touched.
/// </summary>
public class releasing_connection_pools: IntegrationContext
{
    private string pooledConnectionString()
        => new FbConnectionStringBuilder(ConnectionString)
        {
            Pooling = true,
            // Its own pool key, so clearing it cannot evict anybody else's connections.
            ApplicationName = "weasel_356_" + Guid.NewGuid().ToString("N")[..8]
        }.ConnectionString;

    private Task<int> otherUserAttachmentsAsync()
        => ScalarAsync<int>(
            "SELECT COUNT(*) FROM MON$ATTACHMENTS WHERE MON$ATTACHMENT_ID <> CURRENT_CONNECTION AND MON$SYSTEM_FLAG = 0");

    [Fact]
    public async Task releasing_the_pool_closes_the_idle_attachments()
    {
        var connectionString = pooledConnectionString();

        await using (var conn = new FbConnection(connectionString))
        {
            await conn.OpenAsync();
        }

        var before = await otherUserAttachmentsAsync();
        before.ShouldBeGreaterThan(0, "the closed connection went back to the pool, still attached");

        await new FirebirdMigrator().ReleaseConnectionPoolAsync(new FbConnection(connectionString));

        var after = await otherUserAttachmentsAsync();
        after.ShouldBeLessThan(before);
    }

    [Fact]
    public async Task the_database_base_releases_through_the_migrator_and_still_works_afterwards()
    {
        var database = new TestFirebirdDatabase(pooledConnectionString());

        await using (var conn = database.CreateConnection())
        {
            await conn.OpenAsync();
        }

        await database.ReleaseConnectionPoolAsync();

        await database.AssertConnectivityAsync();
    }

    [Fact]
    public async Task releasing_is_safe_when_nothing_is_pooled_or_the_connection_is_not_firebirds()
    {
        await Should.NotThrowAsync(async () =>
            await new FirebirdMigrator().ReleaseConnectionPoolAsync(new FbConnection(pooledConnectionString())));
        await Should.NotThrowAsync(async () =>
            await new FirebirdMigrator().ReleaseConnectionPoolAsync(Substitute.For<System.Data.Common.DbConnection>()));
    }

    public class TestFirebirdDatabase(string connectionString)
        : DatabaseBase<FbConnection>(new DefaultMigrationLogger(), AutoCreate.All, new FirebirdMigrator(),
            "pool_release", connectionString)
    {
        public override IFeatureSchema[] BuildFeatureSchemas() => [];

        public override DatabaseDescriptor Describe() => new()
        {
            Engine = FirebirdProvider.EngineName, ServerName = "localhost", DatabaseName = "pool_release"
        };
    }
}
