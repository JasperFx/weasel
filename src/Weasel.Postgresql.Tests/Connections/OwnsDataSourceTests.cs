using JasperFx;
using Npgsql;
using Shouldly;
using Weasel.Core;
using Weasel.Core.Migrations;
using Weasel.Postgresql.Connections;
using Xunit;

namespace Weasel.Postgresql.Tests.Connections;

public class OwnsDataSourceTests
{
    private static NpgsqlDataSource BuildDataSource() =>
        NpgsqlDataSource.Create(ConnectionSource.ConnectionString);

    [Fact]
    public void default_factory_owns_the_data_sources_it_builds()
    {
        using var factory = new DefaultNpgsqlDataSourceFactory();
        var dataSource = factory.Create(ConnectionSource.ConnectionString);

        factory.OwnsDataSource(dataSource).ShouldBeTrue();
    }

    [Fact]
    public void single_factory_does_not_own_the_wrapped_data_source()
    {
        using var wrapped = BuildDataSource();
        var factory = new SingleNpgsqlDataSourceFactory(
            connectionString => new NpgsqlDataSourceBuilder(connectionString), wrapped);

        factory.OwnsDataSource(wrapped).ShouldBeFalse();
    }

    [Fact]
    public void single_factory_owns_child_data_sources_it_builds_itself()
    {
        using var wrapped = BuildDataSource();
        var factory = new SingleNpgsqlDataSourceFactory(
            connectionString => new NpgsqlDataSourceBuilder(connectionString), wrapped);

        // A different connection string forces the factory to build its own child data source
        var builder = new NpgsqlConnectionStringBuilder(ConnectionSource.ConnectionString)
        {
            ApplicationName = "weasel-owns-test"
        };
        var child = factory.Create(builder.ConnectionString);

        child.ShouldNotBeSameAs(wrapped);
        factory.OwnsDataSource(child).ShouldBeTrue();
    }

    [Fact]
    public async Task dispose_does_not_dispose_data_source_when_not_owned()
    {
        var dataSource = BuildDataSource();
        var database = new TestDatabase(dataSource, ownsDataSource: false);

        database.OwnsDataSource.ShouldBeFalse();

        await database.DisposeAsync();

        // The externally-owned data source must NOT have been disposed. Opening may still fail if no live
        // database is present, but it must never fail with ObjectDisposedException.
        try
        {
            await using var conn = await dataSource.OpenConnectionAsync();
        }
        catch (ObjectDisposedException)
        {
            throw new ShouldAssertException("The externally-owned data source was disposed but should not have been");
        }
        catch
        {
            // any other failure (e.g. no live database) is irrelevant to this test
        }

        await dataSource.DisposeAsync();
    }

    [Fact]
    public async Task dispose_disposes_data_source_when_owned()
    {
        var dataSource = BuildDataSource();
        var database = new TestDatabase(dataSource, ownsDataSource: true);

        database.OwnsDataSource.ShouldBeTrue();

        await database.DisposeAsync();

        // An owned data source is disposed, so it can no longer hand out connections
        await Should.ThrowAsync<ObjectDisposedException>(async () => await dataSource.OpenConnectionAsync());
    }

    [Fact]
    public async Task dispose_disposes_data_source_by_default()
    {
        var dataSource = BuildDataSource();
        var database = new TestDatabase(dataSource);

        database.OwnsDataSource.ShouldBeTrue();

        await database.DisposeAsync();

        await Should.ThrowAsync<ObjectDisposedException>(async () => await dataSource.OpenConnectionAsync());
    }

    private class TestDatabase: PostgresqlDatabase
    {
        public TestDatabase(NpgsqlDataSource dataSource)
            : base(new DefaultMigrationLogger(), AutoCreate.All, new PostgresqlMigrator(), "owns-test", dataSource)
        {
        }

        public TestDatabase(NpgsqlDataSource dataSource, bool ownsDataSource)
            : base(new DefaultMigrationLogger(), AutoCreate.All, new PostgresqlMigrator(), "owns-test", dataSource,
                ownsDataSource)
        {
        }

        public override IFeatureSchema[] BuildFeatureSchemas() => [];
    }
}
