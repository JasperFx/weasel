using System.Collections.Generic;
using System.Threading.Tasks;
using Npgsql;
using Weasel.Postgresql.Migrations;
using Xunit;
using Shouldly;
using Weasel.Core;
using Weasel.Postgresql.Connections;

namespace Weasel.Postgresql.Tests.Migrations;

public class SingleInstanceDatabaseCollectionTests: IAsyncLifetime
{
    public Databases theDatabases = new Databases();

    // Databases are server-scoped and several worker lanes share one server, so the names are unique
    // and the databases are dropped afterwards.
    private readonly string[] _names = Enumerable.Range(0, 3)
        .Select(_ => "weasel_collection_" + Guid.NewGuid().ToString("N")[..12]).ToArray();

    public SingleInstanceDatabaseCollectionTests()
    {
        theDatabases.DropAndRecreate = true;
    }

    public ValueTask InitializeAsync() => ValueTask.CompletedTask;

    public async ValueTask DisposeAsync()
    {
        await using var conn = new NpgsqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();
        foreach (var name in _names)
        {
            await conn.KillIdleSessions(name);
            await conn.DropDatabase(name);
        }
    }

    [Fact]
    public async Task can_build_databases_once()
    {
        var one = await theDatabases.FindOrCreateDatabase(_names[0]);
        var two = await theDatabases.FindOrCreateDatabase(_names[1]);
        var three = await theDatabases.FindOrCreateDatabase(_names[2]);

        one.ShouldNotBeSameAs(two);
        two.ShouldNotBeSameAs(three);

        (await theDatabases.FindOrCreateDatabase(_names[0])).ShouldBeSameAs(one);
        (await theDatabases.FindOrCreateDatabase(_names[1])).ShouldBeSameAs(two);
        (await theDatabases.FindOrCreateDatabase(_names[2])).ShouldBeSameAs(three);
    }

    [Fact]
    public async Task drop_and_recreate_leaves_the_database_in_place()
    {
        // It used to drop the database and never create it again
        await using var conn = new NpgsqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();
        await new DatabaseSpecification().BuildDatabase(conn, _names[0]);

        await theDatabases.FindOrCreateDatabase(_names[0]);
        (await conn.DatabaseExists(_names[0])).ShouldBeTrue();

        var again = new Databases { DropAndRecreate = true };
        again.FindOrCreateDatabaseSynchronously(_names[0]);
        (await conn.DatabaseExists(_names[0])).ShouldBeTrue();
    }

    public class Databases: SingleServerDatabaseCollection<TestDatabaseWithTables>
    {
        public Databases() : base(new DefaultNpgsqlDataSourceFactory(), ConnectionSource.ConnectionString)
        {
        }

        protected override TestDatabaseWithTables buildDatabase(string databaseName, NpgsqlDataSource dataSource)
        {
            return new TestDatabaseWithTables(databaseName, dataSource);
        }
    }
}

[Collection("integration")]
public class creating_and_dropping_databases
{
    public static int DatabaseCount = 0;
    private DatabaseSpecification theSpecification = new DatabaseSpecification();

    private async Task DropDatabaseIfExists(string databaseName)
    {
        using var conn = new NpgsqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();

        await conn.KillIdleSessions(databaseName);
        await conn.DropDatabase(databaseName);
    }

    private async Task<IReadOnlyList<string>> DatabaseNames()
    {
        using var conn = new NpgsqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();
        return await conn.AllDatabaseNames();
    }

    private async Task AssertCanCreateDatabase(Action<DatabaseSpecification> configure = null)
    {
        var databaseName = "database" + ++DatabaseCount;
        var specification = new DatabaseSpecification();
        configure?.Invoke(specification);

        using var conn = new NpgsqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();
        await conn.KillIdleSessions(databaseName);
        await conn.DropDatabase(databaseName);

        var names = await conn.AllDatabaseNames();
        names.ShouldNotContain(databaseName);

        await specification.BuildDatabase(conn, databaseName);

        names = await conn.AllDatabaseNames();

        names.ShouldContain(databaseName);
    }

    //[Fact]
    public async Task all_defaults()
    {
        await AssertCanCreateDatabase();
        await AssertCanCreateDatabase(s => s.Encoding = "UTF-8");
        await AssertCanCreateDatabase(s => s.Owner = "postgres");
        await AssertCanCreateDatabase(s => s.ConnectionLimit = 5);

    }
}
