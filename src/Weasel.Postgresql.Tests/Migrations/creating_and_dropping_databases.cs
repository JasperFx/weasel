using System.Collections.Generic;
using System.Threading.Tasks;
using Npgsql;
using Weasel.Postgresql.Migrations;
using Xunit;
using System;
using Shouldly;
using Weasel.Core;
using Weasel.Postgresql.Connections;

namespace Weasel.Postgresql.Tests.Migrations;

public class SingleInstanceDatabaseCollectionTests
{
    public Databases theDatabases = new Databases();

    public SingleInstanceDatabaseCollectionTests()
    {
        theDatabases.DropAndRecreate = true;
    }

    [Fact]
    public async Task can_build_databases_once()
    {
        var one = await theDatabases.FindOrCreateDatabase("one");
        var two = await theDatabases.FindOrCreateDatabase("two");
        var three = await theDatabases.FindOrCreateDatabase("three");

        one.ShouldNotBeSameAs(two);
        two.ShouldNotBeSameAs(three);

        (await theDatabases.FindOrCreateDatabase("one")).ShouldBeSameAs(one);
        (await theDatabases.FindOrCreateDatabase("two")).ShouldBeSameAs(two);
        (await theDatabases.FindOrCreateDatabase("three")).ShouldBeSameAs(three);
    }

    public class Databases: SingleServerDatabaseCollection<DatabaseWithTables>
    {
        public Databases() : base(new DefaultNpgsqlDataSourceFactory(), ConnectionSource.ConnectionString)
        {
        }

        protected override DatabaseWithTables buildDatabase(string databaseName, NpgsqlDataSource dataSource)
        {
            return new DatabaseWithTables(databaseName, dataSource);
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
