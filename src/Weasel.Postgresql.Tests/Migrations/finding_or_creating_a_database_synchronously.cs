using System.Collections.Concurrent;
using System.Reflection;
using Npgsql;
using Shouldly;
using Weasel.Postgresql.Connections;
using Weasel.Postgresql.Migrations;
using Xunit;

namespace Weasel.Postgresql.Tests.Migrations;

/// <summary>
///     weasel#839: <see cref="SingleServerDatabaseCollection{T}.FindOrCreateDatabaseSynchronously" />, for a
///     store's synchronous tenant lookup, which used to block a pool thread on
///     <see cref="SingleServerDatabaseCollection{T}.FindOrCreateDatabase" /> (marten#5985).
/// </summary>
/// <remarks>
///     Databases are server-scoped, so every name here carries this class's prefix and a fresh suffix:
///     several worker lanes share one server.
/// </remarks>
public class finding_or_creating_a_database_synchronously: IAsyncLifetime
{
    private readonly List<string> _created = [];

    public ValueTask InitializeAsync() => ValueTask.CompletedTask;

    public async ValueTask DisposeAsync()
    {
        await using var conn = new NpgsqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();
        foreach (var name in _created)
        {
            await conn.KillIdleSessions(name);
            await conn.DropDatabase(name);
        }
    }

    private string NewDatabaseName()
    {
        var name = "weasel_sync_foc_" + Guid.NewGuid().ToString("N")[..12];
        _created.Add(name);
        return name;
    }

    private static async Task<bool> ExistsOnServer(string name)
    {
        await using var conn = new NpgsqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();
        return await conn.DatabaseExists(name);
    }

    [Fact]
    public async Task creates_a_missing_database_and_caches_it_for_both_paths()
    {
        var databases = new Databases();
        var name = NewDatabaseName();
        (await ExistsOnServer(name)).ShouldBeFalse();

        var database = databases.FindOrCreateDatabaseSynchronously(name);

        (await ExistsOnServer(name)).ShouldBeTrue();
        databases.FindOrCreateDatabaseSynchronously(name).ShouldBeSameAs(database);
        (await databases.FindOrCreateDatabase(name)).ShouldBeSameAs(database);
        databases.AllDatabases().ShouldBe([database]);
    }

    [Fact]
    public async Task finds_what_the_asynchronous_path_created()
    {
        var databases = new Databases();
        var name = NewDatabaseName();

        var database = await databases.FindOrCreateDatabase(name);

        databases.FindOrCreateDatabaseSynchronously(name).ShouldBeSameAs(database);
    }

    [Fact]
    public async Task an_existing_database_is_used_rather_than_created_again()
    {
        var name = NewDatabaseName();
        await new Databases().FindOrCreateDatabase(name);

        // A second process, with an empty cache.
        var databases = new Databases();
        databases.FindOrCreateDatabaseSynchronously(name).ShouldNotBeNull();
        databases.AllDatabases().Count.ShouldBe(1);
    }

    [Fact]
    public async Task a_synchronous_caller_does_not_wait_for_the_asynchronous_lock()
    {
        var databases = new Databases();
        var name = NewDatabaseName();

        // Take the lock FindOrCreateDatabase holds across its awaits, as a parked asynchronous caller
        // would. It is internal to Weasel.Core, so it is reached by reflection.
        var timedLock = typeof(SingleServerDatabaseCollection<TestDatabaseWithTables>)
            .GetField("_lock", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(databases)!;
        var take = (Task)timedLock.GetType().GetMethod("Lock")!
            .Invoke(timedLock, [TimeSpan.FromSeconds(5), "the test", CancellationToken.None])!;
        await take;
        var releaser = (IDisposable)take.GetType().GetProperty("Result")!.GetValue(take)!;

        try
        {
            // The asynchronous path would time out here after five seconds. On a thread of its own, so
            // a regression fails the WaitAsync rather than hanging the runner.
            var database = await Task.Run(() => databases.FindOrCreateDatabaseSynchronously(name))
                .WaitAsync(TimeSpan.FromSeconds(4));

            database.ShouldNotBeNull();
            (await ExistsOnServer(name)).ShouldBeTrue();
        }
        finally
        {
            releaser.Dispose();
        }
    }

    [Fact]
    public async Task concurrent_synchronous_and_asynchronous_callers_get_one_database()
    {
        var databases = new Databases();
        var name = NewDatabaseName();
        var results = new ConcurrentBag<TestDatabaseWithTables>();
        using var start = new Barrier(16);

        // Both paths can be inside CREATE DATABASE for one name at once; the loser's error is ignored.
        var callers = Enumerable.Range(0, 16).Select(i => Task.Run(async () =>
        {
            start.SignalAndWait(TimeSpan.FromSeconds(10));
            results.Add(i % 2 == 0
                ? databases.FindOrCreateDatabaseSynchronously(name)
                : await databases.FindOrCreateDatabase(name));
        }));

        await Task.WhenAll(callers).WaitAsync(TimeSpan.FromSeconds(30));

        results.Distinct().Count().ShouldBe(1);
        databases.AllDatabases().Count.ShouldBe(1);
        (await ExistsOnServer(name)).ShouldBeTrue();
    }

    [Fact]
    public async Task two_collections_creating_one_database_at_once_both_succeed()
    {
        // The cross-process shape: nothing shared but the server, so CREATE DATABASE really races.
        var name = NewDatabaseName();
        for (var round = 0; round < 5; round++)
        {
            var collections = Enumerable.Range(0, 4).Select(_ => new Databases()).ToArray();
            using var start = new Barrier(collections.Length);

            await Task.WhenAll(collections.Select(c => Task.Run(() =>
            {
                start.SignalAndWait(TimeSpan.FromSeconds(10));
                return c.FindOrCreateDatabaseSynchronously(name);
            }))).WaitAsync(TimeSpan.FromSeconds(30));

            await using var conn = new NpgsqlConnection(ConnectionSource.ConnectionString);
            await conn.OpenAsync();
            await conn.KillIdleSessions(name);
            await conn.DropDatabase(name);
        }
    }

    // Npgsql 11 removed multiplexing and rejects the Multiplexing keyword, so Weasel.Postgresql.Npgsql11.Tests
    // leaves this scenario out (weasel#910).
#if !NPGSQL11
    [Fact]
    public async Task a_multiplexing_master_falls_back_to_the_asynchronous_path()
    {
        // Npgsql refuses synchronous I/O under multiplexing.
        var databases = new Databases(ConnectionSource.ConnectionString + ";Multiplexing=true");
        var name = NewDatabaseName();

        var database = databases.FindOrCreateDatabaseSynchronously(name);

        database.ShouldNotBeNull();
        (await ExistsOnServer(name)).ShouldBeTrue();
        (await databases.FindOrCreateDatabase(name)).ShouldBeSameAs(database);
    }
#endif

    public class Databases(string? masterConnectionString = null)
        : SingleServerDatabaseCollection<TestDatabaseWithTables>(new DefaultNpgsqlDataSourceFactory(),
            masterConnectionString ?? ConnectionSource.ConnectionString)
    {
        protected override TestDatabaseWithTables buildDatabase(string databaseName, NpgsqlDataSource dataSource) =>
            new(databaseName, dataSource);
    }
}
