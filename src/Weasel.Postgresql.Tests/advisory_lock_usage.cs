using System.Runtime.ExceptionServices;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using Shouldly;
using Weasel.Core;
using Weasel.Core.Migrations;
using Xunit;

namespace Weasel.Postgresql.Tests;

public class advisory_lock_usage
{


    [Fact]
    public async Task explicitly_release_global_session_locks()
    {
        await using var conn1 = new NpgsqlConnection(ConnectionSource.ConnectionString);
        await using var conn2 = new NpgsqlConnection(ConnectionSource.ConnectionString);
        await using var conn3 = new NpgsqlConnection(ConnectionSource.ConnectionString);

        await conn1.OpenAsync();
        await conn2.OpenAsync();
        await conn3.OpenAsync();

        await conn1.GetGlobalLock(1);

        // Cannot get the lock here
        (await conn2.TryGetGlobalLock(1)).Succeeded.ShouldBeFalse();

        await conn1.ReleaseGlobalLock(1);

        for (var j = 0; j < 5; j++)
        {
            if ((await conn2.TryGetGlobalLock(1)).Succeeded) return;

            await Task.Delay(250);
        }

        throw new Exception("Advisory lock was not released");
    }

    [Fact]
    public async Task explicitly_release_global_tx_session_locks()
    {
        await using var conn1 = new NpgsqlConnection(ConnectionSource.ConnectionString);
        await using var conn2 = new NpgsqlConnection(ConnectionSource.ConnectionString);
        await using var conn3 = new NpgsqlConnection(ConnectionSource.ConnectionString);

        await conn1.OpenAsync();
        await conn2.OpenAsync();
        await conn3.OpenAsync();

        var tx1 = await conn1.BeginTransactionAsync();
        await tx1.GetGlobalTxLock(2);


        // Cannot get the lock here
        var tx2 = await conn2.BeginTransactionAsync();
        (await tx2.TryGetGlobalTxLock(2)).Succeeded.ShouldBeFalse();


        await tx1.RollbackAsync();


        for (var j = 0; j < 5; j++)
        {
            if ((await tx2.TryGetGlobalTxLock(2)).Succeeded)
            {
                await tx2.RollbackAsync();
                return;
            }

            await Task.Delay(250);
        }

        throw new Exception("Advisory lock was not released");
    }

    [Fact] // - too slow
    public async Task global_session_locks()
    {
        await using var conn1 = new NpgsqlConnection(ConnectionSource.ConnectionString);
        await using var conn2 = new NpgsqlConnection(ConnectionSource.ConnectionString);
        await using var conn3 = new NpgsqlConnection(ConnectionSource.ConnectionString);

        await conn1.OpenAsync();
        await conn2.OpenAsync();
        await conn3.OpenAsync();

        await conn1.GetGlobalLock(24);

        try
        {
            // Cannot get the lock here
            (await conn2.TryGetGlobalLock(24)).Succeeded.ShouldBeFalse();

            // Can get the new lock
            (await conn3.TryGetGlobalLock(25)).Succeeded.ShouldBeTrue();

            // Cannot get the lock here
            (await conn2.TryGetGlobalLock(25)).Succeeded.ShouldBeFalse();
        }
        finally
        {
            await conn1.ReleaseGlobalLock(24);
            await conn3.ReleaseGlobalLock(25);
        }
    }

    [Fact] // -- too slow
    public async Task tx_session_locks()
    {
        await using var conn1 = new NpgsqlConnection(ConnectionSource.ConnectionString);
        await using var conn2 = new NpgsqlConnection(ConnectionSource.ConnectionString);
        await using var conn3 = new NpgsqlConnection(ConnectionSource.ConnectionString);

        await conn1.OpenAsync();
        await conn2.OpenAsync();
        await conn3.OpenAsync();

        var tx1 = await conn1.BeginTransactionAsync();
        await tx1.GetGlobalTxLock(4);


        // Cannot get the lock here
        var tx2 = await conn2.BeginTransactionAsync();
        (await tx2.TryGetGlobalTxLock(4)).Succeeded.ShouldBeFalse();

        // Can get the new lock
        var tx3 = await conn3.BeginTransactionAsync();
        (await tx3.TryGetGlobalTxLock(5)).Succeeded.ShouldBeTrue();

        // Cannot get the lock here
        (await tx2.TryGetGlobalTxLock(5)).Succeeded.ShouldBeFalse();

        await tx1.RollbackAsync();
        await tx2.RollbackAsync();
        await tx3.RollbackAsync();
    }
}

public class AdvisoryLockSpecs : IAsyncLifetime
{
    private NpgsqlDataSource _database = null!;
    private AdvisoryLock theLock = null!;

    public Task InitializeAsync()
    {
        _database = NpgsqlDataSource.Create(ConnectionSource.ConnectionString);
        theLock = new AdvisoryLock(_database, NullLogger.Instance, "localhost", new AdvisoryLockOptions());
        return Task.CompletedTask;
    }

    public async Task DisposeAsync()
    {
        await theLock.DisposeAsync();
        await _database.DisposeAsync();
    }


    [Fact]
    public async Task explicitly_release_global_session_locks()
    {
        await using var conn2 = new NpgsqlConnection(ConnectionSource.ConnectionString);
        await using var conn3 = new NpgsqlConnection(ConnectionSource.ConnectionString);

        await conn2.OpenAsync();
        await conn3.OpenAsync();

        await theLock.TryAttainLockAsync(10, CancellationToken.None);

        // Cannot get the lock here
        (await conn2.TryGetGlobalLock(10)).Succeeded.ShouldBeFalse();

        await theLock.ReleaseLockAsync(10);

        for (var j = 0; j < 5; j++)
        {
            if ((await conn2.TryGetGlobalLock(10)).Succeeded) return;

            await Task.Delay(250);
        }

        throw new Exception("Advisory lock was not released");
    }

}

[Collection("advisory_lock_disposal_guard")]
public class advisory_lock_disposal_guard
{
    // weasel#349: during host shutdown on a HotCold cold/standby node, the projection coordinator's
    // leadership poll can call TryAttainLockAsync while the owned NpgsqlDataSource is being disposed,
    // which used to abort the process with ObjectDisposedException: 'Npgsql.PoolingDataSource'.
    //
    // weasel#353 / marten#4915: #349 swallowed that exception and returned false. The coordinator reads
    // false as "the lock is held elsewhere" and keeps polling on its cadence, so a disposed data source
    // produced an unbounded churn of aborted opens, and the terminate-on-ObjectDisposedException catch
    // that jasperfx#500 added to ProjectionCoordinatorBase.executeAsync could never fire. The exception is
    // now terminal: the lock latches itself disposed and rethrows once.

    [Fact]
    public async Task throws_once_when_the_data_source_is_disposed_out_from_under_it()
    {
        var dataSource = NpgsqlDataSource.Create(ConnectionSource.ConnectionString);
        var theLock = new AdvisoryLock(dataSource, NullLogger.Instance, "localhost", new AdvisoryLockOptions());

        // Simulate the shutdown ordering where the data source is torn down before/while the lock is used.
        await dataSource.DisposeAsync();

        // The data source is gone for good, so this is terminal rather than "not attained right now". The
        // coordinator's ObjectDisposedException catch ends its leadership loop on exactly this.
        await Should.ThrowAsync<ObjectDisposedException>(
            () => theLock.TryAttainLockAsync(4242, CancellationToken.None));

        await theLock.DisposeAsync();
    }

    [Fact]
    public async Task latches_disposed_so_a_later_poll_never_touches_the_dead_pool()
    {
        var dataSource = NpgsqlDataSource.Create(ConnectionSource.ConnectionString);
        var theLock = new AdvisoryLock(dataSource, NullLogger.Instance, "localhost", new AdvisoryLockOptions());

        await dataSource.DisposeAsync();

        await Should.ThrowAsync<ObjectDisposedException>(
            () => theLock.TryAttainLockAsync(4244, CancellationToken.None));

        // A caller that ignores the throw and keeps polling — which is what any coordinator without the
        // jasperfx#500 catch does — must be answered from the latch, not from the disposed pool. Returning
        // false is only reachable through the _disposed short-circuit: every path that actually reaches the
        // pool throws ObjectDisposedException. The first-chance count corroborates that directly.
        var aborts = 0;
        EventHandler<FirstChanceExceptionEventArgs> handler = (_, e) =>
        {
            if (e.Exception is ObjectDisposedException ode && ode.ObjectName?.Contains("PoolingDataSource") == true)
            {
                Interlocked.Increment(ref aborts);
            }
        };

        AppDomain.CurrentDomain.FirstChanceException += handler;
        try
        {
            for (var i = 0; i < 5; i++)
            {
                (await theLock.TryAttainLockAsync(4244, CancellationToken.None)).ShouldBeFalse();
            }
        }
        finally
        {
            AppDomain.CurrentDomain.FirstChanceException -= handler;
        }

        aborts.ShouldBe(0);

        await theLock.DisposeAsync();
    }

    [Fact]
    public async Task returns_false_once_the_lock_itself_has_begun_disposing()
    {
        var dataSource = NpgsqlDataSource.Create(ConnectionSource.ConnectionString);
        var theLock = new AdvisoryLock(dataSource, NullLogger.Instance, "localhost", new AdvisoryLockOptions());

        await theLock.DisposeAsync();

        // Never start a new acquire after disposal has begun, even though the data source is still alive here.
        var attained = await theLock.TryAttainLockAsync(4243, CancellationToken.None);
        attained.ShouldBeFalse();

        await dataSource.DisposeAsync();
    }
}

// FirstChanceException is an AppDomain-wide hook, so the latch test above cannot tolerate another
// collection concurrently provoking a disposed-pool abort. Pin this class to a serial collection.
[CollectionDefinition("advisory_lock_disposal_guard", DisableParallelization = true)]
public class AdvisoryLockDisposalGuardCollection;

public class SimplePostgresqlDatabase: PostgresqlDatabase
{
    public SimplePostgresqlDatabase(NpgsqlDataSource dataSource) : base(new DefaultMigrationLogger(), JasperFx.AutoCreate.CreateOrUpdate, new PostgresqlMigrator(), "Simple", dataSource)
    {
    }

    public override IFeatureSchema[] BuildFeatureSchemas()
    {
        return Array.Empty<IFeatureSchema>();
    }
}
