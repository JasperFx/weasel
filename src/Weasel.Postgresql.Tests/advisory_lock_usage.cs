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

public class advisory_lock_disposal_guard
{
    // weasel#349: during host shutdown on a HotCold cold/standby node, the projection coordinator's
    // leadership poll can call TryAttainLockAsync while the owned NpgsqlDataSource is being disposed,
    // which used to abort the process with ObjectDisposedException: 'Npgsql.PoolingDataSource'.

    [Fact]
    public async Task returns_false_when_the_data_source_is_disposed_out_from_under_it()
    {
        var dataSource = NpgsqlDataSource.Create(ConnectionSource.ConnectionString);
        var theLock = new AdvisoryLock(dataSource, NullLogger.Instance, "localhost", new AdvisoryLockOptions());

        // Simulate the shutdown ordering where the data source is torn down before/while the lock is used.
        await dataSource.DisposeAsync();

        // Pre-fix this threw ObjectDisposedException: 'Npgsql.PoolingDataSource'.
        var attained = await theLock.TryAttainLockAsync(4242, CancellationToken.None);
        attained.ShouldBeFalse();

        await theLock.DisposeAsync();
    }

    [Fact]
    public async Task latches_after_the_first_swallowed_abort_so_cadence_polling_stops_touching_the_dead_pool()
    {
        var dataSource = NpgsqlDataSource.Create(ConnectionSource.ConnectionString);
        var theLock = new AdvisoryLock(dataSource, NullLogger.Instance, "localhost", new AdvisoryLockOptions());

        await dataSource.DisposeAsync();

        // First poll hits the disposed pool; the ObjectDisposedException is swallowed per weasel#349 ...
        (await theLock.TryAttainLockAsync(4244, CancellationToken.None)).ShouldBeFalse();

        // ... and must also LATCH. The HotCold projection coordinator calls TryAttainLockAsync on its
        // LeadershipPollingTime cadence and reads false as "another node holds the lock", so without the
        // latch every subsequent poll re-opens against a pool that can never come back — churning
        // ObjectDisposedExceptions until process exit, and jasperfx#500's terminate-on-ODE never fires
        // because the ODE it waits for is swallowed here (marten#4915). Count the aborts the same way
        // marten's Bug_4874_coordinator_drain_ordering regression test does.
        var aborts = 0;
        EventHandler<FirstChanceExceptionEventArgs> handler = (_, e) =>
        {
            if (e.Exception is ObjectDisposedException ode &&
                (ode.ObjectName?.Contains("PoolingDataSource") == true ||
                 ode.Message.Contains("PoolingDataSource")))
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
