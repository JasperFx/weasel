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

    public ValueTask InitializeAsync()
    {
        _database = NpgsqlDataSource.Create(ConnectionSource.ConnectionString);
        theLock = new AdvisoryLock(_database, NullLogger.Instance, "localhost", new AdvisoryLockOptions());
        return ValueTask.CompletedTask;
    }

    public async ValueTask DisposeAsync()
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

// weasel#396: TryAttainLockAsync checked _disposed only at method entry and then stored the winning
// handle unconditionally. An acquire in flight when DisposeAsync drained _handles put its handle into
// the drained dictionary, where nothing would ever dispose it — a granted advisory lock held until the
// process exits. With transaction-scoped locks (Marten's default) that leaves a backend permanently
// 'idle in transaction' on pg_try_advisory_xact_lock, which Marten's high-water gap detection reads as
// a live pre-gap reserver and never advances past (marten#5090).
public class advisory_lock_dispose_race
{
    // Distinct from every other lock id used in this file so parallel test classes cannot interfere.
    private const int TheLockId = 3960001;

    [Fact]
    public async Task an_acquire_that_completes_after_disposal_does_not_strand_its_lock()
    {
        await using var source = NpgsqlDataSource.Create(ConnectionSource.ConnectionString);

        // Start the acquire and dispose while it is still in flight — the acquire needs a database
        // round trip, disposing an empty handle set does not, so the store reliably lands after the
        // drain. Repeat so a single lucky interleaving cannot pass this by accident.
        for (var i = 0; i < 20; i++)
        {
            var racedLock = new AdvisoryLock(source, NullLogger.Instance, "localhost",
                new AdvisoryLockOptions { TransactionalLockEnabled = true });

            var attain = racedLock.TryAttainLockAsync(TheLockId, CancellationToken.None);
            await racedLock.DisposeAsync();

            // Whatever it reports, it must not leave the lock held: either it stored the handle before
            // the drain (and the drain disposed it) or it saw the disposal and disposed it itself.
            await attain;
        }

        // The proof: a completely separate lock must be able to take the same key. Before the fix the
        // stranded transaction-scoped handles still held it, so this came back false.
        await using var otherSource = NpgsqlDataSource.Create(ConnectionSource.ConnectionString);
        var successor = new AdvisoryLock(otherSource, NullLogger.Instance, "localhost",
            new AdvisoryLockOptions { TransactionalLockEnabled = true });

        try
        {
            (await successor.TryAttainLockAsync(TheLockId, CancellationToken.None))
                .ShouldBeTrue("a disposed AdvisoryLock stranded the handle it acquired mid-disposal");
        }
        finally
        {
            await successor.DisposeAsync();
        }
    }

    [Fact]
    public async Task disposal_releases_a_lock_attained_before_it()
    {
        await using var source = NpgsqlDataSource.Create(ConnectionSource.ConnectionString);

        var theLock = new AdvisoryLock(source, NullLogger.Instance, "localhost",
            new AdvisoryLockOptions { TransactionalLockEnabled = true });

        (await theLock.TryAttainLockAsync(TheLockId + 1, CancellationToken.None)).ShouldBeTrue();
        theLock.HasLock(TheLockId + 1).ShouldBeTrue();

        await theLock.DisposeAsync();

        // The ordinary drain path still works — and a disposed lock reports no lock.
        theLock.HasLock(TheLockId + 1).ShouldBeFalse();

        await using var otherSource = NpgsqlDataSource.Create(ConnectionSource.ConnectionString);
        var successor = new AdvisoryLock(otherSource, NullLogger.Instance, "localhost",
            new AdvisoryLockOptions { TransactionalLockEnabled = true });

        try
        {
            (await successor.TryAttainLockAsync(TheLockId + 1, CancellationToken.None)).ShouldBeTrue();
        }
        finally
        {
            await successor.DisposeAsync();
        }
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
