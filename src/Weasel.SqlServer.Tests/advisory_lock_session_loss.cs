using System.Diagnostics;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging.Abstractions;
using Shouldly;
using Xunit;

namespace Weasel.SqlServer.Tests;

// The Polecat HotCold split brain: a node went on reporting locks that SQL Server released with its session
public class advisory_lock_session_loss
{
    private static CancellationToken ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task a_node_holding_every_lock_notices_that_its_session_ended()
    {
        await using var theLock = new AdvisoryLock(() => new SqlConnection(ConnectionSource.ConnectionString),
            NullLogger.Instance, "Testing");
        await using var otherNode = Locks.Create();
        var lockIds = new[] { Locks.NextId(), Locks.NextId() };

        foreach (var lockId in lockIds)
        {
            (await theLock.TryAttainLockAsync(lockId, ct)).ShouldBeTrue();
        }

        await Locks.KillSessionHoldingAsync(lockIds[0]);

        foreach (var lockId in lockIds)
        {
            await Locks.WaitForAsync(() => !theLock.HasLock(lockId), $"HasLock({lockId}) outlived its session",
                TimeSpan.FromSeconds(20));
            (await otherNode.TryAttainLockAsync(lockId, ct)).ShouldBeTrue();
        }
    }

    [Fact]
    public async Task polling_for_a_lock_another_node_holds_does_not_wait()
    {
        await using var theLock = Locks.Create();
        await using var otherNode = Locks.Create();
        var lockId = Locks.NextId();

        (await otherNode.TryAttainLockAsync(lockId, ct)).ShouldBeTrue();

        var stopwatch = Stopwatch.StartNew();
        (await theLock.TryAttainLockAsync(lockId, ct)).ShouldBeFalse();

        stopwatch.Elapsed.ShouldBeLessThan(TimeSpan.FromMilliseconds(500));
    }

    [Fact]
    public async Task attaining_a_lock_it_already_holds_does_not_stack_the_hold()
    {
        await using var theLock = Locks.Create();
        var lockId = Locks.NextId();

        (await theLock.TryAttainLockAsync(lockId, ct)).ShouldBeTrue();
        (await theLock.TryAttainLockAsync(lockId, ct)).ShouldBeTrue();

        await theLock.ReleaseLockAsync(lockId);

        (await Locks.IsFreeAsync(lockId)).ShouldBeTrue();
    }

    [Fact]
    public async Task a_node_that_reconnects_does_not_report_locks_that_ended_with_its_old_session()
    {
        await using var theLock = Locks.Create();
        await using var otherNode = Locks.Create();
        var held = Locks.NextId();
        var polled = Locks.NextId();

        (await theLock.TryAttainLockAsync(held, ct)).ShouldBeTrue();

        await Locks.KillSessionHoldingAsync(held);

        // The coordinator's poll for a lock it lacks; the first attempt runs into the dead connection
        for (var attempt = 0; attempt < 3 && !theLock.HasLock(polled); attempt++)
        {
            try
            {
                await theLock.TryAttainLockAsync(polled, ct);
            }
            catch (SqlException)
            {
            }
        }

        theLock.HasLock(polled).ShouldBeTrue();
        (await otherNode.TryAttainLockAsync(held, ct)).ShouldBeTrue();
        theLock.HasLock(held).ShouldBeFalse();
    }

    [Fact]
    public async Task verifying_drops_a_lock_its_session_no_longer_holds()
    {
        SqlConnection? connection = null;
        await using var theLock = new AdvisoryLock(() => connection = new SqlConnection(ConnectionSource.ConnectionString),
            NullLogger.Instance, "Testing", new SqlServerAdvisoryLockOptions { MonitoringInterval = TimeSpan.FromHours(1) });
        var released = Locks.NextId();
        var kept = Locks.NextId();

        (await theLock.TryAttainLockAsync(released, ct)).ShouldBeTrue();
        (await theLock.TryAttainLockAsync(kept, ct)).ShouldBeTrue();

        // Stands in for a release that ran on the server after the client had given up waiting for it
        await connection!.ReleaseGlobalLock(released.ToString(), ct);

        await theLock.VerifyHeldLocksAsync();

        theLock.HasLock(released).ShouldBeFalse();
        theLock.HasLock(kept).ShouldBeTrue();
    }

    [Fact]
    public async Task monitoring_keeps_the_locks_of_a_live_session()
    {
        await using var theLock = Locks.Create(monitoringInterval: TimeSpan.FromMilliseconds(100));
        var lockId = Locks.NextId();

        (await theLock.TryAttainLockAsync(lockId, ct)).ShouldBeTrue();

        await Task.Delay(TimeSpan.FromSeconds(1), ct);

        theLock.HasLock(lockId).ShouldBeTrue();
        (await Locks.IsFreeAsync(lockId)).ShouldBeFalse();
    }

    [Fact]
    public async Task disposal_while_monitoring_releases_every_lock()
    {
        for (var i = 0; i < 10; i++)
        {
            var theLock = Locks.Create(monitoringInterval: TimeSpan.FromMilliseconds(1));
            var lockIds = new[] { Locks.NextId(), Locks.NextId() };

            foreach (var lockId in lockIds)
            {
                (await theLock.TryAttainLockAsync(lockId, ct)).ShouldBeTrue();
            }

            await Task.Delay(i, ct);
            await theLock.DisposeAsync();

            foreach (var lockId in lockIds)
            {
                (await Locks.IsFreeAsync(lockId)).ShouldBeTrue();
            }
        }
    }

    private static class Locks
    {
        // A pooled connection could be the session that still holds the lock, and would answer for it
        private static readonly string Unpooled =
            new SqlConnectionStringBuilder(ConnectionSource.ConnectionString) { Pooling = false }.ConnectionString;

        private static int _lastId = 7_400_000;

        public static int NextId() => Interlocked.Increment(ref _lastId);

        // Drives the public options overload rather than a test-only seam, so the suite exercises what a
        // consumer gets. The default interval is long enough that only the tests asking for a monitor get one.
        public static AdvisoryLock Create(TimeSpan? monitoringInterval = null) =>
            new(() => new SqlConnection(ConnectionSource.ConnectionString), NullLogger.Instance, "Testing",
                new SqlServerAdvisoryLockOptions
                {
                    MonitoringInterval = monitoringInterval ?? TimeSpan.FromHours(1)
                });

        public static async Task<bool> IsFreeAsync(int lockId)
        {
            await using var conn = new SqlConnection(Unpooled);
            await conn.OpenAsync();
            return await conn.TryGetGlobalLock(lockId.ToString());
        }

        public static async Task KillSessionHoldingAsync(int lockId)
        {
            await using var conn = new SqlConnection(Unpooled);
            await conn.OpenAsync();

            await using var find = conn.CreateCommand();
            find.CommandText = """
                               SELECT request_session_id FROM sys.dm_tran_locks
                               WHERE resource_type = 'APPLICATION'
                                 AND resource_database_id = DB_ID()
                                 AND request_status = 'GRANT'
                                 AND CHARINDEX(':[' + @resource + ']:', resource_description) > 0
                               """;
            find.With("resource", lockId.ToString());
            var session = (int)(await find.ExecuteScalarAsync())!;

            await using var kill = conn.CreateCommand();
            kill.CommandText = $"KILL {session}";
            await kill.ExecuteNonQueryAsync();

            await WaitForAsync(() => IsFreeAsync(lockId).GetAwaiter().GetResult(), "the killed session kept its lock");
        }

        public static async Task WaitForAsync(Func<bool> condition, string failure, TimeSpan? timeout = null)
        {
            var deadline = DateTime.UtcNow + (timeout ?? TimeSpan.FromSeconds(10));
            while (!condition())
            {
                if (DateTime.UtcNow > deadline) throw new TimeoutException(failure);
                await Task.Delay(50);
            }
        }
    }
}
