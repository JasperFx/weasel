using System.Diagnostics;
using Npgsql;
using Shouldly;
using Xunit;

namespace Weasel.Postgresql.Tests;

// weasel#908: AdvisoryLock takes its advisory locks itself now, instead of through DistributedLock.Postgres. These
// cover what that library's own test suite used to cover for us: holders contending, a release handing the lock
// over, a dropped connection releasing its locks, and the monitor never standing in the way of a lock or unlock.
public class advisory_lock_contention
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task two_holders_contending_for_one_lock_never_both_win(bool transactional)
    {
        await using var source = NpgsqlDataSource.Create(ConnectionSource.ConnectionString);
        await using var otherSource = NpgsqlDataSource.Create(ConnectionSource.ConnectionString);
        await using var first = AdvisoryLockTesting.CreateLock(source, transactional);
        await using var second = AdvisoryLockTesting.CreateLock(otherSource, transactional);

        for (var i = 0; i < 20; i++)
        {
            var lockId = AdvisoryLockTesting.NextLockId();

            var results = await Task.WhenAll(
                first.TryAttainLockAsync(lockId, CancellationToken.None),
                second.TryAttainLockAsync(lockId, CancellationToken.None));

            results.Count(x => x).ShouldBe(1, $"round {i}: exactly one holder should win lock {lockId}");
            first.HasLock(lockId).ShouldBe(results[0]);
            second.HasLock(lockId).ShouldBe(results[1]);
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task releasing_one_of_several_held_locks_hands_just_that_one_to_another_node(bool transactional)
    {
        await using var source = NpgsqlDataSource.Create(ConnectionSource.ConnectionString);
        await using var otherSource = NpgsqlDataSource.Create(ConnectionSource.ConnectionString);
        await using var theLock = AdvisoryLockTesting.CreateLock(source, transactional);
        await using var otherNode = AdvisoryLockTesting.CreateLock(otherSource, transactional);
        var lockIds = new[] { AdvisoryLockTesting.NextLockId(), AdvisoryLockTesting.NextLockId(), AdvisoryLockTesting.NextLockId() };

        foreach (var lockId in lockIds)
        {
            (await theLock.TryAttainLockAsync(lockId, CancellationToken.None)).ShouldBeTrue();
            (await otherNode.TryAttainLockAsync(lockId, CancellationToken.None)).ShouldBeFalse();
        }

        await theLock.ReleaseLockAsync(lockIds[1]);

        (await otherNode.TryAttainLockAsync(lockIds[1], CancellationToken.None)).ShouldBeTrue();
        (await otherNode.TryAttainLockAsync(lockIds[0], CancellationToken.None)).ShouldBeFalse();
        (await otherNode.TryAttainLockAsync(lockIds[2], CancellationToken.None)).ShouldBeFalse();

        theLock.HasLock(lockIds[0]).ShouldBeTrue();
        theLock.HasLock(lockIds[1]).ShouldBeFalse();
        theLock.HasLock(lockIds[2]).ShouldBeTrue();
    }

    // Without monitoring nothing watches the connection, so HasLock keeps answering true. What has to hold is that
    // the server let go of the lock with the connection, and that the next acquire is not stuck on the dead one.
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task a_dropped_connection_releases_its_locks_without_monitoring(bool transactional)
    {
        await using var source = NpgsqlDataSource.Create(ConnectionSource.ConnectionString);
        await using var otherSource = NpgsqlDataSource.Create(ConnectionSource.ConnectionString);
        await using var theLock = AdvisoryLockTesting.CreateLock(source, transactional);
        await using var otherNode = AdvisoryLockTesting.CreateLock(otherSource, transactional);
        var lockId = AdvisoryLockTesting.NextLockId();

        (await theLock.TryAttainLockAsync(lockId, CancellationToken.None)).ShouldBeTrue();
        (await otherNode.TryAttainLockAsync(lockId, CancellationToken.None)).ShouldBeFalse();

        await AdvisoryLockTesting.TerminateBackendsHoldingAsync(lockId);

        (await otherNode.TryAttainLockAsync(lockId, CancellationToken.None)).ShouldBeTrue();
        (await theLock.TryAttainLockAsync(lockId, CancellationToken.None)).ShouldBeFalse();

        // A new lock goes on a live connection, not the terminated one
        var next = AdvisoryLockTesting.NextLockId();
        (await theLock.TryAttainLockAsync(next, CancellationToken.None)).ShouldBeTrue();
        (await AdvisoryLockTesting.IsFreeAsync(next)).ShouldBeFalse();

        // And releasing the lost lock is quiet: the server already released it
        await theLock.ReleaseLockAsync(lockId);
        otherNode.HasLock(lockId).ShouldBeTrue();
        (await AdvisoryLockTesting.IsFreeAsync(lockId)).ShouldBeFalse();
    }

    // The monitor parks the connection in a one-minute wait. Every lock and unlock on that connection has to cut the
    // wait short, doing so must not cost the locks already held there (weasel#676 was a release that waited out the
    // monitor), and the monitor has to go back to watching afterwards.
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task the_monitor_never_holds_up_a_lock_or_an_unlock(bool transactional)
    {
        await using var source = NpgsqlDataSource.Create(ConnectionSource.ConnectionString);
        await using var theLock = AdvisoryLockTesting.CreateLock(source, transactional, monitored: true);
        var first = AdvisoryLockTesting.NextLockId();

        (await theLock.TryAttainLockAsync(first, CancellationToken.None)).ShouldBeTrue();
        theLock.HasLock(first).ShouldBeTrue();

        // Let the monitor settle into its wait
        await Task.Delay(250);

        var stopwatch = Stopwatch.StartNew();
        var more = Enumerable.Range(0, 10).Select(_ => AdvisoryLockTesting.NextLockId()).ToArray();
        foreach (var lockId in more)
        {
            (await theLock.TryAttainLockAsync(lockId, CancellationToken.None)).ShouldBeTrue();
            theLock.HasLock(lockId).ShouldBeTrue();
            await Task.Delay(50);
        }

        foreach (var lockId in more)
        {
            await theLock.ReleaseLockAsync(lockId);
            await Task.Delay(50);
        }

        stopwatch.Stop();
        stopwatch.Elapsed.ShouldBeLessThan(TimeSpan.FromSeconds(20), "a lock or unlock waited out the monitor's sleep");

        foreach (var lockId in more)
        {
            (await AdvisoryLockTesting.IsFreeAsync(lockId)).ShouldBeTrue();
        }

        // The lock that was held while the monitor was cut short twenty times over is still held
        theLock.HasLock(first).ShouldBeTrue();
        (await AdvisoryLockTesting.IsFreeAsync(first)).ShouldBeFalse();

        // And the monitor is still watching it
        await AdvisoryLockTesting.TerminateBackendsHoldingAsync(first);
        await AdvisoryLockTesting.WaitForAsync(() => !theLock.HasLock(first),
            "the monitor stopped watching after it was cut short");
    }
}
