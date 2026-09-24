using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using Shouldly;
using Xunit;

namespace Weasel.Postgresql.Tests;

// Every scenario runs in both lock modes: session locks multiplexed onto shared connections (the default)
// and transaction-scoped locks, each on a dedicated connection.
public class advisory_lock_behavior
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task attains_a_free_lock_and_excludes_other_sessions(bool transactional)
    {
        await using var source = NpgsqlDataSource.Create(ConnectionSource.ConnectionString);
        await using var theLock = AdvisoryLockTesting.CreateLock(source, transactional);
        var lockId = AdvisoryLockTesting.NextLockId();

        theLock.HasLock(lockId).ShouldBeFalse();

        (await theLock.TryAttainLockAsync(lockId, CancellationToken.None)).ShouldBeTrue();

        theLock.HasLock(lockId).ShouldBeTrue();
        (await AdvisoryLockTesting.IsFreeAsync(lockId)).ShouldBeFalse();
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task does_not_attain_a_lock_held_by_another_node(bool transactional)
    {
        await using var source = NpgsqlDataSource.Create(ConnectionSource.ConnectionString);
        await using var otherSource = NpgsqlDataSource.Create(ConnectionSource.ConnectionString);
        await using var theLock = AdvisoryLockTesting.CreateLock(source, transactional);
        await using var otherNode = AdvisoryLockTesting.CreateLock(otherSource, transactional);
        var lockId = AdvisoryLockTesting.NextLockId();

        (await otherNode.TryAttainLockAsync(lockId, CancellationToken.None)).ShouldBeTrue();

        (await theLock.TryAttainLockAsync(lockId, CancellationToken.None)).ShouldBeFalse();
        theLock.HasLock(lockId).ShouldBeFalse();

        await otherNode.ReleaseLockAsync(lockId);

        (await theLock.TryAttainLockAsync(lockId, CancellationToken.None)).ShouldBeTrue();
    }

    // Advisory session locks are reentrant within a Postgres session, so two nodes that multiplex onto the
    // same shared connection would both "win" unless the library moves the second one to another connection.
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task nodes_sharing_a_data_source_still_exclude_each_other(bool transactional)
    {
        await using var source = NpgsqlDataSource.Create(ConnectionSource.ConnectionString);
        await using var first = AdvisoryLockTesting.CreateLock(source, transactional);
        await using var second = AdvisoryLockTesting.CreateLock(source, transactional);
        var lockId = AdvisoryLockTesting.NextLockId();

        (await first.TryAttainLockAsync(lockId, CancellationToken.None)).ShouldBeTrue();
        (await second.TryAttainLockAsync(lockId, CancellationToken.None)).ShouldBeFalse();

        await first.ReleaseLockAsync(lockId);

        (await second.TryAttainLockAsync(lockId, CancellationToken.None)).ShouldBeTrue();
        (await first.TryAttainLockAsync(lockId, CancellationToken.None)).ShouldBeFalse();
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task releasing_one_lock_keeps_the_others_held(bool transactional)
    {
        await using var source = NpgsqlDataSource.Create(ConnectionSource.ConnectionString);
        await using var theLock = AdvisoryLockTesting.CreateLock(source, transactional);
        var first = AdvisoryLockTesting.NextLockId();
        var second = AdvisoryLockTesting.NextLockId();
        var third = AdvisoryLockTesting.NextLockId();

        foreach (var lockId in new[] { first, second, third })
        {
            (await theLock.TryAttainLockAsync(lockId, CancellationToken.None)).ShouldBeTrue();
        }

        await theLock.ReleaseLockAsync(second);

        theLock.HasLock(second).ShouldBeFalse();
        (await AdvisoryLockTesting.IsFreeAsync(second)).ShouldBeTrue();

        theLock.HasLock(first).ShouldBeTrue();
        theLock.HasLock(third).ShouldBeTrue();
        (await AdvisoryLockTesting.IsFreeAsync(first)).ShouldBeFalse();
        (await AdvisoryLockTesting.IsFreeAsync(third)).ShouldBeFalse();
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task attaining_a_lock_it_already_holds_keeps_the_original_hold(bool transactional)
    {
        await using var source = NpgsqlDataSource.Create(ConnectionSource.ConnectionString);
        await using var theLock = AdvisoryLockTesting.CreateLock(source, transactional);
        var lockId = AdvisoryLockTesting.NextLockId();

        (await theLock.TryAttainLockAsync(lockId, CancellationToken.None)).ShouldBeTrue();

        // The second attempt runs on another session, which cannot get past the first hold
        (await theLock.TryAttainLockAsync(lockId, CancellationToken.None)).ShouldBeFalse();

        theLock.HasLock(lockId).ShouldBeTrue();
        (await AdvisoryLockTesting.IsFreeAsync(lockId)).ShouldBeFalse();
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task releasing_a_lock_that_is_not_held_is_a_no_op(bool transactional)
    {
        await using var source = NpgsqlDataSource.Create(ConnectionSource.ConnectionString);
        await using var theLock = AdvisoryLockTesting.CreateLock(source, transactional);
        var held = AdvisoryLockTesting.NextLockId();

        (await theLock.TryAttainLockAsync(held, CancellationToken.None)).ShouldBeTrue();

        await theLock.ReleaseLockAsync(AdvisoryLockTesting.NextLockId());

        theLock.HasLock(held).ShouldBeTrue();
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task disposal_releases_every_held_lock(bool transactional)
    {
        await using var source = NpgsqlDataSource.Create(ConnectionSource.ConnectionString);
        var theLock = AdvisoryLockTesting.CreateLock(source, transactional);
        var lockIds = new[] { AdvisoryLockTesting.NextLockId(), AdvisoryLockTesting.NextLockId(), AdvisoryLockTesting.NextLockId() };

        foreach (var lockId in lockIds)
        {
            (await theLock.TryAttainLockAsync(lockId, CancellationToken.None)).ShouldBeTrue();
        }

        await theLock.DisposeAsync();

        foreach (var lockId in lockIds)
        {
            theLock.HasLock(lockId).ShouldBeFalse();
            (await AdvisoryLockTesting.IsFreeAsync(lockId)).ShouldBeTrue();
        }
    }

    [Theory]
    [InlineData(false, 1)]
    [InlineData(true, 3)]
    public async Task held_locks_share_a_connection_only_when_multiplexing(bool transactional, int expectedBackends)
    {
        await using var source = NpgsqlDataSource.Create(ConnectionSource.ConnectionString);
        await using var theLock = AdvisoryLockTesting.CreateLock(source, transactional);
        var lockIds = new[] { AdvisoryLockTesting.NextLockId(), AdvisoryLockTesting.NextLockId(), AdvisoryLockTesting.NextLockId() };

        foreach (var lockId in lockIds)
        {
            (await theLock.TryAttainLockAsync(lockId, CancellationToken.None)).ShouldBeTrue();
        }

        (await AdvisoryLockTesting.BackendsHoldingAsync(lockIds)).Length.ShouldBe(expectedBackends);
    }

    // AdvisoryLock resolves a multi-host data source to its primary on every lock id, so multiplexing only
    // works if that resolution hands back the same data source each time.
    [Fact]
    public async Task multiplexes_across_lock_ids_on_a_multi_host_data_source()
    {
        var builder = new NpgsqlConnectionStringBuilder(ConnectionSource.ConnectionString);
        builder.Host = $"{builder.Host},{builder.Host}";
        await using var source = new NpgsqlDataSourceBuilder(builder.ConnectionString).BuildMultiHost();
        await using var theLock = AdvisoryLockTesting.CreateLock(source, transactional: false);
        var lockIds = new[] { AdvisoryLockTesting.NextLockId(), AdvisoryLockTesting.NextLockId(), AdvisoryLockTesting.NextLockId() };

        foreach (var lockId in lockIds)
        {
            (await theLock.TryAttainLockAsync(lockId, CancellationToken.None)).ShouldBeTrue();
        }

        (await AdvisoryLockTesting.BackendsHoldingAsync(lockIds)).Length.ShouldBe(1);
    }
}

// Losing the connection that holds a lock releases it server side. With monitoring on, HasLock has to notice,
// and the lock has to be attainable again through the same AdvisoryLock.
public class advisory_lock_connection_loss
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task monitoring_reports_every_lock_lost_with_the_connection(bool transactional)
    {
        await using var source = NpgsqlDataSource.Create(ConnectionSource.ConnectionString);
        await using var theLock = AdvisoryLockTesting.CreateLock(source, transactional, monitored: true);
        var lockIds = new[] { AdvisoryLockTesting.NextLockId(), AdvisoryLockTesting.NextLockId() };

        foreach (var lockId in lockIds)
        {
            (await theLock.TryAttainLockAsync(lockId, CancellationToken.None)).ShouldBeTrue();
            theLock.HasLock(lockId).ShouldBeTrue();
        }

        await AdvisoryLockTesting.TerminateBackendsHoldingAsync(lockIds);

        foreach (var lockId in lockIds)
        {
            await AdvisoryLockTesting.WaitForAsync(() => !theLock.HasLock(lockId),
                $"HasLock({lockId}) still reports a lock whose connection was terminated");
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task reattains_a_lock_after_losing_its_connection(bool transactional)
    {
        await using var source = NpgsqlDataSource.Create(ConnectionSource.ConnectionString);
        await using var theLock = AdvisoryLockTesting.CreateLock(source, transactional, monitored: true);
        var lockId = AdvisoryLockTesting.NextLockId();

        (await theLock.TryAttainLockAsync(lockId, CancellationToken.None)).ShouldBeTrue();
        theLock.HasLock(lockId).ShouldBeTrue();

        await AdvisoryLockTesting.TerminateBackendsHoldingAsync(lockId);
        await AdvisoryLockTesting.WaitForAsync(() => !theLock.HasLock(lockId),
            "the terminated connection was never reported lost");

        (await theLock.TryAttainLockAsync(lockId, CancellationToken.None)).ShouldBeTrue();

        theLock.HasLock(lockId).ShouldBeTrue();
        (await AdvisoryLockTesting.IsFreeAsync(lockId)).ShouldBeFalse();

        await theLock.ReleaseLockAsync(lockId);
        (await AdvisoryLockTesting.IsFreeAsync(lockId)).ShouldBeTrue();
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task another_node_can_take_a_lock_lost_with_its_connection(bool transactional)
    {
        await using var source = NpgsqlDataSource.Create(ConnectionSource.ConnectionString);
        await using var otherSource = NpgsqlDataSource.Create(ConnectionSource.ConnectionString);
        await using var theLock = AdvisoryLockTesting.CreateLock(source, transactional, monitored: true);
        await using var otherNode = AdvisoryLockTesting.CreateLock(otherSource, transactional, monitored: true);
        var lockId = AdvisoryLockTesting.NextLockId();

        (await theLock.TryAttainLockAsync(lockId, CancellationToken.None)).ShouldBeTrue();
        theLock.HasLock(lockId).ShouldBeTrue();

        await AdvisoryLockTesting.TerminateBackendsHoldingAsync(lockId);
        await AdvisoryLockTesting.WaitForAsync(() => !theLock.HasLock(lockId),
            "the terminated connection was never reported lost");

        (await otherNode.TryAttainLockAsync(lockId, CancellationToken.None)).ShouldBeTrue();
        (await theLock.TryAttainLockAsync(lockId, CancellationToken.None)).ShouldBeFalse();
    }
}

internal static class AdvisoryLockTesting
{
    // Distinct from the fixed ids used elsewhere, and unique per call so parallel tests never share a lock
    private static int _lastLockId = 7_100_000;

    public static int NextLockId() => Interlocked.Increment(ref _lastLockId);

    public static AdvisoryLock CreateLock(NpgsqlDataSource source, bool transactional, bool monitored = false)
    {
        return new AdvisoryLock(source, NullLogger.Instance, "localhost",
            new AdvisoryLockOptions { TransactionalLockEnabled = transactional, LockMonitoringEnabled = monitored });
    }

    /// <summary>
    ///     Whether a separate session could take the lock right now. The probe releases whatever it takes.
    /// </summary>
    public static async Task<bool> IsFreeAsync(int lockId)
    {
        await using var conn = new NpgsqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();

        if (!(await conn.TryGetGlobalLock(lockId)).Succeeded) return false;

        // Pooled connections keep session locks after Close, so release explicitly
        await conn.ReleaseGlobalLock(lockId);
        return true;
    }

    /// <summary>
    ///     The distinct backend pids holding any of the given advisory locks. A bigint key is stored as
    ///     classid (high 32 bits) and objid (low 32 bits) with objsubid = 1.
    /// </summary>
    public static async Task<int[]> BackendsHoldingAsync(params int[] lockIds)
    {
        await using var conn = new NpgsqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();

        await using var cmd = new NpgsqlCommand(
            "select distinct pid from pg_locks where locktype = 'advisory' and granted and objsubid = 1 and classid = 0 and objid::bigint = any(@ids)",
            conn);
        cmd.Parameters.AddWithValue("ids", lockIds.Select(x => (long)x).ToArray());

        var pids = new List<int>();
        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            pids.Add(reader.GetInt32(0));
        }

        return pids.ToArray();
    }

    public static async Task TerminateBackendsHoldingAsync(params int[] lockIds)
    {
        var pids = await BackendsHoldingAsync(lockIds);
        pids.ShouldNotBeEmpty();

        await using var conn = new NpgsqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();

        await using var cmd = new NpgsqlCommand("select pg_terminate_backend(pid) from unnest(@pids) as pid", conn);
        cmd.Parameters.AddWithValue("pids", pids);
        await cmd.ExecuteNonQueryAsync();
    }

    public static async Task WaitForAsync(Func<bool> condition, string failureMessage)
    {
        var deadline = DateTime.UtcNow.AddSeconds(10);
        while (!condition())
        {
            if (DateTime.UtcNow > deadline) throw new TimeoutException(failureMessage);
            await Task.Delay(50);
        }
    }
}
