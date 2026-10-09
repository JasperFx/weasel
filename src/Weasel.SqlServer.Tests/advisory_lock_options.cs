using JasperFx.Core;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Shouldly;
using Xunit;

namespace Weasel.SqlServer.Tests;

// weasel#700: the monitor weasel#695 added was a hard-coded 5 second timer with no seam at all
public class advisory_lock_options
{
    private static CancellationToken ct => TestContext.Current.CancellationToken;

    [Fact]
    public void the_defaults_are_what_9_42_shipped()
    {
        var options = new SqlServerAdvisoryLockOptions();

        options.MonitoringInterval.ShouldBe(5.Seconds());
        options.ProbeTimeout.ShouldBe(5.Seconds());
    }

    // weasel#616 discipline: the three-argument constructor is what Polecat's ProjectionCoordinator is
    // compiled against, so the options overload has to be purely additive. Both shapes are pinned here
    // rather than left to review, and the pre-change one must never be "tidied" into the new one.
    [Fact]
    public void both_public_constructor_signatures_exist()
    {
        typeof(AdvisoryLock)
            .GetConstructor([typeof(Func<SqlConnection>), typeof(ILogger), typeof(string)])
            .ShouldNotBeNull();

        typeof(AdvisoryLock)
            .GetConstructor([
                typeof(Func<SqlConnection>), typeof(ILogger), typeof(string), typeof(SqlServerAdvisoryLockOptions)
            ])
            .ShouldNotBeNull();
    }

    [Theory]
    [InlineData(1)]
    [InlineData(100)]
    public void a_positive_interval_starts_the_monitor(int milliseconds)
    {
        AdvisoryLock.MonitoringEnabled(TimeSpan.FromMilliseconds(milliseconds)).ShouldBeTrue();
    }

    [Fact]
    public void a_non_positive_or_infinite_interval_opts_out()
    {
        AdvisoryLock.MonitoringEnabled(TimeSpan.Zero).ShouldBeFalse();
        AdvisoryLock.MonitoringEnabled(TimeSpan.FromSeconds(-1)).ShouldBeFalse();
        AdvisoryLock.MonitoringEnabled(Timeout.InfiniteTimeSpan).ShouldBeFalse();
    }

    // CommandTimeout reads 0 as "wait forever", so rounding a sub-second value down would remove the bound
    // from the probe that holds _gate -- the one outcome this conversion must never produce
    [Theory]
    [InlineData(5000, 5)]
    [InlineData(1000, 1)]
    [InlineData(1500, 2)]
    [InlineData(1, 1)]
    [InlineData(0, 1)]
    [InlineData(-1, 1)]
    public void the_probe_timeout_never_converts_to_an_unbounded_command(int milliseconds, int expected)
    {
        AdvisoryLock.ProbeTimeoutSeconds(TimeSpan.FromMilliseconds(milliseconds)).ShouldBe(expected);
    }

    [Fact]
    public void the_probe_timeout_conversion_does_not_overflow()
    {
        AdvisoryLock.ProbeTimeoutSeconds(TimeSpan.MaxValue).ShouldBe(int.MaxValue);
        AdvisoryLock.ProbeTimeoutSeconds(TimeSpan.MinValue).ShouldBe(1);
    }

    [Fact]
    public async Task a_monitored_lock_drops_one_the_session_no_longer_holds()
    {
        SqlConnection? connection = null;
        await using var theLock = new AdvisoryLock(
            () => connection = new SqlConnection(ConnectionSource.ConnectionString), NullLogger.Instance, "Testing",
            new SqlServerAdvisoryLockOptions { MonitoringInterval = TimeSpan.FromMilliseconds(100) });

        var lockId = NextId();
        (await theLock.TryAttainLockAsync(lockId, ct)).ShouldBeTrue();

        // Stands in for a release that ran on the server after the client had given up waiting for it
        await connection!.ReleaseGlobalLock(lockId.ToString(), ct);

        await waitForAsync(() => !theLock.HasLock(lockId), $"the monitor never dropped {lockId}");
    }

    [Fact]
    public async Task opting_out_of_monitoring_leaves_the_lock_reported_as_held()
    {
        SqlConnection? connection = null;
        await using var theLock = new AdvisoryLock(
            () => connection = new SqlConnection(ConnectionSource.ConnectionString), NullLogger.Instance, "Testing",
            new SqlServerAdvisoryLockOptions { MonitoringInterval = Timeout.InfiniteTimeSpan });

        var lockId = NextId();
        (await theLock.TryAttainLockAsync(lockId, ct)).ShouldBeTrue();

        await connection!.ReleaseGlobalLock(lockId.ToString(), ct);

        // Ten times the interval the monitored test above needs. Nothing is watching, so this is the
        // weasel#695 split brain deliberately restored -- which is why the opt-out is documented as such.
        await Task.Delay(TimeSpan.FromSeconds(1), ct);

        theLock.HasLock(lockId).ShouldBeTrue();
    }

    // Why there is no ReleaseTimeout to match the Postgres sibling's. That bound is safe there because
    // abandoning a release still ends with the connection closing and the session-scoped lock going with it.
    // Closing a POOLED connection does not end its session, so the lock survives the close -- a release that
    // a deadline skipped would strand it against the node that needs it next.
    [Fact]
    public async Task a_pooled_connection_keeps_its_locks_when_it_closes()
    {
        var lockId = NextId();

        await using (var pooled = new SqlConnection(ConnectionSource.ConnectionString))
        {
            await pooled.OpenAsync(ct);
            (await pooled.TryGetGlobalLock(lockId.ToString(), cancellation: ct)).ShouldBeTrue();
            await pooled.CloseAsync();
        }

        (await isFreeAsync(lockId)).ShouldBeFalse();
    }

    [Fact]
    public async Task an_unpooled_connection_drops_its_locks_when_it_closes()
    {
        var lockId = NextId();

        await using (var unpooled = new SqlConnection(Unpooled))
        {
            await unpooled.OpenAsync(ct);
            (await unpooled.TryGetGlobalLock(lockId.ToString(), cancellation: ct)).ShouldBeTrue();
            await unpooled.CloseAsync();
        }

        (await isFreeAsync(lockId)).ShouldBeTrue();
    }

    private static int _lastId = 7_700_000;

    private static int NextId() => Interlocked.Increment(ref _lastId);

    // A pooled connection could be the session that still holds the lock, and would answer for it
    private static readonly string Unpooled =
        new SqlConnectionStringBuilder(ConnectionSource.ConnectionString) { Pooling = false }.ConnectionString;

    private static async Task<bool> isFreeAsync(int lockId)
    {
        await using var conn = new SqlConnection(Unpooled);
        await conn.OpenAsync();
        return await conn.TryGetGlobalLock(lockId.ToString());
    }

    private static async Task waitForAsync(Func<bool> condition, string failure)
    {
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(10);
        while (!condition())
        {
            if (DateTime.UtcNow > deadline) throw new TimeoutException(failure);
            await Task.Delay(50);
        }
    }
}
