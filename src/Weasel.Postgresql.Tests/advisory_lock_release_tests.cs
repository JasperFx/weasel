using JasperFx.Core;
using Npgsql;
using Shouldly;
using Xunit;

namespace Weasel.Postgresql.Tests;

/// <summary>
///     weasel#676. Covers the release behaviour of <see cref="AdvisoryLock.DisposeAsync" />, which is
///     concurrent and bounded by <see cref="AdvisoryLockOptions.ReleaseTimeout" /> rather than a
///     sequential unbounded loop.
/// </summary>
/// <remarks>
///     The bound itself -- that a dispose cannot exceed its release timeout no matter what Medallion's
///     connection monitor is doing -- is not covered here, because making a real release hang on demand
///     would mean reaching inside that monitor. It was measured against a daemon host instead
///     (marten#5567). What is pinned here is everything that has to stay true either side of the bound:
///     the locks really are released in the configuration that used to stall, and the opt-out convention
///     is honoured rather than read as "give up immediately".
///     <para>
///     Only the negative-budget case of the theory fails without the opt-out guard, and it is worth
///     knowing why, because the other two look like they should and do not. A zero or infinite budget is
///     indistinguishable here either way: the releases finish in milliseconds on a healthy connection, so
///     by the time the assertions run the locks are off the session whether <c>DisposeAsync</c> waited for
///     them or abandoned them. A negative budget is different in kind -- <c>Task.Delay</c> throws
///     <see cref="ArgumentOutOfRangeException" /> on anything negative except
///     <see cref="Timeout.InfiniteTimeSpan" /> -- so that case fails outright without the guard, which is
///     what makes it the one with teeth.
///     </para>
///     <para>
///     <see cref="advisory_lock_behavior.disposal_releases_every_held_lock" /> already covers the ordinary
///     unmonitored release. These tests hold the locks the way the stall needed them held: session-scoped,
///     multiplexed and monitored.
///     </para>
/// </remarks>
public class advisory_lock_release_tests
{
    private const int LockCount = 8;

    [Theory]
    [InlineData(5)] // the ordinary bounded case
    [InlineData(0)] // opts out of the bound -- must still release, not give up immediately
    [InlineData(-1)] // Timeout.InfiniteTimeSpan, same opt-out
    [InlineData(-5)] // a negative budget: Task.Delay would throw on this if it reached it
    public async Task dispose_releases_every_held_lock(int releaseTimeoutSeconds)
    {
        var releaseTimeout = releaseTimeoutSeconds switch
        {
            -1 => Timeout.InfiniteTimeSpan,
            _ => releaseTimeoutSeconds.Seconds()
        };

        await using var source = NpgsqlDataSource.Create(ConnectionSource.ConnectionString);

        // The combination that used to stall: session-scoped, multiplexed, monitored.
        var theLock = AdvisoryLockTesting.CreateLock(source, false, true, releaseTimeout);

        var lockIds = new int[LockCount];
        for (var i = 0; i < LockCount; i++)
        {
            lockIds[i] = AdvisoryLockTesting.NextLockId();
            (await theLock.TryAttainLockAsync(lockIds[i], CancellationToken.None)).ShouldBeTrue();
        }

        // Reading HasLock is what activates Medallion's connection monitor, which is the whole reason
        // the release path needed changing -- so the release has to be exercised with monitoring live,
        // not just with the locks held.
        foreach (var lockId in lockIds)
        {
            theLock.HasLock(lockId).ShouldBeTrue();
        }

        await theLock.DisposeAsync();

        // With an opt-out timeout this is true the moment DisposeAsync returns; with a bound it is true
        // once the releases finish, which for a healthy connection is well inside the bound.
        foreach (var lockId in lockIds)
        {
            (await AdvisoryLockTesting.IsFreeAsync(lockId))
                .ShouldBeTrue($"lock {lockId} was not released by DisposeAsync");
        }
    }

    [Fact]
    public async Task dispose_with_nothing_held_is_a_no_op()
    {
        await using var source = NpgsqlDataSource.Create(ConnectionSource.ConnectionString);
        var theLock = AdvisoryLockTesting.CreateLock(source, false, true);

        // Must not fault, and must not wait out the release timeout for zero handles
        await theLock.DisposeAsync();
    }
}
