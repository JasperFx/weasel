using System;
using System.Diagnostics;
using Microsoft.Data.SqlClient;
using Shouldly;
using Weasel.Core.Migrations;
using Xunit;

namespace Weasel.SqlServer.Tests;

/// <summary>
///     weasel#766. <c>sp_getapplock</c> is told how long to wait through <c>@LockTimeout</c>, but the
///     command carrying it ran under the connection's <c>CommandTimeout</c> -- 30 seconds unless the
///     connection string says otherwise. So the client gave up first: a wait of 30 seconds or more
///     silently had no effect, and at exactly 30 the caller got a raw
///     <see cref="SqlException" /> ("Execution Timeout Expired") rather than a failed
///     <see cref="AttainLockResult" />, which is the one thing
///     <see cref="ResourceMigrationFailureMode.ContinueOnFailures" /> cannot rule on.
/// </summary>
public class applock_wait_outlasts_the_command_timeout
{
    /// <summary>
    ///     The wait belongs to the server; the command timeout only has to outlast it. Five seconds of
    ///     margin, never narrowing a command timeout the caller set deliberately, and "wait forever"
    ///     maps to an infinite command timeout because no finite one can outlast it.
    /// </summary>
    [Theory]
    [InlineData(30, 1000, 30)]      // the default already outlasts a short wait
    [InlineData(30, 45_000, 50)]    // 45s wait: raised past it, with margin
    [InlineData(30, 30_000, 35)]    // the boundary the bug report hit
    [InlineData(120, 1000, 120)]    // a deliberately long command timeout is kept
    [InlineData(30, 0, 30)]         // do not wait at all
    [InlineData(30, -1, 0)]         // wait forever -> infinite
    [InlineData(0, 45_000, 0)]      // already infinite, and nothing narrows that
    public void command_timeout_for_a_lock_wait(int current, int lockTimeoutMs, int expected)
    {
        SharedLockExtensions.CommandTimeoutForLockWait(current, lockTimeoutMs).ShouldBe(expected);
    }

    /// <summary>
    ///     The live half, and the half a unit test cannot reach: the requested wait has to survive a
    ///     command timeout shorter than itself. Two seconds of command timeout against a six second
    ///     wait on a held lock -- before the fix this threw at two seconds.
    /// </summary>
    [Fact]
    public async Task a_wait_longer_than_the_command_timeout_reports_contention_instead_of_throwing()
    {
        const string lockId = "gh766_wait_outlasts_command_timeout";

        var impatient = new SqlConnectionStringBuilder(ConnectionSource.ConnectionString)
        {
            CommandTimeout = 2
        }.ConnectionString;

        await using var holder = new SqlConnection(ConnectionSource.ConnectionString);
        await using var waiter = new SqlConnection(impatient);

        await holder.OpenAsync();
        await waiter.OpenAsync();

        await holder.GetGlobalLock(lockId);

        try
        {
            var stopwatch = Stopwatch.StartNew();
            var result = await waiter.TryAttainGlobalLock(lockId, lockTimeoutMs: 6000);
            stopwatch.Stop();

            result.Succeeded.ShouldBeFalse();

            // The server answered, which is the whole point -- and it answered when it was asked
            // to, not when the 2 second command timeout would have aborted the round trip.
            stopwatch.Elapsed.ShouldBeGreaterThan(TimeSpan.FromSeconds(4));
        }
        finally
        {
            await holder.ReleaseGlobalLock(lockId);
        }
    }
}
