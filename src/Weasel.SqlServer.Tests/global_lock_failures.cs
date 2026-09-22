using System;
using Microsoft.Data.SqlClient;
using Shouldly;
using Weasel.Core.Migrations;
using Xunit;

namespace Weasel.SqlServer.Tests;

/// <summary>
///     weasel#599. <c>sp_getapplock</c>'s return codes are documented and specific, and the old
///     failure -- a bare <see cref="Exception" /> reading
///     <c>sp_getapplock failed with errorCode '-1'</c> -- carried neither the meaning nor a type
///     anything could catch. It was also inconsistent with the PostgreSQL side, where lock
///     contention surfaces as an <see cref="InvalidOperationException" /> and is ruled on by
///     <see cref="ResourceMigrationFailureMode" />.
/// </summary>
public class global_lock_failures
{
    [Fact]
    public void minus_one_is_contention_and_says_so()
    {
        var ex = SharedLockExtensions.GlobalLockFailure("migrations", -1, 2500);

        ex.Message.ShouldContain("Another session holds the application lock 'migrations'");
        ex.Message.ShouldContain("2500 ms");
        ex.Message.ShouldContain("ContinueOnFailures");
        ex.ReturnCode.ShouldBe(-1);
        ex.Resource.ShouldBe("migrations");
    }

    [Fact]
    public void minus_two_is_cancellation()
    {
        SharedLockExtensions.GlobalLockFailure("migrations", -2, 1000)
            .Message.ShouldContain("cancelled");
    }

    [Fact]
    public void minus_three_is_a_deadlock_victim()
    {
        SharedLockExtensions.GlobalLockFailure("migrations", -3, 1000)
            .Message.ShouldContain("deadlock victim");
    }

    [Fact]
    public void minus_999_is_a_malformed_request()
    {
        SharedLockExtensions.GlobalLockFailure("migrations", -999, 1000)
            .Message.ShouldContain("255 characters");
    }

    /// <summary>
    ///     The type has to stay catchable as <see cref="InvalidOperationException" />: that is what
    ///     the PostgreSQL path in <c>DatabaseBase</c> has always thrown for the same situation, so
    ///     code that already catches it keeps working and gains something to narrow to.
    /// </summary>
    [Fact]
    public void is_an_invalid_operation_exception()
    {
        SharedLockExtensions.GlobalLockFailure("migrations", -1, 1000)
            .ShouldBeAssignableTo<InvalidOperationException>();
    }

    [Fact]
    public async Task the_blocking_call_throws_the_typed_exception_on_contention()
    {
        await using var conn1 = new SqlConnection(ConnectionSource.ConnectionString);
        await using var conn2 = new SqlConnection(ConnectionSource.ConnectionString);

        await conn1.OpenAsync();
        await conn2.OpenAsync();

        await conn1.GetGlobalLock("gh599_blocking");

        try
        {
            // Pre-fix this was `Exception: sp_getapplock failed with errorCode '-1'`.
            var ex = await Should.ThrowAsync<GlobalLockUnavailableException>(
                () => conn2.GetGlobalLock("gh599_blocking", lockTimeoutMs: 100));

            ex.ReturnCode.ShouldBe(-1);
            ex.Resource.ShouldBe("gh599_blocking");
            ex.Message.ShouldContain("100 ms");
        }
        finally
        {
            await conn1.ReleaseGlobalLock("gh599_blocking");
        }
    }

    /// <summary>
    ///     The <see cref="IGlobalLock{TConnection}" /> seam, which is what routes SQL Server
    ///     contention through <see cref="ResourceMigrationFailureMode" /> the way PostgreSQL's
    ///     already is: contention is a result, not an exception.
    /// </summary>
    [Fact]
    public async Task the_global_lock_seam_reports_contention_as_a_failed_result()
    {
        await using var conn1 = new SqlConnection(ConnectionSource.ConnectionString);
        await using var conn2 = new SqlConnection(ConnectionSource.ConnectionString);

        await conn1.OpenAsync();
        await conn2.OpenAsync();

        var theLock = new SqlServerGlobalLock("gh599_seam", 100);

        (await theLock.TryAttainLock(conn1)).ShouldBe(AttainLockResult.Success);

        try
        {
            var second = await theLock.TryAttainLock(conn2);
            second.Succeeded.ShouldBeFalse();
            second.ShouldReconnect.ShouldBeFalse();
        }
        finally
        {
            await theLock.ReleaseLock(conn1);
        }

        (await theLock.TryAttainLock(conn2)).ShouldBe(AttainLockResult.Success);
        await theLock.ReleaseLock(conn2);
    }

    /// <summary>
    ///     A timeout that can be set is the point of the change; the default stays 1000ms so
    ///     nothing that did not ask for a different one changes.
    /// </summary>
    [Fact]
    public void the_default_timeout_is_unchanged()
    {
        SharedLockExtensions.DefaultLockTimeoutMilliseconds.ShouldBe(1000);
    }
}
