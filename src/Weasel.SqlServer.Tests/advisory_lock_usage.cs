using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging.Abstractions;
using Shouldly;
using Xunit;

namespace Weasel.SqlServer.Tests;

public class advisory_lock_usage
{
    [Fact]
    public async Task explicitly_release_global_session_locks()
    {
        await using (var conn1 = new SqlConnection(ConnectionSource.ConnectionString))
        await using (var conn2 = new SqlConnection(ConnectionSource.ConnectionString))
        await using (var conn3 = new SqlConnection(ConnectionSource.ConnectionString))
        {
            await conn1.OpenAsync();
            await conn2.OpenAsync();
            await conn3.OpenAsync();


            await conn1.GetGlobalLock("1");


            // Cannot get the lock here
            (await conn2.TryGetGlobalLock("1")).ShouldBeFalse();


            await conn1.ReleaseGlobalLock("1");


            for (var j = 0; j < 5; j++)
            {
                if (await conn2.TryGetGlobalLock("1")) return;

                await Task.Delay(250);
            }

            throw new Exception("Advisory lock was not released");
        }
    }

    [Fact]
    public async Task explicitly_release_global_tx_session_locks()
    {
        await using (var conn1 = new SqlConnection(ConnectionSource.ConnectionString))
        await using (var conn2 = new SqlConnection(ConnectionSource.ConnectionString))
        await using (var conn3 = new SqlConnection(ConnectionSource.ConnectionString))
        {
            await conn1.OpenAsync();
            await conn2.OpenAsync();
            await conn3.OpenAsync();

            var tx1 = conn1.BeginTransaction();
            await tx1.GetGlobalTxLock("2");


            // Cannot get the lock here
            var tx2 = conn2.BeginTransaction();
            (await tx2.TryGetGlobalTxLock("2")).ShouldBeFalse();


            tx1.Rollback();


            for (var j = 0; j < 5; j++)
            {
                if (await tx2.TryGetGlobalTxLock("2"))
                {
                    tx2.Rollback();
                    return;
                }

                await Task.Delay(250);
            }

            throw new Exception("Advisory lock was not released");
        }
    }

    [Fact] // - too slow
    public async Task global_session_locks()
    {
        await using (var conn1 = new SqlConnection(ConnectionSource.ConnectionString))
        await using (var conn2 = new SqlConnection(ConnectionSource.ConnectionString))
        await using (var conn3 = new SqlConnection(ConnectionSource.ConnectionString))
        {
            await conn1.OpenAsync();
            await conn2.OpenAsync();
            await conn3.OpenAsync();

            await conn1.GetGlobalLock("24");


            try
            {
                // Cannot get the lock here
                (await conn2.TryGetGlobalLock("24")).ShouldBeFalse();

                // Can get the new lock
                (await conn3.TryGetGlobalLock("25")).ShouldBeTrue();

                // Cannot get the lock here
                (await conn2.TryGetGlobalLock("25")).ShouldBeFalse();
            }
            finally
            {
                await conn1.ReleaseGlobalLock("24");
                await conn3.ReleaseGlobalLock("25");
            }
        }
    }

    [Fact] // -- too slow
    public async Task tx_session_locks()
    {
        await using (var conn1 = new SqlConnection(ConnectionSource.ConnectionString))
        await using (var conn2 = new SqlConnection(ConnectionSource.ConnectionString))
        await using (var conn3 = new SqlConnection(ConnectionSource.ConnectionString))
        {
            await conn1.OpenAsync();
            await conn2.OpenAsync();
            await conn3.OpenAsync();

            var tx1 = conn1.BeginTransaction();
            await tx1.GetGlobalTxLock("4");


            // Cannot get the lock here
            var tx2 = conn2.BeginTransaction();
            (await tx2.TryGetGlobalTxLock("4")).ShouldBeFalse();

            // Can get the new lock
            var tx3 = conn3.BeginTransaction();
            (await tx3.TryGetGlobalTxLock("5")).ShouldBeTrue();

            // Cannot get the lock here
            (await tx2.TryGetGlobalTxLock("5")).ShouldBeFalse();

            tx1.Rollback();
            tx2.Rollback();
            tx3.Rollback();
        }
    }
}

public class AdvisoryLockSpecs : IAsyncLifetime
{
    private AdvisoryLock theLock;

    public Task InitializeAsync()
    {

        theLock = new AdvisoryLock(() => new SqlConnection(ConnectionSource.ConnectionString), NullLogger.Instance, "Testing");
        return Task.CompletedTask;
    }

    public async Task DisposeAsync()
    {
        await theLock.DisposeAsync();
    }


    [Fact]
    public async Task explicitly_release_global_session_locks()
    {
        await using var conn2 = new SqlConnection(ConnectionSource.ConnectionString);
        await using var conn3 = new SqlConnection(ConnectionSource.ConnectionString);

        await conn2.OpenAsync();
        await conn3.OpenAsync();

        await theLock.TryAttainLockAsync(10, CancellationToken.None);

        // Cannot get the lock here
        (await conn2.TryGetGlobalLock(10.ToString())).ShouldBeFalse();

        await theLock.ReleaseLockAsync(10);

        for (var j = 0; j < 5; j++)
        {
            if ((await conn2.TryGetGlobalLock(10.ToString()))) return;

            await Task.Delay(250);
        }

        throw new Exception("Advisory lock was not released");
    }

}

public class advisory_lock_disposal_guard
{
    // weasel#349: mirror of the Postgres guard for Polecat parity — opening the lock connection during host
    // shutdown must never let a process-aborting disposed-connection exception escape TryAttainLockAsync.

    [Fact]
    public async Task returns_false_when_opening_the_connection_throws_a_disposed_exception()
    {
        // The connection factory is disposed out from under an in-flight acquire during shutdown.
        var theLock = new AdvisoryLock(
            () => throw new ObjectDisposedException("SqlConnection"), NullLogger.Instance, "Testing");

        // Pre-fix this let the ObjectDisposedException escape and abort the process.
        var attained = await theLock.TryAttainLockAsync(4242, CancellationToken.None);
        attained.ShouldBeFalse();

        await theLock.DisposeAsync();
    }

    [Fact]
    public async Task returns_false_once_the_lock_itself_has_begun_disposing()
    {
        var theLock = new AdvisoryLock(
            () => new SqlConnection(ConnectionSource.ConnectionString), NullLogger.Instance, "Testing");

        await theLock.DisposeAsync();

        // Never start a new acquire (never open a connection) after disposal has begun.
        var attained = await theLock.TryAttainLockAsync(4243, CancellationToken.None);
        attained.ShouldBeFalse();
    }
}
