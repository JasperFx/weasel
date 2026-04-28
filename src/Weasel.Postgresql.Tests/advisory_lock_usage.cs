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

public class NativeAdvisoryLockSpecs: IAsyncLifetime
{
    private NpgsqlDataSource _database = null!;
    private NativeAdvisoryLock theLock = null!;

    public Task InitializeAsync()
    {
        _database = NpgsqlDataSource.Create(ConnectionSource.ConnectionString);
        theLock = new NativeAdvisoryLock(_database, NullLogger.Instance, "localhost");
        return Task.CompletedTask;
    }

    public async Task DisposeAsync()
    {
        await theLock.DisposeAsync();
        await _database.DisposeAsync();
    }

    [Fact]
    public async Task acquire_and_release_a_session_lock()
    {
        await using var conn2 = new NpgsqlConnection(ConnectionSource.ConnectionString);
        await conn2.OpenAsync();

        theLock.HasLock(20).ShouldBeFalse();

        (await theLock.TryAttainLockAsync(20, CancellationToken.None)).ShouldBeTrue();
        theLock.HasLock(20).ShouldBeTrue();

        // Cannot get the lock from another connection
        (await conn2.TryGetGlobalLock(20)).Succeeded.ShouldBeFalse();

        await theLock.ReleaseLockAsync(20);
        theLock.HasLock(20).ShouldBeFalse();

        for (var j = 0; j < 5; j++)
        {
            if ((await conn2.TryGetGlobalLock(20)).Succeeded) return;

            await Task.Delay(250);
        }

        throw new Exception("Advisory lock was not released");
    }

    [Fact]
    public async Task hold_multiple_locks_and_release_one()
    {
        (await theLock.TryAttainLockAsync(30, CancellationToken.None)).ShouldBeTrue();
        (await theLock.TryAttainLockAsync(31, CancellationToken.None)).ShouldBeTrue();
        (await theLock.TryAttainLockAsync(32, CancellationToken.None)).ShouldBeTrue();

        await theLock.ReleaseLockAsync(31);

        theLock.HasLock(30).ShouldBeTrue();
        theLock.HasLock(31).ShouldBeFalse();
        theLock.HasLock(32).ShouldBeTrue();
    }

    [Fact]
    public async Task acquire_returns_false_when_held_by_another_instance()
    {
        await using var second = NpgsqlDataSource.Create(ConnectionSource.ConnectionString);
        await using var lock2 = new NativeAdvisoryLock(second, NullLogger.Instance, "localhost");

        (await theLock.TryAttainLockAsync(40, CancellationToken.None)).ShouldBeTrue();
        (await lock2.TryAttainLockAsync(40, CancellationToken.None)).ShouldBeFalse();
    }

    [Fact]
    public async Task dispose_releases_held_locks()
    {
        await using (var owner = new NativeAdvisoryLock(_database, NullLogger.Instance, "localhost"))
        {
            (await owner.TryAttainLockAsync(60, CancellationToken.None)).ShouldBeTrue();
            (await owner.TryAttainLockAsync(61, CancellationToken.None)).ShouldBeTrue();
        }

        await using var second = new NativeAdvisoryLock(_database, NullLogger.Instance, "localhost");

        for (var j = 0; j < 5; j++)
        {
            if (await second.TryAttainLockAsync(60, CancellationToken.None)
                && await second.TryAttainLockAsync(61, CancellationToken.None))
            {
                return;
            }

            await Task.Delay(250);
        }

        throw new Exception("Advisory locks were not released by dispose");
    }

    [Fact]
    public async Task try_get_global_lock_does_not_emit_set_local_warning()
    {
        var notices = new List<string>();
        await using var conn = new NpgsqlConnection(ConnectionSource.ConnectionString);
        conn.Notice += (_, args) => notices.Add(args.Notice.MessageText);
        await conn.OpenAsync();

        await conn.TryGetGlobalLock(70);
        await conn.ReleaseGlobalLock(70);

        notices.ShouldNotContain(n => n.Contains("SET LOCAL", StringComparison.OrdinalIgnoreCase));
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
