using System;
using System.Threading;
using System.Threading.Tasks;
using JasperFx;
using Npgsql;
using Shouldly;
using Weasel.Core;
using Weasel.Core.Migrations;
using Xunit;

namespace Weasel.Postgresql.Tests.Migrations;

/// <summary>
///     weasel#659: <c>ApplyAllConfiguredChangesToDatabaseAsync</c> released the global migration lock
///     only on its success paths. A migration that threw left the lock held, and every other store or
///     replica sharing the database was then locked out of migrating -- told "Unable to attain the
///     global lock in time to apply database changes", whose own advice (<c>ContinueOnFailures</c>)
///     would start them against an unmigrated schema.
/// </summary>
/// <remarks>
///     Found while investigating JasperFx/marten#5529: one test's host migration failed with a DDL
///     error and every later test that migrated against that database failed, which reads as flakiness
///     in a suite and as one node bricking the rest in a deployment.
/// </remarks>
[Collection("migrations")]
public class releasing_the_global_lock: IntegrationContext, IAsyncLifetime
{
    private readonly TestDatabaseWithTables theDatabase;
    private readonly CountingGlobalLock theLock = new(AdvisoryLockTesting.NextLockId());

    public releasing_the_global_lock(): base("migrations")
    {
        theDatabase = new TestDatabaseWithTables(AutoCreate.All, "Migrations", theDataSource);
    }

    public override ValueTask InitializeAsync()
    {
        return new(ResetSchema());
    }

    /// <summary>
    ///     A table whose DDL the server will refuse, so the apply throws from inside
    ///     <c>Migrator.ApplyAllAsync</c> -- after the lock has been attained.
    /// </summary>
    private void configureAMigrationThatWillFail()
    {
        var table = theDatabase.Features["One"].AddTable(SchemaName, "one");
        table.AddColumn("bad", "this_type_does_not_exist");
    }

    [Fact]
    public async Task the_lock_is_released_after_a_successful_apply()
    {
        theDatabase.Features["One"].AddTable(SchemaName, "one");

        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync(theLock);

        theLock.Attained.ShouldBe(1);
        theLock.Released.ShouldBe(1);
    }

    [Fact]
    public async Task the_lock_is_released_after_a_failed_apply()
    {
        configureAMigrationThatWillFail();

        await Should.ThrowAsync<PostgresException>(() =>
            theDatabase.ApplyAllConfiguredChangesToDatabaseAsync(theLock));

        theLock.Attained.ShouldBe(1);
        theLock.Released.ShouldBe(1);
    }

    [Fact]
    public async Task another_node_can_attain_the_lock_after_a_failed_apply()
    {
        // The postcondition the other replicas actually care about, asserted from a separate session.
        // Disposing the connection is not enough on its own: Npgsql resets a pooled connection when it
        // is next used rather than when it is returned, so the session-scoped lock outlives the apply
        // for as long as that connection sits idle in the pool.
        configureAMigrationThatWillFail();

        await Should.ThrowAsync<PostgresException>(() =>
            theDatabase.ApplyAllConfiguredChangesToDatabaseAsync(theLock));

        (await AdvisoryLockTesting.IsFreeAsync(theLock.LockId)).ShouldBeTrue();
    }

    [Fact]
    public async Task a_release_that_fails_does_not_replace_the_migration_failure()
    {
        // The migration failure is the one an operator needs to see. A release that cannot run has
        // nothing to fall back on anyway.
        theLock.ReleaseThrows = true;
        configureAMigrationThatWillFail();

        await Should.ThrowAsync<PostgresException>(() =>
            theDatabase.ApplyAllConfiguredChangesToDatabaseAsync(theLock));
    }
}

/// <summary>
///     A real session-scoped advisory lock on a lock id of its own, counting what the caller does with it.
/// </summary>
internal class CountingGlobalLock: IGlobalLock<NpgsqlConnection>
{
    public CountingGlobalLock(int lockId)
    {
        LockId = lockId;
    }

    public int LockId { get; }
    public int Attained { get; private set; }
    public int Released { get; private set; }
    public bool ReleaseThrows { get; set; }

    public async Task<AttainLockResult> TryAttainLock(NpgsqlConnection conn, CancellationToken ct = default)
    {
        var result = await conn.TryGetGlobalLock(LockId, cancellation: ct).ConfigureAwait(false);
        if (result.Succeeded)
        {
            Attained++;
        }

        return result;
    }

    public Task ReleaseLock(NpgsqlConnection conn, CancellationToken ct = default)
    {
        Released++;
        if (ReleaseThrows)
        {
            throw new InvalidOperationException("the lock could not be released");
        }

        return conn.ReleaseGlobalLock(LockId, cancellation: ct);
    }
}
