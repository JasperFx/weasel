using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using Shouldly;
using Xunit;

namespace Weasel.Postgresql.Tests;

/// <summary>
///     <c>IAdvisoryLock</c> could acquire a distribution lock but not say who holds it.
/// </summary>
/// <remarks>
///     <para>
///         <c>HasLock(lockId)</c> answers only "does <em>this</em> node", so no node — and no monitoring
///         tool — could learn which node owns a lock set it does not hold. That is the diagnostic an
///         operator needs when a projection agent stops with
///         <c>ProgressionProgressOutOfOrderException</c>, which means two processes believe they own the
///         same shard. Under Wolverine-managed distribution the assignment table answers "who owns
///         this"; under Marten HotCold the lock <em>is</em> the authority and nothing exposed its holder
///         (weasel#650).
///     </para>
///     <para>
///         The key decomposition these tests pin was measured against the server, not assumed:
///         <c>pg_locks</c> reports the advisory key as the two 32-bit halves of a 64-bit key, so a
///         negative lock id sign-extends into <c>classid = 0xFFFFFFFF</c>. A read matching
///         <c>objid = lockId</c> alone finds nothing for a negative id — and reports it as unheld.
///     </para>
/// </remarks>
[Collection("advisory_holder")]
public class finding_the_advisory_lock_holder : IAsyncLifetime
{
    private NpgsqlDataSource _source = null!;

    private static CancellationToken ct => TestContext.Current.CancellationToken;

    public ValueTask InitializeAsync()
    {
        _source = NpgsqlDataSource.Create(ConnectionSource.ConnectionString);
        return ValueTask.CompletedTask;
    }

    public async ValueTask DisposeAsync()
    {
        await _source.DisposeAsync();
    }

    private AdvisoryLock lockFor(bool transactional = false)
    {
        return new AdvisoryLock(_source, NullLogger.Instance, "holder-tests",
            new AdvisoryLockOptions { TransactionalLockEnabled = transactional });
    }

    [Fact]
    public async Task an_unheld_lock_has_no_holder()
    {
        await using var advisory = lockFor();

        (await advisory.FindHolderAsync(918_271, ct)).ShouldBeNull();
    }

    [Fact]
    public async Task a_held_lock_reports_its_holder()
    {
        await using var advisory = lockFor();
        (await advisory.TryAttainLockAsync(918_272, ct)).ShouldBeTrue();

        var holder = await advisory.FindHolderAsync(918_272, ct);

        holder.ShouldNotBeNull();
        holder.LockId.ShouldBe(918_272);
        holder.SessionId.ShouldNotBeNull("the backend pid should be reported");
        holder.HeldSince.ShouldNotBeNull();
    }

    /// <summary>
    ///     The measured trap. A negative id sign-extends into both halves of the key, so this fails
    ///     against a read that matches the id alone.
    /// </summary>
    [Fact]
    public async Task a_negative_lock_id_is_found_too()
    {
        await using var advisory = lockFor();
        (await advisory.TryAttainLockAsync(-918_273, ct)).ShouldBeTrue();

        var holder = await advisory.FindHolderAsync(-918_273, ct);

        holder.ShouldNotBeNull("a negative lock id was reported as unheld");
        holder.LockId.ShouldBe(-918_273);
    }

    /// <summary>Both lock modes put the same row in pg_locks; both have to be readable.</summary>
    [Fact]
    public async Task a_transactional_lock_reports_its_holder_too()
    {
        await using var advisory = lockFor(transactional: true);
        (await advisory.TryAttainLockAsync(918_274, ct)).ShouldBeTrue();

        (await advisory.FindHolderAsync(918_274, ct)).ShouldNotBeNull();
    }

    [Fact]
    public async Task the_holder_is_this_node_when_this_node_holds_it()
    {
        await using var advisory = lockFor();
        (await advisory.TryAttainLockAsync(918_275, ct)).ShouldBeTrue();

        (await advisory.FindHolderAsync(918_275, ct))!.IsCurrentNode.ShouldBe(true);
    }

    /// <summary>
    ///     The question the feature exists to answer: a node that does not hold the lock can still learn
    ///     who does, and is told it is not itself.
    /// </summary>
    [Fact]
    public async Task another_node_can_see_who_holds_the_lock()
    {
        await using var holderNode = lockFor();
        await using var otherNode = lockFor();

        (await holderNode.TryAttainLockAsync(918_276, ct)).ShouldBeTrue();
        (await otherNode.TryAttainLockAsync(918_276, ct)).ShouldBeFalse("the lock should be contended");

        var seen = await otherNode.FindHolderAsync(918_276, ct);

        seen.ShouldNotBeNull();
        seen.SessionId.ShouldBe((await holderNode.FindHolderAsync(918_276, ct))!.SessionId);
        seen.IsCurrentNode.ShouldBe(false);
    }

    [Fact]
    public async Task a_released_lock_has_no_holder_again()
    {
        await using var advisory = lockFor();
        (await advisory.TryAttainLockAsync(918_277, ct)).ShouldBeTrue();
        (await advisory.FindHolderAsync(918_277, ct)).ShouldNotBeNull();

        await advisory.ReleaseLockAsync(918_277);

        (await advisory.FindHolderAsync(918_277, ct)).ShouldBeNull();
    }

    /// <summary>
    ///     weasel#719. The lock is per database, the view is per server.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         An advisory lock is taken on the acquiring session's database, so two databases on one
    ///         server hold the same id independently and neither blocks the other -- measured, not
    ///         assumed: <c>pg_try_advisory_lock</c> for an id the neighbour holds returns true.
    ///         <c>pg_locks</c>, though, is server-wide and shows every database's rows, so the unscoped
    ///         read named the neighbour's backend as this database's holder.
    ///     </para>
    ///     <para>
    ///         That is the worst direction for this read to be wrong in: it reports a lock as held, with
    ///         a pid and an application name to make it convincing, when a local acquire would in fact
    ///         succeed. Database-per-tenant deployments and parallel test lanes share a server and reuse
    ///         lock ids, so they hit it by construction -- in weasel#716's lanes a test helper of this
    ///         shape went on to kill the other lane's connections.
    ///     </para>
    /// </remarks>
    [Fact]
    public async Task a_lock_held_in_another_database_is_not_this_database_s_holder()
    {
        await using var neighbour = await NeighbouringDatabase.StartAsync(ct);

        await using var theirs = neighbour.AdvisoryLock();
        (await theirs.TryAttainLockAsync(918_278, ct)).ShouldBeTrue();
        (await theirs.FindHolderAsync(918_278, ct)).ShouldNotBeNull("the neighbour does hold it");

        await using var ours = lockFor();

        (await ours.FindHolderAsync(918_278, ct))
            .ShouldBeNull("a holder in another database was reported as this database's");
    }

    /// <summary>
    ///     The other half: both databases really can hold the id at once, and each reads back its own
    ///     backend rather than whichever row <c>pg_locks</c> returned first.
    /// </summary>
    [Fact]
    public async Task each_database_reports_its_own_holder_for_the_same_lock_id()
    {
        await using var neighbour = await NeighbouringDatabase.StartAsync(ct);

        await using var theirs = neighbour.AdvisoryLock();
        await using var ours = lockFor();

        (await theirs.TryAttainLockAsync(918_279, ct)).ShouldBeTrue();
        (await ours.TryAttainLockAsync(918_279, ct))
            .ShouldBeTrue("the id is held per database, so this acquire should not be contended");

        var theirHolder = await theirs.FindHolderAsync(918_279, ct);
        var ourHolder = await ours.FindHolderAsync(918_279, ct);

        theirHolder.ShouldNotBeNull();
        ourHolder.ShouldNotBeNull();
        ourHolder.SessionId.ShouldNotBe(theirHolder.SessionId);
        ourHolder.IsCurrentNode.ShouldBe(true);
    }

    /// <summary>
    ///     A throwaway database on the same server as the test database, with a data source onto it.
    /// </summary>
    private sealed class NeighbouringDatabase : IAsyncDisposable
    {
        private readonly NpgsqlDataSource _source;
        private readonly string _name;

        private NeighbouringDatabase(string name, NpgsqlDataSource source)
        {
            _name = name;
            _source = source;
        }

        public static async Task<NeighbouringDatabase> StartAsync(CancellationToken token)
        {
            var name = $"weasel_holder_scope_{Guid.NewGuid():N}";
            var builder = new NpgsqlConnectionStringBuilder(ConnectionSource.ConnectionString)
            {
                Database = name
            };

            await using (var conn = new NpgsqlConnection(builder.ConnectionString))
            {
                await new PostgresqlMigrator().EnsureDatabaseExistsAsync(conn, token);
            }

            return new NeighbouringDatabase(name, NpgsqlDataSource.Create(builder.ConnectionString));
        }

        public AdvisoryLock AdvisoryLock()
        {
            return new AdvisoryLock(_source, NullLogger.Instance, _name, new AdvisoryLockOptions());
        }

        public async ValueTask DisposeAsync()
        {
            await _source.DisposeAsync();

            var adminBuilder = new NpgsqlConnectionStringBuilder(ConnectionSource.ConnectionString)
            {
                Database = "postgres"
            };

            await using var admin = new NpgsqlConnection(adminBuilder.ConnectionString);
            await admin.OpenAsync();
            await admin.KillIdleSessions(_name);
            await admin.DropDatabase(_name);
        }
    }
}
