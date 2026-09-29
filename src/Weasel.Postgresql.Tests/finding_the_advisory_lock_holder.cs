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
}
