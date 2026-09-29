using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging.Abstractions;
using Shouldly;
using Xunit;

namespace Weasel.SqlServer.Tests;

/// <summary>
///     The SQL Server half of weasel#650: <c>IAdvisoryLock</c> could acquire a distribution lock but not
///     say who holds it.
/// </summary>
/// <remarks>
///     <c>HasLock(lockId)</c> answers only "does <em>this</em> node", so no node — and no monitoring
///     tool — could learn which node owns a lock set it does not hold. Under Polecat's lock-based
///     distribution the lock <em>is</em> the authority, so nothing could answer the question an operator
///     asks when two processes appear to be running one shard. The
///     <c>resource_description</c> shape these tests rest on (<c>0:[4242]:(1bb08fa7)</c>) was measured
///     against the server.
/// </remarks>
public class finding_the_advisory_lock_holder
{
    private static CancellationToken ct => TestContext.Current.CancellationToken;

    private static AdvisoryLock lockFor()
    {
        return new AdvisoryLock(() => new SqlConnection(ConnectionSource.ConnectionString),
            NullLogger.Instance, "holder-tests");
    }

    [Fact]
    public async Task an_unheld_lock_has_no_holder()
    {
        await using var advisory = lockFor();

        (await advisory.FindHolderAsync(918_371, ct)).ShouldBeNull();
    }

    [Fact]
    public async Task a_held_lock_reports_its_holder()
    {
        await using var advisory = lockFor();
        (await advisory.TryAttainLockAsync(918_372, ct)).ShouldBeTrue();

        var holder = await advisory.FindHolderAsync(918_372, ct);

        holder.ShouldNotBeNull();
        holder.LockId.ShouldBe(918_372);
        holder.SessionId.ShouldNotBeNull("the session id should be reported");
        holder.HeldSince.ShouldNotBeNull();
    }

    [Fact]
    public async Task a_negative_lock_id_is_found_too()
    {
        await using var advisory = lockFor();
        (await advisory.TryAttainLockAsync(-918_373, ct)).ShouldBeTrue();

        (await advisory.FindHolderAsync(-918_373, ct)).ShouldNotBeNull();
    }

    [Fact]
    public async Task the_holder_is_this_node_when_this_node_holds_it()
    {
        await using var advisory = lockFor();
        (await advisory.TryAttainLockAsync(918_374, ct)).ShouldBeTrue();

        (await advisory.FindHolderAsync(918_374, ct))!.IsCurrentNode.ShouldBe(true);
    }

    /// <summary>The question the feature exists to answer.</summary>
    [Fact]
    public async Task another_node_can_see_who_holds_the_lock()
    {
        await using var holderNode = lockFor();
        await using var otherNode = lockFor();

        (await holderNode.TryAttainLockAsync(918_375, ct)).ShouldBeTrue();
        (await otherNode.TryAttainLockAsync(918_375, ct)).ShouldBeFalse("the lock should be contended");

        var seen = await otherNode.FindHolderAsync(918_375, ct);

        seen.ShouldNotBeNull();
        seen.SessionId.ShouldBe((await holderNode.FindHolderAsync(918_375, ct))!.SessionId);
        seen.IsCurrentNode.ShouldBe(false);
    }

    [Fact]
    public async Task a_released_lock_has_no_holder_again()
    {
        await using var advisory = lockFor();
        (await advisory.TryAttainLockAsync(918_376, ct)).ShouldBeTrue();
        (await advisory.FindHolderAsync(918_376, ct)).ShouldNotBeNull();

        await advisory.ReleaseLockAsync(918_376);

        (await advisory.FindHolderAsync(918_376, ct)).ShouldBeNull();
    }

    /// <summary>
    ///     An id that is a prefix of another must not match it: the read matches the bracketed resource
    ///     name, not a substring of the description.
    /// </summary>
    [Fact]
    public async Task a_lock_id_that_prefixes_another_is_not_confused_with_it()
    {
        await using var advisory = lockFor();
        (await advisory.TryAttainLockAsync(918_3771, ct)).ShouldBeTrue();

        (await advisory.FindHolderAsync(918_377, ct)).ShouldBeNull("a prefix of the held id matched it");
    }
}
