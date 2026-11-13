using Weasel.Core.Sequences;

namespace Weasel.Core.Tests.Sequences;

/// <summary>
///     Test double that supplies the "database I/O" the abstract base leaves
///     open. Each "advance to next hi" just bumps an in-memory hi counter,
///     matching what a real dialect does after a successful fetch.
/// </summary>
/// <remarks>
///     The hi counter is deliberately not thread-safe: the base class's gate is the
///     only thing standing between two concurrent advances, so a broken gate shows up
///     as a duplicated or skipped block of ids.
/// </remarks>
internal sealed class FakeHiloSequence: HiloSequenceBase
{
    private long _nextHi;

    public FakeHiloSequence(string entityName, IReadOnlyHiloSettings settings)
        : base(entityName, settings)
    {
    }

    /// <summary>Every advance, from either path or from <see cref="SetFloor" />.</summary>
    public int AdvanceCount { get; private set; }

    /// <summary>Advances made through the asynchronous <see cref="AdvanceToNextHi" />.</summary>
    public int AsyncAdvanceCount { get; private set; }

    /// <summary>Advances made through the synchronous <see cref="AdvanceToNextHiSync" />.</summary>
    public int SyncAdvanceCount { get; private set; }

    /// <summary>
    ///     Stands in for the asynchronous database round trip. It is awaited with
    ///     <c>ConfigureAwait(true)</c>, as an override written without thought for
    ///     synchronization contexts would be.
    /// </summary>
    public Func<CancellationToken, Task>? AsyncIo { get; set; }

    /// <summary>Stands in for the synchronous database round trip.</summary>
    public Action? SyncIo { get; set; }

    // Expose the protected hi-setter so a test can drive the "database
    // returned a negative value, could not secure the next hi" branch.
    public bool TrySetCurrentHiForTest(object? raw) => TrySetCurrentHi(raw);

    public override async Task AdvanceToNextHi(CancellationToken ct = default)
    {
        if (AsyncIo != null)
        {
            await AsyncIo(ct).ConfigureAwait(true);
        }

        AsyncAdvanceCount++;
        DoAdvance();
    }

    protected override void AdvanceToNextHiSync()
    {
        SyncIo?.Invoke();
        SyncAdvanceCount++;
        DoAdvance();
    }

    public override Task SetFloor(long floor)
    {
        // Mirror the dialect contract: ensure the next issued id clears the floor.
        var pages = (long)Math.Ceiling((double)floor / MaxLo);
        _nextHi = pages;
        DoAdvance();
        return Task.CompletedTask;
    }

    private void DoAdvance()
    {
        AdvanceCount++;
        var hi = _nextHi;

        // Widen the window between reading and writing the hi counter, so that two
        // advances which are not serialized by the gate collide.
        Thread.SpinWait(50);

        TrySetCurrentHi(hi);
        _nextHi = hi + 1;
    }
}
