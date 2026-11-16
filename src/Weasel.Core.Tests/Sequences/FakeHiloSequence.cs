using Weasel.Core.Sequences;

namespace Weasel.Core.Tests.Sequences;

/// <summary>
///     Test double that supplies the "database I/O" the abstract base leaves
///     open. Each "advance to next hi" just bumps an in-memory hi counter,
///     matching what a real dialect does after a successful fetch.
/// </summary>
/// <remarks>
///     <para>
///     The hi counter is atomic, as the database's is: since weasel#833 a synchronous and an
///     asynchronous advance may overlap by design, and each must get a block of its own.
///     </para>
///     <para>
///     Two advances on the <i>same</i> path must still never overlap, so the fake counts it when
///     they do (<see cref="SamePathOverlaps" />), with a spin inside the advance to widen the window.
///     </para>
/// </remarks>
internal sealed class FakeHiloSequence: HiloSequenceBase
{
    private long _nextHi;
    private int _advanceCount;
    private int _asyncAdvanceCount;
    private int _syncAdvanceCount;
    private int _asyncInFlight;
    private int _syncInFlight;
    private int _samePathOverlaps;

    public FakeHiloSequence(string entityName, IReadOnlyHiloSettings settings)
        : base(entityName, settings)
    {
    }

    /// <summary>Every advance, from either path or from <see cref="SetFloor" />.</summary>
    public int AdvanceCount => Volatile.Read(ref _advanceCount);

    /// <summary>Advances made through the asynchronous <see cref="AdvanceToNextHi" />.</summary>
    public int AsyncAdvanceCount => Volatile.Read(ref _asyncAdvanceCount);

    /// <summary>Advances made through the synchronous <see cref="AdvanceToNextHiSync" />.</summary>
    public int SyncAdvanceCount => Volatile.Read(ref _syncAdvanceCount);

    /// <summary>
    ///     How often an advance started while another on the same path was still running. The base
    ///     class must keep this at zero.
    /// </summary>
    public int SamePathOverlaps => Volatile.Read(ref _samePathOverlaps);

    /// <summary>
    ///     When set, an advance does its I/O and then installs nothing, as a broken subclass would.
    /// </summary>
    public bool InstallNothing { get; set; }

    /// <summary>
    ///     Stands in for the asynchronous database round trip. It is awaited with
    ///     <c>ConfigureAwait(true)</c>, as an override written without thought for
    ///     synchronization contexts would be.
    /// </summary>
    public Func<CancellationToken, Task>? AsyncIo { get; set; }

    /// <summary>Stands in for the synchronous database round trip.</summary>
    public Action? SyncIo { get; set; }

    /// <summary>
    ///     When set, the asynchronous advance reserves its block <i>before</i> awaiting
    ///     <see cref="AsyncIo" /> and installs it after, as a real round trip does: the database
    ///     hands out the hi when the statement runs, and the install happens on the way back. So a
    ///     synchronous advance during the await reserves a <i>higher</i> block and installs it first.
    /// </summary>
    public bool ReserveBeforeAsyncIo { get; set; }

    // Expose the protected hi-setter so a test can drive the "database
    // returned a negative value, could not secure the next hi" branch.
    public bool TrySetCurrentHiForTest(object? raw) => TrySetCurrentHi(raw);

    public override async Task AdvanceToNextHi(CancellationToken ct = default)
    {
        enter(ref _asyncInFlight);
        try
        {
            long? reserved = ReserveBeforeAsyncIo ? reserve() : null;

            if (AsyncIo != null)
            {
                await AsyncIo(ct).ConfigureAwait(true);
            }

            Interlocked.Increment(ref _asyncAdvanceCount);
            if (reserved.HasValue)
            {
                install(reserved.Value);
            }
            else
            {
                DoAdvance();
            }
        }
        finally
        {
            Interlocked.Decrement(ref _asyncInFlight);
        }
    }

    protected override void AdvanceToNextHiSync()
    {
        enter(ref _syncInFlight);
        try
        {
            SyncIo?.Invoke();
            Interlocked.Increment(ref _syncAdvanceCount);
            DoAdvance();
        }
        finally
        {
            Interlocked.Decrement(ref _syncInFlight);
        }
    }

    private void enter(ref int inFlight)
    {
        if (Interlocked.Increment(ref inFlight) > 1)
        {
            Interlocked.Increment(ref _samePathOverlaps);
        }
    }

    /// <summary>
    ///     Moves the "database" counter without installing anything, as a reset or truncated hilo
    ///     row would: the next advance reserves <paramref name="nextHi" />, lower or not.
    /// </summary>
    public void ResetDatabaseTo(long nextHi) => Interlocked.Exchange(ref _nextHi, nextHi);

    public override Task SetFloor(long floor)
    {
        // Mirror the dialect contract: ensure the next issued id clears the floor.
        var pages = (long)Math.Ceiling((double)floor / MaxLo);
        Interlocked.Exchange(ref _nextHi, pages);
        DoAdvance();
        return Task.CompletedTask;
    }

    private void DoAdvance()
    {
        install(reserve());
    }

    private long reserve()
    {
        Interlocked.Increment(ref _advanceCount);

        // Reserve the block the way the database does: atomically, one per advance.
        return Interlocked.Increment(ref _nextHi) - 1;
    }

    private void install(long hi)
    {
        // Widen the window an advance is in flight for, so two on one path would overlap.
        Thread.SpinWait(50);

        if (!InstallNothing)
        {
            TrySetCurrentHi(hi);
        }
    }
}
