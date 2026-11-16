namespace Weasel.Core.Sequences;

/// <summary>
///     Dialect-agnostic core of a Hi-Lo <see cref="ISequence" />: owns the
///     <see cref="CurrentHi" /> / <see cref="CurrentLo" /> / <see cref="MaxLo" />
///     state and the client-side id arithmetic (<see cref="AdvanceValue" />,
///     <see cref="ShouldAdvanceHi" />, <see cref="NextInt" /> / <see cref="NextLong" />,
///     <see cref="NextIntAsync" /> / <see cref="NextLongAsync" />,
///     <see cref="TrySetCurrentHi" />). Concrete subclasses supply only the
///     database I/O — fetching the next "hi" allocation
///     (<see cref="AdvanceToNextHi" /> / <see cref="AdvanceToNextHiSync" />) and
///     resetting the floor (<see cref="SetFloor" />).
///     <para>
///     Lifted into Weasel.Core in weasel#287. The arithmetic was line-for-line
///     identical between Marten's <c>HiLoSequence</c> (PostgreSQL, via the
///     <c>mt_get_next_hi</c> stored function) and Polecat's <c>HiloSequence</c>
///     (SQL Server, via an optimistic UPDATE on <c>pc_hilo</c>); only the I/O
///     was dialect-forked, which is exactly the seam left abstract here.
///     </para>
/// </summary>
public abstract class HiloSequenceBase: ISequence
{
    // weasel#833: three locks, none of them held by a synchronous caller across asynchronous work.
    //
    // _state guards CurrentHi / CurrentLo: the check-and-issue of an id and the install of a new
    // block. It is held for arithmetic only, never across I/O or an await.
    //
    // _syncAdvance serializes the synchronous advances among themselves, and _asyncAdvance the
    // asynchronous ones. weasel#808 had one SemaphoreSlim for both, held across the await of
    // AdvanceToNextHi, so a synchronous NextLong could block a thread-pool thread on asynchronous
    // work -- Marten's, for one, awaits an in-flight prefetch -- which is how thread-pool starvation
    // starts (marten#5969). Now a synchronous caller that finds an asynchronous advance in flight
    // advances by itself instead of waiting for it. The database hands each advance a block of its
    // own, so the cost is a gap, never a duplicate.
    private readonly Lock _state = new();
    private readonly Lock _syncAdvance = new();
    private readonly SemaphoreSlim _asyncAdvance = new(1, 1);

    // Ordering after an overlap. Each install of a block bumps _installs (under _state), and every
    // advance records the count it started at in _advanceStartedAt, which reaches the subclass's
    // TrySetCurrentHi through the execution context -- across the await, on the asynchronous path.
    // When another advance installed a block in the meantime and this one's block is not above it,
    // installing it would hand out ids behind ones already issued (marten#5984's CI caught 2137
    // followed by 2134), so the block is dropped as a gap instead. Outside an overlap
    // TrySetCurrentHi installs whatever it is given, as it always has, so a sequence whose database
    // row was reset below it keeps working. Holds the count + 1, so 0 means "not in an advance".
    private readonly AsyncLocal<long> _advanceStartedAt = new();
    private long _installs;

    protected HiloSequenceBase(string entityName, IReadOnlyHiloSettings settings)
    {
        EntityName = entityName;
        CurrentHi = -1;
        CurrentLo = 1;
        MaxLo = settings.MaxLo;
        Settings = settings;
    }

    /// <summary>
    ///     The logical entity / document name this sequence allocates ids for.
    ///     Used as the key into the database hilo row.
    /// </summary>
    public string EntityName { get; }

    /// <summary>
    ///     The current "hi" allocation. <c>-1</c> until the first database fetch.
    /// </summary>
    public long CurrentHi { get; protected set; }

    /// <summary>
    ///     The next "lo" offset within the current hi allocation (1-based).
    /// </summary>
    public int CurrentLo { get; protected set; }

    /// <inheritdoc />
    public int MaxLo { get; }

    /// <summary>
    ///     The configuration this sequence was created with, including
    ///     <see cref="IReadOnlyHiloSettings.MaxAdvanceToNextHiAttempts" /> which
    ///     the dialect <see cref="AdvanceToNextHi" /> retry loop should honor.
    /// </summary>
    protected IReadOnlyHiloSettings Settings { get; }

    /// <inheritdoc />
    public int NextInt()
    {
        return (int)NextLong();
    }

    /// <summary>
    ///     The next id as a 64-bit integer. When the current lo allocation is
    ///     exhausted this calls <see cref="AdvanceToNextHiSync" />, blocking the
    ///     calling thread for the database round trip; prefer
    ///     <see cref="NextLongAsync" /> from asynchronous code.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///     Safe to call concurrently with itself and with <see cref="NextLongAsync" />
    ///     on the same instance; no two calls ever return the same id.
    ///     </para>
    ///     <para>
    ///     This never waits on asynchronous work (weasel#833). It blocks only for its own
    ///     <see cref="AdvanceToNextHiSync" /> or another synchronous caller's. If an
    ///     asynchronous caller is part way through an advance, this advances by itself rather
    ///     than wait for it, so both reserve a block. The higher of the two is the one issued
    ///     from: if it is installed first, the lower one is dropped when it arrives, rather than
    ///     issue ids behind ones already handed out. Either way the rest of the other block is a
    ///     gap in the ids, never a duplicate, and the ids one instance issues only increase.
    ///     </para>
    /// </remarks>
    public long NextLong()
    {
        if (tryIssue(out var id))
        {
            return id;
        }

        lock (_syncAdvance)
        {
            while (true)
            {
                // Re-checked after taking the advance lock: another synchronous caller, or an
                // asynchronous one, may have installed a block while this one waited.
                if (tryIssue(out id))
                {
                    return id;
                }

                var before = snapshot();
                _advanceStartedAt.Value = before.Installs + 1;
                try
                {
                    AdvanceToNextHiSync();
                }
                finally
                {
                    _advanceStartedAt.Value = 0;
                }

                assertTheAdvanceInstalledABlock(before, nameof(AdvanceToNextHiSync));
            }
        }
    }

    /// <summary>
    ///     The next id as a 64-bit integer, fetching the next "hi" allocation through
    ///     the asynchronous <see cref="AdvanceToNextHi" /> when the current lo
    ///     allocation is exhausted (weasel#808).
    /// </summary>
    /// <param name="ct">
    ///     Cancels the wait for another asynchronous advance and is passed to
    ///     <see cref="AdvanceToNextHi" />. A token that is already cancelled yields a
    ///     cancelled task without issuing an id. A cancelled call never consumes an id.
    /// </param>
    /// <remarks>
    ///     <para>
    ///     Can be mixed with <see cref="NextLong" /> on one instance without a duplicate id.
    ///     When the current allocation still has room this completes synchronously without
    ///     allocating — the arithmetic path costs one short lock, as <see cref="NextLong" />'s does.
    ///     </para>
    ///     <para>
    ///     Concurrent asynchronous callers share one advance: the others await it rather
    ///     than each reserving a block. Nothing is held across the await that a synchronous
    ///     <see cref="NextLong" /> would wait on (weasel#833); a synchronous caller that needs
    ///     a block meanwhile reserves its own, as described there.
    ///     </para>
    ///     <para>
    ///     <see cref="AdvanceToNextHi" /> is invoked with no
    ///     <see cref="SynchronizationContext" />, so its continuations never queue to the
    ///     caller's context.
    ///     </para>
    /// </remarks>
    public ValueTask<long> NextLongAsync(CancellationToken ct = default)
    {
        if (ct.IsCancellationRequested)
        {
            return ValueTask.FromCanceled<long>(ct);
        }

        // Fast path: the current allocation has room, so the id is pure arithmetic and the
        // call completes synchronously.
        if (tryIssue(out var id))
        {
            return new ValueTask<long>(id);
        }

        return nextLongWithAdvanceAsync(ct);
    }

    private async ValueTask<long> nextLongWithAdvanceAsync(CancellationToken ct)
    {
        // Awaited, never blocked on, and only asynchronous callers ever wait for it.
        await _asyncAdvance.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            while (true)
            {
                // Re-checked after taking the advance gate: another caller may have
                // installed a block between the fast path's check and this acquire.
                if (tryIssue(out var id))
                {
                    return id;
                }

                var before = snapshot();

                // Set inside this async method, so it flows into AdvanceToNextHi and its
                // continuations, and is gone again once this method returns to its caller.
                _advanceStartedAt.Value = before.Installs + 1;
                try
                {
                    await startAdvanceWithoutContext(ct).ConfigureAwait(false);
                }
                finally
                {
                    _advanceStartedAt.Value = 0;
                }

                assertTheAdvanceInstalledABlock(before, nameof(AdvanceToNextHi));
            }
        }
        finally
        {
            _asyncAdvance.Release();
        }
    }

    private bool tryIssue(out long id)
    {
        lock (_state)
        {
            if (ShouldAdvanceHi())
            {
                id = 0;
                return false;
            }

            id = AdvanceValue();
            return true;
        }
    }

    private (long Hi, int Lo, long Installs) snapshot()
    {
        lock (_state)
        {
            return (CurrentHi, CurrentLo, _installs);
        }
    }

    // An advance has to install a block or throw. Once the advance no longer holds a lock that
    // every caller waits on, one that returns having done neither would otherwise be retried
    // forever, a database round trip at a time. A concurrent install from the other path changes
    // the state as well, which correctly counts as progress: the loop then simply checks again.
    // That includes an advance whose own block was dropped because the other path installed a
    // higher one while it was in flight.
    private void assertTheAdvanceInstalledABlock((long Hi, int Lo, long Installs) before, string advance)
    {
        var after = snapshot();
        if (after.Installs == before.Installs && needsAdvance(after.Hi, after.Lo))
        {
            throw new InvalidOperationException(
                $"{GetType().FullName}.{advance} returned without installing a new Hi-Lo block for '{EntityName}'. " +
                $"An advance must either install one through {nameof(TrySetCurrentHi)} or throw.");
        }
    }

    private bool needsAdvance(long hi, int lo) => hi < 0 || lo > MaxLo;

    private Task startAdvanceWithoutContext(CancellationToken ct)
    {
        // An override of AdvanceToNextHi that awaits without ConfigureAwait(false)
        // would capture the caller's context. Clearing it for the synchronous part of
        // the call means there is nothing to capture.
        var context = SynchronizationContext.Current;
        if (context == null)
        {
            return AdvanceToNextHi(ct);
        }

        SynchronizationContext.SetSynchronizationContext(null);
        try
        {
            return AdvanceToNextHi(ct);
        }
        finally
        {
            SynchronizationContext.SetSynchronizationContext(context);
        }
    }

    /// <summary>
    ///     The next id as a 32-bit integer: a narrowing cast of
    ///     <see cref="NextLongAsync" />, as <see cref="NextInt" /> is of
    ///     <see cref="NextLong" />.
    /// </summary>
    /// <param name="ct">Passed through to <see cref="NextLongAsync" />.</param>
    public async ValueTask<int> NextIntAsync(CancellationToken ct = default)
    {
        // An async ValueTask method that completes synchronously does not allocate,
        // so the fast path of NextLongAsync stays allocation-free through here.
        return (int)await NextLongAsync(ct).ConfigureAwait(false);
    }

    /// <summary>
    ///     Compute the next id from the current hi/lo state and advance the lo
    ///     offset. Pure arithmetic — no I/O.
    /// </summary>
    public long AdvanceValue()
    {
        var result = (CurrentHi * MaxLo) + CurrentLo;
        CurrentLo++;

        return result;
    }

    /// <summary>
    ///     True when the current lo allocation is exhausted (or the sequence has
    ///     never fetched a hi), meaning a database round trip is needed before the
    ///     next id can be issued.
    /// </summary>
    public bool ShouldAdvanceHi()
    {
        return CurrentHi < 0 || CurrentLo > MaxLo;
    }

    /// <summary>
    ///     Apply a freshly-fetched "hi" value (the raw scalar from the dialect's
    ///     database call). The database is expected to return a negative value
    ///     when it could not atomically secure the next hi; in that case this
    ///     returns false, leaves the current block as it was, and the caller should
    ///     retry. On success the lo offset is reset to 1.
    /// </summary>
    /// <remarks>
    ///     The new hi and lo are installed together, atomically with respect to the ids
    ///     <see cref="NextLong" /> and <see cref="NextLongAsync" /> issue. A negative value
    ///     no longer overwrites <see cref="CurrentHi" /> (weasel#833): with a synchronous and
    ///     an asynchronous advance able to overlap, one advance's failed attempt would
    ///     otherwise throw away the block the other had just installed.
    ///     <para>
    ///     Ids issued by one instance only increase across an overlap. When another advance
    ///     installed a block while the calling advance was in flight, and <paramref name="raw" />
    ///     is not above it, the block is dropped instead of installed: the rest of it is a gap.
    ///     That still returns <c>true</c>, since there is nothing for the advance to retry.
    ///     Outside an overlap, and outside an advance (from <see cref="SetFloor" />, say), the
    ///     value is installed as given, lower than the current one or not.
    ///     </para>
    /// </remarks>
    protected bool TrySetCurrentHi(object? raw)
    {
        var hi = Convert.ToInt64(raw);
        if (hi < 0)
        {
            return false;
        }

        var startedAt = _advanceStartedAt.Value - 1;

        lock (_state)
        {
            // Another advance installed a block since this one began, and this block is not above
            // it: installing it would issue ids behind ones already handed out.
            if (startedAt >= 0 && _installs > startedAt && CurrentHi >= 0 && hi <= CurrentHi)
            {
                return true;
            }

            CurrentHi = hi;
            CurrentLo = 1;
            _installs++;
        }

        return true;
    }

    /// <summary>
    ///     Fetch the next "hi" allocation from the database asynchronously, looping
    ///     up to <see cref="IReadOnlyHiloSettings.MaxAdvanceToNextHiAttempts" /> and
    ///     throwing <see cref="HiloSequenceAdvanceToNextHiAttemptsExceededException" />
    ///     if it can't be secured. Dialect-specific (PostgreSQL stored function vs
    ///     SQL Server optimistic update).
    ///     <para>
    ///     Invoked from <see cref="NextLongAsync" /> when the current lo allocation is
    ///     exhausted (weasel#808). Never runs concurrently with itself on one instance,
    ///     but since weasel#833 it can overlap an <see cref="AdvanceToNextHiSync" />: each
    ///     must reserve a block of its own from the database and install it through
    ///     <see cref="TrySetCurrentHi" />. It must install a block or throw; it should honor
    ///     <paramref name="ct" />, must not call back into <see cref="NextLongAsync" />, and
    ///     should not rely on a <see cref="SynchronizationContext" />:
    ///     <see cref="NextLongAsync" /> invokes it with none.
    ///     </para>
    /// </summary>
    public abstract Task AdvanceToNextHi(CancellationToken ct = default);

    /// <summary>
    ///     Synchronous counterpart to <see cref="AdvanceToNextHi" />, invoked from
    ///     <see cref="NextLong" /> when the current lo allocation is exhausted. Never runs
    ///     concurrently with itself on one instance, but can overlap an
    ///     <see cref="AdvanceToNextHi" /> (weasel#833); it must not wait for one. It must
    ///     install a block through <see cref="TrySetCurrentHi" /> or throw. Retained for
    ///     the synchronous path only: once every consumer issues ids through
    ///     <see cref="NextLongAsync" /> it can go (weasel#808).
    /// </summary>
    protected abstract void AdvanceToNextHiSync();

    /// <inheritdoc />
    public abstract Task SetFloor(long floor);
}
