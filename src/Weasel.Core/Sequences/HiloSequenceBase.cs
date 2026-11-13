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
    // weasel#808: one gate for the lo-allocation / hi-advance critical section,
    // shared by NextLong and NextLongAsync so a synchronous and an asynchronous
    // caller can never both advance the hi or hand out the same id. It was a
    // System.Threading.Lock, which cannot be held across an await. Unlike that
    // Lock, a SemaphoreSlim is not re-entrant: an AdvanceToNextHi /
    // AdvanceToNextHiSync override must not call back into NextLong(Async).
    private readonly SemaphoreSlim _gate = new(1, 1);

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
    ///     Safe to call concurrently with itself and with <see cref="NextLongAsync" />
    ///     on the same instance. If an asynchronous caller is part way through an
    ///     advance, this blocks until that advance completes and then issues an id
    ///     from the allocation it fetched.
    /// </remarks>
    public long NextLong()
    {
        _gate.Wait();
        try
        {
            if (ShouldAdvanceHi())
            {
                AdvanceToNextHiSync();
            }

            return AdvanceValue();
        }
        finally
        {
            _gate.Release();
        }
    }

    /// <summary>
    ///     The next id as a 64-bit integer, fetching the next "hi" allocation through
    ///     the asynchronous <see cref="AdvanceToNextHi" /> when the current lo
    ///     allocation is exhausted (weasel#808).
    /// </summary>
    /// <param name="ct">
    ///     Cancels the wait for the gate and is passed to <see cref="AdvanceToNextHi" />.
    ///     A token that is already cancelled yields a cancelled task without issuing an
    ///     id. A cancelled call never consumes an id and leaves the gate free.
    /// </param>
    /// <remarks>
    ///     <para>
    ///     Shares one gate with <see cref="NextLong" />, so synchronous and asynchronous
    ///     callers can be mixed on one instance without duplicate ids or a double advance.
    ///     When the current allocation still has room, and nobody else holds the gate,
    ///     this completes synchronously without allocating — the arithmetic path costs
    ///     an uncontended semaphore acquire and release, as <see cref="NextLong" /> does.
    ///     </para>
    ///     <para>
    ///     <see cref="AdvanceToNextHi" /> is invoked with no
    ///     <see cref="SynchronizationContext" />, so its continuations never queue to the
    ///     caller's context. That matters because a synchronous <see cref="NextLong" />
    ///     on a single-threaded context would otherwise block the very thread the advance
    ///     needs to finish on.
    ///     </para>
    /// </remarks>
    public ValueTask<long> NextLongAsync(CancellationToken ct = default)
    {
        if (ct.IsCancellationRequested)
        {
            return ValueTask.FromCanceled<long>(ct);
        }

        // Fast path: the gate is free and the current allocation has room, so the id
        // is pure arithmetic and the call completes synchronously. A zero timeout
        // never blocks: it takes the gate only if it is free right now.
#pragma warning disable VSTHRD103
        var acquired = _gate.Wait(0, CancellationToken.None);
#pragma warning restore VSTHRD103
        if (acquired)
        {
            try
            {
                if (!ShouldAdvanceHi())
                {
                    return new ValueTask<long>(AdvanceValue());
                }
            }
            finally
            {
                _gate.Release();
            }
        }

        return nextLongWithAdvanceAsync(ct);
    }

    private async ValueTask<long> nextLongWithAdvanceAsync(CancellationToken ct)
    {
        await _gate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            // Re-checked under the gate: another caller may have advanced the hi
            // between the fast path's check and this acquire.
            if (ShouldAdvanceHi())
            {
                await startAdvanceWithoutContext(ct).ConfigureAwait(false);
            }

            return AdvanceValue();
        }
        finally
        {
            _gate.Release();
        }
    }

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
    ///     returns false and the caller should retry. On success the lo offset is
    ///     reset to 1.
    /// </summary>
    protected bool TrySetCurrentHi(object? raw)
    {
        CurrentHi = Convert.ToInt64(raw);

        if (0 <= CurrentHi)
        {
            CurrentLo = 1;
            return true;
        }

        return false;
    }

    /// <summary>
    ///     Fetch the next "hi" allocation from the database asynchronously, looping
    ///     up to <see cref="IReadOnlyHiloSettings.MaxAdvanceToNextHiAttempts" /> and
    ///     throwing <see cref="HiloSequenceAdvanceToNextHiAttemptsExceededException" />
    ///     if it can't be secured. Dialect-specific (PostgreSQL stored function vs
    ///     SQL Server optimistic update).
    ///     <para>
    ///     Invoked from <see cref="NextLongAsync" />, under the gate it shares with
    ///     <see cref="NextLong" />, when the current lo allocation is exhausted
    ///     (weasel#808) — so it must not call back into either. It should honor
    ///     <paramref name="ct" />, and should not rely on a
    ///     <see cref="SynchronizationContext" />: <see cref="NextLongAsync" /> invokes it
    ///     with none.
    ///     </para>
    /// </summary>
    public abstract Task AdvanceToNextHi(CancellationToken ct = default);

    /// <summary>
    ///     Synchronous counterpart to <see cref="AdvanceToNextHi" />, invoked from
    ///     <see cref="NextLong" /> under the gate when the current lo allocation is
    ///     exhausted. Retained for the synchronous path only: once every consumer
    ///     issues ids through <see cref="NextLongAsync" /> it can go (weasel#808).
    /// </summary>
    protected abstract void AdvanceToNextHiSync();

    /// <inheritdoc />
    public abstract Task SetFloor(long floor);
}
