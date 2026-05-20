namespace Weasel.Core.Sequences;

/// <summary>
///     Dialect-agnostic core of a Hi-Lo <see cref="ISequence" />: owns the
///     <see cref="CurrentHi" /> / <see cref="CurrentLo" /> / <see cref="MaxLo" />
///     state and the client-side id arithmetic (<see cref="AdvanceValue" />,
///     <see cref="ShouldAdvanceHi" />, <see cref="NextInt" /> / <see cref="NextLong" />,
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
    // System.Threading.Lock (net9+) — guards the lo-allocation / hi-advance
    // critical section in NextLong so concurrent id requests don't hand out
    // duplicates or race the hi advance.
    private readonly Lock _lock = new();

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

    /// <inheritdoc />
    public long NextLong()
    {
        lock (_lock)
        {
            if (ShouldAdvanceHi())
            {
                AdvanceToNextHiSync();
            }

            return AdvanceValue();
        }
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
    /// </summary>
    public abstract Task AdvanceToNextHi(CancellationToken ct = default);

    /// <summary>
    ///     Synchronous counterpart to <see cref="AdvanceToNextHi" />, invoked from
    ///     <see cref="NextLong" /> under the lock when the current lo allocation is
    ///     exhausted.
    /// </summary>
    protected abstract void AdvanceToNextHiSync();

    /// <inheritdoc />
    public abstract Task SetFloor(long floor);
}
