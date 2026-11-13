namespace Weasel.Core.Sequences;

/// <summary>
///     A Hi-Lo identity sequence: hands out monotonically increasing 64-bit ids
///     in client-side batches ("lo" values) drawn from a database-backed "hi"
///     allocation, so the database is only contacted once per <see cref="MaxLo" />
///     ids.
///     <para>
///     Lifted into Weasel.Core in weasel#287 — the contract was byte-identical
///     between Marten (<c>Marten.Schema.Identity.Sequences.ISequence</c>) and
///     Polecat. Concrete dialect implementations derive from
///     <see cref="HiloSequenceBase" /> and supply only the database I/O.
///     </para>
/// </summary>
public interface ISequence
{
    /// <summary>
    ///     Size of each client-side allocation ("lo" range) drawn from a single
    ///     database "hi" fetch. Larger values mean fewer database round trips at
    ///     the cost of larger id gaps if the process restarts mid-batch.
    /// </summary>
    int MaxLo { get; }

    /// <summary>
    ///     The next id as a 32-bit integer (a narrowing cast of <see cref="NextLong" />).
    /// </summary>
    int NextInt();

    /// <summary>
    ///     The next id as a 64-bit integer. Blocks the calling thread for a database
    ///     round trip whenever the current lo allocation is exhausted; prefer
    ///     <see cref="NextLongAsync" /> from asynchronous code.
    /// </summary>
    long NextLong();

    /// <summary>
    ///     The next id as a 64-bit integer, fetching the next "hi" allocation from the
    ///     database asynchronously when the current lo allocation is exhausted
    ///     (weasel#808). On <see cref="HiloSequenceBase" /> this completes
    ///     synchronously, without allocating, whenever the id can be issued from the
    ///     current allocation.
    /// </summary>
    /// <param name="ct">
    ///     Cancels the wait for the sequence and the database fetch. A token that is
    ///     already cancelled yields a cancelled task without issuing an id.
    /// </param>
    /// <remarks>
    ///     <para>
    ///     <see cref="HiloSequenceBase" /> implements this with real asynchronous I/O,
    ///     sharing one gate with <see cref="NextLong" /> so synchronous and asynchronous
    ///     callers on the same instance never both advance the hi.
    ///     </para>
    ///     <para>
    ///     <b>Stale implementers.</b> An <see cref="ISequence" /> compiled before this
    ///     member existed gets this default, which delegates to that implementer's own
    ///     <see cref="NextLong" />. The id is therefore a real one, unique and drawn from
    ///     the same allocation as every synchronous caller's — there is no sentinel,
    ///     because nothing this default returns could be mistaken for one. What a stale
    ///     implementer does not get is the asynchrony: the returned task has already
    ///     completed, and the calling thread blocked for any database round trip
    ///     <see cref="NextLong" /> made. An exception from <see cref="NextLong" /> is
    ///     returned as a faulted task rather than thrown from this call.
    ///     </para>
    /// </remarks>
    ValueTask<long> NextLongAsync(CancellationToken ct = default)
    {
        if (ct.IsCancellationRequested)
        {
            return ValueTask.FromCanceled<long>(ct);
        }

        try
        {
            return new ValueTask<long>(NextLong());
        }
        catch (Exception e)
        {
            return ValueTask.FromException<long>(e);
        }
    }

    /// <summary>
    ///     The next id as a 32-bit integer: a narrowing cast of
    ///     <see cref="NextLongAsync" />, as <see cref="NextInt" /> is of
    ///     <see cref="NextLong" />.
    /// </summary>
    /// <param name="ct">Passed through to <see cref="NextLongAsync" />.</param>
    /// <remarks>
    ///     A stale implementer gets this default, which goes through
    ///     <see cref="NextLongAsync" /> — and so, for an implementer that predates both,
    ///     through its own <see cref="NextLong" />, with the blocking behaviour described
    ///     there.
    /// </remarks>
    async ValueTask<int> NextIntAsync(CancellationToken ct = default)
    {
        return (int)await NextLongAsync(ct).ConfigureAwait(false);
    }

    /// <summary>
    ///     Advance the sequence so that subsequently-issued ids are guaranteed to
    ///     be greater than <paramref name="floor" />. Used to reset a sequence past
    ///     externally-seeded data.
    /// </summary>
    Task SetFloor(long floor);
}
