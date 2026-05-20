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
    ///     The next id as a 64-bit integer.
    /// </summary>
    long NextLong();

    /// <summary>
    ///     Advance the sequence so that subsequently-issued ids are guaranteed to
    ///     be greater than <paramref name="floor" />. Used to reset a sequence past
    ///     externally-seeded data.
    /// </summary>
    Task SetFloor(long floor);
}
