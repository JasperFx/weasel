namespace Weasel.Core.Sequences;

/// <summary>
///     Read-only view of the configuration for a Hi-Lo <see cref="ISequence" />.
///     Lifted into Weasel.Core in weasel#287 (byte-identical between Marten and
///     Polecat).
/// </summary>
public interface IReadOnlyHiloSettings
{
    /// <summary>
    ///     Size of each client-side "lo" allocation. Defaults to 1000.
    /// </summary>
    int MaxLo { get; }

    /// <summary>
    ///     Optional override of the database sequence/row name. When null the
    ///     consuming store derives a name from the entity.
    /// </summary>
    string? SequenceName { get; }

    /// <summary>
    ///     How many times to retry the dialect's "advance to next hi" operation
    ///     before giving up with
    ///     <see cref="HiloSequenceAdvanceToNextHiAttemptsExceededException" />.
    ///     Defaults to 30.
    /// </summary>
    int MaxAdvanceToNextHiAttempts { get; }
}

/// <summary>
///     Mutable configuration for a Hi-Lo <see cref="ISequence" />.
/// </summary>
public class HiloSettings: IReadOnlyHiloSettings
{
    public int MaxLo { get; set; } = 1000;
    public string? SequenceName { get; set; } = null;
    public int MaxAdvanceToNextHiAttempts { get; set; } = 30;
}
