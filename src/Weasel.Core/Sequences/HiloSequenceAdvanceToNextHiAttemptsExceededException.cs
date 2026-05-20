namespace Weasel.Core.Sequences;

/// <summary>
///     Thrown when a Hi-Lo sequence exhausts
///     <see cref="IReadOnlyHiloSettings.MaxAdvanceToNextHiAttempts" /> tries to
///     atomically secure the next "hi" allocation from the database (typically
///     because of sustained write contention on the hilo row).
///     <para>
///     Lifted into Weasel.Core in weasel#287 — previously duplicated with the
///     same message and parameterless constructor in both Marten
///     (<c>Marten.Exceptions</c>) and Polecat (<c>Polecat.Exceptions</c>).
///     </para>
/// </summary>
public class HiloSequenceAdvanceToNextHiAttemptsExceededException: Exception
{
    private const string message =
        "Advance to next hilo sequence retry limit exceeded. Unable to secure next hi sequence";

    public HiloSequenceAdvanceToNextHiAttemptsExceededException(): base(message)
    {
    }
}
