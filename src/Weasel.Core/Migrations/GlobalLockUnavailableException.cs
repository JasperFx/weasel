namespace Weasel.Core.Migrations;

/// <summary>
///     Thrown when a blocking attempt to take the global migration lock does not get it
///     (weasel#599).
/// </summary>
/// <remarks>
///     <para>
///     Derives from <see cref="InvalidOperationException" /> deliberately: that is what the
///     PostgreSQL advisory-lock path in <c>DatabaseBase</c> has always thrown for this, so an
///     existing <c>catch (InvalidOperationException)</c> keeps working and gains a type it can
///     narrow to. The SQL Server side used to throw a bare <see cref="Exception" /> carrying an
///     undecoded <c>sp_getapplock</c> return code.
///     </para>
///     <para>
///     Failing to get the lock is usually not a fault at all -- on a multi-replica rolling deploy
///     it is the expected outcome for every replica but one, which is why the message names
///     <c>ResourceMigrationFailureMode.ContinueOnFailures</c>.
///     </para>
/// </remarks>
public class GlobalLockUnavailableException: InvalidOperationException
{
    public GlobalLockUnavailableException(string message): base(message)
    {
    }

    public GlobalLockUnavailableException(string message, Exception? innerException)
        : base(message, innerException)
    {
    }

    /// <summary>
    ///     The lock that could not be taken, when the provider names one.
    /// </summary>
    public string? Resource { get; init; }

    /// <summary>
    ///     The provider's own return code or error number, when there is one -- an
    ///     <c>sp_getapplock</c> return value on SQL Server. Null where the provider reports the
    ///     failure as a plain false, as PostgreSQL's <c>pg_try_advisory_lock</c> does.
    /// </summary>
    public int? ReturnCode { get; init; }
}
