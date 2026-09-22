using Microsoft.Data.SqlClient;
using Weasel.Core.Migrations;

namespace Weasel.SqlServer;

/// <summary>
///     The <see cref="IGlobalLock{TConnection}" /> that <c>DatabaseBase</c> drives, backed by
///     <c>sp_getapplock</c> (weasel#599).
/// </summary>
/// <remarks>
///     <para>
///     Before this, SQL Server had no implementation of the seam at all, so
///     <c>ApplyAllConfiguredChangesToDatabaseAsync</c> ran under the nullo lock and the only
///     SQL Server migration lock in the stack was the one callers took for themselves through
///     <see cref="SharedLockExtensions.GetGlobalLock" /> -- where losing the race threw rather
///     than being ruled on by <see cref="ResourceMigrationFailureMode" />. With this, contention
///     is an <see cref="AttainLockResult" /> failure on SQL Server exactly as it is on PostgreSQL,
///     and <c>ContinueOnFailures</c> means the same thing on both.
///     </para>
///     <para>
///     The lock is taken with <c>LockOwner = Session</c>, so it is held for the life of the
///     connection rather than a transaction, and released explicitly.
///     </para>
/// </remarks>
public class SqlServerGlobalLock: IGlobalLock<SqlConnection>
{
    private readonly string _lockId;
    private readonly int? _lockTimeoutMs;

    /// <param name="lockId">The <c>sp_getapplock</c> resource name. 255 characters or fewer.</param>
    /// <param name="lockTimeoutMs">
    ///     Overrides <see cref="SharedLockExtensions.DefaultLockTimeoutMilliseconds" /> for this lock.
    /// </param>
    public SqlServerGlobalLock(string lockId, int? lockTimeoutMs = null)
    {
        _lockId = lockId;
        _lockTimeoutMs = lockTimeoutMs;
    }

    public Task<AttainLockResult> TryAttainLock(SqlConnection conn, CancellationToken ct = default)
    {
        return conn.TryAttainGlobalLock(_lockId, ct, _lockTimeoutMs);
    }

    public Task ReleaseLock(SqlConnection conn, CancellationToken ct = default)
    {
        return conn.ReleaseGlobalLock(_lockId, ct);
    }
}
