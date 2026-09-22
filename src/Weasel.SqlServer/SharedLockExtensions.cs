using System.Data;
using Microsoft.Data.SqlClient;
using Weasel.Core.Migrations;

namespace Weasel.SqlServer;

public static class SharedLockExtensions
{
    /// <summary>
    ///     How long <c>sp_getapplock</c> waits for the lock before giving up, in milliseconds.
    ///     Used by every method here that does not take an explicit timeout. This was hard-coded
    ///     to 1000ms and was the only migration-lock timeout in the stack that could not be
    ///     changed (weasel#599).
    /// </summary>
    /// <remarks>
    ///     <c>sp_getapplock</c>'s own <c>@LockTimeout</c> defaults to the session's
    ///     <c>@@LOCK_TIMEOUT</c>; Weasel passes an explicit value so the wait is bounded and
    ///     predictable regardless of session settings. A longer value suits a deploy where one
    ///     replica is expected to wait out another's migration; a shorter one suits
    ///     <see cref="ResourceMigrationFailureMode.ContinueOnFailures" />, where losing the race
    ///     is the normal outcome and there is no point waiting for it.
    /// </remarks>
    public static int DefaultLockTimeoutMilliseconds { get; set; } = 1000;

    /// <summary>
    ///     Retrieve a global, shared lock at the transaction level for the given lock id. This will block until
    ///     it is able to attain the lock
    /// </summary>
    /// <param name="tx">The current transaction</param>
    /// <param name="lockId">The identity of the lock</param>
    /// <param name="cancellation"></param>
    /// <param name="lockTimeoutMs">Overrides <see cref="DefaultLockTimeoutMilliseconds" /> for this call</param>
    /// <returns></returns>
    /// <exception cref="GlobalLockUnavailableException">The lock could not be attained</exception>
    public static Task GetGlobalTxLock(this SqlTransaction tx, string lockId,
        CancellationToken cancellation = default, int? lockTimeoutMs = null)
    {
        return getLock(tx.Connection, lockId, "Transaction", tx, cancellation, lockTimeoutMs);
    }

    private static async Task getLock(this SqlConnection conn, string lockId, string owner, SqlTransaction? tx,
        CancellationToken cancellation, int? lockTimeoutMs = null)
    {
        var timeout = lockTimeoutMs ?? DefaultLockTimeoutMilliseconds;
        var returnValue = await tryGetLock(conn, lockId, owner, tx, cancellation, timeout).ConfigureAwait(false);

        if (returnValue < 0)
        {
            throw GlobalLockFailure(lockId, returnValue, timeout);
        }
    }

    /// <summary>
    ///     Turn an <c>sp_getapplock</c> return code into an exception that says what it means.
    ///     The codes are documented and specific, and the old message
    ///     (<c>sp_getapplock failed with errorCode '-1'</c>, on a bare
    ///     <see cref="Exception" />) carried neither the meaning nor a type anything could catch.
    ///     Public so the decoding can be asserted without a SQL Server to hand.
    /// </summary>
    public static GlobalLockUnavailableException GlobalLockFailure(string lockId, int returnValue, int lockTimeoutMs)
    {
        var explanation = returnValue switch
        {
            -1 =>
                $"Another session holds the application lock '{lockId}' (sp_getapplock returned -1 after {lockTimeoutMs} ms). "
                + "Usually another replica is applying migrations; wait and retry, raise "
                + $"{nameof(SharedLockExtensions)}.{nameof(DefaultLockTimeoutMilliseconds)}, or set "
                + "ResourceMigrationFailureMode.ContinueOnFailures so losing the race is not a startup failure.",
            -2 =>
                $"The request for application lock '{lockId}' was cancelled (sp_getapplock returned -2).",
            -3 =>
                $"The session requesting application lock '{lockId}' was chosen as a deadlock victim "
                + "(sp_getapplock returned -3). The request can be retried.",
            -999 =>
                $"SQL Server rejected the request for application lock '{lockId}' (sp_getapplock returned -999: "
                + "parameter validation or other error). Check that the resource name is 255 characters or fewer.",
            _ =>
                $"Unable to attain the application lock '{lockId}' (sp_getapplock returned {returnValue})."
        };

        return new GlobalLockUnavailableException(explanation)
        {
            Resource = lockId, ReturnCode = returnValue
        };
    }

    private static async Task<int> tryGetLock(this SqlConnection conn, string lockId, string owner, SqlTransaction? tx,
        CancellationToken cancellation, int? lockTimeoutMs = null)
    {
        var cmd = conn.CreateCommand("sp_getapplock");
        cmd.Transaction = tx;

        cmd.CommandType = CommandType.StoredProcedure;
        cmd.With("Resource", lockId);
        cmd.With("LockMode", "Exclusive");

        cmd.With("LockOwner", owner);
        cmd.With("LockTimeout", lockTimeoutMs ?? DefaultLockTimeoutMilliseconds);

        var returnValue = cmd.CreateParameter();
        returnValue.ParameterName = "ReturnValue";
        returnValue.DbType = DbType.Int32;
        returnValue.Direction = ParameterDirection.ReturnValue;
        cmd.Parameters.Add(returnValue);

        await cmd.ExecuteNonQueryAsync(cancellation).ConfigureAwait(false);

        return (int)returnValue.Value;
    }

    /// <summary>
    ///     Attempt to attain a shared lock for the duration of the transaction. This method
    ///     will return a boolean designating whether or not it was able to attain
    ///     the shared lock.
    /// </summary>
    /// <param name="tx"></param>
    /// <param name="lockId"></param>
    /// <param name="cancellation"></param>
    /// <param name="lockTimeoutMs">Overrides <see cref="DefaultLockTimeoutMilliseconds" /> for this call</param>
    /// <returns></returns>
    public static async Task<bool> TryGetGlobalTxLock(this SqlTransaction tx, string lockId,
        CancellationToken cancellation = default, int? lockTimeoutMs = null)
    {
        return await tryGetLock(tx.Connection, lockId, "Transaction", tx, cancellation, lockTimeoutMs)
            .ConfigureAwait(false) >= 0;
    }


    /// <summary>
    ///     Attempts to attain a shared lock at the session level that will be retained until the connection is closed.
    ///     This will block until it attains the lock.
    /// </summary>
    /// <param name="conn"></param>
    /// <param name="lockId"></param>
    /// <param name="cancellation"></param>
    /// <param name="transaction"></param>
    /// <param name="lockTimeoutMs">Overrides <see cref="DefaultLockTimeoutMilliseconds" /> for this call</param>
    /// <returns></returns>
    /// <exception cref="GlobalLockUnavailableException">The lock could not be attained</exception>
    public static Task GetGlobalLock(this SqlConnection conn, string lockId, CancellationToken cancellation = default,
        SqlTransaction? transaction = null, int? lockTimeoutMs = null)
    {
        return getLock(conn, lockId, "Session", transaction, cancellation, lockTimeoutMs);
    }

    /// <summary>
    ///     Attempts to attain a shared lock at the session level. This method
    ///     will return a boolean designating whether or not it was able to attain
    ///     the shared lock.
    /// </summary>
    /// <param name="conn"></param>
    /// <param name="lockId"></param>
    /// <param name="cancellation"></param>
    /// <param name="lockTimeoutMs">Overrides <see cref="DefaultLockTimeoutMilliseconds" /> for this call</param>
    /// <returns></returns>
    public static async Task<bool> TryGetGlobalLock(this SqlConnection conn, string lockId,
        CancellationToken cancellation = default, int? lockTimeoutMs = null)
    {
        return await tryGetLock(conn, lockId, "Session", null, cancellation, lockTimeoutMs).ConfigureAwait(false) >= 0;
    }

    /// <summary>
    ///     The <see cref="AttainLockResult" /> shape of <see cref="TryGetGlobalLock" />, for the
    ///     <see cref="IGlobalLock{TConnection}" /> seam that <c>DatabaseBase</c> drives. Contention
    ///     (-1), cancellation (-2) and being picked as a deadlock victim (-3) are reported as a
    ///     failure the caller's <see cref="ResourceMigrationFailureMode" /> gets to rule on; -999
    ///     is a malformed request rather than a busy lock and still throws (weasel#599).
    /// </summary>
    public static async Task<AttainLockResult> TryAttainGlobalLock(this SqlConnection conn, string lockId,
        CancellationToken cancellation = default, int? lockTimeoutMs = null)
    {
        var timeout = lockTimeoutMs ?? DefaultLockTimeoutMilliseconds;
        var returnValue = await tryGetLock(conn, lockId, "Session", null, cancellation, timeout).ConfigureAwait(false);

        if (returnValue >= 0)
        {
            return AttainLockResult.Success;
        }

        if (returnValue == -999)
        {
            throw GlobalLockFailure(lockId, returnValue, timeout);
        }

        return AttainLockResult.Failure();
    }

    /// <summary>
    ///     Explicitly releases a shared lock. The transaction is an optional argument.
    /// </summary>
    /// <param name="conn"></param>
    /// <param name="lockId"></param>
    /// <param name="cancellation"></param>
    /// <param name="tx"></param>
    /// <returns></returns>
    public static Task ReleaseGlobalLock(this SqlConnection conn, string lockId,
        CancellationToken cancellation = default,
        SqlTransaction? tx = null)
    {
        var sqlCommand = conn.CreateCommand("sp_releaseapplock");
        sqlCommand.Transaction = tx;
        sqlCommand.CommandType = CommandType.StoredProcedure;

        sqlCommand.With("Resource", lockId);
        sqlCommand.With("LockOwner", "Session");

        return sqlCommand.ExecuteNonQueryAsync(cancellation);
    }
}
