using System;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;

namespace Weasel.SqlServer
{
    public static class SharedLockExtensions
    {
        /// <summary>
        /// Retrieve a global, shared lock at the transaction level for the given lock id. This will block until
        /// it is able to attain the lock
        /// </summary>
        /// <param name="tx">The current transaction</param>
        /// <param name="lockId">The identity of the lock</param>
        /// <param name="cancellation"></param>
        /// <returns></returns>
        public static Task GetGlobalTxLock(this SqlTransaction tx, string lockId, CancellationToken cancellation = default(CancellationToken))
        {
            return getLock(tx.Connection, lockId, "Transaction", tx, cancellation);
        }

        private static async Task getLock(this SqlConnection conn, string lockId, string owner, SqlTransaction tx,
            CancellationToken cancellation)
        {
            var returnValue = await tryGetLock(conn, lockId, owner, tx, cancellation);

            if (returnValue < 0)
                throw new Exception($"sp_getapplock failed with errorCode '{returnValue}'");
        }

        private static async Task<int> tryGetLock(this SqlConnection conn, string lockId, string owner, SqlTransaction tx,
            CancellationToken cancellation)
        {
            var cmd = conn.CreateCommand("sp_getapplock");
            cmd.Transaction = tx;

            cmd.CommandType = CommandType.StoredProcedure;
            cmd.With("Resource", lockId);
            cmd.With("LockMode", "Exclusive");

            cmd.With("LockOwner", owner);
            cmd.With("LockTimeout", 1000);

            var returnValue = cmd.CreateParameter();
            returnValue.ParameterName = "ReturnValue";
            returnValue.DbType = DbType.Int32;
            returnValue.Direction = ParameterDirection.ReturnValue;
            cmd.Parameters.Add(returnValue);

            await cmd.ExecuteNonQueryAsync(cancellation);

            return (int) returnValue.Value;
        }

        /// <summary>
        /// Attempt to attain a shared lock for the duration of the transaction. This method
        /// will return a boolean designating whether or not it was able to attain
        /// the shared lock.
        /// </summary>
        /// <param name="tx"></param>
        /// <param name="lockId"></param>
        /// <param name="cancellation"></param>
        /// <returns></returns>
        public static async Task<bool> TryGetGlobalTxLock(this SqlTransaction tx, string lockId,
            CancellationToken cancellation = default(CancellationToken))
        {
            return await tryGetLock(tx.Connection, lockId, "Transaction", tx, cancellation) >= 0;
        }


        /// <summary>
        /// Attempts to attain a shared lock at the session level that will be retained until the connection is closed.
        /// This will block until it attains the lock.
        /// </summary>
        /// <param name="conn"></param>
        /// <param name="lockId"></param>
        /// <param name="cancellation"></param>
        /// <param name="transaction"></param>
        /// <returns></returns>
        public static Task GetGlobalLock(this SqlConnection conn, string lockId, CancellationToken cancellation = default(CancellationToken),
            SqlTransaction transaction = null)
        {
            return getLock(conn, lockId, "Session", transaction, cancellation);
        }

        /// <summary>
        /// Attempts to attain a shared lock at the session level. This method
        /// will return a boolean designating whether or not it was able to attain
        /// the shared lock.
        /// </summary>
        /// <param name="conn"></param>
        /// <param name="lockId"></param>
        /// <param name="cancellation"></param>
        /// <returns></returns>
        public static async Task<bool> TryGetGlobalLock(this SqlConnection conn, string lockId, CancellationToken cancellation = default(CancellationToken))
        {
            return await tryGetLock(conn, lockId, "Session", null, cancellation) >= 0;
        }

        /// <summary>
        /// Explicitly releases a shared lock. The transaction is an optional argument.
        /// </summary>
        /// <param name="conn"></param>
        /// <param name="lockId"></param>
        /// <param name="cancellation"></param>
        /// <param name="tx"></param>
        /// <returns></returns>
        public static Task ReleaseGlobalLock(this SqlConnection conn, string lockId, CancellationToken cancellation = default(CancellationToken),
            SqlTransaction tx = null)
        {
            var sqlCommand = conn.CreateCommand("sp_releaseapplock");
            sqlCommand.Transaction = tx;
            sqlCommand.CommandType = CommandType.StoredProcedure;

            sqlCommand.With("Resource", lockId);
            sqlCommand.With("LockOwner", "Session");

            return sqlCommand.ExecuteNonQueryAsync(cancellation);
        }
    }
}