using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using Weasel.Core;

namespace Weasel.Postgresql
{
    public static class AdvisoryLockExtensions
    {
        /// <summary>
        /// Retrieve a global, shared lock at the transaction level for the given lock id. This will block until
        /// it is able to attain the lock
        /// </summary>
        /// <param name="tx">The current transaction</param>
        /// <param name="lockId">The identity of the lock</param>
        /// <param name="cancellation"></param>
        /// <returns></returns>
        public static Task GetGlobalTxLock(this NpgsqlTransaction tx, int lockId, CancellationToken cancellation = default)
        {
            return tx.CreateCommand("SELECT pg_advisory_xact_lock(:id);").With("id", lockId)
                .ExecuteNonQueryAsync(cancellation);
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
        public static async Task<bool> TryGetGlobalTxLock(this NpgsqlTransaction tx, int lockId, CancellationToken cancellation = default)
        {
            var c = await tx.CreateCommand("SELECT pg_try_advisory_xact_lock(:id);")
                .With("id", lockId)
                .ExecuteScalarAsync(cancellation).ConfigureAwait(false);

            return (bool) c;
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
        public static Task GetGlobalLock(this NpgsqlConnection conn, int lockId, CancellationToken cancellation = default)
        {
            return conn.CreateCommand("SELECT pg_advisory_lock(:id);").With("id", lockId)
                .ExecuteNonQueryAsync(cancellation);
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
        public static async Task<bool> TryGetGlobalLock(this NpgsqlConnection conn, int lockId, CancellationToken cancellation = default)
        {
            var c = await conn.CreateCommand("SELECT pg_try_advisory_lock(:id);")
                .With("id", lockId)
                .ExecuteScalarAsync(cancellation).ConfigureAwait(false);

            return (bool) c;
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
        public static async Task<bool> TryGetGlobalLock(this NpgsqlConnection conn, int lockId, NpgsqlTransaction tx, CancellationToken cancellation = default)
        {
            var c = await conn.CreateCommand("SELECT pg_try_advisory_xact_lock(:id);")
                .With("id", lockId)
                .ExecuteScalarAsync(cancellation).ConfigureAwait(false);

            return (bool) c;
        }

        /// <summary>
        /// Explicitly releases a shared lock. The transaction is an optional argument.
        /// </summary>
        /// <param name="conn"></param>
        /// <param name="lockId"></param>
        /// <param name="cancellation"></param>
        /// <param name="tx"></param>
        /// <returns></returns>
        public static Task ReleaseGlobalLock(this NpgsqlConnection conn, int lockId, CancellationToken cancellation = default, NpgsqlTransaction? tx = null)
        {
            return conn.CreateCommand("SELECT pg_advisory_unlock(:id);").With("id", lockId)
                .ExecuteNonQueryAsync(cancellation);
        }


    }
}