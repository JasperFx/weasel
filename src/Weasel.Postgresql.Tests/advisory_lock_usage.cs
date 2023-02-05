using System;
using System.Data.SqlClient;
using System.Threading.Tasks;
using Npgsql;
using Shouldly;
using Weasel.Core.Migrations;
using Xunit;

namespace Weasel.Postgresql.Tests
{
    public class advisory_lock_usage
    {
        [Fact]
        public async Task explicitly_release_global_session_locks()
        {
            await using var conn1 = new NpgsqlConnection(ConnectionSource.ConnectionString);
            await using var conn2 = new NpgsqlConnection(ConnectionSource.ConnectionString);
            await using var conn3 = new NpgsqlConnection(ConnectionSource.ConnectionString);

            await conn1.OpenAsync();
            await conn2.OpenAsync();
            await conn3.OpenAsync();

            await conn1.GetGlobalLock(1);

            // Cannot get the lock here
            (await conn2.TryGetGlobalLock(1)).ShouldBe(AttainLockResult.Failure);

            await conn1.ReleaseGlobalLock(1);

            for (var j = 0; j < 5; j++)
            {
                if (await conn2.TryGetGlobalLock(1) == AttainLockResult.Success) return;

                await Task.Delay(250);
            }

            throw new Exception("Advisory lock was not released");
        }

        [Fact]
        public async Task explicitly_release_global_tx_session_locks()
        {
            await using var conn1 = new NpgsqlConnection(ConnectionSource.ConnectionString);
            await using var conn2 = new NpgsqlConnection(ConnectionSource.ConnectionString);
            await using var conn3 = new NpgsqlConnection(ConnectionSource.ConnectionString);

            await conn1.OpenAsync();
            await conn2.OpenAsync();
            await conn3.OpenAsync();

            var tx1 = await conn1.BeginTransactionAsync();
            await tx1.GetGlobalTxLock(2);


            // Cannot get the lock here
            var tx2 = await conn2.BeginTransactionAsync();
            (await tx2.TryGetGlobalTxLock(2)).ShouldBe(AttainLockResult.Failure);


            await tx1.RollbackAsync();


            for (var j = 0; j < 5; j++)
            {
                if (await tx2.TryGetGlobalTxLock(2) == AttainLockResult.Success)
                {
                    await tx2.RollbackAsync();
                    return;
                }

                await Task.Delay(250);
            }

            throw new Exception("Advisory lock was not released");
        }

        [Fact] // - too slow
        public async Task global_session_locks()
        {
            await using var conn1 = new NpgsqlConnection(ConnectionSource.ConnectionString);
            await using var conn2 = new NpgsqlConnection(ConnectionSource.ConnectionString);
            await using var conn3 = new NpgsqlConnection(ConnectionSource.ConnectionString);

            await conn1.OpenAsync();
            await conn2.OpenAsync();
            await conn3.OpenAsync();

            await conn1.GetGlobalLock(24);

            try
            {
                // Cannot get the lock here
                (await conn2.TryGetGlobalLock(24)).ShouldBe(AttainLockResult.Failure);

                // Can get the new lock
                (await conn3.TryGetGlobalLock(25)).ShouldBe(AttainLockResult.Success);

                // Cannot get the lock here
                (await conn2.TryGetGlobalLock(25)).ShouldBe(AttainLockResult.Failure);
            }
            finally
            {
                await conn1.ReleaseGlobalLock(24);
                await conn3.ReleaseGlobalLock(25);
            }
        }

        [Fact] // -- too slow
        public async Task tx_session_locks()
        {
            await using var conn1 = new NpgsqlConnection(ConnectionSource.ConnectionString);
            await using var conn2 = new NpgsqlConnection(ConnectionSource.ConnectionString);
            await using var conn3 = new NpgsqlConnection(ConnectionSource.ConnectionString);

            await conn1.OpenAsync();
            await conn2.OpenAsync();
            await conn3.OpenAsync();

            var tx1 = await conn1.BeginTransactionAsync();
            await tx1.GetGlobalTxLock(4);


            // Cannot get the lock here
            var tx2 = await conn2.BeginTransactionAsync();
            (await tx2.TryGetGlobalTxLock(4)).ShouldBe(AttainLockResult.Failure);

            // Can get the new lock
            var tx3 = await conn3.BeginTransactionAsync();
            (await tx3.TryGetGlobalTxLock(5)).ShouldBe(AttainLockResult.Success);

            // Cannot get the lock here
            (await tx2.TryGetGlobalTxLock( 5)).ShouldBe(AttainLockResult.Failure);

            await tx1.RollbackAsync();
            await tx2.RollbackAsync();
            await tx3.RollbackAsync();
        }
    }
}
