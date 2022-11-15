using System;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Shouldly;
using Xunit;

namespace Weasel.SqlServer.Tests
{
    public class advisory_lock_usage 
    {

        [Fact]
        public async Task explicitly_release_global_session_locks()
        {
            await using (var conn1 = new SqlConnection(ConnectionSource.ConnectionString))
            await using (var conn2 = new SqlConnection(ConnectionSource.ConnectionString))
            await using (var conn3 = new SqlConnection(ConnectionSource.ConnectionString))
            {
                await conn1.OpenAsync();
                await conn2.OpenAsync();
                await conn3.OpenAsync();


                await conn1.GetGlobalLock("1");


                // Cannot get the lock here
                (await conn2.TryGetGlobalLock("1")).ShouldBeFalse();


                await conn1.ReleaseGlobalLock("1");


                for (var j = 0; j < 5; j++)
                {
                    if (await conn2.TryGetGlobalLock("1")) return;

                    await Task.Delay(250);
                }

                throw new Exception("Advisory lock was not released");
            }
        }

        [Fact]
        public async Task explicitly_release_global_tx_session_locks()
        {
            await using (var conn1 = new SqlConnection(ConnectionSource.ConnectionString))
            await using (var conn2 = new SqlConnection(ConnectionSource.ConnectionString))
            await using (var conn3 = new SqlConnection(ConnectionSource.ConnectionString))
            {
                await conn1.OpenAsync();
                await conn2.OpenAsync();
                await conn3.OpenAsync();

                var tx1 = conn1.BeginTransaction();
                await tx1.GetGlobalTxLock("2");


                // Cannot get the lock here
                var tx2 = conn2.BeginTransaction();
                (await tx2.TryGetGlobalTxLock("2")).ShouldBeFalse();


                tx1.Rollback();


                for (var j = 0; j < 5; j++)
                {
                    if (await tx2.TryGetGlobalTxLock("2"))
                    {
                        tx2.Rollback();
                        return;
                    }

                    await Task.Delay(250);
                }

                throw new Exception("Advisory lock was not released");
            }
        }

        [Fact] // - too slow
        public async Task global_session_locks()
        {
            await using (var conn1 = new SqlConnection(ConnectionSource.ConnectionString))
            await using (var conn2 = new SqlConnection(ConnectionSource.ConnectionString))
            await using (var conn3 = new SqlConnection(ConnectionSource.ConnectionString))
            {
                await conn1.OpenAsync();
                await conn2.OpenAsync();
                await conn3.OpenAsync();

                await conn1.GetGlobalLock("24");


                try
                {
                    // Cannot get the lock here
                    (await conn2.TryGetGlobalLock("24")).ShouldBeFalse();

                    // Can get the new lock
                    (await conn3.TryGetGlobalLock("25")).ShouldBeTrue();

                    // Cannot get the lock here
                    (await conn2.TryGetGlobalLock("25")).ShouldBeFalse();
                }
                finally
                {
                    await conn1.ReleaseGlobalLock("24");
                    await conn3.ReleaseGlobalLock("25");
                }
            }
        }

        [Fact] // -- too slow
        public async Task tx_session_locks()
        {
            await using (var conn1 = new SqlConnection(ConnectionSource.ConnectionString))
            await using (var conn2 = new SqlConnection(ConnectionSource.ConnectionString))
            await using (var conn3 = new SqlConnection(ConnectionSource.ConnectionString))
            {
                await conn1.OpenAsync();
                await conn2.OpenAsync();
                await conn3.OpenAsync();

                var tx1 = conn1.BeginTransaction();
                await tx1.GetGlobalTxLock("4");


                // Cannot get the lock here
                var tx2 = conn2.BeginTransaction();
                (await tx2.TryGetGlobalTxLock("4")).ShouldBeFalse();

                // Can get the new lock
                var tx3 = conn3.BeginTransaction();
                (await tx3.TryGetGlobalTxLock("5")).ShouldBeTrue();

                // Cannot get the lock here
                (await tx2.TryGetGlobalTxLock("5")).ShouldBeFalse();

                tx1.Rollback();
                tx2.Rollback();
                tx3.Rollback();
            }
        }
    }
}
