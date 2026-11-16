using System.Collections.Concurrent;
using Microsoft.Data.Sqlite;
using Shouldly;
using Weasel.Core;
using Xunit;

namespace Weasel.Sqlite.Tests;

/// <summary>
///     Microsoft.Data.Sqlite 10.0.12 and earlier can lend one native handle to two open connections when
///     opens of one pool race (an <c>Activate</c> that publishes <c>_active</c> before its owner, read by a
///     concurrent <c>ReclaimLeakedConnections</c>). Weasel serializes the pool checkout per exact connection
///     string. See JasperFx/weasel#857, fisher#576 and fisher#582, and <see cref="SqlitePoolGate" />.
/// </summary>
public class serializing_the_pool_checkout
{
    // The stand-alone reproduction in weasel#857 used 12 threads over 200 fresh pools. The race lives in a
    // pool that is still growing, so the loop keeps moving to fresh pools rather than hammering one warm
    // one. With the gate disabled on an 18-core Apple silicon machine, these settings failed 37 of 40 runs
    // of the four handle tests on net10.0 and 19 of 20 on net11.0; with it, 0 of 60.
    private const int Threads = 12;
    private const int OpensPerThreadPerPool = 100;
    private static readonly TimeSpan Budget = TimeSpan.FromSeconds(2);

    [Fact]
    public void no_handle_is_held_by_two_open_connections_through_open_connection()
    {
        var shared = countSharedHandles(
            cs => new SqliteDataSource(cs, new NoPragmas()),
            source => (SqliteConnection)((SqliteDataSource)source).OpenConnection());

        shared.ShouldBe(0);
    }

    [Fact]
    public void no_handle_is_held_by_two_open_connections_through_open_connection_async()
    {
        var shared = countSharedHandles(
            cs => new SqliteDataSource(cs, new NoPragmas()),
            source =>
            {
                // The checkout completes synchronously, so this does not block on real async work; it is
                // here to drive OpenDbConnectionAsync, the path session opens take.
#pragma warning disable VSTHRD002
                return (SqliteConnection)((SqliteDataSource)source).OpenConnectionAsync().AsTask().GetAwaiter().GetResult();
#pragma warning restore VSTHRD002
            });

        shared.ShouldBe(0);
    }

    [Fact]
    public void no_handle_is_held_by_two_open_connections_through_create_connection_then_open()
    {
        var shared = countSharedHandles(
            cs => new SqliteDataSource(cs, new NoPragmas()),
            source =>
            {
                var conn = (SqliteConnection)((SqliteDataSource)source).CreateConnection();
                conn.Open();
                return conn;
            });

        shared.ShouldBe(0);
    }

    [Fact]
    public void no_handle_is_held_by_two_open_connections_through_the_database_connection_factory()
    {
        var shared = countSharedHandles(
            cs => new DatabaseWithTables("pool-gate", cs),
            source =>
            {
                var conn = ((DatabaseWithTables)source).CreateConnection();
                conn.Open();
                return conn;
            });

        shared.ShouldBe(0);
    }

    [Fact]
    public void the_data_source_and_the_database_factory_hand_out_gated_connections()
    {
        var cs = uniqueConnectionString();
        using var dataSource = new SqliteDataSource(cs);

        using var created = dataSource.CreateConnection();
        created.ShouldBeOfType<GatedSqliteConnection>();
        created.ShouldBeAssignableTo<SqliteConnection>();

        using var fromDatabase = new DatabaseWithTables("pool-gate", cs).CreateConnection();
        fromDatabase.ShouldBeOfType<GatedSqliteConnection>();
    }

    [Fact]
    public void the_gate_is_keyed_by_the_exact_connection_string()
    {
        var cs = uniqueConnectionString();

        // Comparing identities converts the Lock to object, which is what CS9216 warns about; nothing locks it.
#pragma warning disable CS9216
        SqlitePoolGate.For(cs).ShouldBeSameAs(SqlitePoolGate.For(new string(cs.AsSpan())));
        SqlitePoolGate.For(cs).ShouldNotBeSameAs(SqlitePoolGate.For(cs + ";"));
        SqlitePoolGate.For(cs).ShouldNotBeSameAs(SqlitePoolGate.For(cs.ToUpperInvariant()));
#pragma warning restore CS9216
    }

    [Fact]
    public async Task two_data_sources_over_one_connection_string_share_the_gate()
    {
        var cs = uniqueConnectionString();
        using var first = new SqliteDataSource(cs);
        using var second = new SqliteDataSource(cs);
        var db = new DatabaseWithTables("pool-gate", cs);

        // A connection is opened first, so the waits below are on the gate and not on creating the file.
        using (first.OpenConnection())
        {
        }

        await whileGateIsHeld(cs, async () =>
        {
            var throughSecond = Task.Run(() => second.OpenConnection().Dispose(), TestContext.Current.CancellationToken);
            var throughSecondAsync = Task.Run(async () => await (await second.OpenConnectionAsync(TestContext.Current.CancellationToken)).DisposeAsync(),
                TestContext.Current.CancellationToken);
            var throughDatabase = Task.Run(() =>
            {
                using var conn = db.CreateConnection();
                conn.Open();
            }, TestContext.Current.CancellationToken);

            var all = Task.WhenAll(throughSecond, throughSecondAsync, throughDatabase);
            (await Task.WhenAny(all, Task.Delay(300, TestContext.Current.CancellationToken))).ShouldNotBe(all);
            return all;
        });
    }

    [Fact]
    public async Task a_different_connection_string_does_not_wait_on_the_gate()
    {
        var cs = uniqueConnectionString();
        var other = uniqueConnectionString();
        using var dataSource = new SqliteDataSource(other);

        try
        {
            await whileGateIsHeld(cs, async () =>
            {
                var open = Task.Run(() => dataSource.OpenConnection().Dispose(), TestContext.Current.CancellationToken);
                (await Task.WhenAny(open, Task.Delay(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken))).ShouldBe(open);
                return open;
            });
        }
        finally
        {
            cleanUp(other);
        }
    }

    [Fact]
    public async Task clear_pool_takes_the_gate()
    {
        var cs = uniqueConnectionString();
        using var dataSource = new SqliteDataSource(cs);
        using (dataSource.OpenConnection())
        {
        }

        await whileGateIsHeld(cs, async () =>
        {
            var clear = Task.Run(() => SqliteDataSource.ClearPool(cs), TestContext.Current.CancellationToken);
            var clearAll = Task.Run(SqliteDataSource.ClearAllPools, TestContext.Current.CancellationToken);

            var both = Task.WhenAll(clear, clearAll);
            (await Task.WhenAny(both, Task.Delay(300, TestContext.Current.CancellationToken))).ShouldNotBe(both);
            return both;
        });
    }

    [Fact]
    public async Task an_open_connection_still_works_after_clear_pool()
    {
        var cs = uniqueConnectionString();
        using var dataSource = new SqliteDataSource(cs);

        await using var conn = await dataSource.OpenConnectionAsync(TestContext.Current.CancellationToken);
        SqliteDataSource.ClearPool(cs);

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "select 1";
        (await cmd.ExecuteScalarAsync(TestContext.Current.CancellationToken)).ShouldBe(1L);

        await conn.DisposeAsync();
        cleanUp(cs);
    }

    [Fact]
    public async Task open_async_honours_a_cancelled_token_before_the_checkout()
    {
        var cs = uniqueConnectionString();
        using var dataSource = new SqliteDataSource(cs);
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        await Should.ThrowAsync<OperationCanceledException>(async () =>
            await dataSource.OpenConnectionAsync(cts.Token));

        await using var created = (SqliteConnection)dataSource.CreateConnection();
        await Should.ThrowAsync<OperationCanceledException>(() => created.OpenAsync(cts.Token));
        created.State.ShouldBe(System.Data.ConnectionState.Closed);

        cleanUp(cs);
    }

    [Fact]
    public void a_gated_connection_follows_a_replaced_connection_string()
    {
        var first = uniqueConnectionString();
        var second = uniqueConnectionString();
        using var dataSource = new SqliteDataSource(first);

        using var conn = (SqliteConnection)dataSource.CreateConnection();
        conn.ConnectionString = second;
        conn.Open();
        // DataSource reports the resolved path (/private/var on macOS), so compare file names.
        Path.GetFileName(conn.DataSource).ShouldBe(Path.GetFileName(new SqliteConnectionStringBuilder(second).DataSource));
        conn.Close();

        cleanUp(first);
        cleanUp(second);
    }

    /// <summary>
    ///     Holds <paramref name="connectionString" />'s gate on a dedicated thread (a <see cref="Lock" /> must
    ///     be released by the thread that took it) while <paramref name="whileHeld" /> runs, then releases it
    ///     and waits for the task <paramref name="whileHeld" /> returned.
    /// </summary>
    private static async Task whileGateIsHeld(string connectionString, Func<Task<Task>> whileHeld)
    {
        var gate = SqlitePoolGate.For(connectionString);
        using var entered = new ManualResetEventSlim();
        using var release = new ManualResetEventSlim();

        var holder = new Thread(() =>
        {
            lock (gate)
            {
                entered.Set();
                release.Wait();
            }
        }) { IsBackground = true };
        holder.Start();
        entered.Wait(TestContext.Current.CancellationToken);

        Task pending;
        try
        {
            pending = await whileHeld();
        }
        finally
        {
            release.Set();
            holder.Join();
        }

        await pending.WaitAsync(TimeSpan.FromSeconds(30), TestContext.Current.CancellationToken);
        cleanUp(connectionString);
    }

    /// <summary>
    ///     Opens and closes connections from <see cref="Threads" /> threads at once, moving to a fresh pool
    ///     every <see cref="OpensPerThreadPerPool" /> opens per thread until <see cref="Budget" /> is spent,
    ///     and records every open connection's native handle while it is open. Returns how many times an
    ///     open connection got a handle another open connection already held.
    /// </summary>
    private static int countSharedHandles(Func<string, object> sourceFor, Func<object, SqliteConnection> open)
    {
        string? currentConnectionString = null;
        object? current = null;
        var pools = 0;

        var held = new ConcurrentDictionary<IntPtr, byte>();
        var shared = 0;
        var opens = 0;
        Exception? failure = null;
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();

        // Runs on one thread between phases, while every other thread waits with no connection open: retire
        // the last pool (a pool keeps its file handles open, so a few hundred of them exhaust descriptors),
        // then hand out the next one, or null to stop.
        void nextPool(Barrier _)
        {
            retire();

            if (stopwatch.Elapsed > Budget || Volatile.Read(ref failure) != null)
            {
                Volatile.Write(ref current, null);
                return;
            }

            currentConnectionString = uniqueConnectionString();
            pools++;
            Volatile.Write(ref current, sourceFor(currentConnectionString));
        }

        void retire()
        {
            (current as IDisposable)?.Dispose();
            if (currentConnectionString != null) cleanUp(currentConnectionString);
            currentConnectionString = null;
        }

        using var barrier = new Barrier(Threads, nextPool);
        var threads = Enumerable.Range(0, Threads).Select(_ => new Thread(() =>
        {
            while (true)
            {
                barrier.SignalAndWait();
                var source = Volatile.Read(ref current);
                if (source == null) return;

                try
                {
                    for (var i = 0; i < OpensPerThreadPerPool; i++)
                    {
                        using var conn = open(source);
                        var handle = conn.Handle!.DangerousGetHandle();
                        var mine = held.TryAdd(handle, 0);
                        if (!mine) Interlocked.Increment(ref shared);
                        Interlocked.Increment(ref opens);

                        // Remove before closing: once Close returns the connection to the pool, the next
                        // opener may legitimately get this handle.
                        if (mine) held.TryRemove(handle, out byte _);
                    }
                }
                catch (Exception e)
                {
                    // Keep taking part in the barrier; nextPool stops the loop at the next phase.
                    Interlocked.CompareExchange(ref failure, e, null);
                }
            }
        }) { IsBackground = true }).ToArray();

        foreach (var thread in threads) thread.Start();
        foreach (var thread in threads) thread.Join();

        retire();

        TestContext.Current.TestOutputHelper?.WriteLine(
            $"{opens} opens over {pools} pools x {Threads} threads in {stopwatch.Elapsed.TotalSeconds:F1}s; " +
            $"an open connection got an in-use handle {shared} times");

        if (failure != null) throw new InvalidOperationException("An opener failed", failure);

        return shared;
    }

    private static string uniqueConnectionString()
        => new SqliteConnectionStringBuilder
        {
            DataSource = Path.Combine(Path.GetTempPath(), $"weasel-gate-{Guid.NewGuid():N}.db")
        }.ToString();

    private static void cleanUp(string connectionString)
    {
        SqliteDataSource.ClearPool(connectionString);
        var path = new SqliteConnectionStringBuilder(connectionString).DataSource;
        foreach (var file in new[] { path, path + "-wal", path + "-shm" })
        {
            try
            {
                if (File.Exists(file)) File.Delete(file);
            }
            catch (IOException)
            {
                // a temp file; not worth failing a test over
            }
        }
    }

    /// <summary>
    ///     No PRAGMAs, so the loop measures the pool checkout and nothing else.
    /// </summary>
    private sealed class NoPragmas: SqlitePragmaSettings
    {
        public override Task ApplyConnectionPragmasAsync(SqliteConnection connection, CancellationToken ct = default)
            => Task.CompletedTask;

        public override void ApplyConnectionPragmas(SqliteConnection connection)
        {
        }

        public override Task ApplyDatabasePragmasAsync(SqliteConnection connection, CancellationToken ct = default)
            => Task.CompletedTask;

        public override void ApplyDatabasePragmas(SqliteConnection connection)
        {
        }
    }
}
