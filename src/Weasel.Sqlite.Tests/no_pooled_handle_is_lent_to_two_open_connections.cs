using System.Collections.Concurrent;
using Microsoft.Data.Sqlite;
using Shouldly;
using Weasel.Core;
using Xunit;

namespace Weasel.Sqlite.Tests;

/// <summary>
///     Microsoft.Data.Sqlite 10.0.12 and earlier can lend one native handle to two open connections when
///     opens of one pool race: <c>SqliteConnectionInternal.Activate</c> published <c>_active</c> before its
///     owner, outside the pool lock, and a concurrent <c>ReclaimLeakedConnections</c> could reclaim a
///     connection still being opened (dotnet/efcore#39008, fixed in 10.0.13 by #39012). Weasel pins a fixed
///     version rather than working around it (JasperFx/weasel#857, fisher#576, fisher#582). These tests are
///     the regression guard for that pin: one handle, two open owners, is how it fails.
/// </summary>
public class no_pooled_handle_is_lent_to_two_open_connections
{
    // The stand-alone reproduction in weasel#857 used 12 threads over 200 fresh pools. The race lives in a
    // pool that is still growing, so the loop keeps moving to fresh pools rather than hammering one warm
    // one. On Microsoft.Data.Sqlite 10.0.9, on an 18-core Apple silicon machine, these settings failed 37
    // of 40 runs of the four data-source tests on net10.0 and 19 of 20 on net11.0 (weasel#859).
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
    public void no_handle_is_held_by_two_open_connections_that_application_code_opens_itself()
    {
        // Connections Weasel never sees, which no lock inside Weasel could have covered. The pinned
        // package fixes them too.
        var shared = countSharedHandles(
            cs => cs,
            cs =>
            {
                var conn = new SqliteConnection((string)cs);
                conn.Open();
                return conn;
            });

        shared.ShouldBe(0);
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
        using (var forPool = new SqliteConnection(connectionString))
        {
            SqliteConnection.ClearPool(forPool);
        }
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
