using System.Data;
using System.Diagnostics;
using Npgsql;

namespace Weasel.Postgresql;

/// <summary>
///     One connection that <see cref="AdvisoryLock" /> holds advisory locks on: several session locks share one,
///     a transaction-scoped lock gets one of its own, inside a transaction that lives as long as the lock.
/// </summary>
/// <remarks>
///     <para>
///         Npgsql connections are not thread safe, so every command on this connection goes through one gate. That
///         includes the monitor, which <see cref="AdvisoryLockOptions.LockMonitoringEnabled" /> turns on: it parks
///         the connection in <see cref="NpgsqlConnection.WaitAsync(int, CancellationToken)" />, a read with nothing
///         sent, so that a dropped or terminated backend surfaces at once as an error on that read, instead of on the
///         next lock or unlock. After a minute of silence it sends <c>SELECT 1</c>, so a connection that died without
///         a word is noticed too. Anything else that needs the connection cancels the wait first rather than wait for
///         it (weasel#676). Npgsql cancels that wait on the client, without breaking the connection and without a
///         cancel request to the server, so it cannot touch the transaction a transactional lock lives in. Each wait is
///         one second, so a cancellation Npgsql loses costs at most that.
///     </para>
///     <para>
///         This replaces DistributedLock.Postgres (weasel#908), whose monitor waited the same way, and which stopped
///         working on Npgsql 11 because it read the removed <c>NpgsqlConnectionStringBuilder.Multiplexing</c>.
///     </para>
/// </remarks>
internal sealed class AdvisoryLockConnection: IAsyncDisposable
{
    // One wait at a time is short because Npgsql can lose a cancellation that lands just as a wait starts, and then
    // a lock or unlock would sit out the whole wait. The probe still only goes out after a minute of silence.
    private const int MonitorWaitMilliseconds = 1_000;
    private static readonly TimeSpan SilenceBeforeProbe = TimeSpan.FromMinutes(1);
    private static readonly TimeSpan BackOff = TimeSpan.FromSeconds(1);

    private readonly NpgsqlConnection _connection;
    private readonly NpgsqlTransaction? _transaction;
    private readonly SemaphoreSlim _gate = new(1, 1);
    private readonly CancellationTokenSource _lost = new();

    // Guards _wait, _waiting, _monitoring, _stopMonitoring and _disposed
    private readonly Lock _monitorLock = new();
    private CancellationTokenSource? _wait;
    private int _waiting;
    private CancellationTokenSource? _stopMonitoring;
    private Task? _monitoring;
    private bool _disposed;

    private AdvisoryLockConnection(NpgsqlConnection connection, NpgsqlTransaction? transaction)
    {
        _connection = connection;
        _transaction = transaction;
    }

    /// <summary>
    ///     How many locks <see cref="AdvisoryLock" /> has recorded on this connection. Read and written under
    ///     <see cref="AdvisoryLock" />'s own lock.
    /// </summary>
    internal int Held { get; set; }

    /// <summary>
    ///     How many acquires are using this connection right now. Read and written under <see cref="AdvisoryLock" />'s
    ///     own lock.
    /// </summary>
    internal int Users { get; set; }

    /// <summary>
    ///     Set once, under <see cref="AdvisoryLock" />'s own lock, by whoever closes this connection.
    /// </summary>
    internal bool Closing { get; set; }

    public bool IsTransactional => _transaction != null;

    /// <summary>
    ///     True once this connection is known to be gone. Every lock it held was released by the server with it.
    /// </summary>
    public bool IsLost => _lost.IsCancellationRequested;

    public static async Task<AdvisoryLockConnection> OpenAsync(NpgsqlDataSource dataSource, bool transactional,
        CancellationToken token)
    {
        var connection = await dataSource.OpenConnectionAsync(token).ConfigureAwait(false);
        try
        {
            var transaction = transactional
                ? await connection.BeginTransactionAsync(token).ConfigureAwait(false)
                : null;

            return new AdvisoryLockConnection(connection, transaction);
        }
        catch
        {
            await connection.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    /// <summary>
    ///     <c>pg_try_advisory_lock</c>, or <c>pg_try_advisory_xact_lock</c> on a transactional connection, on the
    ///     bigint key. Never waits for the lock.
    /// </summary>
    public Task<bool> TryLockAsync(long key, CancellationToken token)
    {
        return useAsync(async () =>
        {
            await using var cmd = _connection.CreateCommand();
            cmd.Transaction = _transaction;
            cmd.CommandText = _transaction == null
                ? "SELECT pg_catalog.pg_try_advisory_lock(@key)"
                : "SELECT pg_catalog.pg_try_advisory_xact_lock(@key)";
            cmd.Parameters.Add(new NpgsqlParameter<long>("key", key));

            return (bool)(await cmd.ExecuteScalarAsync(token).ConfigureAwait(false))!;
        }, token);
    }

    /// <summary>
    ///     <c>pg_advisory_unlock</c> on a session connection. A transaction-scoped lock is released by disposing its
    ///     connection instead.
    /// </summary>
    public Task UnlockAsync(long key)
    {
        return useAsync(async () =>
        {
            await using var cmd = _connection.CreateCommand();
            cmd.CommandText = "SELECT pg_catalog.pg_advisory_unlock(@key)";
            cmd.Parameters.Add(new NpgsqlParameter<long>("key", key));
            await cmd.ExecuteScalarAsync(CancellationToken.None).ConfigureAwait(false);
            return true;
        }, CancellationToken.None);
    }

    /// <summary>
    ///     Start watching the connection so that <see cref="IsLost" /> turns true as soon as it is lost. Idempotent.
    /// </summary>
    public void StartMonitoring()
    {
        lock (_monitorLock)
        {
            if (_disposed || _monitoring != null) return;

            _stopMonitoring = new CancellationTokenSource();
            var stop = _stopMonitoring.Token;
            _monitoring = Task.Run(() => monitorAsync(stop), CancellationToken.None);
        }
    }

    /// <summary>
    ///     Stop monitoring and close the connection, which releases whatever it still holds: a transaction-scoped
    ///     lock is rolled back with its transaction, and a session connection is cleared of its advisory locks before
    ///     it goes back to the pool.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        Task? monitoring;
        CancellationTokenSource? stopMonitoring;
        lock (_monitorLock)
        {
            if (_disposed) return;
            _disposed = true;

            monitoring = _monitoring;
            stopMonitoring = _stopMonitoring;
        }

        if (stopMonitoring != null)
        {
            cancelQuietly(stopMonitoring);

            // The monitor only waits for its own cancelled wait and never throws
            await monitoring!.ConfigureAwait(false);
            stopMonitoring.Dispose();
        }

        // Nothing else should be using the connection by now, but never run a command alongside one
        await _gate.WaitAsync(CancellationToken.None).ConfigureAwait(false);
        try
        {
            if (!IsLost)
            {
                if (_transaction != null)
                {
                    await _transaction.RollbackAsync(CancellationToken.None).ConfigureAwait(false);
                }
                else
                {
                    // Every lock should already be unlocked one by one. A pooled connection keeps any session lock
                    // it still has after Close, so make sure it goes back with none.
                    await using var cmd = _connection.CreateCommand();
                    cmd.CommandText = "SELECT pg_catalog.pg_advisory_unlock_all()";
                    await cmd.ExecuteNonQueryAsync(CancellationToken.None).ConfigureAwait(false);
                }
            }
        }
        catch (Exception) when (isBroken())
        {
            // Gone with the connection, and so is every lock it held
            markLost();
        }
        finally
        {
            if (_transaction != null)
            {
                await _transaction.DisposeAsync().ConfigureAwait(false);
            }

            await _connection.DisposeAsync().ConfigureAwait(false);
            markLost();
            _gate.Dispose();
        }
    }

    private async Task<T> useAsync<T>(Func<Task<T>> operation, CancellationToken token)
    {
        CancellationTokenSource? wait;
        lock (_monitorLock)
        {
            if (_disposed) throw new ObjectDisposedException(nameof(AdvisoryLockConnection));

            _waiting++;
            wait = _wait;
        }

        try
        {
            // Take the connection away from the monitor's wait rather than wait out its minute
            if (wait != null) cancelQuietly(wait);

            await _gate.WaitAsync(token).ConfigureAwait(false);
        }
        finally
        {
            lock (_monitorLock)
            {
                _waiting--;
            }
        }

        try
        {
            return await operation().ConfigureAwait(false);
        }
        catch (Exception) when (isBroken())
        {
            markLost();
            throw;
        }
        finally
        {
            _gate.Release();
        }
    }

    private async Task monitorAsync(CancellationToken stop)
    {
        var lastHeard = Stopwatch.StartNew();

        while (!stop.IsCancellationRequested && !IsLost)
        {
            try
            {
                await _gate.WaitAsync(stop).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                return;
            }

            CancellationTokenSource? wait = null;
            var failed = false;
            try
            {
                lock (_monitorLock)
                {
                    // Someone is waiting for the connection: let them have it first
                    if (_waiting == 0 && !stop.IsCancellationRequested)
                    {
                        wait = CancellationTokenSource.CreateLinkedTokenSource(stop);
                        _wait = wait;
                    }
                }

                if (wait != null)
                {
                    // WaitAsync is true only for a notification, which nothing here listens for. A minute of silence
                    // gets a round trip, for a connection that died without the server saying so.
                    await _connection.WaitAsync(MonitorWaitMilliseconds, wait.Token).ConfigureAwait(false);

                    if (lastHeard.Elapsed >= SilenceBeforeProbe)
                    {
                        await executeAsync("SELECT 1", CancellationToken.None).ConfigureAwait(false);
                        lastHeard.Restart();
                    }
                }
            }
            catch (OperationCanceledException) when (wait is { IsCancellationRequested: true } && !isBroken())
            {
                // Cut short for a lock, an unlock or disposal
            }
            catch (Exception)
            {
                if (isBroken())
                {
                    // Gone, and every lock it held with it
                    markLost();
                }
                else
                {
                    failed = true;
                }
            }
            finally
            {
                lock (_monitorLock)
                {
                    _wait = null;
                }

                wait?.Dispose();
                _gate.Release();
            }

            if (failed)
            {
                // Not lost, but not watchable this round either: don't spin on it
                if (!await delayAsync(BackOff, stop).ConfigureAwait(false)) return;
            }
            else if (wait == null)
            {
                // Hand the gate to the waiter before taking it again
                await Task.Yield();
            }
        }
    }

    private static async Task<bool> delayAsync(TimeSpan delay, CancellationToken stop)
    {
        try
        {
            await Task.Delay(delay, stop).ConfigureAwait(false);
            return true;
        }
        catch (OperationCanceledException)
        {
            return false;
        }
    }

    private async Task executeAsync(string sql, CancellationToken token)
    {
        await using var cmd = _connection.CreateCommand();
        cmd.Transaction = _transaction;
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync(token).ConfigureAwait(false);
    }

    private bool isBroken()
    {
        return _connection.State != ConnectionState.Open
               || (_connection.FullState & ConnectionState.Broken) == ConnectionState.Broken;
    }

    private void markLost()
    {
        cancelQuietly(_lost);
    }

    private static void cancelQuietly(CancellationTokenSource source)
    {
        try
        {
            source.Cancel();
        }
        catch (ObjectDisposedException)
        {
            // The wait it would have cut short has already finished
        }
    }
}
