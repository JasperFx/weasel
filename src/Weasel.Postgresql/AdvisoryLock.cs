using JasperFx.Core;
using JasperFx.Events.Daemon;
using Medallion.Threading.Postgres;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace Weasel.Postgresql;

public sealed class AdvisoryLockOptions
{
    /// <summary>
    ///     When true, <see cref="AdvisoryLock.HasLock" /> reports false once the connection holding the lock is lost.
    /// </summary>
    public bool LockMonitoringEnabled { get; set; }

    /// <summary>
    ///     When true, each lock is held by a transaction on its own connection. When false (the default), locks are
    ///     session-scoped and multiplexed so that several held locks share one connection.
    /// </summary>
    public bool TransactionalLockEnabled { get; set; }
}

/// <summary>
///     PostgreSQL implementation of <see cref="IAdvisoryLock" />.
/// </summary>
public class AdvisoryLock : IAdvisoryLock
{
    private readonly string _databaseName;
    private readonly AdvisoryLockOptions _options;
    private readonly ILogger _logger;

    // Guards _handles and _disposed. Acquire, release and disposal can run concurrently, and storing a newly
    // acquired handle must be atomic with disposal (see TryAttainLockAsync).
    private readonly object _handlesLock = new();
    private readonly Dictionary<int, PostgresDistributedLockHandle> _handles = new();
    private readonly LightweightCache<int, PostgresDistributedLock> _distributedLockProviders;
    private bool _disposed;

    public AdvisoryLock(NpgsqlDataSource dataSource, ILogger logger, string databaseName, AdvisoryLockOptions options)
    {
        _logger = logger;

        _distributedLockProviders = new LightweightCache<int, PostgresDistributedLock>(
            (lockId => new PostgresDistributedLock(new PostgresAdvisoryLockKey(lockId),
                EnsurePrimaryWhenMultiHost(dataSource), builder =>
                {
                    // Multiplexing can't be combined with transaction-scoped locks
                    if (options.TransactionalLockEnabled)
                    {
                        builder.UseTransaction();
                    }
                    else
                    {
                        builder.UseMultiplexing();
                    }
                })));
        _databaseName = databaseName;
        _options = options;
    }

    private bool IsDisposed
    {
        get
        {
            lock (_handlesLock)
            {
                return _disposed;
            }
        }
    }

    private static NpgsqlDataSource EnsurePrimaryWhenMultiHost(NpgsqlDataSource source)
    {
        if (source is NpgsqlMultiHostDataSource multiHostDataSource)
            return multiHostDataSource.WithTargetSession(TargetSessionAttributes.ReadWrite);

        return source;
    }

    public bool HasLock(int lockId)
    {
        PostgresDistributedLockHandle? handle;
        lock (_handlesLock)
        {
            if (!_handles.TryGetValue(lockId, out handle))
            {
                return false;
            }
        }

        if (_options.LockMonitoringEnabled)
        {
            return !handle.HandleLostToken.IsCancellationRequested;
        }

        return true;
    }

    /// <summary>
    ///     Attempt to attain the advisory lock with the given identifier.
    /// </summary>
    /// <returns>True when the lock was attained by this node, false when it is held elsewhere.</returns>
    /// <exception cref="ObjectDisposedException">
    ///     Thrown when the underlying <see cref="NpgsqlDataSource" /> has already been disposed. This is terminal:
    ///     the lock latches itself disposed, and every later call returns false without touching the dead pool.
    /// </exception>
    public async Task<bool> TryAttainLockAsync(int lockId, CancellationToken token)
    {
        // Never start an acquire once disposal has begun: the data source may be shutting down with the host
        if (IsDisposed) return false;

        try
        {
            var locker = _distributedLockProviders[lockId];
            var handle = await locker.TryAcquireAsync(cancellationToken: token).ConfigureAwait(false);
            if (handle is null) return false;

            // DisposeAsync can drain _handles while this acquire is in flight. Storing under the same lock means
            // either the drain disposes this handle, or this call sees the disposal and disposes it itself, so a
            // granted lock is never left held for the life of the process.
            PostgresDistributedLockHandle? orphaned = null;
            var stored = false;

            lock (_handlesLock)
            {
                if (_disposed)
                {
                    orphaned = handle;
                }
                else
                {
                    // An existing handle here is one whose connection was lost, so dispose it too
                    _handles.Remove(lockId, out orphaned);
                    _handles[lockId] = handle;
                    stored = true;
                }
            }

            if (orphaned is not null)
            {
                await disposeHandleSafelyAsync(orphaned).ConfigureAwait(false);
            }

            return stored;
        }
        catch (ObjectDisposedException)
        {
            // A disposed data source never comes back. Latch disposed so later polls return false without touching
            // the dead pool, and rethrow so the projection coordinator can end its leadership loop.
            lock (_handlesLock)
            {
                _disposed = true;
            }

            throw;
        }
        catch (Exception e) when (IsDisposed && e is NpgsqlException or InvalidOperationException)
        {
            // The data source was disposed during the acquire and surfaced as a different exception type
            return false;
        }
    }

    public async Task ReleaseLockAsync(int lockId)
    {
        PostgresDistributedLockHandle? handle;
        lock (_handlesLock)
        {
            _handles.Remove(lockId, out handle);
        }

        if (handle is not null)
        {
            await handle.DisposeAsync().ConfigureAwait(false);
        }
    }

    public async ValueTask DisposeAsync()
    {
        PostgresDistributedLockHandle[] handles;

        lock (_handlesLock)
        {
            // Latch and drain atomically so no handle can be stored after this point (see TryAttainLockAsync)
            _disposed = true;
            handles = _handles.Values.ToArray();
            _handles.Clear();
        }

        foreach (var handle in handles)
        {
            await disposeHandleSafelyAsync(handle).ConfigureAwait(false);
        }
    }

    private async Task disposeHandleSafelyAsync(PostgresDistributedLockHandle handle)
    {
        try
        {
            await handle.DisposeAsync().ConfigureAwait(false);
        }
        catch (InvalidOperationException)
        {
            // The connection or data source is already closed (ObjectDisposedException derives from this)
        }
        catch (Exception e)
        {
            _logger.LogError(e, "Error trying to dispose of advisory locks for database {Identifier}", _databaseName);
        }
    }
}
