using JasperFx.Core;
using JasperFx.Events.Daemon;
using Medallion.Threading.Postgres;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace Weasel.Postgresql;

public sealed class AdvisoryLockOptions
{
    public bool LockMonitoringEnabled { get; set; }

    public bool TransactionalLockEnabled { get; set; }
}


/// <summary>
///     PostgreSQL implementation of <see cref="IAdvisoryLock" />. The contract was
///     originally a duplicate in <c>Weasel.Core.IAdvisoryLock</c> (byte-identical
///     to the upstream JasperFx.Events one); it was lifted into
///     <c>JasperFx.Events.Daemon</c> in jasperfx alpha.19 / PR #319 so the daemon
///     contracts have a single canonical home, and Weasel's duplicate was removed
///     in weasel#284. Existing consumers should update their <c>using</c>
///     statement from <c>Weasel.Core</c> to <c>JasperFx.Events.Daemon</c>.
/// </summary>
public class AdvisoryLock : IAdvisoryLock
{
    private readonly string _databaseName;
    private readonly AdvisoryLockOptions _options;
    private readonly ILogger _logger;

    // weasel#396: every read and write of _handles — and every read and write of _disposed that has to
    // agree with them — goes through this lock. The dictionary is touched by the caller's leadership
    // poll, by ReleaseLockAsync, and by DisposeAsync, which can run concurrently; and disposal has to
    // be atomic with respect to storing a freshly acquired handle (see TryAttainLockAsync).
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
                    builder.UseTransaction(options.TransactionalLockEnabled);
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
        // weasel#349: never start a new acquire once disposal has begun. On a HotCold cold/standby node the
        // coordinator polls this on a cadence, and during host shutdown the owned NpgsqlDataSource races with
        // disposal — an in-flight OpenAsync aborts with ObjectDisposedException: 'Npgsql.PoolingDataSource'.
        if (IsDisposed) return false;

        try
        {
            var locker = _distributedLockProviders[lockId];
            var handle = await locker.TryAcquireAsync(cancellationToken: token).ConfigureAwait(false);
            if (handle is null) return false;

            // weasel#396: the entry check above is not enough on its own. DisposeAsync can drain
            // _handles while this acquire is in flight, and the handle would then be stored into a
            // dictionary nothing will ever dispose — a granted advisory lock held for the life of the
            // process. With transaction-scoped locks that is a permanent 'idle in transaction' backend
            // on pg_try_advisory_xact_lock, which Marten's high-water gap detection reads as a live
            // pre-gap reserver and never advances past (marten#5090). Storing under the same lock that
            // DisposeAsync drains under makes the two orderings exhaustive: either the handle lands
            // before the drain and the drain disposes it, or it observes the disposal and disposes
            // itself here.
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
                    // A handle already sitting in this slot is one whose lock we lost (monitored mode)
                    // and re-attained; it is displaced, not released, so it has to be disposed too.
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
            // weasel#353 / marten#4915. The data source was disposed out from under an in-flight acquire. That
            // state is terminal — a disposed NpgsqlDataSource never comes back — so do two things:
            //
            //  1. Latch. Any later poll short-circuits above instead of re-opening against the dead pool. #349
            //     swallowed this and returned false, which the HotCold coordinator reads as "lock held elsewhere",
            //     so it re-polled on its LeadershipPollingTime cadence for the life of the process.
            //  2. Rethrow. ProjectionCoordinatorBase.executeAsync (jasperfx#500) catches ObjectDisposedException
            //     and ends its leadership loop. Swallowing here made that catch unreachable, which is precisely
            //     the composition gap in marten#4915.
            //
            // Callers that would rather not see it can check HasLock, or simply poll again — the latch guarantees
            // the second call returns false quietly.
            lock (_handlesLock)
            {
                _disposed = true;
            }

            throw;
        }
        catch (Exception e) when (IsDisposed && e is NpgsqlException or InvalidOperationException)
        {
            // Same shutdown race, surfaced as a disposed-pool NpgsqlException / InvalidOperationException.
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
            // Latch and drain atomically (weasel#349 for the latch, weasel#396 for the drain): a
            // concurrent TryAttainLockAsync either got its handle into _handles before this snapshot —
            // in which case it is disposed below — or it will see _disposed set when it tries to store
            // and dispose the handle itself. Nothing can land in the dictionary after this point.
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
            // Underlying connection is already closed and there's nothing to dispose. ObjectDisposedException
            // derives from this, so a data source that went first lands here too — nothing worth logging.
        }
        catch (Exception e)
        {
            _logger.LogError(e, "Error trying to dispose of advisory locks for database {Identifier}", _databaseName);
        }
    }
}
