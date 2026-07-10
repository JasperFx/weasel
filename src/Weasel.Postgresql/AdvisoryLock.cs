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
    private readonly Dictionary<int, PostgresDistributedLockHandle> _handles = new();
    private readonly LightweightCache<int, PostgresDistributedLock> _distributedLockProviders;
    private volatile bool _disposed;

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

    private static NpgsqlDataSource EnsurePrimaryWhenMultiHost(NpgsqlDataSource source)
    {
        if (source is NpgsqlMultiHostDataSource multiHostDataSource)
            return multiHostDataSource.WithTargetSession(TargetSessionAttributes.ReadWrite);

        return source;
    }

    public bool HasLock(int lockId)
    {
        var lockState = _handles.TryGetValue(lockId, out var handle);
        if (lockState && _options.LockMonitoringEnabled)
        {
            return !handle!.HandleLostToken.IsCancellationRequested;
        }

        return lockState;
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
        if (_disposed) return false;

        try
        {
            var locker = _distributedLockProviders[lockId];
            var handle = await locker.TryAcquireAsync(cancellationToken: token).ConfigureAwait(false);
            if (handle is not null)
            {
                _handles[lockId] = handle;
                return true;
            }
            return false;
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
            _disposed = true;
            throw;
        }
        catch (Exception e) when (_disposed && e is NpgsqlException or InvalidOperationException)
        {
            // Same shutdown race, surfaced as a disposed-pool NpgsqlException / InvalidOperationException.
            return false;
        }
    }

    public async Task ReleaseLockAsync(int lockId)
    {
        if (_handles.Remove(lockId, out var handle))
        {
            await handle.DisposeAsync().ConfigureAwait(false);
        }
    }

    public async ValueTask DisposeAsync()
    {
        // Set first, before disposing handles, so any concurrent TryAttainLockAsync short-circuits (weasel#349).
        _disposed = true;

        foreach (var i in _handles.Keys)
        {
            if (_handles.Remove(i, out var handle))
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
    }
}
