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
            // The data source / connection pool was disposed out from under an in-flight acquire during
            // shutdown. Treat as "lock not attained" rather than letting a process-aborting exception escape.
            return false;
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
                    // Underlying connection is already closed and there's nothing to dispose
                }
                catch (Exception e)
                {
                    _logger.LogError(e, "Error trying to dispose of advisory locks for database {Identifier}", _databaseName);
                }
            }
        }
    }
}
