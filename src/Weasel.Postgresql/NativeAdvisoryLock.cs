using System.Data;
using JasperFx.Core;
using Microsoft.Extensions.Logging;
using Npgsql;
using Weasel.Core;
using Weasel.Core.Migrations;

namespace Weasel.Postgresql;

public class NativeAdvisoryLock: IAdvisoryLock
{
    private readonly NpgsqlDataSource _dataSource;
    private readonly string _databaseName;
    private readonly ILogger _logger;
    private readonly HashSet<int> _locks = new();
    private NpgsqlConnection? _conn;

    public NativeAdvisoryLock(NpgsqlDataSource dataSource, ILogger logger, string databaseName)
    {
        _dataSource = EnsurePrimaryWhenMultiHost(dataSource);
        _logger = logger;
        _databaseName = databaseName;
    }

    private static NpgsqlDataSource EnsurePrimaryWhenMultiHost(NpgsqlDataSource source)
    {
        if (source is NpgsqlMultiHostDataSource multiHostDataSource)
            return multiHostDataSource.WithTargetSession(TargetSessionAttributes.ReadWrite);

        return source;
    }

    public bool HasLock(int lockId)
    {
        return _conn is { State: ConnectionState.Open } && _locks.Contains(lockId);
    }

    public async Task<bool> TryAttainLockAsync(int lockId, CancellationToken token)
    {
        _conn ??= await _dataSource.OpenConnectionAsync(token).ConfigureAwait(false);

        if (_conn.State != ConnectionState.Open)
        {
            await _conn.DisposeAsync().ConfigureAwait(false);
            _conn = null;
            return false;
        }

        var attained = await _conn.TryGetGlobalLock(lockId, cancellation: token).ConfigureAwait(false);
        if (attained == AttainLockResult.Success)
        {
            _locks.Add(lockId);
            return true;
        }

        return false;
    }

    public async Task ReleaseLockAsync(int lockId)
    {
        if (!_locks.Remove(lockId)) return;

        if (_conn is not { State: ConnectionState.Open }) return;

        using var cancellation = new CancellationTokenSource(1.Seconds());

        await _conn.ReleaseGlobalLock(lockId, cancellation: cancellation.Token).ConfigureAwait(false);

        if (_locks.Count == 0)
        {
            await _conn.CloseAsync().ConfigureAwait(false);
            await _conn.DisposeAsync().ConfigureAwait(false);
            _conn = null;
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_conn is null) return;

        try
        {
            foreach (var lockId in _locks)
            {
                await _conn.ReleaseGlobalLock(lockId, CancellationToken.None).ConfigureAwait(false);
            }

            await _conn.CloseAsync().ConfigureAwait(false);
        }
        catch (Exception e)
        {
            _logger.LogError(e, "Error trying to dispose of advisory locks for database {Identifier}", _databaseName);
        }
        finally
        {
            await _conn.DisposeAsync().ConfigureAwait(false);
            _conn = null;
        }
    }
}
