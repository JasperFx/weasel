using System.Data;
using JasperFx.Core;
using JasperFx.Events.Daemon;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;

namespace Weasel.SqlServer;

/// <summary>
///     SQL Server implementation of <see cref="IAdvisoryLock" />. The contract was
///     originally a duplicate in <c>Weasel.Core.IAdvisoryLock</c> (byte-identical
///     to the upstream JasperFx.Events one); it was lifted into
///     <c>JasperFx.Events.Daemon</c> in jasperfx alpha.19 / PR #319 so the daemon
///     contracts have a single canonical home, and Weasel's duplicate was removed
///     in weasel#284. Existing consumers should update their <c>using</c>
///     statement from <c>Weasel.Core</c> to <c>JasperFx.Events.Daemon</c>.
/// </summary>
public class AdvisoryLock : IAdvisoryLock
{
    private readonly Func<SqlConnection> _source;
    private readonly ILogger _logger;
    private readonly string _databaseName;
    private SqlConnection _conn;
    private readonly List<int> _locks = new();
    private volatile bool _disposed;

    public AdvisoryLock(Func<SqlConnection> source, ILogger logger, string databaseName)
    {
        _source = source;
        _logger = logger;
        _databaseName = databaseName;
    }

    public bool HasLock(int lockId)
    {
        return _conn is not { State: ConnectionState.Closed } && _locks.Contains(lockId);
    }

    public async Task<bool> TryAttainLockAsync(int lockId, CancellationToken token)
    {
        // weasel#349: never start a new acquire once disposal has begun. During host shutdown opening _conn
        // races with disposal and can abort with a process-killing ObjectDisposedException — mirror the
        // Postgres guard for Polecat parity.
        if (_disposed) return false;

        try
        {
            if (_conn == null)
            {
                _conn = _source();
                await _conn.OpenAsync(token).ConfigureAwait(false);
            }

            if (_conn.State == ConnectionState.Closed)
            {
                try
                {
                    await _conn.DisposeAsync().ConfigureAwait(false);
                }
                catch (Exception e)
                {
                    _logger.LogError(e, "Error trying to clean up and restart an advisory lock connection");
                }
                finally
                {
                    _conn = null;
                }

                return false;
            }



            var attained = await _conn.TryGetGlobalLock(lockId.ToString(), cancellation: token).ConfigureAwait(false);
            if (attained)
            {
                _locks.Add(lockId);
                return true;
            }

            return false;
        }
        catch (ObjectDisposedException)
        {
            // The connection was disposed out from under an in-flight open/acquire during shutdown. Treat as
            // "lock not attained" rather than letting a process-aborting exception escape.
            return false;
        }
        catch (Exception e) when (_disposed && e is SqlException or InvalidOperationException)
        {
            // Same shutdown race, surfaced as a disposed-connection SqlException / InvalidOperationException.
            return false;
        }
    }

    public async Task ReleaseLockAsync(int lockId)
    {
        if (!_locks.Contains(lockId)) return;

        if (_conn == null || _conn.State == ConnectionState.Closed)
        {
            _locks.Remove(lockId);
            return;
        }

        var cancellation = new CancellationTokenSource();
        cancellation.CancelAfter(1.Seconds());

        await _conn.ReleaseGlobalLock(lockId.ToString(), cancellation: cancellation.Token).ConfigureAwait(false);
        _locks.Remove(lockId);

        if (!_locks.Any())
        {
            await _conn.CloseAsync().ConfigureAwait(false);
            await _conn.DisposeAsync().ConfigureAwait(false);
            _conn = null;
        }
    }

    public async ValueTask DisposeAsync()
    {
        // Set first, before disposing the connection, so any concurrent TryAttainLockAsync short-circuits (weasel#349).
        _disposed = true;

        if (_conn == null) return;

        try
        {
            foreach (var i in _locks)
            {
                await _conn.ReleaseGlobalLock(i.ToString(), CancellationToken.None).ConfigureAwait(false);
            }

            await _conn.CloseAsync().ConfigureAwait(false);
            await _conn.DisposeAsync().ConfigureAwait(false);
        }
        catch (Exception e)
        {
            _logger.LogError(e, "Error trying to dispose of advisory locks for database {Identifier}",
                _databaseName);
        }
        finally
        {
            await _conn.DisposeAsync().ConfigureAwait(false);
        }
    }
}
