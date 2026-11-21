using JasperFx.Core;
using JasperFx.Events.Daemon;
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
    ///     session-scoped and several held locks share one connection.
    /// </summary>
    public bool TransactionalLockEnabled { get; set; }

    /// <summary>
    ///     How long <see cref="AdvisoryLock.DisposeAsync" /> will wait for the held locks to be released before it
    ///     gives up waiting and leaves the releases running in the background. See the remarks on
    ///     <see cref="AdvisoryLock.DisposeAsync" /> for why a shutdown release needs a bound at all (weasel#676).
    ///     <para>
    ///     A non-positive value or <see cref="Timeout.InfiniteTimeSpan" /> opts out of the bound and waits
    ///     for every release, matching how JasperFx reads <c>DaemonSettings.StopAndDrainTimeout</c>, which is
    ///     what a daemon host would feed this from.
    ///     </para>
    /// </summary>
    public TimeSpan ReleaseTimeout { get; set; } = 5.Seconds();
}

/// <summary>
///     PostgreSQL implementation of <see cref="IAdvisoryLock" />.
/// </summary>
/// <remarks>
///     <para>
///         Takes PostgreSQL advisory locks on the bigint key <c>(long)lockId</c>, on connections it opens from the
///         data source and keeps open while it holds a lock there. Session locks share a connection: a lock goes on
///         the first open connection that does not already hold that id, and a new one is opened only when every
///         connection does (advisory locks are reentrant within a session, so two holds of one id on one session would
///         both succeed). A connection goes back to the pool once it holds nothing. A transactional lock is taken with
///         <c>pg_try_advisory_xact_lock</c> in a transaction of its own, and released by rolling that back.
///     </para>
///     <para>
///         Until weasel#910 (the 9.x port of weasel#908) this sat on DistributedLock.Postgres, which does not
///         work on Npgsql 11. The behaviour is the same, connection loss included: with
///         <see cref="AdvisoryLockOptions.LockMonitoringEnabled" /> on, <see cref="HasLock" /> turns false as soon as
///         the connection holding the lock is lost (see <see cref="AdvisoryLockConnection" />), and the lock can then
///         be taken again on a new connection.
///     </para>
/// </remarks>
public class AdvisoryLock : IAdvisoryLock
{
    private readonly string _databaseName;
    private readonly AdvisoryLockOptions _options;
    private readonly ILogger _logger;
    private readonly NpgsqlDataSource _dataSource;

    // Guards _handles, _sessionConnections, the connections' Held/Users/Closing counts and _disposed. Acquire,
    // release and disposal can run concurrently, and storing a newly acquired lock must be atomic with disposal
    // (see TryAttainLockAsync).
    private readonly Lock _handlesLock = new();
    private readonly Dictionary<int, AdvisoryLockConnection> _handles = new();
    private readonly List<AdvisoryLockConnection> _sessionConnections = new();
    private bool _disposed;

    public AdvisoryLock(NpgsqlDataSource dataSource, ILogger logger, string databaseName, AdvisoryLockOptions options)
    {
        _logger = logger;
        _dataSource = EnsurePrimaryWhenMultiHost(dataSource);
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
        AdvisoryLockConnection? connection;
        lock (_handlesLock)
        {
            if (!_handles.TryGetValue(lockId, out connection))
            {
                return false;
            }
        }

        if (_options.LockMonitoringEnabled)
        {
            return !connection.IsLost;
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
            return _options.TransactionalLockEnabled
                ? await tryAttainTransactionalLockAsync(lockId, token).ConfigureAwait(false)
                : await tryAttainSessionLockAsync(lockId, token).ConfigureAwait(false);
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

    private async Task<bool> tryAttainTransactionalLockAsync(int lockId, CancellationToken token)
    {
        var connection = await AdvisoryLockConnection.OpenAsync(_dataSource, true, token).ConfigureAwait(false);

        bool attained;
        try
        {
            attained = await connection.TryLockAsync(lockId, token).ConfigureAwait(false);
        }
        catch
        {
            await connection.DisposeAsync().ConfigureAwait(false);
            throw;
        }

        if (!attained)
        {
            await connection.DisposeAsync().ConfigureAwait(false);
            return false;
        }

        // DisposeAsync can drain _handles while this acquire is in flight. Storing under the same lock means
        // either the drain releases this lock, or this call sees the disposal and releases it itself, so a
        // granted lock is never left held for the life of the process.
        AdvisoryLockConnection? orphaned = null;
        AdvisoryLockConnection? replaced = null;
        lock (_handlesLock)
        {
            if (_disposed)
            {
                orphaned = connection;
            }
            else
            {
                // An existing hold here is one whose connection was lost, or the server would have refused this one
                _handles.Remove(lockId, out replaced);
                _handles[lockId] = connection;
            }
        }

        if (orphaned is not null)
        {
            await disposeConnectionSafelyAsync(orphaned).ConfigureAwait(false);
            return false;
        }

        if (replaced is not null)
        {
            await disposeConnectionSafelyAsync(replaced).ConfigureAwait(false);
        }

        if (_options.LockMonitoringEnabled) connection.StartMonitoring();

        return true;
    }

    private async Task<bool> tryAttainSessionLockAsync(int lockId, CancellationToken token)
    {
        // One retry, for a shared connection that turns out to have died since its last use
        for (var attempt = 0; ; attempt++)
        {
            var connection = await claimSessionConnectionAsync(lockId, token).ConfigureAwait(false);
            if (connection is null) return false;

            bool attained;
            try
            {
                attained = await connection.TryLockAsync(lockId, token).ConfigureAwait(false);
            }
            catch (Exception) when (connection.IsLost && attempt == 0 && !IsDisposed)
            {
                await unclaimSessionConnectionAsync(connection).ConfigureAwait(false);
                continue;
            }
            catch
            {
                await unclaimSessionConnectionAsync(connection).ConfigureAwait(false);
                throw;
            }

            if (!attained)
            {
                await unclaimSessionConnectionAsync(connection).ConfigureAwait(false);
                return false;
            }

            var stored = false;
            var duplicate = false;
            AdvisoryLockConnection? replaced = null;
            lock (_handlesLock)
            {
                if (_disposed)
                {
                    // Released below, see tryAttainTransactionalLockAsync
                }
                else if (_handles.TryGetValue(lockId, out var existing) && ReferenceEquals(existing, connection))
                {
                    // A concurrent acquire of the same id on this connection got there first, and the session
                    // granted this one again on top of it
                    duplicate = true;
                }
                else
                {
                    // An existing hold here is one whose connection was lost, or the server would have refused
                    // this one
                    if (_handles.Remove(lockId, out replaced))
                    {
                        replaced.Held--;

                        // Keeps it open until releaseSessionLockAsync is done with it
                        replaced.Users++;
                    }

                    _handles[lockId] = connection;
                    connection.Held++;
                    stored = true;
                }
            }

            if (!stored)
            {
                await unlockSafelyAsync(connection, lockId).ConfigureAwait(false);
            }

            await unclaimSessionConnectionAsync(connection).ConfigureAwait(false);

            if (replaced is not null)
            {
                await releaseSessionLockAsync(replaced, lockId, rethrow: false).ConfigureAwait(false);
            }

            if (stored && _options.LockMonitoringEnabled) connection.StartMonitoring();

            return stored && !duplicate;
        }
    }

    /// <summary>
    ///     The first open session connection that does not hold <paramref name="lockId" />, or a new one. Null once
    ///     this lock is disposed.
    /// </summary>
    private async Task<AdvisoryLockConnection?> claimSessionConnectionAsync(int lockId, CancellationToken token)
    {
        lock (_handlesLock)
        {
            if (_disposed) return null;

            _handles.TryGetValue(lockId, out var holder);
            foreach (var candidate in _sessionConnections)
            {
                if (candidate.IsLost || candidate.Closing || ReferenceEquals(candidate, holder)) continue;

                candidate.Users++;
                return candidate;
            }
        }

        var connection = await AdvisoryLockConnection.OpenAsync(_dataSource, false, token).ConfigureAwait(false);

        lock (_handlesLock)
        {
            if (!_disposed)
            {
                connection.Users++;
                _sessionConnections.Add(connection);
                return connection;
            }
        }

        await connection.DisposeAsync().ConfigureAwait(false);
        return null;
    }

    private Task unclaimSessionConnectionAsync(AdvisoryLockConnection connection)
    {
        lock (_handlesLock)
        {
            connection.Users--;
        }

        return closeSessionConnectionIfIdleAsync(connection);
    }

    /// <summary>
    ///     Close a session connection that holds nothing and that nothing is using, or one that is lost. Whoever
    ///     finds it idle last closes it, once.
    /// </summary>
    private async Task closeSessionConnectionIfIdleAsync(AdvisoryLockConnection connection)
    {
        lock (_handlesLock)
        {
            if (connection.Closing || connection.Users > 0) return;
            if (connection.Held > 0 && !connection.IsLost) return;

            connection.Closing = true;
            _sessionConnections.Remove(connection);
        }

        await disposeConnectionSafelyAsync(connection).ConfigureAwait(false);
    }

    /// <summary>
    ///     Who holds <paramref name="lockId" />, or null when nothing does.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         <see cref="HasLock" /> only answers "does this node", so without this no node — and no
    ///         monitoring tool — could learn which node owns a lock set it does not hold. That is the
    ///         diagnostic an operator needs when a projection agent stops with
    ///         <c>ProgressionProgressOutOfOrderException</c>, which means two processes believe they own
    ///         the same shard (weasel#650).
    ///     </para>
    ///     <para>
    ///         <c>pg_locks</c> reports the advisory key as two 32-bit halves of the 64-bit key the lock
    ///         takes for the id, so a negative id sign-extends into <c>classid = 0xFFFFFFFF</c> rather
    ///         than 0. Measured against the server for both signs and for both session and transactional
    ///         locks; <c>objsubid</c> is deliberately not matched, so the read does not depend on whether
    ///         the key is taken in its bigint or two-int form.
    ///     </para>
    ///     <para>
    ///         Scoped to the current database. An advisory lock is taken on the acquiring session's
    ///         database -- two databases on one server hold the id <em>independently</em>, and a
    ///         <c>pg_try_advisory_lock</c> for an id another database holds succeeds. <c>pg_locks</c>,
    ///         though, is server-wide, so the unscoped read reported a neighbouring database's holder as
    ///         this database's: "held", with a name and a pid, for a lock this node could have taken
    ///         (weasel#719). Database-per-tenant and parallel test lanes are where that bites. SQL Server
    ///         never had this: <c>sp_getapplock</c>'s rows are filtered by <c>DB_ID()</c>.
    ///     </para>
    ///     <para>
    ///         The join to <c>pg_stat_activity</c> is a LEFT join: a non-superuser sees every row of
    ///         <c>pg_locks</c> but is shown nothing about another user's backend, and a holder whose
    ///         details are hidden is still a holder worth reporting.
    ///     </para>
    /// </remarks>
    public async Task<AdvisoryLockHolder?> FindHolderAsync(int lockId, CancellationToken token)
    {
        var key = (long)lockId;
        var classId = (long)(uint)(int)(key >> 32);
        var objId = (long)(uint)(int)key;

        await using var conn = await _dataSource.OpenConnectionAsync(token).ConfigureAwait(false);
        await using var cmd = conn.CreateCommand();

        cmd.CommandText = """
                          SELECT l.pid, a.application_name, a.client_addr::text, a.backend_start
                          FROM pg_locks l
                          LEFT JOIN pg_stat_activity a ON a.pid = l.pid
                          WHERE l.locktype = 'advisory'
                            AND l.database = (SELECT oid FROM pg_database WHERE datname = current_database())
                            AND l.classid::bigint = @classid
                            AND l.objid::bigint = @objid
                            AND l.granted
                          LIMIT 1
                          """;

        cmd.Parameters.AddWithValue("classid", classId);
        cmd.Parameters.AddWithValue("objid", objId);

        await using var reader = await cmd.ExecuteReaderAsync(token).ConfigureAwait(false);
        if (!await reader.ReadAsync(token).ConfigureAwait(false))
        {
            return null;
        }

        return new AdvisoryLockHolder(lockId)
        {
            SessionId = await reader.IsDBNullAsync(0, token).ConfigureAwait(false)
                ? null
                : (await reader.GetFieldValueAsync<int>(0, token).ConfigureAwait(false)).ToString(),
            ApplicationName = await nullableStringAsync(reader, 1, token).ConfigureAwait(false),
            ClientAddress = await nullableStringAsync(reader, 2, token).ConfigureAwait(false),

            // The backend's start, which is the earliest the lock can have been taken: PostgreSQL does
            // not record when an advisory lock was acquired
            HeldSince = await reader.IsDBNullAsync(3, token).ConfigureAwait(false)
                ? null
                : await reader.GetFieldValueAsync<DateTimeOffset>(3, token).ConfigureAwait(false),

            // Definitive either way for this instance: the lock is exclusive, so if this instance holds
            // it the single holder is us, and if it does not, the holder is not us
            IsCurrentNode = HasLock(lockId)
        };
    }

    private static async Task<string?> nullableStringAsync(NpgsqlDataReader reader, int ordinal, CancellationToken token)
    {
        return await reader.IsDBNullAsync(ordinal, token).ConfigureAwait(false)
            ? null
            : await reader.GetFieldValueAsync<string>(ordinal, token).ConfigureAwait(false);
    }

    public async Task ReleaseLockAsync(int lockId)
    {
        AdvisoryLockConnection? connection;
        lock (_handlesLock)
        {
            if (!_handles.Remove(lockId, out connection)) return;

            if (!connection.IsTransactional)
            {
                connection.Held--;

                // Keeps it open until releaseSessionLockAsync is done with it
                connection.Users++;
            }
        }

        if (connection.IsTransactional)
        {
            await connection.DisposeAsync().ConfigureAwait(false);
        }
        else
        {
            await releaseSessionLockAsync(connection, lockId, rethrow: true).ConfigureAwait(false);
        }
    }

    /// <summary>
    ///     Unlock one session lock already taken out of <see cref="_handles" />, then close its connection if that
    ///     left it holding nothing. The caller claimed the connection (Users) when it took the lock out.
    /// </summary>
    private async Task releaseSessionLockAsync(AdvisoryLockConnection connection, int lockId, bool rethrow)
    {
        try
        {
            if (rethrow)
            {
                await unlockAsync(connection, lockId).ConfigureAwait(false);
            }
            else
            {
                await unlockSafelyAsync(connection, lockId).ConfigureAwait(false);
            }
        }
        finally
        {
            await unclaimSessionConnectionAsync(connection).ConfigureAwait(false);
        }
    }

    private static async Task unlockAsync(AdvisoryLockConnection connection, int lockId)
    {
        // A lost connection took its locks with it
        if (connection.IsLost) return;

        try
        {
            await connection.UnlockAsync(lockId).ConfigureAwait(false);
        }
        catch (Exception) when (connection.IsLost)
        {
            // Lost during the unlock, which released the lock just the same
        }
    }

    private async Task unlockSafelyAsync(AdvisoryLockConnection connection, int lockId)
    {
        try
        {
            await unlockAsync(connection, lockId).ConfigureAwait(false);
        }
        catch (InvalidOperationException)
        {
            // The connection or data source is already closed (ObjectDisposedException derives from this)
        }
        catch (Exception e)
        {
            _logger.LogError(e, "Error trying to release advisory lock {LockId} for database {Identifier}", lockId,
                _databaseName);
        }
    }

    /// <summary>
    ///     Release every held lock. Best-effort and time-bounded: see the remarks.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         weasel#676. With DistributedLock.Postgres underneath (until weasel#910), the last release on a
    ///         monitored, shared session connection could find the connection parked in the monitor's one-minute
    ///         wait and sit there until the wait ran out. With two such connections a host stop took 120 seconds.
    ///         <see cref="AdvisoryLockConnection" /> now cancels that wait before every command, but the two
    ///         safeguards added for it stay, because a release still depends on a database round trip.
    ///     </para>
    ///     <para>
    ///         The releases run concurrently, one per connection, so a stop pays for the slowest connection rather
    ///         than for all of them. And the wait is bounded by <see cref="AdvisoryLockOptions.ReleaseTimeout" />,
    ///         after which the remaining releases are left to finish in the background: the unlock still goes
    ///         through when the round trip completes, and the process is usually exiting anyway, which closes the
    ///         connections and drops the locks regardless. Abandoning the wait delays leadership handover; it does
    ///         not leak a lock.
    ///     </para>
    ///     <para>
    ///         A release that overruns is logged.
    ///     </para>
    /// </remarks>
    public async ValueTask DisposeAsync()
    {
        List<(AdvisoryLockConnection Connection, int[] LockIds)> releases;
        AdvisoryLockConnection[] idle;

        lock (_handlesLock)
        {
            // Latch and drain atomically so no lock can be stored after this point (see TryAttainLockAsync)
            _disposed = true;

            releases = _handles
                .GroupBy(x => x.Value, x => x.Key)
                .Select(x => (x.Key, x.ToArray()))
                .ToList();

            _handles.Clear();

            // Session connections that hold nothing, and that no acquire in flight is using
            idle = _sessionConnections.Where(x => x.Users == 0 && x.Held == 0).ToArray();

            foreach (var (connection, _) in releases)
            {
                connection.Held = 0;

                // Keeps it open until releaseOnDisposalAsync is done with it
                if (!connection.IsTransactional) connection.Users++;
            }
        }

        foreach (var connection in idle)
        {
            await closeSessionConnectionIfIdleAsync(connection).ConfigureAwait(false);
        }

        var total = releases.Sum(x => x.LockIds.Length);
        if (total == 0) return;

        var tasks = new Task[releases.Count];
        for (var i = 0; i < releases.Count; i++)
        {
            tasks[i] = releaseOnDisposalAsync(releases[i].Connection, releases[i].LockIds);
        }

        var all = Task.WhenAll(tasks);

        // Same convention as JasperFx's DaemonSettings.StopAndDrainTimeout, which is what a daemon host
        // feeds this from: a non-positive or infinite value opts out of the separate bound rather than
        // meaning "give up immediately".
        if (_options.ReleaseTimeout <= TimeSpan.Zero || _options.ReleaseTimeout == Timeout.InfiniteTimeSpan)
        {
            await all.ConfigureAwait(false);
            return;
        }

        // The token only stops the timer once the releases have won the race -- it never cancels a
        // release, which has to run to completion for the lock to actually come off the session.
        using var timer = new CancellationTokenSource();
        var finished = await Task.WhenAny(all, Task.Delay(_options.ReleaseTimeout, timer.Token))
            .ConfigureAwait(false);

        if (ReferenceEquals(finished, all))
        {
            await timer.CancelAsync().ConfigureAwait(false);

            // Observe the faults -- releaseOnDisposalAsync already swallows them, so this only unwraps
            await all.ConfigureAwait(false);
            return;
        }

        var outstanding = 0;
        for (var i = 0; i < tasks.Length; i++)
        {
            if (!tasks[i].IsCompleted) outstanding += releases[i].LockIds.Length;
        }

        _logger.LogWarning(
            "Timed out after {Timeout} waiting to release {Outstanding} of {Total} advisory locks for database {Identifier}. The releases are still running and the locks will be released when they complete, but leadership of those locks cannot move to another node until then. See https://github.com/JasperFx/weasel/issues/676",
            _options.ReleaseTimeout, outstanding, total, _databaseName);

        // Don't leave the abandoned releases as unobserved faulted tasks
        _ = all.ContinueWith(static t => _ = t.Exception, TaskScheduler.Default);
    }

    private async Task releaseOnDisposalAsync(AdvisoryLockConnection connection, int[] lockIds)
    {
        if (connection.IsTransactional)
        {
            await disposeConnectionSafelyAsync(connection).ConfigureAwait(false);
            return;
        }

        try
        {
            foreach (var lockId in lockIds)
            {
                await unlockSafelyAsync(connection, lockId).ConfigureAwait(false);
            }
        }
        finally
        {
            await unclaimSessionConnectionAsync(connection).ConfigureAwait(false);
        }
    }

    private async Task disposeConnectionSafelyAsync(AdvisoryLockConnection connection)
    {
        try
        {
            await connection.DisposeAsync().ConfigureAwait(false);
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
