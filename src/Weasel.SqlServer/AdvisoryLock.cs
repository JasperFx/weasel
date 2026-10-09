using System.Collections.Immutable;
using System.Data;
using JasperFx.Core;
using JasperFx.Events.Daemon;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;

namespace Weasel.SqlServer;

/// <summary>
///     Settings for the lock monitor weasel#695 added to <see cref="AdvisoryLock" />. Every default is the
///     behaviour of the three-argument constructor, which is what shipped in 9.42.0, so an instance built
///     with these settings untouched behaves exactly as that release does.
///     <para>
///     There is no counterpart to <c>Weasel.Postgresql.AdvisoryLockOptions.ReleaseTimeout</c>; see the
///     remarks on <see cref="AdvisoryLock.DisposeAsync" /> for the measurement that rules it out.
///     </para>
/// </summary>
public sealed class SqlServerAdvisoryLockOptions
{
    /// <summary>
    ///     How often the monitor asks SQL Server whether this session still holds the locks that
    ///     <see cref="AdvisoryLock.HasLock" /> reports. The probe short-circuits while no locks are held, so an
    ///     idle lock costs nothing.
    /// </summary>
    /// <remarks>
    ///     A non-positive value or <see cref="Timeout.InfiniteTimeSpan" /> does not start the monitor at all,
    ///     following the convention <c>Weasel.Postgresql.AdvisoryLockOptions.ReleaseTimeout</c> documents and
    ///     JasperFx reads <c>DaemonSettings.StopAndDrainTimeout</c> by. Opting out re-opens the weasel#695
    ///     split brain: a node that loses its session goes on reporting locks the server has already released,
    ///     and two nodes run the same shard. The only reason to do it is a deployment with so many databases
    ///     that one parked timer each is itself the cost — weasel#439 is that shape — and it wants to be a
    ///     deliberate choice rather than a default.
    /// </remarks>
    public TimeSpan MonitoringInterval { get; set; } = 5.Seconds();

    /// <summary>
    ///     <c>CommandTimeout</c> for the monitor's probe.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         This bounds shutdown as much as it bounds a query. <see cref="AdvisoryLock.DisposeAsync" />
    ///         stops the monitor from inside the same gate the probe holds, so an in-flight probe delays
    ///         disposal by up to this long, and a probe that runs to its limit blocks
    ///         <see cref="AdvisoryLock.TryAttainLockAsync" /> for the same stretch. Keep it under
    ///         <see cref="MonitoringInterval" />; at equal values a probe at its limit consumes the whole duty
    ///         cycle.
    ///     </para>
    ///     <para>
    ///         <c>CommandTimeout</c> is whole seconds and reads zero as "no timeout", so a sub-second value is
    ///         rounded up to one second rather than silently removing the bound. There is deliberately no way
    ///         to ask for an unbounded probe: it would hold the gate, and disposal behind it, for as long as
    ///         the server stayed unresponsive.
    ///     </para>
    /// </remarks>
    public TimeSpan ProbeTimeout { get; set; } = 5.Seconds();
}

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
    private readonly int _probeTimeoutSeconds;

    // The monitor shares the connection, and SqlConnection does not support concurrent commands
    private readonly SemaphoreSlim _gate = new(1, 1);

    // Null when monitoring was opted out of: see SqlServerAdvisoryLockOptions.MonitoringInterval
    private readonly PeriodicTimer? _monitorTimer;

    private volatile SqlConnection? _conn;
    private volatile ImmutableHashSet<int> _locks = ImmutableHashSet<int>.Empty;
    private volatile bool _disposed;

    public AdvisoryLock(Func<SqlConnection> source, ILogger logger, string databaseName)
        : this(source, logger, databaseName, new SqlServerAdvisoryLockOptions())
    {
    }

    public AdvisoryLock(Func<SqlConnection> source, ILogger logger, string databaseName,
        SqlServerAdvisoryLockOptions options)
    {
        _source = source;
        _logger = logger;
        _databaseName = databaseName;
        _probeTimeoutSeconds = ProbeTimeoutSeconds(options.ProbeTimeout);

        if (MonitoringEnabled(options.MonitoringInterval))
        {
            // DisposeAsync stops the loop by disposing the timer
            _monitorTimer = new PeriodicTimer(options.MonitoringInterval);
            _ = monitorAsync(_monitorTimer);
        }
    }

    // Timeout.InfiniteTimeSpan is itself negative, so the first test already covers it. Both are spelled out
    // because the opt-out is documented as either, and a reader should not have to know that to trust it.
    internal static bool MonitoringEnabled(TimeSpan interval)
    {
        return interval > TimeSpan.Zero && interval != Timeout.InfiniteTimeSpan;
    }

    // SqlCommand.CommandTimeout is whole seconds and reads 0 as "wait forever", which is the one value this
    // must never produce: an unbounded probe would hold _gate, and the disposal waiting behind it, for as
    // long as the server stayed unresponsive. So round up, and floor at one second.
    internal static int ProbeTimeoutSeconds(TimeSpan timeout)
    {
        if (timeout.TotalSeconds >= int.MaxValue) return int.MaxValue;

        return Math.Max(1, (int)Math.Ceiling(timeout.TotalSeconds));
    }

    public bool HasLock(int lockId)
    {
        return _conn is { State: not ConnectionState.Closed } && _locks.Contains(lockId);
    }

    public async Task<bool> TryAttainLockAsync(int lockId, CancellationToken token)
    {
        // weasel#349: never start a new acquire once disposal has begun. During host shutdown opening _conn
        // races with disposal and can abort with a process-killing ObjectDisposedException — mirror the
        // Postgres guard for Polecat parity.
        if (_disposed) return false;

        try
        {
            await _gate.WaitAsync(token).ConfigureAwait(false);
            try
            {
                if (_disposed) return false;

                if (_conn == null)
                {
                    _conn = _source();
                    await _conn.OpenAsync(token).ConfigureAwait(false);
                }

                if (_conn.State == ConnectionState.Closed)
                {
                    await dropConnectionAsync().ConfigureAwait(false);
                    return false;
                }

                // sp_getapplock is reentrant, so taking a held lock again would take two releases to free it
                if (_locks.Contains(lockId)) return true;

                // No wait: the caller polls every cycle, and waiting only delays it stopping the shards it lost
                var attained = await _conn.TryGetGlobalLock(lockId.ToString(), cancellation: token, lockTimeoutMs: 0)
                    .ConfigureAwait(false);
                if (attained)
                {
                    _locks = _locks.Add(lockId);
                    return true;
                }

                return false;
            }
            finally
            {
                _gate.Release();
            }
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
    ///         <c>sp_getapplock</c> takes the id as a string resource, and
    ///         <c>sys.dm_tran_locks.resource_description</c> reports it wrapped as
    ///         <c>0:[4242]:(1bb08fa7)</c> — the database principal, the resource name in brackets, and a
    ///         hash. Measured against the server. Matching the bracketed name rather than the whole
    ///         string keeps the principal and hash out of it, and the read is confined to the current
    ///         database because an application lock is per-database.
    ///     </para>
    ///     <para>
    ///         Read on its own connection rather than the one holding the locks, which may be busy. The
    ///         dynamic management views need <c>VIEW SERVER STATE</c>; without it this throws rather than
    ///         reporting the lock as unheld, since "nothing holds this" is precisely the reassuring
    ///         conclusion an operator chasing a double-runner must not be handed by accident.
    ///     </para>
    /// </remarks>
    public async Task<AdvisoryLockHolder?> FindHolderAsync(int lockId, CancellationToken token)
    {
        await using var conn = _source();
        await conn.OpenAsync(token).ConfigureAwait(false);

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = """
                          SELECT TOP 1 l.request_session_id, s.program_name, s.login_time, c.client_net_address
                          FROM sys.dm_tran_locks l
                          LEFT JOIN sys.dm_exec_sessions s ON s.session_id = l.request_session_id
                          LEFT JOIN sys.dm_exec_connections c ON c.session_id = l.request_session_id
                          WHERE l.resource_type = 'APPLICATION'
                            AND l.resource_database_id = DB_ID()
                            AND l.request_status = 'GRANT'
                            AND CHARINDEX(':[' + @resource + ']:', l.resource_description) > 0
                          """;

        var resource = cmd.CreateParameter();
        resource.ParameterName = "resource";
        resource.Value = lockId.ToString();
        cmd.Parameters.Add(resource);

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

            // The session's login time, which is the earliest the lock can have been taken: SQL Server
            // does not record when an application lock was acquired
            HeldSince = await reader.IsDBNullAsync(2, token).ConfigureAwait(false)
                ? null
                : new DateTimeOffset(await reader.GetFieldValueAsync<DateTime>(2, token).ConfigureAwait(false),
                    TimeSpan.Zero),
            ClientAddress = await nullableStringAsync(reader, 3, token).ConfigureAwait(false),

            // Definitive either way for this instance: the lock is exclusive, so if this instance holds
            // it the single holder is us, and if it does not, the holder is not us
            IsCurrentNode = HasLock(lockId)
        };
    }

    private static async Task<string?> nullableStringAsync(SqlDataReader reader, int ordinal, CancellationToken token)
    {
        return await reader.IsDBNullAsync(ordinal, token).ConfigureAwait(false)
            ? null
            : await reader.GetFieldValueAsync<string>(ordinal, token).ConfigureAwait(false);
    }

    public async Task ReleaseLockAsync(int lockId)
    {
        await _gate.WaitAsync().ConfigureAwait(false);
        try
        {
            if (!_locks.Contains(lockId)) return;

            if (_conn == null || _conn.State == ConnectionState.Closed)
            {
                _locks = _locks.Remove(lockId);
                return;
            }

            var cancellation = new CancellationTokenSource();
            cancellation.CancelAfter(1.Seconds());

            await _conn.ReleaseGlobalLock(lockId.ToString(), cancellation: cancellation.Token).ConfigureAwait(false);
            _locks = _locks.Remove(lockId);

            if (_locks.IsEmpty)
            {
                await _conn.CloseAsync().ConfigureAwait(false);
                await _conn.DisposeAsync().ConfigureAwait(false);
                _conn = null;
            }
        }
        finally
        {
            _gate.Release();
        }
    }

    /// <summary>
    ///     Release every held lock and close the connection.
    /// </summary>
    /// <remarks>
    ///     There is deliberately no equivalent of the Postgres sibling's
    ///     <c>AdvisoryLockOptions.ReleaseTimeout</c>, and the reason is measured rather than assumed. That
    ///     bound is safe on Postgres because abandoning a release still ends with the connection closing and
    ///     the session-scoped lock going with it. Here the connection is pooled: closing it returns it to the
    ///     pool with its session, and its <c>sp_getapplock</c> holds, intact — the lock is not freed until the
    ///     pooled connection is next reset or the process exits. So a release this method skipped would leave
    ///     the lock held against the node that needs it next, which is the one failure an advisory lock may
    ///     not have. The releases are therefore unbounded on purpose. See
    ///     <c>advisory_lock_options.a_pooled_connection_keeps_its_locks_when_it_closes</c>, which pins it.
    /// </remarks>
    public async ValueTask DisposeAsync()
    {
        // Set first, before disposing the connection, so any concurrent TryAttainLockAsync short-circuits (weasel#349).
        _disposed = true;

        await _gate.WaitAsync().ConfigureAwait(false);
        try
        {
            _monitorTimer?.Dispose();

            if (_conn != null)
            {
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
                    _conn = null;
                    _locks = ImmutableHashSet<int>.Empty;
                }
            }
        }
        finally
        {
            _gate.Release();
        }
    }

    // Callers hold _gate. Every lock was taken by the connection's session, so they all end with it.
    private async Task dropConnectionAsync(Exception? cause = null)
    {
        var conn = _conn;
        var lost = _locks;
        _conn = null;
        _locks = ImmutableHashSet<int>.Empty;

        if (!lost.IsEmpty)
        {
            _logger.LogWarning(cause, "Lost advisory locks {LockIds} for database {Identifier}: the session holding them has ended",
                lost, _databaseName);
        }

        if (conn == null) return;

        try
        {
            await conn.DisposeAsync().ConfigureAwait(false);
        }
        catch (Exception e)
        {
            _logger.LogError(e, "Error trying to clean up and restart an advisory lock connection");
        }
    }

    // A node holding every lock it needs sends nothing on its connection, so only a probe reveals a lost session
    private async Task monitorAsync(PeriodicTimer timer)
    {
        while (await timer.WaitForNextTickAsync(CancellationToken.None).ConfigureAwait(false))
        {
            await VerifyHeldLocksAsync().ConfigureAwait(false);
        }
    }

    internal async Task VerifyHeldLocksAsync()
    {
        await _gate.WaitAsync(CancellationToken.None).ConfigureAwait(false);
        try
        {
            var conn = _conn;
            var held = _locks;
            if (conn == null || held.IsEmpty) return;

            try
            {
                var lost = await locksNotHeldAsync(conn, held, _probeTimeoutSeconds).ConfigureAwait(false);
                if (lost.Count == 0) return;

                _locks = held.Except(lost);
                _logger.LogWarning("Lost advisory locks {LockIds} for database {Identifier}: the session no longer holds them",
                    lost, _databaseName);
            }
            catch (Exception e) when (conn.State != ConnectionState.Open)
            {
                await dropConnectionAsync(e).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                // The connection is still open, so the session may well be alive: the next probe asks again
                _logger.LogWarning(e, "Unable to verify the advisory locks held for database {Identifier}", _databaseName);
            }
        }
        finally
        {
            _gate.Release();
        }
    }

    // Asks per lock: a release whose client-side timeout fired may still have run on the server
    private static async Task<List<int>> locksNotHeldAsync(SqlConnection conn, ImmutableHashSet<int> held,
        int probeTimeoutSeconds)
    {
        var lockIds = held.ToArray();

        await using var cmd = conn.CreateCommand();
        cmd.CommandTimeout = probeTimeoutSeconds;
        cmd.CommandText =
            $"SELECT i FROM (VALUES {string.Join(", ", lockIds.Select((_, i) => $"({i}, @lock{i})"))}) AS held(i, lock_id) " +
            "WHERE APPLOCK_MODE('public', lock_id, 'Session') <> 'Exclusive'";

        for (var i = 0; i < lockIds.Length; i++)
        {
            cmd.With($"lock{i}", lockIds[i].ToString());
        }

        var lost = new List<int>();
        await using var reader = await cmd.ExecuteReaderAsync(CancellationToken.None).ConfigureAwait(false);
        while (await reader.ReadAsync(CancellationToken.None).ConfigureAwait(false))
        {
            lost.Add(lockIds[reader.GetInt32(0)]);
        }

        return lost;
    }
}
