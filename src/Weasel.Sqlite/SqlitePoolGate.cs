using System.Collections.Concurrent;
using Microsoft.Data.Sqlite;

namespace Weasel.Sqlite;

/// <summary>
///     One lock per exact connection string, taken around every Microsoft.Data.Sqlite pool checkout
///     and pool clear that Weasel performs (weasel#857, from fisher#576 and fisher#582).
/// </summary>
/// <remarks>
///     <para>
///         <b>The race.</b> In Microsoft.Data.Sqlite 10.0.12 and earlier,
///         <c>SqliteConnectionInternal.Activate</c> sets <c>_active = true</c> and only then sets the weak
///         reference to its owning <see cref="SqliteConnection" />, outside the pool's lock. A pooled
///         connection counts as <c>Leaked</c> when it is active with no live owner. So an <c>Open</c> that
///         finds the pool empty with an even count runs <c>ReclaimLeakedConnections</c>, and if that lands
///         between the two writes it reclaims a connection that is still being opened and lends its native
///         handle to the next opener. Two open connections then share one handle: one session's
///         <c>ROLLBACK</c> ends the other's transaction ("This SqliteTransaction has completed"),
///         <c>Dispose</c> fails with "unable to delete/modify user-function due to active statements", or
///         two threads replace the handle's rollback hook at once and the process dies in SQLitePCLRaw's
///         <c>rollback_hook_bridge_impl</c>. Fisher found it as an intermittent CI failure (fisher#576)
///         and confirmed it by recording each open connection's native handle.
///     </para>
///     <para>
///         <b>Why one lock per connection string closes it.</b> Every reclaim scan runs inside an
///         <c>Open</c> or a pool clear, and every <c>Activate</c> runs inside an <c>Open</c>. The pool is
///         keyed by the exact connection string with ordinal comparison, and so is this gate, which is
///         static so that two data sources, or a data source and a <see cref="SqliteDatabase" />, over one
///         string share it. Only the checkout is held: <see cref="SqliteDataSource" /> applies its PRAGMAs,
///         extensions and functions after the gate is released.
///     </para>
///     <para>
///         <b>Why a <see cref="Lock" /> and not a <see cref="SemaphoreSlim" />.</b>
///         <see cref="SqliteConnection" /> does not override <c>OpenAsync</c>: it inherits
///         <see cref="System.Data.Common.DbConnection.OpenAsync(CancellationToken)" />, which runs the
///         synchronous <c>Open</c> and returns a completed task (checked against the decompiled 10.0.9,
///         10.0.14 and 11.0.0 assemblies). There is no <c>await</c> inside the checkout to hold a gate
///         across, so an async-capable gate would buy nothing, while a synchronous <c>Open</c> blocking on
///         a semaphore an async opener holds is the thread-pool starvation shape weasel#833 removed from
///         <c>HiloSequenceBase</c>. Both paths take the lock around a synchronous checkout, held for the
///         microseconds a warm checkout takes, or the file open of a cold one.
///     </para>
///     <para>
///         <b>What it does not cover.</b> A <c>new SqliteConnection(...)</c> that application code opens
///         itself bypasses the gate, and so does the pool-pruning timer inside Microsoft.Data.Sqlite, which
///         can clear an idle pool group on its own schedule. A process that opens connections to the same
///         string outside Weasel narrows the window rather than closing it.
///     </para>
///     <para>
///         <b>When it comes out.</b> Microsoft.Data.Sqlite publishes the owner before <c>_active</c> from
///         10.0.13 and 11.0.0 on (dotnet/efcore#39008, fixed by #39009 and backported to release/10.0 by
///         #39012). Once Weasel.Sqlite's floor for Microsoft.Data.Sqlite is at or above that, the gate is
///         dead weight and should be removed.
///     </para>
///     <para>
///         The dictionary is never pruned: one small <see cref="Lock" /> per distinct connection string the
///         process ever opens through Weasel. A gate cannot be removed safely while a connection may still
///         hold a reference to it.
///     </para>
/// </remarks>
internal static class SqlitePoolGate
{
    private static readonly ConcurrentDictionary<string, Lock> _gates = new(StringComparer.Ordinal);

    // Taken only to create a gate and by ClearAllPools, so that ClearAllPools sees every gate that exists
    // while it runs. Never held by an opener while it holds a gate.
    private static readonly Lock _creation = new();

    /// <summary>
    ///     The gate for <paramref name="connectionString" />'s pool.
    /// </summary>
    public static Lock For(string connectionString)
    {
        if (_gates.TryGetValue(connectionString, out var gate)) return gate;

        lock (_creation)
        {
            return _gates.GetOrAdd(connectionString, static _ => new Lock());
        }
    }

    /// <summary>
    ///     <see cref="SqliteConnection.ClearPool" /> for <paramref name="connectionString" />'s pool, under its
    ///     gate: a pool clear runs the same leaked-connection scan an open does.
    /// </summary>
    public static void ClearPool(string connectionString)
    {
        lock (For(connectionString))
        {
            // Constructing the connection only resolves its pool group; it is never opened.
            using var connection = new SqliteConnection(connectionString);
            SqliteConnection.ClearPool(connection);
        }
    }

    /// <summary>
    ///     <see cref="SqliteConnection.ClearAllPools" />, holding every gate at once, because it runs the
    ///     leaked-connection scan on every pool in the process.
    /// </summary>
    public static void ClearAllPools()
    {
        lock (_creation)
        {
            // Holding _creation means no gate can be added while this runs, so every Weasel opener is
            // either already behind one of these gates or waiting to create its own. Only this method takes
            // more than one gate, and _creation serializes it, so no lock order can deadlock.
            var gates = _gates.Values.ToArray();
            var entered = 0;
            try
            {
                foreach (var gate in gates)
                {
                    gate.Enter();
                    entered++;
                }

                SqliteConnection.ClearAllPools();
            }
            finally
            {
                for (var i = entered - 1; i >= 0; i--)
                {
                    gates[i].Exit();
                }
            }
        }
    }
}

/// <summary>
///     A <see cref="SqliteConnection" /> whose <see cref="Open" /> and <see cref="OpenAsync" /> take its
///     connection string's <see cref="SqlitePoolGate" /> around the pool checkout. Handed out by
///     <see cref="SqliteDataSource" /> and by <see cref="SqliteDatabase" />'s connection factory, so callers
///     that open a connection themselves (Weasel's migrations, Weasel.Storage's session-free projection
///     loads) are serialized with the data source's own opens. It is a <see cref="SqliteConnection" />, so
///     code that casts to one keeps working.
/// </summary>
internal sealed class GatedSqliteConnection: SqliteConnection
{
    private string? _gateKey;
    private Lock? _gate;

    public GatedSqliteConnection(string connectionString): base(connectionString)
    {
    }

    public GatedSqliteConnection(string connectionString, Lock gate): base(connectionString)
    {
        _gateKey = connectionString;
        _gate = gate;
    }

    public override void Open()
    {
        if (State == System.Data.ConnectionState.Open) return;

        lock (currentGate())
        {
            base.Open();
        }
    }

    /// <summary>
    ///     <see cref="SqliteConnection" /> inherits <c>DbConnection.OpenAsync</c>, which already calls
    ///     <see cref="Open" />. This override says so explicitly, so that a future Microsoft.Data.Sqlite that
    ///     implements <c>OpenAsync</c> without going through <see cref="Open" /> cannot route around the gate.
    ///     It keeps the base's contract: a cancelled token returns a cancelled task, and a failure is returned
    ///     as a faulted task rather than thrown.
    /// </summary>
    public override Task OpenAsync(CancellationToken cancellationToken)
    {
        if (cancellationToken.IsCancellationRequested)
        {
            return Task.FromCanceled(cancellationToken);
        }

        try
        {
            Open();
            return Task.CompletedTask;
        }
#pragma warning disable CA1031 // the base DbConnection.OpenAsync contract: every failure goes into the task
        catch (Exception e)
#pragma warning restore CA1031
        {
            return Task.FromException(e);
        }
    }

    // The connection string can be replaced while the connection is closed, and the pool follows it, so the
    // gate does too. The reference check keeps the lookup off the common path.
    private Lock currentGate()
    {
        var connectionString = ConnectionString;
        if (_gate is null || !ReferenceEquals(connectionString, _gateKey))
        {
            _gate = SqlitePoolGate.For(connectionString);
            _gateKey = connectionString;
        }

        return _gate;
    }
}
