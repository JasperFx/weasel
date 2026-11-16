using System.Data.Common;
using Microsoft.Data.Sqlite;
using Weasel.Sqlite.Functions;

namespace Weasel.Sqlite;

/// <summary>
/// A DbDataSource for SQLite that applies PRAGMA settings (WAL mode, busy timeout, etc.),
/// loads configured extensions, and registers application-defined functions on every
/// connection it opens. The default DbDataSource from SqliteFactory does none of that.
///
/// For in-memory databases with Cache=Shared, this class maintains a keep-alive connection
/// to prevent the database from being destroyed when all other connections close.
///
/// PRAGMAs are applied by scope: connection-scoped PRAGMAs (busy_timeout, foreign_keys,
/// synchronous, ...) are applied to every connection, while database-file-scoped PRAGMAs
/// (page_size, auto_vacuum, and journal_mode = WAL, which is persisted in the file header)
/// are applied only on the first connection this data source opens. Re-issuing
/// "PRAGMA journal_mode = WAL" on every open forces SQLite to take file locks to verify the
/// journal mode and can return SQLITE_BUSY under a concurrent writer.
///
/// Extensions (<see cref="SqliteExtensionSettings" />) and functions
/// (<see cref="SqliteFunctionRegistry" />) are the opposite of file-scoped: SQLite attaches both
/// to a connection handle, so they are gone the moment the connection closes and have to be
/// re-applied on every open. That is why they belong here rather than in the schema, and why a
/// consumer that registers a function through any other path sees it on some connections and
/// not others. The per-open order is connection PRAGMAs, then extensions, then functions, so a
/// managed function can build on something an extension provides. See JasperFx/weasel#588.
///
/// Assumption: for a database file that is not being modified externally, applying the
/// file-scoped PRAGMAs once gives the same guarantee as re-applying them per open (the per-open
/// reapplication was a no-op after the first open anyway). Multiple data sources pointed at the
/// same file each apply the file-scoped PRAGMAs once, which is harmless — WAL conversion of an
/// already-WAL file is a cheap verification. A file deleted and recreated underneath a live data
/// source is not supported, exactly as before.
///
/// Every connection this data source hands out, opened or not, takes a per-connection-string gate
/// around its pool checkout, shared process-wide with every other data source and
/// <see cref="SqliteDatabase" /> over the same string. Microsoft.Data.Sqlite 10.0.12 and earlier can
/// otherwise lend one native handle to two open connections under concurrent opens. The gate covers
/// the checkout alone; PRAGMAs, extensions and functions are applied after it is released. Use
/// <see cref="ClearPool(string)" /> and <see cref="ClearAllPools" /> rather than
/// <see cref="SqliteConnection.ClearPool" /> and <see cref="SqliteConnection.ClearAllPools" />, because a
/// pool clear runs the same scan an open does. See JasperFx/weasel#857 (from fisher#576 / fisher#582).
/// </summary>
public class SqliteDataSource : DbDataSource
{
    private readonly string _connectionString;
    private readonly SqlitePragmaSettings _pragmaSettings;
    private readonly SqliteFunctionRegistry _functions;
    private readonly SqliteExtensionSettings _extensions;
    private readonly Lock _gate;
    private SqliteConnection? _keepAliveConnection;
    private readonly bool _isInMemory;
    private volatile bool _databasePragmasApplied;

    public SqliteDataSource(string connectionString, SqlitePragmaSettings? pragmaSettings = null)
        : this(connectionString, pragmaSettings, null, null)
    {
    }

    /// <summary>
    /// Create a data source that also loads <paramref name="extensions" /> and registers
    /// <paramref name="functions" /> on every connection it opens.
    /// </summary>
    /// <param name="connectionString">The SQLite connection string.</param>
    /// <param name="pragmaSettings">PRAGMA settings, or <c>null</c> for <see cref="SqlitePragmaSettings.Default" />.</param>
    /// <param name="functions">
    /// Functions to register on every opened connection, or <c>null</c> for an empty registry.
    /// The registry stays live: functions added to <see cref="Functions" /> after construction
    /// are registered on connections opened from then on.
    /// </param>
    /// <param name="extensions">
    /// Extensions to load on every opened connection, or <c>null</c> for none. Likewise live via
    /// <see cref="Extensions" />.
    /// </param>
    public SqliteDataSource(
        string connectionString,
        SqlitePragmaSettings? pragmaSettings,
        SqliteFunctionRegistry? functions,
        SqliteExtensionSettings? extensions)
    {
        _connectionString = connectionString ?? throw new ArgumentNullException(nameof(connectionString));
        _pragmaSettings = pragmaSettings ?? SqlitePragmaSettings.Default;
        _functions = functions ?? new SqliteFunctionRegistry();
        _extensions = extensions ?? new SqliteExtensionSettings();
        _isInMemory = IsInMemoryConnectionString(connectionString);
        _gate = SqlitePoolGate.For(connectionString);
    }

    public override string ConnectionString => _connectionString;

    /// <summary>
    /// Whether this data source targets an in-memory database.
    /// </summary>
    public bool IsInMemory => _isInMemory;

    /// <summary>
    /// The functions registered on every connection this data source opens.
    /// </summary>
    public SqliteFunctionRegistry Functions => _functions;

    /// <summary>
    /// The extensions loaded on every connection this data source opens.
    /// </summary>
    public SqliteExtensionSettings Extensions => _extensions;

    /// <summary>
    /// An unopened connection whose own <c>Open</c> and <c>OpenAsync</c> take this connection string's
    /// pool gate (weasel#857). It is a <see cref="SqliteConnection" />. Opening it yourself gets the
    /// gate but not this data source's PRAGMAs, extensions or functions, exactly as before.
    /// </summary>
    protected override DbConnection CreateDbConnection()
    {
        EnsureKeepAlive();
        return new GatedSqliteConnection(_connectionString, _gate);
    }

    protected override async ValueTask<DbConnection> OpenDbConnectionAsync(CancellationToken cancellationToken = default)
    {
        EnsureKeepAlive();
        var conn = new GatedSqliteConnection(_connectionString, _gate);
        try
        {
            // Only this line runs under the pool gate (weasel#857). It completes synchronously: see
            // SqlitePoolGate for why the checkout has no await to hold a gate across. A cancelled token
            // still fails here, before the checkout.
            await conn.OpenAsync(cancellationToken).ConfigureAwait(false);
            await _pragmaSettings.ApplyConnectionPragmasAsync(conn, cancellationToken).ConfigureAwait(false);
            await EnsureDatabasePragmasAsync(conn, cancellationToken).ConfigureAwait(false);
            applyExtensionsAndFunctions(conn);
        }
        catch
        {
            await conn.DisposeAsync().ConfigureAwait(false);
            throw;
        }

        return conn;
    }

    protected override DbConnection OpenDbConnection()
    {
        EnsureKeepAlive();
        var conn = new GatedSqliteConnection(_connectionString, _gate);
        try
        {
            // Only the pool checkout runs under the gate (weasel#857); everything below runs after it.
            conn.Open();

            // PRAGMAs execute locally with no async I/O, so a genuinely synchronous
            // execution is safe here and avoids blocking on async machinery
            _pragmaSettings.ApplyConnectionPragmas(conn);
            if (_isInMemory || !_databasePragmasApplied)
            {
                _pragmaSettings.ApplyDatabasePragmas(conn);
                _databasePragmasApplied = true;
            }

            applyExtensionsAndFunctions(conn);
        }
        catch
        {
            conn.Dispose();
            throw;
        }

        return conn;
    }

    /// <summary>
    /// Loading an extension and registering a function are both synchronous native calls with no
    /// I/O, so the async open path calls this directly rather than paying a thread hop per open.
    /// </summary>
    private void applyExtensionsAndFunctions(SqliteConnection conn)
    {
        _extensions.ApplyToConnection(conn);
        _functions.RegisterAll(conn);
    }

    /// <summary>
    /// Apply the database-file-scoped PRAGMAs the first time this data source successfully opens
    /// a connection. A benign race between concurrent first opens can apply them more than once,
    /// which is harmless — it is exactly what every open used to do. In-memory databases without
    /// shared cache are a distinct database per connection, so for in-memory data sources the
    /// batch is still applied on every open (where it is lock-free and cheap anyway).
    /// </summary>
    private async ValueTask EnsureDatabasePragmasAsync(SqliteConnection conn, CancellationToken cancellationToken)
    {
        if (!_isInMemory && _databasePragmasApplied) return;

        await _pragmaSettings.ApplyDatabasePragmasAsync(conn, cancellationToken).ConfigureAwait(false);
        _databasePragmasApplied = true;
    }

    /// <summary>
    /// For in-memory databases, keeps at least one connection open to prevent
    /// the database from being destroyed when other connections close.
    /// </summary>
    private void EnsureKeepAlive()
    {
        if (!_isInMemory || _keepAliveConnection != null) return;

        _keepAliveConnection = new GatedSqliteConnection(_connectionString, _gate);
        _keepAliveConnection.Open();
    }

    /// <summary>
    /// Explicitly close the keep-alive connection. For in-memory databases,
    /// this will destroy the database if no other connections are open.
    /// </summary>
    public void CloseKeepAlive()
    {
        _keepAliveConnection?.Dispose();
        _keepAliveConnection = null;
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            CloseKeepAlive();
        }

        base.Dispose(disposing);
    }

    /// <summary>
    /// Empty the Microsoft.Data.Sqlite connection pool for <paramref name="connectionString" />, under the
    /// same per-connection-string gate every Weasel open of that pool takes. Prefer it to
    /// <see cref="SqliteConnection.ClearPool" />: a pool clear runs the leaked-connection scan that can
    /// lend a connection still being opened to a second owner (weasel#857). As with
    /// <see cref="SqliteConnection.ClearPool" />, connections open at the time are closed rather than
    /// returned to the pool when their owners close them.
    /// </summary>
    /// <param name="connectionString">
    /// The exact connection string, as passed to the data source; Microsoft.Data.Sqlite keys its pools by it
    /// with ordinal comparison, so an equivalent string spelled differently is a different pool.
    /// </param>
    public static void ClearPool(string connectionString)
    {
        ArgumentNullException.ThrowIfNull(connectionString);
        SqlitePoolGate.ClearPool(connectionString);
    }

    /// <summary>
    /// Empty every Microsoft.Data.Sqlite connection pool in the process, as
    /// <see cref="SqliteConnection.ClearAllPools" /> does, while holding every Weasel pool gate (weasel#857).
    /// <see cref="SqliteConnection.ClearAllPools" /> runs the leaked-connection scan on every pool, so it
    /// needs every gate; Weasel opens wait for the clear to finish. Pools that only ever had connections
    /// opened outside Weasel are cleared too, but nothing serializes those opens.
    /// </summary>
    public static void ClearAllPools()
    {
        SqlitePoolGate.ClearAllPools();
    }

    /// <summary>
    /// Detect whether a connection string targets an in-memory database.
    /// </summary>
    public static bool IsInMemoryConnectionString(string connectionString)
    {
        if (string.IsNullOrEmpty(connectionString)) return false;

        var builder = new SqliteConnectionStringBuilder(connectionString);
        return builder.Mode == SqliteOpenMode.Memory
               || builder.DataSource == ":memory:"
               || builder.DataSource.Contains("mode=memory", StringComparison.OrdinalIgnoreCase);
    }
}
