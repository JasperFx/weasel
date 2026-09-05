using System.Data.Common;
using Microsoft.Data.Sqlite;

namespace Weasel.Sqlite;

/// <summary>
/// A DbDataSource for SQLite that applies PRAGMA settings (WAL mode, busy timeout, etc.)
/// to every connection it opens. The default DbDataSource from SqliteFactory does not
/// apply any PRAGMAs.
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
/// Assumption: for a database file that is not being modified externally, applying the
/// file-scoped PRAGMAs once gives the same guarantee as re-applying them per open (the per-open
/// reapplication was a no-op after the first open anyway). Multiple data sources pointed at the
/// same file each apply the file-scoped PRAGMAs once, which is harmless — WAL conversion of an
/// already-WAL file is a cheap verification. A file deleted and recreated underneath a live data
/// source is not supported, exactly as before.
/// </summary>
public class SqliteDataSource : DbDataSource
{
    private readonly string _connectionString;
    private readonly SqlitePragmaSettings _pragmaSettings;
    private SqliteConnection? _keepAliveConnection;
    private readonly bool _isInMemory;
    private volatile bool _databasePragmasApplied;

    public SqliteDataSource(string connectionString, SqlitePragmaSettings? pragmaSettings = null)
    {
        _connectionString = connectionString ?? throw new ArgumentNullException(nameof(connectionString));
        _pragmaSettings = pragmaSettings ?? SqlitePragmaSettings.Default;
        _isInMemory = IsInMemoryConnectionString(connectionString);
    }

    public override string ConnectionString => _connectionString;

    /// <summary>
    /// Whether this data source targets an in-memory database.
    /// </summary>
    public bool IsInMemory => _isInMemory;

    protected override DbConnection CreateDbConnection()
    {
        EnsureKeepAlive();
        return new SqliteConnection(_connectionString);
    }

    protected override async ValueTask<DbConnection> OpenDbConnectionAsync(CancellationToken cancellationToken = default)
    {
        EnsureKeepAlive();
        var conn = new SqliteConnection(_connectionString);
        try
        {
            await conn.OpenAsync(cancellationToken).ConfigureAwait(false);
            await _pragmaSettings.ApplyConnectionPragmasAsync(conn, cancellationToken).ConfigureAwait(false);
            await EnsureDatabasePragmasAsync(conn, cancellationToken).ConfigureAwait(false);
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
        var conn = new SqliteConnection(_connectionString);
        try
        {
            conn.Open();

            // PRAGMAs execute locally with no async I/O, so a genuinely synchronous
            // execution is safe here and avoids blocking on async machinery
            _pragmaSettings.ApplyConnectionPragmas(conn);
            if (_isInMemory || !_databasePragmasApplied)
            {
                _pragmaSettings.ApplyDatabasePragmas(conn);
                _databasePragmasApplied = true;
            }
        }
        catch
        {
            conn.Dispose();
            throw;
        }

        return conn;
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

        _keepAliveConnection = new SqliteConnection(_connectionString);
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
