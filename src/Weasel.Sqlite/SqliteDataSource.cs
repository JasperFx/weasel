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
/// </summary>
public class SqliteDataSource : DbDataSource
{
    private readonly string _connectionString;
    private readonly SqlitePragmaSettings _pragmaSettings;
    private SqliteConnection? _keepAliveConnection;
    private readonly bool _isInMemory;

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
        await conn.OpenAsync(cancellationToken).ConfigureAwait(false);
        await _pragmaSettings.ApplyToConnectionAsync(conn, cancellationToken).ConfigureAwait(false);
        return conn;
    }

    protected override DbConnection OpenDbConnection()
    {
        EnsureKeepAlive();
        var conn = new SqliteConnection(_connectionString);
        conn.Open();

        // Apply PRAGMAs synchronously — this is safe because SQLite PRAGMAs
        // execute locally with no async I/O.
#pragma warning disable VSTHRD002
        _pragmaSettings.ApplyToConnectionAsync(conn).GetAwaiter().GetResult();
#pragma warning restore VSTHRD002
        return conn;
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
