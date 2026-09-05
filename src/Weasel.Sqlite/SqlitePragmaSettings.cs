namespace Weasel.Sqlite;

/// <summary>
/// Configuration for SQLite PRAGMA settings optimized for performance and reliability.
/// These settings are applied when creating or opening a database connection.
/// </summary>
/// <remarks>
///     <para>
///         PRAGMAs are split by their actual scope:
///     </para>
///     <list type="bullet">
///         <item>
///             <b>Database-file scoped</b> (<see cref="DatabasePragmaSql" />): <c>page_size</c> and
///             <c>auto_vacuum</c> are properties of the database file that can only take effect before
///             the file is first written (or via VACUUM), and <c>journal_mode = WAL</c> is persisted in
///             the database file header. These only need to be issued once per database file —
///             re-issuing <c>journal_mode = WAL</c> on every connection open forces SQLite to take
///             locks to verify/convert the journal mode and can return SQLITE_BUSY under a concurrent
///             writer.
///         </item>
///         <item>
///             <b>Connection scoped</b> (<see cref="ConnectionPragmaSql" />): everything else,
///             including non-WAL journal modes (DELETE/TRUNCATE/PERSIST/MEMORY/OFF are per-connection
///             settings) and <c>wal_autocheckpoint</c> (a per-connection setting even though it
///             operates on the WAL file). These must be applied to every new connection.
///         </item>
///     </list>
///     <para>
///         Both batch strings are computed lazily and cached; mutating any setting invalidates the
///         cache, so opening a connection does not rebuild the PRAGMA SQL from scratch each time.
///     </para>
/// </remarks>
public class SqlitePragmaSettings
{
    private JournalMode _journalMode = JournalMode.WAL;
    private SynchronousMode _synchronous = SynchronousMode.NORMAL;
    private int _cacheSize = -64000;
    private TempStoreMode _tempStore = TempStoreMode.MEMORY;
    private long _mmapSize = 268435456; // 256MB
    private int _pageSize = 4096;
    private bool _foreignKeys = true;
    private AutoVacuumMode _autoVacuum = AutoVacuumMode.INCREMENTAL;
    private int _busyTimeout = 5000;
    private bool _secureDelete;
    private bool _caseSensitiveLike;
    private int? _walAutoCheckpoint;

    // Cached batch SQL, invalidated whenever a setting is mutated. Reads/writes of a
    // reference are atomic, so the worst case under concurrent mutation is a redundant rebuild.
    private string? _connectionPragmaSql;
    private string? _databasePragmaSql;

    /// <summary>
    /// Default optimized settings for general-purpose applications.
    /// </summary>
    public static SqlitePragmaSettings Default => new()
    {
        JournalMode = JournalMode.WAL,
        Synchronous = SynchronousMode.NORMAL,
        CacheSize = -64000, // 64MB
        TempStore = TempStoreMode.MEMORY,
        MmapSize = 268435456, // 256MB
        PageSize = 4096,
        ForeignKeys = true,
        AutoVacuum = AutoVacuumMode.INCREMENTAL,
        BusyTimeout = 5000,
        SecureDelete = false,
        CaseSensitiveLike = false
    };

    /// <summary>
    /// Settings optimized for maximum performance (potentially less safe).
    /// </summary>
    public static SqlitePragmaSettings HighPerformance => new()
    {
        JournalMode = JournalMode.WAL,
        Synchronous = SynchronousMode.OFF, // Faster but risk of corruption on power loss
        CacheSize = -128000, // 128MB
        TempStore = TempStoreMode.MEMORY,
        MmapSize = 536870912, // 512MB
        PageSize = 4096,
        ForeignKeys = true,
        AutoVacuum = AutoVacuumMode.NONE,
        BusyTimeout = 5000,
        SecureDelete = false,
        CaseSensitiveLike = false
    };

    /// <summary>
    /// Settings optimized for maximum safety and durability.
    /// </summary>
    public static SqlitePragmaSettings HighSafety => new()
    {
        JournalMode = JournalMode.WAL,
        Synchronous = SynchronousMode.FULL,
        CacheSize = -32000, // 32MB
        TempStore = TempStoreMode.MEMORY,
        MmapSize = 134217728, // 128MB
        PageSize = 4096,
        ForeignKeys = true,
        AutoVacuum = AutoVacuumMode.FULL,
        BusyTimeout = 10000,
        SecureDelete = true,
        CaseSensitiveLike = false
    };

    /// <summary>
    /// Journal mode controls how transactions are stored.
    /// WAL (Write-Ahead Logging) is recommended for most use cases.
    /// WAL is persisted in the database file and only needs to be set once per database;
    /// all other journal modes are per-connection settings.
    /// </summary>
    public JournalMode JournalMode
    {
        get => _journalMode;
        set { _journalMode = value; invalidateCache(); }
    }

    /// <summary>
    /// Controls how often SQLite syncs to disk.
    /// NORMAL is a good balance between safety and performance.
    /// </summary>
    public SynchronousMode Synchronous
    {
        get => _synchronous;
        set { _synchronous = value; invalidateCache(); }
    }

    /// <summary>
    /// Cache size in kibibytes (negative) or pages (positive).
    /// Negative values specify size in KiB (e.g., -64000 = 64MB).
    /// </summary>
    public int CacheSize
    {
        get => _cacheSize;
        set { _cacheSize = value; invalidateCache(); }
    }

    /// <summary>
    /// Where temporary tables and indices are stored.
    /// MEMORY is fastest for most operations.
    /// </summary>
    public TempStoreMode TempStore
    {
        get => _tempStore;
        set { _tempStore = value; invalidateCache(); }
    }

    /// <summary>
    /// Maximum size of memory-mapped I/O in bytes.
    /// Can significantly improve read performance.
    /// </summary>
    public long MmapSize
    {
        get => _mmapSize;
        set { _mmapSize = value; invalidateCache(); }
    }

    /// <summary>
    /// Database page size in bytes. Must be a power of 2 between 512 and 65536.
    /// 4096 is optimal for most modern systems.
    /// Can only be set before the database is created.
    /// </summary>
    public int PageSize
    {
        get => _pageSize;
        set { _pageSize = value; invalidateCache(); }
    }

    /// <summary>
    /// Enable foreign key constraints. Strongly recommended.
    /// </summary>
    public bool ForeignKeys
    {
        get => _foreignKeys;
        set { _foreignKeys = value; invalidateCache(); }
    }

    /// <summary>
    /// Auto-vacuum mode for automatically reclaiming space.
    /// INCREMENTAL is a good balance.
    /// Can only take effect before the database file is created (or via VACUUM).
    /// </summary>
    public AutoVacuumMode AutoVacuum
    {
        get => _autoVacuum;
        set { _autoVacuum = value; invalidateCache(); }
    }

    /// <summary>
    /// Timeout in milliseconds when database is locked.
    /// </summary>
    public int BusyTimeout
    {
        get => _busyTimeout;
        set { _busyTimeout = value; invalidateCache(); }
    }

    /// <summary>
    /// Whether to securely delete data (overwrite with zeros).
    /// Slower but more secure.
    /// </summary>
    public bool SecureDelete
    {
        get => _secureDelete;
        set { _secureDelete = value; invalidateCache(); }
    }

    /// <summary>
    /// Whether LIKE operator is case-sensitive.
    /// Default is case-insensitive for ASCII characters.
    /// </summary>
    public bool CaseSensitiveLike
    {
        get => _caseSensitiveLike;
        set { _caseSensitiveLike = value; invalidateCache(); }
    }

    /// <summary>
    /// WAL auto-checkpoint threshold (number of pages).
    /// NULL means use default (1000 pages).
    /// </summary>
    public int? WalAutoCheckpoint
    {
        get => _walAutoCheckpoint;
        set { _walAutoCheckpoint = value; invalidateCache(); }
    }

    private void invalidateCache()
    {
        _connectionPragmaSql = null;
        _databasePragmaSql = null;
    }

    /// <summary>
    ///     The batch of connection-scoped PRAGMA statements that must be applied to every new
    ///     connection. Computed once and cached until a setting is mutated.
    /// </summary>
    public string ConnectionPragmaSql => _connectionPragmaSql ??= buildBatchSql(connectionPragmas());

    /// <summary>
    ///     The batch of database-file-scoped PRAGMA statements (<c>page_size</c>, <c>auto_vacuum</c>,
    ///     and <c>journal_mode</c> when WAL) that only need to be issued once per database file.
    ///     Computed once and cached until a setting is mutated.
    /// </summary>
    public string DatabasePragmaSql => _databasePragmaSql ??= buildBatchSql(databasePragmas());

    /// <summary>
    ///     The database-file-scoped PRAGMA statements. <c>page_size</c> is deliberately ordered
    ///     before <c>journal_mode = WAL</c>: the page size of a brand-new database cannot be changed
    ///     once the database has entered WAL mode.
    /// </summary>
    private IEnumerable<string> databasePragmas()
    {
        yield return $"PRAGMA page_size = {PageSize}";
        yield return $"PRAGMA auto_vacuum = {(int)AutoVacuum}";

        if (JournalMode == JournalMode.WAL)
        {
            // WAL is persisted in the database file header, so it is a property of the
            // database file rather than of any one connection
            yield return "PRAGMA journal_mode = WAL";
        }
    }

    /// <summary>
    ///     The connection-scoped PRAGMA statements. <c>busy_timeout</c> comes first so that any
    ///     subsequent statement that has to take a file lock benefits from it.
    /// </summary>
    private IEnumerable<string> connectionPragmas()
    {
        yield return $"PRAGMA busy_timeout = {BusyTimeout}";

        if (JournalMode != JournalMode.WAL)
        {
            // DELETE/TRUNCATE/PERSIST/MEMORY/OFF are per-connection settings and are not
            // persisted in the database file, so they have to be applied on every open
            yield return $"PRAGMA journal_mode = {JournalMode.ToString().ToUpperInvariant()}";
        }

        yield return $"PRAGMA synchronous = {Synchronous.ToString().ToUpperInvariant()}";
        yield return $"PRAGMA cache_size = {CacheSize}";
        yield return $"PRAGMA temp_store = {(int)TempStore}";
        yield return $"PRAGMA mmap_size = {MmapSize}";
        yield return $"PRAGMA foreign_keys = {(ForeignKeys ? "ON" : "OFF")}";
        yield return $"PRAGMA secure_delete = {(SecureDelete ? "ON" : "OFF")}";
        yield return $"PRAGMA case_sensitive_like = {(CaseSensitiveLike ? "ON" : "OFF")}";

        if (WalAutoCheckpoint.HasValue && JournalMode == JournalMode.WAL)
        {
            // wal_autocheckpoint is a per-connection setting (sqlite3_wal_autocheckpoint)
            // even though it operates on the shared WAL file
            yield return $"PRAGMA wal_autocheckpoint = {WalAutoCheckpoint.Value}";
        }
    }

    private static string buildBatchSql(IEnumerable<string> pragmas)
    {
        return string.Join(";\n", pragmas) + ";";
    }

    /// <summary>
    /// Apply all of these PRAGMA settings — database-file scoped and connection scoped — to a
    /// connection. All PRAGMA statements are executed in a single batch for better performance.
    /// Prefer <see cref="SqliteDataSource" />, which applies the database-file-scoped PRAGMAs only
    /// once instead of on every open.
    /// </summary>
    public async Task ApplyToConnectionAsync(Microsoft.Data.Sqlite.SqliteConnection connection, CancellationToken ct = default)
    {
        if (connection == null)
        {
            throw new ArgumentNullException(nameof(connection));
        }

        // Connection PRAGMAs first so busy_timeout is in force before the journal-mode
        // conversion in the database batch has to take file locks
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = ConnectionPragmaSql + "\n" + DatabasePragmaSql;
        await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Synchronous equivalent of <see cref="ApplyToConnectionAsync" />. PRAGMA statements execute
    /// locally with no network I/O, so a genuinely synchronous execution is safe and avoids
    /// blocking on async machinery.
    /// </summary>
    public void ApplyToConnection(Microsoft.Data.Sqlite.SqliteConnection connection)
    {
        if (connection == null)
        {
            throw new ArgumentNullException(nameof(connection));
        }

        using var cmd = connection.CreateCommand();
        cmd.CommandText = ConnectionPragmaSql + "\n" + DatabasePragmaSql;
        cmd.ExecuteNonQuery();
    }

    /// <summary>
    /// Apply only the connection-scoped PRAGMA settings. Used by <see cref="SqliteDataSource" />
    /// on every connection open.
    /// </summary>
    public virtual async Task ApplyConnectionPragmasAsync(Microsoft.Data.Sqlite.SqliteConnection connection, CancellationToken ct = default)
    {
        if (connection == null)
        {
            throw new ArgumentNullException(nameof(connection));
        }

        await using var cmd = connection.CreateCommand();
        cmd.CommandText = ConnectionPragmaSql;
        await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Synchronous equivalent of <see cref="ApplyConnectionPragmasAsync" />.
    /// </summary>
    public virtual void ApplyConnectionPragmas(Microsoft.Data.Sqlite.SqliteConnection connection)
    {
        if (connection == null)
        {
            throw new ArgumentNullException(nameof(connection));
        }

        using var cmd = connection.CreateCommand();
        cmd.CommandText = ConnectionPragmaSql;
        cmd.ExecuteNonQuery();
    }

    /// <summary>
    /// Apply only the database-file-scoped PRAGMA settings. Used by <see cref="SqliteDataSource" />
    /// once per data source rather than on every connection open.
    /// </summary>
    public virtual async Task ApplyDatabasePragmasAsync(Microsoft.Data.Sqlite.SqliteConnection connection, CancellationToken ct = default)
    {
        if (connection == null)
        {
            throw new ArgumentNullException(nameof(connection));
        }

        await using var cmd = connection.CreateCommand();
        cmd.CommandText = DatabasePragmaSql;
        await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Synchronous equivalent of <see cref="ApplyDatabasePragmasAsync" />.
    /// </summary>
    public virtual void ApplyDatabasePragmas(Microsoft.Data.Sqlite.SqliteConnection connection)
    {
        if (connection == null)
        {
            throw new ArgumentNullException(nameof(connection));
        }

        using var cmd = connection.CreateCommand();
        cmd.CommandText = DatabasePragmaSql;
        cmd.ExecuteNonQuery();
    }

    /// <summary>
    /// Generate SQL script with all PRAGMA settings.
    /// Useful for diagnostics and documentation.
    /// </summary>
    public string ToSqlScript()
    {
        var lines = new List<string> { "-- SQLite PRAGMA Settings" };
        lines.AddRange(connectionPragmas().Select(p => p + ";"));
        lines.AddRange(databasePragmas().Select(p => p + ";"));

        return string.Join(Environment.NewLine, lines);
    }
}

/// <summary>
/// SQLite journal mode options.
/// </summary>
public enum JournalMode
{
    /// <summary>
    /// Delete journal file after each transaction (default).
    /// </summary>
    DELETE,

    /// <summary>
    /// Truncate journal file to zero length instead of deleting.
    /// </summary>
    TRUNCATE,

    /// <summary>
    /// Persist journal file and overwrite with zeros.
    /// </summary>
    PERSIST,

    /// <summary>
    /// Store journal in memory (fastest, but no crash recovery).
    /// </summary>
    MEMORY,

    /// <summary>
    /// Write-Ahead Logging (recommended for most use cases).
    /// Allows concurrent readers with a single writer.
    /// </summary>
    WAL,

    /// <summary>
    /// No journal (fastest, but no rollback or crash recovery).
    /// </summary>
    OFF
}

/// <summary>
/// SQLite synchronous mode options.
/// </summary>
public enum SynchronousMode
{
    /// <summary>
    /// No syncing (fastest, but risk of corruption).
    /// </summary>
    OFF,

    /// <summary>
    /// Sync only at critical moments (good balance).
    /// Recommended for WAL mode.
    /// </summary>
    NORMAL,

    /// <summary>
    /// Sync at every critical moment (safest, slowest).
    /// </summary>
    FULL,

    /// <summary>
    /// Like FULL but uses F_FULLFSYNC on systems that support it.
    /// </summary>
    EXTRA
}

/// <summary>
/// SQLite temp_store mode options.
/// </summary>
public enum TempStoreMode
{
    /// <summary>
    /// Use compile-time default.
    /// </summary>
    DEFAULT = 0,

    /// <summary>
    /// Store temp tables/indices on disk.
    /// </summary>
    FILE = 1,

    /// <summary>
    /// Store temp tables/indices in memory (recommended).
    /// </summary>
    MEMORY = 2
}

/// <summary>
/// SQLite auto_vacuum mode options.
/// </summary>
public enum AutoVacuumMode
{
    /// <summary>
    /// No auto-vacuum. Use VACUUM command manually.
    /// </summary>
    NONE = 0,

    /// <summary>
    /// Automatically reclaim space when data is deleted.
    /// </summary>
    FULL = 1,

    /// <summary>
    /// Make space available for reuse but don't shrink file.
    /// Use pragma_incremental_vacuum to shrink.
    /// </summary>
    INCREMENTAL = 2
}
