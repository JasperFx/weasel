namespace Weasel.Sqlite;

/// <summary>
/// Configuration for SQLite PRAGMA settings optimized for performance and reliability.
/// These settings are applied when creating or opening a database connection.
/// </summary>
public class SqlitePragmaSettings
{
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
    /// </summary>
    public JournalMode JournalMode { get; set; } = JournalMode.WAL;

    /// <summary>
    /// Controls how often SQLite syncs to disk.
    /// NORMAL is a good balance between safety and performance.
    /// </summary>
    public SynchronousMode Synchronous { get; set; } = SynchronousMode.NORMAL;

    /// <summary>
    /// Cache size in kibibytes (negative) or pages (positive).
    /// Negative values specify size in KiB (e.g., -64000 = 64MB).
    /// </summary>
    public int CacheSize { get; set; } = -64000;

    /// <summary>
    /// Where temporary tables and indices are stored.
    /// MEMORY is fastest for most operations.
    /// </summary>
    public TempStoreMode TempStore { get; set; } = TempStoreMode.MEMORY;

    /// <summary>
    /// Maximum size of memory-mapped I/O in bytes.
    /// Can significantly improve read performance.
    /// </summary>
    public long MmapSize { get; set; } = 268435456; // 256MB

    /// <summary>
    /// Database page size in bytes. Must be a power of 2 between 512 and 65536.
    /// 4096 is optimal for most modern systems.
    /// Can only be set before the database is created.
    /// </summary>
    public int PageSize { get; set; } = 4096;

    /// <summary>
    /// Enable foreign key constraints. Strongly recommended.
    /// </summary>
    public bool ForeignKeys { get; set; } = true;

    /// <summary>
    /// Auto-vacuum mode for automatically reclaiming space.
    /// INCREMENTAL is a good balance.
    /// </summary>
    public AutoVacuumMode AutoVacuum { get; set; } = AutoVacuumMode.INCREMENTAL;

    /// <summary>
    /// Timeout in milliseconds when database is locked.
    /// </summary>
    public int BusyTimeout { get; set; } = 5000;

    /// <summary>
    /// Whether to securely delete data (overwrite with zeros).
    /// Slower but more secure.
    /// </summary>
    public bool SecureDelete { get; set; } = false;

    /// <summary>
    /// Whether LIKE operator is case-sensitive.
    /// Default is case-insensitive for ASCII characters.
    /// </summary>
    public bool CaseSensitiveLike { get; set; } = false;

    /// <summary>
    /// WAL auto-checkpoint threshold (number of pages).
    /// NULL means use default (1000 pages).
    /// </summary>
    public int? WalAutoCheckpoint { get; set; }

    /// <summary>
    /// Apply these PRAGMA settings to a connection.
    /// </summary>
    public async Task ApplyToConnectionAsync(Microsoft.Data.Sqlite.SqliteConnection connection, CancellationToken ct = default)
    {
        if (connection == null)
        {
            throw new ArgumentNullException(nameof(connection));
        }

        // Journal mode
        await ExecutePragmaAsync(connection, $"PRAGMA journal_mode = {JournalMode.ToString().ToUpperInvariant()}", ct).ConfigureAwait(false);

        // Synchronous mode
        await ExecutePragmaAsync(connection, $"PRAGMA synchronous = {Synchronous.ToString().ToUpperInvariant()}", ct).ConfigureAwait(false);

        // Cache size
        await ExecutePragmaAsync(connection, $"PRAGMA cache_size = {CacheSize}", ct).ConfigureAwait(false);

        // Temp store
        await ExecutePragmaAsync(connection, $"PRAGMA temp_store = {(int)TempStore}", ct).ConfigureAwait(false);

        // Memory-mapped I/O
        await ExecutePragmaAsync(connection, $"PRAGMA mmap_size = {MmapSize}", ct).ConfigureAwait(false);

        // Page size (only effective before database is created)
        await ExecutePragmaAsync(connection, $"PRAGMA page_size = {PageSize}", ct).ConfigureAwait(false);

        // Foreign keys
        await ExecutePragmaAsync(connection, $"PRAGMA foreign_keys = {(ForeignKeys ? "ON" : "OFF")}", ct).ConfigureAwait(false);

        // Auto vacuum
        await ExecutePragmaAsync(connection, $"PRAGMA auto_vacuum = {(int)AutoVacuum}", ct).ConfigureAwait(false);

        // Busy timeout
        await ExecutePragmaAsync(connection, $"PRAGMA busy_timeout = {BusyTimeout}", ct).ConfigureAwait(false);

        // Secure delete
        await ExecutePragmaAsync(connection, $"PRAGMA secure_delete = {(SecureDelete ? "ON" : "OFF")}", ct).ConfigureAwait(false);

        // Case sensitive LIKE
        await ExecutePragmaAsync(connection, $"PRAGMA case_sensitive_like = {(CaseSensitiveLike ? "ON" : "OFF")}", ct).ConfigureAwait(false);

        // WAL auto-checkpoint
        if (WalAutoCheckpoint.HasValue && JournalMode == JournalMode.WAL)
        {
            await ExecutePragmaAsync(connection, $"PRAGMA wal_autocheckpoint = {WalAutoCheckpoint.Value}", ct).ConfigureAwait(false);
        }
    }

    private static async Task ExecutePragmaAsync(Microsoft.Data.Sqlite.SqliteConnection connection, string pragma, CancellationToken ct)
    {
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = pragma;
        await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Generate SQL script with all PRAGMA settings.
    /// Useful for diagnostics and documentation.
    /// </summary>
    public string ToSqlScript()
    {
        var lines = new List<string>
        {
            "-- SQLite PRAGMA Settings",
            $"PRAGMA journal_mode = {JournalMode.ToString().ToUpperInvariant()};",
            $"PRAGMA synchronous = {Synchronous.ToString().ToUpperInvariant()};",
            $"PRAGMA cache_size = {CacheSize};",
            $"PRAGMA temp_store = {(int)TempStore};",
            $"PRAGMA mmap_size = {MmapSize};",
            $"PRAGMA page_size = {PageSize};",
            $"PRAGMA foreign_keys = {(ForeignKeys ? "ON" : "OFF")};",
            $"PRAGMA auto_vacuum = {(int)AutoVacuum};",
            $"PRAGMA busy_timeout = {BusyTimeout};",
            $"PRAGMA secure_delete = {(SecureDelete ? "ON" : "OFF")};",
            $"PRAGMA case_sensitive_like = {(CaseSensitiveLike ? "ON" : "OFF")};"
        };

        if (WalAutoCheckpoint.HasValue && JournalMode == JournalMode.WAL)
        {
            lines.Add($"PRAGMA wal_autocheckpoint = {WalAutoCheckpoint.Value};");
        }

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
