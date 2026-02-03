namespace Weasel.Sqlite;

/// <summary>
/// Configuration for SQLite extensions that should be loaded when opening database connections.
/// Extensions provide additional functionality like spatial data support, full-text search, or custom functions.
/// </summary>
public class SqliteExtensionSettings
{
    /// <summary>
    /// List of extensions to load when opening connections.
    /// Each extension specifies the library path and optional entry point.
    /// </summary>
    public List<SqliteExtension> Extensions { get; } = new();

    /// <summary>
    /// Add an extension to be loaded.
    /// </summary>
    /// <param name="libraryPath">Path to the shared library containing the extension.
    /// Can be a full path or library name if in system path (PATH/LD_LIBRARY_PATH/DYLD_LIBRARY_PATH).</param>
    /// <param name="entryPoint">Optional entry point function name. If null, SQLite uses default conventions.</param>
    public void AddExtension(string libraryPath, string? entryPoint = null)
    {
        if (string.IsNullOrWhiteSpace(libraryPath))
        {
            throw new ArgumentException("Library path cannot be null or empty", nameof(libraryPath));
        }

        Extensions.Add(new SqliteExtension(libraryPath, entryPoint));
    }

    /// <summary>
    /// Apply extension settings to a connection by loading all configured extensions.
    /// </summary>
    /// <param name="connection">The connection to load extensions into. Must be open.</param>
    /// <param name="ct">Cancellation token</param>
    public async Task ApplyToConnectionAsync(Microsoft.Data.Sqlite.SqliteConnection connection, CancellationToken ct = default)
    {
        if (connection == null)
        {
            throw new ArgumentNullException(nameof(connection));
        }

        if (connection.State != System.Data.ConnectionState.Open)
        {
            throw new InvalidOperationException("Connection must be open before loading extensions");
        }

        // EnableExtensions is required before LoadExtension can be called
        connection.EnableExtensions(true);

        foreach (var extension in Extensions)
        {
            try
            {
                // LoadExtension is synchronous in Microsoft.Data.Sqlite
                // but we'll wrap it in Task.Run to be awaitable and respect cancellation
                await Task.Run(() =>
                {
                    ct.ThrowIfCancellationRequested();

                    if (extension.EntryPoint != null)
                    {
                        connection.LoadExtension(extension.LibraryPath, extension.EntryPoint);
                    }
                    else
                    {
                        connection.LoadExtension(extension.LibraryPath);
                    }
                }, ct).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException(
                    $"Failed to load SQLite extension '{extension.LibraryPath}'" +
                    (extension.EntryPoint != null ? $" with entry point '{extension.EntryPoint}'" : "") +
                    ". Ensure the extension library is in the system path (PATH/LD_LIBRARY_PATH/DYLD_LIBRARY_PATH) " +
                    "or provide a full path to the library file.", ex);
            }
        }

        // Disable extension loading after all extensions are loaded for security
        connection.EnableExtensions(false);
    }
}

/// <summary>
/// Represents a SQLite extension to be loaded.
/// </summary>
public record SqliteExtension
{
    /// <summary>
    /// Path to the shared library containing the extension.
    /// Can be a full path or library name if in system path.
    /// </summary>
    public string LibraryPath { get; }

    /// <summary>
    /// Optional entry point function name.
    /// If null, SQLite uses default naming conventions (sqlite3_[name]_init).
    /// </summary>
    public string? EntryPoint { get; }

    public SqliteExtension(string libraryPath, string? entryPoint = null)
    {
        if (string.IsNullOrWhiteSpace(libraryPath))
        {
            throw new ArgumentException("Library path cannot be null or empty", nameof(libraryPath));
        }

        LibraryPath = libraryPath;
        EntryPoint = entryPoint;
    }
}
