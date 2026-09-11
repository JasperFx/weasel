namespace Weasel.Sqlite;

/// <summary>
/// Configuration for SQLite extensions that should be loaded when opening database connections.
/// Extensions provide additional functionality like spatial data support, full-text search, or custom functions.
/// </summary>
/// <remarks>
/// Hand an instance to <see cref="SqliteDataSource" /> and every connection it opens loads the
/// configured extensions, in order, right after the connection-scoped PRAGMAs and before any
/// <see cref="Functions.SqliteFunctionRegistry" /> functions are registered. Loaded extensions live
/// only for the connection's lifetime, which is why this is a per-open step and not a schema object.
/// </remarks>
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
    /// <remarks>
    /// Loading is a synchronous native call in Microsoft.Data.Sqlite, so this is the real
    /// implementation and <see cref="ApplyToConnectionAsync" /> is a thin wrapper over it.
    /// Extension loading is enabled only for the duration of the loads and disabled again
    /// afterwards; with nothing configured the connection is not touched at all.
    /// </remarks>
    public void ApplyToConnection(Microsoft.Data.Sqlite.SqliteConnection connection)
    {
        if (connection == null)
        {
            throw new ArgumentNullException(nameof(connection));
        }

        if (connection.State != System.Data.ConnectionState.Open)
        {
            throw new InvalidOperationException("Connection must be open before loading extensions");
        }

        if (Extensions.Count == 0)
        {
            return;
        }

        // EnableExtensions is required before LoadExtension can be called
        connection.EnableExtensions(true);
        try
        {
            foreach (var extension in Extensions)
            {
                try
                {
                    if (extension.EntryPoint != null)
                    {
                        connection.LoadExtension(extension.LibraryPath, extension.EntryPoint);
                    }
                    else
                    {
                        connection.LoadExtension(extension.LibraryPath);
                    }
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
        }
        finally
        {
            // Disable extension loading after all extensions are loaded for security
            connection.EnableExtensions(false);
        }
    }

    /// <summary>
    /// Apply extension settings to a connection by loading all configured extensions.
    /// </summary>
    /// <param name="connection">The connection to load extensions into. Must be open.</param>
    /// <param name="ct">Cancellation token, checked before loading begins.</param>
    public Task ApplyToConnectionAsync(Microsoft.Data.Sqlite.SqliteConnection connection, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        ApplyToConnection(connection);
        return Task.CompletedTask;
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
