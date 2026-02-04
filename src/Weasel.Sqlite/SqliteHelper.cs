using Microsoft.Data.Sqlite;

namespace Weasel.Sqlite;

/// <summary>
/// Helper methods for SQLite database setup and configuration.
/// </summary>
public static class SqliteHelper
{
    /// <summary>
    /// Create and configure a SQLite database connection with PRAGMA settings.
    /// </summary>
    /// <param name="connectionString">The connection string for the main database</param>
    /// <param name="configurePragmas">Optional action to configure PRAGMA settings (defaults to SqlitePragmaSettings.Default)</param>
    /// <param name="ct">Cancellation token</param>
    /// <returns>An open SqliteConnection with all settings applied</returns>
    public static async Task<SqliteConnection> CreateConnectionAsync(
        string connectionString,
        Action<SqlitePragmaSettings>? configurePragmas = null,
        CancellationToken ct = default)
    {
        var connection = new SqliteConnection(connectionString);
        await connection.OpenAsync(ct).ConfigureAwait(false);

        // Apply PRAGMA settings
        var settings = SqlitePragmaSettings.Default;
        configurePragmas?.Invoke(settings);
        await settings.ApplyToConnectionAsync(connection, ct).ConfigureAwait(false);

        return connection;
    }

    /// <summary>
    /// Create a new SqliteMigrator.
    /// </summary>
    /// <returns>A new SqliteMigrator instance</returns>
    public static SqliteMigrator CreateMigrator()
    {
        return new SqliteMigrator();
    }
}
