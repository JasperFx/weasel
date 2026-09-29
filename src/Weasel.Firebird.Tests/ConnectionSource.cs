using FirebirdSql.Data.FirebirdClient;

namespace Weasel.Firebird.Tests;

public static class ConnectionSource
{
    /// <summary>
    ///     The server under test. Point it at Firebird 3, 4 or 5 -- the compose file publishes them on
    ///     3063, 3064 and 3065 -- and the suite runs against that one. The database file it names is only
    ///     a directory: each test class gets a file of its own beside it.
    /// </summary>
    /// <remarks>
    ///     The path is absolute because Firebird does not resolve a relative database name against its
    ///     data directory, and the connection's character set becomes the default of every database the
    ///     suite creates.
    /// </remarks>
    public static readonly string ConnectionString =
        Environment.GetEnvironmentVariable("weasel_firebird_testing_database")
        ?? "DataSource=localhost;Port=3065;Database=/var/lib/firebird/data/weasel_testing.fdb;User=SYSDBA;Password=P@55w0rd;Charset=UTF8";

    /// <summary>
    ///     The connection string for a database file of its own, <paramref name="name" />.fdb, in the
    ///     directory the configured connection string names.
    /// </summary>
    public static string ForDatabase(string name, string? charset = null)
    {
        var builder = new FbConnectionStringBuilder(ConnectionString);
        var path = builder.Database;
        var separator = Math.Max(path.LastIndexOf('/'), path.LastIndexOf('\\'));
        builder.Database = $"{path[..(separator + 1)]}{name.ToLowerInvariant()}.fdb";

        if (charset != null)
        {
            builder.Charset = charset;
        }

        return builder.ConnectionString;
    }

    /// <summary>
    ///     Create (or replace) the database a connection string names, with the 16 KB pages Firebird needs
    ///     for keys over long UTF8 columns. Forced writes are off: nothing here has to survive a crash.
    /// </summary>
    public static async Task CreateDatabaseAsync(string connectionString)
    {
        FbConnection.ClearAllPools();
        await FbConnection.CreateDatabaseAsync(connectionString, 16384, false, true);
    }

    private static FirebirdServerVersion? _version;

    /// <summary>
    ///     The version of the server under test.
    /// </summary>
    public static async Task<FirebirdServerVersion> ServerVersionAsync()
    {
        if (_version.HasValue)
        {
            return _version.Value;
        }

        var connectionString = ForDatabase("server_version");
        await CreateDatabaseAsync(connectionString);

        await using var conn = new FbConnection(connectionString);
        await conn.OpenAsync();
        _version = FirebirdServerVersion.Of(conn);

        return _version!.Value;
    }
}
