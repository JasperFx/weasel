using JasperFx.Core;
using Npgsql;
using Weasel.Core.Migrations;

namespace Weasel.Postgresql.Migrations;

/// <summary>
///     This models the condition of having multiple databases by name in the same
///     Postgresql database instance
/// </summary>
/// <typeparam name="T"></typeparam>
public abstract class SingleServerDatabaseCollection<T> where T : PostgresqlDatabase
{
    private readonly NpgsqlDataSource? npgsqlDataSource;
    private readonly TimedLock _lock = new();
    private readonly string _masterConnectionString;
    private ImHashMap<string, T> _databases = ImHashMap<string, T>.Empty;

    protected SingleServerDatabaseCollection(NpgsqlDataSource npgsqlDataSource)
    {
        this.npgsqlDataSource = npgsqlDataSource;
        _masterConnectionString = npgsqlDataSource.ConnectionString;
    }

    protected SingleServerDatabaseCollection(string masterConnectionString)
    {
        _masterConnectionString = masterConnectionString;
    }

    private DatabaseSpecification Specification { get; } = new();

    /// <summary>
    ///     Force the database to be dropped and re-created
    /// </summary>
    public bool DropAndRecreate { get; set; } = false;

    public IReadOnlyList<T> AllDatabases()
    {
        return _databases.Enumerate().Select(x => x.Value).ToList();
    }

    protected abstract T buildDatabase(string databaseName, string connectionString);

    public virtual async ValueTask<T> FindOrCreateDatabase(string databaseName, CancellationToken ct = default)
    {
        if (_databases.TryFind(databaseName, out var database))
        {
            return database;
        }

        using (await _lock.Lock(5.Seconds(), ct).ConfigureAwait(false))
        {
            if (_databases.TryFind(databaseName, out database))
            {
                return database;
            }

            await using var conn =
                npgsqlDataSource?.CreateConnection()
                ?? new NpgsqlConnection(_masterConnectionString);
            await conn.OpenAsync(ct).ConfigureAwait(false);

            if (DropAndRecreate)
            {
                await conn.KillIdleSessions(databaseName, ct: ct).ConfigureAwait(false);
                await conn.DropDatabase(databaseName, ct: ct).ConfigureAwait(false);
            }
            else if (!await conn.DatabaseExists(databaseName, ct: ct).ConfigureAwait(false))
            {
                await Specification.BuildDatabase(conn, databaseName, ct).ConfigureAwait(false);
            }

            var builder = new NpgsqlConnectionStringBuilder(_masterConnectionString) { Database = databaseName };

            var connectionString = builder.ConnectionString;
            database = buildDatabase(databaseName, connectionString);

            _databases = _databases.AddOrUpdate(databaseName, database);

            return database;
        }
    }
}
