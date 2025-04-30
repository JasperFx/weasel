using ImTools;
using JasperFx.Core;
using Npgsql;
using Weasel.Core.Migrations;
using Weasel.Postgresql.Connections;

namespace Weasel.Postgresql.Migrations;

/// <summary>
///     This models the condition of having multiple databases by name in the same
///     Postgresql database instance
/// </summary>
/// <typeparam name="T"></typeparam>
public abstract class SingleServerDatabaseCollection<T> where T : PostgresqlDatabase
{
    private readonly INpgsqlDataSourceFactory _dataSourceFactory;
    private readonly string _masterConnectionString;
    private readonly TimedLock _lock = new();
    private ImHashMap<string, T> _databases = ImHashMap<string, T>.Empty;

    protected SingleServerDatabaseCollection(INpgsqlDataSourceFactory dataSourceFactory, string masterConnectionString)
    {
        _dataSourceFactory = dataSourceFactory;
        _masterConnectionString = masterConnectionString;
    }

    static
        private DatabaseSpecification Specification { get; } = new();

    /// <summary>
    ///     Force the database to be dropped and re-created
    /// </summary>
    public bool DropAndRecreate { get; set; } = false;

    public IReadOnlyList<T> AllDatabases()
    {
        return _databases.Enumerate().Select(x => x.Value).ToList();
    }

    protected abstract T buildDatabase(string databaseName, NpgsqlDataSource dataSource);

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

            await using var conn = new NpgsqlConnection(_masterConnectionString);
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

            database = buildDatabase(
                databaseName,
                _dataSourceFactory.Create(_masterConnectionString, databaseName)
            );

            _databases = _databases.AddOrUpdate(databaseName, database);

            return database;
        }
    }
}
