using ImTools;
using JasperFx.Core;
using Npgsql;
using Weasel.Core;
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
    private static readonly TimeSpan LockTimeout = 5.Seconds();

    private readonly INpgsqlDataSourceFactory _dataSourceFactory;
    private readonly string _masterConnectionString;
    private readonly TimedLock _lock = new();

    // weasel#839: the synchronous path's own lock. It never waits on _lock, which asynchronous callers
    // hold across awaits, so a thread in FindOrCreateDatabaseSynchronously never blocks on async work.
    // The two paths can therefore run at once for one name: CREATE DATABASE tolerates losing that
    // race (see isDuplicateDatabase), and _publish makes the cache entry one database either way.
    private readonly Lock _syncLock = new();

    // Held only around the cache re-check, buildDatabase and the publish -- never around I/O -- so
    // both paths take it, and whichever comes second gets the first one's database.
    private readonly Lock _publish = new();

    private ImHashMap<string, T> _databases = ImHashMap<string, T>.Empty;
    private bool? _masterTakesSynchronousIo;

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

    /// <summary>
    /// Does the underlying data source factory own the lifetime of the given data source? Subclasses should
    /// pass this through to the <see cref="PostgresqlDatabase"/> <c>ownsDataSource</c> constructor argument so
    /// that a caller-supplied, externally-owned data source is not disposed out from under other consumers.
    /// </summary>
    protected bool OwnsDataSource(NpgsqlDataSource dataSource) => _dataSourceFactory.OwnsDataSource(dataSource);

    public virtual async ValueTask<T> FindOrCreateDatabase(string databaseName, CancellationToken ct = default)
    {
        if (_databases.TryFind(databaseName, out var database))
        {
            return database;
        }

        using (await _lock.Lock(LockTimeout, $"creating the database '{databaseName}'", ct).ConfigureAwait(false))
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
                try
                {
                    await Specification.BuildDatabase(conn, databaseName, ct).ConfigureAwait(false);
                }
                catch (PostgresException e) when (isDuplicateDatabase(e))
                {
                    // Created since the check: by FindOrCreateDatabaseSynchronously, or by another process.
                }
            }

            return publish(databaseName);
        }
    }

    /// <summary>
    ///     The synchronous twin of <see cref="FindOrCreateDatabase" /> (weasel#839), for a caller that
    ///     must return a database without awaiting -- a store's synchronous tenant lookup -- and would
    ///     otherwise block a thread on <see cref="FindOrCreateDatabase" />. Under a burst of uncached
    ///     databases from pool threads that is thread-pool starvation (marten#5985).
    /// </summary>
    /// <remarks>
    ///     <para>
    ///     Same cache, same work: a cached database is returned at once; otherwise the existence check,
    ///     and <c>CREATE DATABASE</c> when it is missing (or the <see cref="DropAndRecreate" /> drop), run
    ///     over a synchronously opened connection, one statement per command, and the database is
    ///     cached for both paths.
    ///     </para>
    ///     <para>
    ///     Synchronous callers serialize on a lock of their own, never on the one asynchronous callers
    ///     hold across their awaits, so this never waits on asynchronous work. The two paths can create
    ///     the same database at once; the loser's <c>CREATE DATABASE</c> error is ignored, and both get the
    ///     one cached database. Like the asynchronous path, it gives up with a
    ///     <see cref="TimeoutException" /> after five seconds waiting for another synchronous caller.
    ///     </para>
    ///     <para>
    ///     When the master connection string sets <c>Multiplexing=true</c>, Npgsql refuses synchronous
    ///     I/O, and this falls back to blocking on <see cref="FindOrCreateDatabase" /> -- what the caller
    ///     would have done without it. It does not call an override of <see cref="FindOrCreateDatabase" />
    ///     otherwise; a subclass that overrides one should override both.
    ///     </para>
    ///     <para>
    ///     Named apart from <see cref="FindOrCreateDatabase" /> on purpose: an overload taking only the
    ///     name would capture every existing <c>FindOrCreateDatabase(name)</c> call.
    ///     </para>
    /// </remarks>
    public virtual T FindOrCreateDatabaseSynchronously(string databaseName)
    {
        if (_databases.TryFind(databaseName, out var database))
        {
            return database;
        }

        if (!masterTakesSynchronousIo())
        {
            // The one blocking wait, and only because Npgsql leaves no synchronous alternative under
            // multiplexing. It is what the caller did before this method existed.
#pragma warning disable VSTHRD002
            return FindOrCreateDatabase(databaseName, CancellationToken.None).AsTask().GetAwaiter().GetResult();
#pragma warning restore VSTHRD002
        }

        if (!_syncLock.TryEnter(LockTimeout))
        {
            throw new TimeoutException(
                $"Timed out after {LockTimeout.TotalSeconds:0.##}s waiting for another thread in this process to finish "
                + $"creating the database '{databaseName}'.");
        }

        try
        {
            if (_databases.TryFind(databaseName, out database))
            {
                return database;
            }

            using var conn = new NpgsqlConnection(_masterConnectionString);
            conn.Open();

            if (DropAndRecreate)
            {
                using (var kill = conn.CreateCommand(
                           "SELECT pg_terminate_backend(pg_stat_activity.pid) FROM pg_stat_activity WHERE pg_stat_activity.datname = :db AND pid <> pg_backend_pid();")
                           .With("db", databaseName))
                {
                    kill.ExecuteNonQuery();
                }

                using var drop = conn.CreateCommand($"DROP DATABASE IF EXISTS {databaseName}");
                drop.ExecuteNonQuery();
            }
            else if (!databaseExists(conn, databaseName))
            {
                try
                {
                    using var create = conn.CreateCommand(Specification.ToCreateDatabaseStatement(databaseName));
                    create.ExecuteNonQuery();
                }
                catch (PostgresException e) when (isDuplicateDatabase(e))
                {
                    // Created since the check: by FindOrCreateDatabase, or by another process.
                }
            }

            return publish(databaseName);
        }
        finally
        {
            _syncLock.Exit();
        }
    }

    private T publish(string databaseName)
    {
        lock (_publish)
        {
            if (_databases.TryFind(databaseName, out var existing))
            {
                return existing;
            }

            var database = buildDatabase(
                databaseName,
                _dataSourceFactory.Create(_masterConnectionString, databaseName)
            );

            _databases = _databases.AddOrUpdate(databaseName, database);
            return database;
        }
    }

    private static bool databaseExists(NpgsqlConnection conn, string databaseName)
    {
        using var cmd = conn.CreateCommand("SELECT datname FROM pg_database where datname = :db")
            .With("db", databaseName);

        return cmd.ExecuteScalar() != null;
    }

    // Two CREATE DATABASE statements for one name: the second fails with duplicate_database, or, when
    // they really are simultaneous, with a unique violation on pg_database's name index.
    private static bool isDuplicateDatabase(PostgresException e) =>
        e.SqlState == PostgresErrorCodes.DuplicateDatabase
        || (e.SqlState == PostgresErrorCodes.UniqueViolation && e.ConstraintName == "pg_database_datname_index");

    // Read by keyword rather than through NpgsqlConnectionStringBuilder.Multiplexing. Weasel 9's Npgsql
    // dependency is only a minimum, and Npgsql 11 removed that property, so reading it threw
    // MissingMethodException there. Npgsql 11 has no multiplexing and does not know the keyword (weasel#912).
    private bool masterTakesSynchronousIo() =>
        _masterTakesSynchronousIo ??= !(new NpgsqlConnectionStringBuilder(_masterConnectionString)
            .TryGetValue("Multiplexing", out var multiplexing) && multiplexing is true);
}
