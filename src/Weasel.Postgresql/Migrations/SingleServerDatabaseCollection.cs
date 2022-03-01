using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Baseline.Dates;
using Baseline.ImTools;
using Npgsql;
using Weasel.Core.Migrations;

namespace Weasel.Postgresql.Migrations
{
    /// <summary>
    /// This models the condition of having multiple databases by name in the same
    /// Postgresql database instance
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public abstract class SingleServerDatabaseCollection<T> where T : PostgresqlDatabase
    {
        private readonly string _masterConnectionString;
        private ImHashMap<string, T> _databases = ImHashMap<string, T>.Empty;
        private readonly TimedLock _lock = new TimedLock();

        public SingleServerDatabaseCollection(string masterConnectionString)
        {
            _masterConnectionString = masterConnectionString;
        }

        public DatabaseSpecification Specification { get; } = new DatabaseSpecification();

        public IReadOnlyList<T> AllDatabases()
        {
            return _databases.Enumerate().Select(x => x.Value).ToList();
        }

        /// <summary>
        /// Force the database to be dropped and re-created
        /// </summary>
        public bool DropAndRecreate { get; set; } = false;

        protected abstract T buildDatabase(string databaseName, string connectionString);

        public async ValueTask<T> FindOrCreateDatabase(string databaseName)
        {
            if (_databases.TryFind(databaseName, out var database))
            {
                return database;
            }

            using (await _lock.Lock(5.Seconds()).ConfigureAwait(false))
            {
                if (_databases.TryFind(databaseName, out database))
                {
                    return database;
                }

                await using var conn = new NpgsqlConnection(_masterConnectionString);
                await conn.OpenAsync().ConfigureAwait(false);

                if (DropAndRecreate)
                {
                    await conn.KillIdleSessions(databaseName).ConfigureAwait(false);
                    await conn.DropDatabase(databaseName).ConfigureAwait(false);

                }
                else if (!await conn.DatabaseExists(databaseName).ConfigureAwait(false))
                {
                    await Specification.BuildDatabase(conn, databaseName).ConfigureAwait(false);
                }

                var builder = new NpgsqlConnectionStringBuilder(_masterConnectionString) { Database = databaseName };

                var connectionString = builder.ConnectionString;
                database = buildDatabase(databaseName, connectionString);

                _databases = _databases.AddOrUpdate(databaseName, database);

                return database;
            }
        }


    }
}
