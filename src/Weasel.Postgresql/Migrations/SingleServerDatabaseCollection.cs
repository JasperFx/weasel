using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Baseline.Dates;
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
        private readonly Dictionary<string, T> _databases = new Dictionary<string, T>();
        private readonly TimedLock _lock = new TimedLock();

        public SingleServerDatabaseCollection(string masterConnectionString)
        {
            _masterConnectionString = masterConnectionString;
        }

        public DatabaseSpecification Specification { get; } = new DatabaseSpecification();

        public IReadOnlyList<T> AllDatabases()
        {
            return _databases.Values.ToList();
        }

        /// <summary>
        /// Force the database to be dropped and re-created
        /// </summary>
        public bool DropAndRecreate { get; set; } = false;

        protected abstract T buildDatabase(string databaseName, string connectionString);

        public async ValueTask<T> FindOrCreateDatabase(string databaseName)
        {
            if (_databases.TryGetValue(databaseName, out var database))
            {
                return database;
            }

            await using var conn = new NpgsqlConnection(_masterConnectionString);
            await conn.OpenAsync().ConfigureAwait(false);

            using (await _lock.Lock(5.Seconds()).ConfigureAwait(false))
            {
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

                _databases.Add(databaseName, database);

                return database;
            }
        }


    }
}
