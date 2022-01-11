using System.Collections.Generic;
using System.Threading.Tasks;
using Baseline.Dates;
using Npgsql;
using Weasel.Core.Migrations;

namespace Weasel.Postgresql.Migrations
{
    public abstract class SingleInstanceDatabaseCollection
    {
        private readonly string _masterConnectionString;
        private readonly Dictionary<string, IDatabase> _databases = new Dictionary<string, IDatabase>();
        private readonly TimedLock _lock = new TimedLock();

        public SingleInstanceDatabaseCollection(string masterConnectionString)
        {
            _masterConnectionString = masterConnectionString;
        }

        public DatabaseSpecification Specification { get; } = new DatabaseSpecification();

        /// <summary>
        /// Force the database to be dropped and re-created
        /// </summary>
        public bool DropAndRecreate { get; set; } = false;

        protected abstract PostgresqlDatabase buildDatabase(string databaseName, string connectionString);

        public async ValueTask<IDatabase> FindOrCreateDatabase(string databaseName)
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

                var builder = new NpgsqlConnectionStringBuilder(_masterConnectionString);
                builder.Database = databaseName;

                var connectionString = builder.ConnectionString;
                database = buildDatabase(databaseName, connectionString);

                _databases.Add(databaseName, database);

                return database;
            }
        }


    }
}
