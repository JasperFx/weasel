using JasperFx.Core;
using Npgsql;

namespace Weasel.Postgresql.Tests;

public class ConnectionSource
{
    // TODO -- use a separate database
    public static readonly string ConnectionString =
        Environment.GetEnvironmentVariable("weasel_postgresql_testing_database")
        ?? "Host=localhost;Port=5432;Database=weasel_testing;Username=postgres;password=postgres;SSL Mode=Disable";

    private static readonly Lazy<string> _concurrentIndexConnectionString =
        new(ensureConcurrentIndexDatabase, LazyThreadSafetyMode.ExecutionAndPublication);

    /// <summary>
    ///     A database of its own beside the configured one (<c>weasel_testing_cic</c> beside
    ///     <c>weasel_testing</c>), for tests that run <c>CREATE INDEX CONCURRENTLY</c>. Created on first use.
    /// </summary>
    /// <remarks>
    ///     <c>CREATE INDEX CONCURRENTLY</c> waits for every statement already running in its database, not
    ///     only those touching its table. Other tests in the suite keep statements running for as long as
    ///     they want on purpose (a <c>CREATE DATABASE</c> or a lock wait), and on a loaded server those
    ///     ran past the 30 second command timeout of a concurrent index build in the same database.
    /// </remarks>
    public static string ConcurrentIndexConnectionString => _concurrentIndexConnectionString.Value;

    private static string ensureConcurrentIndexDatabase()
    {
        var builder = new NpgsqlConnectionStringBuilder(ConnectionString);
        var database = builder.Database + "_cic";

        // CREATE DATABASE cannot run inside a transaction or on a multiplexed pooled connection
        using (var conn = new NpgsqlConnection(
                   new NpgsqlConnectionStringBuilder(ConnectionString) { Pooling = false }.ConnectionString))
        {
            conn.Open();

            using var check = conn.CreateCommand();
            check.CommandText = "select 1 from pg_database where datname = @name";
            check.Parameters.AddWithValue("name", database);
            if (check.ExecuteScalar() is null)
            {
                using var create = conn.CreateCommand();
                create.CommandText = $"CREATE DATABASE \"{database}\"";
                try
                {
                    create.ExecuteNonQuery();
                }
                // Another test process (the other framework) created it first
                catch (PostgresException e) when (e.SqlState is PostgresErrorCodes.DuplicateDatabase
                                                      or PostgresErrorCodes.UniqueViolation)
                {
                }
            }
        }

        builder.Database = database;
        return builder.ConnectionString;
    }

    static ConnectionSource()
    {
        if (ConnectionString.IsEmpty())
            throw new Exception(
                "You need to set the connection string for your local Postgresql database in the environment variable 'weasel_postgresql_testing_database'");
    }
}
