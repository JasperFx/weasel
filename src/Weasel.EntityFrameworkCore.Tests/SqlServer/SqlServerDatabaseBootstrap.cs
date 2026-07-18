using Microsoft.Data.SqlClient;

namespace Weasel.EntityFrameworkCore.Tests.SqlServer;

/// <summary>
///     Race-tolerant creation of the shared test database. On a cold CI
///     SQL Server the database doesn't exist yet, several test classes reach
///     for it at once, and a freshly-created database can briefly refuse
///     logins — so creation swallows the "already exists" race and login is
///     retried until the database is actually reachable.
/// </summary>
public static class SqlServerDatabaseBootstrap
{
    public static async Task EnsureDatabaseExistsAsync(string connectionString)
    {
        var builder = new SqlConnectionStringBuilder(connectionString);
        var database = builder.InitialCatalog;
        builder.InitialCatalog = "master";

        await using (var master = new SqlConnection(builder.ConnectionString))
        {
            await master.OpenAsync();
            await using var cmd = master.CreateCommand();
            cmd.CommandText = $"IF DB_ID('{database}') IS NULL CREATE DATABASE [{database}]";
            try
            {
                await cmd.ExecuteNonQueryAsync();
            }
            // 1801: database already exists — a concurrent test won the race
            catch (SqlException e) when (e.Number == 1801)
            {
            }
        }

        for (var attempt = 0; ; attempt++)
        {
            try
            {
                await using var conn = new SqlConnection(connectionString);
                await conn.OpenAsync();
                return;
            }
            catch (SqlException) when (attempt < 30)
            {
                await Task.Delay(1000);
            }
        }
    }
}
