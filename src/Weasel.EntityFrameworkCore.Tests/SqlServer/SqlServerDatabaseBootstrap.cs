using Microsoft.Data.SqlClient;
using Weasel.SqlServer;

namespace Weasel.EntityFrameworkCore.Tests.SqlServer;

/// <summary>
///     Race-tolerant creation of the shared test database. On a cold CI
///     SQL Server the database doesn't exist yet, several test classes reach
///     for it at once, and a freshly-created database can briefly refuse
///     logins — so creation swallows the "already exists" race and login is
///     retried until the database is actually reachable.
/// </summary>
/// <remarks>
///     This used to carry its own copy of that logic. It now lives in
///     <see cref="SqlServerMigrator.EnsureDatabaseExistsAsync" /> (weasel#415), so this is a thin
///     shim kept for the call sites.
/// </remarks>
public static class SqlServerDatabaseBootstrap
{
    public static async Task EnsureDatabaseExistsAsync(string connectionString)
    {
        await using var connection = new SqlConnection(connectionString);
        await new SqlServerMigrator().EnsureDatabaseExistsAsync(connection);
    }
}
