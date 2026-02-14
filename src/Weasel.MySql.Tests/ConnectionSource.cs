using MySqlConnector;

namespace Weasel.MySql.Tests;

public static class ConnectionSource
{
    public static readonly string ConnectionString =
        Environment.GetEnvironmentVariable("weasel_mysql_testing_database")
        ?? "Server=localhost;Port=3306;Database=weasel_testing;Uid=weasel;Pwd=P@55w0rd;";

    public static MySqlConnection Create()
    {
        return new MySqlConnection(ConnectionString);
    }

    public static async Task<MySqlConnection> CreateOpenConnectionAsync(CancellationToken ct = default)
    {
        var conn = Create();
        await conn.OpenAsync(ct).ConfigureAwait(false);
        return conn;
    }
}
