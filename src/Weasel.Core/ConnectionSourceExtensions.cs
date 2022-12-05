using System.Data.Common;

namespace Weasel.Core;

public static class ConnectionSourceExtensions
{
    /// <summary>
    ///     Synchronously run a single SQL statement with a new connection against this
    ///     connection source
    /// </summary>
    /// <param name="source"></param>
    /// <param name="sql"></param>
    /// <typeparam name="T"></typeparam>
    public static void RunSql<T>(this IConnectionSource<T> source, string sql) where T : DbConnection
    {
        using var conn = source.CreateConnection();
        conn.Open();

        try
        {
            conn.CreateCommand(sql).ExecuteNonQuery();
        }
        finally
        {
            conn.Close();
            conn.Dispose();
        }
    }

    /// <summary>
    ///     Asynchronously run a single SQL statement with a new connection against this
    ///     connection source
    /// </summary>
    /// <param name="source"></param>
    /// <param name="sql"></param>
    /// <typeparam name="T"></typeparam>
    public static async Task RunSqlAsync<T>(this IConnectionSource<T> source, string sql) where T : DbConnection
    {
        await using var conn = source.CreateConnection();
        await conn.OpenAsync().ConfigureAwait(false);

        try
        {
            await conn.CreateCommand(sql).ExecuteNonQueryAsync().ConfigureAwait(false);
        }
        finally
        {
            await conn.CloseAsync().ConfigureAwait(false);
            conn.Dispose();
        }
    }
}
