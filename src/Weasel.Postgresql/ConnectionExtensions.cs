using System.Threading;
using System.Threading.Tasks;
using Npgsql;

namespace Weasel.Postgresql
{
    public static class ConnectionExtensions
    {
        public static async Task<int> RunOne(NpgsqlConnection conn, string sql, CancellationToken cancellation = default)
        {
            try
            {
                await conn.OpenAsync(cancellation);

                var result = await conn
                    .CreateCommand(sql)
                    .ExecuteNonQueryAsync(cancellation);

                return result;
            }
            finally
            {
                await conn.CloseAsync();
            }
        }
    }
}