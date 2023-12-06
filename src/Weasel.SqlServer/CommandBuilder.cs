using System.Data;
using System.Data.Common;
using Microsoft.Data.SqlClient;
using Weasel.Core;

namespace Weasel.SqlServer;

public class CommandBuilder: CommandBuilderBase<SqlCommand, SqlParameter, SqlDbType>
{
    public CommandBuilder(): this(new SqlCommand())
    {
    }

    public CommandBuilder(SqlCommand command): base(SqlServerProvider.Instance, ':', command)
    {
    }
}

public static class CommandBuilderExtensions
{
    /// <summary>
    ///     Compile and execute the batched command against the user supplied connection
    /// </summary>
    /// <param name="conn"></param>
    /// <param name="cancellation"></param>
    /// <param name="tx"></param>
    /// <returns></returns>
    public static Task<int> ExecuteNonQueryAsync(
        this CommandBuilder commandBuilder,
        SqlConnection conn,
        CancellationToken cancellation = default,
        SqlTransaction? tx = null
    )
    {
        var cmd = commandBuilder.Compile();

        cmd.Connection = conn;
        cmd.Transaction = tx;

        return cmd.ExecuteNonQueryAsync(cancellation);
    }


    /// <summary>
    ///     Compile and execute the command against the user supplied connection and
    ///     return a data reader for the results
    /// </summary>
    /// <param name="conn"></param>
    /// <param name="cancellation"></param>
    /// <param name="tx"></param>
    /// <returns></returns>
    public static async Task<SqlDataReader> ExecuteReaderAsync(
        this CommandBuilder commandBuilder,
        SqlConnection conn,
        CancellationToken cancellation = default,
        SqlTransaction? tx = null
    )
    {
        var cmd = commandBuilder.Compile();

        cmd.Connection = conn;
        cmd.Transaction = tx;

        return (SqlDataReader)await cmd.ExecuteReaderAsync(cancellation).ConfigureAwait(false);
    }

    /// <summary>
    ///     Compile and execute the query and returns the results transformed from the raw database reader
    /// </summary>
    /// <param name="conn"></param>
    /// <param name="transform"></param>
    /// <param name="ct"></param>
    /// <param name="tx"></param>
    /// <typeparam name="T"></typeparam>
    /// <returns></returns>
    public static async Task<IReadOnlyList<T>> FetchListAsync<T>(
        this CommandBuilder commandBuilder,
        SqlConnection conn,
        Func<DbDataReader, CancellationToken, Task<T>> transform,
        CancellationToken ct = default,
        SqlTransaction? tx = null
    )
    {
        var cmd = commandBuilder.Compile();

        cmd.Connection = conn;
        cmd.Transaction = tx;

        var list = new List<T>();

        await using var reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            list.Add(await transform(reader, ct).ConfigureAwait(false));
        }

        return list;
    }
}
