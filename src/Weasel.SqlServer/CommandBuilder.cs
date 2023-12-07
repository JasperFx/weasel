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
    /// <param name="connection"></param>
    /// <param name="ct"></param>
    /// <returns></returns>
    public static Task<int> ExecuteNonQueryAsync(
        this SqlConnection connection,
        CommandBuilder commandBuilder,
        CancellationToken ct = default
    ) => connection.ExecuteNonQueryAsync(commandBuilder, null, ct);

    /// <summary>
    ///     Compile and execute the batched command against the user supplied connection
    /// </summary>
    /// <param name="connection"></param>
    /// <param name="commandBuilder"></param>
    /// <param name="ct"></param>
    /// <param name="tx"></param>
    /// <returns></returns>
    public static Task<int> ExecuteNonQueryAsync(
        this SqlConnection connection,
        CommandBuilder commandBuilder,
        SqlTransaction? tx,
        CancellationToken ct = default
    )
    {
        var cmd = commandBuilder.Compile();

        cmd.Connection = connection;
        cmd.Transaction = tx;

        return cmd.ExecuteNonQueryAsync(ct);
    }

    /// <summary>
    ///     Compile and execute the command against the user supplied connection and
    ///     return a data reader for the results
    /// </summary>
    /// <param name="connection"></param>
    /// <param name="commandBuilder"></param>
    /// <param name="ct"></param>
    /// <returns></returns>
    public static Task<SqlDataReader> ExecuteReaderAsync(
        this SqlConnection connection,
        CommandBuilder commandBuilder,
        CancellationToken ct = default
    ) => connection.ExecuteReaderAsync(commandBuilder, null, ct);


    /// <summary>
    ///     Compile and execute the command against the user supplied connection and
    ///     return a data reader for the results
    /// </summary>
    /// <param name="connection"></param>
    /// <param name="commandBuilder"></param>
    /// <param name="tx"></param>
    /// <param name="ct"></param>
    /// <returns></returns>
    public static async Task<SqlDataReader> ExecuteReaderAsync(
        this SqlConnection connection,
        CommandBuilder commandBuilder,
        SqlTransaction? tx,
        CancellationToken ct = default
    )
    {
        var cmd = commandBuilder.Compile();

        cmd.Connection = connection;
        cmd.Transaction = tx;

        return (SqlDataReader)await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
    }


    /// <summary>
    ///     Compile and execute the query and returns the results transformed from the raw database reader
    /// </summary>
    /// <param name="connection"></param>
    /// <param name="commandBuilder"></param>
    /// <param name="transform"></param>
    /// <param name="ct"></param>
    /// <param name="tx"></param>
    /// <typeparam name="T"></typeparam>
    /// <returns></returns>
    public static Task<IReadOnlyList<T>> FetchListAsync<T>(
        this SqlConnection connection,
        CommandBuilder commandBuilder,
        Func<DbDataReader, CancellationToken, Task<T>> transform,
        CancellationToken ct = default
    ) => connection.FetchListAsync(commandBuilder, transform, null, ct);

    /// <summary>
    ///     Compile and execute the query and returns the results transformed from the raw database reader
    /// </summary>
    /// <param name="connection"></param>
    /// <param name="commandBuilder"></param>
    /// <param name="transform"></param>
    /// <param name="ct"></param>
    /// <param name="tx"></param>
    /// <typeparam name="T"></typeparam>
    /// <returns></returns>
    public static async Task<IReadOnlyList<T>> FetchListAsync<T>(
        this SqlConnection connection,
        CommandBuilder commandBuilder,
        Func<DbDataReader, CancellationToken, Task<T>> transform,
        SqlTransaction? tx,
        CancellationToken ct = default
    )
    {
        var cmd = commandBuilder.Compile();

        cmd.Connection = connection;
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
