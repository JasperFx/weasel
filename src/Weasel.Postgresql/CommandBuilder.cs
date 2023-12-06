using System.Data.Common;
using Npgsql;
using NpgsqlTypes;
using Weasel.Core;

namespace Weasel.Postgresql;

public class CommandBuilder: CommandBuilderBase<NpgsqlCommand, NpgsqlParameter, NpgsqlDbType>
{
    public CommandBuilder(NpgsqlDataSource dataSource): this(dataSource.CreateCommand())
    {
    }

    public CommandBuilder(): this(new NpgsqlCommand())
    {
    }

    public CommandBuilder(NpgsqlCommand command): base(PostgresqlProvider.Instance, ':', command)
    {
    }

    /// <summary>
    ///     Append a parameter with the supplied value to the underlying command parameter
    ///     collection and adds the parameter usage to the SQL
    /// </summary>
    /// <param name="value"></param>
    /// <param name="dbType"></param>
    public void AppendParameter(string[] values)
    {
        AppendParameter(values, NpgsqlDbType.Varchar | NpgsqlDbType.Array);
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
        NpgsqlConnection conn,
        CancellationToken cancellation = default,
        NpgsqlTransaction? tx = null
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
    public static async Task<NpgsqlDataReader> ExecuteReaderAsync(
        this CommandBuilder commandBuilder,
        NpgsqlConnection conn,
        CancellationToken cancellation = default,
        NpgsqlTransaction? tx = null
    )
    {
        var cmd = commandBuilder.Compile();

        cmd.Connection = conn;
        cmd.Transaction = tx;

        return (NpgsqlDataReader)await cmd.ExecuteReaderAsync(cancellation).ConfigureAwait(false);
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
        NpgsqlConnection conn,
        Func<DbDataReader, CancellationToken, Task<T>> transform,
        CancellationToken ct = default,
        NpgsqlTransaction? tx = null
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
