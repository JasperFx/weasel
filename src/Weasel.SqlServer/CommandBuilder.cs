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

    public CommandBuilder(SqlCommand command): base(SqlServerProvider.Instance, '@', command)
    {
    }

    /// <summary>
    ///     Append a parameter with the supplied value to the underlying command
    ///     parameter collection *and* the command text
    /// </summary>
    /// <param name="value"></param>
    /// <param name="dbType"></param>
    public void AppendParameter(object? value, SqlDbType? dbType = null)
    {
        var parameter = AddParameter(value);
        if (dbType.HasValue) parameter.SqlDbType = dbType.Value;
        Append(_parameterPrefix);
        Append(parameter.ParameterName);
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
    ) => Weasel.Core.CommandBuilderExtensions.ExecuteNonQueryAsync(connection, commandBuilder, tx, ct);

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
    ) =>
        (SqlDataReader)await Weasel.Core.CommandBuilderExtensions
            .ExecuteReaderAsync(connection, commandBuilder, tx, ct).ConfigureAwait(false);


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
    public static Task<IReadOnlyList<T>> FetchListAsync<T>(
        this SqlConnection connection,
        CommandBuilder commandBuilder,
        Func<DbDataReader, CancellationToken, Task<T>> transform,
        SqlTransaction? tx,
        CancellationToken ct = default
    )=>  Weasel.Core.CommandBuilderExtensions.FetchListAsync(connection, commandBuilder, transform, tx, ct);
}
