using System.Data.Common;
using Microsoft.Data.Sqlite;
using Weasel.Core;

namespace Weasel.Sqlite;

public class CommandBuilder: CommandBuilderBase<SqliteCommand, SqliteParameter, SqliteType>
{
    public CommandBuilder(): this(new SqliteCommand())
    {
    }

    public CommandBuilder(SqliteCommand command): base(SqliteProvider.Instance, '@', command)
    {
    }
}

public static class CommandBuilderExtensions
{
    /// <summary>
    ///     Compile and execute the batched command against the user supplied connection
    /// </summary>
    /// <param name="connection"></param>
    /// <param name="commandBuilder"></param>
    /// <param name="ct"></param>
    /// <returns></returns>
    public static Task<int> ExecuteNonQueryAsync(
        this SqliteConnection connection,
        CommandBuilder commandBuilder,
        CancellationToken ct = default
    ) => connection.ExecuteNonQueryAsync(commandBuilder, null, ct);

    /// <summary>
    ///     Compile and execute the batched command against the user supplied connection
    /// </summary>
    /// <param name="connection"></param>
    /// <param name="commandBuilder"></param>
    /// <param name="tx"></param>
    /// <param name="ct"></param>
    /// <returns></returns>
    public static Task<int> ExecuteNonQueryAsync(
        this SqliteConnection connection,
        CommandBuilder commandBuilder,
        SqliteTransaction? tx,
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
    public static Task<SqliteDataReader> ExecuteReaderAsync(
        this SqliteConnection connection,
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
    public static async Task<SqliteDataReader> ExecuteReaderAsync(
        this SqliteConnection connection,
        CommandBuilder commandBuilder,
        SqliteTransaction? tx,
        CancellationToken ct = default
    ) =>
        (SqliteDataReader)await Weasel.Core.CommandBuilderExtensions
            .ExecuteReaderAsync(connection, commandBuilder, tx, ct).ConfigureAwait(false);

    /// <summary>
    ///     Compile and execute the query and returns the results transformed from the raw database reader
    /// </summary>
    /// <param name="connection"></param>
    /// <param name="commandBuilder"></param>
    /// <param name="transform"></param>
    /// <param name="ct"></param>
    /// <typeparam name="T"></typeparam>
    /// <returns></returns>
    public static Task<IReadOnlyList<T>> FetchListAsync<T>(
        this SqliteConnection connection,
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
    /// <param name="tx"></param>
    /// <param name="ct"></param>
    /// <typeparam name="T"></typeparam>
    /// <returns></returns>
    public static Task<IReadOnlyList<T>> FetchListAsync<T>(
        this SqliteConnection connection,
        CommandBuilder commandBuilder,
        Func<DbDataReader, CancellationToken, Task<T>> transform,
        SqliteTransaction? tx,
        CancellationToken ct = default
    ) => Weasel.Core.CommandBuilderExtensions.FetchListAsync(connection, commandBuilder, transform, tx, ct);
}
