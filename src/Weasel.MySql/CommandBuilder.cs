using System.Data.Common;
using MySqlConnector;
using Weasel.Core;

namespace Weasel.MySql;

public class CommandBuilder: CommandBuilderBase<MySqlCommand, MySqlParameter, MySqlDbType>
{
    public CommandBuilder(): this(new MySqlCommand())
    {
    }

    public CommandBuilder(MySqlCommand command): base(MySqlProvider.Instance, '@', command)
    {
    }
}

public static class CommandBuilderExtensions
{
    public static Task<int> ExecuteNonQueryAsync(
        this MySqlConnection connection,
        CommandBuilder commandBuilder,
        CancellationToken ct = default
    ) => connection.ExecuteNonQueryAsync(commandBuilder, null, ct);

    public static Task<int> ExecuteNonQueryAsync(
        this MySqlConnection connection,
        CommandBuilder commandBuilder,
        MySqlTransaction? tx,
        CancellationToken ct = default
    ) => Weasel.Core.CommandBuilderExtensions.ExecuteNonQueryAsync(connection, commandBuilder, tx, ct);

    public static Task<MySqlDataReader> ExecuteReaderAsync(
        this MySqlConnection connection,
        CommandBuilder commandBuilder,
        CancellationToken ct = default
    ) => connection.ExecuteReaderAsync(commandBuilder, null, ct);

    public static async Task<MySqlDataReader> ExecuteReaderAsync(
        this MySqlConnection connection,
        CommandBuilder commandBuilder,
        MySqlTransaction? tx,
        CancellationToken ct = default
    ) =>
        (MySqlDataReader)await Weasel.Core.CommandBuilderExtensions
            .ExecuteReaderAsync(connection, commandBuilder, tx, ct).ConfigureAwait(false);

    public static Task<IReadOnlyList<T>> FetchListAsync<T>(
        this MySqlConnection connection,
        CommandBuilder commandBuilder,
        Func<DbDataReader, CancellationToken, Task<T>> transform,
        CancellationToken ct = default
    ) => connection.FetchListAsync(commandBuilder, transform, null, ct);

    public static Task<IReadOnlyList<T>> FetchListAsync<T>(
        this MySqlConnection connection,
        CommandBuilder commandBuilder,
        Func<DbDataReader, CancellationToken, Task<T>> transform,
        MySqlTransaction? tx,
        CancellationToken ct = default
    ) => Weasel.Core.CommandBuilderExtensions.FetchListAsync(connection, commandBuilder, transform, tx, ct);
}
