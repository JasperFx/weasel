using System.Data.Common;
using MySqlConnector;
using Weasel.Core;

namespace Weasel.MySql;

public class CommandBuilder: CommandBuilderBase<MySqlCommand, MySqlParameter, MySqlDbType>, ICommandBuilder
{
    public CommandBuilder(): this(new MySqlCommand())
    {
    }

    public CommandBuilder(MySqlCommand command): base(MySqlProvider.Instance, '@', command)
    {
    }

    /// <summary>
    /// It became so common, that it's turned out to be convenient to place
    /// this here
    /// </summary>
    public string TenantId { get; set; } = string.Empty;

    /// <summary>
    /// Append a single parameter through the dialect-neutral value path, returning the newly created
    /// parameter upcast to <see cref="DbParameter" />.
    /// <para>
    /// Explicitly implemented, as in Weasel.Oracle: the base class already exposes void-returning
    /// <c>AppendParameter</c> overloads, so a public member here would hide them and silently change
    /// which one existing call sites bind to.
    /// </para>
    /// </summary>
    DbParameter ICommandBuilder.AppendParameter(object value)
    {
        base.AppendParameter(value);
        return _command.Parameters[^1];
    }

    void ICommandBuilder.AppendParameters(params object[] parameters)
    {
        if (parameters.Length == 0)
            throw new ArgumentOutOfRangeException(nameof(parameters),
                "Must be at least one parameter value, but got " + parameters.Length);

        AppendParameter(parameters[0]);

        for (var i = 1; i < parameters.Length; i++)
        {
            Append(", ");
            AppendParameter(parameters[i]);
        }
    }

    public IGroupedParameterBuilder CreateGroupedParameterBuilder(char? seperator = null)
    {
        return new GroupedParameterBuilder(this, seperator);
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
