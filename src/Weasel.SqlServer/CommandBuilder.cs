using System.Data;
using System.Data.Common;
using Microsoft.Data.SqlClient;
using Weasel.Core;

namespace Weasel.SqlServer;

public class CommandBuilder: CommandBuilderBase<SqlCommand, SqlParameter, SqlDbType>, Weasel.Core.ICommandBuilder
{
    public CommandBuilder(): this(new SqlCommand())
    {
    }

    public CommandBuilder(SqlCommand command): base(SqlServerProvider.Instance, '@', command)
    {
    }

    /// <summary>
    /// It became so common, that it's turned out to be convenient to place
    /// this here
    /// </summary>
    public string TenantId { get; set; }

    /// <summary>
    ///     Append the supplied parameter values to the command, comma-separated, adding each to
    ///     the underlying command's parameter collection and to the SQL text.
    /// </summary>
    /// <param name="parameters"></param>
    public void AppendParameters(params object[] parameters)
    {
        if (!parameters.Any())
            throw new ArgumentOutOfRangeException(nameof(parameters),
                "Must be at least one parameter value, but got " + parameters.Length);

        AppendParameter(parameters[0]);

        for (var i = 1; i < parameters.Length; i++)
        {
            Append(", ");
            AppendParameter(parameters[i]);
        }
    }

    /// <summary>
    ///     Append a single parameter through the dialect-neutral value path, returning the newly created
    ///     parameter upcast to <see cref="DbParameter" />. Implements
    ///     <see cref="Weasel.Core.ICommandBuilder.AppendParameter(object)" />.
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public DbParameter AppendParameter(object value)
    {
        base.AppendParameter(value);
        return _command.Parameters[^1];
    }

    public Weasel.Core.IGroupedParameterBuilder CreateGroupedParameterBuilder(char? seperator = null)
    {
        return new Weasel.Core.GroupedParameterBuilder(this, seperator);
    }

    public void StartNewCommand()
    {
        // do nothing!
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
