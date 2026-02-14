using System.Data.Common;
using Oracle.ManagedDataAccess.Client;
using Weasel.Core;

namespace Weasel.Oracle;

public class CommandBuilder: CommandBuilderBase<OracleCommand, OracleParameter, OracleDbType>
{
    public CommandBuilder(): this(new OracleCommand())
    {
    }

    public CommandBuilder(OracleCommand command): base(OracleProvider.Instance, ':', command)
    {
    }

    /// <summary>
    /// Oracle-specific override: converts Guid to byte[] before setting parameter value,
    /// since Oracle stores Guids as RAW(16) and OracleParameter.Value rejects raw Guid objects.
    /// </summary>
    public new void AppendParameter(Guid value)
    {
        AppendParameter((object)value.ToByteArray(), OracleDbType.Raw);
    }

    /// <summary>
    /// Oracle-specific override: converts Guid values to byte[] before setting parameter value.
    /// </summary>
    public new OracleParameter AddParameter(object? value, OracleDbType? dbType = null)
    {
        if (value is Guid guidValue)
        {
            return base.AddParameter(guidValue.ToByteArray(), OracleDbType.Raw);
        }

        return base.AddParameter(value, dbType);
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
        this OracleConnection connection,
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
        this OracleConnection connection,
        CommandBuilder commandBuilder,
        OracleTransaction? tx,
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
    public static Task<OracleDataReader> ExecuteReaderAsync(
        this OracleConnection connection,
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
    public static async Task<OracleDataReader> ExecuteReaderAsync(
        this OracleConnection connection,
        CommandBuilder commandBuilder,
        OracleTransaction? tx,
        CancellationToken ct = default
    ) =>
        (OracleDataReader)await Weasel.Core.CommandBuilderExtensions
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
        this OracleConnection connection,
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
        this OracleConnection connection,
        CommandBuilder commandBuilder,
        Func<DbDataReader, CancellationToken, Task<T>> transform,
        OracleTransaction? tx,
        CancellationToken ct = default
    ) => Weasel.Core.CommandBuilderExtensions.FetchListAsync(connection, commandBuilder, transform, tx, ct);
}
