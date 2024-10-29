using System.Data;
using System.Data.Common;
using JasperFx.Core.Reflection;
using Npgsql;
using NpgsqlTypes;
using Weasel.Core;
using Weasel.Core.Operations;
using Weasel.Core.Serialization;

namespace Weasel.Postgresql;

public class CommandBuilder: CommandBuilderBase<NpgsqlCommand, NpgsqlParameter, NpgsqlDbType>, IPostgresqlCommandBuilder
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

    public string TenantId { get; set; }

    public void SetParameterAsJson(DbParameter parameter, string json)
    {
        parameter.Value = json;
        parameter.As<NpgsqlParameter>().NpgsqlDbType = NpgsqlDbType.Jsonb;
    }

    public void AppendLongArrayParameter(long[] values)
    {
        AppendParameter(values, NpgsqlDbType.Array | NpgsqlDbType.Bigint);
    }

    public void AppendJsonParameter(ISerializer serializer, object value)
    {
        if (value == null)
        {
            AppendParameter(DBNull.Value, NpgsqlDbType.Jsonb);
        }
        else
        {
            AppendParameter(serializer.ToJson(value), NpgsqlDbType.Jsonb);
        }
    }

    public void AppendJsonParameter(string json)
    {
        AppendParameter(json, NpgsqlDbType.Jsonb);
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

    public void AppendStringArrayParameter(string[] values)
    {
        AppendParameter(values, NpgsqlDbType.Array | NpgsqlDbType.Varchar);
    }

    public void AppendGuidArrayParameter(Guid[] values)
    {
        AppendParameter(values, NpgsqlDbType.Array | NpgsqlDbType.Uuid);
    }

    public void AppendParameter<T>(T value)
    {
        base.AppendParameter(value);
    }

    public void AppendParameter<T>(T value, DbType? dbType)
    {
        base.AppendParameter(value, dbType);
    }

    public void AppendParameter(object value)
    {
        base.AppendParameter(value);
    }

    public NpgsqlParameter AppendParameter(object? value, NpgsqlDbType? dbType)
    {
        var parameter = AddParameter(value);
        if (dbType.HasValue) parameter.NpgsqlDbType = dbType.Value;
        Append(_parameterPrefix);
        Append(parameter.ParameterName);
        return _command.Parameters[^1];
    }

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

    public IGroupedParameterBuilder CreateGroupedParameterBuilder(char? seperator = null)
    {
        return new GroupedParameterBuilder(this, seperator);
    }

    public void StartNewCommand()
    {
        // do nothing!
    }
}

public static class CommandBuilderExtensions
{
    /// <summary>
    ///     Compile and execute the command against the user supplied connection
    /// </summary>
    /// <param name="connection"></param>
    /// <param name="commandBuilder"></param>
    /// <param name="ct"></param>
    /// <returns></returns>
    public static Task<int> ExecuteNonQueryAsync(
        this NpgsqlConnection connection,
        CommandBuilder commandBuilder,
        CancellationToken ct = default
    ) => connection.ExecuteNonQueryAsync(commandBuilder, null, ct);

    /// <summary>
    ///     Compile and execute the command against the user supplied connection
    /// </summary>
    /// <param name="connection"></param>
    /// <param name="commandBuilder"></param>
    /// <param name="tx"></param>
    /// <param name="ct"></param>
    /// <returns></returns>
    public static Task<int> ExecuteNonQueryAsync(
        this NpgsqlConnection connection,
        CommandBuilder commandBuilder,
        NpgsqlTransaction? tx,
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
    public static Task<NpgsqlDataReader> ExecuteReaderAsync(
        this NpgsqlConnection connection,
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
    public static async Task<NpgsqlDataReader> ExecuteReaderAsync(
        this NpgsqlConnection connection,
        CommandBuilder commandBuilder,
        NpgsqlTransaction? tx,
        CancellationToken ct = default
    ) =>
        (NpgsqlDataReader)await Weasel.Core.CommandBuilderExtensions
            .ExecuteReaderAsync(connection, commandBuilder, tx, ct).ConfigureAwait(false);

    public static Task<IReadOnlyList<T>> FetchListAsync<T>(
        this NpgsqlConnection connection,
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
        this NpgsqlConnection connection,
        CommandBuilder commandBuilder,
        Func<DbDataReader, CancellationToken, Task<T>> transform,
        NpgsqlTransaction? tx,
        CancellationToken ct = default
    ) =>  Weasel.Core.CommandBuilderExtensions.FetchListAsync(connection, commandBuilder, transform, tx, ct);
}
