using System.Data.Common;
using System.Text;
using Npgsql;
using NpgsqlTypes;
using Weasel.Core;

namespace Weasel.Postgresql;

public class BatchBuilder
{
    private readonly NpgsqlBatch _batch;
    private readonly StringBuilder _builder = new();
    private NpgsqlBatchCommand? _current;

    public BatchBuilder(NpgsqlBatch batch)
    {
        _batch = batch;
    }

    public BatchBuilder()
    {
        _batch = new NpgsqlBatch();
    }

    private NpgsqlBatchCommand appendCommand()
    {
        var command = _batch.CreateBatchCommand();
        _batch.BatchCommands.Add(command);

        return command;
    }

    public void Append(string sql)
    {
        _current ??= appendCommand();
        _builder.Append(sql);
    }

    public void Append(char character)
    {
        _builder.Append(character);
    }

    public void AppendParameter(object value)
    {
        _current ??= _batch.CreateBatchCommand();
        var param = new NpgsqlParameter { Value = value };
        _current.Parameters.Add(param);

        _builder.Append('$');
        _builder.Append(_current.Parameters.Count);
    }

    public void AppendParameters(params object[] parameters)
    {
        if (!parameters.Any())
            throw new ArgumentOutOfRangeException(nameof(parameters),
                "Must be at least one parameter value, but got " + parameters.Length);

        AppendParameter(parameters[0]);

        for (var i = 1; i < parameters.Length; i++)
        {
            _builder.Append(", ");
            AppendParameter(parameters[i]);
        }
    }

    public void StartNewCommand()
    {
        if (_current != null)
        {
            _current.CommandText = _builder.ToString();
        }

        _builder.Clear();
        _current = appendCommand();
    }

    public NpgsqlBatch Compile()
    {
        if (_current != null)
        {
            _current.CommandText = _builder.ToString();
        }

        return _batch;
    }
}

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
