using System.Data.Common;
using Oracle.ManagedDataAccess.Client;
using Weasel.Core;

namespace Weasel.Oracle;

public class CommandBuilder: CommandBuilderBase<OracleCommand, OracleParameter, OracleDbType>, ICommandBuilder
{
    public CommandBuilder(): this(new OracleCommand())
    {
    }

    public CommandBuilder(OracleCommand command): base(OracleProvider.Instance, ':', command)
    {
        // ODP.NET binds by position unless told otherwise, which silently mis-binds any
        // command whose parameters were added in a different order than they appear in the SQL.
        command.BindByName = true;
    }

    /// <summary>
    /// It became so common, that it's turned out to be convenient to place
    /// this here
    /// </summary>
    public string TenantId { get; set; } = string.Empty;

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
    /// <para>
    /// This has to be an <c>override</c> rather than a <c>new</c> member — the base class routes
    /// every one of its typed <c>AppendParameter</c> overloads through <c>AddParameter</c>, so a
    /// hiding member would be bypassed on all of those paths and the raw <see cref="Guid" /> would
    /// reach <see cref="OracleParameter.Value" /> and be rejected.
    /// </para>
    /// </summary>
    public override OracleParameter AddParameter(object? value, OracleDbType? dbType = null)
    {
        return base.AddParameter(normalize(value), dbType ?? inferType(value));
    }

    /// <summary>
    /// Oracle-specific override, for the same reason as <see cref="AddParameter" />.
    /// </summary>
    public override OracleParameter AddNamedParameter(string name, object value, OracleDbType? dbType = null)
    {
        return base.AddNamedParameter(name, normalize(value)!, dbType ?? inferType(value));
    }

    /// <summary>
    /// Oracle has no boolean type and stores Guids as RAW(16), so both have to be converted
    /// before they reach <see cref="OracleParameter.Value" />.
    /// </summary>
    private static object? normalize(object? value)
    {
        return value switch
        {
            Guid guid => guid.ToByteArray(),
            bool boolean => boolean ? 1 : 0,
            _ => value
        };
    }

    private static OracleDbType? inferType(object? value)
    {
        return value switch
        {
            Guid => OracleDbType.Raw,
            bool => OracleDbType.Int16,
            _ => null
        };
    }

    OracleParameter ICommandBuilder.AppendParameter<T>(T value)
    {
        base.AppendParameter(value);
        return _command.Parameters[^1];
    }

    public OracleParameter AppendParameter<T>(T value, OracleDbType dbType)
    {
        base.AppendParameter(value, dbType);
        return _command.Parameters[^1];
    }

    OracleParameter ICommandBuilder.AppendParameter(object value)
    {
        base.AppendParameter(value);
        return _command.Parameters[^1];
    }

    OracleParameter ICommandBuilder.AppendParameter(object? value, OracleDbType? dbType)
    {
        base.AppendParameter(value, dbType);
        return _command.Parameters[^1];
    }

    DbParameter Weasel.Core.ICommandBuilder.AppendParameter(object value)
    {
        base.AppendParameter(value);
        return _command.Parameters[^1];
    }

    void Weasel.Core.ICommandBuilder.AppendParameters(params object[] parameters)
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

    public Weasel.Core.IGroupedParameterBuilder CreateGroupedParameterBuilder(char? seperator = null)
    {
        return new Weasel.Core.GroupedParameterBuilder(this, seperator);
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
