using System.Data.Common;
using FirebirdSql.Data.FirebirdClient;
using Weasel.Core;

namespace Weasel.Firebird;

/// <summary>
///     The Firebird-typed command builder.
/// </summary>
/// <remarks>
///     It builds one command and never splits, because <see cref="CommandBuilderBase{TCommand,TParameter,TParameterType}.Compile" />
///     is not virtual and a splitting builder would hand every caller of it only its last statement.
///     Firebird executes one statement per command, so build one statement here; for a batch of several,
///     use <see cref="FirebirdDbCommandBuilder" />, which splits on
///     <see cref="CommandBuilderBase{TCommand,TParameter,TParameterType}.StartNewCommand" />.
/// </remarks>
public class CommandBuilder: CommandBuilderBase<FbCommand, FbParameter, FbDbType>, ICommandBuilder
{
    public CommandBuilder(): this(new FbCommand())
    {
    }

    public CommandBuilder(FbCommand command): base(FirebirdProvider.Instance, '@', command)
    {
    }

    /// <summary>
    /// It became so common, that it's turned out to be convenient to place
    /// this here
    /// </summary>
    public string TenantId { get; set; } = string.Empty;

    /// <summary>
    ///     Firebird-specific override: a <see cref="DateTimeOffset" /> value has to reach
    ///     <see cref="FbParameter.Value" /> as FirebirdClient's own zoned timestamp, because the
    ///     parameter rejects the value the moment it is assigned. An override rather than a hiding
    ///     member, because every typed <c>AppendParameter</c> in the base class routes through here.
    /// </summary>
    public override FbParameter AddParameter(object? value, FbDbType? dbType = null)
    {
        return base.AddParameter(FirebirdProvider.NormalizeValue(value),
            dbType ?? (value is DateTimeOffset ? FbDbType.TimeStampTZ : null));
    }

    /// <summary>
    ///     Firebird-specific override, for the same reason as <see cref="AddParameter" />.
    /// </summary>
    public override FbParameter AddNamedParameter(string name, object value, FbDbType? dbType = null)
    {
        return base.AddNamedParameter(name, FirebirdProvider.NormalizeValue(value)!,
            dbType ?? (value is DateTimeOffset ? FbDbType.TimeStampTZ : null));
    }

    FbParameter ICommandBuilder.AppendParameter<T>(T value)
    {
        base.AppendParameter(value);
        return _command.Parameters[^1];
    }

    public FbParameter AppendParameter<T>(T value, FbDbType dbType)
    {
        base.AppendParameter(value, dbType);
        return _command.Parameters[^1];
    }

    FbParameter ICommandBuilder.AppendParameter(object value)
    {
        base.AppendParameter(value);
        return _command.Parameters[^1];
    }

    FbParameter ICommandBuilder.AppendParameter(object? value, FbDbType? dbType)
    {
        base.AppendParameter(value, dbType);
        return _command.Parameters[^1];
    }

    /// <summary>
    ///     Explicitly implemented, as in Weasel.Oracle: the base class already exposes void-returning
    ///     <c>AppendParameter</c> overloads, so a public member here would hide them and silently change
    ///     which one existing call sites bind to.
    /// </summary>
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

    public IGroupedParameterBuilder CreateGroupedParameterBuilder(char? seperator = null)
    {
        return new GroupedParameterBuilder(this, seperator);
    }
}

public static class CommandBuilderExtensions
{
    /// <summary>
    ///     Compile and execute the command against the user supplied connection
    /// </summary>
    public static Task<int> ExecuteNonQueryAsync(
        this FbConnection connection,
        CommandBuilder commandBuilder,
        CancellationToken ct = default
    ) => connection.ExecuteNonQueryAsync(commandBuilder, null, ct);

    /// <summary>
    ///     Compile and execute the command against the user supplied connection
    /// </summary>
    public static Task<int> ExecuteNonQueryAsync(
        this FbConnection connection,
        CommandBuilder commandBuilder,
        FbTransaction? tx,
        CancellationToken ct = default
    ) => Weasel.Core.CommandBuilderExtensions.ExecuteNonQueryAsync(connection, commandBuilder, tx, ct);

    /// <summary>
    ///     Compile and execute the command against the user supplied connection and
    ///     return a data reader for the results
    /// </summary>
    public static Task<FbDataReader> ExecuteReaderAsync(
        this FbConnection connection,
        CommandBuilder commandBuilder,
        CancellationToken ct = default
    ) => connection.ExecuteReaderAsync(commandBuilder, null, ct);

    /// <summary>
    ///     Compile and execute the command against the user supplied connection and
    ///     return a data reader for the results
    /// </summary>
    public static async Task<FbDataReader> ExecuteReaderAsync(
        this FbConnection connection,
        CommandBuilder commandBuilder,
        FbTransaction? tx,
        CancellationToken ct = default
    ) =>
        (FbDataReader)await Weasel.Core.CommandBuilderExtensions
            .ExecuteReaderAsync(connection, commandBuilder, tx, ct).ConfigureAwait(false);

    /// <summary>
    ///     Compile and execute the query and returns the results transformed from the raw database reader
    /// </summary>
    public static Task<IReadOnlyList<T>> FetchListAsync<T>(
        this FbConnection connection,
        CommandBuilder commandBuilder,
        Func<DbDataReader, CancellationToken, Task<T>> transform,
        CancellationToken ct = default
    ) => connection.FetchListAsync(commandBuilder, transform, null, ct);

    /// <summary>
    ///     Compile and execute the query and returns the results transformed from the raw database reader
    /// </summary>
    public static Task<IReadOnlyList<T>> FetchListAsync<T>(
        this FbConnection connection,
        CommandBuilder commandBuilder,
        Func<DbDataReader, CancellationToken, Task<T>> transform,
        FbTransaction? tx,
        CancellationToken ct = default
    ) => Weasel.Core.CommandBuilderExtensions.FetchListAsync(connection, commandBuilder, transform, tx, ct);
}
