using System.Data;
using System.Data.Common;
using JasperFx.Core;
using Oracle.ManagedDataAccess.Client;
using Weasel.Core;
// System.Data.Common has its own unrelated DbCommandBuilder (the SQL-generating one)
using DbCommandBuilder = Weasel.Core.DbCommandBuilder;

namespace Weasel.Oracle;

/// <summary>
///     A <see cref="DbCommandBuilder" /> that emits Oracle-shaped SQL, for database-agnostic consumers
///     that build batches against the dialect-neutral <see cref="DbCommandBuilder" /> surface rather than
///     against <see cref="CommandBuilder" />.
///     <para>
///     Two things make Oracle different from every other Weasel provider here. First, its bind marker is
///     <c>:</c> rather than <c>@</c>. Second — and this is the one that can't be papered over — ODP.NET
///     does not implement the ADO.NET batching API (<see cref="DbConnection.CanCreateBatch" /> is
///     <see langword="false" /> and <see cref="DbConnection.CreateBatch" /> throws), and Oracle will not
///     execute several semicolon-separated statements from a single <see cref="OracleCommand" />. So
///     <see cref="StartNewCommand" /> here does real work: it closes the current statement and starts a
///     new one, and <see cref="CompileCommands" /> hands back one <see cref="OracleCommand" /> per
///     boundary, each carrying only the parameters that its own statement bound.
///     </para>
///     <para>
///     Consumers do not need to know any of that. Build the batch exactly as you would for PostgreSQL or
///     SQL Server, calling <see cref="StartNewCommand" /> between logical statements — on those providers
///     it is a no-op and you still get a single multi-statement command back.
///     </para>
/// </summary>
public class OracleDbCommandBuilder: DbCommandBuilder
{
    private readonly OracleCommand _oracleCommand;
    private readonly List<Statement> _statements = [];

    /// <summary>
    ///     Index into the underlying command's parameter collection at which the statement
    ///     currently being built started binding.
    /// </summary>
    private int _boundary;

    public OracleDbCommandBuilder(): this(new OracleCommand())
    {
    }

    public OracleDbCommandBuilder(OracleCommand command): base(command, ':')
    {
        _oracleCommand = command;

        // ODP.NET binds by position unless told otherwise, which silently mis-binds any command
        // whose parameters were not added in the same order they appear in the SQL. Splitting a
        // batch into per-statement commands reorders them by construction, so this is mandatory.
        _oracleCommand.BindByName = true;
    }

    /// <summary>
    ///     Closes the statement currently being built and starts a new one. Unlike every other
    ///     Weasel provider, this is not a no-op — see the type-level remarks.
    /// </summary>
    public override void StartNewCommand()
    {
        var sql = trim(TakeSql());
        var end = _oracleCommand.Parameters.Count;

        if (sql.IsNotEmpty())
        {
            _statements.Add(new Statement(sql, _boundary, end));
        }

        _boundary = end;
    }

    /// <inheritdoc />
    public override int CommandCount => _statements.Count + (trim(ToString()).IsNotEmpty() ? 1 : 0);

    /// <summary>
    ///     Callers separate statements with a trailing semicolon, because that is what the providers
    ///     that concatenate into one command need. Oracle executes one statement per command, where a
    ///     trailing semicolon is a syntax error (ORA-00911), so strip it here rather than making every
    ///     caller branch on the provider.
    /// </summary>
    private static string trim(string sql)
    {
        return sql.Trim().TrimEnd(';').Trim();
    }

    /// <summary>
    ///     Compile into one <see cref="OracleCommand" /> per <see cref="StartNewCommand" /> boundary.
    ///     Each command carries only the parameters bound by its own statement.
    /// </summary>
    public override IReadOnlyList<DbCommand> CompileCommands()
    {
        // Flush whatever statement is still open
        StartNewCommand();

        if (_statements.Count == 0)
        {
            return [];
        }

        if (_statements.Count == 1)
        {
            // Nothing to split -- hand back the command we've been building all along, parameters
            // and all, so that callers keep the single-command diagnostics they'd get elsewhere.
            _oracleCommand.CommandText = _statements[0].Sql;
            return [_oracleCommand];
        }

        // An OracleParameter cannot belong to two collections at once, so detach them all first
        // and then deal each one out to the command whose statement actually bound it.
        var parameters = _oracleCommand.Parameters.Cast<OracleParameter>().ToArray();
        _oracleCommand.Parameters.Clear();

        var commands = new List<DbCommand>(_statements.Count);
        foreach (var statement in _statements)
        {
            var command = new OracleCommand(statement.Sql) { BindByName = true };

            for (var i = statement.Start; i < statement.End; i++)
            {
                command.Parameters.Add(parameters[i]);
            }

            commands.Add(command);
        }

        return commands;
    }

    /// <inheritdoc />
    public override DbParameter AddParameter(object? value, DbType? dbType = null)
    {
        var parameter = base.AddParameter(normalize(value), null);
        applyOracleType(parameter, value, dbType);

        return parameter;
    }

    /// <inheritdoc />
    public override DbParameter AddNamedParameter(string name, object value, DbType? dbType = null)
    {
        var parameter = base.AddNamedParameter(name, normalize(value)!, null);
        applyOracleType(parameter, value, dbType);

        return parameter;
    }

    /// <summary>
    ///     Oracle has no boolean type and stores Guids as RAW(16), so neither can be handed to
    ///     <see cref="OracleParameter.Value" /> as-is.
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

    /// <summary>
    ///     Type the parameter from the *original* CLR value through <see cref="OracleProvider" />, rather
    ///     than through the generic <see cref="DbType" /> mapping the neutral builder would otherwise
    ///     apply. The generic mapping has no entry for <see cref="Guid" /> and resolves it to
    ///     <see cref="DbType.Object" />, which ODP.NET rejects.
    /// </summary>
    private static void applyOracleType(DbParameter parameter, object? original, DbType? dbType)
    {
        if (parameter is not OracleParameter oracleParameter)
        {
            if (dbType.HasValue)
            {
                parameter.DbType = dbType.Value;
            }

            return;
        }

        if (original is null or DBNull)
        {
            return;
        }

        oracleParameter.OracleDbType = OracleProvider.Instance.ToParameterType(original.GetType());
    }

    private readonly record struct Statement(string Sql, int Start, int End);
}
