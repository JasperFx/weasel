using System.Data;
using System.Data.Common;
using FirebirdSql.Data.FirebirdClient;
using JasperFx.Core;
using Weasel.Core;
// System.Data.Common has its own unrelated DbCommandBuilder (the SQL-generating one)
using DbCommandBuilder = Weasel.Core.DbCommandBuilder;

namespace Weasel.Firebird;

/// <summary>
///     A <see cref="DbCommandBuilder" /> that emits Firebird-shaped SQL, for database-agnostic consumers
///     that build batches against the dialect-neutral <see cref="DbCommandBuilder" /> surface, and for
///     Weasel's own schema introspection.
///     <para>
///     Firebird executes exactly one statement per command -- two statements in one
///     <see cref="FbCommand" /> fail with SQLCODE -104 -- so <see cref="StartNewCommand" /> here does real
///     work, as on Oracle: it closes the current statement and starts a new one, and
///     <see cref="CompileCommands" /> hands back one <see cref="FbCommand" /> per boundary, each carrying
///     only the parameters its own statement bound. The reader Weasel wraps around them walks the
///     commands as one continuous sequence of result sets.
///     </para>
///     <para>
///     Build the batch exactly as you would for PostgreSQL or SQL Server, calling
///     <see cref="StartNewCommand" /> between logical statements; on those providers it is a no-op.
///     </para>
/// </summary>
public class FirebirdDbCommandBuilder: DbCommandBuilder
{
    private readonly FbCommand _firebirdCommand;
    private readonly List<Statement> _statements = [];

    /// <summary>
    ///     Index into the underlying command's parameter collection at which the statement
    ///     currently being built started binding.
    /// </summary>
    private int _boundary;

    public FirebirdDbCommandBuilder(): this(new FbCommand())
    {
    }

    public FirebirdDbCommandBuilder(FbConnection connection): this(connection.CreateCommand())
    {
    }

    public FirebirdDbCommandBuilder(FbCommand command): base(command, '@')
    {
        _firebirdCommand = command;
    }

    /// <summary>
    ///     The version of the server the command's connection is attached to, when the builder was made
    ///     against an open connection. Introspection reads it to decide whether it may name a catalog
    ///     column that only a later Firebird has.
    /// </summary>
    public FirebirdServerVersion? ServerVersion => FirebirdServerVersion.Of(_firebirdCommand.Connection);

    /// <summary>
    ///     Closes the statement currently being built and starts a new one. Unlike most Weasel
    ///     providers, this is not a no-op -- see the type-level remarks.
    /// </summary>
    public override void StartNewCommand()
    {
        var sql = trim(TakeSql());
        var end = _firebirdCommand.Parameters.Count;

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
    ///     that concatenate into one command need. Firebird accepts one trailing semicolon, but a
    ///     statement is one command here, so it is stripped rather than relied on.
    /// </summary>
    private static string trim(string sql)
    {
        return sql.Trim().TrimEnd(';').Trim();
    }

    /// <summary>
    ///     Compile into one <see cref="FbCommand" /> per <see cref="StartNewCommand" /> boundary. Each
    ///     command carries only the parameters bound by its own statement.
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
            _firebirdCommand.CommandText = _statements[0].Sql;
            return [_firebirdCommand];
        }

        // An FbParameter cannot belong to two collections at once, so detach them all first and then
        // deal each one out to the command whose statement actually bound it.
        var parameters = _firebirdCommand.Parameters.Cast<FbParameter>().ToArray();
        _firebirdCommand.Parameters.Clear();

        var commands = new List<DbCommand>(_statements.Count);
        foreach (var statement in _statements)
        {
            // The options set on the command being built go with every statement of it: a single
            // statement executes that very command, so several have to behave the same (the gap
            // upstream's #660 closed for Oracle).
            var command = new FbCommand(statement.Sql, _firebirdCommand.Connection, _firebirdCommand.Transaction)
            {
                CommandTimeout = _firebirdCommand.CommandTimeout,
                FetchSize = _firebirdCommand.FetchSize
            };

            for (var i = 0; i < parameters.Length; i++)
            {
                // A parameter belongs to this statement if it was bound while the statement was open,
                // or if the statement's SQL names it. The second case matters for a named parameter
                // shared by more than one statement -- AddNamedParameter finds-or-adds, so it is only
                // ever created once, but every statement that references it needs it bound.
                var owned = i >= statement.Start && i < statement.End;
                if (owned || references(statement.Sql, parameters[i].ParameterName))
                {
                    command.Parameters.Add(owned ? parameters[i] : (FbParameter)((ICloneable)parameters[i]).Clone());
                }
            }

            commands.Add(command);
        }

        return commands;
    }

    /// <inheritdoc />
    public override DbParameter AddParameter(object? value, DbType? dbType = null)
    {
        var parameter = base.AddParameter(FirebirdProvider.NormalizeValue(value), null);
        applyFirebirdType(parameter, value, dbType);

        return parameter;
    }

    /// <inheritdoc />
    public override DbParameter AddNamedParameter(string name, object value, DbType? dbType = null)
    {
        var parameter = base.AddNamedParameter(name, FirebirdProvider.NormalizeValue(value)!, null);
        applyFirebirdType(parameter, value, dbType);

        return parameter;
    }

    /// <summary>
    ///     Type the parameter from the *original* CLR value through <see cref="FirebirdProvider" />,
    ///     rather than through the generic <see cref="DbType" /> mapping the neutral builder would
    ///     otherwise apply. That mapping has no entry for <see cref="Guid" />, resolves it to
    ///     <see cref="DbType.Object" /> -- and the base class passes exactly that for every
    ///     <c>AppendParameter(Guid)</c> -- which FirebirdClient binds as a blob.
    /// </summary>
    private static void applyFirebirdType(DbParameter parameter, object? original, DbType? dbType)
    {
        if (parameter is FbParameter firebirdParameter && original is not (null or DBNull))
        {
            firebirdParameter.FbDbType = FirebirdProvider.Instance.ToParameterType(original.GetType());
            return;
        }

        if (dbType.HasValue)
        {
            parameter.DbType = dbType.Value;
        }
    }

    /// <summary>
    ///     Does this statement's SQL bind <paramref name="name" />? Matches <c>@name</c> as a whole
    ///     token, so that <c>@p1</c> does not count as a reference to <c>@p11</c>.
    /// </summary>
    private static bool references(string sql, string name)
    {
        var marker = '@' + name.TrimStart('@');
        var from = 0;
        while (true)
        {
            var at = sql.IndexOf(marker, from, StringComparison.OrdinalIgnoreCase);
            if (at < 0)
            {
                return false;
            }

            var after = at + marker.Length;
            if (after >= sql.Length || !isIdentifierCharacter(sql[after]))
            {
                return true;
            }

            from = after;
        }
    }

    private static bool isIdentifierCharacter(char c)
    {
        return char.IsLetterOrDigit(c) || c == '_' || c == '$';
    }

    private readonly record struct Statement(string Sql, int Start, int End);
}
