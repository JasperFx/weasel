using System.Data.Common;
using System.Text;
using JasperFx;
using JasperFx.Core;
using MySqlConnector;
using Weasel.Core;
using Weasel.Core.Migrations;

namespace Weasel.MySql;

public class MySqlMigrator: Migrator
{
    public MySqlMigrator(): base(MySqlProvider.Instance.DefaultDatabaseSchemaName)
    {
    }

    public override bool MatchesConnection(DbConnection connection)
    {
        return connection is MySqlConnection;
    }

    public override async ValueTask ReleaseConnectionPoolAsync(DbConnection connection, CancellationToken ct = default)
    {
        if (connection is MySqlConnection mysql)
        {
            await MySqlConnection.ClearPoolAsync(mysql, ct).ConfigureAwait(false);
        }
    }

    public override bool IsTransientConnectionFailure(Exception exception)
    {
        foreach (var e in ExceptionChain.Flatten(exception))
        {
            if (e is MySqlException mysql && IsTransientConnectionError(mysql.ErrorCode)) return true;
        }

        return false;
    }

    /// <summary>
    ///     True for the MySQL errors that mean the server would not hand out a connection right now
    ///     (weasel#356):
    ///     <list type="bullet">
    ///         <item><c>ER_CON_COUNT_ERROR</c> (1040) too many connections</item>
    ///         <item><c>ER_TOO_MANY_USER_CONNECTIONS</c> (1203) too many connections for this user</item>
    ///         <item><c>ER_USER_LIMIT_REACHED</c> (1226) user resource limit reached</item>
    ///         <item><c>UnableToConnectToHost</c> the server refused or was unreachable</item>
    ///     </list>
    ///     Deliberately excludes lock/deadlock errors, which are statement conflicts rather than connection
    ///     refusals. Pure over the error code so it is unit-testable without constructing a
    ///     <see cref="MySqlException" />.
    /// </summary>
    internal static bool IsTransientConnectionError(MySqlErrorCode code)
    {
        return code is MySqlErrorCode.ConnectionCountError
            or MySqlErrorCode.TooManyUserConnections
            or MySqlErrorCode.UserLimitReached
            or MySqlErrorCode.UnableToConnectToHost;
    }

    public override IDatabaseProvider Provider => MySqlProvider.Instance;

    public override void WriteScript(TextWriter writer, Action<Migrator, TextWriter> writeStep)
    {
        writeStep(this, writer);
    }

    public override void WriteSchemaCreationSql(IEnumerable<string> schemaNames, TextWriter writer)
    {
        foreach (var schemaName in schemaNames)
        {
            writer.WriteLine(CreateDatabaseStatementFor(schemaName));
        }
    }

    public override void WriteSchemaDropSql(IEnumerable<string> schemaNames, TextWriter writer)
    {
        foreach (var schemaName in schemaNames)
        {
            writer.WriteLine($"DROP DATABASE IF EXISTS {SchemaUtils.QuoteName(schemaName)};");
        }
    }

    protected override async Task executeDelta(
        SchemaMigration migration,
        DbConnection conn,
        AutoCreate autoCreate,
        IMigrationLogger logger,
        CancellationToken ct = default)
    {
        await createSchemas(migration, conn, logger, ct).ConfigureAwait(false);

        foreach (var delta in migration.Deltas)
        {
            var writer = new StringWriter();
            WriteUpdate(writer, delta);

            if (writer.ToString().Trim().IsNotEmpty())
            {
                await executeCommand(conn, logger, writer, ct).ConfigureAwait(false);
            }
        }
    }

    public override string ToExecuteScriptLine(string scriptName)
    {
        return $"source {scriptName}";
    }

    /// <summary>
    ///     The characters that are unsafe in a MySQL identifier beyond the universal ones: the backtick
    ///     MySQL delimits identifiers with (<see cref="SchemaUtils.QuoteName" /> wraps every name in
    ///     backticks and does not double an embedded one), the double quote that delimits identifiers under
    ///     <c>ANSI_QUOTES</c> and string literals otherwise, and the backslash, which is an escape character
    ///     inside MySQL string literals unless <c>NO_BACKSLASH_ESCAPES</c> is set -- a trailing one would
    ///     otherwise swallow the closing quote of the literal a name is written into.
    /// </summary>
    private const string UnsafeIdentifierCharacters = "`\"\\";

    /// <summary>
    ///     MySQL's identifier length limit.
    /// </summary>
    public int MaxIdentifierLength { get; set; } = 64;

    /// <summary>
    ///     Validates a database object name before it is written into DDL. See
    ///     <see cref="IdentifierValidation" /> for why each rule is here; before weasel#416 this checked
    ///     length only, and threw <see cref="NullReferenceException" /> on a null name.
    /// </summary>
    /// <exception cref="ArgumentException">The name cannot be safely written into DDL.</exception>
    public override void AssertValidIdentifier(string name)
    {
        var problem = IdentifierValidation.FindProblem(name, UnsafeIdentifierCharacters);
        if (problem != null)
        {
            throw new ArgumentException($"MySQL identifier '{name}' is not valid because {problem}.");
        }

        if (name.Length > MaxIdentifierLength)
        {
            throw new ArgumentException(
                $"MySQL identifier '{name}' exceeds the {MaxIdentifierLength} character limit.");
        }
    }

    private static async Task createSchemas(
        SchemaMigration migration,
        DbConnection conn,
        IMigrationLogger logger,
        CancellationToken ct = default)
    {
        var writer = new StringWriter();

        if (migration.Schemas.Any())
        {
            new MySqlMigrator().WriteSchemaCreationSql(migration.Schemas, writer);
            if (writer.ToString().Trim().IsNotEmpty())
            {
                await executeCommand(conn, logger, writer, ct).ConfigureAwait(false);
            }
        }
    }

    private static async Task executeCommand(DbConnection conn, IMigrationLogger logger, StringWriter writer, CancellationToken ct = default)
    {
        var sql = writer.ToString().Trim();

        if (sql.IsEmpty())
        {
            return;
        }

        // One command for the whole delta. This used to split on semicolons and execute the pieces,
        // which shreds any body that contains one -- a trigger's BEGIN ... END block, a stored
        // procedure, a string literal with a semicolon in it. MySqlConnector executes several
        // semicolon-separated statements from a single command, so the split bought nothing and
        // cost correctness (weasel#452).
        {
            var cmd = conn.CreateCommand();
            cmd.CommandText = sql;
            logger.SchemaChange(cmd.CommandText);

            try
            {
                await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                if (logger is DefaultMigrationLogger)
                {
                    throw;
                }

                logger.OnFailure(cmd, e);
            }
        }
    }

    public static string CreateDatabaseStatementFor(string databaseName)
    {
        return $"CREATE DATABASE IF NOT EXISTS {SchemaUtils.QuoteName(databaseName)};";
    }

    public override async Task EnsureDatabaseExistsAsync(DbConnection connection, CancellationToken ct = default)
    {
        var builder = new MySqlConnectionStringBuilder(connection.ConnectionString);
        var databaseName = builder.Database;

        if (string.IsNullOrEmpty(databaseName))
        {
            throw new ArgumentException("The connection string does not specify a database name.");
        }

        builder.Database = "";
        await using var adminConn = new MySqlConnection(builder.ConnectionString);
        await adminConn.OpenAsync(ct).ConfigureAwait(false);

        var cmd = adminConn.CreateCommand();
        cmd.CommandText = $"CREATE DATABASE IF NOT EXISTS {SchemaUtils.QuoteName(databaseName)}";
        await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    public override ITable CreateTable(DbObjectName identifier)
    {
        return new Tables.Table(identifier);
    }

    public override string GenerateDeleteAllSql(IReadOnlyList<DbObjectName> tables, bool resetIdentity = true)
    {
        if (tables.Count == 0)
        {
            return string.Empty;
        }

        var sb = new StringBuilder();
        sb.AppendLine("SET FOREIGN_KEY_CHECKS = 0;");

        foreach (var table in tables)
        {
            sb.AppendLine($"TRUNCATE TABLE {table.QualifiedName};");
        }

        sb.AppendLine("SET FOREIGN_KEY_CHECKS = 1;");

        return sb.ToString();
    }

    public override IDatabaseWithTables CreateDatabase(DbConnection connection, string? identifier = null)
    {
        if (connection is not MySqlConnection)
        {
            throw new ArgumentException("Expected MySqlConnection", nameof(connection));
        }

        var builder = new MySqlConnectionStringBuilder(connection.ConnectionString);
        return new DatabaseWithTables(identifier ?? builder.Database ?? "weasel", connection.ConnectionString);
    }
}
