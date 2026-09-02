using System.Data.Common;
using System.Text;
using JasperFx;
using JasperFx.Core;
using Microsoft.Data.SqlClient;
using Weasel.Core;
using Weasel.Core.Migrations;

namespace Weasel.SqlServer;

public class SqlServerMigrator: Migrator
{
    private const string BeginScript = @"DO $$
BEGIN";

    private const string EndScript = @"END
$$;
";

    public SqlServerMigrator(): base(SqlServerProvider.Instance.DefaultDatabaseSchemaName)
    {
    }

    public override bool MatchesConnection(DbConnection connection)
    {
        return connection is SqlConnection;
    }

    public override ValueTask ReleaseConnectionPoolAsync(DbConnection connection, CancellationToken ct = default)
    {
        if (connection is SqlConnection sql)
        {
            SqlConnection.ClearPool(sql);
        }

        return ValueTask.CompletedTask;
    }

    public override bool IsTransientConnectionFailure(Exception exception)
    {
        foreach (var e in ExceptionChain.Flatten(exception))
        {
            if (e is SqlException sql && sql.Errors.Cast<SqlError>().Any(x => IsTransientConnectionError(x.Number)))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    ///     True for the SQL Server error numbers that unambiguously mean "could not give you a connection
    ///     right now, try again" rather than "your migration is wrong" (weasel#356) -- the on-premises
    ///     connection-limit error plus the Azure SQL resource/throttling set:
    ///     <list type="bullet">
    ///         <item><c>17809</c> could not connect: too many user connections (on-premises)</item>
    ///         <item><c>40197</c>, <c>40501</c> service is busy / error processing the request</item>
    ///         <item><c>40613</c> database is currently unavailable</item>
    ///         <item><c>10928</c>, <c>10929</c> resource limits reached</item>
    ///         <item><c>49918</c>, <c>49919</c>, <c>49920</c> not enough resources to process the request</item>
    ///     </list>
    ///     The exclusions matter more than the inclusions here, because a false positive re-runs a
    ///     migration that actually failed:
    ///     <list type="bullet">
    ///         <item>
    ///             <c>-2</c> is <b>not</b> here despite being the classic "timeout expired": SqlClient
    ///             raises it for <i>command</i> timeouts too, so retrying it would silently re-run a DDL
    ///             statement that merely exceeded CommandTimeout (a slow CREATE INDEX on a large table)
    ///             three times over -- exactly what this predicate promises not to do.
    ///         </item>
    ///         <item>
    ///             <c>10053</c>/<c>10054</c>/<c>233</c>/<c>64</c>, likewise: a transport-level drop is
    ///             reported the same way whether it happened while connecting or midway through a
    ///             statement, so they cannot be distinguished from a half-applied migration.
    ///         </item>
    ///         <item><c>1205</c> deadlock is a statement conflict, handled at the statement level.</item>
    ///         <item><c>18456</c> login failed is a credential problem that will never clear.</item>
    ///     </list>
    ///     Erring narrow is the safe direction: a missed code just means no retry, i.e. today's behavior.
    ///     Pure over the error number so it is unit-testable without constructing a
    ///     <see cref="SqlException" />.
    /// </summary>
    internal static bool IsTransientConnectionError(int number)
    {
        return number is 10928 or 10929 or 17809 or 40197 or 40501 or 40613 or 49918 or 49919 or 49920;
    }

    public override IDatabaseProvider Provider => SqlServerProvider.Instance;

    public override void WriteScript(TextWriter writer, Action<Migrator, TextWriter> writeStep)
    {
        writeStep(this, writer);
    }

    public override void WriteSchemaCreationSql(IEnumerable<string> schemaNames, TextWriter writer)
    {
        foreach (var schemaName in schemaNames) writer.WriteLine(CreateSchemaStatementFor(schemaName));
    }

    public override void WriteSchemaDropSql(IEnumerable<string> schemaNames, TextWriter writer)
    {
        foreach (var schemaName in schemaNames)
        {
            writer.WriteLine($@"IF EXISTS ( SELECT  *
                    FROM    sys.schemas
                    WHERE   name = N'{SchemaUtils.EscapeLiteral(schemaName)}' )
        EXEC('DROP SCHEMA {SchemaUtils.EscapeLiteral(SchemaUtils.BracketName(schemaName))}');
");
        }
    }

    protected override async Task executeDelta(
        SchemaMigration migration,
        DbConnection conn,
        AutoCreate autoCreate,
        IMigrationLogger logger,
        CancellationToken ct = default
    )
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

        var deferred = new StringWriter();
        migration.WriteDeferredForeignKeys(deferred, this);

        if (deferred.ToString().Trim().IsNotEmpty())
        {
            await executeCommand(conn, logger, deferred, ct).ConfigureAwait(false);
        }
    }

    public override string ToExecuteScriptLine(string scriptName)
    {
        return $":r {scriptName}";
    }

    /// <summary>
    ///     The characters SQL Server delimits identifiers with. <c>]</c> is what closes a
    ///     <c>[...]</c> delimited identifier and <c>"</c> what closes a quoted one (SQL Server accepts both,
    ///     the latter under <c>QUOTED_IDENTIFIER ON</c>); <c>[</c> is rejected alongside <c>]</c> so that an
    ///     already-bracketed name is caught rather than being bracketed a second time.
    /// </summary>
    private const string UnsafeIdentifierCharacters = "[]\"";

    /// <summary>
    ///     SQL Server's identifier length limit -- <c>sysname</c> is <c>nvarchar(128)</c>, so anything
    ///     longer is rejected by the server itself.
    /// </summary>
    public int MaxIdentifierLength { get; set; } = 128;

    /// <summary>
    ///     SQL Server refuses any request carrying more than 2100 parameters. The budget sits under
    ///     that rather than on it: undershooting only costs a round trip, while reaching it costs the
    ///     whole migration.
    /// </summary>
    public override int MaxParametersPerCommand => 2000;

    /// <summary>
    ///     Validates a database object name before it is written into DDL. See
    ///     <see cref="IdentifierValidation" /> for why each rule is here; this method had no body at all
    ///     before weasel#416.
    /// </summary>
    /// <exception cref="InvalidOperationException">The name cannot be safely written into DDL.</exception>
    public override void AssertValidIdentifier(string name)
    {
        AssertValidLocalIdentifier(name);

        if (name.Length > MaxIdentifierLength)
        {
            throw new InvalidOperationException(
                $"SQL Server identifiers cannot exceed {MaxIdentifierLength} characters. '{name}' is {name.Length} characters.");
        }
    }

    /// <summary>
    ///     The safety half of <see cref="AssertValidIdentifier" />, without the length limit.
    /// </summary>
    /// <remarks>
    ///     A column, primary key or check constraint name is only ever emitted inside its own
    ///     table's DDL and is never addressed by name afterwards, and the delta comparison already
    ///     reads both sides through TruncatedNameIdentifier so a name SQL Server truncated still
    ///     matches. Refusing to create one would reject schemas the rest of Weasel handles
    ///     (weasel#485). The safety rules still apply in full.
    /// </remarks>
    public override void AssertValidLocalIdentifier(string name)
    {
        var problem = IdentifierValidation.FindProblem(name, UnsafeIdentifierCharacters);
        if (problem != null)
        {
            throw new InvalidOperationException(
                $"SQL Server identifier '{name}' is not valid because {problem}.");
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
            new SqlServerMigrator().WriteSchemaCreationSql(migration.Schemas, writer);
            if (writer.ToString().Trim().IsNotEmpty()) // Cheesy way of knowing if there is any delta
            {
                await executeCommand(conn, logger, writer, ct).ConfigureAwait(false);
            }
        }
    }

    private static async Task executeCommand(DbConnection conn, IMigrationLogger logger, StringWriter writer, CancellationToken ct = default)
    {
        var cmd = conn.CreateCommand(writer.ToString());
        logger.SchemaChange(cmd.CommandText);

        try
        {
            await cmd
                .ExecuteNonQueryAsync(ct).ConfigureAwait(false);
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

    public static string CreateSchemaStatementFor(string schemaName)
    {
        return $@"
IF NOT EXISTS ( SELECT  *
                FROM    sys.schemas
                WHERE   name = N'{SchemaUtils.EscapeLiteral(schemaName)}' )
    EXEC('CREATE SCHEMA {SchemaUtils.EscapeLiteral(SchemaUtils.BracketName(schemaName))}');

";
    }

    /// <summary>
    ///     How long <see cref="EnsureDatabaseExistsAsync" /> will keep retrying a connection to the target
    ///     database before giving up. A freshly created SQL Server database can briefly refuse logins, and
    ///     on a cold container that window can run to tens of seconds -- so the method does not return until
    ///     the database actually accepts a connection. Set to <see cref="TimeSpan.Zero" /> to make a single
    ///     attempt and fail fast, which is usually what you want against a warm local server.
    /// </summary>
    public TimeSpan DatabaseAvailabilityTimeout { get; set; } = 30.Seconds();

    /// <summary>
    ///     How long <see cref="EnsureDatabaseExistsAsync" /> waits between connection attempts while the
    ///     newly created database is still refusing logins.
    /// </summary>
    public TimeSpan DatabaseAvailabilityPollingInterval { get; set; } = 1.Seconds();

    /// <summary>
    ///     Creates the database named by the connection's <c>Initial Catalog</c> if it does not already
    ///     exist, then blocks until that database accepts a connection.
    /// </summary>
    /// <remarks>
    ///     Safe to call from several processes at once (weasel#415). The existence check and the
    ///     <c>CREATE DATABASE</c> are not atomic -- SQL Server offers no form that makes them so -- so a
    ///     failed create is judged by whether the database exists afterwards rather than by the error it
    ///     raised. Matching error 1801 alone was not enough: SQL Server serializes database creation, and
    ///     under real contention a losing session is killed with a severe error carrying no useful number
    ///     instead of getting the tidy "already exists". Waiting for the database to accept a connection is
    ///     done unconditionally rather than only after we create it, because a concurrent creator leaves
    ///     the same window open for us.
    /// </remarks>
    /// <exception cref="ArgumentException">The connection string does not name a database.</exception>
    /// <exception cref="TimeoutException">
    ///     The database exists but did not accept a connection within <see cref="DatabaseAvailabilityTimeout" />.
    /// </exception>
    public override async Task EnsureDatabaseExistsAsync(DbConnection connection, CancellationToken ct = default)
    {
        var builder = new SqlConnectionStringBuilder(connection.ConnectionString);
        var databaseName = builder.InitialCatalog;

        if (string.IsNullOrEmpty(databaseName))
        {
            throw new ArgumentException("The connection string does not specify a database name (Initial Catalog).");
        }

        var targetConnectionString = builder.ConnectionString;

        builder.InitialCatalog = "master";
        var adminConnectionString = builder.ConnectionString;

        await using (var adminConn = new SqlConnection(adminConnectionString))
        {
            await adminConn.OpenAsync(ct).ConfigureAwait(false);

            if (!await databaseExistsAsync(adminConn, databaseName, ct).ConfigureAwait(false))
            {
                var createCmd = adminConn.CreateCommand();

                // CREATE DATABASE takes no parameters, so the name has to be interpolated. Doubling ']'
                // is what makes it a well-formed delimited identifier. Bracket rather than
                // BracketName: this name comes off a connection string, where a leading '[' is part
                // of the name and not a delimiter the caller added.
                createCmd.CommandText = $"CREATE DATABASE {SchemaUtils.Bracket(databaseName)}";

                try
                {
                    await createCmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
                }
                catch (SqlException)
                {
                    // Losing the creation race is decided by the postcondition, not by the error code.
                    // Error 1801 is only the tidy outcome; SQL Server serializes CREATE DATABASE and
                    // under real contention it will instead kill a losing session outright with a severe
                    // error carrying no useful number, which the old `when (e.Number == 1801)` filter
                    // let straight through to the caller. What matters either way is whether the
                    // database is now there.
                    if (!await databaseExistsOnNewConnectionAsync(adminConnectionString, databaseName, ct)
                            .ConfigureAwait(false))
                    {
                        throw;
                    }
                }
            }
        }

        await waitUntilDatabaseAcceptsConnectionsAsync(targetConnectionString, databaseName, ct)
            .ConfigureAwait(false);
    }

    private static async Task<bool> databaseExistsAsync(SqlConnection conn, string databaseName,
        CancellationToken ct)
    {
        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT DB_ID(@name)";

        var param = cmd.CreateParameter();
        param.ParameterName = "@name";
        param.Value = databaseName;
        cmd.Parameters.Add(param);

        var result = await cmd.ExecuteScalarAsync(ct).ConfigureAwait(false);

        return result is not (null or DBNull);
    }

    /// <summary>
    ///     Re-checks existence on a connection of its own. The severe error that a losing CREATE DATABASE
    ///     racer gets is raised with <c>breakConnection</c> set, so the connection that issued the
    ///     statement is already dead by the time we want to ask this question on it.
    /// </summary>
    private static async Task<bool> databaseExistsOnNewConnectionAsync(string adminConnectionString,
        string databaseName, CancellationToken ct)
    {
        try
        {
            await using var conn = new SqlConnection(adminConnectionString);
            await conn.OpenAsync(ct).ConfigureAwait(false);

            return await databaseExistsAsync(conn, databaseName, ct).ConfigureAwait(false);
        }
        catch (SqlException)
        {
            // Could not find out. Report the original creation failure rather than this one.
            return false;
        }
    }

    private async Task waitUntilDatabaseAcceptsConnectionsAsync(
        string connectionString,
        string databaseName,
        CancellationToken ct
    )
    {
        var timeout = DatabaseAvailabilityTimeout;
        var deadline = DateTimeOffset.UtcNow + timeout;

        while (true)
        {
            ct.ThrowIfCancellationRequested();

            try
            {
                await using var conn = new SqlConnection(connectionString);
                await conn.OpenAsync(ct).ConfigureAwait(false);
                return;
            }
            catch (SqlException e)
            {
                if (DateTimeOffset.UtcNow >= deadline)
                {
                    throw new TimeoutException(
                        $"Database '{databaseName}' exists, but did not accept a connection within {timeout}. See {nameof(SqlServerMigrator)}.{nameof(DatabaseAvailabilityTimeout)} if the database needs longer to come online.",
                        e);
                }

                await Task.Delay(DatabaseAvailabilityPollingInterval, ct).ConfigureAwait(false);
            }
        }
    }

    public override ITable CreateTable(DbObjectName identifier)
    {
        return new Tables.Table(identifier);
    }

    public override SequenceBase CreateSequence(DbObjectName identifier)
    {
        return new Sequence(identifier);
    }

    public override string GenerateDeleteAllSql(IReadOnlyList<DbObjectName> tables, bool resetIdentity = true)
    {
        if (tables.Count == 0)
        {
            return string.Empty;
        }

        var sb = new StringBuilder();

        foreach (var table in tables)
        {
            sb.AppendLine($"DELETE FROM {table};");
        }

        if (resetIdentity)
        {
            foreach (var table in tables)
            {
                sb.AppendLine($"BEGIN TRY DBCC CHECKIDENT('{table}', RESEED, 0); END TRY BEGIN CATCH END CATCH;");
            }
        }

        return sb.ToString();
    }

    public override IDatabaseWithTables CreateDatabase(DbConnection connection, string? identifier = null)
    {
        if (connection is not SqlConnection)
        {
            throw new ArgumentException("Expected SqlConnection", nameof(connection));
        }

        var builder = new SqlConnectionStringBuilder(connection.ConnectionString);
        return new DatabaseWithTables(identifier ?? builder.InitialCatalog ?? "weasel", connection.ConnectionString);
    }
}
